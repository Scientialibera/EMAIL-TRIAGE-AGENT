from __future__ import annotations

import logging
from typing import Any

from botbuilder.core import ActivityHandler, CardFactory, MessageFactory, TurnContext

from src.cards.digest_card import build_digest_card
from src.cards.draft_card import build_draft_card
from src.cards.triage_card import build_triage_card
from src.graph.mail import send_reply
from src.services.cosmos_store import (
    get_email_record,
    get_pending_drafts,
    get_stats,
    get_today_records,
    update_feedback,
)
from src.services.mailbox_config import invalidate_cache, load_mailbox_config

logger = logging.getLogger(__name__)

_conversation_refs: dict[str, dict] = {}


def store_conversation_reference(user_key: str, ref: dict) -> None:
    _conversation_refs[user_key] = ref


def get_conversation_reference(user_key: str) -> dict | None:
    return _conversation_refs.get(user_key)


def get_all_conversation_references() -> dict[str, dict]:
    return dict(_conversation_refs)


class TriageBot(ActivityHandler):
    """Handles draft review card actions and text commands."""

    async def on_message_activity(self, turn_context: TurnContext) -> None:
        if turn_context.activity.value:
            await self._handle_card_action(turn_context)
            return

        text = (turn_context.activity.text or "").strip().lower()

        if text == "inbox":
            await self._handle_inbox(turn_context)
        elif text == "pending":
            await self._handle_pending(turn_context)
        elif text == "stats":
            await self._handle_stats(turn_context)
        elif text == "refresh":
            await self._handle_refresh(turn_context)
        else:
            await turn_context.send_activity(
                MessageFactory.text(
                    "Available commands: **inbox**, **pending**, **stats**, **refresh**"
                )
            )

    async def on_members_added_activity(self, members_added, turn_context: TurnContext) -> None:
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                await turn_context.send_activity(
                    MessageFactory.text(
                        "Welcome to Email Triage Agent! I classify incoming emails, draft replies, "
                        "and surface them for your review.\n\n"
                        "Commands: **inbox**, **pending**, **stats**, **refresh**"
                    )
                )

    async def _handle_card_action(self, turn_context: TurnContext) -> None:
        data = turn_context.activity.value or {}
        action = data.get("action", "")
        record_id = data.get("record_id", "")
        mailbox = data.get("mailbox", "")

        if action == "view_draft":
            record = await get_email_record(record_id, mailbox)
            if record and record.get("draft"):
                card = build_draft_card(record)
                await turn_context.send_activity(MessageFactory.attachment(CardFactory.adaptive_card(card)))
            else:
                await turn_context.send_activity(MessageFactory.text("Draft not found."))

        elif action == "approve_draft":
            record = await get_email_record(record_id, mailbox)
            if record and record.get("draft"):
                draft = record["draft"]
                await send_reply(mailbox, record["message_id"], draft["body"])
                await update_feedback(record_id, mailbox, "approved")
                await turn_context.send_activity(MessageFactory.text(f"Reply sent for: **{record.get('subject', '')}**"))
            else:
                await turn_context.send_activity(MessageFactory.text("Record not found."))

        elif action == "edit_draft":
            edited_body = data.get("edited_body", "").strip()
            if not edited_body:
                await turn_context.send_activity(MessageFactory.text("Please provide edited text in the text field."))
                return
            record = await get_email_record(record_id, mailbox)
            if record:
                await send_reply(mailbox, record["message_id"], edited_body)
                await update_feedback(record_id, mailbox, "edited")
                await turn_context.send_activity(MessageFactory.text(f"Edited reply sent for: **{record.get('subject', '')}**"))

        elif action == "reject_draft":
            await update_feedback(record_id, mailbox, "rejected")
            await turn_context.send_activity(MessageFactory.text("Draft rejected. Feedback recorded."))

        else:
            logger.warning("Unknown card action: %s", action)

    async def _handle_inbox(self, turn_context: TurnContext) -> None:
        configs = await load_mailbox_config()
        if not configs:
            await turn_context.send_activity(MessageFactory.text("No mailboxes configured."))
            return

        for mb in configs:
            records = await get_today_records(mb.mailbox)
            card = build_digest_card(mb.display_name, records)
            await turn_context.send_activity(MessageFactory.attachment(CardFactory.adaptive_card(card)))

    async def _handle_pending(self, turn_context: TurnContext) -> None:
        configs = await load_mailbox_config()
        total_pending = 0
        for mb in configs:
            pending = await get_pending_drafts(mb.mailbox)
            total_pending += len(pending)
            for record in pending[:5]:
                card = build_draft_card(record)
                await turn_context.send_activity(MessageFactory.attachment(CardFactory.adaptive_card(card)))

        if total_pending == 0:
            await turn_context.send_activity(MessageFactory.text("No pending drafts to review."))

    async def _handle_stats(self, turn_context: TurnContext) -> None:
        configs = await load_mailbox_config()
        lines = ["**Classification Stats (last 7 days)**\n"]
        for mb in configs:
            stats = await get_stats(mb.mailbox)
            lines.append(f"**{mb.display_name}** ({mb.mailbox})")
            lines.append(f"- Total: {stats['total']}")
            for cls, count in stats.get("by_classification", {}).items():
                lines.append(f"  - {cls}: {count}")
            lines.append(f"- Avg confidence: {stats['avg_confidence']:.0%}")
            fb = stats.get("feedback", {})
            lines.append(f"- Feedback: {fb.get('approved', 0)} approved, {fb.get('edited', 0)} edited, {fb.get('rejected', 0)} rejected\n")

        await turn_context.send_activity(MessageFactory.text("\n".join(lines)))

    async def _handle_refresh(self, turn_context: TurnContext) -> None:
        invalidate_cache()
        await load_mailbox_config(force_refresh=True)
        await turn_context.send_activity(MessageFactory.text("Mailbox configuration reloaded from blob."))
