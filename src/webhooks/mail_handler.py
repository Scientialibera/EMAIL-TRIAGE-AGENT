"""Process Graph mail change notifications: fetch -> classify -> draft -> store -> notify."""

from __future__ import annotations

import base64
import logging
import re
from html.parser import HTMLParser
from io import StringIO
from typing import Any

from botbuilder.core import BotFrameworkAdapter, CardFactory, MessageFactory, TurnContext

from src.bot import get_all_conversation_references
from src.cards.draft_card import build_draft_card
from src.cards.triage_card import build_triage_card
from src.graph.mail import fetch_attachments, fetch_message
from src.services.classifier import classify_email
from src.services.cosmos_store import store_email_record
from src.services.drafter import draft_reply
from src.services.mailbox_config import MailboxDefinition, get_mailbox_by_address
from src.services.ocr import extract_text_from_attachment

logger = logging.getLogger(__name__)


class _HTMLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self._text = StringIO()

    def handle_data(self, data: str) -> None:
        self._text.write(data)

    def get_text(self) -> str:
        return self._text.getvalue()


def _strip_html(html: str) -> str:
    stripper = _HTMLStripper()
    stripper.feed(html)
    return stripper.get_text()


def _extract_mailbox_from_resource(resource: str) -> str:
    """Extract mailbox address from Graph resource path like /users/inbox@co.com/..."""
    match = re.search(r"/users/([^/]+)/", resource)
    return match.group(1) if match else ""


async def process_mail_notification(
    notification: dict[str, Any],
    adapter: BotFrameworkAdapter,
) -> None:
    """Full triage pipeline for a single mail notification."""
    resource = notification.get("resource", "")
    mailbox = _extract_mailbox_from_resource(resource)
    if not mailbox:
        logger.warning("Could not extract mailbox from resource: %s", resource)
        return

    mb_config = get_mailbox_by_address(mailbox)
    if not mb_config:
        logger.warning("No config for mailbox %s, skipping.", mailbox)
        return

    resource_data = notification.get("resourceData", {})
    message_id = resource_data.get("id", "")
    if not message_id:
        logger.warning("No message ID in notification for %s.", mailbox)
        return

    msg = await fetch_message(mailbox, message_id)
    from_addr = msg.get("from", {}).get("emailAddress", {}).get("address", "").lower()
    from_name = msg.get("from", {}).get("emailAddress", {}).get("name", "")

    if from_addr in mb_config.rules.skip_senders:
        logger.info("Skipping email from %s (in skip_senders).", from_addr)
        return

    body_html = msg.get("body", {}).get("content", "")
    body_text = _strip_html(body_html) if msg.get("body", {}).get("contentType") == "html" else body_html

    attachment_text = ""
    if msg.get("hasAttachments"):
        attachments = await fetch_attachments(mailbox, message_id)
        for att in attachments:
            content_type = att.get("contentType", "")
            content_bytes = base64.b64decode(att.get("contentBytes", "")) if att.get("contentBytes") else b""
            if content_bytes:
                text = await extract_text_from_attachment(content_bytes, content_type)
                if text:
                    attachment_text += f"\n--- {att.get('name', 'attachment')} ---\n{text}\n"

    classification = await classify_email(
        subject=msg.get("subject", ""),
        from_address=from_addr,
        body_text=body_text,
        attachment_text=attachment_text,
    )

    if from_addr in mb_config.rules.always_urgent_senders and classification.classification != "urgent":
        classification.classification = "urgent"
        classification.urgency = "high"
        classification.reasoning += " (Sender is in always_urgent list.)"

    record: dict[str, Any] = {
        "mailbox": mailbox,
        "message_id": message_id,
        "conversation_id": msg.get("conversationId", ""),
        "subject": msg.get("subject", ""),
        "from_address": from_addr,
        "from_name": from_name,
        "received_at": msg.get("receivedDateTime", ""),
        "has_attachments": msg.get("hasAttachments", False),
        "attachment_text": attachment_text[:5000] if attachment_text else "",
        **classification.to_dict(),
    }

    if classification.classification == "needs_reply" and mb_config.auto_draft:
        draft = await draft_reply(
            subject=msg.get("subject", ""),
            from_address=from_addr,
            body_text=body_text,
            classification_topic=classification.topic,
        )
        record["draft"] = draft.to_dict()

    stored = await store_email_record(record)

    await _notify_users(stored, mb_config, adapter)

    logger.info(
        "Processed email %s for %s: %s (%s)",
        message_id, mailbox, classification.classification, classification.urgency,
    )


async def _notify_users(
    record: dict[str, Any],
    mb_config: MailboxDefinition,
    adapter: BotFrameworkAdapter,
) -> None:
    """Send triage/draft cards to configured users via proactive messaging."""
    refs = get_all_conversation_references()

    triage_card = build_triage_card(record)
    triage_attachment = CardFactory.adaptive_card(triage_card)

    draft_attachment = None
    if record.get("draft"):
        draft_card = build_draft_card(record)
        draft_attachment = CardFactory.adaptive_card(draft_card)

    for upn in mb_config.notify_user_upns:
        ref = refs.get(upn)
        if not ref:
            logger.debug("No conversation reference for %s, cannot notify.", upn)
            continue
        try:
            async def _send_cards(tc: TurnContext) -> None:
                await tc.send_activity(MessageFactory.attachment(triage_attachment))
                if draft_attachment:
                    await tc.send_activity(MessageFactory.attachment(draft_attachment))

            await adapter.continue_conversation(ref, _send_cards, app_id=None)
        except Exception:
            logger.error("Failed to notify %s", upn, exc_info=True)
