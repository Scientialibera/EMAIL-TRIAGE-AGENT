"""Background scheduler for Graph subscription renewal and daily digest."""

from __future__ import annotations

import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from botbuilder.core import BotFrameworkAdapter, CardFactory, MessageFactory, TurnContext

from src.bot import get_all_conversation_references
from src.cards.digest_card import build_digest_card
from src.graph.subscriptions import renew_all_subscriptions
from src.services.cosmos_store import get_today_records
from src.services.mailbox_config import load_mailbox_config

logger = logging.getLogger(__name__)

SUBSCRIPTION_RENEWAL_INTERVAL_MINUTES = 40
DIGEST_HOUR = 17
DIGEST_MINUTE = 0


class TriageScheduler:
    def __init__(self, adapter: BotFrameworkAdapter):
        self.adapter = adapter
        self.scheduler = AsyncIOScheduler()

    async def start(self) -> None:
        configs = await load_mailbox_config()
        mailboxes = [c.mailbox for c in configs]

        self.scheduler.add_job(
            self._renew_subscriptions,
            IntervalTrigger(minutes=SUBSCRIPTION_RENEWAL_INTERVAL_MINUTES),
            args=[mailboxes],
            id="renew_subscriptions",
            replace_existing=True,
        )

        self.scheduler.add_job(
            self._send_daily_digest,
            CronTrigger(hour=DIGEST_HOUR, minute=DIGEST_MINUTE),
            id="daily_digest",
            replace_existing=True,
        )

        self.scheduler.start()

        await self._renew_subscriptions(mailboxes)
        logger.info("Scheduler started: %d mailboxes, renewal every %d min, digest at %d:%02d UTC.",
                     len(mailboxes), SUBSCRIPTION_RENEWAL_INTERVAL_MINUTES, DIGEST_HOUR, DIGEST_MINUTE)

    async def _renew_subscriptions(self, mailboxes: list[str]) -> None:
        try:
            await renew_all_subscriptions(mailboxes)
        except Exception:
            logger.error("Subscription renewal failed.", exc_info=True)

    async def _send_daily_digest(self) -> None:
        configs = await load_mailbox_config()
        refs = get_all_conversation_references()

        for mb in configs:
            records = await get_today_records(mb.mailbox)
            if not records:
                continue

            card = build_digest_card(mb.display_name, records)
            attachment = CardFactory.adaptive_card(card)

            for upn in mb.notify_user_upns:
                ref = refs.get(upn)
                if not ref:
                    continue
                try:
                    await self.adapter.continue_conversation(
                        ref,
                        lambda tc: tc.send_activity(MessageFactory.attachment(attachment)),
                        app_id=None,
                    )
                except Exception:
                    logger.error("Failed to send digest to %s", upn, exc_info=True)

        logger.info("Daily digest sent for %d mailboxes.", len(configs))

    async def reload_config(self) -> None:
        from src.services.mailbox_config import invalidate_cache
        invalidate_cache()
        configs = await load_mailbox_config(force_refresh=True)
        mailboxes = [c.mailbox for c in configs]

        for job in self.scheduler.get_jobs():
            if job.id == "renew_subscriptions":
                job.modify(args=[mailboxes])

        await self._renew_subscriptions(mailboxes)
        logger.info("Scheduler reloaded with %d mailboxes.", len(mailboxes))
