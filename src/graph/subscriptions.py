from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from src.config import get_settings
from src.graph.auth import graph_delete, graph_get, graph_patch, graph_post

logger = logging.getLogger(__name__)

SUBSCRIPTION_RENEWAL_MINUTES = 50
_ACTIVE_SUBSCRIPTIONS: dict[str, str] = {}


async def ensure_mail_subscription(mailbox: str) -> str:
    """Create or renew a Graph change notification subscription for new mail."""
    s = get_settings()
    sub_id = _ACTIVE_SUBSCRIPTIONS.get(mailbox)

    if sub_id:
        try:
            new_expiry = (datetime.now(timezone.utc) + timedelta(minutes=SUBSCRIPTION_RENEWAL_MINUTES)).isoformat()
            await graph_patch(f"/subscriptions/{sub_id}", {"expirationDateTime": new_expiry})
            logger.info("Renewed subscription %s for %s", sub_id, mailbox)
            return sub_id
        except Exception:
            logger.warning("Failed to renew subscription %s, creating new one.", sub_id, exc_info=True)
            _ACTIVE_SUBSCRIPTIONS.pop(mailbox, None)

    expiry = (datetime.now(timezone.utc) + timedelta(minutes=SUBSCRIPTION_RENEWAL_MINUTES)).isoformat()
    body = {
        "changeType": "created",
        "notificationUrl": s.webhook_url,
        "resource": f"/users/{mailbox}/mailFolders/inbox/messages",
        "expirationDateTime": expiry,
        "clientState": "email-triage-agent",
    }
    result = await graph_post("/subscriptions", body)
    sub_id = result["id"]
    _ACTIVE_SUBSCRIPTIONS[mailbox] = sub_id
    logger.info("Created subscription %s for %s (expires %s)", sub_id, mailbox, expiry)
    return sub_id


async def delete_mail_subscription(mailbox: str) -> None:
    sub_id = _ACTIVE_SUBSCRIPTIONS.pop(mailbox, None)
    if sub_id:
        try:
            await graph_delete(f"/subscriptions/{sub_id}")
            logger.info("Deleted subscription %s for %s", sub_id, mailbox)
        except Exception:
            logger.warning("Failed to delete subscription %s", sub_id, exc_info=True)


async def renew_all_subscriptions(mailboxes: list[str]) -> None:
    """Renew subscriptions for all monitored mailboxes."""
    for mailbox in mailboxes:
        try:
            await ensure_mail_subscription(mailbox)
        except Exception:
            logger.error("Failed to ensure subscription for %s", mailbox, exc_info=True)
