from __future__ import annotations

import logging
from typing import Any

from src.graph.auth import graph_get, graph_get_binary, graph_post

logger = logging.getLogger(__name__)


async def fetch_message(mailbox: str, message_id: str) -> dict[str, Any]:
    """Fetch full email message including body and metadata."""
    path = f"/users/{mailbox}/messages/{message_id}"
    params = {"$select": "id,conversationId,subject,from,receivedDateTime,body,hasAttachments,importance,isRead"}
    return await graph_get(path, params=params)


async def fetch_attachments(mailbox: str, message_id: str) -> list[dict[str, Any]]:
    """Fetch all attachments for a message."""
    path = f"/users/{mailbox}/messages/{message_id}/attachments"
    result = await graph_get(path)
    return result.get("value", [])


async def fetch_attachment_content(mailbox: str, message_id: str, attachment_id: str) -> bytes:
    """Download raw attachment content."""
    path = f"/users/{mailbox}/messages/{message_id}/attachments/{attachment_id}/$value"
    return await graph_get_binary(path)


async def send_reply(mailbox: str, message_id: str, body: str, subject: str | None = None) -> None:
    """Send a reply to a message via Graph."""
    path = f"/users/{mailbox}/messages/{message_id}/reply"
    reply_body: dict[str, Any] = {
        "message": {
            "body": {
                "contentType": "Text",
                "content": body,
            }
        }
    }
    await graph_post(path, reply_body)
    logger.info("Reply sent for message %s in %s", message_id, mailbox)


async def mark_as_read(mailbox: str, message_id: str) -> None:
    """Mark a message as read."""
    from src.graph.auth import graph_patch
    path = f"/users/{mailbox}/messages/{message_id}"
    await graph_patch(path, {"isRead": True})
