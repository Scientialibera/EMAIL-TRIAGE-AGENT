"""Cosmos DB CRUD for email classification, drafts, and feedback."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.exceptions import CosmosResourceNotFoundError
from azure.identity import DefaultAzureCredential

from src.config import get_settings

logger = logging.getLogger(__name__)

_container = None


def _get_container():
    global _container
    if _container is None:
        s = get_settings()
        credential = DefaultAzureCredential()
        client = CosmosClient(url=s.cosmos_endpoint, credential=credential)
        db = client.get_database_client(s.cosmos_database)
        _container = db.get_container_client(s.cosmos_container)
    return _container


async def store_email_record(record: dict[str, Any]) -> dict[str, Any]:
    """Upsert an email classification record."""
    container = _get_container()
    if "id" not in record:
        record["id"] = str(uuid.uuid4())
    if "processed_at" not in record:
        record["processed_at"] = datetime.now(timezone.utc).isoformat()
    container.upsert_item(record)
    logger.info("Stored email record %s for %s", record["id"], record.get("mailbox", "?"))
    return record


async def get_email_record(record_id: str, mailbox: str) -> dict[str, Any] | None:
    container = _get_container()
    try:
        return container.read_item(item=record_id, partition_key=mailbox)
    except CosmosResourceNotFoundError:
        return None


async def update_feedback(record_id: str, mailbox: str, feedback: str) -> dict[str, Any] | None:
    """Update feedback on an email record (approved, edited, rejected)."""
    container = _get_container()
    try:
        item = container.read_item(item=record_id, partition_key=mailbox)
        item["feedback"] = feedback
        item["feedback_at"] = datetime.now(timezone.utc).isoformat()
        if feedback == "approved":
            item["response_sent_at"] = datetime.now(timezone.utc).isoformat()
        container.upsert_item(item)
        return item
    except CosmosResourceNotFoundError:
        logger.warning("Record %s not found for feedback update.", record_id)
        return None


async def get_pending_drafts(mailbox: str) -> list[dict[str, Any]]:
    """Get emails with drafts awaiting review."""
    container = _get_container()
    query = (
        "SELECT * FROM c WHERE c.mailbox = @mailbox "
        "AND c.classification = 'needs_reply' "
        "AND IS_DEFINED(c.draft) "
        "AND (NOT IS_DEFINED(c.feedback) OR c.feedback = null) "
        "ORDER BY c.received_at DESC"
    )
    params = [{"name": "@mailbox", "value": mailbox}]
    return list(container.query_items(query=query, parameters=params, partition_key=mailbox))


async def get_today_records(mailbox: str) -> list[dict[str, Any]]:
    """Get all email records processed today for a mailbox."""
    container = _get_container()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    query = (
        "SELECT * FROM c WHERE c.mailbox = @mailbox "
        "AND STARTSWITH(c.processed_at, @today) "
        "ORDER BY c.received_at DESC"
    )
    params = [
        {"name": "@mailbox", "value": mailbox},
        {"name": "@today", "value": today},
    ]
    return list(container.query_items(query=query, parameters=params, partition_key=mailbox))


async def get_stats(mailbox: str, days: int = 7) -> dict[str, Any]:
    """Get classification stats for a mailbox over the last N days."""
    container = _get_container()
    cutoff = (datetime.now(timezone.utc) - __import__("datetime").timedelta(days=days)).isoformat()
    query = (
        "SELECT c.classification, c.feedback, c.confidence "
        "FROM c WHERE c.mailbox = @mailbox AND c.processed_at >= @cutoff"
    )
    params = [
        {"name": "@mailbox", "value": mailbox},
        {"name": "@cutoff", "value": cutoff},
    ]
    items = list(container.query_items(query=query, parameters=params, partition_key=mailbox))

    total = len(items)
    by_class = {}
    feedback_count = {"approved": 0, "edited": 0, "rejected": 0}
    total_confidence = 0.0

    for item in items:
        cls = item.get("classification", "unknown")
        by_class[cls] = by_class.get(cls, 0) + 1
        fb = item.get("feedback")
        if fb in feedback_count:
            feedback_count[fb] += 1
        total_confidence += item.get("confidence", 0)

    return {
        "total": total,
        "by_classification": by_class,
        "feedback": feedback_count,
        "avg_confidence": total_confidence / total if total > 0 else 0,
        "days": days,
    }
