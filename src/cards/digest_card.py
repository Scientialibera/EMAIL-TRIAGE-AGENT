from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def build_digest_card(mailbox_name: str, records: list[dict[str, Any]]) -> dict[str, Any]:
    """Adaptive Card for the daily email triage digest."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    counts: dict[str, int] = {"urgent": 0, "needs_reply": 0, "fyi": 0, "spam": 0}
    pending_review = 0
    replied = 0
    for r in records:
        cls = r.get("classification", "fyi")
        counts[cls] = counts.get(cls, 0) + 1
        if r.get("classification") == "needs_reply":
            if r.get("feedback") in ("approved", "edited"):
                replied += 1
            elif not r.get("feedback"):
                pending_review += 1

    total = len(records)

    body: list[dict[str, Any]] = [
        {"type": "TextBlock", "text": f"Daily Digest -- {mailbox_name}", "weight": "Bolder", "size": "Large"},
        {"type": "TextBlock", "text": today, "isSubtle": True, "spacing": "None"},
        {"type": "TextBlock", "text": f"{total} emails processed today", "spacing": "Medium"},
        {"type": "FactSet", "facts": [
            {"title": "Urgent", "value": str(counts["urgent"])},
            {"title": "Needs Reply", "value": str(counts["needs_reply"])},
            {"title": "FYI", "value": str(counts["fyi"])},
            {"title": "Spam", "value": str(counts["spam"])},
        ], "spacing": "Medium"},
    ]

    if counts["needs_reply"] > 0:
        body.append({"type": "FactSet", "facts": [
            {"title": "Replied", "value": str(replied)},
            {"title": "Awaiting Review", "value": str(pending_review)},
        ], "spacing": "Medium"})

    if pending_review > 0:
        body.append({
            "type": "TextBlock",
            "text": f"{pending_review} draft(s) still awaiting your review.",
            "color": "Warning",
            "weight": "Bolder",
            "spacing": "Medium",
        })

    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": body,
    }
