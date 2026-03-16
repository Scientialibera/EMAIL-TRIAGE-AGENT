from __future__ import annotations

from typing import Any

URGENCY_COLORS = {
    "critical": "Attention",
    "high": "Warning",
    "medium": "Default",
    "low": "Good",
}

CLASSIFICATION_LABELS = {
    "urgent": "URGENT",
    "needs_reply": "Needs Reply",
    "fyi": "FYI",
    "spam": "Spam",
}


def build_triage_card(record: dict[str, Any]) -> dict[str, Any]:
    """Adaptive Card showing the classification result for a triaged email."""
    classification = record.get("classification", "unknown")
    urgency = record.get("urgency", "medium")
    color = URGENCY_COLORS.get(urgency, "Default")
    label = CLASSIFICATION_LABELS.get(classification, classification)

    body: list[dict[str, Any]] = [
        {
            "type": "ColumnSet",
            "columns": [
                {
                    "type": "Column",
                    "width": "auto",
                    "items": [{"type": "TextBlock", "text": label, "weight": "Bolder", "size": "Large", "color": color}],
                },
                {
                    "type": "Column",
                    "width": "stretch",
                    "items": [
                        {"type": "TextBlock", "text": f"Urgency: {urgency}", "isSubtle": True, "horizontalAlignment": "Right"},
                    ],
                },
            ],
        },
        {"type": "TextBlock", "text": record.get("subject", "(no subject)"), "weight": "Bolder", "wrap": True, "spacing": "Medium"},
        {"type": "TextBlock", "text": f"From: {record.get('from_name', '')} <{record.get('from_address', '')}>", "isSubtle": True, "spacing": "None"},
        {"type": "FactSet", "facts": [
            {"title": "Topic", "value": record.get("topic", "N/A")},
            {"title": "Sentiment", "value": record.get("sentiment", "N/A")},
            {"title": "Confidence", "value": f"{record.get('confidence', 0):.0%}"},
        ], "spacing": "Medium"},
        {"type": "TextBlock", "text": record.get("reasoning", ""), "wrap": True, "isSubtle": True, "spacing": "Small"},
    ]

    if record.get("requires_attachment_review"):
        body.append({"type": "TextBlock", "text": "Attachments require manual review", "color": "Warning", "weight": "Bolder", "spacing": "Small"})

    card: dict[str, Any] = {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": body,
    }

    if classification == "needs_reply" and record.get("draft"):
        card["actions"] = [
            {
                "type": "Action.Submit",
                "title": "View Draft Reply",
                "data": {"action": "view_draft", "record_id": record["id"], "mailbox": record["mailbox"]},
            }
        ]

    return card
