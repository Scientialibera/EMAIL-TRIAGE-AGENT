from __future__ import annotations

from typing import Any


def build_draft_card(record: dict[str, Any]) -> dict[str, Any]:
    """Adaptive Card for reviewing a draft reply (Approve / Edit / Reject)."""
    draft = record.get("draft", {})
    key_points = draft.get("key_points_addressed", [])

    body: list[dict[str, Any]] = [
        {"type": "TextBlock", "text": "Draft Reply for Review", "weight": "Bolder", "size": "Large"},
        {"type": "TextBlock", "text": f"Re: {record.get('subject', '(no subject)')}", "isSubtle": True, "spacing": "None"},
        {"type": "TextBlock", "text": f"To: {record.get('from_address', '')}", "isSubtle": True, "spacing": "None"},
        {"type": "FactSet", "facts": [
            {"title": "Tone", "value": draft.get("tone", "N/A")},
            {"title": "Topic", "value": record.get("topic", "N/A")},
        ], "spacing": "Medium"},
    ]

    if key_points:
        body.append({"type": "TextBlock", "text": "Key Points Addressed", "weight": "Bolder", "spacing": "Medium"})
        for kp in key_points:
            body.append({"type": "TextBlock", "text": f"- {kp}", "wrap": True, "spacing": "None"})

    body.append({"type": "TextBlock", "text": "Draft", "weight": "Bolder", "spacing": "Medium"})
    body.append({"type": "TextBlock", "text": draft.get("body", "(empty)"), "wrap": True, "spacing": "Small"})

    body.append({
        "type": "Input.Text",
        "id": "edited_body",
        "placeholder": "Edit the reply here (only used if you click 'Edit & Send')...",
        "isMultiline": True,
        "maxLength": 5000,
        "spacing": "Medium",
    })

    return {
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": body,
        "actions": [
            {
                "type": "Action.Submit",
                "title": "Approve & Send",
                "data": {"action": "approve_draft", "record_id": record["id"], "mailbox": record["mailbox"]},
            },
            {
                "type": "Action.Submit",
                "title": "Edit & Send",
                "data": {"action": "edit_draft", "record_id": record["id"], "mailbox": record["mailbox"]},
            },
            {
                "type": "Action.Submit",
                "title": "Reject",
                "style": "destructive",
                "data": {"action": "reject_draft", "record_id": record["id"], "mailbox": record["mailbox"]},
            },
        ],
    }
