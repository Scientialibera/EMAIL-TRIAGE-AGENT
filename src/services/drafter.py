"""Draft reply generation via OpenAI function calling."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from azure.identity import DefaultAzureCredential
from openai import AzureOpenAI

from src.config import get_settings
from src.services.prompt_loader import load_prompt

logger = logging.getLogger(__name__)

DRAFT_TOOL = {
    "type": "function",
    "function": {
        "name": "draft_reply",
        "description": "Draft a reply to an email that needs a response.",
        "parameters": {
            "type": "object",
            "properties": {
                "subject": {
                    "type": "string",
                    "description": "Reply subject line.",
                },
                "body": {
                    "type": "string",
                    "description": "The draft reply body in plain text.",
                },
                "tone": {
                    "type": "string",
                    "enum": ["formal", "friendly", "concise"],
                    "description": "Tone of the reply.",
                },
                "key_points_addressed": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Key points from the original email addressed in this reply.",
                },
            },
            "required": ["subject", "body", "tone", "key_points_addressed"],
        },
    },
}


@dataclass
class DraftResult:
    subject: str
    body: str
    tone: str
    key_points_addressed: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "subject": self.subject,
            "body": self.body,
            "tone": self.tone,
            "key_points_addressed": self.key_points_addressed,
        }


_client: AzureOpenAI | None = None


def _token_provider() -> str:
    credential = DefaultAzureCredential()
    return credential.get_token("https://cognitiveservices.azure.com/.default").token


def _get_client() -> AzureOpenAI:
    global _client
    if _client is None:
        s = get_settings()
        _client = AzureOpenAI(
            azure_endpoint=s.aoai_endpoint,
            azure_ad_token_provider=_token_provider,
            api_version=s.aoai_api_version,
        )
    return _client


async def draft_reply(
    subject: str,
    from_address: str,
    body_text: str,
    classification_topic: str = "",
) -> DraftResult:
    """Generate a draft reply using OpenAI function calling."""
    s = get_settings()
    client = _get_client()
    system_prompt = load_prompt("draft_reply.txt")

    user_content = (
        f"Original email:\n"
        f"From: {from_address}\n"
        f"Subject: {subject}\n"
        f"Topic: {classification_topic}\n\n"
        f"Body:\n{body_text[:4000]}\n"
    )

    completion = client.chat.completions.create(
        model=s.aoai_chat_deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        tools=[DRAFT_TOOL],
        tool_choice={"type": "function", "function": {"name": "draft_reply"}},
        temperature=0.4,
    )

    tool_call = completion.choices[0].message.tool_calls[0]
    args = json.loads(tool_call.function.arguments)

    return DraftResult(
        subject=args["subject"],
        body=args["body"],
        tone=args["tone"],
        key_points_addressed=args.get("key_points_addressed", []),
    )
