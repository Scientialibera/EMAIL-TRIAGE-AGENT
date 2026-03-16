"""Email classification via OpenAI function calling."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from azure.identity import DefaultAzureCredential
from openai import AzureOpenAI

from src.config import get_settings
from src.services.prompt_loader import load_prompt

logger = logging.getLogger(__name__)

CLASSIFY_TOOL = {
    "type": "function",
    "function": {
        "name": "classify_email",
        "description": "Classify an incoming email by urgency, topic, and required action.",
        "parameters": {
            "type": "object",
            "properties": {
                "classification": {
                    "type": "string",
                    "enum": ["urgent", "needs_reply", "fyi", "spam"],
                    "description": "Action category for this email.",
                },
                "urgency": {
                    "type": "string",
                    "enum": ["critical", "high", "medium", "low"],
                    "description": "How quickly this email needs attention.",
                },
                "topic": {
                    "type": "string",
                    "description": "Concise category label for the email subject matter.",
                },
                "sentiment": {
                    "type": "string",
                    "enum": ["positive", "neutral", "negative", "angry"],
                    "description": "Emotional tone of the sender.",
                },
                "requires_attachment_review": {
                    "type": "boolean",
                    "description": "Whether attachments need manual human review.",
                },
                "confidence": {
                    "type": "number",
                    "minimum": 0,
                    "maximum": 1,
                    "description": "Confidence level in this classification (0-1).",
                },
                "reasoning": {
                    "type": "string",
                    "description": "Brief explanation for this classification decision.",
                },
            },
            "required": [
                "classification", "urgency", "topic", "sentiment",
                "requires_attachment_review", "confidence", "reasoning",
            ],
        },
    },
}


@dataclass
class ClassificationResult:
    classification: str
    urgency: str
    topic: str
    sentiment: str
    requires_attachment_review: bool
    confidence: float
    reasoning: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "classification": self.classification,
            "urgency": self.urgency,
            "topic": self.topic,
            "sentiment": self.sentiment,
            "requires_attachment_review": self.requires_attachment_review,
            "confidence": self.confidence,
            "reasoning": self.reasoning,
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


async def classify_email(
    subject: str,
    from_address: str,
    body_text: str,
    attachment_text: str = "",
) -> ClassificationResult:
    """Classify an email using OpenAI function calling."""
    s = get_settings()
    client = _get_client()
    system_prompt = load_prompt("classify.txt")

    user_content = (
        f"From: {from_address}\n"
        f"Subject: {subject}\n\n"
        f"Body:\n{body_text[:4000]}\n"
    )
    if attachment_text:
        user_content += f"\nAttachment text:\n{attachment_text[:2000]}\n"

    completion = client.chat.completions.create(
        model=s.aoai_chat_deployment,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        tools=[CLASSIFY_TOOL],
        tool_choice={"type": "function", "function": {"name": "classify_email"}},
        temperature=0.1,
    )

    tool_call = completion.choices[0].message.tool_calls[0]
    args = json.loads(tool_call.function.arguments)

    return ClassificationResult(
        classification=args["classification"],
        urgency=args["urgency"],
        topic=args["topic"],
        sentiment=args["sentiment"],
        requires_attachment_review=args["requires_attachment_review"],
        confidence=args["confidence"],
        reasoning=args["reasoning"],
    )
