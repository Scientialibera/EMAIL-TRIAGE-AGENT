from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from src.config import get_settings

logger = logging.getLogger(__name__)

BLOB_NAME = "mailbox_config.json"


@dataclass
class MailboxRules:
    skip_senders: list[str] = field(default_factory=list)
    always_urgent_senders: list[str] = field(default_factory=list)


@dataclass
class MailboxDefinition:
    mailbox: str
    display_name: str
    notify_user_upns: list[str] = field(default_factory=list)
    auto_draft: bool = True
    rules: MailboxRules = field(default_factory=MailboxRules)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> MailboxDefinition:
        rules_raw = d.get("rules", {})
        rules = MailboxRules(
            skip_senders=[s.lower() for s in rules_raw.get("skip_senders", [])],
            always_urgent_senders=[s.lower() for s in rules_raw.get("always_urgent_senders", [])],
        )
        return cls(
            mailbox=d["mailbox"],
            display_name=d.get("display_name", d["mailbox"]),
            notify_user_upns=d.get("notify_user_upns", []),
            auto_draft=d.get("auto_draft", True),
            rules=rules,
        )


_cached: list[MailboxDefinition] | None = None


def _get_blob_client():
    s = get_settings()
    credential = DefaultAzureCredential()
    service = BlobServiceClient(account_url=s.blob_account_url, credential=credential)
    return service.get_blob_client(container=s.blob_config_container, blob=BLOB_NAME)


async def load_mailbox_config(force_refresh: bool = False) -> list[MailboxDefinition]:
    global _cached
    if _cached is not None and not force_refresh:
        return _cached

    blob_client = _get_blob_client()
    download = blob_client.download_blob()
    raw = download.readall().decode("utf-8")
    data = json.loads(raw)
    _cached = [MailboxDefinition.from_dict(m) for m in data]
    logger.info("Loaded %d mailbox definitions from blob.", len(_cached))
    return _cached


def get_mailbox_by_address(address: str) -> MailboxDefinition | None:
    if _cached is None:
        return None
    for mb in _cached:
        if mb.mailbox.lower() == address.lower():
            return mb
    return None


def invalidate_cache() -> None:
    global _cached
    _cached = None
