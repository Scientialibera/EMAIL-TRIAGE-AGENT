"""Load prompts from blob storage with local file fallback."""

from __future__ import annotations

import logging
import os
from pathlib import Path

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from src.config import get_settings

logger = logging.getLogger(__name__)

_cache: dict[str, str] = {}


def _load_from_blob(blob_name: str) -> str | None:
    s = get_settings()
    if not s.storage_account_name or not s.blob_prompts_container:
        return None
    try:
        credential = DefaultAzureCredential()
        service = BlobServiceClient(account_url=s.blob_account_url, credential=credential)
        client = service.get_blob_client(container=s.blob_prompts_container, blob=blob_name)
        return client.download_blob().readall().decode("utf-8")
    except Exception:
        logger.debug("Could not load prompt '%s' from blob, falling back to local.", blob_name, exc_info=True)
        return None


def _load_from_local(blob_name: str) -> str | None:
    local_path = Path(__file__).resolve().parent.parent.parent / "deploy" / "assets" / "prompts" / blob_name
    if local_path.exists():
        return local_path.read_text(encoding="utf-8")
    return None


def load_prompt(name: str) -> str:
    """Load a prompt by filename (e.g. 'classify.txt'). Blob-first, local fallback."""
    if name in _cache:
        return _cache[name]

    content = _load_from_blob(name)
    if content is None:
        content = _load_from_local(name)
    if content is None:
        raise FileNotFoundError(f"Prompt '{name}' not found in blob or locally.")

    _cache[name] = content
    logger.info("Loaded prompt '%s' (%d chars).", name, len(content))
    return content


def invalidate_prompt_cache() -> None:
    _cache.clear()
