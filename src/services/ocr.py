"""Document Intelligence OCR for email attachments."""

from __future__ import annotations

import io
import logging

from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.documentintelligence.models import AnalyzeDocumentRequest
from azure.identity import DefaultAzureCredential

from src.config import get_settings

logger = logging.getLogger(__name__)

_client: DocumentIntelligenceClient | None = None

SUPPORTED_CONTENT_TYPES = {
    "application/pdf",
    "image/jpeg",
    "image/png",
    "image/tiff",
    "image/bmp",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation",
}


def _get_client() -> DocumentIntelligenceClient:
    global _client
    if _client is None:
        s = get_settings()
        if not s.doc_intel_endpoint:
            raise RuntimeError("DOC_INTEL_ENDPOINT not configured.")
        credential = DefaultAzureCredential()
        _client = DocumentIntelligenceClient(
            endpoint=s.doc_intel_endpoint, credential=credential
        )
    return _client


async def extract_text_from_attachment(content: bytes, content_type: str) -> str:
    """OCR an attachment using Document Intelligence prebuilt-read model."""
    if content_type not in SUPPORTED_CONTENT_TYPES:
        logger.debug("Skipping unsupported content type: %s", content_type)
        return ""

    if not content:
        return ""

    try:
        client = _get_client()
        poller = client.begin_analyze_document(
            model_id="prebuilt-read",
            analyze_request=AnalyzeDocumentRequest(bytes_source=content),
            content_type="application/octet-stream",
        )
        result = poller.result()
        pages_text = []
        for page in result.pages:
            for line in page.lines:
                pages_text.append(line.content)
        extracted = "\n".join(pages_text)
        logger.info("OCR extracted %d chars from attachment (%s).", len(extracted), content_type)
        return extracted
    except Exception:
        logger.warning("OCR failed for attachment (%s).", content_type, exc_info=True)
        return ""
