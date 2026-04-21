from __future__ import annotations

from typing import Any

from integrations.google_docs_writer import create_google_doc


def run_google_docs_create_document(input_payload: dict[str, Any]) -> dict[str, str]:
    if not isinstance(input_payload, dict):
        raise ValueError("Google Docs tool input must be an object.")

    result = create_google_doc(
        {
            "title": str(input_payload.get("title") or ""),
            "content": str(input_payload.get("content") or ""),
        }
    )
    return {
        "doc_id": str(result["doc_id"]),
        "url": str(result["url"]),
    }


def run_google_docs_create_document_external(
    input_payload: dict[str, Any],
) -> dict[str, str]:
    return run_google_docs_create_document(input_payload)


def run_google_docs_create(input_payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(input_payload, dict):
        raise ValueError("Google Docs tool input must be an object.")

    result = create_google_doc(
        {
            "title": str(input_payload.get("title") or ""),
            "content": str(input_payload.get("content") or ""),
        }
    )
    return {
        "result": {
            "doc_url": str(result["url"]),
        },
        "execution_trace": {
            "tool_source": "external",
            "model": "google_api",
        },
    }


def run_google_docs_create_external(input_payload: dict[str, Any]) -> dict[str, Any]:
    return run_google_docs_create(input_payload)
