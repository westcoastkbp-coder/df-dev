from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

DEFAULT_TRANSFORM_MODE = "plain_summary"
CONTENT_SUMMARY_CHAR_LIMIT = 200


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_generated_at(value: Any) -> str:
    if isinstance(value, datetime):
        normalized = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return normalized.astimezone(timezone.utc).isoformat(timespec="seconds").replace(
            "+00:00",
            "Z",
        )

    text = _normalize_text(value)
    if text:
        return text

    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _normalized_external_files(context_packet: dict[str, Any]) -> list[dict[str, str]]:
    raw_external_files = context_packet.get("external_files")
    if not isinstance(raw_external_files, list):
        return []

    external_files: list[dict[str, str]] = []
    for entry in raw_external_files:
        if not isinstance(entry, dict):
            continue

        file_id = _normalize_text(entry.get("file_id") or entry.get("id"))
        if not file_id:
            continue

        external_files.append(
            {
                "file_id": file_id,
                "name": _normalize_text(entry.get("name")) or file_id,
                "content": str(entry.get("content") or "").replace("\r\n", "\n").strip(),
            }
        )

    return external_files


def build_drive_to_google_doc_content(
    context_packet: dict[str, Any],
    *,
    output_doc_title: str,
    transform_mode: str,
    generated_at: Any | None = None,
) -> dict[str, Any]:
    title = _normalize_text(output_doc_title)
    if not title:
        raise ValueError("drive_to_google_doc requires output_doc_title")

    normalized_mode = _normalize_text(transform_mode) or DEFAULT_TRANSFORM_MODE
    if normalized_mode != DEFAULT_TRANSFORM_MODE:
        raise ValueError(f"unsupported transform_mode: {normalized_mode}")

    external_files = _normalized_external_files(context_packet)
    if not external_files:
        raise ValueError("no external files were loaded from Google Drive context")

    generated_at_text = _normalize_generated_at(generated_at)
    lines = [
        f"Document Title: {title}",
        f"Transform Mode: {normalized_mode}",
        f"Source File Count: {len(external_files)}",
        f"Generated At: {generated_at_text}",
    ]

    for index, external_file in enumerate(external_files, start=1):
        lines.extend(
            [
                "",
                f"Source File {index}: {external_file['name']}",
                f"Source File ID: {external_file['file_id']}",
                "Extracted Content:",
                external_file["content"],
            ]
        )

    content = "\n".join(lines).rstrip() + "\n"
    return {
        "content": content,
        "content_summary": content[:CONTENT_SUMMARY_CHAR_LIMIT],
        "generated_at": generated_at_text,
        "source_file_ids": [external_file["file_id"] for external_file in external_files],
        "source_file_names": [external_file["name"] for external_file in external_files],
        "transform_mode": normalized_mode,
    }
