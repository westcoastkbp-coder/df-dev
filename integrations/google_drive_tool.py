from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError

from integrations import google_drive_reader

BINARY_FILE_PLACEHOLDER = "[BINARY FILE - NOT PARSED]"
CACHE_DIR = Path(__file__).resolve().parents[1] / "artifacts" / "tool_cache" / "google_drive"


def _http_error_message(error: HTTPError) -> str:
    try:
        payload = error.read().decode("utf-8", errors="replace")
    except Exception:
        payload = ""

    if payload:
        try:
            message = json.loads(payload)["error"]["message"]
            if str(message).strip():
                return str(message).strip()
        except (KeyError, TypeError, ValueError):
            if payload.strip():
                return payload.strip()

    return str(error.reason or "Google Drive API request failed.").strip()


def _cache_path(file_id: str) -> Path:
    cache_key = sha256(str(file_id).strip().encode("utf-8")).hexdigest()
    return CACHE_DIR / f"{cache_key}.json"


def _read_cached_result(file_id: str) -> dict[str, str] | None:
    cache_path = _cache_path(file_id)
    if not cache_path.is_file():
        return None

    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None

    if not isinstance(payload, dict):
        return None

    content_text = str(payload.get("content_text") or "")
    if not content_text.strip():
        return None

    return {
        "file_id": str(payload.get("file_id") or file_id).strip() or file_id,
        "name": str(payload.get("name") or file_id).strip() or file_id,
        "mime_type": str(payload.get("mime_type") or "").strip(),
        "content_text": content_text,
    }


def _write_cached_result(result: dict[str, str]) -> None:
    file_id = str(result.get("file_id") or "").strip()
    if not file_id:
        return
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _cache_path(file_id).write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _validated_file_id(input_payload: dict[str, Any]) -> str:
    if not isinstance(input_payload, dict):
        raise ValueError("Google Drive tool input must be an object.")

    file_id = str(input_payload.get("file_id") or "").strip()
    if not file_id:
        raise ValueError("Google Drive file_id is required.")
    return file_id


def run_google_drive_read_file_external(input_payload: dict[str, Any]) -> dict[str, str]:
    file_id = _validated_file_id(input_payload)
    if not google_drive_reader._has_required_credentials():
        raise RuntimeError("Google Drive credentials are not configured.")

    access_token = google_drive_reader._access_token()
    metadata = google_drive_reader._fetch_file_metadata(file_id, access_token)
    mime_type = str(metadata.get("mimeType") or "").strip()
    content_bytes = google_drive_reader._fetch_file_content(file_id, mime_type, access_token)

    if content_bytes is None:
        content_text = BINARY_FILE_PLACEHOLDER
    else:
        content_text = google_drive_reader._normalize_text(content_bytes)[
            : google_drive_reader.MAX_CONTENT_CHARS
        ]

    result = {
        "file_id": file_id,
        "name": str(metadata.get("name") or file_id).strip() or file_id,
        "mime_type": mime_type,
        "content_text": content_text,
    }
    if content_text.strip() and content_text != BINARY_FILE_PLACEHOLDER:
        _write_cached_result(result)
    return result


def run_google_drive_read_file_fallback(input_payload: dict[str, Any]) -> dict[str, str]:
    file_id = _validated_file_id(input_payload)
    cached_result = _read_cached_result(file_id)
    if cached_result is None:
        raise RuntimeError(f"Google Drive fallback cache miss for file_id: {file_id}")
    return cached_result


def run_google_drive_read_file(input_payload: dict[str, Any]) -> dict[str, str]:
    file_id = _validated_file_id(input_payload)
    try:
        return run_google_drive_read_file_external({"file_id": file_id})
    except HTTPError as error:
        cached_result = _read_cached_result(file_id)
        if cached_result is not None:
            return cached_result
        raise RuntimeError(_http_error_message(error)) from error
    except (URLError, OSError, TimeoutError, json.JSONDecodeError, ValueError, RuntimeError) as error:
        cached_result = _read_cached_result(file_id)
        if cached_result is not None:
            return cached_result
        raise RuntimeError(str(error) or "Google Drive API request failed.") from error
