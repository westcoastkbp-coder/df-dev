from __future__ import annotations

import json
import os
from pathlib import Path
from time import sleep
from time import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from control.env_loader import load_env

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_DRIVE_FILES_URL = "https://www.googleapis.com/drive/v3/files"
GOOGLE_DOC_MIME_TYPE = "application/vnd.google-apps.document"
MAX_CONTENT_CHARS = 10_000
REQUEST_TIMEOUT_SECONDS = 15
REQUEST_RETRY_DELAYS_SECONDS = (1.0, 2.0, 4.0, 8.0)
REQUIRED_ENV_VARS = (
    "GOOGLE_CLIENT_ID",
    "GOOGLE_CLIENT_SECRET",
    "GOOGLE_REFRESH_TOKEN",
)
TEXT_MIME_TYPES = {
    "application/javascript",
    "application/json",
    "application/ld+json",
    "application/rtf",
    "application/x-httpd-php",
    "application/x-python-code",
    "application/xhtml+xml",
    "application/xml",
}
TEXT_MIME_PREFIXES = ("text/",)
TOKEN_CACHE_PATH = (
    Path(__file__).resolve().parents[1]
    / "artifacts"
    / "tool_cache"
    / "google_oauth_token.json"
)


def _env_value(name: str) -> str:
    value = str(os.environ.get(name) or "").strip()
    if value:
        return value
    return str(load_env().get(name) or "").strip()


def _has_required_credentials() -> bool:
    return all(_env_value(name) for name in REQUIRED_ENV_VARS)


def _read_cached_access_token() -> str:
    if not TOKEN_CACHE_PATH.is_file():
        return ""

    try:
        payload = json.loads(TOKEN_CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""

    if not isinstance(payload, dict):
        return ""

    access_token = str(payload.get("access_token") or "").strip()
    expires_at = float(payload.get("expires_at") or 0)
    if not access_token:
        return ""
    if expires_at and expires_at > (time() + 60):
        return access_token
    return ""


def _write_cached_access_token(access_token: str, expires_in: Any) -> None:
    normalized_token = str(access_token or "").strip()
    if not normalized_token:
        return

    try:
        expires_in_seconds = max(0, int(expires_in))
    except (TypeError, ValueError):
        expires_in_seconds = 3000

    TOKEN_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_CACHE_PATH.write_text(
        json.dumps(
            {
                "access_token": normalized_token,
                "expires_at": int(time()) + expires_in_seconds,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _request_bytes(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    data: bytes | None = None,
) -> bytes:
    request = Request(url, headers=headers or {}, data=data)
    with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
        return response.read()


def _should_retry_http_error(error: HTTPError) -> bool:
    return int(getattr(error, "code", 0) or 0) in {408, 429, 500, 502, 503, 504}


def _request_bytes_with_retry(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    data: bytes | None = None,
) -> bytes:
    for attempt_index, delay_seconds in enumerate(
        (0.0, *REQUEST_RETRY_DELAYS_SECONDS), start=1
    ):
        try:
            return _request_bytes(url, headers=headers, data=data)
        except HTTPError as error:
            if error is None or not _should_retry_http_error(error):
                raise
            if attempt_index > len(REQUEST_RETRY_DELAYS_SECONDS):
                raise
        except (URLError, OSError, TimeoutError):
            if attempt_index > len(REQUEST_RETRY_DELAYS_SECONDS):
                raise

        sleep(delay_seconds)

    raise RuntimeError("Google Drive request retries exhausted.")


def _request_json(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    data: bytes | None = None,
) -> dict[str, Any]:
    payload = _request_bytes_with_retry(url, headers=headers, data=data)
    loaded = json.loads(payload.decode("utf-8"))
    return dict(loaded)


def _access_token() -> str:
    cached_token = _read_cached_access_token()
    if cached_token:
        return cached_token

    payload = urlencode(
        {
            "client_id": _env_value("GOOGLE_CLIENT_ID"),
            "client_secret": _env_value("GOOGLE_CLIENT_SECRET"),
            "refresh_token": _env_value("GOOGLE_REFRESH_TOKEN"),
            "grant_type": "refresh_token",
        }
    ).encode("utf-8")
    try:
        response = _request_json(
            GOOGLE_TOKEN_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=payload,
        )
    except (HTTPError, URLError, OSError, TimeoutError, json.JSONDecodeError):
        cached_token = _read_cached_access_token()
        if cached_token:
            return cached_token
        raise
    token = str(response.get("access_token") or "").strip()
    if not token:
        raise ValueError("Google access token exchange returned no access_token.")
    _write_cached_access_token(token, response.get("expires_in"))
    return token


def _fetch_file_metadata(file_id: str, access_token: str) -> dict[str, Any]:
    query = urlencode({"fields": "id,name,mimeType,size"})
    encoded_file_id = quote(file_id, safe="")
    return _request_json(
        f"{GOOGLE_DRIVE_FILES_URL}/{encoded_file_id}?{query}",
        headers={"Authorization": f"Bearer {access_token}"},
    )


def _is_supported_text_mime_type(mime_type: str) -> bool:
    normalized = str(mime_type or "").strip().lower()
    if not normalized:
        return False
    if normalized.startswith(TEXT_MIME_PREFIXES):
        return True
    return normalized in TEXT_MIME_TYPES


def _fetch_file_content(
    file_id: str, mime_type: str, access_token: str
) -> bytes | None:
    encoded_file_id = quote(file_id, safe="")
    headers = {"Authorization": f"Bearer {access_token}"}

    if mime_type == GOOGLE_DOC_MIME_TYPE:
        query = urlencode({"mimeType": "text/plain"})
        return _request_bytes_with_retry(
            f"{GOOGLE_DRIVE_FILES_URL}/{encoded_file_id}/export?{query}",
            headers=headers,
        )

    if not _is_supported_text_mime_type(mime_type):
        return None

    query = urlencode({"alt": "media"})
    return _request_bytes_with_retry(
        f"{GOOGLE_DRIVE_FILES_URL}/{encoded_file_id}?{query}",
        headers=headers,
    )


def _normalize_text(content_bytes: bytes) -> str:
    return content_bytes.decode("utf-8", errors="replace").replace("\r\n", "\n")


def _content_size(metadata: dict[str, Any], content_bytes: bytes) -> int:
    try:
        return int(metadata["size"])
    except (KeyError, TypeError, ValueError):
        return len(content_bytes)


def read_google_drive_file(payload: dict[str, Any]) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None

    file_id = str(payload.get("drive_file_id") or "").strip()
    if not file_id or not _has_required_credentials():
        return None

    try:
        access_token = _access_token()
        metadata = _fetch_file_metadata(file_id, access_token)
        content_bytes = _fetch_file_content(
            file_id,
            str(metadata.get("mimeType") or ""),
            access_token,
        )
        if content_bytes is None:
            return None
    except (
        HTTPError,
        URLError,
        OSError,
        TimeoutError,
        ValueError,
        json.JSONDecodeError,
    ):
        return None

    content = _normalize_text(content_bytes)
    return {
        "file_id": file_id,
        "name": str(metadata.get("name") or file_id),
        "content": content[:MAX_CONTENT_CHARS],
        "size": _content_size(metadata, content_bytes),
    }
