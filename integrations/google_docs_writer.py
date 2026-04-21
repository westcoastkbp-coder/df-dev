from __future__ import annotations

import json
import os
from uuid import uuid4
from pathlib import Path
from time import sleep
from time import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from control.env_loader import load_env

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GOOGLE_DOCS_API_URL = "https://docs.googleapis.com/v1/documents"
GOOGLE_DRIVE_UPLOAD_URL = "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,name"
MAX_CONTENT_CHARS = 10_000
REQUEST_TIMEOUT_SECONDS = 15
REQUEST_RETRY_DELAYS_SECONDS = (1.0, 2.0, 4.0, 8.0)
REQUIRED_ENV_VARS = (
    "GOOGLE_CLIENT_ID",
    "GOOGLE_CLIENT_SECRET",
    "GOOGLE_REFRESH_TOKEN",
)
TOKEN_CACHE_PATH = Path(__file__).resolve().parents[1] / "artifacts" / "tool_cache" / "google_oauth_token.json"


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


def _request_json(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    data: bytes | None = None,
) -> dict[str, Any]:
    request = Request(url, headers=headers or {}, data=data)
    with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
        payload = response.read()
    loaded = json.loads(payload.decode("utf-8"))
    return dict(loaded)


def _should_retry_http_error(error: HTTPError) -> bool:
    return int(getattr(error, "code", 0) or 0) in {408, 429, 500, 502, 503, 504}


def _request_json_with_retry(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    data: bytes | None = None,
) -> dict[str, Any]:
    for attempt_index, delay_seconds in enumerate((0.0, *REQUEST_RETRY_DELAYS_SECONDS), start=1):
        try:
            return _request_json(url, headers=headers, data=data)
        except HTTPError as error:
            if not _should_retry_http_error(error):
                raise
            if attempt_index > len(REQUEST_RETRY_DELAYS_SECONDS):
                raise
        except (URLError, OSError, TimeoutError):
            if attempt_index > len(REQUEST_RETRY_DELAYS_SECONDS):
                raise

        sleep(delay_seconds)

    raise RuntimeError("Google Docs API request retries exhausted.")


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

    return str(error.reason or "Google Docs API request failed.").strip()


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
        response = _request_json_with_retry(
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
        raise RuntimeError("Google access token exchange returned no access_token.")
    _write_cached_access_token(token, response.get("expires_in"))
    return token


def _authorized_headers(access_token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }


def _create_document(title: str, access_token: str) -> dict[str, str]:
    response = _request_json_with_retry(
        GOOGLE_DOCS_API_URL,
        headers=_authorized_headers(access_token),
        data=json.dumps({"title": title}).encode("utf-8"),
    )
    doc_id = str(response.get("documentId") or "").strip()
    if not doc_id:
        raise RuntimeError("Google Docs API create response missing documentId.")
    return {
        "doc_id": doc_id,
        "name": str(response.get("title") or title).strip() or title,
    }


def _write_document_content(doc_id: str, content: str, access_token: str) -> None:
    _request_json_with_retry(
        f"{GOOGLE_DOCS_API_URL}/{quote(doc_id, safe='')}:batchUpdate",
        headers=_authorized_headers(access_token),
        data=json.dumps(
            {
                "requests": [
                    {
                        "insertText": {
                            "text": content,
                            "endOfSegmentLocation": {},
                        }
                    }
                ]
            }
        ).encode("utf-8"),
    )


def _create_document_via_drive_import(
    title: str,
    content: str,
    access_token: str,
) -> dict[str, str]:
    boundary = f"df-boundary-{uuid4().hex}"
    metadata = json.dumps(
        {
            "name": title,
            "mimeType": "application/vnd.google-apps.document",
        }
    )
    body = (
        f"--{boundary}\r\n"
        "Content-Type: application/json; charset=UTF-8\r\n\r\n"
        f"{metadata}\r\n"
        f"--{boundary}\r\n"
        "Content-Type: text/plain; charset=UTF-8\r\n\r\n"
        f"{content}\r\n"
        f"--{boundary}--\r\n"
    ).encode("utf-8")
    response = _request_json_with_retry(
        GOOGLE_DRIVE_UPLOAD_URL,
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": f"multipart/related; boundary={boundary}",
        },
        data=body,
    )
    doc_id = str(response.get("id") or "").strip()
    if not doc_id:
        raise RuntimeError("Google Drive import response missing file id.")
    return {
        "doc_id": doc_id,
        "name": str(response.get("name") or title).strip() or title,
    }


def create_google_doc(payload: dict[str, Any]) -> dict[str, str]:
    if not isinstance(payload, dict):
        raise ValueError("Google Docs payload must be an object.")

    title = str(payload.get("title") or "").strip()
    content = str(payload.get("content") or "")[:MAX_CONTENT_CHARS]

    if not title:
        raise ValueError("Google Doc title is required.")
    if not content.strip():
        raise ValueError("Google Doc content is required.")
    if not _has_required_credentials():
        raise RuntimeError("Google Docs credentials are not configured.")

    try:
        access_token = _access_token()
        try:
            document = _create_document(title, access_token)
            _write_document_content(document["doc_id"], content, access_token)
        except (HTTPError, URLError, OSError, TimeoutError, json.JSONDecodeError):
            document = _create_document_via_drive_import(title, content, access_token)
    except HTTPError as error:
        raise RuntimeError(_http_error_message(error)) from error
    except (URLError, OSError, TimeoutError, json.JSONDecodeError) as error:
        raise RuntimeError(str(error) or "Google Docs API request failed.") from error

    return {
        "doc_id": document["doc_id"],
        "name": document["name"],
        "url": f"https://docs.google.com/document/d/{document['doc_id']}",
    }
