from __future__ import annotations

import base64
import html
import json
import os
import re
import time
from email.message import EmailMessage
from email.utils import parseaddr
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from app.execution.paths import OUTPUT_DIR
from control.env_loader import load_env
from integrations import google_drive_reader

GMAIL_API_BASE_URL = "https://gmail.googleapis.com/gmail/v1/users/me"
REQUEST_TIMEOUT_SECONDS = 15
CACHE_DIR = Path(__file__).resolve().parents[1] / "artifacts" / "tool_cache" / "gmail"
LATEST_EMAIL_CACHE_PATH = CACHE_DIR / "latest_email.json"
GMAIL_TOKEN_CACHE_PATH = CACHE_DIR / "google_oauth_token.json"
MOCK_GMAIL_ROOT = OUTPUT_DIR / "external_business" / "gmail_gateway"
MOCK_INBOX_DIR = MOCK_GMAIL_ROOT / "inbox"
MOCK_DRAFTS_DIR = MOCK_GMAIL_ROOT / "drafts"
SAFE_HEADER_PATTERN = re.compile(r"[\r\n]")
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
REPLY_MARKERS = ("reply draft:", "suggested reply:", "reply:")
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
GMAIL_CREDENTIAL_ENV_MAP = {
    "client_id": "GMAIL_EXECUTION_CLIENT_ID",
    "client_secret": "GMAIL_EXECUTION_CLIENT_SECRET",
    "refresh_token": "GMAIL_EXECUTION_REFRESH_TOKEN",
}


class GmailToolError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = str(code)
        self.message = str(message)


def _env_value(name: str, default: str = "") -> str:
    return str(os.environ.get(name) or default).strip()


def _loaded_env_value(name: str, default: str = "") -> str:
    env_value = _env_value(name, default)
    if env_value:
        return env_value
    return str(load_env().get(name) or default).strip()


def _gmail_credentials_configured() -> bool:
    return all(
        _loaded_env_value(env_name) for env_name in GMAIL_CREDENTIAL_ENV_MAP.values()
    )


def _gmail_access_token_cache_path() -> Path:
    return GMAIL_TOKEN_CACHE_PATH


def _read_cached_gmail_access_token() -> str:
    cache_path = _gmail_access_token_cache_path()
    if not cache_path.is_file():
        return ""

    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""

    if not isinstance(payload, dict):
        return ""

    access_token = str(payload.get("access_token") or "").strip()
    expires_at = float(payload.get("expires_at") or 0)
    if not access_token:
        return ""
    if expires_at and expires_at > (time.time() + 60):
        return access_token
    return ""


def _write_cached_gmail_access_token(access_token: str, expires_in: Any) -> None:
    normalized_token = str(access_token or "").strip()
    if not normalized_token:
        return

    try:
        expires_in_seconds = max(0, int(expires_in))
    except (TypeError, ValueError):
        expires_in_seconds = 3000

    cache_path = _gmail_access_token_cache_path()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(
            {
                "access_token": normalized_token,
                "expires_at": int(time.time()) + expires_in_seconds,
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _gmail_access_token() -> str:
    if not _gmail_credentials_configured():
        return google_drive_reader._access_token()

    cached_token = _read_cached_gmail_access_token()
    if cached_token:
        return cached_token

    payload = urlencode(
        {
            "client_id": _loaded_env_value(GMAIL_CREDENTIAL_ENV_MAP["client_id"]),
            "client_secret": _loaded_env_value(
                GMAIL_CREDENTIAL_ENV_MAP["client_secret"]
            ),
            "refresh_token": _loaded_env_value(
                GMAIL_CREDENTIAL_ENV_MAP["refresh_token"]
            ),
            "grant_type": "refresh_token",
        }
    ).encode("utf-8")
    try:
        request = Request(
            GOOGLE_TOKEN_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=payload,
            method="POST",
        )
        with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as token_response:
            response = dict(json.loads(token_response.read().decode("utf-8")))
    except HTTPError as error:
        raise GmailToolError("GMAIL_API_FAILED", _http_error_message(error)) from error
    except (URLError, OSError, TimeoutError, ValueError, json.JSONDecodeError) as error:
        raise GmailToolError(
            "GMAIL_API_FAILED",
            str(error).strip() or "Gmail token exchange failed.",
        ) from error

    access_token = str(response.get("access_token") or "").strip()
    if not access_token:
        raise GmailToolError(
            "GMAIL_API_FAILED", "Gmail token exchange returned no access token."
        )
    _write_cached_gmail_access_token(access_token, response.get("expires_in"))
    return access_token


def _gmail_credentials_available() -> bool:
    if _gmail_credentials_configured():
        return True
    return google_drive_reader._has_required_credentials()


def gmail_execution_sender() -> str:
    return _loaded_env_value("GMAIL_EXECUTION_SENDER_EMAIL")


def _requested_mode() -> str:
    normalized = _env_value("DIGITAL_FOREMAN_GMAIL_TOOL_MODE", "auto").lower()
    if normalized in {"mock", "real"}:
        return normalized
    return "auto"


def _timestamp() -> str:
    return time.strftime("%Y%m%d%H%M%S")


def _slugify(value: str) -> str:
    cleaned = "".join(
        character.lower() if character.isalnum() else "-"
        for character in str(value or "").strip()
    )
    collapsed = "-".join(part for part in cleaned.split("-") if part)
    return collapsed or "draft"


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

    return str(error.reason or "Gmail API request failed.").strip()


def _request_json(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body = None
    normalized_headers = dict(headers or {})
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        normalized_headers.setdefault("Content-Type", "application/json")

    request = Request(url, headers=normalized_headers, data=body, method=method)
    with urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
        return dict(json.loads(response.read().decode("utf-8")))


def _gmail_request_json(
    path: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    access_token = _gmail_access_token()
    try:
        return _request_json(
            f"{GMAIL_API_BASE_URL}{path}",
            method=method,
            headers={"Authorization": f"Bearer {access_token}"},
            payload=payload,
        )
    except HTTPError as error:
        raise GmailToolError("GMAIL_API_FAILED", _http_error_message(error)) from error
    except (URLError, OSError, TimeoutError, ValueError, json.JSONDecodeError) as error:
        raise GmailToolError(
            "GMAIL_API_FAILED",
            str(error).strip() or "Gmail API request failed.",
        ) from error


def _header_value(headers: list[dict[str, Any]] | None, name: str) -> str:
    if not isinstance(headers, list):
        return ""
    normalized_name = str(name or "").strip().lower()
    for header in headers:
        if not isinstance(header, dict):
            continue
        if str(header.get("name") or "").strip().lower() != normalized_name:
            continue
        return str(header.get("value") or "").strip()
    return ""


def _decode_body_data(encoded_body: str) -> str:
    normalized = str(encoded_body or "").strip()
    if not normalized:
        return ""
    padding = "=" * ((4 - len(normalized) % 4) % 4)
    decoded = base64.urlsafe_b64decode(f"{normalized}{padding}".encode("ascii"))
    return decoded.decode("utf-8", errors="replace").replace("\r\n", "\n").strip()


def _html_to_text(html_body: str) -> str:
    without_tags = HTML_TAG_PATTERN.sub(" ", str(html_body or ""))
    return re.sub(r"\s+", " ", html.unescape(without_tags)).strip()


def _payload_body_text(payload: dict[str, Any]) -> str:
    mime_type = str(payload.get("mimeType") or "").strip().lower()
    body_payload = payload.get("body")
    if isinstance(body_payload, dict):
        data = _decode_body_data(str(body_payload.get("data") or ""))
        if data:
            if mime_type == "text/html":
                return _html_to_text(data)
            return data

    parts = payload.get("parts")
    if isinstance(parts, list):
        html_candidate = ""
        for part in parts:
            if not isinstance(part, dict):
                continue
            text = _payload_body_text(part)
            if not text:
                continue
            if str(part.get("mimeType") or "").strip().lower() == "text/plain":
                return text
            if not html_candidate:
                html_candidate = text
        if html_candidate:
            return html_candidate

    return ""


def _ensure_non_empty_email(
    subject: str, sender: str, body_text: str
) -> tuple[str, str, str]:
    normalized_sender = str(sender or "").strip()
    normalized_subject = str(subject or "").strip() or "(no subject)"
    normalized_body = str(body_text or "").strip()

    if not normalized_sender:
        raise GmailToolError("GMAIL_READ_FAILED", "Latest email sender is empty.")
    if not normalized_body:
        raise GmailToolError("GMAIL_READ_FAILED", "Latest email body is empty.")
    return normalized_subject, normalized_sender, normalized_body


def _write_latest_email_cache(payload: dict[str, Any]) -> None:
    LATEST_EMAIL_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    LATEST_EMAIL_CACHE_PATH.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _read_latest_email_cache() -> dict[str, Any]:
    if not LATEST_EMAIL_CACHE_PATH.is_file():
        raise GmailToolError(
            "GMAIL_DRAFT_FAILED",
            "No email context is available. Run gmail.read_latest first.",
        )

    try:
        payload = json.loads(LATEST_EMAIL_CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise GmailToolError(
            "GMAIL_DRAFT_FAILED",
            "Latest email context cache is unreadable.",
        ) from error

    if not isinstance(payload, dict):
        raise GmailToolError(
            "GMAIL_DRAFT_FAILED",
            "Latest email context cache is invalid.",
        )
    return payload


def _latest_mock_email_path() -> Path:
    candidates = sorted(
        (path for path in MOCK_INBOX_DIR.glob("*.json") if path.is_file()),
        key=lambda path: (path.stat().st_mtime, path.name.lower()),
        reverse=True,
    )
    if not candidates:
        raise GmailToolError("GMAIL_READ_FAILED", "Mock Gmail inbox is empty.")
    return candidates[0]


def _mock_read_latest() -> dict[str, Any]:
    path = _latest_mock_email_path()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise GmailToolError(
            "GMAIL_READ_FAILED", "Mock Gmail inbox entry is invalid."
        ) from error

    subject, sender, body_text = _ensure_non_empty_email(
        str(payload.get("subject") or ""),
        str(payload.get("sender") or ""),
        str(payload.get("body_text") or ""),
    )
    reply_to_email = parseaddr(str(payload.get("reply_to") or sender))[1].strip()
    if "@" not in reply_to_email:
        raise GmailToolError(
            "GMAIL_READ_FAILED", "Mock Gmail sender address is invalid."
        )

    cached_payload = {
        "message_id": str(payload.get("message_id") or path.stem).strip() or path.stem,
        "thread_id": str(payload.get("thread_id") or path.stem).strip() or path.stem,
        "internet_message_id": str(payload.get("internet_message_id") or "").strip(),
        "references": str(payload.get("references") or "").strip(),
        "reply_to_email": reply_to_email,
        "sender": sender,
        "subject": subject,
        "body_text": body_text,
        "mode": "mock",
    }
    _write_latest_email_cache(cached_payload)
    return {
        "subject": subject,
        "sender": sender,
        "body_text": body_text,
        "message_id": cached_payload["message_id"],
        "thread_id": cached_payload["thread_id"],
        "reply_to_email": reply_to_email,
        "mode": "mock",
    }


def _real_read_latest() -> dict[str, Any]:
    query = urlencode({"labelIds": "INBOX", "maxResults": 1})
    list_payload = _gmail_request_json(f"/messages?{query}")
    messages = list_payload.get("messages")
    if not isinstance(messages, list) or not messages:
        raise GmailToolError("GMAIL_READ_FAILED", "Inbox is empty.")

    latest_message = messages[0]
    if not isinstance(latest_message, dict):
        raise GmailToolError(
            "GMAIL_READ_FAILED", "Latest inbox message metadata is invalid."
        )

    message_id = str(latest_message.get("id") or "").strip()
    if not message_id:
        raise GmailToolError("GMAIL_READ_FAILED", "Latest inbox message id is missing.")

    encoded_message_id = quote(message_id, safe="")
    detail_payload = _gmail_request_json(f"/messages/{encoded_message_id}?format=full")
    payload = detail_payload.get("payload")
    if not isinstance(payload, dict):
        raise GmailToolError("GMAIL_READ_FAILED", "Latest email payload is invalid.")

    headers = payload.get("headers")
    sender = _header_value(headers, "Reply-To") or _header_value(headers, "From")
    subject = _header_value(headers, "Subject")
    body_text = (
        _payload_body_text(payload) or str(detail_payload.get("snippet") or "").strip()
    )
    subject, sender, body_text = _ensure_non_empty_email(subject, sender, body_text)

    reply_to_email = parseaddr(sender)[1].strip()
    if "@" not in reply_to_email:
        raise GmailToolError(
            "GMAIL_READ_FAILED", "Latest email sender address is invalid."
        )

    cached_payload = {
        "message_id": message_id,
        "thread_id": str(detail_payload.get("threadId") or "").strip(),
        "internet_message_id": _header_value(headers, "Message-ID"),
        "references": _header_value(headers, "References"),
        "reply_to_email": reply_to_email,
        "sender": _header_value(headers, "From") or sender,
        "subject": subject,
        "body_text": body_text,
        "mode": "real",
    }
    _write_latest_email_cache(cached_payload)
    return {
        "subject": subject,
        "sender": cached_payload["sender"],
        "body_text": body_text,
        "message_id": message_id,
        "thread_id": cached_payload["thread_id"],
        "reply_to_email": reply_to_email,
        "mode": "real",
    }


def _safe_header_value(
    field_name: str, value: str, *, error_code: str = "GMAIL_DRAFT_FAILED"
) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise GmailToolError(error_code, f"Draft {field_name} is required.")
    if SAFE_HEADER_PATTERN.search(normalized):
        raise GmailToolError(
            error_code,
            f"Draft {field_name} contains unsafe header characters.",
        )
    return normalized


def _normalized_draft_body(value: str) -> str:
    normalized = str(value or "").replace("\r\n", "\n").strip()
    if not normalized:
        raise GmailToolError("GMAIL_DRAFT_FAILED", "Draft body is required.")

    lowered = normalized.lower()
    for marker in REPLY_MARKERS:
        index = lowered.find(marker)
        if index < 0:
            continue
        candidate = normalized[index + len(marker) :].strip()
        if candidate:
            normalized = candidate
            break

    normalized = normalized.strip()
    if not normalized:
        raise GmailToolError("GMAIL_DRAFT_FAILED", "Draft body is required.")
    return normalized


def _normalized_send_body(value: str) -> str:
    normalized = str(value or "").replace("\r\n", "\n").strip()
    if not normalized:
        raise GmailToolError("GMAIL_SEND_FAILED", "Email body is required.")
    return normalized


def _draft_payload(
    subject: str, body: str, latest_email: dict[str, Any]
) -> tuple[EmailMessage, str]:
    recipient = _safe_header_value(
        "recipient",
        str(latest_email.get("reply_to_email") or ""),
    )
    if "@" not in recipient:
        raise GmailToolError("GMAIL_DRAFT_FAILED", "Latest email recipient is invalid.")

    message = EmailMessage()
    message["To"] = recipient
    message["Subject"] = _safe_header_value("subject", subject)

    internet_message_id = str(latest_email.get("internet_message_id") or "").strip()
    if internet_message_id:
        internet_message_id = _safe_header_value("message id", internet_message_id)
        message["In-Reply-To"] = internet_message_id
        references = str(latest_email.get("references") or "").strip()
        message["References"] = (
            f"{references} {internet_message_id}".strip()
            if references
            else internet_message_id
        )

    message.set_content(_normalized_draft_body(body))
    return message, recipient


def _cached_thread_context_for_recipient(recipient: str) -> dict[str, Any]:
    try:
        latest_email = _read_latest_email_cache()
    except GmailToolError:
        return {}

    cached_recipient = str(latest_email.get("reply_to_email") or "").strip().lower()
    if cached_recipient != str(recipient or "").strip().lower():
        return {}
    return latest_email


def _send_payload(to: str, subject: str, body: str) -> tuple[EmailMessage, str, str]:
    recipient = _safe_header_value(
        "recipient", str(to or ""), error_code="GMAIL_SEND_FAILED"
    )
    if "@" not in recipient:
        raise GmailToolError("GMAIL_SEND_FAILED", "Email recipient is invalid.")

    message = EmailMessage()
    message["To"] = recipient
    message["Subject"] = _safe_header_value(
        "subject", subject, error_code="GMAIL_SEND_FAILED"
    )

    latest_email = _cached_thread_context_for_recipient(recipient)
    internet_message_id = str(latest_email.get("internet_message_id") or "").strip()
    if internet_message_id:
        internet_message_id = _safe_header_value(
            "message id",
            internet_message_id,
            error_code="GMAIL_SEND_FAILED",
        )
        message["In-Reply-To"] = internet_message_id
        references = str(latest_email.get("references") or "").strip()
        message["References"] = (
            f"{references} {internet_message_id}".strip()
            if references
            else internet_message_id
        )

    message.set_content(_normalized_send_body(body))
    return message, recipient, str(latest_email.get("thread_id") or "").strip()


def _mock_create_draft(
    subject: str, body: str, latest_email: dict[str, Any]
) -> dict[str, Any]:
    message, recipient = _draft_payload(subject, body, latest_email)
    draft_id = f"gmail-draft-{_timestamp()}-{_slugify(subject)}"
    MOCK_DRAFTS_DIR.mkdir(parents=True, exist_ok=True)
    target_path = MOCK_DRAFTS_DIR / f"{draft_id}.json"
    target_path.write_text(
        json.dumps(
            {
                "draft_id": draft_id,
                "thread_id": str(latest_email.get("thread_id") or "").strip(),
                "to": recipient,
                "subject": str(message["Subject"] or ""),
                "body": message.get_content().strip(),
                "source_subject": str(latest_email.get("subject") or "").strip(),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return {
        "draft_id": draft_id,
        "thread_id": str(latest_email.get("thread_id") or "").strip(),
        "to": recipient,
        "subject": str(message["Subject"] or ""),
        "source_subject": str(latest_email.get("subject") or "").strip(),
        "draft_created": True,
        "saved_path": str(target_path),
        "mode": "mock",
    }


def _real_create_draft(
    subject: str, body: str, latest_email: dict[str, Any]
) -> dict[str, Any]:
    message, recipient = _draft_payload(subject, body, latest_email)
    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode("ascii")
    payload: dict[str, Any] = {
        "message": {
            "raw": raw_message,
        }
    }
    thread_id = str(latest_email.get("thread_id") or "").strip()
    if thread_id:
        payload["message"]["threadId"] = thread_id

    response = _gmail_request_json("/drafts", method="POST", payload=payload)
    response_message = response.get("message")
    if not isinstance(response_message, dict):
        response_message = {}

    draft_id = str(response.get("id") or "").strip()
    if not draft_id:
        raise GmailToolError(
            "GMAIL_DRAFT_FAILED", "Gmail draft creation returned no draft id."
        )

    return {
        "draft_id": draft_id,
        "thread_id": str(response_message.get("threadId") or thread_id).strip(),
        "to": recipient,
        "subject": str(message["Subject"] or ""),
        "source_subject": str(latest_email.get("subject") or "").strip(),
        "draft_created": True,
        "mode": "real",
    }


def _real_send_email(to: str, subject: str, body: str) -> dict[str, Any]:
    message, recipient, thread_id = _send_payload(to, subject, body)
    payload: dict[str, Any] = {
        "raw": base64.urlsafe_b64encode(message.as_bytes()).decode("ascii"),
    }
    if thread_id:
        payload["threadId"] = thread_id

    response = _gmail_request_json("/messages/send", method="POST", payload=payload)
    message_id = str(response.get("id") or "").strip()
    if not message_id:
        raise GmailToolError("GMAIL_SEND_FAILED", "Gmail send returned no message id.")

    return {
        "message_id": message_id,
        "thread_id": str(response.get("threadId") or thread_id).strip(),
        "to": recipient,
        "subject": str(message["Subject"] or ""),
        "email_sent": True,
        "mode": "real",
    }


def run_gmail_read_latest_external(input_payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(input_payload, dict):
        raise GmailToolError("GMAIL_READ_FAILED", "Gmail tool input must be an object.")

    mode = _requested_mode()
    if mode == "mock":
        raise GmailToolError(
            "GMAIL_API_FAILED", "Gmail external execution is disabled in mock mode."
        )
    if not _gmail_credentials_available():
        raise GmailToolError(
            "GMAIL_API_FAILED", "Gmail credentials are not configured."
        )
    return _real_read_latest()


def run_gmail_read_latest_fallback(input_payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(input_payload, dict):
        raise GmailToolError("GMAIL_READ_FAILED", "Gmail tool input must be an object.")
    return _mock_read_latest()


def run_gmail_create_draft_external(input_payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(input_payload, dict):
        raise GmailToolError(
            "GMAIL_DRAFT_FAILED", "Gmail tool input must be an object."
        )

    subject = _safe_header_value("subject", str(input_payload.get("subject") or ""))
    body = _normalized_draft_body(str(input_payload.get("body") or ""))
    latest_email = _read_latest_email_cache()
    mode = _requested_mode()

    if mode == "mock":
        raise GmailToolError(
            "GMAIL_API_FAILED", "Gmail external execution is disabled in mock mode."
        )
    if not _gmail_credentials_available():
        raise GmailToolError(
            "GMAIL_API_FAILED", "Gmail credentials are not configured."
        )
    return _real_create_draft(subject, body, latest_email)


def run_gmail_create_draft_fallback(input_payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(input_payload, dict):
        raise GmailToolError(
            "GMAIL_DRAFT_FAILED", "Gmail tool input must be an object."
        )

    subject = _safe_header_value("subject", str(input_payload.get("subject") or ""))
    body = _normalized_draft_body(str(input_payload.get("body") or ""))
    latest_email = _read_latest_email_cache()
    return _mock_create_draft(subject, body, latest_email)


def run_google_gmail_send_external(input_payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(input_payload, dict):
        raise GmailToolError("GMAIL_SEND_FAILED", "Gmail tool input must be an object.")

    to = _safe_header_value(
        "recipient", str(input_payload.get("to") or ""), error_code="GMAIL_SEND_FAILED"
    )
    subject = _safe_header_value(
        "subject",
        str(input_payload.get("subject") or ""),
        error_code="GMAIL_SEND_FAILED",
    )
    body = _normalized_send_body(str(input_payload.get("body") or ""))
    mode = _requested_mode()

    if mode == "mock":
        raise GmailToolError(
            "GMAIL_API_FAILED", "Gmail external execution is disabled in mock mode."
        )
    if not _gmail_credentials_available():
        raise GmailToolError(
            "GMAIL_API_FAILED", "Gmail credentials are not configured."
        )
    return _real_send_email(to, subject, body)


def run_gmail_read_latest(input_payload: dict[str, Any]) -> dict[str, Any]:
    mode = _requested_mode()
    try:
        return run_gmail_read_latest_external(input_payload)
    except GmailToolError:
        if mode == "real":
            raise
        return run_gmail_read_latest_fallback(input_payload)


def run_gmail_create_draft(input_payload: dict[str, Any]) -> dict[str, Any]:
    mode = _requested_mode()
    try:
        return run_gmail_create_draft_external(input_payload)
    except GmailToolError:
        if mode == "real":
            raise
        return run_gmail_create_draft_fallback(input_payload)
