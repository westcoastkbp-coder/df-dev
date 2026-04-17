from __future__ import annotations

import logging
from typing import Any

from integrations.gmail_tool import GmailToolError, run_google_gmail_send_external

LOGGER = logging.getLogger(__name__)


def _log_event(log: list[str], message: str) -> None:
    LOGGER.info("gmail_gateway: %s", message)
    log.append(message)


def _base_result(operation: str) -> dict[str, Any]:
    return {
        "ok": True,
        "provider": "gmail_gateway",
        "operation": operation,
        "status": "completed",
        "mode": "real",
        "requested_mode": "real",
        "api_connected": True,
        "fallback_reason": None,
        "resource": {},
        "error": None,
        "log": [],
    }


def _fail(result: dict[str, Any], message: str) -> dict[str, Any]:
    result["ok"] = False
    result["status"] = "failed"
    result["error"] = message
    _log_event(result["log"], message)
    return result


def send_email(
    to: str,
    subject: str,
    body: str,
    link: str | None = None,
    attachments: list[str] | None = None,
) -> dict[str, Any]:
    result = _base_result("send_email")
    normalized_to = str(to or "").strip()
    normalized_subject = str(subject or "").strip()
    normalized_body = str(body or "").strip()
    normalized_attachments = [
        str(item) for item in attachments or [] if str(item).strip()
    ]

    _log_event(
        result["log"],
        f"start send_email to={normalized_to or '(empty)'} subject={normalized_subject or '(empty)'}",
    )

    if not normalized_to:
        return _fail(result, "Gmail recipient is required.")

    if not normalized_subject:
        return _fail(result, "Gmail subject is required.")

    rendered_body = normalized_body
    if link:
        rendered_body = (
            f"{normalized_body}\n\nShared document link: {link}"
            if normalized_body
            else f"Shared document link: {link}"
        )
    if normalized_attachments:
        _log_event(
            result["log"],
            "attachments requested but Gmail gateway sends message body only; attachments skipped",
        )

    try:
        gmail_result = run_google_gmail_send_external(
            {
                "to": normalized_to,
                "subject": normalized_subject,
                "body": rendered_body,
            }
        )
    except GmailToolError as error:
        return _fail(result, error.message)

    result["resource"] = {
        "message_id": str(gmail_result.get("message_id") or "").strip(),
        "thread_id": str(gmail_result.get("thread_id") or "").strip(),
        "to": normalized_to,
        "subject": normalized_subject,
        "link": link,
        "attachments": normalized_attachments,
    }
    _log_event(
        result["log"],
        (
            "email sent"
            f" to={normalized_to} subject={normalized_subject} "
            f"message_id={result['resource']['message_id'] or '(missing)'}"
        ),
    )
    return result

