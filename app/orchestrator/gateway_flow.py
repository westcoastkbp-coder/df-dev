from __future__ import annotations

import logging
from typing import Any

from integrations.drive_gateway import (
    create_file,
    create_folder,
    get_share_link,
    upload_file,
)
from integrations.gmail_gateway import send_email

LOGGER = logging.getLogger(__name__)


def _log_event(log: list[str], message: str) -> None:
    LOGGER.info("gateway_flow: %s", message)
    log.append(message)


def _skipped_result(operation: str, reason: str) -> dict[str, Any]:
    return {
        "ok": False,
        "provider": "gateway_flow",
        "operation": operation,
        "status": "skipped",
        "mode": "mock",
        "error": reason,
        "resource": {},
        "log": [reason],
    }


def create_and_send_document(
    *,
    folder_name: str,
    file_name: str,
    content: str,
    recipient_email: str,
    subject: str,
    body: str,
    upload_path: str | None = None,
) -> dict[str, Any]:
    scenario_log: list[str] = []
    _log_event(scenario_log, "start create_and_send_document")

    folder_result = create_folder(folder_name)
    document_result = (
        upload_file(upload_path) if upload_path else create_file(file_name, content)
    )

    if document_result.get("ok"):
        file_id = str(document_result.get("resource", {}).get("file_id", "")).strip()
        link_result = get_share_link(file_id)
    else:
        link_result = _skipped_result(
            "get_share_link",
            "Skipped share link because the Drive document step failed.",
        )

    if document_result.get("ok") and link_result.get("ok"):
        attachment_path = document_result.get("resource", {}).get("path")
        email_result = send_email(
            to=recipient_email,
            subject=subject,
            body=body,
            link=link_result.get("resource", {}).get("share_link"),
            attachments=[attachment_path] if attachment_path else None,
        )
    else:
        email_result = _skipped_result(
            "send_email",
            "Skipped email because the Drive share-link step did not complete.",
        )

    overall_ok = all(
        step.get("ok")
        for step in (folder_result, document_result, link_result, email_result)
    )
    _log_event(
        scenario_log,
        (
            "completed create_and_send_document"
            if overall_ok
            else "completed create_and_send_document with errors"
        ),
    )

    return {
        "ok": overall_ok,
        "scenario": "create_and_send_document",
        "status": "completed" if overall_ok else "failed",
        "mode": "mock",
        "inputs": {
            "folder_name": folder_name,
            "file_name": file_name,
            "recipient_email": recipient_email,
            "subject": subject,
            "upload_path": upload_path,
        },
        "steps": {
            "drive_folder": folder_result,
            "drive_document": document_result,
            "drive_share_link": link_result,
            "gmail_send": email_result,
        },
        "summary": {
            "folder_id": folder_result.get("resource", {}).get("folder_id"),
            "file_id": document_result.get("resource", {}).get("file_id"),
            "share_link": link_result.get("resource", {}).get("share_link"),
            "message_id": email_result.get("resource", {}).get("message_id"),
        },
        "log": scenario_log,
    }
