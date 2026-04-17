from __future__ import annotations

import json
import os

import pytest

from integrations import gmail_gateway, gmail_tool


def test_run_gmail_read_latest_returns_latest_mock_email(tmp_path, monkeypatch) -> None:
    inbox_dir = tmp_path / "runtime" / "out" / "external_business" / "gmail_gateway" / "inbox"
    drafts_dir = tmp_path / "runtime" / "out" / "external_business" / "gmail_gateway" / "drafts"
    cache_path = tmp_path / "artifacts" / "tool_cache" / "gmail" / "latest_email.json"
    inbox_dir.mkdir(parents=True, exist_ok=True)
    drafts_dir.mkdir(parents=True, exist_ok=True)

    (inbox_dir / "older.json").write_text(
        json.dumps(
            {
                "subject": "Earlier note",
                "sender": "Alex <alex@example.com>",
                "reply_to": "alex@example.com",
                "body_text": "First message.",
                "thread_id": "thread-001",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (inbox_dir / "latest.json").write_text(
        json.dumps(
            {
                "subject": "Client follow-up",
                "sender": "Jamie Client <jamie@example.com>",
                "reply_to": "jamie@example.com",
                "body_text": "Can you send a reply draft today?",
                "thread_id": "thread-002",
                "message_id": "msg-002",
                "internet_message_id": "<msg-002@example.com>",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    os.utime(inbox_dir / "latest.json", (2000000000, 2000000000))

    monkeypatch.setenv("DIGITAL_FOREMAN_GMAIL_TOOL_MODE", "mock")
    monkeypatch.setattr(gmail_tool, "MOCK_INBOX_DIR", inbox_dir)
    monkeypatch.setattr(gmail_tool, "MOCK_DRAFTS_DIR", drafts_dir)
    monkeypatch.setattr(gmail_tool, "LATEST_EMAIL_CACHE_PATH", cache_path)

    result = gmail_tool.run_gmail_read_latest({})

    assert result == {
        "subject": "Client follow-up",
        "sender": "Jamie Client <jamie@example.com>",
        "body_text": "Can you send a reply draft today?",
        "message_id": "msg-002",
        "thread_id": "thread-002",
        "reply_to_email": "jamie@example.com",
        "mode": "mock",
    }
    cached_payload = json.loads(cache_path.read_text(encoding="utf-8"))
    assert cached_payload["reply_to_email"] == "jamie@example.com"
    assert cached_payload["subject"] == "Client follow-up"


def test_run_gmail_create_draft_writes_mock_draft(tmp_path, monkeypatch) -> None:
    drafts_dir = tmp_path / "runtime" / "out" / "external_business" / "gmail_gateway" / "drafts"
    cache_path = tmp_path / "artifacts" / "tool_cache" / "gmail" / "latest_email.json"
    drafts_dir.mkdir(parents=True, exist_ok=True)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(
            {
                "thread_id": "thread-200",
                "internet_message_id": "<thread-200@example.com>",
                "reply_to_email": "jamie@example.com",
                "sender": "Jamie Client <jamie@example.com>",
                "subject": "Client follow-up",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setenv("DIGITAL_FOREMAN_GMAIL_TOOL_MODE", "mock")
    monkeypatch.setattr(gmail_tool, "MOCK_DRAFTS_DIR", drafts_dir)
    monkeypatch.setattr(gmail_tool, "LATEST_EMAIL_CACHE_PATH", cache_path)

    result = gmail_tool.run_gmail_create_draft(
        {
            "subject": "Re: Client follow-up",
            "body": "Summary: Jamie needs a response.\n\nReply draft:\nHi Jamie,\n\nThanks for the follow-up.\n\nBest,",
        }
    )

    draft_path = drafts_dir / f"{result['draft_id']}.json"
    saved_payload = json.loads(draft_path.read_text(encoding="utf-8"))

    assert result["draft_created"] is True
    assert result["to"] == "jamie@example.com"
    assert result["source_subject"] == "Client follow-up"
    assert result["mode"] == "mock"
    assert saved_payload["body"] == "Hi Jamie,\n\nThanks for the follow-up.\n\nBest,"


def test_run_gmail_create_draft_rejects_unsafe_subject(tmp_path, monkeypatch) -> None:
    cache_path = tmp_path / "artifacts" / "tool_cache" / "gmail" / "latest_email.json"
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(
        json.dumps(
            {
                "reply_to_email": "jamie@example.com",
                "internet_message_id": "<thread-300@example.com>",
                "subject": "Client follow-up",
            }
        )
        + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(gmail_tool, "LATEST_EMAIL_CACHE_PATH", cache_path)

    with pytest.raises(gmail_tool.GmailToolError) as error:
        gmail_tool.run_gmail_create_draft(
            {
                "subject": "Re: Client follow-up\nBCC: someone@example.com",
                "body": "Thanks for the note.",
            }
        )

    assert error.value.code == "GMAIL_DRAFT_FAILED"
    assert error.value.message == "Draft subject contains unsafe header characters."


def test_run_google_gmail_send_external_returns_sent_message(monkeypatch) -> None:
    monkeypatch.setenv("DIGITAL_FOREMAN_GMAIL_TOOL_MODE", "real")
    monkeypatch.setattr(gmail_tool.google_drive_reader, "_has_required_credentials", lambda: True)
    monkeypatch.setattr(
        gmail_tool,
        "_gmail_request_json",
        lambda path, method="GET", payload=None: {
            "id": "msg-sent-001",
            "threadId": "thread-sent-001",
        },
    )

    result = gmail_tool.run_google_gmail_send_external(
        {
            "to": "client@example.com",
            "subject": "ADU project update",
            "body": "Thanks for reaching out.",
        }
    )

    assert result == {
        "message_id": "msg-sent-001",
        "thread_id": "thread-sent-001",
        "to": "client@example.com",
        "subject": "ADU project update",
        "email_sent": True,
        "mode": "real",
    }


def test_gmail_execution_sender_prefers_dedicated_env(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_EXECUTION_SENDER_EMAIL", "personal.sender@gmail.com")

    assert gmail_tool.gmail_execution_sender() == "personal.sender@gmail.com"


def test_gmail_credentials_available_prefers_dedicated_refresh_token(monkeypatch) -> None:
    monkeypatch.setenv("GMAIL_EXECUTION_CLIENT_ID", "gmail-client-id")
    monkeypatch.setenv("GMAIL_EXECUTION_CLIENT_SECRET", "gmail-client-secret")
    monkeypatch.setenv("GMAIL_EXECUTION_REFRESH_TOKEN", "gmail-refresh-token")
    monkeypatch.setattr(gmail_tool.google_drive_reader, "_has_required_credentials", lambda: False)

    assert gmail_tool._gmail_credentials_available() is True


def test_gmail_gateway_send_email_uses_real_gmail_send(monkeypatch) -> None:
    captured_payload: dict[str, str] = {}
    monkeypatch.setattr(
        gmail_gateway,
        "run_google_gmail_send_external",
        lambda payload: (
            captured_payload.update(dict(payload))
            or {
                "message_id": "msg-sent-101",
                "thread_id": "thread-sent-101",
                "to": payload["to"],
                "subject": payload["subject"],
                "email_sent": True,
                "mode": "real",
            }
        ),
    )

    result = gmail_gateway.send_email(
        to="client@example.com",
        subject="Project update",
        body="Thanks for reaching out.",
        link="https://docs.google.com/document/d/doc-101",
        attachments=["D:\\digital_foreman\\tmp\\quote.pdf"],
    )

    assert captured_payload == {
        "to": "client@example.com",
        "subject": "Project update",
        "body": "Thanks for reaching out.\n\nShared document link: https://docs.google.com/document/d/doc-101",
    }
    assert result["ok"] is True
    assert result["mode"] == "real"
    assert result["resource"] == {
        "message_id": "msg-sent-101",
        "thread_id": "thread-sent-101",
        "to": "client@example.com",
        "subject": "Project update",
        "link": "https://docs.google.com/document/d/doc-101",
        "attachments": ["D:\\digital_foreman\\tmp\\quote.pdf"],
    }


def test_gmail_gateway_send_email_returns_failure_without_mock_fallback(monkeypatch) -> None:
    monkeypatch.setattr(
        gmail_gateway,
        "run_google_gmail_send_external",
        lambda payload: (_ for _ in ()).throw(
            gmail_tool.GmailToolError("GMAIL_API_FAILED", "Gmail credentials are not configured.")
        ),
    )

    result = gmail_gateway.send_email(
        to="client@example.com",
        subject="Project update",
        body="Thanks for reaching out.",
    )

    assert result["ok"] is False
    assert result["status"] == "failed"
    assert result["mode"] == "real"
    assert result["error"] == "Gmail credentials are not configured."
