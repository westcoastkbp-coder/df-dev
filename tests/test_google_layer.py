from __future__ import annotations

from control.tool_executor import execute_tool, execute_tool_call
from control.tool_registry import (
    GOOGLE_DRIVE_READ_FILE_LAYER_TOOL,
    GOOGLE_GMAIL_SEND_TOOL,
    GOOGLE_LAYER_TOOL,
    has_registered_tool,
    resolve_tool_executor,
)


def test_google_layer_tool_resolves_from_registry() -> None:
    executor = resolve_tool_executor(GOOGLE_LAYER_TOOL)

    assert has_registered_tool(GOOGLE_LAYER_TOOL) is True
    assert callable(executor)


def test_google_drive_layer_tool_resolves_from_registry() -> None:
    executor = resolve_tool_executor(GOOGLE_DRIVE_READ_FILE_LAYER_TOOL)

    assert has_registered_tool(GOOGLE_DRIVE_READ_FILE_LAYER_TOOL) is True
    assert callable(executor)


def test_execute_tool_call_routes_google_layer_to_google_gmail_send(monkeypatch) -> None:
    monkeypatch.setattr(
        "integrations.gmail_tool.run_google_gmail_send_external",
        lambda payload: {
            "message_id": "msg-layer-001",
            "thread_id": "thread-layer-001",
            "to": payload["to"],
            "subject": payload["subject"],
            "email_sent": True,
            "mode": "real",
        },
    )
    monkeypatch.setattr(
        "control.tool_registry._TOOL_REGISTRY",
        {
            GOOGLE_LAYER_TOOL: __import__(
                "integrations.google_layer_tool",
                fromlist=["run_google_layer_external"],
            ).run_google_layer_external,
            GOOGLE_GMAIL_SEND_TOOL: __import__(
                "integrations.gmail_tool",
                fromlist=["run_google_gmail_send_external"],
            ).run_google_gmail_send_external,
        },
    )

    result = execute_tool_call(
        {
            "tool_name": GOOGLE_LAYER_TOOL,
            "input": {
                "tool": GOOGLE_GMAIL_SEND_TOOL,
                "input": {
                    "to": "jamie@example.com",
                    "subject": "Re: Client follow-up",
                    "body": "Thanks for the follow-up.",
                },
            },
        }
    )

    assert result["ok"] is True
    assert result["tool_name"] == GOOGLE_LAYER_TOOL
    assert result["output"] == {
        "message_id": "msg-layer-001",
        "thread_id": "thread-layer-001",
        "to": "jamie@example.com",
        "subject": "Re: Client follow-up",
        "email_sent": True,
        "mode": "real",
    }
    assert result["error"] is None
    assert result["source"] == "external"


def test_execute_tool_routes_google_layer_by_payload_shape(monkeypatch) -> None:
    monkeypatch.setattr(
        "integrations.gmail_tool.run_google_gmail_send_external",
        lambda payload: {
            "message_id": "msg-shape-001",
            "thread_id": "thread-shape-001",
            "to": payload["to"],
            "subject": payload["subject"],
            "email_sent": True,
            "mode": "real",
        },
    )
    monkeypatch.setattr(
        "control.tool_registry._TOOL_REGISTRY",
        {
            GOOGLE_LAYER_TOOL: __import__(
                "integrations.google_layer_tool",
                fromlist=["run_google_layer_external"],
            ).run_google_layer_external,
            GOOGLE_GMAIL_SEND_TOOL: __import__(
                "integrations.gmail_tool",
                fromlist=["run_google_gmail_send_external"],
            ).run_google_gmail_send_external,
        },
    )

    result = execute_tool(
        GOOGLE_LAYER_TOOL,
        {
            "to": "jamie@example.com",
            "subject": "Re: Client follow-up",
            "body": "Thanks for the follow-up.",
        },
    )

    assert result == {
        "status": "success",
        "data": {
            "message_id": "msg-shape-001",
            "thread_id": "thread-shape-001",
            "to": "jamie@example.com",
            "subject": "Re: Client follow-up",
            "email_sent": True,
            "mode": "real",
        },
        "error": None,
        "source": "external",
    }


def test_execute_tool_call_supports_grouped_google_drive_alias(monkeypatch) -> None:
    monkeypatch.setattr(
        "integrations.google_drive_tool.run_google_drive_read_file",
        lambda payload: {
            "file_id": payload["file_id"],
            "name": "Project Notes",
            "mime_type": "text/plain",
            "content_text": "Line one\nLine two",
        },
    )
    monkeypatch.setattr(
        "control.tool_registry._TOOL_REGISTRY",
        {
            GOOGLE_DRIVE_READ_FILE_LAYER_TOOL: __import__(
                "integrations.google_drive_tool",
                fromlist=["run_google_drive_read_file"],
            ).run_google_drive_read_file,
        },
    )

    result = execute_tool_call(
        {
            "tool_name": GOOGLE_DRIVE_READ_FILE_LAYER_TOOL,
            "input": {
                "file_id": "file-live-001",
            },
        }
    )

    assert result["ok"] is True
    assert result["tool_name"] == GOOGLE_DRIVE_READ_FILE_LAYER_TOOL
    assert result["output"] == {
        "file_id": "file-live-001",
        "name": "Project Notes",
        "mime_type": "text/plain",
        "content_text": "Line one\nLine two",
    }
    assert result["error"] is None
    assert result["source"] == "external"
