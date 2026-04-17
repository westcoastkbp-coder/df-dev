from __future__ import annotations

from app.execution.vendor_router import route


def test_route_selects_google_for_docs_and_email() -> None:
    vendor = route(
        {"intent": "process_email"},
        {"command_name": "send email"},
        {
            "action": "google_docs.create_document",
            "action_type": "SEND_EMAIL",
            "target": "docs.google.com/document/d/example",
            "parameters": {"tool_name": "gmail.read_latest"},
        },
    )

    assert vendor == "google"


def test_route_selects_claude_for_browser_actions() -> None:
    vendor = route(
        {"intent": "browser_task"},
        {"command_name": "browser form submit"},
        {
            "action": "browser_tool",
            "action_type": "BROWSER_TOOL",
            "target": "https://example.com/form",
            "parameters": {"url": "https://example.com/form"},
        },
    )

    assert vendor == "claude"


def test_route_selects_codex_for_repo_and_code_targets() -> None:
    vendor = route(
        {"intent": "patch_repo", "payload": {"path": "app/orchestrator/execution_runner.py"}},
        {"command_name": "update repo"},
        {
            "action": "write_file",
            "action_type": "WRITE_FILE",
            "target": "tests/test_vendor_router.py",
            "parameters": {"path": "tests/test_vendor_router.py"},
        },
    )

    assert vendor == "codex"


def test_route_defaults_to_openai_for_general_reasoning() -> None:
    vendor = route(
        {"intent": "summarize_status"},
        {"command_name": "summarize status"},
        {
            "action": "summarize",
            "action_type": "SUMMARY",
            "target": "execution",
            "parameters": {"summary": "daily status"},
        },
    )

    assert vendor == "openai"
