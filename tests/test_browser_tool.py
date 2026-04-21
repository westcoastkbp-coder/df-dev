from __future__ import annotations

import json
from pathlib import Path

import pytest

import app.execution.browser_tool as browser_tool_module
from app.execution.browser_tool import (
    BrowserToolValidationError,
    execute_browser_action,
)
from app.execution.execution_boundary import ExecutionBoundaryViolationError, execution_boundary


class _FakeResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code


class _FakeClient:
    def __init__(self, *args, **kwargs) -> None:
        self.calls: list[tuple[str, str, dict[str, str] | None]] = []

    def get(self, url: str) -> _FakeResponse:
        self.calls.append(("GET", url, None))
        pages = {
            "https://example.com/contact": "<html><body><h1>Contact</h1><p>Send us a note.</p></body></html>",
            "https://example.com/more-info": "<html><body><h1>More Info</h1></body></html>",
        }
        return _FakeResponse(pages.get(url, "<html><body>OK</body></html>"))

    def post(self, url: str, data: dict[str, str] | None = None) -> _FakeResponse:
        self.calls.append(("POST", url, dict(data or {})))
        return _FakeResponse("<html><body><h1>Submitted</h1></body></html>")

    def close(self) -> None:
        return None


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_browser_tool_requires_execution_boundary() -> None:
    with pytest.raises(ExecutionBoundaryViolationError, match="direct_browser_tool_call_blocked"):
        execute_browser_action(
            {
                "task_id": "DF-BROWSER-DIRECT-V1",
                "steps": [
                    {"operation": "open_url", "url": "https://example.com/contact"},
                ],
            }
        )


def test_browser_tool_blocks_unallowlisted_url() -> None:
    with execution_boundary(
        {"task_id": "DF-BROWSER-BLOCKED-V1", "intent": "browser_task"},
        policy_validated=True,
    ):
        with pytest.raises(BrowserToolValidationError, match="not allowlisted"):
            execute_browser_action(
                {
                    "task_id": "DF-BROWSER-BLOCKED-V1",
                    "steps": [
                        {"operation": "open_url", "url": "https://malicious.example.net/"},
                    ],
                }
            )


def test_browser_tool_executes_sequence_and_logs(monkeypatch, tmp_path: Path) -> None:
    browser_log = tmp_path / "runtime" / "logs" / "browser_tool.jsonl"
    monkeypatch.setattr(browser_tool_module, "BROWSER_TOOL_LOG_FILE", browser_log)
    monkeypatch.setattr(browser_tool_module.httpx, "Client", _FakeClient)

    with execution_boundary(
        {"task_id": "DF-BROWSER-SEQUENCE-V1", "intent": "browser_task"},
        policy_validated=True,
    ):
        result = execute_browser_action(
            {
                "task_id": "DF-BROWSER-SEQUENCE-V1",
                "steps": [
                    {"operation": "open_url", "url": "https://example.com/contact"},
                    {
                        "operation": "fill_form",
                        "selector": "input[name='email']",
                        "value": "ops@example.com",
                    },
                    {
                        "operation": "fill_form",
                        "selector": "textarea[name='message']",
                        "value": "Need an update",
                    },
                    {"operation": "click", "selector": "button[type='submit']"},
                    {"operation": "get_page_text"},
                ],
                "confirmed": True,
            }
        )

    assert result["status"] == "completed"
    assert result["action_type"] == "BROWSER_TOOL"
    steps = result["result_payload"]["steps"]
    assert steps[0]["url"] == "https://example.com/contact"
    assert steps[3]["method"] == "POST"
    assert "Submitted" in steps[4]["text"]

    logged = _read_jsonl(browser_log)
    assert [entry["details"]["operation"] for entry in logged] == [
        "open_url",
        "fill_form",
        "fill_form",
        "click",
        "get_page_text",
    ]


def test_browser_tool_blocks_form_submission_without_confirmation(monkeypatch) -> None:
    monkeypatch.setattr(browser_tool_module.httpx, "Client", _FakeClient)

    with execution_boundary(
        {"task_id": "DF-BROWSER-CONFIRM-V1", "intent": "browser_task"},
        policy_validated=True,
    ):
        result = execute_browser_action(
            {
                "task_id": "DF-BROWSER-CONFIRM-V1",
                "steps": [
                    {"operation": "open_url", "url": "https://example.com/contact"},
                    {
                        "operation": "fill_form",
                        "selector": "input[name='email']",
                        "value": "ops@example.com",
                    },
                    {"operation": "click", "selector": "button[type='submit']"},
                ],
            }
        )

    assert result["status"] == "policy_blocked"
    assert result["error_code"] == "POLICY_VIOLATION"
    assert result["error_message"] == "critical action requires confirmation: BROWSER_TOOL"
