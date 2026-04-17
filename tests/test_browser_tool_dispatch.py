from __future__ import annotations

from pathlib import Path

import app.execution.execution_boundary as execution_boundary_module
import app.execution.browser_tool as browser_tool_module
import app.product.runner as product_runner_module
import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_state_store as task_state_store_module
from app.product.runner import dispatch_action_trigger


class _FakeResponse:
    def __init__(self, text: str, status_code: int = 200) -> None:
        self.text = text
        self.status_code = status_code


class _FakeClient:
    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs

    def get(self, url: str) -> _FakeResponse:
        return _FakeResponse("<html><body><h1>Contact</h1></body></html>")

    def post(self, url: str, data=None) -> _FakeResponse:
        return _FakeResponse("<html><body><h1>Submitted</h1></body></html>")

    def close(self) -> None:
        return None


def _configure_state_backend(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store_module,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(product_runner_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        execution_boundary_module,
        "EXECUTION_BOUNDARY_VIOLATIONS_LOG",
        tmp_path / "runtime" / "logs" / "execution_boundary_violations.jsonl",
    )
    task_factory_module.clear_task_runtime_store()


def test_dispatch_action_trigger_executes_browser_tool(monkeypatch, tmp_path: Path) -> None:
    _configure_state_backend(monkeypatch, tmp_path)
    monkeypatch.setattr(browser_tool_module.httpx, "Client", _FakeClient)

    with execution_boundary_module.execution_boundary(
        {"task_id": "DF-BROWSER-DISPATCH-V1", "intent": "browser_task"},
        policy_validated=True,
    ):
        result = dispatch_action_trigger(
            {
                "action_type": "BROWSER_TOOL",
                "payload": {
                    "task_id": "DF-BROWSER-DISPATCH-V1",
                    "steps": [
                        {"operation": "open_url", "url": "https://example.com/contact"},
                        {"operation": "get_page_text"},
                    ],
                },
            },
            task_state={"task_id": "DF-BROWSER-DISPATCH-V1", "status": "EXECUTING"},
        )

    assert result["status"] == "completed"
    assert result["action_type"] == "BROWSER_TOOL"
    assert "Contact" in result["result_payload"]["steps"][1]["text"]
