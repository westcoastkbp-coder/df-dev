from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

import app.execution.lead_estimate_decision as lead_estimate_decision_module
import app.execution.paths as paths_module
import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_state_store as task_state_store_module
from app.execution.lead_estimate_contract import WORKFLOW_TYPE
from app.execution.product_runtime import (
    PRODUCT_RUNTIME_ALLOWLIST,
    ProductRuntimeBoundaryError,
    assert_product_runtime_module,
)
from app.execution.task_schema import TASK_CONTRACT_VERSION
from app.orchestrator.execution_runner import run_execution
from app.orchestrator.mock_executor import execute_mock_task
from app.orchestrator.task_queue import InMemoryTaskQueue
from functools import partial
from app.orchestrator.task_worker import (
    process_next_queued_task as _process_next_queued_task,
)
from tests.system_context import WORKING_SYSTEM_CONTEXT

process_next_queued_task = partial(
    _process_next_queued_task, system_context=WORKING_SYSTEM_CONTEXT
)
from app.product.runner import execute_product_task_request


def _configure_runtime(monkeypatch, tmp_path: Path) -> Path:
    task_store_path = tmp_path / "data" / "tasks.json"
    queue_file = tmp_path / "runtime" / "state" / "task_queue.json"
    task_log_file = tmp_path / "runtime" / "logs" / "tasks.log"
    system_log_file = tmp_path / "runtime" / "logs" / "system.log"

    import app.orchestrator.task_queue as task_queue_module
    import runtime.system_log as system_log_module

    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store_module,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(paths_module, "TASKS_FILE", task_store_path)
    monkeypatch.setattr(lead_estimate_decision_module, "TASKS_FILE", task_store_path)
    monkeypatch.setattr(task_queue_module, "TASK_QUEUE_FILE", queue_file)
    monkeypatch.setattr(task_queue_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(system_log_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)
    task_factory_module.clear_task_runtime_store()
    return task_store_path


def _build_workflow_task(*, store_path: Path, task_id: str) -> dict[str, object]:
    return task_factory_module.save_task(
        {
            "task_contract_version": TASK_CONTRACT_VERSION,
            "task_id": task_id,
            "created_at": "2026-04-04T00:00:00Z",
            "intent": WORKFLOW_TYPE,
            "payload": {
                "workflow_type": WORKFLOW_TYPE,
                "lead_id": "lead-boundary-allow-001",
                "lead_data": {
                    "project_type": "ADU",
                    "scope_summary": "Detached ADU with pricing request",
                    "contact_info": {"phone": "555-0100"},
                    "lead_exists": True,
                },
            },
            "status": "pending",
            "notes": [],
            "history": [],
            "interaction_id": task_id,
            "job_id": task_id,
            "trace_id": task_id,
        },
        store_path=store_path,
    )


def test_product_flow_still_runs_inside_allowlist(monkeypatch, tmp_path: Path) -> None:
    task_store_path = _configure_runtime(monkeypatch, tmp_path)
    queue = InMemoryTaskQueue()
    task_data = _build_workflow_task(
        store_path=task_store_path,
        task_id="DF-PRODUCT-ALLOWLIST-V1",
    )

    def fetch_task(task_id: str) -> dict[str, object] | None:
        return task_data if task_id == task_data["task_id"] else None

    def persist(updated_task: dict[str, object]) -> None:
        task_data.update(updated_task)

    assert queue.enqueue_task(task_data["task_id"]) is True

    executed_task = process_next_queued_task(
        queue=queue,
        now=lambda: "2026-04-04T00:00:00Z",
        fetch_task=fetch_task,
        persist=persist,
        timeout=0.0,
        telemetry_collector=lambda: {},
        network_snapshot_collector=lambda: {},
    )

    assert executed_task is not None
    assert executed_task["status"] == "COMPLETED"
    assert executed_task["result"]["result_type"] == WORKFLOW_TYPE


def test_dev_module_import_is_rejected() -> None:
    with pytest.raises(ProductRuntimeBoundaryError, match="execution_replay"):
        assert_product_runtime_module(
            "app.execution.execution_replay",
            context="test_product_runtime_boundary",
        )


def test_script_execution_is_blocked() -> None:
    result = execute_product_task_request(
        SimpleNamespace(
            objective="EXECUTE:DF-RUN-TESTS",
            user_id="codex",
            user_role="admin",
        ),
        principal={"actor_id": "codex", "role": "admin"},
        request_source="api",
        execute_single=lambda *args, **kwargs: {"status": "completed"},
    )

    assert isinstance(result, dict)
    assert result["status"] == "error"
    assert result["result_type"] == "runtime_boundary_blocked"
    assert "RUN_TESTS" in result["result_summary"]


def test_mock_executor_usage_is_blocked(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_runtime(monkeypatch, tmp_path)
    task = _build_workflow_task(
        store_path=store_path,
        task_id="DF-PRODUCT-MOCK-BLOCK-V1",
    )

    persist_calls = 0

    def persist(_: dict[str, object]) -> None:
        nonlocal persist_calls
        persist_calls += 1

    with pytest.raises(ProductRuntimeBoundaryError, match="mock_executor"):
        run_execution(
            task,
            now=lambda: "2026-04-04T00:00:00Z",
            persist=persist,
            executor=execute_mock_task,
        )

    assert persist_calls == 0


def test_allowlist_contains_product_executor_and_voice_runtime() -> None:
    assert "app.execution.product_executor" in PRODUCT_RUNTIME_ALLOWLIST
    assert "app.voice" in PRODUCT_RUNTIME_ALLOWLIST
    assert "app.execution.browser_tool" in PRODUCT_RUNTIME_ALLOWLIST
