from __future__ import annotations

from pathlib import Path

import pytest

from app.context.shared_context_store import get_context
from app.execution.action_result import build_action_result
from app.execution.task_schema import TASK_CONTRACT_VERSION
import app.orchestrator.execution_runner as execution_runner_module
import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_state_store as task_state_store_module
import app.orchestrator.task_store as task_store_module


def _configure_runtime(monkeypatch, tmp_path: Path) -> Path:
    task_system_path = tmp_path / "data" / "task_system.json"
    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store_module,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(task_factory_module, "TASK_SYSTEM_FILE", task_system_path)
    task_factory_module.clear_task_runtime_store()
    return task_system_path


def _raw_task(task_id: str) -> dict[str, object]:
    return {
        "task_contract_version": TASK_CONTRACT_VERSION,
        "task_id": task_id,
        "created_at": "2026-04-12T00:00:00Z",
        "last_updated_at": "2026-04-12T00:00:00Z",
        "intent": "WRITE_FILE",
        "payload": {"path": r"runtime\out\task-layer.txt"},
        "status": "VALIDATED",
        "notes": [],
        "history": [
            {
                "timestamp": "2026-04-12T00:00:00Z",
                "event": "created",
                "from_status": "",
                "to_status": "VALIDATED",
                "details": {
                    "summary": "execution-bound task",
                },
            }
        ],
        "interaction_id": task_id,
        "job_id": task_id,
        "trace_id": task_id,
    }


def test_create_task_persists_public_task_and_active_summary(
    monkeypatch, tmp_path: Path
) -> None:
    task_system_path = _configure_runtime(monkeypatch, tmp_path)

    task = task_store_module.create_task(
        task_type="WRITE_FILE",
        task_input={"path": r"runtime\out\create.txt"},
        task_id="DF-TASK-LAYER-CREATE-V1",
        store_path=task_system_path,
    )

    assert task == {
        "task_id": "DF-TASK-LAYER-CREATE-V1",
        "type": "WRITE_FILE",
        "status": "CREATED",
        "input": {"path": r"runtime\out\create.txt"},
        "result": {},
        "history": task["history"],
        "timestamps": task["timestamps"],
    }
    assert task["history"][0]["event"] == "created"
    assert task["timestamps"]["created_at"]

    system_context = get_context("system_context", root_dir=tmp_path)
    assert system_context["active_tasks"][0]["task_id"] == "DF-TASK-LAYER-CREATE-V1"
    assert system_context["active_tasks"][0]["type"] == "WRITE_FILE"


def test_update_task_updates_result_and_history(monkeypatch, tmp_path: Path) -> None:
    task_system_path = _configure_runtime(monkeypatch, tmp_path)
    created = task_store_module.create_task(
        task_type="READ_FILE",
        task_input={"path": r"runtime\out\update.txt"},
        task_id="DF-TASK-LAYER-UPDATE-V1",
        store_path=task_system_path,
    )

    updated = task_store_module.update_task(
        created["task_id"],
        status="VALIDATED",
        task_input={"path": r"runtime\out\update.txt", "mode": "safe"},
        result={"preview": "ready"},
        decision_trace={"reason": "task input refined"},
        store_path=task_system_path,
    )

    assert updated["status"] == "VALIDATED"
    assert updated["input"] == {"path": r"runtime\out\update.txt", "mode": "safe"}
    assert updated["result"] == {"preview": "ready"}
    assert updated["history"][-1]["event"] == "task_updated"
    assert updated["history"][-1]["details"]["decision_trace"] == {
        "reason": "task input refined"
    }

    system_context = get_context("system_context", root_dir=tmp_path)
    assert system_context["active_tasks"][0]["status"] == "VALIDATED"


def test_run_execution_auto_binds_to_persisted_task_and_history(
    monkeypatch, tmp_path: Path
) -> None:
    task_system_path = _configure_runtime(monkeypatch, tmp_path)
    created = task_store_module.create_task(
        task_type="WRITE_FILE",
        task_input={"path": r"runtime\out\task-layer.txt"},
        task_id="DF-TASK-LAYER-EXEC-V1",
        store_path=task_system_path,
    )
    task = task_factory_module.get_task(created["task_id"], store_path=task_system_path)
    assert task is not None
    task["status"] = "VALIDATED"
    task = task_factory_module.save_task(task, store_path=task_system_path)

    executed = execution_runner_module.run_execution(
        task,
        now=lambda: "2026-04-12T00:00:00Z",
        persist=lambda updated_task: task_factory_module.save_task(
            updated_task,
            store_path=task_system_path,
        ),
        executor=lambda _: build_action_result(
            status="completed",
            task_id="DF-TASK-LAYER-EXEC-V1",
            action_type="WRITE_FILE",
            result_payload={"path": r"runtime\out\task-layer.txt"},
            error_code="",
            error_message="",
            source="test_executor",
            diagnostic_message="write linked to task",
        ),
    )

    assert executed["status"] == "COMPLETED"
    persisted = task_factory_module.get_task(
        "DF-TASK-LAYER-EXEC-V1", store_path=task_system_path
    )
    assert persisted is not None
    assert persisted["status"] == "COMPLETED"
    assert persisted["result"]["task_id"] == "DF-TASK-LAYER-EXEC-V1"
    assert persisted["history"][-1]["event"] == "execution_completed"
    assert (
        persisted["history"][-1]["details"]["decision_trace"]["action_type"]
        == "WRITE_FILE"
    )

    system_context = get_context("system_context", root_dir=tmp_path)
    assert all(
        item["task_id"] != "DF-TASK-LAYER-EXEC-V1"
        for item in system_context.get("active_tasks", [])
    )


def test_missing_task_fails(monkeypatch, tmp_path: Path) -> None:
    task_system_path = _configure_runtime(monkeypatch, tmp_path)
    missing_task = _raw_task("")

    with pytest.raises(ValueError, match="task_id must not be empty"):
        task_store_module.ensure_task_for_execution(
            missing_task,
            store_path=task_system_path,
        )
