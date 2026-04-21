from __future__ import annotations

from pathlib import Path
import json

import pytest

from app.execution.action_result import build_action_result
import app.execution.execution_boundary as execution_boundary_module
import app.execution.paths as paths_module
import app.product.runner as product_runner_module
from app.execution.lead_estimate_contract import WORKFLOW_TYPE
from app.execution.task_schema import TASK_CONTRACT_VERSION
from app.orchestrator import execution_runner
from app.orchestrator import task_factory
from app.orchestrator import task_state_store
from app.product.runner import dispatch_action_trigger


def _configure_state_backend(monkeypatch, tmp_path: Path) -> Path:
    store_path = tmp_path / "data" / "task_system.json"
    monkeypatch.setattr(task_state_store, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(
        execution_boundary_module,
        "EXECUTION_BOUNDARY_VIOLATIONS_LOG",
        tmp_path / "runtime" / "logs" / "execution_boundary_violations.jsonl",
    )
    monkeypatch.setattr(paths_module, "TASKS_FILE", store_path)
    monkeypatch.setattr(task_factory, "TASK_SYSTEM_FILE", store_path)
    monkeypatch.setattr(product_runner_module, "ROOT_DIR", tmp_path)
    task_factory.clear_task_runtime_store()
    return store_path


def _read_violation_log(tmp_path: Path) -> list[dict[str, object]]:
    log_file = tmp_path / "runtime" / "logs" / "execution_boundary_violations.jsonl"
    if not log_file.exists():
        return []
    return [
        json.loads(line)
        for line in log_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _build_task(*, task_id: str, intent: str = "new_lead") -> dict[str, object]:
    return {
        "task_contract_version": TASK_CONTRACT_VERSION,
        "task_id": task_id,
        "created_at": "2026-04-04T00:00:00Z",
        "intent": intent,
        "payload": {"summary": "Detached ADU"},
        "status": "VALIDATED",
        "notes": [],
        "history": [],
        "interaction_id": task_id,
        "job_id": task_id,
        "trace_id": task_id,
    }


def test_run_execution_fails_before_persist_or_executor_when_task_contract_is_invalid() -> (
    None
):
    persist_calls = 0
    executor_calls = 0

    def persist(_: dict[str, object]) -> None:
        nonlocal persist_calls
        persist_calls += 1

    def executor(_: dict[str, object]) -> dict[str, object]:
        nonlocal executor_calls
        executor_calls += 1
        return {"summary": "should not run"}

    with pytest.raises(
        ValueError,
        match="task missing required fields: created_at, history, notes, task_contract_version",
    ):
        execution_runner.run_execution(
            {
                "task_id": "DF-BOUNDARY-INVALID-TASK-V1",
                "status": "pending",
                "intent": "new_lead",
                "payload": {"summary": "invalid"},
            },
            now=lambda: "2026-04-04T00:00:00Z",
            persist=persist,
            executor=executor,
        )

    assert persist_calls == 0
    assert executor_calls == 0


def test_invalid_action_result_blocks_deeper_execution(
    monkeypatch, tmp_path: Path
) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    task = task_factory.save_task(
        {
            "task_contract_version": TASK_CONTRACT_VERSION,
            "task_id": "DF-BOUNDARY-WORKFLOW-V1",
            "created_at": "2026-04-04T00:00:00Z",
            "intent": WORKFLOW_TYPE,
            "payload": {
                "workflow_type": WORKFLOW_TYPE,
                "lead_id": "lead-boundary-001",
                "lead_data": {
                    "project_type": "ADU",
                    "scope_summary": "Detached ADU",
                    "contact_info": {"phone": "555-0100"},
                    "lead_exists": True,
                },
            },
            "status": "VALIDATED",
            "notes": [],
            "history": [],
            "interaction_id": "DF-BOUNDARY-WORKFLOW-V1",
            "job_id": "DF-BOUNDARY-WORKFLOW-V1",
            "trace_id": "DF-BOUNDARY-WORKFLOW-V1",
        },
        store_path=store_path,
    )

    persisted_snapshots: list[dict[str, object]] = []

    def persist(updated_task: dict[str, object]) -> None:
        persisted_snapshots.append(dict(updated_task))
        task_factory.save_task(updated_task, store_path=store_path)

    executed = execution_runner.run_execution(
        task,
        now=lambda: "2026-04-04T00:00:00Z",
        persist=persist,
        executor=lambda _: {
            "status": "completed",
            "action_type": WORKFLOW_TYPE.upper(),
            "result_payload": {
                "decision": {
                    "decision": "create_estimate",
                    "confidence": "high",
                    "next_step": "create_estimate_task",
                }
            },
            "error_code": "",
            "error_message": "",
            "source": "test_executor",
            "decision_trace": {
                "reason": "workflow result emitted",
                "context_used": "task_id=DF-BOUNDARY-WORKFLOW-V1; source=test_executor",
                "action_type": WORKFLOW_TYPE.upper(),
                "policy_result": "allowed",
                "confidence": "medium",
            },
            "unexpected_field": "blocked",
        },
    )

    assert executed["status"] == "FAILED"
    assert (
        executed["error"]
        == "invalid action result: action result contains unsupported fields: unexpected_field"
    )
    assert executed["result"]["status"] == "invalid_action_result"
    assert [snapshot["status"] for snapshot in persisted_snapshots] == [
        "EXECUTING",
        "FAILED",
    ]
    assert len(task_factory.load_tasks(store_path)) == 1


def test_workflow_validation_fails_before_any_persist_or_executor(
    monkeypatch, tmp_path: Path
) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    task = task_factory.save_task(
        {
            "task_contract_version": TASK_CONTRACT_VERSION,
            "task_id": "DF-BOUNDARY-WORKFLOW-VALIDATION-V1",
            "created_at": "2026-04-04T00:00:00Z",
            "intent": WORKFLOW_TYPE,
            "payload": {
                "workflow_type": WORKFLOW_TYPE,
                "lead_id": "",
                "lead_data": {
                    "project_type": "ADU",
                    "scope_summary": "Detached ADU",
                    "contact_info": {"phone": "555-0100"},
                    "lead_exists": True,
                },
            },
            "status": "VALIDATED",
            "notes": [],
            "history": [],
            "interaction_id": "DF-BOUNDARY-WORKFLOW-VALIDATION-V1",
            "job_id": "DF-BOUNDARY-WORKFLOW-VALIDATION-V1",
            "trace_id": "DF-BOUNDARY-WORKFLOW-VALIDATION-V1",
        },
        store_path=store_path,
    )

    persist_calls = 0
    executor_calls = 0

    def persist(_: dict[str, object]) -> None:
        nonlocal persist_calls
        persist_calls += 1

    def executor(_: dict[str, object]) -> dict[str, object]:
        nonlocal executor_calls
        executor_calls += 1
        return {"summary": "should not run"}

    with pytest.raises(
        ValueError, match="policy gate blocked workflow: lead_id must not be empty"
    ):
        execution_runner.run_execution(
            task,
            now=lambda: "2026-04-04T00:00:00Z",
            persist=persist,
            executor=executor,
        )

    assert persist_calls == 0
    assert executor_calls == 0
    restored = task_factory.get_task("DF-BOUNDARY-WORKFLOW-VALIDATION-V1", store_path)
    assert restored is not None
    assert restored["status"] == "VALIDATED"


def test_direct_action_call_is_blocked_and_logged(monkeypatch, tmp_path: Path) -> None:
    _configure_state_backend(monkeypatch, tmp_path)

    result = dispatch_action_trigger(
        {
            "action_type": "WRITE_FILE",
            "payload": {
                "task_id": "DF-BOUNDARY-DIRECT-ACTION-V1",
                "path": r"runtime\out\direct.txt",
                "content": "blocked",
            },
        },
        task_state={"task_id": "DF-BOUNDARY-DIRECT-ACTION-V1", "status": "EXECUTING"},
    )

    assert result["status"] == "execution_boundary_violation"
    assert result["reason"] == "direct_action_call_blocked"
    logged = _read_violation_log(tmp_path)
    assert logged[-1]["status"] == "execution_boundary_violation"
    assert logged[-1]["details"]["reason"] == "direct_action_call_blocked"


def test_execution_without_task_is_blocked(monkeypatch, tmp_path: Path) -> None:
    _configure_state_backend(monkeypatch, tmp_path)

    with execution_boundary_module.execution_boundary(
        {"intent": "new_lead"}, policy_validated=True
    ):
        result = dispatch_action_trigger(
            {
                "action_type": "WRITE_FILE",
                "payload": {
                    "task_id": "DF-BOUNDARY-NO-TASK-V1",
                    "path": r"runtime\out\missing-task.txt",
                    "content": "blocked",
                },
            },
            task_state={"task_id": "DF-BOUNDARY-NO-TASK-V1", "status": "EXECUTING"},
        )

    assert result["status"] == "execution_boundary_violation"
    assert result["reason"] == "execution_without_task_blocked"


def test_execution_without_policy_is_blocked(monkeypatch, tmp_path: Path) -> None:
    _configure_state_backend(monkeypatch, tmp_path)

    with execution_boundary_module.execution_boundary(
        {"task_id": "DF-BOUNDARY-NO-POLICY-V1", "intent": "new_lead"},
        policy_validated=False,
    ):
        result = dispatch_action_trigger(
            {
                "action_type": "WRITE_FILE",
                "payload": {
                    "task_id": "DF-BOUNDARY-NO-POLICY-V1",
                    "path": r"runtime\out\missing-policy.txt",
                    "content": "blocked",
                },
            },
            task_state={"task_id": "DF-BOUNDARY-NO-POLICY-V1", "status": "EXECUTING"},
        )

    assert result["status"] == "execution_boundary_violation"
    assert result["reason"] == "execution_without_policy_blocked"


def test_valid_execution_still_runs_through_execution_runner(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_state_backend(monkeypatch, tmp_path)
    task = _build_task(task_id="DF-BOUNDARY-VALID-EXECUTION-V1")
    persisted_statuses: list[str] = []

    def persist(updated_task: dict[str, object]) -> None:
        persisted_statuses.append(str(updated_task.get("status", "")))

    executed = execution_runner.run_execution(
        task,
        now=lambda: "2026-04-04T00:00:00Z",
        persist=persist,
        executor=lambda task_data: {
            **build_action_result(
                status="completed",
                task_id=str(task_data.get("task_id", "")),
                action_type="NEW_LEAD",
                result_payload={
                    "action": dispatch_action_trigger(
                        {
                            "action_type": "WRITE_FILE",
                            "payload": {
                                "task_id": str(task_data.get("task_id", "")),
                                "path": r"runtime\out\valid.txt",
                                "content": "created by execution runner",
                            },
                        },
                        task_state=task_data,
                    )
                },
                error_code="",
                error_message="",
                source="test_executor",
                diagnostic_message="execution completed",
            ),
        },
    )

    assert executed["status"] == "COMPLETED"
    assert executed["result"]["result_payload"]["action"]["status"] == "completed"
    assert persisted_statuses == ["EXECUTING", "COMPLETED"]
