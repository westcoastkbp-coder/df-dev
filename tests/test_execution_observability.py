from __future__ import annotations

import json
from pathlib import Path

import app.execution.lead_estimate_decision as lead_estimate_decision_module
import app.execution.paths as paths_module
import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_state_store as task_state_store_module
import app.policy.policy_gate as policy_gate_module
import runtime.system_log as system_log_module
from app.orchestrator.task_queue import InMemoryTaskQueue
from functools import partial
from app.orchestrator.task_worker import process_next_queued_task as _process_next_queued_task
from tests.system_context import WORKING_SYSTEM_CONTEXT

process_next_queued_task = partial(_process_next_queued_task, system_context=WORKING_SYSTEM_CONTEXT)


def _configure_runtime(monkeypatch, tmp_path: Path) -> Path:
    task_store_path = tmp_path / "data" / "tasks.json"
    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store_module,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(paths_module, "TASKS_FILE", task_store_path)
    monkeypatch.setattr(lead_estimate_decision_module, "TASKS_FILE", task_store_path)
    task_factory_module.clear_task_runtime_store()
    return task_store_path


def _build_task(
    *,
    store_path: Path,
    task_id: str,
    intent: str,
    payload: dict[str, object],
    status: str = "pending",
) -> dict[str, object]:
    task = task_factory_module.create_task(
        {
            "task_id": task_id,
            "status": "created",
            "intent": intent,
            "payload": payload,
        }
        ,
        store_path=store_path,
    )
    task["intent"] = intent
    task["payload"] = dict(payload)
    task["status"] = status
    return task_factory_module.save_task(task, store_path=store_path)


def _trace_entries(system_log_file: Path) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    if not system_log_file.exists():
        return entries
    for line in system_log_file.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("event_type") != "trace":
            continue
        entries.append(dict(payload.get("details", {})))
    return entries


def test_valid_execution_trace_sequence_is_ordered(monkeypatch, tmp_path: Path) -> None:
    logs_dir = tmp_path / "runtime" / "logs"
    queue_file = tmp_path / "runtime" / "state" / "task_queue.json"
    task_log_file = logs_dir / "tasks.log"
    system_log_file = logs_dir / "system.log"
    policy_log_file = logs_dir / "policy.log"

    import app.orchestrator.task_queue as task_queue_module

    monkeypatch.setattr(task_queue_module, "TASK_QUEUE_FILE", queue_file)
    monkeypatch.setattr(task_queue_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(system_log_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)
    monkeypatch.setattr(policy_gate_module, "POLICY_LOG_FILE", policy_log_file)
    task_store_path = _configure_runtime(monkeypatch, tmp_path)

    queue = InMemoryTaskQueue()
    task_data = _build_task(
        store_path=task_store_path,
        task_id="DF-TRACE-VALID-V1",
        intent="lead_estimate_decision",
        payload={
            "workflow_type": "lead_estimate_decision",
            "lead_id": "lead-trace-001",
            "lead_data": {
                "project_type": "ADU",
                "scope_summary": "Detached ADU with pricing request",
                "contact_info": {"phone": "555-0100"},
                "lead_exists": True,
            },
        },
    )

    def fetch_task(task_id: str) -> dict[str, object] | None:
        return task_data if task_id == "DF-TRACE-VALID-V1" else None

    def persist(updated_task: dict[str, object]) -> None:
        task_data.update(updated_task)

    assert queue.enqueue_task("DF-TRACE-VALID-V1") is True

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
    traces = _trace_entries(system_log_file)
    assert traces
    trace = traces[-1]
    assert trace["run_id"] == "DF-TRACE-VALID-V1"
    assert trace["lead_id"] == "lead-trace-001"
    assert [step["step_name"] for step in trace["step_sequence"]] == [
        "input_validated",
        "decision_recorded",
        "decision_evaluated",
        "action_bound",
        "task_created",
        "reporting_generated",
    ]
    assert all(step["result_status"] == "success" for step in trace["step_sequence"])


def test_failure_trace_logs_failed_step(monkeypatch, tmp_path: Path) -> None:
    logs_dir = tmp_path / "runtime" / "logs"
    queue_file = tmp_path / "runtime" / "state" / "task_queue.json"
    task_log_file = logs_dir / "tasks.log"
    system_log_file = logs_dir / "system.log"
    policy_log_file = logs_dir / "policy.log"

    import app.orchestrator.task_queue as task_queue_module

    monkeypatch.setattr(task_queue_module, "TASK_QUEUE_FILE", queue_file)
    monkeypatch.setattr(task_queue_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(system_log_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)
    monkeypatch.setattr(policy_gate_module, "POLICY_LOG_FILE", policy_log_file)
    task_store_path = _configure_runtime(monkeypatch, tmp_path)

    queue = InMemoryTaskQueue()
    task_data = _build_task(
        store_path=task_store_path,
        task_id="DF-TRACE-FAIL-V1",
        intent="lead_estimate_decision",
        payload={
            "workflow_type": "lead_estimate_decision",
            "lead_id": "",
            "lead_data": {
                "lead_exists": True,
            },
        },
    )

    def fetch_task(task_id: str) -> dict[str, object] | None:
        return task_data if task_id == "DF-TRACE-FAIL-V1" else None

    def persist(updated_task: dict[str, object]) -> None:
        task_data.update(updated_task)

    assert queue.enqueue_task("DF-TRACE-FAIL-V1") is True

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
    assert executed_task["status"] == "FAILED"
    traces = _trace_entries(system_log_file)
    assert traces
    trace = traces[-1]
    assert trace["run_id"] == "DF-TRACE-FAIL-V1"
    assert [step["step_name"] for step in trace["step_sequence"]] == ["input_validated"]
    assert trace["step_sequence"][0]["result_status"] == "fail"

