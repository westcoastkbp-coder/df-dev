from __future__ import annotations

import json
from pathlib import Path

from app.execution.action_result import build_action_result
import app.execution.lead_estimate_decision as lead_estimate_decision_module
import app.execution.paths as paths_module
import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_state_store as task_state_store_module
import app.policy.policy_gate as policy_gate_module
import runtime.system_log as system_log_module
from app.execution.execution_replay import replay_execution
from app.orchestrator.task_queue import InMemoryTaskQueue
from functools import partial
from app.orchestrator.task_worker import process_next_queued_task as _process_next_queued_task
from tests.system_context import WORKING_SYSTEM_CONTEXT

process_next_queued_task = partial(_process_next_queued_task, system_context=WORKING_SYSTEM_CONTEXT)


def _configure_runtime(monkeypatch, tmp_path: Path) -> tuple[Path, Path]:
    logs_dir = tmp_path / "runtime" / "logs"
    queue_file = tmp_path / "runtime" / "state" / "task_queue.json"
    task_log_file = logs_dir / "tasks.log"
    system_log_file = logs_dir / "system.log"
    policy_log_file = logs_dir / "policy.log"
    task_store_path = tmp_path / "data" / "tasks.json"

    import app.orchestrator.task_queue as task_queue_module

    monkeypatch.setattr(task_queue_module, "TASK_QUEUE_FILE", queue_file)
    monkeypatch.setattr(task_queue_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(system_log_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)
    monkeypatch.setattr(policy_gate_module, "POLICY_LOG_FILE", policy_log_file)
    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(task_state_store_module, "TASK_STATE_DB_FILE", Path("runtime/state/task_state.sqlite3"))
    monkeypatch.setattr(paths_module, "TASKS_FILE", task_store_path)
    monkeypatch.setattr(lead_estimate_decision_module, "TASKS_FILE", task_store_path)
    monkeypatch.setattr(task_factory_module, "TASK_SYSTEM_FILE", task_store_path)
    task_factory_module.clear_task_runtime_store()
    return task_store_path, system_log_file


def _trace_steps(system_log_file: Path) -> list[dict[str, object]]:
    entries: list[dict[str, object]] = []
    if not system_log_file.exists():
        return entries
    for line in system_log_file.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("event_type") != "trace":
            continue
        entries.append(dict(payload.get("details", {})))
    return list(entries[-1].get("step_sequence", [])) if entries else []


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
        },
        store_path=store_path,
    )
    task["intent"] = intent
    task["payload"] = dict(payload)
    task["status"] = status
    return task_factory_module.save_task(task, store_path=store_path)


def test_replay_execution_matches_stored_run_without_new_side_effects(monkeypatch, tmp_path: Path) -> None:
    task_store_path, system_log_file = _configure_runtime(monkeypatch, tmp_path)

    queue = InMemoryTaskQueue()
    task_data = _build_task(
        store_path=task_store_path,
        task_id="DF-REPLAY-MATCH-V1",
        intent="lead_estimate_decision",
        payload={
            "workflow_type": "lead_estimate_decision",
            "lead_id": "lead-replay-001",
            "lead_data": {
                "project_type": "ADU",
                "scope_summary": "Detached ADU with pricing request",
                "contact_info": {"phone": "555-0100"},
                "lead_exists": True,
            },
        },
    )

    def fetch_task(task_id: str) -> dict[str, object] | None:
        return task_data if task_id == "DF-REPLAY-MATCH-V1" else None

    def persist(updated_task: dict[str, object]) -> None:
        task_data.update(updated_task)

    def fake_executor(task_data: dict[str, object]) -> dict[str, object]:
        decision = lead_estimate_decision_module.resolve_estimate_decision(
            task_id=str(task_data.get("task_id", "")).strip(),
            payload=dict(task_data.get("payload", {}) or {}),
        )
        return build_action_result(
            status="completed",
            task_id=task_data.get("task_id"),
            action_type=str(task_data.get("intent", "")).strip().upper(),
            result_payload={
                "decision": decision,
                "summary": "replay seed",
            },
            error_code="",
            error_message="",
            source="test_execution_replay",
            diagnostic_message="replay seed",
        )

    fake_executor.__module__ = "runtime.pipeline.managed_execution"

    assert queue.enqueue_task("DF-REPLAY-MATCH-V1") is True

    executed_task = process_next_queued_task(
        queue=queue,
        now=lambda: "2026-04-04T00:00:00Z",
        fetch_task=fetch_task,
        persist=persist,
        executor=fake_executor,
        timeout=0.0,
        telemetry_collector=lambda: {},
        network_snapshot_collector=lambda: {},
    )

    assert executed_task is not None
    task_factory_module.save_task(executed_task, store_path=task_store_path)
    before_tasks = task_factory_module.load_tasks(task_store_path)
    before_count = len(before_tasks)
    report = replay_execution(
        "DF-REPLAY-MATCH-V1",
        stored_trace_sequence=_trace_steps(system_log_file),
        store_path=task_store_path,
    )
    after_tasks = task_factory_module.load_tasks(task_store_path)

    assert report == {
        "run_id": "DF-REPLAY-MATCH-V1",
        "replay_status": "match",
        "mismatched_step": "",
        "notes": "dry-run replay matched stored execution",
    }
    assert len(after_tasks) == before_count
    assert after_tasks == before_tasks


def test_replay_execution_reports_trace_mismatch(monkeypatch, tmp_path: Path) -> None:
    task_store_path, system_log_file = _configure_runtime(monkeypatch, tmp_path)

    queue = InMemoryTaskQueue()
    task_data = _build_task(
        store_path=task_store_path,
        task_id="DF-REPLAY-MISMATCH-V1",
        intent="lead_estimate_decision",
        payload={
            "workflow_type": "lead_estimate_decision",
            "lead_id": "lead-replay-002",
            "lead_data": {
                "project_type": "ADU",
                "scope_summary": "Detached ADU with pricing request",
                "contact_info": {"phone": "555-0101"},
                "lead_exists": True,
            },
        },
    )

    def fetch_task(task_id: str) -> dict[str, object] | None:
        return task_data if task_id == "DF-REPLAY-MISMATCH-V1" else None

    def persist(updated_task: dict[str, object]) -> None:
        task_data.update(updated_task)

    assert queue.enqueue_task("DF-REPLAY-MISMATCH-V1") is True

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
    task_factory_module.save_task(executed_task, store_path=task_store_path)
    stored_trace = _trace_steps(system_log_file)
    mismatched_trace = list(stored_trace[:-1])
    report = replay_execution(
        "DF-REPLAY-MISMATCH-V1",
        stored_trace_sequence=mismatched_trace,
        store_path=task_store_path,
    )

    assert report["run_id"] == "DF-REPLAY-MISMATCH-V1"
    assert report["replay_status"] == "mismatch"
    assert report["mismatched_step"] == "trace_length"
    assert report["notes"] == "trace sequence diverged"

