from __future__ import annotations

import json
from pathlib import Path

import app.execution.lead_estimate_decision as lead_estimate_decision_module
import app.execution.paths as paths_module
import app.execution.determinism_replay as determinism_replay_module
import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_state_store as task_state_store_module
import app.policy.policy_gate as policy_gate_module
import runtime.system_log as system_log_module
from app.execution.determinism_replay import (
    append_determinism_replay_log,
    build_determinism_snapshot,
    compare_determinism_snapshots,
)
from app.execution.lead_estimate_contract import WORKFLOW_TYPE
from runtime.decision.evaluator import reset_runtime_decision_history
from runtime.decision.stability import reset_runtime_decision_stabilizer
from runtime.network.monitor import reset_network_monitor
from app.execution.task_schema import TASK_CONTRACT_VERSION
from app.orchestrator.task_queue import InMemoryTaskQueue
from functools import partial
from app.orchestrator.task_worker import (
    process_next_queued_task as _process_next_queued_task,
)
from tests.system_context import WORKING_SYSTEM_CONTEXT

process_next_queued_task = partial(
    _process_next_queued_task, system_context=WORKING_SYSTEM_CONTEXT
)


def _configure_runtime(
    monkeypatch,
    tmp_path: Path,
    *,
    determinism_log_file: Path,
) -> tuple[Path, Path, Path]:
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
    monkeypatch.setattr(
        determinism_replay_module,
        "DETERMINISM_REPLAY_LOG_FILE",
        determinism_log_file,
    )
    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store_module,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(paths_module, "TASKS_FILE", task_store_path)
    monkeypatch.setattr(lead_estimate_decision_module, "TASKS_FILE", task_store_path)
    task_factory_module.clear_task_runtime_store()
    return task_store_path, system_log_file, determinism_log_file


def _trace_steps(system_log_file: Path) -> list[dict[str, object]]:
    if not system_log_file.exists():
        return []
    trace_entries: list[dict[str, object]] = []
    for line in system_log_file.read_text(encoding="utf-8").splitlines():
        payload = json.loads(line)
        if payload.get("event_type") != "trace":
            continue
        trace_entries.append(dict(payload.get("details", {})))
    return list(trace_entries[-1].get("step_sequence", [])) if trace_entries else []


def _build_task(*, store_path: Path, task_id: str) -> dict[str, object]:
    return task_factory_module.save_task(
        {
            "task_contract_version": TASK_CONTRACT_VERSION,
            "task_id": task_id,
            "created_at": "2026-04-04T00:00:00Z",
            "intent": WORKFLOW_TYPE,
            "payload": {
                "workflow_type": WORKFLOW_TYPE,
                "lead_id": "lead-determinism-001",
                "lead_data": {
                    "project_type": "ADU",
                    "scope_summary": "Detached ADU",
                    "contact_info": {"phone": "555-0100"},
                    "lead_exists": True,
                    "lead_invalid": True,
                },
            },
            "status": "VALIDATED",
            "notes": [],
            "history": [],
            "interaction_id": task_id,
            "job_id": task_id,
            "trace_id": task_id,
        },
        store_path=store_path,
    )


def _run_once(
    *, monkeypatch, tmp_path: Path, run_index: int
) -> tuple[dict[str, object], Path]:
    shared_determinism_log_file = (
        tmp_path / "runtime" / "logs" / "determinism_replay.jsonl"
    )
    reset_runtime_decision_history()
    reset_runtime_decision_stabilizer()
    reset_network_monitor()
    store_path, system_log_file, determinism_log_file = _configure_runtime(
        monkeypatch,
        tmp_path / f"run_{run_index}",
        determinism_log_file=shared_determinism_log_file,
    )
    queue = InMemoryTaskQueue()
    task = _build_task(
        store_path=store_path,
        task_id="DF-DETERMINISM-REPLAY-V1",
    )

    def fetch_task(task_id: str) -> dict[str, object] | None:
        return task if task_id == "DF-DETERMINISM-REPLAY-V1" else None

    def persist(updated_task: dict[str, object]) -> None:
        task.update(updated_task)

    assert queue.enqueue_task(task["task_id"]) is True
    executed = process_next_queued_task(
        queue=queue,
        now=lambda: "2026-04-04T00:00:00Z",
        fetch_task=fetch_task,
        persist=persist,
        timeout=0.0,
        telemetry_collector=lambda: {},
        network_snapshot_collector=lambda: {},
    )
    assert executed is not None
    snapshot = build_determinism_snapshot(
        task_data=executed,
        trace_sequence=_trace_steps(system_log_file),
    )
    append_determinism_replay_log(
        {
            "scenario": WORKFLOW_TYPE,
            "run_index": run_index,
            "task_id": "DF-DETERMINISM-REPLAY-V1",
            "snapshot": snapshot,
        },
        log_path=determinism_log_file,
    )
    return snapshot, determinism_log_file


def test_lead_estimate_decision_is_identical_across_10_replays(
    monkeypatch, tmp_path: Path
) -> None:
    baseline_snapshot: dict[str, object] | None = None
    determinism_log_file: Path | None = None
    mismatches: list[str] = []

    for run_index in range(1, 11):
        snapshot, determinism_log_file = _run_once(
            monkeypatch=monkeypatch,
            tmp_path=tmp_path,
            run_index=run_index,
        )
        if baseline_snapshot is None:
            baseline_snapshot = snapshot
            continue
        matches, mismatched_field = compare_determinism_snapshots(
            baseline_snapshot,
            snapshot,
        )
        append_determinism_replay_log(
            {
                "scenario": WORKFLOW_TYPE,
                "run_index": run_index,
                "comparison_status": "match" if matches else "critical_failure",
                "mismatched_field": mismatched_field,
            },
            log_path=determinism_log_file,
        )
        if not matches:
            mismatches.append(f"run_{run_index}:{mismatched_field}")

    assert baseline_snapshot is not None
    assert determinism_log_file is not None
    log_lines = [
        json.loads(line)
        for line in determinism_log_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert len([line for line in log_lines if "snapshot" in line]) == 10
    assert all(
        line.get("comparison_status") == "match"
        for line in log_lines
        if "comparison_status" in line
    ), (
        f"critical failure: deterministic replay mismatch detected: {', '.join(mismatches)}"
    )
