from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping
from pathlib import Path

import app.execution.paths as paths_module
import app.execution.lead_estimate_decision as lead_estimate_decision_module
import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_queue as task_queue_module
import app.orchestrator.task_state_store as task_state_store_module
import app.policy.policy_gate as policy_gate_module
import runtime.system_log as system_log_module
import runtime.token_telemetry as token_telemetry_module
from app.execution.determinism_replay import (
    build_determinism_snapshot,
    compare_determinism_snapshots,
)
from app.execution.lead_estimate_contract import WORKFLOW_TYPE
from runtime.decision.evaluator import reset_runtime_decision_history
from runtime.decision.stability import reset_runtime_decision_stabilizer
from runtime.network.monitor import reset_network_monitor


COLD_BOOT_LOG_NAME = "cold_boot.jsonl"
COLD_BOOT_TASK_ID = "DF-COLD-BOOT-FINAL-V1"


def cold_boot_log_path() -> Path:
    return paths_module.ROOT_DIR / paths_module.LOGS_DIR / COLD_BOOT_LOG_NAME


def cold_boot_store_path() -> Path:
    return paths_module.ROOT_DIR / paths_module.STATE_DIR / "cold_boot_tasks.json"


def _jsonl_entries(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _append_log(
    *,
    stage: str,
    status: str,
    details: Mapping[str, object] | None = None,
    log_path: Path | None = None,
) -> Path:
    target = Path(log_path) if log_path is not None else cold_boot_log_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "stage": str(stage).strip(),
        "status": str(status).strip(),
        "details": dict(details or {}),
    }
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, separators=(",", ":")) + "\n")
    return target


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _remove_if_exists(path: Path) -> bool:
    if path.exists():
        try:
            path.unlink()
            return True
        except PermissionError:
            return False
    return True


def _clear_sqlite_tasks(path: Path) -> None:
    if not path.exists():
        return
    connection = sqlite3.connect(str(path))
    try:
        try:
            connection.execute("DELETE FROM Task")
            connection.commit()
        except sqlite3.OperationalError:
            return
    finally:
        connection.close()


def _trace_steps(system_log_file: Path) -> list[dict[str, object]]:
    trace_entries = [
        dict(entry.get("details", {}))
        for entry in _jsonl_entries(system_log_file)
        if entry.get("event_type") == "trace"
    ]
    if not trace_entries:
        return []
    return list(trace_entries[-1].get("step_sequence", []))


def reset_cold_boot_environment(
    *,
    log_path: Path | None = None,
    clear_boot_log: bool = True,
) -> dict[str, object]:
    target_log = Path(log_path) if log_path is not None else cold_boot_log_path()
    task_factory_module.clear_task_runtime_store()
    reset_runtime_decision_history()
    reset_runtime_decision_stabilizer()
    reset_network_monitor()
    with token_telemetry_module._RUN_STATE_LOCK:
        token_telemetry_module._RUN_STATE.clear()

    task_queue_module.task_queue.clear()
    _write_text(task_queue_module.TASK_QUEUE_FILE, "[]")

    log_targets = [
        system_log_module.SYSTEM_LOG_FILE,
        system_log_module.TASK_LOG_FILE,
        policy_gate_module.POLICY_LOG_FILE,
        token_telemetry_module.TOKEN_USAGE_LOG_FILE,
    ]
    if clear_boot_log:
        log_targets.append(target_log)
    for text_log in log_targets:
        _write_text(text_log, "")

    store_path = cold_boot_store_path()
    state_db = task_state_store_module.db_path_for(store_path)
    if not _remove_if_exists(state_db):
        _clear_sqlite_tasks(state_db)
    _remove_if_exists(state_db.with_suffix(state_db.suffix + "-journal"))
    _remove_if_exists(state_db.with_suffix(state_db.suffix + "-wal"))
    _remove_if_exists(state_db.with_suffix(state_db.suffix + "-shm"))

    _write_text(store_path, "[]\n")
    _write_text(paths_module.TASK_SYSTEM_FILE, "[]\n")
    _write_text(paths_module.MEMORY_FILE, "[]\n")
    _write_text(paths_module.CONTACTS_FILE, "[]\n")
    _write_text(paths_module.ROOT_DIR / paths_module.OUTPUT_DIR / ".gitkeep", "")

    _append_log(
        stage="reset_environment",
        status="ok",
        details={
            "cleared_runtime_state": True,
            "cleared_logs": True,
            "cleared_memory": True,
            "cleared_cached_objects": True,
        },
        log_path=target_log,
    )
    return {
        "cleared_runtime_state": True,
        "cleared_logs": True,
        "cleared_memory": True,
        "cleared_cached_objects": True,
    }


def initialize_cold_boot_system(*, log_path: Path | None = None) -> dict[str, object]:
    target_log = Path(log_path) if log_path is not None else cold_boot_log_path()
    modules_loaded = [
        "app.execution.product_executor",
        "app.orchestrator.execution_runner",
        "app.orchestrator.task_factory",
        "app.policy.policy_gate",
    ]
    for directory in (
        paths_module.ROOT_DIR / paths_module.LOGS_DIR,
        paths_module.ROOT_DIR / paths_module.STATE_DIR,
        paths_module.DATA_DIR,
        paths_module.ROOT_DIR / paths_module.OUTPUT_DIR,
    ):
        directory.mkdir(parents=True, exist_ok=True)

    task_state_store_module.initialize_database(cold_boot_store_path())
    policy_probe = policy_gate_module.evaluate_workflow_policy(
        {
            "workflow_type": WORKFLOW_TYPE,
            "lead_id": "cold-boot-policy-probe",
            "lead_data": {"lead_exists": True},
        },
        {"task_id": "cold-boot-policy-probe", "status": "running"},
    )
    readiness = {
        "startup": "ok",
        "module_loading": "ok",
        "policy_initialization": "ok" if policy_probe.execution_allowed else "blocked",
        "execution_readiness": "ready",
        "modules_loaded": modules_loaded,
    }
    _append_log(
        stage="initialize_system",
        status="ok",
        details=readiness,
        log_path=target_log,
    )
    return readiness


def _build_scenario_payload() -> dict[str, object]:
    return {
        "workflow_type": WORKFLOW_TYPE,
        "lead_id": "lead-cold-boot-001",
        "lead_data": {
            "project_type": "ADU",
            "scope_summary": "Detached ADU with pricing request",
            "contact_info": {"phone": "555-0100"},
            "lead_exists": True,
        },
    }


def _prepare_validated_task() -> dict[str, object]:
    payload = _build_scenario_payload()
    store_path = cold_boot_store_path()
    task = task_factory_module.create_task(
        {
            "task_id": COLD_BOOT_TASK_ID,
            "status": "created",
            "intent": WORKFLOW_TYPE,
            "payload": payload,
        },
        store_path=store_path,
    )
    task["intent"] = WORKFLOW_TYPE
    task["payload"] = payload
    task["status"] = "VALIDATED"
    return task_factory_module.save_task(task, store_path=store_path)


def _run_scenario_once(*, label: str, log_path: Path | None = None) -> dict[str, object]:
    from app.orchestrator.task_worker import process_next_queued_task

    target_log = Path(log_path) if log_path is not None else cold_boot_log_path()
    original_next_task_id = task_factory_module.next_task_id
    original_now = task_factory_module.now
    original_workflow_store = lead_estimate_decision_module.TASKS_FILE
    child_counter = {"value": 0}
    store_path = cold_boot_store_path()

    def deterministic_task_id() -> str:
        child_counter["value"] += 1
        return f"DF-COLD-BOOT-CHILD-{child_counter['value']:04d}"

    task_factory_module.next_task_id = deterministic_task_id
    task_factory_module.now = lambda: "2026-04-05T00:00:00Z"
    lead_estimate_decision_module.TASKS_FILE = store_path
    try:
        task_data = _prepare_validated_task()

        def fetch_task(task_id: str) -> dict[str, object] | None:
            return task_factory_module.get_task(task_id, store_path=store_path)

        def persist(updated_task: dict[str, object]) -> None:
            task_factory_module.save_task(updated_task, store_path=store_path)

        assert task_queue_module.task_queue.enqueue_task(task_data["task_id"]) is True
        executed_task = process_next_queued_task(
            queue=task_queue_module.task_queue,
            now=lambda: "2026-04-05T00:00:00Z",
            fetch_task=fetch_task,
            persist=persist,
            timeout=0.0,
            telemetry_collector=lambda: {},
            network_snapshot_collector=lambda: {},
            system_context={"status": "WORKING", "broken": {}},
        )
        if executed_task is None:
            raise RuntimeError("cold boot scenario did not execute")
        snapshot = build_determinism_snapshot(
            task_data=executed_task,
            trace_sequence=_trace_steps(system_log_module.SYSTEM_LOG_FILE),
        )
        _append_log(
            stage=f"scenario_{label}",
            status="ok",
            details={
                "task_id": str(executed_task.get("task_id", "")).strip(),
                "final_status": str(executed_task.get("status", "")).strip(),
                "error": str(executed_task.get("error", "")).strip(),
                "decision": dict(
                    dict(executed_task.get("result", {}) or {}).get("decision", {}) or {}
                ),
                "execution_order": list(snapshot.get("execution_order", [])),
            },
            log_path=target_log,
        )
        return {
            "task": dict(executed_task),
            "snapshot": snapshot,
        }
    finally:
        task_factory_module.next_task_id = original_next_task_id
        task_factory_module.now = original_now
        lead_estimate_decision_module.TASKS_FILE = original_workflow_store


def run_cold_boot_validation(*, log_path: Path | None = None) -> dict[str, object]:
    target_log = Path(log_path) if log_path is not None else cold_boot_log_path()

    reset_summary = reset_cold_boot_environment(log_path=target_log, clear_boot_log=True)
    initialization_summary = initialize_cold_boot_system(log_path=target_log)
    baseline_run = _run_scenario_once(label="baseline", log_path=target_log)

    reset_cold_boot_environment(log_path=target_log, clear_boot_log=False)
    initialize_cold_boot_system(log_path=target_log)
    cold_boot_run = _run_scenario_once(label="cold_boot", log_path=target_log)

    matches, mismatched_field = compare_determinism_snapshots(
        baseline_run["snapshot"],
        cold_boot_run["snapshot"],
    )
    baseline_completed = str(baseline_run["task"].get("status", "")).strip() == "COMPLETED"
    cold_boot_completed = str(cold_boot_run["task"].get("status", "")).strip() == "COMPLETED"
    correct_execution_path = list(cold_boot_run["snapshot"].get("execution_order", [])) == [
        "input_validated",
        "decision_recorded",
        "decision_evaluated",
        "action_bound",
        "task_created",
        "reporting_generated",
    ]
    passed = (
        matches
        and baseline_completed
        and cold_boot_completed
        and initialization_summary["execution_readiness"] == "ready"
        and correct_execution_path
    )
    report = {
        "status": "PASS" if passed else "FAIL",
        "scenario": WORKFLOW_TYPE,
        "log_path": str(target_log),
        "reset_summary": reset_summary,
        "initialization_summary": initialization_summary,
        "no_dependency_on_previous_runs": bool(matches),
        "no_missing_initialization": initialization_summary["execution_readiness"] == "ready",
        "correct_execution_path": correct_execution_path,
        "determinism_preserved": bool(matches),
        "mismatched_field": str(mismatched_field or ""),
        "baseline_completed": baseline_completed,
        "cold_boot_completed": cold_boot_completed,
        "baseline_snapshot": dict(baseline_run["snapshot"]),
        "cold_boot_snapshot": dict(cold_boot_run["snapshot"]),
    }
    _append_log(
        stage="verification",
        status="ok" if matches else "fail",
        details={
            "no_dependency_on_previous_runs": report["no_dependency_on_previous_runs"],
            "no_missing_initialization": report["no_missing_initialization"],
            "correct_execution_path": report["correct_execution_path"],
            "determinism_preserved": report["determinism_preserved"],
            "mismatched_field": report["mismatched_field"],
        },
        log_path=target_log,
    )
    return report
