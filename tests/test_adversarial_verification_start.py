from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import app.execution.paths as paths_module
import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_lifecycle as task_lifecycle_module
import app.orchestrator.task_memory as task_memory_module
import app.orchestrator.task_queue as task_queue_module
import app.orchestrator.task_state_store as task_state_store_module
import app.policy.policy_gate as policy_gate_module
import runtime.system_log as system_log_module
import runtime.token_efficiency as token_efficiency_module
import runtime.token_telemetry as token_telemetry_module
from app.execution.action_result import build_action_result
from app.execution.determinism_replay import build_determinism_snapshot
from app.execution.real_lead_contract import MISSING_INPUT_FOLLOWUP_WORKFLOW_TYPE
from app.orchestrator.execution_runner import run_execution
from app.orchestrator.task_state_store import (
    StatePersistenceError,
    build_state_persist_failure,
    build_execution_key,
    claim_execution_record,
    complete_execution_record,
)
from runtime.decision.evaluator import reset_runtime_decision_history
from runtime.decision.stability import reset_runtime_decision_stabilizer
from runtime.network.monitor import reset_network_monitor


FIXTURE_FILE = Path(__file__).parent / "fixtures" / "adversarial_task.json"


@dataclass(slots=True)
class AdversarialRuntime:
    root_dir: Path
    store_path: Path
    effect_log_file: Path
    system_log_file: Path


class SequencedClock:
    def __init__(self) -> None:
        self._values = [
            "2026-04-05T00:00:00Z",
            "2026-04-05T00:00:01Z",
            "2026-04-05T00:00:02Z",
            "2026-04-05T00:00:03Z",
        ]
        self._index = 0

    def __call__(self) -> str:
        if self._index < len(self._values):
            value = self._values[self._index]
            self._index += 1
            return value
        return self._values[-1]


class CrashOnFinalPersist:
    def __init__(self) -> None:
        self.triggered = False

    def __call__(self, task_data: dict[str, object]) -> None:
        if str(task_data.get("status", "")).strip() == "COMPLETED" and not self.triggered:
            self.triggered = True
            raise StatePersistenceError(
                build_state_persist_failure(
                    task_id=task_data.get("task_id"),
                    operation="write_task",
                )
            )
        task_factory_module.save_task(task_data, store_path=task_factory_module.TASK_SYSTEM_FILE)


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _load_task_fixture() -> dict[str, object]:
    return json.loads(FIXTURE_FILE.read_text(encoding="utf-8"))


def _reset_runtime_globals() -> None:
    task_factory_module.clear_task_runtime_store()
    task_queue_module.task_queue.clear()
    reset_runtime_decision_history()
    reset_runtime_decision_stabilizer()
    reset_network_monitor()
    with token_telemetry_module._RUN_STATE_LOCK:
        token_telemetry_module._RUN_STATE.clear()


def _assert_no_runtime_leakage() -> None:
    assert task_factory_module._TASK_STORE == []
    assert task_factory_module._TASK_INDEX == {}
    assert task_factory_module._STORE_SOURCE is None
    assert task_queue_module.task_queue.queued_task_ids() == []
    with token_telemetry_module._RUN_STATE_LOCK:
        assert token_telemetry_module._RUN_STATE == {}


def _configure_isolated_runtime(monkeypatch, root_dir: Path) -> AdversarialRuntime:
    data_dir = root_dir / "data"
    store_path = data_dir / "task_system.json"
    memory_file = data_dir / "memory.json"
    contacts_file = data_dir / "contacts.json"
    task_log_file = root_dir / "runtime" / "logs" / "tasks.log"
    system_log_file = root_dir / "runtime" / "logs" / "system.log"
    policy_log_file = root_dir / "runtime" / "logs" / "policy.log"
    task_queue_file = root_dir / "runtime" / "state" / "task_queue.json"
    effect_log_file = root_dir / "runtime" / "out" / "effects.jsonl"
    token_usage_log_file = root_dir / "runtime" / "logs" / "token_usage.jsonl"
    token_efficiency_log_file = root_dir / "runtime" / "logs" / "token_efficiency.jsonl"

    monkeypatch.setattr(paths_module, "ROOT_DIR", root_dir)
    monkeypatch.setattr(paths_module, "DATA_DIR", data_dir)
    monkeypatch.setattr(paths_module, "TASK_SYSTEM_FILE", store_path)
    monkeypatch.setattr(paths_module, "TASKS_FILE", data_dir / "tasks.json")
    monkeypatch.setattr(paths_module, "MEMORY_FILE", memory_file)
    monkeypatch.setattr(paths_module, "CONTACTS_FILE", contacts_file)

    monkeypatch.setattr(task_factory_module, "TASK_SYSTEM_FILE", store_path)
    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", root_dir)
    monkeypatch.setattr(
        task_state_store_module,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )

    monkeypatch.setattr(task_memory_module, "ROOT_DIR", root_dir)
    monkeypatch.setattr(
        task_memory_module,
        "TASK_MEMORY_FILE",
        Path("runtime/state/task_memory.json"),
    )

    monkeypatch.setattr(task_queue_module, "TASK_QUEUE_FILE", task_queue_file)
    monkeypatch.setattr(task_queue_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)
    monkeypatch.setattr(system_log_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(policy_gate_module, "POLICY_LOG_FILE", policy_log_file)
    monkeypatch.setattr(
        task_lifecycle_module,
        "TASK_LIFECYCLE_LOG_FILE",
        root_dir / "runtime" / "logs" / "task_lifecycle.jsonl",
    )
    monkeypatch.setattr(token_telemetry_module, "TOKEN_USAGE_LOG_FILE", token_usage_log_file)
    monkeypatch.setattr(
        token_efficiency_module,
        "TOKEN_EFFICIENCY_LOG_FILE",
        token_efficiency_log_file,
    )

    _reset_runtime_globals()
    _assert_no_runtime_leakage()
    task_state_store_module.initialize_database(store_path)

    return AdversarialRuntime(
        root_dir=root_dir,
        store_path=store_path,
        effect_log_file=effect_log_file,
        system_log_file=system_log_file,
    )


def _build_validated_task(store_path: Path) -> dict[str, object]:
    fixture = _load_task_fixture()
    task = task_factory_module.create_task(fixture, store_path=store_path)
    task["status"] = "VALIDATED"
    task["payload"] = {
        **dict(task.get("payload", {}) or {}),
        "status": "completed",
    }
    return task_factory_module.save_task(task, store_path=store_path)


def _trace_sequence(system_log_file: Path) -> list[dict[str, object]]:
    entries = [
        entry
        for entry in _read_jsonl(system_log_file)
        if entry.get("event_type") == "trace"
    ]
    if not entries:
        return []
    return list(dict(entries[-1].get("details", {})).get("step_sequence", []))


def _effect_entries(effect_log_file: Path) -> list[dict[str, object]]:
    return _read_jsonl(effect_log_file)


def _make_executor(effect_log_file: Path) -> Callable[[dict[str, object]], dict[str, object]]:
    def executor(task_data: dict[str, object]) -> dict[str, object]:
        effect_log_file.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "task_id": str(task_data.get("task_id", "")).strip(),
            "intent": str(task_data.get("intent", "")).strip(),
            "effect": "recorded",
        }
        with effect_log_file.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=True, separators=(",", ":")) + "\n")
        return build_action_result(
            status="completed",
            task_id=task_data.get("task_id"),
            action_type="MISSING_INPUT_FOLLOWUP",
            result_payload={"effect_entry": entry},
            error_code="",
            error_message="",
            source="test_adversarial_runtime_executor",
            diagnostic_message="effect recorded",
        )

    executor.__module__ = "test_adversarial_runtime_executor"
    return executor


def _persist_task(task_data: dict[str, object]) -> None:
    task_factory_module.save_task(task_data, store_path=task_factory_module.TASK_SYSTEM_FILE)


def _build_executing_task(runtime: AdversarialRuntime, *, task_id: str) -> dict[str, object]:
    task = task_factory_module.create_task(
        {
            "task_id": task_id,
            "status": "created",
            "intent": MISSING_INPUT_FOLLOWUP_WORKFLOW_TYPE,
            "payload": {"status": "completed"},
        },
        store_path=runtime.store_path,
    )
    task["status"] = "EXECUTING"
    task["execution_key"] = build_execution_key(
        task_id=task["task_id"],
        action_type=task["intent"],
    )
    return task_factory_module.save_task(task, store_path=runtime.store_path)


def _persist_recovery_result(
    runtime: AdversarialRuntime,
    *,
    task_data: dict[str, object],
    action_result: dict[str, object],
) -> None:
    claim_execution_record(
        execution_key=task_data["execution_key"],
        task_id=task_data["task_id"],
        action_type=task_data["intent"],
        store_path=runtime.store_path,
    )
    complete_execution_record(
        execution_key=task_data["execution_key"],
        action_result=action_result,
        store_path=runtime.store_path,
    )


def _run_once(
    runtime: AdversarialRuntime,
    *,
    persist: Callable[[dict[str, object]], None] | None = None,
) -> tuple[dict[str, object], dict[str, object]]:
    task = _build_validated_task(runtime.store_path)
    executed_task = run_execution(
        task,
        now=SequencedClock(),
        persist=persist or _persist_task,
        executor=_make_executor(runtime.effect_log_file),
    )
    snapshot = build_determinism_snapshot(
        task_data=executed_task,
        trace_sequence=_trace_sequence(runtime.system_log_file),
    )
    return executed_task, snapshot


def _assert_final_state_equal(left: dict[str, object], right: dict[str, object]) -> None:
    assert left["final_task_state"] == right["final_task_state"]


def _assert_lifecycle_equal(left: dict[str, object], right: dict[str, object]) -> None:
    assert left["lifecycle_transitions"] == right["lifecycle_transitions"]


def _assert_action_result_equal(left: dict[str, object], right: dict[str, object]) -> None:
    assert left["action_result"] == right["action_result"]


def test_context_free_execution_is_identical_across_clean_room_restarts(monkeypatch, tmp_path: Path) -> None:
    left_runtime = _configure_isolated_runtime(monkeypatch, tmp_path / "context_free_left")
    _, left_snapshot = _run_once(left_runtime)

    right_runtime = _configure_isolated_runtime(monkeypatch, tmp_path / "context_free_right")
    _, right_snapshot = _run_once(right_runtime)

    _assert_final_state_equal(left_snapshot, right_snapshot)
    _assert_lifecycle_equal(left_snapshot, right_snapshot)
    _assert_action_result_equal(left_snapshot, right_snapshot)
    assert left_snapshot["execution_order"] == right_snapshot["execution_order"]


def test_repeated_identical_task_execution_does_not_duplicate_external_effects(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runtime = _configure_isolated_runtime(monkeypatch, tmp_path / "idempotency")
    shared_effect_log = runtime.effect_log_file
    snapshots: list[dict[str, object]] = []

    for run_index in range(3):
        _reset_runtime_globals()
        task_state_store_module.initialize_database(runtime.store_path)
        _, snapshot = _run_once(runtime)
        snapshots.append(snapshot)

    assert snapshots[1:] == [snapshots[0], snapshots[0]]
    observed = {
        "effect_count": len(_effect_entries(shared_effect_log)),
        "effect_task_ids": [
            entry.get("task_id")
            for entry in _effect_entries(shared_effect_log)
        ],
    }
    assert observed == {
        "effect_count": 1,
        "effect_task_ids": ["DF-ADVERSARIAL-TASK-V1"],
    }


def test_partial_failure_does_not_leave_state_and_effects_diverged(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runtime = _configure_isolated_runtime(monkeypatch, tmp_path / "partial_failure")
    crash_injector = CrashOnFinalPersist()

    crashed_task, _ = _run_once(runtime, persist=crash_injector)
    persisted_after_crash = task_factory_module.get_task("DF-ADVERSARIAL-TASK-V1", runtime.store_path)

    _reset_runtime_globals()
    recovered_task = run_execution(
        dict(persisted_after_crash or {}),
        now=SequencedClock(),
        persist=_persist_task,
        executor=_make_executor(runtime.effect_log_file),
    )
    persisted_after_recovery = task_factory_module.get_task("DF-ADVERSARIAL-TASK-V1", runtime.store_path)

    observed = {
        "returned_status_after_crash": str(crashed_task.get("status", "")).strip(),
        "persisted_status_after_crash": str((persisted_after_crash or {}).get("status", "")).strip(),
        "recovered_status": str(recovered_task.get("status", "")).strip(),
        "recovered_result_status": str(dict(recovered_task.get("result", {}) or {}).get("status", "")).strip(),
        "persisted_status_after_recovery": str((persisted_after_recovery or {}).get("status", "")).strip(),
        "effect_count": len(_effect_entries(runtime.effect_log_file)),
    }
    assert observed == {
        "returned_status_after_crash": "EXECUTING",
        "persisted_status_after_crash": "EXECUTING",
        "recovered_status": "COMPLETED",
        "recovered_result_status": "completed",
        "persisted_status_after_recovery": "COMPLETED",
        "effect_count": 1,
    }


def test_failed_result_recovery_does_not_write_completed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runtime = _configure_isolated_runtime(monkeypatch, tmp_path / "failed_result_recovery")
    task = _build_executing_task(runtime, task_id="DF-FAILED-RECOVERY-V1")
    _persist_recovery_result(
        runtime,
        task_data=task,
        action_result=build_action_result(
            status="failed",
            task_id=task["task_id"],
            action_type="MISSING_INPUT_FOLLOWUP",
            result_payload={},
            error_code="simulated_failure",
            error_message="simulated failure",
            source="test_failed_result_recovery",
            diagnostic_message="simulated failure",
        ),
    )

    _reset_runtime_globals()
    recovered_task = run_execution(
        dict(task_factory_module.get_task(task["task_id"], runtime.store_path) or {}),
        now=SequencedClock(),
        persist=_persist_task,
        executor=_make_executor(runtime.effect_log_file),
    )
    persisted_after_recovery = task_factory_module.get_task(task["task_id"], runtime.store_path)

    observed = {
        "recovered_status": str(recovered_task.get("status", "")).strip(),
        "recovered_result_status": str(dict(recovered_task.get("result", {}) or {}).get("status", "")).strip(),
        "persisted_status_after_recovery": str((persisted_after_recovery or {}).get("status", "")).strip(),
        "effect_count": len(_effect_entries(runtime.effect_log_file)),
    }
    assert observed == {
        "recovered_status": "FAILED",
        "recovered_result_status": "failed",
        "persisted_status_after_recovery": "FAILED",
        "effect_count": 0,
    }


def test_failed_task_rerun_does_not_upgrade_to_completed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runtime = _configure_isolated_runtime(monkeypatch, tmp_path / "failed_task_rerun")
    task = _build_executing_task(runtime, task_id="DF-FAILED-RERUN-V1")
    _persist_recovery_result(
        runtime,
        task_data=task,
        action_result=build_action_result(
            status="failed",
            task_id=task["task_id"],
            action_type="MISSING_INPUT_FOLLOWUP",
            result_payload={},
            error_code="simulated_failure",
            error_message="simulated failure",
            source="test_failed_task_rerun",
            diagnostic_message="simulated failure",
        ),
    )
    failed_task = dict(task_factory_module.get_task(task["task_id"], runtime.store_path) or {})
    failed_task["status"] = "FAILED"
    task_factory_module.save_task(failed_task, store_path=runtime.store_path)

    _reset_runtime_globals()
    rerun_task = run_execution(
        dict(task_factory_module.get_task(task["task_id"], runtime.store_path) or {}),
        now=SequencedClock(),
        persist=_persist_task,
        executor=_make_executor(runtime.effect_log_file),
    )
    persisted_after_rerun = task_factory_module.get_task(task["task_id"], runtime.store_path)

    observed = {
        "rerun_status": str(rerun_task.get("status", "")).strip(),
        "persisted_status_after_rerun": str((persisted_after_rerun or {}).get("status", "")).strip(),
        "effect_count": len(_effect_entries(runtime.effect_log_file)),
    }
    assert observed == {
        "rerun_status": "FAILED",
        "persisted_status_after_rerun": "FAILED",
        "effect_count": 0,
    }


def test_partial_result_recovery_does_not_write_completed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    runtime = _configure_isolated_runtime(monkeypatch, tmp_path / "partial_result_recovery")
    task = _build_executing_task(runtime, task_id="DF-PARTIAL-RECOVERY-V1")
    _persist_recovery_result(
        runtime,
        task_data=task,
        action_result=build_action_result(
            status="partial",
            task_id=task["task_id"],
            action_type="MISSING_INPUT_FOLLOWUP",
            result_payload={"completed_steps": ["step_1"], "pending_steps": ["step_2"]},
            error_code="",
            error_message="",
            source="test_partial_result_recovery",
            diagnostic_message="step 2 pending recovery",
        ),
    )

    _reset_runtime_globals()
    recovered_task = run_execution(
        dict(task_factory_module.get_task(task["task_id"], runtime.store_path) or {}),
        now=SequencedClock(),
        persist=_persist_task,
        executor=_make_executor(runtime.effect_log_file),
    )
    persisted_after_recovery = task_factory_module.get_task(task["task_id"], runtime.store_path)

    observed = {
        "recovered_status": str(recovered_task.get("status", "")).strip(),
        "recovered_result_status": str(dict(recovered_task.get("result", {}) or {}).get("status", "")).strip(),
        "persisted_status_after_recovery": str((persisted_after_recovery or {}).get("status", "")).strip(),
        "effect_count": len(_effect_entries(runtime.effect_log_file)),
    }
    assert observed == {
        "recovered_status": "DEFERRED",
        "recovered_result_status": "partial",
        "persisted_status_after_recovery": "DEFERRED",
        "effect_count": 0,
    }
