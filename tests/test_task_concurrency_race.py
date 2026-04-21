from __future__ import annotations

import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Barrier, Event, Lock

import app.orchestrator.execution_runner as execution_runner_module
from app.execution.action_result import build_action_result
from app.orchestrator import task_factory
from app.orchestrator import task_state_store
from app.orchestrator.execution_runner import run_execution
from app.orchestrator.task_lifecycle import transition_task_status


def _clone(value: object) -> object:
    return json.loads(json.dumps(value))


def _configure_state_backend(monkeypatch, tmp_path: Path) -> Path:
    monkeypatch.setattr(task_state_store, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(
        task_factory,
        "TASK_SYSTEM_FILE",
        tmp_path / "data" / "task_system.json",
    )
    monkeypatch.setattr(
        execution_runner_module,
        "store_task_result",
        lambda result: dict(result),
    )
    task_factory.clear_task_runtime_store()
    return tmp_path / "data" / "task_system.json"


def _create_validated_task(store_path: Path, *, task_id: str) -> dict[str, object]:
    task = task_factory.create_task(
        {
            "task_id": task_id,
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": "concurrency race check"},
        },
        store_path=store_path,
    )
    task["status"] = "VALIDATED"
    return task_factory.save_task(task, store_path)


def _execution_key(task_data: dict[str, object]) -> str:
    return task_state_store.build_execution_key(
        task_id=task_data.get("task_id"),
        action_type=task_data.get("intent"),
    )


def _task_snapshot(task_id: str, store_path: Path) -> dict[str, object]:
    restored = task_state_store.read_task(task_id, store_path)
    return dict(restored or {})


def _persisted_status_sequence(task_id: str, store_path: Path) -> list[str]:
    connection = sqlite3.connect(str(store_path.with_suffix(".sqlite3")))
    try:
        rows = connection.execute(
            """
            SELECT descriptor
            FROM task_versions
            WHERE task_id = ?
            ORDER BY version_number ASC
            """,
            (task_id,),
        ).fetchall()
    finally:
        connection.close()

    sequence: list[str] = []
    for (descriptor,) in rows:
        payload = json.loads(str(descriptor))
        sequence.append(str(payload.get("status", "")).strip())
    return sequence


def _status_transitions(task_data: dict[str, object]) -> list[tuple[str, str]]:
    transitions: list[tuple[str, str]] = []
    for entry in list(task_data.get("history", [])):
        if str(entry.get("event", "")).strip() != "status_transition":
            continue
        transitions.append(
            (
                str(entry.get("from_status", "")).strip(),
                str(entry.get("to_status", "")).strip(),
            )
        )
    return transitions


def _execution_ledger_row(store_path: Path, execution_key: str) -> dict[str, object]:
    record = task_state_store.read_execution_record(
        execution_key, store_path=store_path
    )
    return dict(record or {})


def test_double_start_only_one_execution_path_wins(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    task = _create_validated_task(
        store_path, task_id="DF-CONCURRENCY-RACE-DOUBLE-START-V1"
    )
    execution_key = _execution_key(task)
    task_id = str(task["task_id"])
    start_barrier = Barrier(2)
    start_gate = Event()
    persist_lock = Lock()
    side_effect_lock = Lock()
    persisted_snapshots: list[dict[str, object]] = []
    side_effects: list[str] = []

    def persist(updated_task: dict[str, object]) -> None:
        candidate = dict(_clone(updated_task))
        if str(candidate.get("status", "")).strip() == "EXECUTING":
            start_barrier.wait(timeout=2)
        task_factory.save_task(candidate, store_path)
        with persist_lock:
            persisted_snapshots.append(_task_snapshot(task_id, store_path))

    def executor(task_data: dict[str, object]) -> dict[str, object]:
        with side_effect_lock:
            side_effects.append(str(task_data.get("task_id", "")).strip())
        time.sleep(0.05)
        return build_action_result(
            status="completed",
            task_id=task_data.get("task_id"),
            action_type="NEW_LEAD",
            result_payload={"scenario": "DOUBLE_START", "winner": "single"},
            error_code="",
            error_message="",
            source="test_task_concurrency_race",
        )

    executor.__module__ = "test_task_concurrency_race"

    def worker(snapshot: dict[str, object]) -> dict[str, object]:
        start_gate.wait(timeout=2)
        return run_execution(
            snapshot,
            now=lambda: "2026-04-11T09:00:00Z",
            persist=persist,
            executor=executor,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(worker, dict(_clone(task))),
            pool.submit(worker, dict(_clone(task))),
        ]
        start_gate.set()
        results = [future.result(timeout=3) for future in futures]

    restored = _task_snapshot(task_id, store_path)
    ledger_row = _execution_ledger_row(store_path, execution_key)

    assert len(side_effects) == 1
    assert ledger_row["status"] == task_state_store.EXECUTION_LEDGER_STATUS_EXECUTED
    assert restored["status"] == "COMPLETED"
    assert sorted(str(result.get("status", "")).strip() for result in results) == [
        "COMPLETED",
        "COMPLETED",
    ]
    assert _status_transitions(restored) == [
        ("VALIDATED", "EXECUTING"),
        ("EXECUTING", "COMPLETED"),
    ]
    assert [snapshot["status"] for snapshot in persisted_snapshots].count(
        "COMPLETED"
    ) >= 1


def test_stale_read_cannot_regress_completed_task_state(
    monkeypatch, tmp_path: Path
) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    task = _create_validated_task(
        store_path, task_id="DF-CONCURRENCY-RACE-STALE-READ-V1"
    )
    stale_snapshot = dict(_clone(task))
    task_id = str(task["task_id"])
    persist_lock = Lock()
    side_effect_lock = Lock()
    runner_a_done = Event()
    persisted_snapshots: list[dict[str, object]] = []
    side_effects: list[str] = []

    def persist(updated_task: dict[str, object]) -> None:
        task_factory.save_task(dict(_clone(updated_task)), store_path)
        snapshot = _task_snapshot(task_id, store_path)
        with persist_lock:
            persisted_snapshots.append(snapshot)
        if str(snapshot.get("status", "")).strip() == "COMPLETED":
            runner_a_done.set()

    def executor(task_data: dict[str, object]) -> dict[str, object]:
        with side_effect_lock:
            side_effects.append(str(task_data.get("task_id", "")).strip())
        return build_action_result(
            status="completed",
            task_id=task_data.get("task_id"),
            action_type="NEW_LEAD",
            result_payload={"scenario": "STALE_READ", "winner": "runner-a"},
            error_code="",
            error_message="",
            source="test_task_concurrency_race",
        )

    executor.__module__ = "test_task_concurrency_race"

    with ThreadPoolExecutor(max_workers=2) as pool:
        future_a = pool.submit(
            run_execution,
            dict(_clone(task)),
            now=lambda: "2026-04-11T09:10:00Z",
            persist=persist,
            executor=executor,
        )
        future_b = pool.submit(
            lambda: (
                runner_a_done.wait(timeout=2),
                run_execution(
                    dict(_clone(stale_snapshot)),
                    now=lambda: "2026-04-11T09:11:00Z",
                    persist=persist,
                    executor=executor,
                ),
            )[1]
        )
        result_a = future_a.result(timeout=3)
        result_b = future_b.result(timeout=3)

    restored = _task_snapshot(task_id, store_path)
    persisted_statuses = [
        str(snapshot.get("status", "")).strip() for snapshot in persisted_snapshots
    ]
    first_completed_index = persisted_statuses.index("COMPLETED")

    assert len(side_effects) == 1
    assert str(result_a.get("status", "")).strip() == "COMPLETED"
    assert str(result_b.get("status", "")).strip() == "COMPLETED"
    assert restored["status"] == "COMPLETED"
    assert restored["started_at"] == "2026-04-11T09:10:00Z"
    assert restored["completed_at"] == "2026-04-11T09:10:00Z"
    assert all(
        status == "COMPLETED" for status in persisted_statuses[first_completed_index:]
    )
    assert _status_transitions(restored) == [
        ("VALIDATED", "EXECUTING"),
        ("EXECUTING", "COMPLETED"),
    ]


def test_double_finalize_keeps_first_completion_result(
    monkeypatch, tmp_path: Path
) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    task = _create_validated_task(
        store_path, task_id="DF-CONCURRENCY-RACE-DOUBLE-FINALIZE-V1"
    )
    execution_key = _execution_key(task)
    task_id = str(task["task_id"])
    executing_task = dict(_clone(task))
    executing_task["execution_key"] = execution_key
    transition_task_status(
        executing_task,
        "EXECUTING",
        timestamp="2026-04-11T09:20:00Z",
        details="prepare finalize race",
    )
    task_factory.save_task(executing_task, store_path)
    task_state_store.claim_execution_record(
        execution_key=execution_key,
        task_id=task_id,
        action_type=task["intent"],
        store_path=store_path,
    )

    finalize_barrier = Barrier(2)

    def finalize(
        writer: str, *, delay_seconds: float, timestamp: str
    ) -> dict[str, object]:
        finalize_barrier.wait(timeout=2)
        if delay_seconds > 0:
            time.sleep(delay_seconds)
        action_result = build_action_result(
            status="completed",
            task_id=task_id,
            action_type="NEW_LEAD",
            result_payload={"scenario": "DOUBLE_FINALIZE", "writer": writer},
            error_code="",
            error_message="",
            source="test_task_concurrency_race",
        )
        task_state_store.complete_execution_record(
            execution_key=execution_key,
            action_result=action_result,
            store_path=store_path,
        )
        completed_task = dict(_clone(executing_task))
        completed_task["execution_key"] = execution_key
        completed_task["result"] = dict(action_result)
        transition_task_status(
            completed_task,
            "COMPLETED",
            timestamp=timestamp,
            details=f"finalized by {writer}",
        )
        task_factory.save_task(completed_task, store_path)
        return {
            "ledger": _execution_ledger_row(store_path, execution_key),
            "task": _task_snapshot(task_id, store_path),
        }

    with ThreadPoolExecutor(max_workers=2) as pool:
        future_a = pool.submit(
            finalize,
            "runner-a",
            delay_seconds=0.0,
            timestamp="2026-04-11T09:20:01Z",
        )
        future_b = pool.submit(
            finalize,
            "runner-b",
            delay_seconds=0.05,
            timestamp="2026-04-11T09:20:02Z",
        )
        result_a = future_a.result(timeout=3)
        result_b = future_b.result(timeout=3)

    restored = _task_snapshot(task_id, store_path)
    ledger_row = _execution_ledger_row(store_path, execution_key)

    assert result_a["ledger"]["action_result"]["result_payload"]["writer"] == "runner-a"
    assert result_b["ledger"]["action_result"]["result_payload"]["writer"] == "runner-a"
    assert ledger_row["action_result"]["result_payload"]["writer"] == "runner-a"
    assert restored["status"] == "COMPLETED"
    assert restored["result"]["result_payload"]["writer"] == "runner-a"
    assert restored["completed_at"] == "2026-04-11T09:20:01Z"
    assert _status_transitions(restored) == [
        ("VALIDATED", "EXECUTING"),
        ("EXECUTING", "COMPLETED"),
    ]
    assert _persisted_status_sequence(task_id, store_path).count("COMPLETED") == 1
