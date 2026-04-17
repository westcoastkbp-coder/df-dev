from __future__ import annotations

import json
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Lock

from app.execution.action_result import build_action_result
from app.orchestrator import task_factory
from app.orchestrator import task_state_store
from app.orchestrator.execution_runner import run_execution


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
    task_factory.clear_task_runtime_store()
    return tmp_path / "data" / "task_system.json"


def _db_path(store_path: Path) -> Path:
    return store_path.with_suffix(".sqlite3")


def _create_validated_task(store_path: Path, *, task_id: str) -> dict[str, object]:
    task = task_factory.create_task(
        {
            "task_id": task_id,
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": "concurrency check"},
        },
        store_path=store_path,
    )
    task["status"] = "VALIDATED"
    return task_factory.save_task(task, store_path)


def _persist(updated_task: dict[str, object], *, store_path: Path) -> None:
    task_factory.save_task(updated_task, store_path)


def _ledger_rows(store_path: Path, *, execution_key: str) -> list[tuple[str, str, str]]:
    connection = sqlite3.connect(str(_db_path(store_path)))
    try:
        return [
            (str(row[0]), str(row[1]), str(row[2]))
            for row in connection.execute(
                """
                SELECT execution_key, task_id, action_type
                FROM ExecutionLedger
                WHERE execution_key = ?
                """,
                (execution_key,),
            ).fetchall()
        ]
    finally:
        connection.close()


def test_same_task_parallel_execution_must_not_duplicate_side_effects(
    monkeypatch,
    tmp_path: Path,
) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    task = _create_validated_task(store_path, task_id="DF-CONCURRENCY-TASK-V1")
    execution_key = task_state_store.build_execution_key(
        task_id=task["task_id"],
        action_type=task["intent"],
    )
    worker_count = 10
    effect_lock = Lock()
    side_effects: list[str] = []

    def executor(task_data: dict[str, object]) -> dict[str, object]:
        with effect_lock:
            side_effects.append(str(task_data.get("task_id", "")).strip())
        time.sleep(0.1)
        return build_action_result(
            status="completed",
            task_id=task_data.get("task_id"),
            action_type="NEW_LEAD",
            result_payload={"side_effect_count": len(side_effects)},
            error_code="",
            error_message="",
            source="test_adversarial_concurrency_executor",
        )

    executor.__module__ = "test_adversarial_concurrency_executor"

    task_snapshots = [json.loads(json.dumps(task)) for _ in range(worker_count)]
    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        results = list(
            pool.map(
                lambda snapshot: run_execution(
                    snapshot,
                    now=lambda: "2026-04-06T01:00:00Z",
                    persist=lambda updated_task: _persist(updated_task, store_path=store_path),
                    executor=executor,
                ),
                task_snapshots,
            )
        )

    restored = task_factory.get_task("DF-CONCURRENCY-TASK-V1", store_path)
    observed = {
        "side_effect_count": len(side_effects),
        "ledger_rows": len(_ledger_rows(store_path, execution_key=execution_key)),
        "returned_statuses": sorted(str(result.get("status", "")).strip() for result in results),
        "persisted_status": str((restored or {}).get("status", "")).strip(),
    }
    assert observed == {
        "side_effect_count": 1,
        "ledger_rows": 1,
        "returned_statuses": ["COMPLETED"] * worker_count,
        "persisted_status": "COMPLETED",
    }


def test_same_execution_key_parallel_ledger_write_must_not_double_insert(
    monkeypatch,
    tmp_path: Path,
) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    task = _create_validated_task(store_path, task_id="DF-CONCURRENCY-LEDGER-V1")
    execution_key = task_state_store.build_execution_key(
        task_id=task["task_id"],
        action_type=task["intent"],
    )
    worker_count = 10

    def writer(_: int) -> dict[str, object]:
        return task_state_store.write_execution_record(
            execution_key=execution_key,
            task_id=task["task_id"],
            action_type=task["intent"],
            action_result=build_action_result(
                status="completed",
                task_id=task["task_id"],
                action_type="NEW_LEAD",
                result_payload={"writer": "parallel"},
                error_code="",
                error_message="",
                source="test_adversarial_concurrency",
            ),
            store_path=store_path,
        )

    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        records = list(pool.map(writer, range(worker_count)))

    rows = _ledger_rows(store_path, execution_key=execution_key)
    assert len(rows) == 1
    assert all(record["execution_key"] == execution_key for record in records)


def test_parallel_run_execution_must_not_leave_inconsistent_task_state(
    monkeypatch,
    tmp_path: Path,
) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    task = _create_validated_task(store_path, task_id="DF-CONCURRENCY-STATE-V1")
    worker_count = 6

    def executor(task_data: dict[str, object]) -> dict[str, object]:
        time.sleep(0.1)
        return build_action_result(
            status="completed",
            task_id=task_data.get("task_id"),
            action_type="NEW_LEAD",
            result_payload={"summary": "parallel completion"},
            error_code="",
            error_message="",
            source="test_adversarial_concurrency_executor",
        )

    executor.__module__ = "test_adversarial_concurrency_executor"

    task_snapshots = [json.loads(json.dumps(task)) for _ in range(worker_count)]
    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        results = list(
            pool.map(
                lambda snapshot: run_execution(
                    snapshot,
                    now=lambda: "2026-04-06T01:10:00Z",
                    persist=lambda updated_task: _persist(updated_task, store_path=store_path),
                    executor=executor,
                ),
                task_snapshots,
            )
        )

    restored = task_factory.get_task("DF-CONCURRENCY-STATE-V1", store_path)
    assert all(str(result.get("status", "")).strip() == "COMPLETED" for result in results)
    assert restored is not None
    assert restored["status"] == "COMPLETED"


def test_completed_task_replay_is_deterministic_without_new_side_effects(
    monkeypatch,
    tmp_path: Path,
) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    task = _create_validated_task(store_path, task_id="DF-CONCURRENCY-REPLAY-V1")
    execution_key = task_state_store.build_execution_key(
        task_id=task["task_id"],
        action_type=task["intent"],
    )
    side_effects: list[str] = []

    def executor(task_data: dict[str, object]) -> dict[str, object]:
        side_effects.append(str(task_data.get("task_id", "")).strip())
        return build_action_result(
            status="completed",
            task_id=task_data.get("task_id"),
            action_type="NEW_LEAD",
            result_payload={"summary": "deterministic replay", "count": len(side_effects)},
            error_code="",
            error_message="",
            source="test_adversarial_concurrency_executor",
        )

    executor.__module__ = "test_adversarial_concurrency_executor"

    first = run_execution(
        json.loads(json.dumps(task)),
        now=lambda: "2026-04-06T01:20:00Z",
        persist=lambda updated_task: _persist(updated_task, store_path=store_path),
        executor=executor,
    )
    assert str(first.get("status", "")).strip() == "COMPLETED"
    first_result = json.loads(json.dumps(first.get("result", {})))
    first_snapshot = json.loads(
        json.dumps(task_factory.get_task("DF-CONCURRENCY-REPLAY-V1", store_path))
    )
    assert len(side_effects) == 1

    replay_results: list[dict[str, object]] = []
    replay_snapshots: list[dict[str, object]] = []
    for _ in range(100):
        current = task_factory.get_task("DF-CONCURRENCY-REPLAY-V1", store_path)
        replayed = run_execution(
            json.loads(json.dumps(current)),
            now=lambda: "2026-04-06T01:20:00Z",
            persist=lambda updated_task: _persist(updated_task, store_path=store_path),
            executor=executor,
        )
        replay_results.append(json.loads(json.dumps(replayed.get("result", {}))))
        replay_snapshots.append(
            json.loads(json.dumps(task_factory.get_task("DF-CONCURRENCY-REPLAY-V1", store_path)))
        )

    assert len(side_effects) == 1
    assert len(_ledger_rows(store_path, execution_key=execution_key)) == 1
    assert all(result == first_result for result in replay_results)
    assert all(snapshot == first_snapshot for snapshot in replay_snapshots)


def test_execution_replay_attack_fifty_repeated_execute_calls_has_single_real_effect(
    monkeypatch,
    tmp_path: Path,
) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    task = _create_validated_task(store_path, task_id="DF-EXEC-REPLAY-ATTACK-V1")
    external_calls: list[str] = []

    def executor(task_data: dict[str, object]) -> dict[str, object]:
        external_calls.append(str(task_data.get("task_id", "")).strip())
        return build_action_result(
            status="completed",
            task_id=task_data.get("task_id"),
            action_type="NEW_LEAD",
            result_payload={"external_calls_count": len(external_calls)},
            error_code="",
            error_message="",
            source="test_execution_replay_attack_executor",
        )

    executor.__module__ = "test_execution_replay_attack_executor"

    executions_triggered = 50
    returned_statuses: list[str] = []
    for _ in range(executions_triggered):
        current = task_factory.get_task("DF-EXEC-REPLAY-ATTACK-V1", store_path) or task
        replayed = run_execution(
            json.loads(json.dumps(current)),
            now=lambda: "2026-04-06T02:00:00Z",
            persist=lambda updated_task: _persist(updated_task, store_path=store_path),
            executor=executor,
        )
        returned_statuses.append(str(replayed.get("status", "")).strip())

    restored = task_factory.get_task("DF-EXEC-REPLAY-ATTACK-V1", store_path)
    observed = {
        "executions_triggered": executions_triggered,
        "real_effects": len(external_calls),
        "external_calls_count": len(external_calls),
        "duplicate_actions": len(external_calls) > 1,
        "PASS/FAIL": "PASS"
        if len(external_calls) == 1
        and all(status == "COMPLETED" for status in returned_statuses)
        and str((restored or {}).get("status", "")).strip() == "COMPLETED"
        else "FAIL",
    }

    assert observed == {
        "executions_triggered": 50,
        "real_effects": 1,
        "external_calls_count": 1,
        "duplicate_actions": False,
        "PASS/FAIL": "PASS",
    }
