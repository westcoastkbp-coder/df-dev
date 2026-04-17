from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event, Lock
import time

import pytest
import sqlite3

from app.execution.action_result import build_action_result
from app.orchestrator import task_factory
from app.orchestrator import task_state_store
import app.orchestrator.execution_runner as execution_runner_module
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
    return tmp_path / "runtime" / "state" / "task_state.sqlite3"


def _task_rows(store_path: Path) -> list[tuple[str, str]]:
    database_path = store_path.with_suffix(".sqlite3")
    connection = sqlite3.connect(str(database_path))
    try:
        return [
            (str(row[0]), str(row[1]))
            for row in connection.execute(
                "SELECT task_id, status FROM Task WHERE memory_ref = '' ORDER BY task_id ASC"
            ).fetchall()
        ]
    finally:
        connection.close()


def test_create_task_persists_and_is_readable_after_restart(monkeypatch, tmp_path: Path) -> None:
    store_path = tmp_path / "data" / "task_system.json"
    database_path = store_path.with_suffix(".sqlite3")
    _configure_state_backend(monkeypatch, tmp_path)

    created = task_factory.create_task(
        {
            "task_id": "DF-STATE-CREATE-V1",
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": "deterministic create"},
        },
        store_path=store_path,
    )

    assert database_path.exists()
    task_factory.clear_task_runtime_store()

    restored = task_factory.get_task("DF-STATE-CREATE-V1", store_path)

    assert created["task_id"] == "DF-STATE-CREATE-V1"
    assert restored is not None
    assert restored["task_id"] == "DF-STATE-CREATE-V1"
    assert restored["payload"]["summary"] == "deterministic create"


def test_update_task_is_atomic_and_persists_final_state(monkeypatch, tmp_path: Path) -> None:
    _configure_state_backend(monkeypatch, tmp_path)
    store_path = tmp_path / "data" / "task_system.json"
    created = task_factory.create_task(
        {
            "task_id": "DF-STATE-UPDATE-V1",
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": "before"},
        },
        store_path=store_path,
    )

    created["status"] = "EXECUTING"
    created["payload"] = {"summary": "after"}
    task_factory.save_task(created, store_path)
    task_factory.clear_task_runtime_store()

    restored = task_factory.get_task("DF-STATE-UPDATE-V1", store_path)

    assert restored is not None
    assert restored["status"] == "EXECUTING"
    assert restored["payload"] == {"summary": "after"}


def test_failed_update_rolls_back_previous_valid_state(monkeypatch, tmp_path: Path) -> None:
    _configure_state_backend(monkeypatch, tmp_path)
    store_path = tmp_path / "data" / "task_system.json"
    task_factory.create_task(
        {
            "task_id": "DF-STATE-ROLLBACK-V1",
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": "stable"},
        },
        store_path=store_path,
    )

    def broken_update(connection) -> None:
        connection.execute(
            "UPDATE Task SET descriptor = ? WHERE task_id = ?",
            ('{"task_id":"DF-STATE-ROLLBACK-V1","status":"corrupted"}', "DF-STATE-ROLLBACK-V1"),
        )
        raise RuntimeError("force rollback")

    with pytest.raises(RuntimeError, match="force rollback"):
        task_state_store.run_in_transaction(
            broken_update,
            store_path=store_path,
            operation_name="test_failed_update",
            task_id="DF-STATE-ROLLBACK-V1",
        )

    task_factory.clear_task_runtime_store()
    restored = task_factory.get_task("DF-STATE-ROLLBACK-V1", store_path)

    assert restored is not None
    assert restored["status"] == "CREATED"
    assert restored["payload"]["summary"] == "stable"


def test_failed_insert_rolls_back_cleanly(monkeypatch, tmp_path: Path) -> None:
    _configure_state_backend(monkeypatch, tmp_path)
    store_path = tmp_path / "data" / "task_system.json"

    def broken_insert(connection) -> None:
        connection.execute(
            """
            INSERT INTO Task (
                task_id, status, descriptor, result, created_at,
                updated_at, control_fields, memory_ref
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "DF-STATE-FAILED-INSERT-V1",
                "CREATED",
                "{}",
                "{}",
                "2026-04-04T00:00:00Z",
                "2026-04-04T00:00:00Z",
                "{}",
                "",
            ),
        )
        connection.execute("INSERT INTO MissingTable(value) VALUES (1)")

    with pytest.raises(task_state_store.StatePersistenceError) as exc_info:
        task_state_store.run_in_transaction(
            broken_insert,
            store_path=store_path,
            operation_name="test_failed_insert",
            task_id="DF-STATE-FAILED-INSERT-V1",
        )

    assert exc_info.value.signal == {
        "status": "state_persist_failed",
        "task_id": "DF-STATE-FAILED-INSERT-V1",
        "operation": "test_failed_insert",
        "storage": "sqlite",
    }
    assert _task_rows(store_path) == []


def test_failed_multi_write_sequence_leaves_no_partial_state(monkeypatch, tmp_path: Path) -> None:
    _configure_state_backend(monkeypatch, tmp_path)
    store_path = tmp_path / "data" / "task_system.json"
    task_factory.create_task(
        {
            "task_id": "DF-STATE-MULTI-BASE-V1",
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": "base"},
        },
        store_path=store_path,
    )
    before_rows = _task_rows(store_path)

    def broken_multi_write(connection) -> None:
        connection.execute("DELETE FROM Task WHERE memory_ref = ''")
        connection.execute(
            """
            INSERT INTO Task (
                task_id, status, descriptor, result, created_at,
                updated_at, control_fields, memory_ref
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "DF-STATE-MULTI-NEW-V1",
                "CREATED",
                "{}",
                "{}",
                "2026-04-04T00:00:00Z",
                "2026-04-04T00:00:00Z",
                "{}",
                "",
            ),
        )
        connection.execute("INSERT INTO MissingTable(value) VALUES (1)")

    with pytest.raises(task_state_store.StatePersistenceError):
        task_state_store.run_in_transaction(
            broken_multi_write,
            store_path=store_path,
            operation_name="test_failed_multi_write",
            task_id="DF-STATE-MULTI-NEW-V1",
        )

    assert _task_rows(store_path) == before_rows


def test_retryable_disk_io_error_retries_write_and_persists(monkeypatch, tmp_path: Path) -> None:
    _configure_state_backend(monkeypatch, tmp_path)
    store_path = tmp_path / "data" / "task_system.json"
    original_execute_once = task_state_store._execute_transaction_once
    attempts: list[str] = []

    def flaky_execute_once(connection, operation, *, operation_name: str, task_id: object = ""):
        if operation_name == "write_task" and not attempts:
            attempts.append("retry")
            raise task_state_store.StatePersistenceError(
                task_state_store.build_state_persist_failure(
                    task_id=task_id,
                    operation=operation_name,
                )
            ) from sqlite3.OperationalError("disk I/O error")
        return original_execute_once(
            connection,
            operation,
            operation_name=operation_name,
            task_id=task_id,
        )

    monkeypatch.setattr(task_state_store, "_execute_transaction_once", flaky_execute_once)

    created = task_factory.create_task(
        {
            "task_id": "DF-STATE-RETRY-IO-V1",
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": "retryable io"},
        },
        store_path=store_path,
    )

    restored = task_factory.get_task("DF-STATE-RETRY-IO-V1", store_path)

    assert len(attempts) == 1
    assert created["task_id"] == "DF-STATE-RETRY-IO-V1"
    assert restored is not None
    assert restored["payload"]["summary"] == "retryable io"
    assert _task_rows(store_path) == [("DF-STATE-RETRY-IO-V1", "CREATED")]


def test_retry_budget_exhaustion_returns_controlled_failure_without_partial_write(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_state_backend(monkeypatch, tmp_path)
    store_path = tmp_path / "data" / "task_system.json"
    original_execute_once = task_state_store._execute_transaction_once
    attempts: list[str] = []

    def always_fail(connection, operation, *, operation_name: str, task_id: object = ""):
        if operation_name == "write_task":
            attempts.append(str(task_id or ""))
            raise task_state_store.StatePersistenceError(
                task_state_store.build_state_persist_failure(
                    task_id=task_id,
                    operation=operation_name,
                )
            ) from sqlite3.OperationalError("disk I/O error")
        return original_execute_once(
            connection,
            operation,
            operation_name=operation_name,
            task_id=task_id,
        )

    monkeypatch.setattr(task_state_store, "_execute_transaction_once", always_fail)

    with pytest.raises(task_state_store.StatePersistenceError) as exc_info:
        task_factory.create_task(
            {
                "task_id": "DF-STATE-RETRY-EXHAUST-V1",
                "status": "created",
                "intent": "new_lead",
                "payload": {"summary": "exhaust retries"},
            },
            store_path=store_path,
        )

    assert exc_info.value.signal == {
        "status": "state_persist_failed",
        "task_id": "DF-STATE-RETRY-EXHAUST-V1",
        "operation": "write_task",
        "storage": "sqlite",
    }
    assert len(attempts) == task_state_store.SQLITE_WRITE_RETRY_ATTEMPTS
    assert _task_rows(store_path) == []


def test_repeated_failed_update_leaves_same_final_persisted_state(monkeypatch, tmp_path: Path) -> None:
    _configure_state_backend(monkeypatch, tmp_path)
    store_path = tmp_path / "data" / "task_system.json"
    task_factory.create_task(
        {
            "task_id": "DF-STATE-REPEAT-V1",
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": "stable"},
        },
        store_path=store_path,
    )
    initial_rows = _task_rows(store_path)

    def broken_update(connection) -> None:
        connection.execute(
            "UPDATE Task SET status = ? WHERE task_id = ?",
            ("FAILED", "DF-STATE-REPEAT-V1"),
        )
        connection.execute("INSERT INTO MissingTable(value) VALUES (1)")

    for _ in range(2):
        with pytest.raises(task_state_store.StatePersistenceError):
            task_state_store.run_in_transaction(
                broken_update,
                store_path=store_path,
                operation_name="test_repeated_failed_update",
                task_id="DF-STATE-REPEAT-V1",
            )
        assert _task_rows(store_path) == initial_rows


def test_repeated_restart_does_not_lose_state(monkeypatch, tmp_path: Path) -> None:
    _configure_state_backend(monkeypatch, tmp_path)
    store_path = tmp_path / "data" / "task_system.json"
    task_factory.create_task(
        {
            "task_id": "DF-STATE-RESTART-V1",
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": "restart safe"},
        },
        store_path=store_path,
    )

    for _ in range(3):
        task_factory.clear_task_runtime_store()
        restored = task_factory.get_task("DF-STATE-RESTART-V1", store_path)
        assert restored is not None
        assert restored["payload"]["summary"] == "restart safe"


def test_basic_database_write_lock_serializes_concurrent_writers(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_state_backend(monkeypatch, tmp_path)
    store_path = tmp_path / "data" / "task_system.json"
    target = task_state_store.initialize_database(store_path)
    first_entered = Event()
    release_first = Event()
    active_lock = Lock()
    active_writers = 0
    max_active_writers = 0
    entered_order: list[str] = []

    def worker(name: str) -> None:
        nonlocal active_writers, max_active_writers
        with task_state_store._database_write_lock(target):
            with active_lock:
                entered_order.append(name)
                active_writers += 1
                max_active_writers = max(max_active_writers, active_writers)
                if name == "first":
                    first_entered.set()
            if name == "first":
                release_first.wait(timeout=2)
            time.sleep(0.02)
            with active_lock:
                active_writers -= 1

    with ThreadPoolExecutor(max_workers=2) as pool:
        future_first = pool.submit(worker, "first")
        assert first_entered.wait(timeout=2) is True
        future_second = pool.submit(worker, "second")
        time.sleep(0.05)
        assert entered_order == ["first"]
        release_first.set()
        future_first.result(timeout=2)
        future_second.result(timeout=2)

    assert entered_order == ["first", "second"]
    assert max_active_writers == 1


def test_execution_behavior_deterministic_and_context_free_unchanged(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_state_backend(monkeypatch, tmp_path)
    monkeypatch.setattr(execution_runner_module, "store_task_result", lambda result: dict(result))
    store_path = tmp_path / "data" / "task_system.json"
    task = task_factory.create_task(
        {
            "task_id": "DF-STATE-EXECUTION-V1",
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": "unchanged execution"},
        },
        store_path=store_path,
    )
    task["status"] = "VALIDATED"
    task_factory.save_task(task, store_path)

    persisted_snapshots: list[dict[str, object]] = []

    def persist(updated_task: dict[str, object]) -> None:
        persisted_snapshots.append(dict(updated_task))
        task_factory.save_task(updated_task, store_path)

    def executor(task_data: dict[str, object]) -> dict[str, object]:
        assert task_data["payload"]["summary"] == "unchanged execution"
        return build_action_result(
            status="completed",
            task_id=task_data.get("task_id"),
            action_type="NEW_LEAD",
            result_payload={
                "result_type": "deterministic_check",
                "result_summary": "deterministic output",
                "summary": "deterministic output",
            },
            error_code="",
            error_message="",
            source="test_task_state_hardening",
        )
    executor.__module__ = "test_task_state_hardening"

    executed = run_execution(
        task_factory.get_task("DF-STATE-EXECUTION-V1", store_path) or {},
        now=lambda: "2026-04-04T00:00:00Z",
        persist=persist,
        executor=executor,
    )

    task_factory.clear_task_runtime_store()
    restored = task_factory.get_task("DF-STATE-EXECUTION-V1", store_path)

    assert executed["status"] == "COMPLETED"
    assert persisted_snapshots[0]["status"] == "EXECUTING"
    assert persisted_snapshots[-1]["status"] == "COMPLETED"
    assert restored is not None
    assert restored["result"]["status"] == "completed"
    assert restored["result"]["action_type"] == "NEW_LEAD"
    assert restored["result"]["result_payload"] == {
        "result_type": "deterministic_check",
        "result_summary": "deterministic output",
        "summary": "deterministic output",
    }
    assert restored["result"]["error_code"] == ""
    assert restored["result"]["error_message"] == ""
    assert restored["result"]["source"] == "test_task_state_hardening"
    assert restored["result"]["task_id"] == "DF-STATE-EXECUTION-V1"


def test_runtime_returns_structured_failure_signal_after_persist_failure(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_state_backend(monkeypatch, tmp_path)
    monkeypatch.setattr(execution_runner_module, "store_task_result", lambda result: dict(result))
    store_path = tmp_path / "data" / "task_system.json"
    task = task_factory.create_task(
        {
            "task_id": "DF-STATE-PERSIST-FAIL-V1",
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": "persist fail"},
        },
        store_path=store_path,
    )
    task["status"] = "VALIDATED"
    task_factory.save_task(task, store_path)

    def persist(_: dict[str, object]) -> None:
        raise task_state_store.StatePersistenceError(
            task_state_store.build_state_persist_failure(
                task_id="DF-STATE-PERSIST-FAIL-V1",
                operation="write_task",
            )
        )

    executed = run_execution(
        task_factory.get_task("DF-STATE-PERSIST-FAIL-V1", store_path) or {},
        now=lambda: "2026-04-04T00:00:00Z",
        persist=persist,
        executor=lambda _: {
            "status": "completed",
            "action_type": "NEW_LEAD",
            "result_payload": {
                "result_type": "deterministic_check",
                "result_summary": "deterministic output",
                "summary": "deterministic output",
            },
            "error_code": "",
            "error_message": "",
            "source": "test_task_state_hardening",
            "task_id": "DF-STATE-PERSIST-FAIL-V1",
        },
    )

    assert executed["status"] == "VALIDATED"
    assert executed.get("result") is None
    restored = task_factory.get_task("DF-STATE-PERSIST-FAIL-V1", store_path)
    assert restored is not None
    assert restored["status"] == "VALIDATED"
