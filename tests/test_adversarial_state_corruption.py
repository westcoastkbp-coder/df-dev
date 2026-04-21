from __future__ import annotations

import sqlite3
from pathlib import Path


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
            "payload": {"summary": "corruption check"},
        },
        store_path=store_path,
    )
    task["status"] = "VALIDATED"
    return task_factory.save_task(task, store_path)


def _mutate_descriptor(store_path: Path, *, task_id: str, descriptor: str) -> None:
    connection = sqlite3.connect(str(_db_path(store_path)))
    try:
        connection.execute(
            "UPDATE Task SET descriptor = ? WHERE task_id = ? AND memory_ref = ''",
            (descriptor, task_id),
        )
        connection.commit()
    finally:
        connection.close()


def _mutate_row_status(store_path: Path, *, task_id: str, status: str) -> None:
    connection = sqlite3.connect(str(_db_path(store_path)))
    try:
        connection.execute(
            "UPDATE Task SET status = ? WHERE task_id = ? AND memory_ref = ''",
            (status, task_id),
        )
        connection.commit()
    finally:
        connection.close()


def _read_row_status(store_path: Path, *, task_id: str) -> str:
    connection = sqlite3.connect(str(_db_path(store_path)))
    try:
        row = connection.execute(
            "SELECT status FROM Task WHERE task_id = ? AND memory_ref = ''",
            (task_id,),
        ).fetchone()
    finally:
        connection.close()
    return str(row[0]) if row is not None else ""


def test_broken_db_record_stops_before_execution(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    _create_validated_task(store_path, task_id="DF-CORRUPT-DB-V1")
    _mutate_descriptor(
        store_path,
        task_id="DF-CORRUPT-DB-V1",
        descriptor="{not valid json",
    )

    executed = {"called": False}

    def executor(_: dict[str, object]) -> dict[str, object]:
        executed["called"] = True
        return build_action_result(
            status="completed",
            task_id="DF-CORRUPT-DB-V1",
            action_type="NEW_LEAD",
            result_payload={},
            error_code="",
            error_message="",
            source="test_adversarial_state_corruption",
        )

    blocked_task = task_factory.get_task("DF-CORRUPT-DB-V1", store_path)
    assert blocked_task is not None
    assert blocked_task["status"] == "FAILED"
    assert blocked_task["result"] == {
        "status": "failed",
        "error_code": "invalid_state",
        "error_message": "task missing required fields: created_at, history, intent, notes, payload, status, task_contract_version, task_id",
        "persisted_status": "VALIDATED",
    }
    assert executed["called"] is False


def test_missing_fields_in_task_blocks_execution(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    _create_validated_task(store_path, task_id="DF-CORRUPT-MISSING-V1")
    _mutate_descriptor(
        store_path,
        task_id="DF-CORRUPT-MISSING-V1",
        descriptor='{"task_id":"DF-CORRUPT-MISSING-V1","status":"VALIDATED"}',
    )

    blocked_task = task_factory.get_task("DF-CORRUPT-MISSING-V1", store_path)
    assert blocked_task is not None
    assert blocked_task["status"] == "FAILED"
    assert blocked_task["result"] == {
        "status": "failed",
        "error_code": "invalid_state",
        "error_message": "task missing required fields: created_at, history, intent, notes, payload, task_contract_version",
        "persisted_status": "VALIDATED",
    }


def test_invalid_status_value_blocks_execution(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    task = _create_validated_task(store_path, task_id="DF-CORRUPT-STATUS-V1")
    corrupted = dict(task)
    corrupted["status"] = "BROKEN"
    import json

    _mutate_descriptor(
        store_path,
        task_id="DF-CORRUPT-STATUS-V1",
        descriptor=json.dumps(corrupted, ensure_ascii=True, separators=(",", ":")),
    )

    blocked_task = task_factory.get_task("DF-CORRUPT-STATUS-V1", store_path)
    assert blocked_task is not None
    assert blocked_task["status"] == "FAILED"
    assert blocked_task["result"] == {
        "status": "failed",
        "error_code": "invalid_state",
        "error_message": "status must be one of: COMPLETED, CREATED, DEFERRED, EXECUTING, FAILED, VALIDATED",
        "persisted_status": "VALIDATED",
    }


def test_partial_write_row_descriptor_divergence_must_not_execute(
    monkeypatch, tmp_path: Path
) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    _create_validated_task(store_path, task_id="DF-CORRUPT-PARTIAL-V1")
    _mutate_row_status(
        store_path,
        task_id="DF-CORRUPT-PARTIAL-V1",
        status="COMPLETED",
    )

    task_data = task_factory.get_task("DF-CORRUPT-PARTIAL-V1", store_path)
    executed = {"called": False}

    def executor(task_payload: dict[str, object]) -> dict[str, object]:
        executed["called"] = True
        return build_action_result(
            status="completed",
            task_id=task_payload.get("task_id"),
            action_type="NEW_LEAD",
            result_payload={"summary": "should not execute"},
            error_code="",
            error_message="",
            source="test_adversarial_state_corruption",
        )

    executor.__module__ = "test_adversarial_state_corruption"

    run_execution(
        task_data or {},
        now=lambda: "2026-04-05T02:00:00Z",
        persist=lambda updated_task: task_factory.save_task(updated_task, store_path),
        executor=executor,
    )

    assert _read_row_status(store_path, task_id="DF-CORRUPT-PARTIAL-V1") == "FAILED"
    assert executed["called"] is False
