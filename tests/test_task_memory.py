from __future__ import annotations

import sqlite3
from pathlib import Path

from app.orchestrator import task_memory
from app.orchestrator import task_state_store
from app.orchestrator.task_memory import get_task_history, store_task_result


def test_store_task_result_appends_entry(monkeypatch, tmp_path: Path) -> None:
    task_memory_path = tmp_path / "runtime" / "task_memory.json"
    monkeypatch.setattr(task_memory, "TASK_MEMORY_FILE", task_memory_path)
    monkeypatch.setattr(task_state_store, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store,
        "TASK_STATE_DB_FILE",
        Path("runtime/task_state.sqlite3"),
    )

    store_task_result(
        {
            "task_id": "DF-TASK-1",
            "result_type": "file_written",
            "result_summary": "wrote runtime/out/task-dispatch.log",
        }
    )

    history = get_task_history("DF-TASK-1")

    assert len(history) == 1
    assert history[0] == {
        "task_id": "DF-TASK-1",
        "status": "completed",
        "result_type": "file_written",
        "result_summary": "wrote runtime/out/task-dispatch.log",
    }


def test_get_task_history_filters_entries(monkeypatch, tmp_path: Path) -> None:
    task_memory_path = tmp_path / "runtime" / "task_memory.json"
    monkeypatch.setattr(task_memory, "TASK_MEMORY_FILE", task_memory_path)
    monkeypatch.setattr(task_state_store, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store,
        "TASK_STATE_DB_FILE",
        Path("runtime/task_state.sqlite3"),
    )

    store_task_result(
        {
            "task_id": "DF-TASK-1",
            "result_type": "file_written",
            "result_summary": "first",
        }
    )
    store_task_result(
        {
            "task_id": "DF-TASK-2",
            "result_type": "file_read",
            "result_summary": "second",
        }
    )

    history = get_task_history("DF-TASK-1")

    assert history == [
        {
            "task_id": "DF-TASK-1",
            "status": "completed",
            "result_type": "file_written",
            "result_summary": "first",
        }
    ]


def test_store_task_result_enforces_max_entries(monkeypatch, tmp_path: Path) -> None:
    task_memory_path = tmp_path / "runtime" / "task_memory.json"
    monkeypatch.setattr(task_memory, "TASK_MEMORY_FILE", task_memory_path)
    monkeypatch.setattr(task_state_store, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store,
        "TASK_STATE_DB_FILE",
        Path("runtime/task_state.sqlite3"),
    )

    for index in range(105):
        store_task_result(
            {
                "task_id": f"DF-TASK-{index}",
                "result_type": "file_written",
                "result_summary": f"result-{index}",
            }
        )

    database_path = tmp_path / "runtime" / "task_state.sqlite3"
    connection = sqlite3.connect(str(database_path))
    try:
        row_count = connection.execute(
            "SELECT COUNT(1) FROM Task WHERE memory_ref != ''"
        ).fetchone()[0]
    finally:
        connection.close()
    history_first = get_task_history("DF-TASK-0")
    history_preserved = get_task_history("DF-TASK-104")

    assert row_count == 100
    assert history_first == []
    assert history_preserved == [
        {
            "task_id": "DF-TASK-104",
            "status": "completed",
            "result_type": "file_written",
            "result_summary": "result-104",
        }
    ]
