from __future__ import annotations

import json
from pathlib import Path

import app.execution.paths as paths_module
import app.orchestrator.stuck_tasks as stuck_tasks_module
import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_state_store as task_state_store_module
from app.orchestrator.task_queue import InMemoryTaskQueue
from functools import partial
from app.orchestrator.task_worker import (
    process_next_queued_task as _process_next_queued_task,
)
from tests.system_context import WORKING_SYSTEM_CONTEXT

process_next_queued_task = partial(
    _process_next_queued_task, system_context=WORKING_SYSTEM_CONTEXT
)


def _configure_runtime(monkeypatch, tmp_path: Path) -> Path:
    store_path = tmp_path / "data" / "tasks.json"
    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store_module,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(paths_module, "TASKS_FILE", store_path)
    monkeypatch.setattr(
        stuck_tasks_module,
        "STUCK_TASK_LOG_FILE",
        tmp_path / "runtime" / "logs" / "stuck_tasks.jsonl",
    )
    task_factory_module.clear_task_runtime_store()
    return store_path


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _build_task(
    store_path: Path,
    *,
    task_id: str,
    created_at: str,
    last_updated_at: str,
    status: str,
) -> dict[str, object]:
    task = task_factory_module.create_task(
        {
            "task_id": task_id,
            "created_at": created_at,
            "last_updated_at": last_updated_at,
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": "stuck-task"},
        },
        store_path=store_path,
    )
    task["status"] = status
    task["last_updated_at"] = last_updated_at
    return task_factory_module.save_task(task, store_path=store_path)


def test_stalled_task_is_detected_and_failed(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_runtime(monkeypatch, tmp_path)
    log_file = tmp_path / "runtime" / "logs" / "stuck_tasks.jsonl"
    _build_task(
        store_path,
        task_id="DF-STUCK-CREATED-V1",
        created_at="2026-04-05T00:00:00Z",
        last_updated_at="2026-04-05T00:00:00Z",
        status="CREATED",
    )

    result = process_next_queued_task(
        queue=InMemoryTaskQueue(),
        now=lambda: "2026-04-05T00:01:00Z",
        fetch_task=lambda task_id: task_factory_module.get_task(
            task_id, store_path=store_path
        ),
        persist=lambda task_data: task_factory_module.save_task(
            task_data, store_path=store_path
        ),
        timeout=0.0,
        telemetry_collector=lambda: {},
        network_snapshot_collector=lambda: {},
        active_task_loader=lambda: task_factory_module.get_open_tasks(store_path),
    )

    assert result is None
    persisted = task_factory_module.get_task(
        "DF-STUCK-CREATED-V1", store_path=store_path
    )
    assert persisted is not None
    assert persisted["status"] == "FAILED"
    assert persisted["result"] == {
        "status": "task_stuck",
        "task_id": "DF-STUCK-CREATED-V1",
        "state": "CREATED",
        "duration": 60,
        "threshold_seconds": 60,
    }
    assert _read_jsonl(log_file)[0]["details"] == persisted["result"]


def test_recently_updated_task_is_not_flagged(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_runtime(monkeypatch, tmp_path)
    log_file = tmp_path / "runtime" / "logs" / "stuck_tasks.jsonl"
    _build_task(
        store_path,
        task_id="DF-STUCK-FRESH-V1",
        created_at="2026-04-05T00:00:00Z",
        last_updated_at="2026-04-05T00:00:45Z",
        status="CREATED",
    )

    process_next_queued_task(
        queue=InMemoryTaskQueue(),
        now=lambda: "2026-04-05T00:01:00Z",
        fetch_task=lambda task_id: task_factory_module.get_task(
            task_id, store_path=store_path
        ),
        persist=lambda task_data: task_factory_module.save_task(
            task_data, store_path=store_path
        ),
        timeout=0.0,
        telemetry_collector=lambda: {},
        network_snapshot_collector=lambda: {},
        active_task_loader=lambda: task_factory_module.get_open_tasks(store_path),
    )

    persisted = task_factory_module.get_task("DF-STUCK-FRESH-V1", store_path=store_path)
    assert persisted is not None
    assert persisted["status"] == "CREATED"
    assert _read_jsonl(log_file) == []


def test_repeated_detection_is_identical(monkeypatch, tmp_path: Path) -> None:
    _configure_runtime(monkeypatch, tmp_path)
    task = {
        "task_id": "DF-STUCK-REPEAT-V1",
        "created_at": "2026-04-05T00:00:00Z",
        "last_updated_at": "2026-04-05T00:00:00Z",
        "status": "DEFERRED",
        "intent": "new_lead",
        "payload": {"summary": "repeat"},
        "notes": [],
        "history": [],
        "task_contract_version": 1,
    }

    first = stuck_tasks_module.detect_stuck_tasks(
        [task], now_timestamp="2026-04-05T00:03:00Z"
    )
    second = stuck_tasks_module.detect_stuck_tasks(
        [task], now_timestamp="2026-04-05T00:03:00Z"
    )

    assert (
        first
        == second
        == [
            {
                "status": "task_stuck",
                "task_id": "DF-STUCK-REPEAT-V1",
                "state": "DEFERRED",
                "duration": 180,
                "threshold_seconds": 180,
            }
        ]
    )
