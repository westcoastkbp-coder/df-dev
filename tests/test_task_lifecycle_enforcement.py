from __future__ import annotations

import json
from pathlib import Path

from app.orchestrator import task_factory, task_state_store
from app.orchestrator.execution_runner import run_execution
import app.orchestrator.execution_runner as execution_runner_module
import app.orchestrator.task_lifecycle as task_lifecycle_module
from app.orchestrator.task_lifecycle import (
    InvalidTaskStateTransitionError,
    set_task_state,
    validate_transition,
)


def _configure_state_backend(monkeypatch, tmp_path: Path) -> Path:
    monkeypatch.setattr(task_state_store, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(
        task_lifecycle_module,
        "TASK_LIFECYCLE_LOG_FILE",
        tmp_path / "runtime" / "logs" / "task_lifecycle.jsonl",
    )
    monkeypatch.setattr(
        task_factory,
        "TASK_SYSTEM_FILE",
        tmp_path / "data" / "task_system.json",
    )
    monkeypatch.setattr(
        execution_runner_module.task_factory_module,
        "TASK_SYSTEM_FILE",
        tmp_path / "data" / "task_system.json",
    )
    task_factory.clear_task_runtime_store()
    return tmp_path / "data" / "task_system.json"


def _read_lifecycle_log(log_file: Path) -> list[dict[str, object]]:
    if not log_file.exists():
        return []
    return [
        json.loads(line)
        for line in log_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _build_created_task(store_path: Path, *, task_id: str) -> dict[str, object]:
    return task_factory.create_task(
        {
            "task_id": task_id,
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": "lifecycle"},
        },
        store_path=store_path,
    )


def test_valid_transitions_pass() -> None:
    assert validate_transition("CREATED", "AWAITING_APPROVAL") is True
    assert validate_transition("AWAITING_APPROVAL", "VALIDATED") is True
    assert validate_transition("CREATED", "VALIDATED") is True
    assert validate_transition("VALIDATED", "EXECUTING") is True
    assert validate_transition("EXECUTING", "COMPLETED") is True
    assert validate_transition("EXECUTING", "FAILED") is True
    assert validate_transition("EXECUTING", "DEFERRED") is True
    assert validate_transition("DEFERRED", "EXECUTING") is True


def test_invalid_transitions_fail() -> None:
    assert validate_transition("CREATED", "EXECUTING") is False
    assert validate_transition("AWAITING_APPROVAL", "EXECUTING") is False
    assert validate_transition("VALIDATED", "COMPLETED") is False
    assert validate_transition("FAILED", "EXECUTING") is False


def test_repeated_transitions_blocked(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    task = _build_created_task(store_path, task_id="DF-LIFECYCLE-REPEAT-V1")

    set_task_state(task, "VALIDATED", timestamp="2026-04-04T00:00:00Z")

    try:
        set_task_state(task, "VALIDATED", timestamp="2026-04-04T00:00:01Z")
    except InvalidTaskStateTransitionError as exc:
        assert exc.signal == {
            "status": "invalid_state_transition",
            "task_id": "DF-LIFECYCLE-REPEAT-V1",
            "from": "VALIDATED",
            "to": "VALIDATED",
        }
    else:
        raise AssertionError("repeated transition should be blocked")


def test_terminal_states_locked(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    task = _build_created_task(store_path, task_id="DF-LIFECYCLE-TERMINAL-V1")
    set_task_state(task, "VALIDATED", timestamp="2026-04-04T00:00:00Z")
    set_task_state(task, "EXECUTING", timestamp="2026-04-04T00:00:01Z")
    set_task_state(task, "COMPLETED", timestamp="2026-04-04T00:00:02Z")

    try:
        set_task_state(task, "DEFERRED", timestamp="2026-04-04T00:00:03Z")
    except InvalidTaskStateTransitionError as exc:
        assert exc.signal["from"] == "COMPLETED"
        assert exc.signal["to"] == "DEFERRED"
    else:
        raise AssertionError("terminal state should be locked")


def test_deferred_to_executing_works(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    log_file = tmp_path / "runtime" / "logs" / "task_lifecycle.jsonl"
    task = _build_created_task(store_path, task_id="DF-LIFECYCLE-DEFERRED-V1")

    set_task_state(task, "VALIDATED", timestamp="2026-04-04T00:00:00Z")
    set_task_state(task, "EXECUTING", timestamp="2026-04-04T00:00:01Z")
    set_task_state(task, "DEFERRED", timestamp="2026-04-04T00:00:02Z")
    set_task_state(task, "EXECUTING", timestamp="2026-04-04T00:00:03Z")

    assert task["status"] == "EXECUTING"
    last_entry = _read_lifecycle_log(log_file)[-1]
    assert last_entry["task_id"] == "DF-LIFECYCLE-DEFERRED-V1"
    assert last_entry["event_type"] == "task_lifecycle"
    assert last_entry["status"] == "allowed"
    assert last_entry["details"] == {
        "task_id": "DF-LIFECYCLE-DEFERRED-V1",
        "from_state": "DEFERRED",
        "to_state": "EXECUTING",
        "result": "allowed",
    }


def test_execution_runner_returns_invalid_transition_signal(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    monkeypatch.setattr(execution_runner_module, "store_task_result", lambda result: dict(result))
    task = _build_created_task(store_path, task_id="DF-LIFECYCLE-RUNNER-V1")
    task["status"] = "COMPLETED"
    task_factory.save_task(task, store_path)

    persisted_snapshots: list[dict[str, object]] = []

    result = run_execution(
        task_factory.get_task("DF-LIFECYCLE-RUNNER-V1", store_path) or {},
        now=lambda: "2026-04-04T00:00:00Z",
        persist=lambda updated_task: persisted_snapshots.append(dict(updated_task)),
        executor=lambda _: {"summary": "should not run"},
    )

    assert result["status"] == "COMPLETED"
    assert result["result"] is None
    assert persisted_snapshots == []
