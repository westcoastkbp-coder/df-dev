from __future__ import annotations

import json
from pathlib import Path

import memory.storage as memory_storage_module

import app.orchestrator.task_queue as task_queue_module
import app.orchestrator.task_worker as task_worker_module
from app.orchestrator.task_queue import InMemoryTaskQueue
from functools import partial
from app.orchestrator.task_worker import (
    process_next_queued_task as _process_next_queued_task,
)
from tests.system_context import WORKING_SYSTEM_CONTEXT

process_next_queued_task = partial(
    _process_next_queued_task, system_context=WORKING_SYSTEM_CONTEXT
)


def _build_task(
    task_id: str,
    *,
    priority: str | None = None,
    intent: str = "generic_task",
    status: str = "pending",
) -> dict[str, object]:
    payload: dict[str, object] = {}
    if priority is not None:
        payload["priority"] = priority
    return {
        "task_id": task_id,
        "intent": intent,
        "payload": payload,
        "status": status,
    }


def _read_priority_log(log_file: Path) -> list[dict[str, object]]:
    if not log_file.exists():
        return []
    return [
        json.loads(line)
        for line in log_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _configure_queue_runtime(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        task_queue_module,
        "TASK_QUEUE_FILE",
        tmp_path / "runtime" / "state" / "task_queue.json",
    )
    monkeypatch.setattr(
        task_queue_module, "TASK_LOG_FILE", tmp_path / "runtime" / "logs" / "tasks.log"
    )


def test_priority_ordering_correct(monkeypatch, tmp_path: Path) -> None:
    _configure_queue_runtime(monkeypatch, tmp_path)
    log_file = tmp_path / "runtime" / "logs" / "execution_priority.jsonl"
    monkeypatch.setattr(task_worker_module, "EXECUTION_PRIORITY_LOG_FILE", log_file)
    monkeypatch.setattr(memory_storage_module, "save_task_record", lambda _: None)

    queue = InMemoryTaskQueue()
    tasks = {
        "task-low": _build_task("task-low", priority="LOW"),
        "task-critical": _build_task("task-critical", priority="CRITICAL"),
        "task-high": _build_task("task-high", priority="HIGH"),
    }
    executed_order: list[str] = []

    monkeypatch.setattr(
        task_worker_module,
        "run_execution",
        lambda task_data, **_: (
            executed_order.append(str(task_data["task_id"]))
            or {
                **task_data,
                "status": "completed",
                "result": {"summary": "done"},
            }
        ),
    )

    for task_id in ("task-low", "task-critical", "task-high"):
        assert queue.enqueue_task(task_id) is True

    for _ in range(3):
        process_next_queued_task(
            queue=queue,
            fetch_task=lambda task_id: tasks.get(task_id),
            persist=lambda task_data: tasks[str(task_data["task_id"])].update(
                task_data
            ),
            timeout=0.0,
            decision_resolver=lambda *args, **kwargs: {"execution_mode": "LOCAL"},
            telemetry_collector=lambda: {},
            network_snapshot_collector=lambda: {},
        )

    assert executed_order == ["task-critical", "task-high", "task-low"]


def test_fifo_preserved_within_same_priority(monkeypatch, tmp_path: Path) -> None:
    _configure_queue_runtime(monkeypatch, tmp_path)
    log_file = tmp_path / "runtime" / "logs" / "execution_priority.jsonl"
    monkeypatch.setattr(task_worker_module, "EXECUTION_PRIORITY_LOG_FILE", log_file)
    monkeypatch.setattr(memory_storage_module, "save_task_record", lambda _: None)

    queue = InMemoryTaskQueue()
    tasks = {
        "task-high-1": _build_task("task-high-1", priority="HIGH"),
        "task-high-2": _build_task("task-high-2", priority="HIGH"),
    }
    executed_order: list[str] = []

    monkeypatch.setattr(
        task_worker_module,
        "run_execution",
        lambda task_data, **_: (
            executed_order.append(str(task_data["task_id"]))
            or {
                **task_data,
                "status": "completed",
                "result": {"summary": "done"},
            }
        ),
    )

    assert queue.enqueue_task("task-high-1") is True
    assert queue.enqueue_task("task-high-2") is True

    process_next_queued_task(
        queue=queue,
        fetch_task=lambda task_id: tasks.get(task_id),
        persist=lambda task_data: tasks[str(task_data["task_id"])].update(task_data),
        timeout=0.0,
        decision_resolver=lambda *args, **kwargs: {"execution_mode": "LOCAL"},
        telemetry_collector=lambda: {},
        network_snapshot_collector=lambda: {},
    )
    process_next_queued_task(
        queue=queue,
        fetch_task=lambda task_id: tasks.get(task_id),
        persist=lambda task_data: tasks[str(task_data["task_id"])].update(task_data),
        timeout=0.0,
        decision_resolver=lambda *args, **kwargs: {"execution_mode": "LOCAL"},
        telemetry_collector=lambda: {},
        network_snapshot_collector=lambda: {},
    )

    assert executed_order == ["task-high-1", "task-high-2"]


def test_budget_skip_triggers_structured_signal(monkeypatch, tmp_path: Path) -> None:
    _configure_queue_runtime(monkeypatch, tmp_path)
    log_file = tmp_path / "runtime" / "logs" / "execution_priority.jsonl"
    monkeypatch.setattr(task_worker_module, "EXECUTION_PRIORITY_LOG_FILE", log_file)

    queue = InMemoryTaskQueue()
    tasks = {
        "task-critical-heavy": _build_task(
            "task-critical-heavy", priority="CRITICAL", intent="estimate"
        ),
        "task-high-heavy": _build_task(
            "task-high-heavy", priority="HIGH", intent="follow_up"
        ),
    }
    assert queue.enqueue_task("task-critical-heavy") is True
    assert queue.enqueue_task("task-high-heavy") is True

    result = process_next_queued_task(
        queue=queue,
        fetch_task=lambda task_id: tasks.get(task_id),
        persist=lambda task_data: tasks[str(task_data["task_id"])].update(task_data),
        timeout=0.0,
        decision_resolver=lambda *args, **kwargs: {"execution_mode": "LOCAL"},
        telemetry_collector=lambda: {},
        network_snapshot_collector=lambda: {},
        max_tokens_per_run=300,
        token_cost_resolver=lambda: {
            "avg_tokens_per_run": 600,
            "breakdown_per_task_type": {
                "estimate": {"avg_tokens_per_run": 900},
                "follow_up": {"avg_tokens_per_run": 700},
            },
        },
    )

    assert result == {
        "status": "budget_deferred",
        "task_id": "task-critical-heavy",
        "reason": "insufficient_token_budget",
    }
    log_entries = _read_priority_log(log_file)
    assert log_entries[-1]["reason"] == "insufficient_token_budget"
    assert log_entries[-1]["skipped_tasks"][0]["task_id"] == "task-critical-heavy"


def test_system_continues_with_next_budget_eligible_task(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_queue_runtime(monkeypatch, tmp_path)
    log_file = tmp_path / "runtime" / "logs" / "execution_priority.jsonl"
    monkeypatch.setattr(task_worker_module, "EXECUTION_PRIORITY_LOG_FILE", log_file)
    monkeypatch.setattr(memory_storage_module, "save_task_record", lambda _: None)

    queue = InMemoryTaskQueue()
    tasks = {
        "task-critical-heavy": _build_task(
            "task-critical-heavy", priority="CRITICAL", intent="estimate"
        ),
        "task-normal-cheap": _build_task(
            "task-normal-cheap", priority="NORMAL", intent="generic_task"
        ),
    }
    executed_order: list[str] = []

    monkeypatch.setattr(
        task_worker_module,
        "run_execution",
        lambda task_data, **_: (
            executed_order.append(str(task_data["task_id"]))
            or {
                **task_data,
                "status": "completed",
                "result": {"summary": "done"},
            }
        ),
    )

    assert queue.enqueue_task("task-critical-heavy") is True
    assert queue.enqueue_task("task-normal-cheap") is True

    result = process_next_queued_task(
        queue=queue,
        fetch_task=lambda task_id: tasks.get(task_id),
        persist=lambda task_data: tasks[str(task_data["task_id"])].update(task_data),
        timeout=0.0,
        decision_resolver=lambda *args, **kwargs: {"execution_mode": "LOCAL"},
        telemetry_collector=lambda: {},
        network_snapshot_collector=lambda: {},
        max_tokens_per_run=250,
        default_estimated_tokens=100,
        token_cost_resolver=lambda: {
            "avg_tokens_per_run": 500,
            "breakdown_per_task_type": {
                "estimate": {"avg_tokens_per_run": 1000},
                "generic_task": {"avg_tokens_per_run": 100},
            },
        },
    )

    assert result is not None
    assert result["task_id"] == "task-normal-cheap"
    assert executed_order == ["task-normal-cheap"]
    log_entries = _read_priority_log(log_file)
    assert log_entries[-1]["selected_task"] == "task-normal-cheap"
    assert log_entries[-1]["skipped_tasks"] == [
        {
            "task_id": "task-critical-heavy",
            "reason": "insufficient_token_budget",
            "priority": "CRITICAL",
            "estimated_cost": 1000,
        }
    ]


def test_no_execution_behavior_change_otherwise(monkeypatch, tmp_path: Path) -> None:
    _configure_queue_runtime(monkeypatch, tmp_path)
    log_file = tmp_path / "runtime" / "logs" / "execution_priority.jsonl"
    monkeypatch.setattr(task_worker_module, "EXECUTION_PRIORITY_LOG_FILE", log_file)
    monkeypatch.setattr(memory_storage_module, "save_task_record", lambda _: None)

    queue = InMemoryTaskQueue()
    tasks = {
        "task-default": _build_task("task-default"),
    }
    executed_order: list[str] = []

    monkeypatch.setattr(
        task_worker_module,
        "run_execution",
        lambda task_data, **_: (
            executed_order.append(str(task_data["task_id"]))
            or {
                **task_data,
                "status": "completed",
                "result": {"summary": "done"},
            }
        ),
    )

    assert queue.enqueue_task("task-default") is True

    result = process_next_queued_task(
        queue=queue,
        fetch_task=lambda task_id: tasks.get(task_id),
        persist=lambda task_data: tasks[str(task_data["task_id"])].update(task_data),
        timeout=0.0,
        decision_resolver=lambda *args, **kwargs: {"execution_mode": "LOCAL"},
        telemetry_collector=lambda: {},
        network_snapshot_collector=lambda: {},
    )

    assert result is not None
    assert result["task_id"] == "task-default"
    assert executed_order == ["task-default"]
    log_entries = _read_priority_log(log_file)
    assert log_entries[-1]["selected_task"] == "task-default"
    assert log_entries[-1]["reason"] == "selected_for_execution"
