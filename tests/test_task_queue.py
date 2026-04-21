from __future__ import annotations

import json
from pathlib import Path

import memory.storage
from app.execution.action_result import build_action_result
from app.execution.task_schema import TASK_CONTRACT_VERSION

from app.orchestrator.task_queue import InMemoryTaskQueue
from functools import partial
from app.orchestrator.task_worker import (
    process_next_queued_task as _process_next_queued_task,
)
from tests.system_context import WORKING_SYSTEM_CONTEXT

process_next_queued_task = partial(
    _process_next_queued_task, system_context=WORKING_SYSTEM_CONTEXT
)


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_enqueue_execute_dequeue(monkeypatch, tmp_path: Path) -> None:
    queue_file = tmp_path / "runtime" / "state" / "task_queue.json"
    log_file = tmp_path / "runtime" / "logs" / "tasks.log"

    import app.orchestrator.task_queue as task_queue_module
    import app.orchestrator.execution_runner as execution_runner_module
    import app.orchestrator.task_worker as task_worker_module

    monkeypatch.setattr(task_queue_module, "TASK_QUEUE_FILE", queue_file)
    monkeypatch.setattr(task_queue_module, "TASK_LOG_FILE", log_file)
    monkeypatch.setattr(
        execution_runner_module.task_factory_module,
        "TASK_SYSTEM_FILE",
        tmp_path / "data" / "task_system.json",
    )
    monkeypatch.setattr(
        execution_runner_module,
        "assert_product_runtime_executor",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        execution_runner_module,
        "claim_execution_record",
        lambda **kwargs: {"claimed": True},
    )
    monkeypatch.setattr(
        execution_runner_module,
        "complete_execution_record",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        execution_runner_module,
        "read_execution_record",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        task_worker_module,
        "run_execution",
        lambda task_data, **kwargs: {**dict(task_data), "status": "COMPLETED"},
    )
    monkeypatch.setattr(memory.storage, "save_task_record", lambda task_data: None)

    queue = InMemoryTaskQueue()
    task_data = {
        "task_contract_version": TASK_CONTRACT_VERSION,
        "task_id": "DF-TASK-QUEUE-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "last_updated_at": "2026-04-04T00:00:00Z",
        "intent": "queue_test",
        "payload": {},
        "status": "pending",
        "approval_status": "approved",
        "notes": [],
        "history": [],
    }
    persisted_statuses: list[str] = []

    def fetch_task(task_id: str) -> dict[str, object] | None:
        return task_data if task_id == "DF-TASK-QUEUE-V1" else None

    def persist(updated_task: dict[str, object]) -> None:
        task_data.update(updated_task)
        persisted_statuses.append(str(updated_task.get("status", "")))

    def executor(_: dict[str, object]) -> dict[str, object]:
        return build_action_result(
            status="completed",
            task_id="DF-TASK-QUEUE-V1",
            action_type="QUEUE_TEST",
            result_payload={"summary": "queue test passed"},
            error_code="",
            error_message="",
            source="tests.test_task_queue",
        )

    def decision_resolver(*args, **kwargs) -> dict[str, object]:
        return {"execution_mode": "LOCAL"}

    assert queue.enqueue_task("DF-TASK-QUEUE-V1") is True
    assert json.loads(queue_file.read_text(encoding="utf-8")) == [
        {"task_id": "DF-TASK-QUEUE-V1", "status": "pending"}
    ]

    executed_task = process_next_queued_task(
        queue=queue,
        now=lambda: "2026-04-04T00:00:00Z",
        fetch_task=fetch_task,
        persist=persist,
        timeout=0.0,
        executor=executor,
        telemetry_collector=lambda: {},
        network_snapshot_collector=lambda: {},
        decision_resolver=decision_resolver,
        active_task_loader=lambda: [],
    )

    assert executed_task is not None
    assert executed_task["status"] == "COMPLETED"
    assert json.loads(queue_file.read_text(encoding="utf-8")) == []

    log_output = _read_jsonl(log_file)
    assert [entry["event_type"] for entry in log_output] == [
        "queue_enqueue",
        "queue_dequeue",
    ]
    assert all(entry["task_id"] == "DF-TASK-QUEUE-V1" for entry in log_output)


def test_process_next_queued_task_prints_and_logs_mode_trace(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    queue_file = tmp_path / "runtime" / "state" / "task_queue.json"
    task_log_file = tmp_path / "runtime" / "logs" / "tasks.log"
    system_log_file = tmp_path / "runtime" / "logs" / "system.log"

    import app.orchestrator.task_queue as task_queue_module
    import runtime.system_log as system_log_module
    import app.orchestrator.execution_runner as execution_runner_module
    import app.orchestrator.task_worker as task_worker_module

    monkeypatch.setattr(task_queue_module, "TASK_QUEUE_FILE", queue_file)
    monkeypatch.setattr(task_queue_module, "TASK_LOG_FILE", task_log_file)
    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", system_log_file)
    monkeypatch.setattr(
        execution_runner_module.task_factory_module,
        "TASK_SYSTEM_FILE",
        tmp_path / "data" / "task_system.json",
    )
    monkeypatch.setattr(
        execution_runner_module,
        "assert_product_runtime_executor",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        execution_runner_module,
        "claim_execution_record",
        lambda **kwargs: {"claimed": True},
    )
    monkeypatch.setattr(
        execution_runner_module,
        "complete_execution_record",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        execution_runner_module,
        "read_execution_record",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        task_worker_module,
        "run_execution",
        lambda task_data, **kwargs: {**dict(task_data), "status": "COMPLETED"},
    )
    monkeypatch.setattr(memory.storage, "save_task_record", lambda task_data: None)

    queue = InMemoryTaskQueue()
    task_data = {
        "task_contract_version": TASK_CONTRACT_VERSION,
        "task_id": "DF-TRACE-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "last_updated_at": "2026-04-04T00:00:00Z",
        "intent": "trace_test",
        "payload": {},
        "status": "pending",
        "approval_status": "approved",
        "notes": [],
        "history": [],
    }

    def fetch_task(task_id: str) -> dict[str, object] | None:
        return task_data if task_id == "DF-TRACE-V1" else None

    def persist(updated_task: dict[str, object]) -> None:
        task_data.update(updated_task)

    def executor(_: dict[str, object]) -> dict[str, object]:
        return build_action_result(
            status="completed",
            task_id="DF-TRACE-V1",
            action_type="TRACE_TEST",
            result_payload={"summary": "trace test passed"},
            error_code="",
            error_message="",
            source="tests.test_task_queue",
        )

    def decision_resolver(*args, **kwargs) -> dict[str, object]:
        return {
            "execution_mode": "LOCAL",
            "execution_compute_mode": "cpu_mode",
        }

    assert queue.enqueue_task("DF-TRACE-V1") is True

    executed_task = process_next_queued_task(
        queue=queue,
        now=lambda: "2026-04-04T00:00:00Z",
        fetch_task=fetch_task,
        persist=persist,
        timeout=0.0,
        executor=executor,
        telemetry_collector=lambda: {},
        network_snapshot_collector=lambda: {},
        decision_resolver=decision_resolver,
        active_task_loader=lambda: [],
    )

    assert executed_task is not None
    captured = capsys.readouterr()
    assert "[MODE]" in captured.out
    assert "task: DF-TRACE-V1" in captured.out
    assert "execution: LOCAL" in captured.out
    assert "compute: cpu_mode" in captured.out

    system_log = _read_jsonl(system_log_file)
    mode_entry = next(entry for entry in system_log if entry["event_type"] == "mode")
    assert mode_entry["task_id"] == "DF-TRACE-V1"
    assert mode_entry["details"]["message"].startswith("[MODE]")
    assert "task: DF-TRACE-V1" in mode_entry["details"]["message"]
    assert "execution: LOCAL" in mode_entry["details"]["message"]
    assert "compute: cpu_mode" in mode_entry["details"]["message"]


def test_process_next_queued_task_skips_pending_approval(
    monkeypatch, tmp_path: Path
) -> None:
    queue_file = tmp_path / "runtime" / "state" / "task_queue.json"
    log_file = tmp_path / "runtime" / "logs" / "tasks.log"

    import app.orchestrator.task_queue as task_queue_module
    import app.orchestrator.execution_runner as execution_runner_module

    monkeypatch.setattr(task_queue_module, "TASK_QUEUE_FILE", queue_file)
    monkeypatch.setattr(task_queue_module, "TASK_LOG_FILE", log_file)
    monkeypatch.setattr(
        execution_runner_module.task_factory_module,
        "TASK_SYSTEM_FILE",
        tmp_path / "data" / "task_system.json",
    )
    monkeypatch.setattr(
        execution_runner_module,
        "assert_product_runtime_executor",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(
        execution_runner_module,
        "claim_execution_record",
        lambda **kwargs: {"claimed": True},
    )
    monkeypatch.setattr(
        execution_runner_module,
        "complete_execution_record",
        lambda **kwargs: None,
    )
    monkeypatch.setattr(
        execution_runner_module,
        "read_execution_record",
        lambda *args, **kwargs: None,
    )
    monkeypatch.setattr(memory.storage, "save_task_record", lambda task_data: None)

    queue = InMemoryTaskQueue()
    task_data = {
        "task_contract_version": TASK_CONTRACT_VERSION,
        "task_id": "DF-TASK-QUEUE-APPROVAL-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "last_updated_at": "2026-04-04T00:00:00Z",
        "intent": "approval_test",
        "payload": {},
        "status": "pending",
        "approval_status": "pending",
        "notes": [],
        "history": [],
    }

    def fetch_task(task_id: str) -> dict[str, object] | None:
        return task_data if task_id == "DF-TASK-QUEUE-APPROVAL-V1" else None

    assert queue.enqueue_task("DF-TASK-QUEUE-APPROVAL-V1") is True

    executed_task = process_next_queued_task(
        queue=queue,
        now=lambda: "2026-04-04T00:00:00Z",
        fetch_task=fetch_task,
        persist=lambda updated_task: task_data.update(updated_task),
        timeout=0.0,
        executor=lambda _: {"summary": "should not run"},
        telemetry_collector=lambda: {},
        network_snapshot_collector=lambda: {},
        decision_resolver=lambda *args, **kwargs: {"execution_mode": "LOCAL"},
        active_task_loader=lambda: [],
    )

    assert executed_task is None
    assert queue.queued_task_ids() == ["DF-TASK-QUEUE-APPROVAL-V1"]
