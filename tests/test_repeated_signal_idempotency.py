from __future__ import annotations

from pathlib import Path

import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_queue as task_queue_module
import app.orchestrator.task_state_store as task_state_store_module
import app.orchestrator.task_worker as task_worker_module
from tests.system_context import WORKING_SYSTEM_CONTEXT


def _configure_runtime(monkeypatch, tmp_path: Path) -> Path:
    store_path = tmp_path / "data" / "task_system.json"
    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store_module,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(task_factory_module, "TASK_SYSTEM_FILE", store_path)
    monkeypatch.setattr(
        task_queue_module,
        "TASK_QUEUE_FILE",
        tmp_path / "runtime" / "state" / "task_queue.json",
    )
    monkeypatch.setattr(
        task_queue_module, "TASK_LOG_FILE", tmp_path / "runtime" / "logs" / "tasks.log"
    )
    task_factory_module.clear_task_runtime_store()
    return store_path


def test_repeated_identical_text_signal_creates_single_task_single_execution_and_stable_fallback(
    monkeypatch,
    tmp_path: Path,
) -> None:
    store_path = _configure_runtime(monkeypatch, tmp_path)
    signal_text = "Создать задачу: перезвонить клиенту John Doe"
    created_task_ids: list[str] = []

    for _ in range(50):
        task = task_factory_module.create_task(
            {
                "status": "awaiting_approval",
                "text": signal_text,
                "payload": {
                    "summary": signal_text,
                    "fallback": "call_client",
                },
            },
            store_path=store_path,
        )
        created_task_ids.append(str(task.get("task_id", "")).strip())

    tasks_after_creation = task_factory_module.load_tasks(store_path)
    assert len(set(created_task_ids)) == 1
    assert len(tasks_after_creation) == 1
    assert (
        sum(task["status"] == "AWAITING_APPROVAL" for task in tasks_after_creation) == 1
    )

    approved_task = task_factory_module.apply_task_approval(
        created_task_ids[0],
        approved=True,
        approved_by="stress-test",
        store_path=store_path,
    )
    approval_events = [
        entry
        for entry in list(approved_task.get("history", []))
        if str(entry.get("event", "")).strip() == "approval_granted"
    ]
    assert len(approval_events) == 1

    queue = task_queue_module.InMemoryTaskQueue()
    queued_results = [queue.enqueue_task(task_id) for task_id in created_task_ids]
    assert queued_results.count(True) == 1

    execution_calls: list[str] = []
    fallback_observed: list[str] = []
    persisted_task = dict(approved_task)

    def fetch_task(task_id: str) -> dict[str, object] | None:
        return dict(persisted_task) if task_id == persisted_task["task_id"] else None

    def persist(updated_task: dict[str, object]) -> None:
        nonlocal persisted_task
        persisted_task = dict(updated_task)

    def fake_run_execution(task_data: dict[str, object], **kwargs) -> dict[str, object]:
        execution_calls.append(str(task_data.get("task_id", "")).strip())
        fallback_observed.append(
            str(dict(task_data.get("payload", {})).get("fallback", "")).strip()
        )
        return {
            **dict(task_data),
            "status": "COMPLETED",
            "result": {
                "outcome": "completed",
                "fallback": str(
                    dict(task_data.get("payload", {})).get("fallback", "")
                ).strip(),
            },
        }

    monkeypatch.setattr(task_worker_module, "run_execution", fake_run_execution)

    processed = task_worker_module.process_next_queued_task(
        queue=queue,
        now=lambda: "2026-04-05T12:00:00Z",
        fetch_task=fetch_task,
        persist=persist,
        timeout=0.0,
        executor=lambda task_data: dict(task_data),
        telemetry_collector=lambda: {},
        network_snapshot_collector=lambda: {},
        decision_resolver=lambda *args, **kwargs: {"execution_mode": "LOCAL"},
        active_task_loader=lambda: [dict(persisted_task)],
        system_context=WORKING_SYSTEM_CONTEXT,
    )

    observed = {
        "number_of_tasks_created": len(task_factory_module.load_tasks(store_path)),
        "number_of_executions": len(execution_calls),
        "fallback_variation": len(set(fallback_observed)) > 1,
        "PASS/FAIL": "PASS"
        if len(task_factory_module.load_tasks(store_path)) == 1
        and len(approval_events) == 1
        and len(execution_calls) == 1
        and len(set(fallback_observed)) <= 1
        and processed is not None
        and str(processed.get("status", "")).strip() == "COMPLETED"
        else "FAIL",
    }

    assert observed == {
        "number_of_tasks_created": 1,
        "number_of_executions": 1,
        "fallback_variation": False,
        "PASS/FAIL": "PASS",
    }
