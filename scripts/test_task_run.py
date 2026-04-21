from __future__ import annotations

import json

from scripts import run_task as run_task_module


def test_run_task_requeues_failed_task_before_max_retries(
    monkeypatch, tmp_path
) -> None:
    task_path = tmp_path / "task.json"
    queue_path = tmp_path / "task_queue.json"
    event_log_path = tmp_path / "event_log.jsonl"
    event_log_path.write_text(
        json.dumps(
            {
                "event_id": "evt_failed",
                "verification": {"status": "failed"},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    task_path.write_text(
        json.dumps(
            {
                "task_id": "task_write_file",
                "created_at": "2026-01-01T00:00:00Z",
                "status": "pending",
                "retries": 0,
                "max_retries": 3,
                "action": {"type": "write_file", "input": {"path": "output/test.txt"}},
                "result": {"status": "pending", "output": {}},
                "verification": {"status": "pending"},
                "events": [],
                "next_tasks": ["task_002"],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(run_task_module, "TASK_QUEUE_PATH", queue_path)
    monkeypatch.setattr(run_task_module, "EVENT_LOG_PATH", event_log_path)
    monkeypatch.setattr(
        run_task_module,
        "execute_action",
        lambda action_type, action_input: {"status": "success"},
    )

    task = run_task_module.run_task(task_path)
    queue = json.loads(queue_path.read_text(encoding="utf-8"))

    assert task["status"] == "failed"
    assert task["verification"]["status"] == "failed"
    assert task["retries"] == 1
    assert task["events"] == ["evt_failed"]
    assert queue == [{"task_id": "task_write_file"}]


def test_run_task_marks_failure_terminal_after_max_retries(
    monkeypatch, tmp_path
) -> None:
    task_path = tmp_path / "task.json"
    queue_path = tmp_path / "task_queue.json"
    task_path.write_text(
        json.dumps(
            {
                "task_id": "task_write_file",
                "created_at": "2026-01-01T00:00:00Z",
                "status": "pending",
                "retries": 3,
                "max_retries": 3,
                "action": {"type": "write_file", "input": {"path": "output/test.txt"}},
                "result": {"status": "pending", "output": {}},
                "verification": {"status": "pending"},
                "events": [],
                "next_tasks": ["task_002"],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(run_task_module, "TASK_QUEUE_PATH", queue_path)
    monkeypatch.setattr(
        run_task_module,
        "execute_action",
        lambda action_type, action_input: {"status": "failed"},
    )

    task = run_task_module.run_task(task_path)

    assert task["status"] == "failed"
    assert task["retries"] == 3
    assert not queue_path.exists()


def test_run_task_appends_next_tasks_after_success(monkeypatch, tmp_path) -> None:
    task_path = tmp_path / "task.json"
    queue_path = tmp_path / "task_queue.json"
    task_path.write_text(
        json.dumps(
            {
                "task_id": "task_write_file",
                "created_at": "2026-01-01T00:00:00Z",
                "status": "pending",
                "action": {"type": "write_file", "input": {"path": "output/test.txt"}},
                "result": {"status": "pending", "output": {}},
                "verification": {"status": "pending"},
                "events": [],
                "next_tasks": ["task_002", "task_003"],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(run_task_module, "TASK_QUEUE_PATH", queue_path)
    monkeypatch.setattr(
        run_task_module,
        "execute_action",
        lambda action_type, action_input: {"status": "success"},
    )

    task = run_task_module.run_task(task_path)
    queue = json.loads(queue_path.read_text(encoding="utf-8"))

    assert task["status"] == "completed"
    assert task["retries"] == 0
    assert task["max_retries"] == 3
    assert queue == [{"task_id": "task_002"}, {"task_id": "task_003"}]


if __name__ == "__main__":
    raise SystemExit("Run with pytest")
