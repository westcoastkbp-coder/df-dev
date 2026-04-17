from __future__ import annotations

import json

from scripts import derive_state as derive_state_module
from scripts import orchestrator


def test_orchestrate_derives_state_after_pending_task(monkeypatch, tmp_path) -> None:
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    queue_path = tasks_dir / "task_queue.json"
    task_path = tasks_dir / "task_write_file.json"
    queue_path.write_text('[{"task_id": "task_write_file"}]\n', encoding="utf-8")
    task_path.write_text(
        json.dumps({"task_id": "task_write_file", "status": "pending"}) + "\n",
        encoding="utf-8",
    )

    calls: list[str] = []

    def fake_run_task(path):
        calls.append(f"run:{path.name}")
        return {"task_id": "task_write_file", "status": "completed"}

    def fake_derive_state():
        calls.append("derive")
        return {"last_event_id": "evt_test"}

    monkeypatch.setattr(orchestrator, "TASKS_DIR", tasks_dir)
    monkeypatch.setattr(orchestrator, "TASK_QUEUE_PATH", queue_path)
    monkeypatch.setattr(orchestrator, "run_task", fake_run_task)
    monkeypatch.setattr(orchestrator, "derive_state", fake_derive_state)

    results = orchestrator.orchestrate()

    assert results == [{"task_id": "task_write_file", "status": "completed"}]
    assert calls == ["run:task_write_file.json", "derive"]


def test_orchestrate_skips_completed_and_logs_failed(monkeypatch, tmp_path, capsys) -> None:
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    queue_path = tasks_dir / "task_queue.json"
    queue_path.write_text(
        json.dumps(
            [
                {"task_id": "task_completed"},
                {"task_id": "task_failed"},
                {"task_id": "task_pending"},
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (tasks_dir / "task_completed.json").write_text(
        json.dumps({"task_id": "task_completed", "status": "completed"}) + "\n",
        encoding="utf-8",
    )
    (tasks_dir / "task_failed.json").write_text(
        json.dumps(
            {
                "task_id": "task_failed",
                "status": "failed",
                "retries": 1,
                "max_retries": 3,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (tasks_dir / "task_pending.json").write_text(
        json.dumps({"task_id": "task_pending", "status": "pending"}) + "\n",
        encoding="utf-8",
    )

    calls: list[str] = []

    def fake_run_task(path):
        calls.append(f"run:{path.name}")
        return {"task_id": "task_pending", "status": "completed"}

    monkeypatch.setattr(orchestrator, "TASKS_DIR", tasks_dir)
    monkeypatch.setattr(orchestrator, "TASK_QUEUE_PATH", queue_path)
    monkeypatch.setattr(orchestrator, "run_task", fake_run_task)
    monkeypatch.setattr(orchestrator, "derive_state", lambda: calls.append("derive"))

    results = orchestrator.orchestrate()
    output = capsys.readouterr().out

    assert results == [{"task_id": "task_pending", "status": "completed"}]
    assert calls == ["run:task_pending.json", "derive"]
    assert "FAILED_TASK: task_failed retries=1 max_retries=3" in output


def test_orchestrate_logs_newly_failed_execution(monkeypatch, tmp_path, capsys) -> None:
    tasks_dir = tmp_path / "tasks"
    tasks_dir.mkdir()
    queue_path = tasks_dir / "task_queue.json"
    task_path = tasks_dir / "task_pending.json"
    queue_path.write_text('[{"task_id": "task_pending"}]\n', encoding="utf-8")
    task_path.write_text(
        json.dumps({"task_id": "task_pending", "status": "pending"}) + "\n",
        encoding="utf-8",
    )

    calls: list[str] = []

    def fake_run_task(path):
        calls.append(f"run:{path.name}")
        return {
            "task_id": "task_pending",
            "status": "failed",
            "retries": 2,
            "max_retries": 3,
        }

    monkeypatch.setattr(orchestrator, "TASKS_DIR", tasks_dir)
    monkeypatch.setattr(orchestrator, "TASK_QUEUE_PATH", queue_path)
    monkeypatch.setattr(orchestrator, "run_task", fake_run_task)
    monkeypatch.setattr(orchestrator, "derive_state", lambda: calls.append("derive"))

    results = orchestrator.orchestrate()
    output = capsys.readouterr().out

    assert results == [
        {
            "task_id": "task_pending",
            "status": "failed",
            "retries": 2,
            "max_retries": 3,
        }
    ]
    assert calls == ["run:task_pending.json", "derive"]
    assert "FAILED_TASK: task_pending retries=2 max_retries=3" in output


def test_derive_state_rewrites_current_state_from_event_log(monkeypatch, tmp_path) -> None:
    event_log_path = tmp_path / "event_log.jsonl"
    state_path = tmp_path / "current_state.json"
    event_log_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "event_id": "evt_0001",
                        "event_type": "system_init",
                        "verification": {"status": "success"},
                    }
                ),
                json.dumps(
                    {
                        "event_id": "evt_0002",
                        "event_type": "action_execution",
                        "verification": {"status": "failed"},
                    }
                ),
                json.dumps(
                    {
                        "event_id": "evt_0003",
                        "event_type": "action_execution",
                        "verification": {"status": "success"},
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(derive_state_module, "EVENT_LOG_PATH", event_log_path)
    monkeypatch.setattr(derive_state_module, "STATE_PATH", state_path)

    state = derive_state_module.derive_state()

    assert state == {
        "system_status": "running",
        "memory_layer": "active",
        "last_event_id": "evt_0003",
    }
    assert json.loads(state_path.read_text(encoding="utf-8")) == state


if __name__ == "__main__":
    raise SystemExit("Run with pytest")
