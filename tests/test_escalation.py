from __future__ import annotations

import json
from pathlib import Path

import app.execution.paths as paths_module
import app.orchestrator.escalation as escalation_module
import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_state_store as task_state_store_module
from app.orchestrator.execution_runner import run_execution


def _configure_runtime(monkeypatch, tmp_path: Path) -> Path:
    store_path = tmp_path / "data" / "tasks.json"
    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store_module,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(paths_module, "TASKS_FILE", store_path)
    monkeypatch.setattr(task_factory_module, "TASK_SYSTEM_FILE", store_path)
    monkeypatch.setattr(
        escalation_module,
        "ESCALATION_LOG_FILE",
        tmp_path / "runtime" / "logs" / "escalations.jsonl",
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


def _build_validated_task(
    store_path: Path, *, task_id: str, priority: str = "NORMAL"
) -> dict[str, object]:
    task = task_factory_module.create_task(
        {
            "task_id": task_id,
            "created_at": "2026-04-05T00:00:00Z",
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": "escalation-test", "priority": priority},
        },
        store_path=store_path,
    )
    task["status"] = "VALIDATED"
    task["payload"]["priority"] = priority
    return task_factory_module.save_task(task, store_path=store_path)


def test_invalid_action_result_triggers_escalation(monkeypatch, tmp_path: Path) -> None:
    store_path = _configure_runtime(monkeypatch, tmp_path)
    log_file = tmp_path / "runtime" / "logs" / "escalations.jsonl"
    task = _build_validated_task(store_path, task_id="DF-ESCALATE-INVALID-V1")

    def invalid_executor(_: dict[str, object]) -> dict[str, object]:
        return {"summary": "invalid"}

    invalid_executor.__module__ = "test_executor"

    executed = run_execution(
        dict(task),
        now=lambda: "2026-04-05T00:00:00Z",
        persist=lambda task_data: task_factory_module.save_task(
            task_data, store_path=store_path
        ),
        executor=invalid_executor,
    )

    assert executed["status"] == "FAILED"
    assert executed["runtime_verdict"]["escalation_signal"] == {
        "status": "escalation_required",
        "task_id": "DF-ESCALATE-INVALID-V1",
        "reason": "invalid_action_result",
        "severity": "critical",
    }
    assert (
        _read_jsonl(log_file)[0]["details"]
        == executed["runtime_verdict"]["escalation_signal"]
    )


def test_normal_failure_does_not_trigger_escalation() -> None:
    decision = escalation_module.decide_escalation_action(
        {
            "task_id": "DF-ESCALATE-NORMAL-V1",
            "status": "FAILED",
            "history": [{"event": "execution_failed"}],
            "payload": {"priority": "NORMAL"},
            "result": {"status": "failed"},
        },
        reason="execution failed",
    )

    assert decision == {"action": "fail", "signal": None}


def test_identical_runs_produce_same_escalation() -> None:
    task = {
        "task_id": "DF-ESCALATE-REPEAT-V1",
        "status": "FAILED",
        "history": [
            {"event": "execution_failed"},
            {"event": "execution_failed"},
        ],
        "payload": {"priority": "NORMAL"},
        "result": {"status": "failed"},
    }

    first = escalation_module.decide_escalation_action(task, reason="execution failed")
    second = escalation_module.decide_escalation_action(task, reason="execution failed")

    assert (
        first
        == second
        == {
            "action": "escalate",
            "signal": {
                "status": "escalation_required",
                "task_id": "DF-ESCALATE-REPEAT-V1",
                "reason": "repeated_task_failure",
                "severity": "medium",
            },
        }
    )
