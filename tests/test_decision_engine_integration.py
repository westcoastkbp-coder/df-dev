from __future__ import annotations

import copy
import json
from pathlib import Path

import memory.memory_store as memory_store_module
import pytest
import runtime.pipeline.managed_execution as managed_execution_module

from app.execution.action_result import build_action_result
from app.execution.task_schema import TASK_CONTRACT_VERSION
from app.orchestrator.execution_runner import run_execution
import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_state_store as task_state_store_module


def _build_task(task_id: str) -> dict[str, object]:
    return {
        "task_contract_version": TASK_CONTRACT_VERSION,
        "task_id": task_id,
        "created_at": "2026-04-12T00:00:00Z",
        "intent": "write_file",
        "payload": {"path": r"runtime\out\decision.txt"},
        "status": "VALIDATED",
        "notes": [],
        "history": [],
        "interaction_id": task_id,
        "job_id": task_id,
        "trace_id": task_id,
    }


def _prime_required_execution_context(memory_dir: Path) -> None:
    memory_store_module.write_execution_system_context(
        copy.deepcopy(memory_store_module.DEFAULT_EXECUTION_SYSTEM_CONTEXT),
        memory_dir=memory_dir,
    )


def _configure_runtime(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store_module,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(
        task_factory_module,
        "TASK_SYSTEM_FILE",
        tmp_path / "data" / "task_system.json",
    )
    task_factory_module.clear_task_runtime_store()


def test_run_execution_records_decision_before_executor(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_runtime(monkeypatch, tmp_path)
    executor_calls = 0

    def executor(task_data: dict[str, object]) -> dict[str, object]:
        nonlocal executor_calls
        executor_calls += 1
        return build_action_result(
            status="completed",
            task_id=str(task_data.get("task_id", "")).strip(),
            action_type="WRITE_FILE",
            result_payload={"path": r"runtime\out\decision.txt"},
            error_code="",
            error_message="",
            source="test_executor",
            diagnostic_message="decision approved execution",
        )

    executed = run_execution(
        _build_task("DF-DECISION-REQUIRED-PASS-V1"),
        now=lambda: "2026-04-12T00:00:00Z",
        persist=lambda _: None,
        executor=executor,
    )

    assert executed["status"] == "COMPLETED"
    assert executor_calls == 1
    assert executed["result"]["decision_trace"]["action_type"] == "WRITE_FILE"
    assert executed["result"]["decision_trace"]["vendor"] == "openai"


def test_run_execution_supports_ai_decision_mode(monkeypatch, tmp_path: Path) -> None:
    _configure_runtime(monkeypatch, tmp_path)
    monkeypatch.setenv("DF_DECISION_MODE", "ai")
    executor_calls = 0

    def fake_call(*_args, **_kwargs) -> dict[str, object]:
        return {
            "action": "write_file",
            "action_type": "write",
            "target": r"runtime\out\decision.txt",
            "parameters": {
                "path": r"runtime\out\decision.txt",
            },
            "requires_confirmation": False,
            "reason": "AI approved write_file for the validated execution task.",
        }

    def executor(task_data: dict[str, object]) -> dict[str, object]:
        nonlocal executor_calls
        executor_calls += 1
        return build_action_result(
            status="completed",
            task_id=str(task_data.get("task_id", "")).strip(),
            action_type="WRITE_FILE",
            result_payload={"path": r"runtime\out\decision.txt"},
            error_code="",
            error_message="",
            source="test_executor",
            diagnostic_message="decision approved execution",
        )

    monkeypatch.setattr(
        managed_execution_module.decision_engine_module.orchestrator_client_module,
        "call_orchestrator",
        fake_call,
    )

    executed = run_execution(
        _build_task("DF-DECISION-AI-PASS-V1"),
        now=lambda: "2026-04-12T00:00:00Z",
        persist=lambda _: None,
        executor=executor,
    )

    assert executed["status"] == "COMPLETED"
    assert executor_calls == 1
    assert (
        executed["result"]["decision_trace"]["reason"] == "decision approved execution"
    )
    assert executed["result"]["decision_trace"]["vendor"] == "openai"


def test_run_execution_fails_when_decision_is_missing(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_runtime(monkeypatch, tmp_path)
    executor_calls = 0

    def executor(_: dict[str, object]) -> dict[str, object]:
        nonlocal executor_calls
        executor_calls += 1
        return {"status": "completed"}

    monkeypatch.setattr(
        managed_execution_module.decision_engine_module,
        "decide",
        lambda *_args, **_kwargs: None,
    )

    executed = run_execution(
        _build_task("DF-DECISION-MISSING-FAIL-V1"),
        now=lambda: "2026-04-12T00:00:00Z",
        persist=lambda _: None,
        executor=executor,
    )

    assert executed["status"] == "FAILED"
    assert executed["error"] == "decision engine must return an action plan dict"
    assert executor_calls == 0
    assert executed["result"]["error_code"] == "missing_decision"
    assert executed["result"]["decision_trace"]["policy_result"].startswith("blocked:")


def test_run_execution_decision_can_block_for_confirmation(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_runtime(monkeypatch, tmp_path)
    executor_calls = 0

    def executor(_: dict[str, object]) -> dict[str, object]:
        nonlocal executor_calls
        executor_calls += 1
        return {"status": "completed"}

    monkeypatch.setattr(
        managed_execution_module.decision_engine_module,
        "decide",
        lambda task, _context: {
            "task_id": str(task.get("task_id", "")).strip(),
            "action": "write_file",
            "action_type": "WRITE_FILE",
            "requires_confirmation": True,
            "reason": "manual confirmation required before write_file",
        },
    )

    executed = run_execution(
        _build_task("DF-DECISION-INFLUENCE-V1"),
        now=lambda: "2026-04-12T00:00:00Z",
        persist=lambda _: None,
        executor=executor,
    )

    assert executed["status"] == "FAILED"
    assert executed["error"] == "manual confirmation required before write_file"
    assert executor_calls == 0
    assert executed["result"]["error_code"] == "decision_confirmation_required"
    assert (
        executed["result"]["result_payload"]["action_plan"]["requires_confirmation"]
        is True
    )


def test_run_command_fails_without_decision(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    pytest.importorskip("requests")
    import scripts.run_command as run_command_module

    context_dir = tmp_path / "context"
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    artifact_dir = tmp_path / "artifacts"
    state_path = tmp_path / "state" / "state.json"
    execution_log_path = tmp_path / "logs" / "execution_log.jsonl"
    memory_dir = tmp_path / "memory"

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "EXECUTION_LOG_PATH", execution_log_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 202604120202)
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)

    executor_calls = 0

    def fake_run_codex_task(*_args, **_kwargs):
        nonlocal executor_calls
        executor_calls += 1
        raise AssertionError("command execution should not run without decision")

    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        fake_run_codex_task,
    )
    monkeypatch.setattr(
        run_command_module.decision_engine_module,
        "decide",
        lambda *_args, **_kwargs: None,
    )

    exit_code = run_command_module.main(["create doc from analysis"])

    assert exit_code == 1
    assert executor_calls == 0
    output_payload = json.loads(capsys.readouterr().out.strip())
    assert output_payload["result"]["status"] == "error"
    assert (
        output_payload["result"]["message"]
        == "decision engine must return an action plan dict"
    )
    assert output_payload["decision_trace"]["policy_result"].startswith("blocked:")
    assert not execution_log_path.exists()
