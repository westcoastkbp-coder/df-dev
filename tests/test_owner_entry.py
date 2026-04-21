from __future__ import annotations

import copy
import json
from pathlib import Path

import memory.memory_store as memory_store_module
import owner_entry as owner_entry_module
import scripts.run_command as run_command_module
import scripts.run_interface as run_interface_module


def _prime_required_execution_context(memory_dir: Path) -> None:
    memory_store_module.write_execution_system_context(
        copy.deepcopy(memory_store_module.DEFAULT_EXECUTION_SYSTEM_CONTEXT),
        memory_dir=memory_dir,
    )


def _configure_paths(monkeypatch, tmp_path: Path, *, task_id: int) -> tuple[Path, Path]:
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
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: task_id)
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)
    return task_path, artifact_dir


def test_handle_owner_input_creates_owner_command_task(
    monkeypatch,
    tmp_path: Path,
) -> None:
    task_path, artifact_dir = _configure_paths(
        monkeypatch, tmp_path, task_id=202604120201
    )
    analysis_text = "Action Steps:\n1. Review the owner queue."

    def fake_run_codex_task(task_source, artifact_dir=None, repo_root=None):
        payload = json.loads(Path(task_source).read_text(encoding="utf-8"))
        artifact_path = Path(artifact_dir) / f"pipeline-{payload['task_id']}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps({"final_output": {"analysis": analysis_text}}, indent=2) + "\n",
            encoding="utf-8",
        )
        return {
            "task_id": payload["task_id"],
            "pipeline_trace": [{"step_index": 1, "tool_ok": True}],
        }, artifact_path

    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        fake_run_codex_task,
    )

    result = owner_entry_module.handle_owner_input("Review the owner queue")

    written_task = json.loads(task_path.read_text(encoding="utf-8"))
    assert result["status"] == "success"
    assert result["task_id"] == "202604120201"
    assert written_task["task_type"] == "owner_command"
    assert written_task["input_text"] == "Review the owner queue"
    assert written_task["context_mode"] == "owner"
    assert written_task["pipeline_route"] == "owner task"
    assert written_task["context_summary"]["product"]["name"] == "Execution OS"
    assert written_task["pipeline"][0]["input"]["context"]["context_summary"][
        "product"
    ]["name"] == ("Execution OS")


def test_handle_owner_input_runs_through_decision_engine(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _task_path, artifact_dir = _configure_paths(
        monkeypatch, tmp_path, task_id=202604120202
    )
    captured: dict[str, object] = {}

    def fake_decide(task, context):
        captured["task"] = copy.deepcopy(task)
        captured["context"] = copy.deepcopy(context)
        return {
            "task_id": str(task["task_id"]),
            "requires_confirmation": False,
            "reason": "allowed",
        }

    monkeypatch.setattr(
        run_command_module.decision_engine_module,
        "decide",
        fake_decide,
    )
    monkeypatch.setattr(
        run_command_module.decision_engine_module,
        "validate_action_plan",
        lambda action_plan, expected_task_id=None: action_plan,
    )
    monkeypatch.setattr(
        run_command_module,
        "route_vendor",
        lambda task, context, action_plan: "google",
    )

    def fake_run_codex_task(task_source, artifact_dir=None, repo_root=None):
        payload = json.loads(Path(task_source).read_text(encoding="utf-8"))
        artifact_path = Path(artifact_dir) / f"pipeline-{payload['task_id']}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps({"final_output": {"analysis": "Decision path ok"}}, indent=2)
            + "\n",
            encoding="utf-8",
        )
        return {
            "task_id": payload["task_id"],
            "pipeline_trace": [{"step_index": 1, "tool_ok": True}],
        }, artifact_path

    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        fake_run_codex_task,
    )

    owner_entry_module.handle_owner_input("Plan next owner action")

    decision_task = captured["task"]
    decision_context = captured["context"]
    assert isinstance(decision_task, dict)
    assert decision_task["task_type"] == "owner_command"
    assert isinstance(decision_context, dict)
    assert decision_context["input_text"] == "Plan next owner action"
    assert decision_context["owner_command"]["input_text"] == "Plan next owner action"
    assert decision_context["owner_command"]["context_summary"]["product"]["name"] == (
        "Execution OS"
    )


def test_handle_owner_input_executes_action_and_returns_decision_trace(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _task_path, artifact_dir = _configure_paths(
        monkeypatch, tmp_path, task_id=202604120203
    )
    analysis_text = (
        "Action Steps:\n1. Review open priorities.\n\n"
        "Priorities:\n1. Resolve blocker.\n\n"
        "Next Moves:\n- Send next command."
    )
    captured_run: dict[str, bool] = {"called": False}

    def fake_run_codex_task(task_source, artifact_dir=None, repo_root=None):
        captured_run["called"] = True
        payload = json.loads(Path(task_source).read_text(encoding="utf-8"))
        artifact_path = Path(artifact_dir) / f"pipeline-{payload['task_id']}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps({"final_output": {"analysis": analysis_text}}, indent=2) + "\n",
            encoding="utf-8",
        )
        return {
            "task_id": payload["task_id"],
            "pipeline_trace": [{"step_index": 1, "tool_ok": True}],
        }, artifact_path

    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        fake_run_codex_task,
    )

    result = owner_entry_module.handle_owner_input("Review open priorities")

    assert captured_run["called"] is True
    assert result["result"] == analysis_text
    assert result["task_id"] == "202604120203"
    assert result["status"] == "success"
    assert result["decision_trace"]["reason"] == (
        "owner task review open priorities executed successfully"
    )
    assert (
        result["decision_trace"]["action_type"] == "owner task review open priorities"
    )
    assert result["decision_trace"]["policy_result"] == (
        "allowed: context loaded and command routed"
    )
    assert result["decision_trace"]["confidence"] == "high"
    assert str(result["decision_trace"]["vendor"]).strip()
    assert str(result["decision_trace"]["context_used"]).strip()


def test_run_interface_routes_owner_requests_through_owner_entry(monkeypatch) -> None:
    captured: dict[str, str] = {}

    def fake_handle_owner_input(text: str) -> dict[str, object]:
        captured["input_text"] = text
        return {
            "result": "noop",
            "task_id": "1",
            "status": "success",
            "decision_trace": {},
        }

    monkeypatch.setattr(
        owner_entry_module,
        "handle_owner_input",
        fake_handle_owner_input,
    )

    exit_code, payload = run_interface_module.run_interface(
        "Need help with immigration plan"
    )

    assert exit_code == 0
    assert captured["input_text"] == "Need help with immigration plan"
    assert payload == {
        "mode": "owner",
        "command": "owner task need help with immigration plan",
        "status": "success",
        "response": "noop",
    }


def test_run_command_owner_input_delegates_to_owner_entry(
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(
        owner_entry_module,
        "handle_owner_input",
        lambda text: {
            "result": f"handled: {text}",
            "task_id": "77",
            "status": "success",
            "decision_trace": {"reason": "delegated"},
        },
    )

    exit_code = run_command_module.main(["owner", "input", "review", "queue"])

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out.strip()) == {
        "decision_trace": {"reason": "delegated"},
        "result": "handled: review queue",
        "status": "success",
        "task_id": "77",
    }
