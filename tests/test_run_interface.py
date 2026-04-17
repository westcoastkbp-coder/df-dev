from __future__ import annotations

import copy
import json
from pathlib import Path

import memory.memory_store as memory_store_module
import scripts.run_command as run_command_module
import scripts.run_interface as run_interface_module


def _prime_required_execution_context(memory_dir: Path) -> None:
    memory_store_module.write_execution_system_context(
        copy.deepcopy(memory_store_module.DEFAULT_EXECUTION_SYSTEM_CONTEXT),
        memory_dir=memory_dir,
    )


def test_run_interface_routes_eb1_requests_to_owner_mode(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    context_dir = tmp_path / "context"
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    artifact_dir = tmp_path / "artifacts"
    state_path = tmp_path / "state" / "state.json"
    analysis_text = (
        "Action Steps:\n"
        "1. Build the EB1 evidence list.\n\n"
        "Priorities:\n"
        "1. Filing readiness.\n\n"
        "Next Moves:\n"
        "- Review strongest proof this week."
    )

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 1010101010)
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)

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

    exit_code = run_interface_module.main(["what should I do next for EB1"])

    output_payload = json.loads(capsys.readouterr().out)
    written_task = json.loads(task_path.read_text(encoding="utf-8"))
    system_context = json.loads((context_dir / "system_context.json").read_text(encoding="utf-8"))

    assert exit_code == 0
    assert output_payload == {
        "mode": "owner",
        "command": "owner task what should i do next for eb1",
        "status": "success",
        "response": analysis_text,
    }
    assert system_context["active_mode"] == "owner"
    assert written_task["context_mode"] == "owner"
    assert written_task["pipeline_route"] == "owner task"
    assert written_task["pipeline"][0]["input"]["text"] == "what should I do next for EB1"
    assert written_task["pipeline"][0]["input"]["context"] == {
        "context_summary": {
            "owner": {
                "name": "Anton Vorontsov",
                "role": "owner",
            },
            "business": {
                "name": "West Coast KBP",
                "type": "real operating business",
            },
            "product": {
                "name": "Execution OS",
                "status": "active development",
            },
            "architecture": {
                "model": "Execution OS",
                "rules": [
                    "external-first",
                    "role-based execution",
                    "human-as-verifier",
                    "adapters-as-hands",
                ],
            },
            "current_stage": {
                "phase": "system integration",
                "priority": "global context",
            },
            "active_flows": [],
            "recent_actions": [],
        },
        "identity": {},
        "immigration": {},
        "memory_summary": {
            "active_block": "",
            "architecture_rules": [],
            "core_status": "",
            "current_stage": "",
            "focus": "",
            "last_decisions": [],
            "next_step": "",
            "operating_phase": "",
            "owner_priorities": [],
            "system_mode": "",
        },
        "permits": {},
        "notes": "",
    }


def test_detect_mode_prefers_business_and_dev_defaults() -> None:
    assert run_interface_module.detect_mode("follow up with client about project status") == "business"
    assert run_interface_module.detect_mode("check logs for task runner") == "dev"


def test_run_interface_process_email_returns_sent_email_summary(
    monkeypatch,
    tmp_path: Path,
) -> None:
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    artifact_path = tmp_path / "artifacts" / "pipeline-1.json"
    state_path = tmp_path / "state" / "state.json"

    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", tmp_path / "context")
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)

    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text(
        json.dumps(
            {
                "final_output": {
                    "message_id": "msg-123",
                    "to": "jamie@example.com",
                    "subject": "Re: Client follow-up",
                    "email_sent": True,
                }
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(
        run_command_module,
        "execute_command_task_with_retry",
        lambda task_path: (True, "", artifact_path, ""),
    )

    exit_code, payload = run_interface_module.run_interface("process email")

    assert exit_code == 0
    assert payload == {
        "mode": "dev",
        "command": "process email",
        "status": "success",
        "response": {
            "action": "email sent",
            "status": "success",
            "output": "jamie@example.com | Re: Client follow-up",
        },
    }


def test_run_interface_prints_linkedin_draft_console_output(capsys) -> None:
    run_interface_module._print_payload(
        {
            "status": "success",
            "response": {
                "post_text": "Digital Foreman is becoming explainable in public.",
                "tone": "grounded and specific",
                "intent": "share progress and invite feedback",
            },
        }
    )

    assert capsys.readouterr().out.strip().splitlines() == [
        "Digital Foreman is becoming explainable in public.",
        "SUMMARY: grounded and specific | share progress and invite feedback",
        "SUGGESTION: Review and approve manually before posting.",
    ]


def test_run_interface_injects_context_summary_and_updates_recent_actions(
    monkeypatch,
    tmp_path: Path,
) -> None:
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    artifact_dir = tmp_path / "artifacts"
    state_path = tmp_path / "state" / "state.json"
    memory_dir = tmp_path / "memory"

    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", tmp_path / "context")
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 202604120101)
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)

    def fake_run_codex_task(task_source, artifact_dir=None, repo_root=None):
        payload = json.loads(Path(task_source).read_text(encoding="utf-8"))
        artifact_path = Path(artifact_dir) / f"pipeline-{payload['task_id']}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps({"final_output": {"analysis": "Structured summary"}}, indent=2) + "\n",
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

    exit_code, payload = run_interface_module.run_interface("analyze drive file")

    written_task = json.loads(task_path.read_text(encoding="utf-8"))
    system_context = json.loads((memory_dir / "system_context.json").read_text(encoding="utf-8"))
    claude_context = written_task["pipeline"][1]["input"]["context"]

    assert exit_code == 0
    assert payload["status"] == "success"
    assert claude_context["context_summary"]["product"]["name"] == "Execution OS"
    assert claude_context["context_summary"]["current_stage"]["phase"] == "system integration"
    assert system_context["last_actions"][-1] == "analyze drive file: completed"
