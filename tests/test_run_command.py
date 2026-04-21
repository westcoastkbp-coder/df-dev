from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from control.context_store import update_context
from control.state_store import append_command_state, get_last_command
import memory.memory_store as memory_store_module
import scripts.run_command as run_command_module


def _default_memory_summary() -> dict[str, object]:
    return {
        "active_block": "",
        "core_status": "",
        "current_stage": "",
        "focus": "",
        "last_decisions": [],
        "next_step": "",
        "operating_phase": "",
        "architecture_rules": [],
        "owner_priorities": [],
        "system_mode": "",
    }


def _default_context_summary() -> dict[str, object]:
    return {
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
    }


def _prime_required_execution_context(memory_dir: Path) -> None:
    memory_store_module.write_execution_system_context(
        copy.deepcopy(memory_store_module.DEFAULT_EXECUTION_SYSTEM_CONTEXT),
        memory_dir=memory_dir,
    )


def _assert_output_contract(payload: dict[str, object], *, tool_source: str) -> None:
    assert {"result", "execution_trace"}.issubset(payload)
    execution_trace = payload["execution_trace"]
    assert isinstance(execution_trace, dict)
    assert set(execution_trace) == {
        "execution_id",
        "memory_state",
        "timestamp",
        "tool_source",
    }
    assert execution_trace["tool_source"] == tool_source
    assert isinstance(execution_trace["execution_id"], str)
    assert execution_trace["execution_id"]
    assert isinstance(execution_trace["timestamp"], str)
    assert execution_trace["timestamp"]
    assert execution_trace["memory_state"] == {
        "core_status": execution_trace["memory_state"]["core_status"],
        "operating_phase": execution_trace["memory_state"]["operating_phase"],
        "system_mode": execution_trace["memory_state"]["system_mode"],
    }
    if "decision_trace" in payload:
        assert isinstance(payload["decision_trace"], dict)


def _claude_input(
    instruction: str,
    text: str,
    selected_context: dict[str, object] | None = None,
    *,
    owner_mode: bool = False,
) -> dict[str, object]:
    resolved_instruction = instruction
    if owner_mode:
        resolved_instruction = (
            f"{run_command_module.OWNER_ANALYZE_PREAMBLE}\n\n{instruction}"
        )
    payload: dict[str, object] = {
        "instruction": resolved_instruction,
        "text": text,
    }
    if selected_context is not None:
        payload["context"] = selected_context
    return payload


def _expected_summarize_drive_file_task(
    task_id: int,
    *,
    selected_context: dict[str, object] | None = None,
    owner_mode: bool = False,
) -> dict[str, object]:
    return {
        "task_id": task_id,
        "instruction": run_command_module.PIPELINE_TASK_INSTRUCTION,
        "force_execution": True,
        "command_name": "summarize drive file",
        "pipeline_route": "summarize drive file",
        "pipeline": [
            {
                "tool_name": "google_drive.read_file",
                "input": {
                    "file_id": run_command_module.DEFAULT_TEST_FILE_ID,
                },
                "output_key": "drive_data",
            },
            {
                "tool_name": "claude.analyze",
                "input": _claude_input(
                    "Summarize this document in 3 sentences",
                    "{{drive_data.content_text}}",
                    selected_context,
                    owner_mode=owner_mode,
                ),
                "output_key": "analysis_data",
            },
            {
                "tool_name": "google_docs.create_document",
                "input": {
                    "title": "DF COMMAND RESULT",
                    "content": "{{analysis_data.analysis}}",
                },
            },
        ],
    }


def _expected_analyze_drive_file_task(
    task_id: int,
    *,
    selected_context: dict[str, object] | None = None,
    owner_mode: bool = False,
) -> dict[str, object]:
    return {
        "task_id": task_id,
        "instruction": run_command_module.PIPELINE_TASK_INSTRUCTION,
        "force_execution": True,
        "command_name": "analyze drive file",
        "pipeline_route": "analyze drive file",
        "pipeline": [
            {
                "tool_name": "google_drive.read_file",
                "input": {
                    "file_id": run_command_module.DEFAULT_TEST_FILE_ID,
                },
                "output_key": "drive_data",
            },
            {
                "tool_name": "claude.analyze",
                "input": _claude_input(
                    "Analyze this document and produce a concise structured summary",
                    "{{drive_data.content_text}}",
                    selected_context,
                    owner_mode=owner_mode,
                ),
                "output_key": "analysis_data",
            },
        ],
    }


def _expected_create_doc_from_analysis_task(
    task_id: int,
    *,
    selected_context: dict[str, object] | None = None,
    owner_mode: bool = False,
) -> dict[str, object]:
    return {
        "task_id": task_id,
        "instruction": run_command_module.PIPELINE_TASK_INSTRUCTION,
        "force_execution": True,
        "command_name": "create doc from analysis",
        "pipeline_route": "create doc from analysis",
        "pipeline": [
            {
                "tool_name": "claude.analyze",
                "input": _claude_input(
                    "Summarize this text in 3 sentences",
                    run_command_module.ANALYSIS_SAMPLE_TEXT,
                    selected_context,
                    owner_mode=owner_mode,
                ),
                "output_key": "analysis_data",
            },
            {
                "tool_name": "google_docs.create_document",
                "input": {
                    "title": "DF ANALYSIS DOC",
                    "content": "{{analysis_data.analysis}}",
                },
            },
        ],
    }


def _expected_process_email_task(
    task_id: int,
    *,
    selected_context: dict[str, object] | None = None,
    owner_mode: bool = False,
) -> dict[str, object]:
    return {
        "task_id": task_id,
        "instruction": run_command_module.PIPELINE_TASK_INSTRUCTION,
        "force_execution": True,
        "command_name": "process email",
        "pipeline_route": "process email",
        "pipeline": [
            {
                "tool_name": "gmail.read_latest",
                "input": {},
                "output_key": "email_data",
            },
            {
                "tool_name": "claude.analyze",
                "input": _claude_input(
                    run_command_module.EMAIL_ANALYZE_INSTRUCTION,
                    (
                        "Subject: {{email_data.subject}}\n"
                        "Sender: {{email_data.sender}}\n\n"
                        "Body:\n{{email_data.body_text}}"
                    ),
                    selected_context,
                    owner_mode=owner_mode,
                ),
                "output_key": "analysis_data",
            },
            {
                "tool_name": "google.gmail.send",
                "input": {
                    "to": "{{email_data.reply_to_email}}",
                    "subject": "Re: {{email_data.subject}}",
                    "body": "{{analysis_data.analysis}}",
                },
            },
        ],
    }


def _expected_owner_task(
    task_id: int,
    owner_request: str,
    *,
    selected_context: dict[str, object] | None = None,
) -> dict[str, object]:
    normalized_command = f"owner task {' '.join(owner_request.lower().split())}"
    return {
        "task_id": task_id,
        "instruction": run_command_module.PIPELINE_TASK_INSTRUCTION,
        "force_execution": True,
        "command_name": normalized_command,
        "pipeline_route": "owner task",
        "context_mode": "owner",
        "print_analysis": True,
        "pipeline": [
            {
                "tool_name": "claude.analyze",
                "input": _claude_input(
                    run_command_module.OWNER_TASK_INSTRUCTION,
                    owner_request,
                    selected_context,
                    owner_mode=True,
                ),
                "output_key": "analysis_data",
            }
        ],
    }


def _expected_http_request_task(
    task_id: int, method: str, url: str
) -> dict[str, object]:
    return {
        "task_id": task_id,
        "instruction": run_command_module.PIPELINE_TASK_INSTRUCTION,
        "force_execution": True,
        "command_name": f"http {method.lower()} {url}",
        "pipeline_route": "http request",
        "requires_planning": True,
        "roles": ["planner", "executor", "verifier"],
        "planning_route": "http_request",
        "planning_input": {
            "method": method.upper(),
            "url": url,
        },
        "pipeline": [],
    }


def test_main_summarize_drive_file_maps_correctly_and_prints_doc_url(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    context_dir = tmp_path / "context"
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    artifact_dir = tmp_path / "artifacts"
    state_path = tmp_path / "state" / "state.json"
    doc_url = "https://docs.google.com/document/d/doc-command-result"

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 1234567890)
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)

    def fake_run_codex_task(task_source, artifact_dir=None, repo_root=None):
        payload = json.loads(Path(task_source).read_text(encoding="utf-8"))
        artifact_path = Path(artifact_dir) / f"pipeline-{payload['task_id']}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps({"final_output": {"url": doc_url}}, indent=2) + "\n",
            encoding="utf-8",
        )
        return {
            "task_id": payload["task_id"],
            "doc_url": doc_url,
        }, artifact_path

    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        fake_run_codex_task,
    )

    exit_code = run_command_module.main(["summarize drive file"])

    written_task = json.loads(task_path.read_text(encoding="utf-8"))
    payload = json.loads(capsys.readouterr().out.strip())

    assert exit_code == 0
    _assert_output_contract(payload, tool_source="external")
    assert payload["result"] == doc_url
    assert written_task == _expected_summarize_drive_file_task(
        1234567890,
        selected_context={
            "context_summary": _default_context_summary(),
            "memory_summary": _default_memory_summary(),
        },
    )
    state_payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert state_payload["commands"][-1] == {
        "artifact": str(artifact_dir / "pipeline-1234567890.json"),
        "command": "summarize drive file",
        "doc_id": "doc-command-result",
        "result": "SUCCESS",
        "timestamp": state_payload["commands"][-1]["timestamp"],
    }


def test_main_analyze_drive_file_maps_correctly_and_prints_success_only(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    context_dir = tmp_path / "context"
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    artifact_dir = tmp_path / "artifacts"
    state_path = tmp_path / "state" / "state.json"

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 987654321)
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)

    def fake_run_codex_task(task_source, artifact_dir=None, repo_root=None):
        payload = json.loads(Path(task_source).read_text(encoding="utf-8"))
        artifact_path = Path(artifact_dir) / f"pipeline-{payload['task_id']}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps({"final_output": {"analysis": "Structured summary"}}, indent=2)
            + "\n",
            encoding="utf-8",
        )
        return {
            "task_id": payload["task_id"],
            "pipeline_trace": [
                {"step_index": 1, "tool_ok": True},
                {"step_index": 2, "tool_ok": True},
            ],
        }, artifact_path

    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        fake_run_codex_task,
    )

    exit_code = run_command_module.main(["analyze drive file"])

    written_task = json.loads(task_path.read_text(encoding="utf-8"))
    payload = json.loads(capsys.readouterr().out.strip())

    assert exit_code == 0
    _assert_output_contract(payload, tool_source="external")
    assert payload["result"] == ""
    assert written_task == _expected_analyze_drive_file_task(
        987654321,
        selected_context={
            "context_summary": _default_context_summary(),
            "memory_summary": _default_memory_summary(),
        },
    )
    state_payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert state_payload["commands"][-1]["command"] == "analyze drive file"
    assert state_payload["commands"][-1]["result"] == "SUCCESS"
    assert state_payload["commands"][-1]["doc_id"] == ""


def test_main_create_doc_from_analysis_maps_correctly_and_prints_doc_url(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    context_dir = tmp_path / "context"
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    artifact_dir = tmp_path / "artifacts"
    state_path = tmp_path / "state" / "state.json"
    doc_url = "https://docs.google.com/document/d/doc-analysis-result"

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 456789123)
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)

    def fake_run_codex_task(task_source, artifact_dir=None, repo_root=None):
        payload = json.loads(Path(task_source).read_text(encoding="utf-8"))
        artifact_path = Path(artifact_dir) / f"pipeline-{payload['task_id']}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps({"final_output": {"url": doc_url}}, indent=2) + "\n",
            encoding="utf-8",
        )
        return {
            "task_id": payload["task_id"],
            "doc_url": doc_url,
        }, artifact_path

    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        fake_run_codex_task,
    )

    exit_code = run_command_module.main(["create doc from analysis"])

    written_task = json.loads(task_path.read_text(encoding="utf-8"))
    payload = json.loads(capsys.readouterr().out.strip())

    assert exit_code == 0
    _assert_output_contract(payload, tool_source="external")
    assert payload["result"] == doc_url
    assert written_task == _expected_create_doc_from_analysis_task(
        456789123,
        selected_context={
            "context_summary": _default_context_summary(),
            "memory_summary": _default_memory_summary(),
        },
    )


def test_main_process_email_maps_correctly_and_prints_subject_and_send_status(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    context_dir = tmp_path / "context"
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    artifact_dir = tmp_path / "artifacts"
    state_path = tmp_path / "state" / "state.json"

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 555666777)
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)

    def fake_run_codex_task(task_source, artifact_dir=None, repo_root=None):
        payload = json.loads(Path(task_source).read_text(encoding="utf-8"))
        artifact_path = Path(artifact_dir) / f"pipeline-{payload['task_id']}.json"
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
        return {
            "task_id": payload["task_id"],
            "pipeline_trace": [
                {"step_index": 1, "tool_ok": True},
                {"step_index": 2, "tool_ok": True},
                {"step_index": 3, "tool_ok": True},
            ],
        }, artifact_path

    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        fake_run_codex_task,
    )

    exit_code = run_command_module.main(["process email"])

    written_task = json.loads(task_path.read_text(encoding="utf-8"))
    payload = json.loads(capsys.readouterr().out.strip())

    assert exit_code == 0
    _assert_output_contract(payload, tool_source="external")
    assert payload["result"] == {
        "action": "email sent",
        "status": "success",
        "output": "jamie@example.com | Re: Client follow-up",
    }
    assert written_task == _expected_process_email_task(
        555666777,
        selected_context={
            "context_summary": _default_context_summary(),
            "memory_summary": _default_memory_summary(),
        },
    )
    state_payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert state_payload["commands"][-1]["command"] == "process email"
    assert state_payload["commands"][-1]["result"] == "SUCCESS"
    project_state = memory_store_module.get_project_state()
    assert project_state["last_action"] == "process email"
    assert project_state["last_result_summary"] == "email sent"


def test_main_unknown_command_returns_unknown_command(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    state_path = tmp_path / "state" / "state.json"
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("runner should not be called")
        ),
    )

    exit_code = run_command_module.main(["open spreadsheet"])

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().out.strip())
    _assert_output_contract(payload, tool_source="external")
    assert payload["result"] == {
        "status": "error",
        "message": "UNKNOWN COMMAND",
    }
    assert not task_path.exists()


def test_main_empty_input_failure_prints_reason(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    context_dir = tmp_path / "context"
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    artifact_dir = tmp_path / "artifacts"
    state_path = tmp_path / "state" / "state.json"

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 111222333)
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)

    def fake_run_codex_task(task_source, artifact_dir=None, repo_root=None):
        payload = json.loads(Path(task_source).read_text(encoding="utf-8"))
        artifact_path = Path(artifact_dir) / f"pipeline-{payload['task_id']}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps(
                {"failure_reason": "EMPTY_INPUT", "reason": "EMPTY_INPUT"}, indent=2
            )
            + "\n",
            encoding="utf-8",
        )
        return {
            "task_id": payload["task_id"],
            "failure_reason": "EMPTY_INPUT",
            "pipeline_trace": [
                {
                    "step_index": 2,
                    "tool_name": "claude.analyze",
                    "tool_ok": False,
                    "tool_error_code": "EMPTY_INPUT",
                }
            ],
        }, artifact_path

    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        fake_run_codex_task,
    )

    exit_code = run_command_module.main(["analyze drive file"])

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().out.strip())
    _assert_output_contract(payload, tool_source="external")
    assert payload["result"] == {
        "status": "error",
        "message": "EMPTY_INPUT",
    }
    assert not state_path.exists()


def test_parse_command_normalizes_uppercase_and_extra_spaces(monkeypatch) -> None:
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 222333444)

    assert run_command_module.parse_command("  SUMMARIZE   DRIVE   FILE  ") == (
        _expected_summarize_drive_file_task(222333444)
    )


def test_parse_command_process_email_maps_correctly(monkeypatch) -> None:
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 333444555)

    assert run_command_module.parse_command(" PROCESS   EMAIL ") == (
        _expected_process_email_task(333444555)
    )


def test_parse_command_owner_task_maps_correctly(monkeypatch) -> None:
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 444555666)

    assert run_command_module.parse_command(
        "owner task plan next immigration step"
    ) == {
        "task_id": 444555666,
        "instruction": run_command_module.PIPELINE_TASK_INSTRUCTION,
        "force_execution": True,
        "command_name": "owner task plan next immigration step",
        "pipeline_route": "owner task",
        "context_mode": "owner",
        "print_analysis": True,
        "pipeline": [
            {
                "tool_name": "claude.analyze",
                "input": {
                    "instruction": run_command_module.OWNER_TASK_INSTRUCTION,
                    "text": "plan next immigration step",
                },
                "output_key": "analysis_data",
            }
        ],
    }


def test_parse_command_linkedin_post_maps_correctly(monkeypatch) -> None:
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 777888999)

    assert run_command_module.parse_command(
        "linkedin post first DF system explanation"
    ) == {
        "task_id": 777888999,
        "instruction": run_command_module.PIPELINE_TASK_INSTRUCTION,
        "force_execution": True,
        "command_name": "linkedin post first df system explanation",
        "pipeline_route": "linkedin post",
        "print_linkedin_post": True,
        "pipeline": [
            {
                "tool_name": "linkedin.create_post_draft",
                "input": {
                    "topic": "first DF system explanation",
                    "context": "",
                },
                "output_key": "linkedin_post",
            }
        ],
    }


def test_parse_command_http_get_maps_to_role_pipeline(monkeypatch) -> None:
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 909090909)

    assert run_command_module.parse_command("http get https://example.com") == (
        _expected_http_request_task(909090909, "GET", "https://example.com")
    )


@pytest.mark.parametrize(
    ("mode", "context_name", "updates", "expected_context"),
    [
        (
            "owner",
            "owner_context",
            {
                "identity": {"name": "Alex Owner"},
                "notes": "Prioritize permit deadlines.",
            },
            {
                "identity": {"name": "Alex Owner"},
                "immigration": {},
                "context_summary": _default_context_summary(),
                "memory_summary": _default_memory_summary(),
                "notes": "Prioritize permit deadlines.",
                "permits": {},
            },
        ),
        (
            "business",
            "business_context",
            {
                "projects": [{"name": "Warehouse refresh"}],
                "status": "Waiting on client approval",
            },
            {
                "clients": [],
                "context_summary": _default_context_summary(),
                "memory_summary": _default_memory_summary(),
                "projects": [{"name": "Warehouse refresh"}],
                "status": "Waiting on client approval",
            },
        ),
        (
            "dev",
            None,
            None,
            {
                "context_summary": _default_context_summary(),
                "memory_summary": _default_memory_summary(),
            },
        ),
    ],
)
def test_main_routes_mode_context_into_claude_step(
    monkeypatch,
    tmp_path: Path,
    capsys,
    mode: str,
    context_name: str | None,
    updates: dict[str, object] | None,
    expected_context: dict[str, object] | None,
) -> None:
    context_dir = tmp_path / "context"
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    artifact_dir = tmp_path / "artifacts"
    state_path = tmp_path / "state" / "state.json"
    doc_url = f"https://docs.google.com/document/d/doc-{mode}-context"

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 2468101214)
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)

    run_command_module.set_active_mode(mode, context_dir=context_dir)
    if context_name is not None and updates is not None:
        update_context(context_name, updates, context_dir=context_dir)

    def fake_run_codex_task(task_source, artifact_dir=None, repo_root=None):
        payload = json.loads(Path(task_source).read_text(encoding="utf-8"))
        artifact_path = Path(artifact_dir) / f"pipeline-{payload['task_id']}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps({"final_output": {"url": doc_url}}, indent=2) + "\n",
            encoding="utf-8",
        )
        return {
            "task_id": payload["task_id"],
            "doc_url": doc_url,
        }, artifact_path

    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        fake_run_codex_task,
    )

    exit_code = run_command_module.main(["summarize drive file"])

    written_task = json.loads(task_path.read_text(encoding="utf-8"))
    payload = json.loads(capsys.readouterr().out.strip())

    assert exit_code == 0
    _assert_output_contract(payload, tool_source="external")
    assert payload["result"] == doc_url
    assert written_task == _expected_summarize_drive_file_task(
        2468101214,
        selected_context=expected_context,
        owner_mode=(mode == "owner"),
    )


def test_main_owner_task_uses_owner_context_and_prints_analysis(
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
        "1. Build the EB1 evidence gap list.\n\n"
        "Priorities:\n"
        "1. Immigration evidence and filing readiness.\n\n"
        "Next Moves:\n"
        "- Package the strongest proof this week."
    )

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 9988776655)
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)

    run_command_module.set_active_mode("owner", context_dir=context_dir)
    update_context(
        "owner_context",
        {
            "identity": {"name": "Anton Vorontsov"},
            "immigration": {"active_cases": ["EB1"]},
            "notes": "Prioritize the next immigration move.",
            "permits": {},
        },
        context_dir=context_dir,
    )

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
            "pipeline_trace": [
                {"step_index": 1, "tool_ok": True},
            ],
        }, artifact_path

    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        fake_run_codex_task,
    )

    exit_code = run_command_module.main(["owner task", "what should I do next for EB1"])

    written_task = json.loads(task_path.read_text(encoding="utf-8"))
    payload = json.loads(capsys.readouterr().out.strip())

    assert exit_code == 0
    _assert_output_contract(payload, tool_source="external")
    assert payload["result"] == analysis_text
    assert written_task == _expected_owner_task(
        9988776655,
        "what should I do next for EB1",
        selected_context={
            "context_summary": _default_context_summary(),
            "identity": {"name": "Anton Vorontsov"},
            "immigration": {"active_cases": ["EB1"]},
            "memory_summary": _default_memory_summary(),
            "notes": "Prioritize the next immigration move.",
            "permits": {},
        },
    )


def test_main_linkedin_post_uses_owner_context_and_prints_draft(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    context_dir = tmp_path / "context"
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    artifact_dir = tmp_path / "artifacts"
    state_path = tmp_path / "state" / "state.json"
    linkedin_output = {
        "post_text": "Digital Foreman is now starting to explain itself in public.",
        "tone": "grounded and specific",
        "intent": "share progress and invite feedback",
    }

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 1122334455)
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)

    run_command_module.set_active_mode("owner", context_dir=context_dir)
    update_context(
        "owner_context",
        {
            "identity": {"name": "Anton Vorontsov"},
            "immigration": {"active_cases": ["EB1"]},
            "notes": "Building Digital Foreman in public from real work.",
            "permits": {"priority": "high"},
        },
        context_dir=context_dir,
    )

    def fake_run_codex_task(task_source, artifact_dir=None, repo_root=None):
        payload = json.loads(Path(task_source).read_text(encoding="utf-8"))
        artifact_path = Path(artifact_dir) / f"pipeline-{payload['task_id']}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps({"final_output": linkedin_output}, indent=2) + "\n",
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

    exit_code = run_command_module.main(
        ["linkedin post", "first DF system explanation"]
    )

    written_task = json.loads(task_path.read_text(encoding="utf-8"))
    payload = json.loads(capsys.readouterr().out.strip())
    injected_context = written_task["pipeline"][0]["input"]["context"]

    assert exit_code == 0
    _assert_output_contract(payload, tool_source="external")
    assert payload["result"] == linkedin_output
    assert (
        written_task["pipeline"][0]["input"]["topic"] == "first DF system explanation"
    )
    assert "Owner context summary:" in injected_context
    assert '"phase": "system integration"' in injected_context
    return
    assert "твою систему" in injected_context
    assert "текущую стадию" in injected_context
    assert "реальные действия" in injected_context


def test_state_lookup_returns_last_command_entry(tmp_path: Path) -> None:
    state_path = tmp_path / "state" / "state.json"

    append_command_state(
        command_name="summarize drive file",
        result="SUCCESS",
        artifact=tmp_path / "artifacts" / "pipeline-1.json",
        doc_id="doc-1",
        timestamp="2026-04-11T18:00:00.000Z",
        state_path=state_path,
    )
    append_command_state(
        command_name="summarize drive file",
        result="SUCCESS",
        artifact=tmp_path / "artifacts" / "pipeline-2.json",
        doc_id="doc-2",
        timestamp="2026-04-11T18:01:00.000Z",
        state_path=state_path,
    )

    assert get_last_command("summarize drive file", state_path=state_path) == {
        "command": "summarize drive file",
        "timestamp": "2026-04-11T18:01:00.000Z",
        "last_result": "SUCCESS",
        "last_doc_id": "doc-2",
        "last_artifact": str(tmp_path / "artifacts" / "pipeline-2.json"),
    }


def test_main_prints_last_result_found_and_duplicate_warning(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    context_dir = tmp_path / "context"
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    artifact_dir = tmp_path / "artifacts"
    state_path = tmp_path / "state" / "state.json"
    doc_url = "https://docs.google.com/document/d/doc-duplicate-check"

    append_command_state(
        command_name="summarize drive file",
        result="SUCCESS",
        artifact=artifact_dir / "pipeline-previous.json",
        doc_id="doc-previous",
        timestamp="2026-04-11T18:35:00.000Z",
        state_path=state_path,
    )

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 123123123)
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)

    def fake_run_codex_task(task_source, artifact_dir=None, repo_root=None):
        payload = json.loads(Path(task_source).read_text(encoding="utf-8"))
        artifact_path = Path(artifact_dir) / f"pipeline-{payload['task_id']}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps({"final_output": {"url": doc_url}}, indent=2) + "\n",
            encoding="utf-8",
        )
        return {
            "task_id": payload["task_id"],
            "doc_url": doc_url,
        }, artifact_path

    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        fake_run_codex_task,
    )

    exit_code = run_command_module.main(["summarize drive file"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out.strip())
    _assert_output_contract(payload, tool_source="external")
    assert payload["result"] == doc_url
    state_payload = json.loads(state_path.read_text(encoding="utf-8"))
    assert state_payload["commands"][-1]["command"] == "summarize drive file"
    assert state_payload["commands"][-1]["result"] == "SUCCESS"


def test_main_set_mode_owner_updates_context(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    context_dir = tmp_path / "context"
    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)

    exit_code = run_command_module.main(["set mode owner"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out.strip())
    _assert_output_contract(payload, tool_source="external")
    assert payload["result"]["active_mode"] == "owner"
    system_context = json.loads(
        (context_dir / "system_context.json").read_text(encoding="utf-8")
    )
    assert system_context["active_mode"] == "owner"
    assert system_context["last_update"]


def test_main_show_context_prints_current_context(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    context_dir = tmp_path / "context"
    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    run_command_module.set_active_mode("business", context_dir=context_dir)

    exit_code = run_command_module.main(["show context"])

    payload = json.loads(capsys.readouterr().out.strip())
    assert exit_code == 0
    _assert_output_contract(payload, tool_source="external")
    context_payload = payload["result"]
    assert context_payload["system_context"]["active_mode"] == "business"
    assert context_payload["owner_context"] == {
        "identity": {},
        "immigration": {},
        "permits": {},
        "notes": "",
    }
    assert context_payload["business_context"] == {
        "projects": [],
        "clients": [],
        "status": "",
    }


def test_main_save_decision_persists_unified_memory(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    context_dir = tmp_path / "context"
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)

    exit_code = run_command_module.main(
        ["save decision adopt unified memory because track long-term context"]
    )

    payload = json.loads(capsys.readouterr().out.strip())
    decision_payload = payload["result"]
    stored_memory = json.loads(
        (memory_dir / "decisions.json").read_text(encoding="utf-8")
    )

    assert exit_code == 0
    _assert_output_contract(payload, tool_source="external")
    assert decision_payload["decision"] == "adopt unified memory"
    assert decision_payload["reason"] == "track long-term context"
    assert stored_memory["decisions"][-1]["decision"] == "adopt unified memory"


def test_main_show_memory_prints_unified_memory_payload(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    context_dir = tmp_path / "context"
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)
    memory_store_module.write_memory(
        "owner_memory",
        {
            "important_notes": ["Keep system explainable"],
            "long_term_goals": ["Build durable execution control"],
            "owner_name": "Anton Vorontsov",
            "priorities": ["EB1", "Digital Foreman"],
            "product_relation": "Digital Foreman is the product being used and validated",
            "public_positioning": "Owner/operator of Digital Foreman",
            "business_relation": "Digital Foreman supports owner operations and commercialization",
            "strategic_focus": "use + validate + preserve architecture + commercialize correctly",
        },
    )

    exit_code = run_command_module.main(["show memory"])

    payload = json.loads(capsys.readouterr().out.strip())
    memory_payload = payload["result"]

    assert exit_code == 0
    _assert_output_contract(payload, tool_source="external")
    assert memory_payload["owner_memory"]["priorities"] == ["EB1", "Digital Foreman"]
    assert (
        memory_payload["memory_summary"]["owner_context"]["owner_name"]
        == "Anton Vorontsov"
    )


def test_main_show_state_prints_project_state(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    context_dir = tmp_path / "context"
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)
    memory_store_module.write_memory(
        "project_state",
        {
            "current_stage": "core complete",
            "active_block": "none",
            "core_status": "complete",
            "operating_phase": "use_phase",
            "system_mode": "whole_system_coherence",
            "focus": "real usage, coherence, validation, commercialization path",
            "next_step": "owner operations",
            "next_steps": ["owner operations"],
        },
    )

    exit_code = run_command_module.main(["show state"])

    payload = json.loads(capsys.readouterr().out.strip())

    assert exit_code == 0
    _assert_output_contract(payload, tool_source="external")
    assert payload["result"] == {
        "active_block": "none",
        "core_status": "complete",
        "current_stage": "core complete",
        "focus": "real usage, coherence, validation, commercialization path",
        "next_step": "owner operations",
        "next_steps": ["owner operations"],
        "operating_phase": "use_phase",
        "system_mode": "whole_system_coherence",
    }


def test_main_show_decisions_and_architecture_commands(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    context_dir = tmp_path / "context"
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)
    memory_store_module.write_memory(
        "decisions",
        {
            "decision": "Do not use database storage",
            "reason": "Keep memory deterministic",
        },
    )
    memory_store_module.write_memory(
        "architecture",
        {
            "system_rules": ["Keep deterministic routing"],
            "tool_strategy": ["Prefer explicit command execution"],
        },
    )

    decisions_exit_code = run_command_module.main(["show decisions"])
    decisions_payload = json.loads(capsys.readouterr().out.strip())

    architecture_exit_code = run_command_module.main(["show architecture"])
    architecture_payload = json.loads(capsys.readouterr().out.strip())

    assert decisions_exit_code == 0
    _assert_output_contract(decisions_payload, tool_source="external")
    assert (
        decisions_payload["result"]["decisions"][-1]["decision"]
        == "Do not use database storage"
    )
    assert architecture_exit_code == 0
    _assert_output_contract(architecture_payload, tool_source="external")
    assert architecture_payload["result"]["system_rules"] == [
        "Keep deterministic routing"
    ]


def test_main_prints_conflict_with_memory_for_contradicting_analysis(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    context_dir = tmp_path / "context"
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    artifact_dir = tmp_path / "artifacts"
    state_path = tmp_path / "state" / "state.json"
    memory_dir = tmp_path / "memory"

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 700700700)
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)

    memory_store_module.write_memory(
        "decisions",
        {
            "decision": "Do not use database storage",
            "reason": "Keep memory deterministic",
        },
    )

    def fake_run_codex_task(task_source, artifact_dir=None, repo_root=None):
        payload = json.loads(Path(task_source).read_text(encoding="utf-8"))
        artifact_path = Path(artifact_dir) / f"pipeline-{payload['task_id']}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps(
                {
                    "final_output": {
                        "analysis": "Use database storage for the next memory layer."
                    }
                },
                indent=2,
            )
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

    exit_code = run_command_module.main(["owner task review memory plan"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out.strip())
    _assert_output_contract(payload, tool_source="external")
    assert payload["result"] == "Use database storage for the next memory layer."


def test_main_process_email_runtime_failure_returns_error_without_fallback(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    context_dir = tmp_path / "context"
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    artifact_dir = tmp_path / "artifacts"
    state_path = tmp_path / "state" / "state.json"
    execution_log_path = tmp_path / "logs" / "execution_log.jsonl"

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "EXECUTION_LOG_PATH", execution_log_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 202604110001)
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)
    monkeypatch.setattr(
        run_command_module,
        "execute_command_task_with_retry",
        lambda task_path: (
            False,
            "",
            artifact_dir / "pipeline-202604110001.json",
            "urlopen error [WinError 10061]",
        ),
    )

    exit_code = run_command_module.main(["process email for adu client"])

    payload = json.loads(capsys.readouterr().out.strip())
    assert exit_code == 1
    _assert_output_contract(payload, tool_source="external")
    assert payload["execution_trace"] == {
        "execution_id": "202604110001",
        "memory_state": {
            "core_status": "",
            "operating_phase": "",
            "system_mode": "",
        },
        "timestamp": payload["execution_trace"]["timestamp"],
        "tool_source": "external",
    }
    assert payload["result"] == {
        "status": "error",
        "message": "urlopen error [WinError 10061]",
    }
    project_state = memory_store_module.get_project_state()
    assert "last_action" not in project_state
    assert "last_result_summary" not in project_state


def test_main_http_role_pipeline_retries_once_and_returns_verified_result(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    context_dir = tmp_path / "context"
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    state_path = tmp_path / "state" / "state.json"
    execution_log_path = tmp_path / "logs" / "execution_log.jsonl"
    memory_dir = tmp_path / "memory"

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "EXECUTION_LOG_PATH", execution_log_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 202604110101)
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)
    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError(
                "run_codex_task should not be called for planned HTTP commands"
            )
        ),
    )

    responses = iter(
        [
            {
                "status": "success",
                "data": {
                    "status_code": 500,
                    "body": {"ok": False},
                    "url": "https://example.com",
                    "method": "GET",
                },
                "error": None,
                "source": "external",
            },
            {
                "status": "success",
                "data": {
                    "status_code": 200,
                    "body": {"ok": True},
                    "url": "https://example.com",
                    "method": "GET",
                },
                "error": None,
                "source": "external",
            },
        ]
    )

    monkeypatch.setattr(
        run_command_module,
        "execute_tool",
        lambda tool_name, payload: next(responses),
    )

    exit_code = run_command_module.main(["http get https://example.com"])

    assert exit_code == 0
    written_task = json.loads(task_path.read_text(encoding="utf-8"))
    assert written_task == _expected_http_request_task(
        202604110101,
        "GET",
        "https://example.com",
    )
    payload = json.loads(capsys.readouterr().out.strip())
    _assert_output_contract(payload, tool_source="external")
    assert payload["result"] == {
        "status_code": 200,
        "body": {"ok": True},
        "url": "https://example.com",
        "method": "GET",
    }
    project_state = memory_store_module.get_project_state()
    assert project_state["last_action"] == "http get https://example.com"
    assert project_state["last_result_summary"] == "http request success"


def test_main_http_role_pipeline_fails_after_single_retry(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    context_dir = tmp_path / "context"
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    state_path = tmp_path / "state" / "state.json"
    memory_dir = tmp_path / "memory"

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 202604110102)
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)
    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError(
                "run_codex_task should not be called for planned HTTP commands"
            )
        ),
    )
    monkeypatch.setattr(
        run_command_module,
        "execute_tool",
        lambda tool_name, payload: {
            "status": "success",
            "data": {
                "status_code": 500,
                "body": {"ok": False},
                "url": "https://example.com",
                "method": "GET",
            },
            "error": None,
            "source": "external",
        },
    )

    exit_code = run_command_module.main(["http get https://example.com"])

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().out.strip())
    _assert_output_contract(payload, tool_source="external")
    assert payload["result"] == {
        "status": "error",
        "message": "HTTP verification failed: status 500",
    }
    project_state = memory_store_module.get_project_state()
    assert "last_action" not in project_state
    assert "last_result_summary" not in project_state


def test_main_success_writes_only_memory_summary_fields_without_replacing_project_state(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    context_dir = tmp_path / "context"
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    artifact_dir = tmp_path / "artifacts"
    state_path = tmp_path / "state" / "state.json"
    execution_log_path = tmp_path / "logs" / "execution_log.jsonl"
    doc_url = "https://docs.google.com/document/d/doc-memory-write"

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "EXECUTION_LOG_PATH", execution_log_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 202604110002)
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)

    memory_store_module.write_memory(
        "project_state",
        {
            "current_stage": "interface layer",
            "focus": "wire unified memory",
        },
    )

    def fake_run_codex_task(task_source, artifact_dir=None, repo_root=None):
        payload = json.loads(Path(task_source).read_text(encoding="utf-8"))
        artifact_path = Path(artifact_dir) / f"pipeline-{payload['task_id']}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps({"final_output": {"url": doc_url}}, indent=2) + "\n",
            encoding="utf-8",
        )
        return {
            "task_id": payload["task_id"],
            "doc_url": doc_url,
        }, artifact_path

    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        fake_run_codex_task,
    )

    exit_code = run_command_module.main(["create doc from analysis"])

    assert exit_code == 0
    capsys.readouterr()
    project_state = memory_store_module.get_project_state()
    assert project_state["current_stage"] == "interface layer"
    assert project_state["focus"] == "wire unified memory"
    assert project_state["last_action"] == "create doc from analysis"
    assert project_state["last_result_summary"] == "fallback executed"
    assert "result" not in project_state
    assert "execution_trace" not in project_state


def test_main_failure_does_not_write_success_memory_fields(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    context_dir = tmp_path / "context"
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    artifact_dir = tmp_path / "artifacts"
    state_path = tmp_path / "state" / "state.json"

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 202604110003)
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)

    memory_store_module.write_memory(
        "project_state",
        {
            "current_stage": "interface layer",
        },
    )

    def fake_run_codex_task(task_source, artifact_dir=None, repo_root=None):
        payload = json.loads(Path(task_source).read_text(encoding="utf-8"))
        artifact_path = Path(artifact_dir) / f"pipeline-{payload['task_id']}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps({"failure_reason": "EMPTY_INPUT"}, indent=2) + "\n",
            encoding="utf-8",
        )
        return {
            "task_id": payload["task_id"],
            "failure_reason": "EMPTY_INPUT",
        }, artifact_path

    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        fake_run_codex_task,
    )

    exit_code = run_command_module.main(["analyze drive file"])

    assert exit_code == 1
    capsys.readouterr()
    project_state = memory_store_module.get_project_state()
    assert project_state["current_stage"] == "interface layer"
    assert "last_action" not in project_state
    assert "last_result_summary" not in project_state


def test_main_fails_when_required_execution_context_is_missing(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    context_dir = tmp_path / "context"
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    artifact_dir = tmp_path / "artifacts"
    state_path = tmp_path / "state" / "state.json"
    memory_dir = tmp_path / "memory"

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 202604120010)
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("run_codex_task should not execute without system context")
        ),
    )

    exit_code = run_command_module.main(["create doc from analysis"])

    payload = json.loads(capsys.readouterr().out.strip())
    assert exit_code == 1
    _assert_output_contract(payload, tool_source="external")
    assert payload["result"] == {
        "status": "error",
        "message": "CONTEXT_NOT_LOADED",
    }
    assert not task_path.exists()


def test_main_executes_when_required_execution_context_is_present(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    context_dir = tmp_path / "context"
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    artifact_dir = tmp_path / "artifacts"
    state_path = tmp_path / "state" / "state.json"
    memory_dir = tmp_path / "memory"
    doc_url = "https://docs.google.com/document/d/context-pass"
    captured_run: dict[str, object] = {"called": False}

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 202604120011)
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)

    def fake_run_codex_task(task_source, artifact_dir=None, repo_root=None):
        captured_run["called"] = True
        payload = json.loads(Path(task_source).read_text(encoding="utf-8"))
        artifact_path = Path(artifact_dir) / f"pipeline-{payload['task_id']}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps({"final_output": {"url": doc_url}}, indent=2) + "\n",
            encoding="utf-8",
        )
        return {
            "task_id": payload["task_id"],
            "doc_url": doc_url,
        }, artifact_path

    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        fake_run_codex_task,
    )

    exit_code = run_command_module.main(["create doc from analysis"])

    payload = json.loads(capsys.readouterr().out.strip())
    assert exit_code == 0
    assert captured_run["called"] is True
    _assert_output_contract(payload, tool_source="external")
    assert payload["result"] == doc_url


def test_main_injects_system_context_summary_and_updates_recent_actions(
    monkeypatch,
    tmp_path: Path,
) -> None:
    context_dir = tmp_path / "context"
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    artifact_dir = tmp_path / "artifacts"
    state_path = tmp_path / "state" / "state.json"
    memory_dir = tmp_path / "memory"

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 202604120001)
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)

    def fake_run_codex_task(task_source, artifact_dir=None, repo_root=None):
        payload = json.loads(Path(task_source).read_text(encoding="utf-8"))
        artifact_path = Path(artifact_dir) / f"pipeline-{payload['task_id']}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps(
                {
                    "final_output": {
                        "url": "https://docs.google.com/document/d/test-doc"
                    }
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        return {
            "task_id": payload["task_id"],
            "doc_url": "https://docs.google.com/document/d/test-doc",
        }, artifact_path

    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        fake_run_codex_task,
    )

    exit_code = run_command_module.main(["create doc from analysis"])

    written_task = json.loads(task_path.read_text(encoding="utf-8"))
    system_context = json.loads(
        (memory_dir / "system_context.json").read_text(encoding="utf-8")
    )
    claude_context = written_task["pipeline"][0]["input"]["context"]

    assert exit_code == 0
    assert claude_context["memory_summary"] == _default_memory_summary()
    assert claude_context["context_summary"] == _default_context_summary()
    assert system_context["last_actions"][-1] == (
        "create doc from analysis: completed | "
        "create doc from analysis executed successfully"
    )
