from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import control.task_guard as task_guard_module
import scripts.run_codex_task as run_codex_task_module
from app.memory import memory_registry
from app.storage import storage_adapter
from control.context_builder import (
    build_and_write_context_packet as build_and_write_context_packet_helper,
)
from control.task_decomposer import write_subtask_record as write_subtask_record_helper
from control.tool_registry import GOOGLE_DOCS_CREATE_DOCUMENT_TOOL
from scripts.run_codex_task import commit_and_push_artifact, load_codex_task, run_codex_task
from scripts.run_codex_task import _build_subtask_task


def _write_policy(tmp_path: Path) -> Path:
    policy_path = tmp_path / "config" / "contour_policy.json"
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    policy_path.write_text(
        json.dumps(
            {
                "contours": {
                    "df-dev": {
                        "working_root": str(tmp_path / "df-dev"),
                    },
                    "ownerbox": {
                        "working_root": str(tmp_path / "ownerbox"),
                    },
                }
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return policy_path


def test_load_repo_env_sets_missing_variables_without_overriding_existing(
    monkeypatch, tmp_path
) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text(
        (
            "\n"
            "# comment\n"
            " GOOGLE_CLIENT_ID = client-id \n"
            "GOOGLE_CLIENT_SECRET=client=secret\n"
            "EXISTING_VALUE=from-file\n"
            "INVALID_LINE\n"
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("GOOGLE_CLIENT_ID", raising=False)
    monkeypatch.delenv("GOOGLE_CLIENT_SECRET", raising=False)
    monkeypatch.setenv("EXISTING_VALUE", "from-env")

    run_codex_task_module._load_repo_env(env_path)

    assert os.environ["GOOGLE_CLIENT_ID"] == "client-id"
    assert os.environ["GOOGLE_CLIENT_SECRET"] == "client=secret"
    assert os.environ["EXISTING_VALUE"] == "from-env"


def test_load_codex_task_extracts_required_fields(tmp_path) -> None:
    task_path = tmp_path / "task-9.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 9,\n'
            '  "instruction": "Implement the task",\n'
            '  "constraints": "Do not break existing code. Modify only necessary parts.",\n'
            '  "success_criteria": "Code runs without errors and matches task description",\n'
            '  "labels": ["STATUS: DONE"]\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    task = load_codex_task(
        task_path,
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    assert task == {
        "task_id": 9,
        "instruction": "Implement the task",
        "constraints": "Do not break existing code. Modify only necessary parts.",
        "success_criteria": "Code runs without errors and matches task description",
        "context_packet_path": str(tmp_path / "tasks" / "context" / "task-9-context.json"),
    }


def test_load_codex_task_creates_context_when_optional_files_are_missing(tmp_path) -> None:
    task_path = tmp_path / "task-7.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 7,\n'
            '  "instruction": "Implement the task",\n'
            '  "constraints": "Do not break existing code. Modify only necessary parts.",\n'
            '  "success_criteria": "Code runs without errors and matches task description"\n'
            "}\n"
        ),
        encoding="utf-8",
    )
    task = load_codex_task(
        task_path,
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    context_path = tmp_path / "tasks" / "context" / "task-7-context.json"
    context_packet = json.loads(context_path.read_text(encoding="utf-8"))

    assert task["context_packet_path"] == str(context_path)
    assert context_packet["config_files"] == []
    assert context_packet["related_files"] == []


def test_load_codex_task_preserves_personal_context_payload(tmp_path) -> None:
    task_path = tmp_path / "task-21.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 21,\n'
            '  "instruction": "Update personal context",\n'
            '  "constraints": "Do not mix with business data.",\n'
            '  "success_criteria": "Personal context is updated.",\n'
            '  "task_type": "personal_context_update",\n'
            '  "personal_context_update": {"owner": {"name": "Avery"}}\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    task = load_codex_task(
        task_path,
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    assert task["task_type"] == "personal_context_update"
    assert task["personal_context_update"] == {"owner": {"name": "Avery"}}


def test_load_codex_task_preserves_memory_context(tmp_path) -> None:
    task_path = tmp_path / "task-22.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 22,\n'
            '  "instruction": "Implement the task",\n'
            '  "constraints": "Do not break existing code.",\n'
            '  "success_criteria": "Execution continues normally.",\n'
            '  "memory_context": {"domain": "dev", "type": "task", "tags": ["finance"]}\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    task = load_codex_task(
        task_path,
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    assert task["memory_context"] == {"domain": "dev", "type": "task", "tags": ["finance"]}


def test_load_codex_task_defaults_required_fields_for_tool_call(tmp_path) -> None:
    task_path = tmp_path / "task-23.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 23,\n'
            '  "instruction": "Create a Google Doc through Tool Layer v1",\n'
            '  "tool_call": {\n'
            '    "tool_name": "google_docs.create_document",\n'
            '    "input": {\n'
            '      "title": "DF FIRST REAL TEST VIA TOOL",\n'
            '      "content": "This document was created through Tool Layer v1."\n'
            "    }\n"
            "  }\n"
            "}\n"
        ),
        encoding="utf-8",
    )

    task = load_codex_task(
        task_path,
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    assert "Do not break existing code" in task["constraints"]
    assert "Modify only necessary parts" in task["constraints"]
    assert "stable" in task["success_criteria"]
    assert "clarification request" in task["success_criteria"]
    assert task["tool_call"] == {
        "tool_name": "google_docs.create_document",
        "input": {
            "title": "DF FIRST REAL TEST VIA TOOL",
            "content": "This document was created through Tool Layer v1.",
        },
    }


def test_run_codex_task_writes_instruction_artifact(tmp_path) -> None:
    task_path = tmp_path / "task-9.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 9,\n'
            '  "instruction": "Implement the task",\n'
            '  "constraints": "Do not break existing code. Modify only necessary parts.",\n'
            '  "success_criteria": "Code runs without errors and matches task description"\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    task, artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    assert task["task_id"] == 9
    assert artifact_path == tmp_path / "artifacts" / "task-9.txt"
    assert artifact_path.read_text(encoding="utf-8") == "Implement the task"


def test_run_codex_task_resolves_memory_before_execution(tmp_path, monkeypatch) -> None:
    task_path = tmp_path / "task-11.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 11,\n'
            '  "instruction": "Implement the task",\n'
            '  "constraints": "Do not break existing code. Modify only necessary parts.",\n'
            '  "success_criteria": "Code runs without errors and matches task description",\n'
            '  "memory_context": {"domain": "dev", "type": "task", "tags": ["urgent"]}\n'
            "}\n"
        ),
        encoding="utf-8",
    )
    captured: dict[str, object] = {}

    def fake_resolve_memory(context):
        captured["context"] = context
        return [
            {
                "id": "latest-task",
                "domain": "dev",
                "type": "task",
                "local_path": "/tmp/latest-task.json",
                "remote_path": None,
                "timestamp": "2026-04-14T11:00:00Z",
                "tags": ["urgent"],
            }
        ]

    monkeypatch.setattr(run_codex_task_module, "resolve_memory", fake_resolve_memory)

    task, artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    assert captured["context"] == {"domain": "dev", "type": "task", "tags": ["urgent"]}
    assert task["resolved_memory"] == [
        {
            "id": "latest-task",
            "domain": "dev",
            "type": "task",
            "local_path": "/tmp/latest-task.json",
            "remote_path": None,
            "timestamp": "2026-04-14T11:00:00Z",
            "tags": ["urgent"],
        }
    ]
    assert artifact_path == tmp_path / "artifacts" / "task-11.txt"


def test_run_codex_task_blocks_when_memory_policy_disallows_execution(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    task_path = tmp_path / "task-31.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 31,\n'
            '  "instruction": "Create the duplicate document",\n'
            '  "constraints": "Do not create duplicates.",\n'
            '  "success_criteria": "Execution blocks when duplicate work is detected.",\n'
            '  "memory_context": {"domain": "dev", "type": "task", "tags": ["urgent"]},\n'
            '  "tool_call": {\n'
            '    "tool_name": "google_docs.create_document",\n'
            '    "input": {\n'
            '      "title": "Duplicate document",\n'
            '      "content": "Should never execute."\n'
            "    }\n"
            "  }\n"
            "}\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        run_codex_task_module,
        "evaluate_memory_policy",
        lambda task_packet, resolved_memory: {
            "allowed": False,
            "reason": "recent_duplicate_detected",
            "matched_artifact_id": "artifact-123",
            "action": "block",
        },
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "_contextual_tool_call",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("normal execution should not continue after a blocked memory policy")
        ),
    )

    task, artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    output = capsys.readouterr().out
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert "[MEMORY_POLICY] blocked_recent_duplicate artifact=artifact-123" in output
    assert task["memory_policy_decision"] == {
        "allowed": False,
        "reason": "recent_duplicate_detected",
        "matched_artifact_id": "artifact-123",
        "action": "block",
    }
    assert task["failure_reason"] == "recent_duplicate_detected"
    assert artifact["result_type"] == "memory_policy_blocked"
    assert artifact["reason"] == "recent_duplicate_detected"
    assert artifact["matched_artifact_id"] == "artifact-123"
    assert artifact["instruction"] == "Create the duplicate document"


def test_run_codex_task_continues_when_memory_policy_allows_execution(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    task_path = tmp_path / "task-32.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 32,\n'
            '  "instruction": "Implement the allowed task",\n'
            '  "constraints": "Modify only necessary parts.",\n'
            '  "success_criteria": "Artifact is written when no recent duplicate exists.",\n'
            '  "memory_context": {"domain": "dev", "type": "task"}\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        run_codex_task_module,
        "evaluate_memory_policy",
        lambda task_packet, resolved_memory: {
            "allowed": True,
            "reason": "no_recent_duplicate",
            "matched_artifact_id": None,
            "action": "continue",
        },
    )

    task, artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    output = capsys.readouterr().out

    assert "[MEMORY_POLICY] no_recent_duplicate" in output
    assert task["memory_policy_decision"] == {
        "allowed": True,
        "reason": "no_recent_duplicate",
        "matched_artifact_id": None,
        "action": "continue",
    }
    assert artifact_path == tmp_path / "artifacts" / "task-32.txt"
    assert artifact_path.read_text(encoding="utf-8") == "Implement the allowed task"


def test_run_codex_task_blocks_when_cross_domain_conflict_is_detected(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    monkeypatch.setattr(
        memory_registry,
        "REGISTRY_FILE",
        tmp_path / "df-system" / "memory_registry.json",
    )

    task_path = tmp_path / "task-33.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 33,\n'
            '  "instruction": "Create the cross-domain document",\n'
            '  "constraints": "Do not conflict across domains.",\n'
            '  "success_criteria": "Execution blocks when another domain owns the same resource.",\n'
            '  "memory_context": {"domain": "dev", "type": "task"},\n'
            '  "payload": {"domain": "dev", "resource_id": "crew-west"},\n'
            '  "tool_call": {\n'
            '    "tool_name": "google_docs.create_document",\n'
            '    "input": {\n'
            '      "title": "Blocked cross-domain document",\n'
            '      "content": "Should never execute."\n'
            "    }\n"
            "  }\n"
            "}\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        run_codex_task_module,
        "evaluate_memory_policy",
        lambda task_packet, resolved_memory: {
            "allowed": True,
            "reason": "no_recent_duplicate",
            "matched_artifact_id": None,
            "action": "continue",
        },
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "resolve_memory",
        lambda context: [
            {
                "id": "artifact-ownerbox-1",
                "domain": "ownerbox",
                "type": "task",
                "status": "running",
                "payload": {
                    "domain": "ownerbox",
                    "resource_id": "crew-west",
                },
            }
        ],
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "_contextual_tool_call",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            AssertionError("normal execution should not continue after a blocked conflict gate")
        ),
    )

    task, artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    output = capsys.readouterr().out
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert (
        "[CONFLICT] blocked resource=crew-west domain=dev other_domain=ownerbox"
        in output
    )
    assert task["cross_domain_conflict_decision"] == {
        "allowed": False,
        "reason": "cross_domain_conflict_detected",
        "conflict_with": "artifact-ownerbox-1",
        "action": "block",
    }
    assert task["failure_reason"] == "cross_domain_conflict_detected"
    assert artifact["result_type"] == "cross_domain_conflict_blocked"
    assert artifact["reason"] == "cross_domain_conflict_detected"
    assert artifact["conflict_with"] == "artifact-ownerbox-1"
    assert artifact["instruction"] == "Create the cross-domain document"


def test_run_codex_task_updates_personal_context_and_tracks_commit_paths(tmp_path) -> None:
    task_path = tmp_path / "task-10.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 10,\n'
            '  "instruction": "Update personal context with DMV reminder",\n'
            '  "constraints": "Do not mix with business data.",\n'
            '  "success_criteria": "Personal context is updated.",\n'
            '  "task_type": "personal_context_update",\n'
            '  "personal_context_update": {\n'
            '    "reminders": [\n'
            '      {"id": "dmv-renewal", "title": "Renew registration"}\n'
            "    ]\n"
            "  }\n"
            "}\n"
        ),
        encoding="utf-8",
    )

    task, artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    personal_context_path = tmp_path / "personal" / "personal_context.json"
    written_context = json.loads(personal_context_path.read_text(encoding="utf-8"))

    assert artifact_path == tmp_path / "artifacts" / "task-10.txt"
    assert task["commit_paths"] == [str(personal_context_path)]
    assert task["required_validation_paths"] == [str(personal_context_path)]
    assert "PERSONAL_CONTEXT:" in artifact_path.read_text(encoding="utf-8")
    assert written_context["reminders"] == [
        {"id": "dmv-renewal", "title": "Renew registration"},
    ]


def test_run_codex_task_creates_google_doc_artifact(tmp_path, monkeypatch) -> None:
    task_path = tmp_path / "task-88.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 88,\n'
            '  "instruction": "Create the kickoff summary document",\n'
            '  "constraints": "Create a new doc only.",\n'
            '  "success_criteria": "Doc is created.",\n'
            '  "task_type": "external_write_google_doc",\n'
            '  "title": "Kickoff Summary",\n'
            '  "content": "Project kickoff summary text."\n'
            "}\n"
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "integrations.google_docs_tool.create_google_doc",
        lambda payload: {
            "doc_id": "doc-abc123",
            "name": payload["title"],
            "url": "https://docs.google.com/document/d/doc-abc123",
        },
    )

    task, artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert task["doc_id"] == "doc-abc123"
    assert task["doc_url"] == "https://docs.google.com/document/d/doc-abc123"
    assert artifact_path == tmp_path / "artifacts" / "doc-88.json"
    assert artifact["content_summary"] == "Project kickoff summary text."
    assert artifact["doc_id"] == "doc-abc123"
    assert artifact["url"] == "https://docs.google.com/document/d/doc-abc123"
    assert isinstance(artifact["execution_timeline"], dict)
    assert isinstance(artifact["step_metrics"], list)
    assert isinstance(artifact["retry_info"], dict)
    assert "tool_trace" not in task


def test_run_codex_task_google_doc_failure_writes_reason(tmp_path, monkeypatch) -> None:
    task_path = tmp_path / "task-89.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 89,\n'
            '  "instruction": "Create the client handoff document",\n'
            '  "constraints": "Create a new doc only.",\n'
            '  "success_criteria": "Doc is created.",\n'
            '  "task_type": "external_write_google_doc",\n'
            '  "title": "Client Handoff",\n'
            '  "content": "Client-ready handoff text."\n'
            "}\n"
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "integrations.google_docs_tool.create_google_doc",
        lambda payload: (_ for _ in ()).throw(RuntimeError("google docs api failed")),
    )

    _, artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert artifact_path == tmp_path / "artifacts" / "doc-89.json"
    assert artifact["content_summary"] == "Client-ready handoff text."
    assert artifact["doc_id"] == ""
    assert artifact["reason"] == "google docs api failed"
    assert artifact["url"] == ""
    assert isinstance(artifact["execution_timeline"], dict)
    assert isinstance(artifact["step_metrics"], list)
    assert isinstance(artifact["retry_info"], dict)


def test_run_codex_task_tool_call_creates_google_doc_artifact_with_tool_trace(
    tmp_path,
    monkeypatch,
) -> None:
    task_path = tmp_path / "task-23.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 23,\n'
            '  "instruction": "Create a Google Doc through Tool Layer v1",\n'
            '  "force_execution": true,\n'
            '  "tool_call": {\n'
            '    "tool_name": "google_docs.create_document",\n'
            '    "input": {\n'
            '      "title": "DF FIRST REAL TEST VIA TOOL",\n'
            '      "content": "This document was created through Tool Layer v1."\n'
            "    }\n"
            "  }\n"
            "}\n"
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "integrations.google_docs_tool.create_google_doc",
        lambda payload: {
            "doc_id": "doc-tool-456",
            "name": payload["title"],
            "url": "https://docs.google.com/document/d/doc-tool-456",
        },
    )

    task, artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert task["doc_id"] == "doc-tool-456"
    assert task["doc_url"] == "https://docs.google.com/document/d/doc-tool-456"
    assert task["tool_trace"]["tool_name"] == GOOGLE_DOCS_CREATE_DOCUMENT_TOOL
    assert task["tool_trace"]["tool_ok"] is True
    assert task["tool_trace"]["tool_input_summary"] == {
        "title": "DF FIRST REAL TEST VIA TOOL",
        "content_chars": 48,
    }
    assert task["tool_trace"]["tool_output_summary"] == {
        "doc_id": "doc-tool-456",
        "url": "https://docs.google.com/document/d/doc-tool-456",
    }
    assert artifact_path == tmp_path / "artifacts" / "doc-23.json"
    assert artifact["content_summary"] == "This document was created through Tool Layer v1."
    assert artifact["doc_id"] == "doc-tool-456"
    assert artifact["tool_input_summary"] == {
        "content_chars": 48,
        "title": "DF FIRST REAL TEST VIA TOOL",
    }
    assert artifact["tool_name"] == GOOGLE_DOCS_CREATE_DOCUMENT_TOOL
    assert artifact["tool_ok"] is True
    assert artifact["tool_output_summary"] == {
        "doc_id": "doc-tool-456",
        "url": "https://docs.google.com/document/d/doc-tool-456",
    }
    assert artifact["url"] == "https://docs.google.com/document/d/doc-tool-456"


def test_run_codex_task_creates_drive_to_google_doc_artifact(tmp_path, monkeypatch) -> None:
    task_path = tmp_path / "task-81.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 81,\n'
            '  "task_type": "drive_to_google_doc",\n'
            '  "instruction": "Create a client summary doc from Google Drive notes.",\n'
            '  "constraints": "Use only read-only Drive input and create a new Google Doc only.",\n'
            '  "success_criteria": "New Google Doc created from external file content.",\n'
            '  "external_context": {"google_drive": ["drive-file-001"]},\n'
            '  "output_doc_title": "Client Summary",\n'
            '  "transform_mode": "plain_summary"\n'
            "}\n"
        ),
        encoding="utf-8",
    )
    captured_payload: dict[str, str] = {}
    monkeypatch.setattr(
        "control.context_builder.read_google_drive_file",
        lambda payload: {
            "file_id": payload["drive_file_id"],
            "name": "Client Notes",
            "content": "Line one\nLine two",
            "size": 17,
        },
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "utc_now",
        lambda: datetime(2026, 4, 10, 18, 0, 0, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(
        "integrations.google_docs_tool.create_google_doc",
        lambda payload: (
            captured_payload.update(payload)
            or {
                "doc_id": "doc-drive-123",
                "name": payload["title"],
                "url": "https://docs.google.com/document/d/doc-drive-123",
            }
        ),
    )

    task, artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))
    expected_content = (
        "Document Title: Client Summary\n"
        "Transform Mode: plain_summary\n"
        "Source File Count: 1\n"
        "Generated At: 2026-04-10T18:00:00Z\n\n"
        "Source File 1: Client Notes\n"
        "Source File ID: drive-file-001\n"
        "Extracted Content:\n"
        "Line one\n"
        "Line two\n"
    )

    assert task["doc_id"] == "doc-drive-123"
    assert task["doc_url"] == "https://docs.google.com/document/d/doc-drive-123"
    assert artifact_path == tmp_path / "artifacts" / "drive-to-doc-81.json"
    assert captured_payload == {
        "title": "Client Summary",
        "content": expected_content,
    }
    assert artifact["content_summary"] == expected_content[:200]
    assert artifact["loaded_source_file_ids"] == ["drive-file-001"]
    assert artifact["output_doc_id"] == "doc-drive-123"
    assert artifact["output_doc_title"] == "Client Summary"
    assert artifact["output_doc_url"] == "https://docs.google.com/document/d/doc-drive-123"
    assert artifact["source_file_ids"] == ["drive-file-001"]
    assert artifact["transform_mode"] == "plain_summary"
    assert isinstance(artifact["execution_timeline"], dict)
    assert isinstance(artifact["step_metrics"], list)
    assert isinstance(artifact["retry_info"], dict)


def test_run_codex_task_drive_to_google_doc_missing_external_files_writes_reason(
    tmp_path,
    monkeypatch,
) -> None:
    task_path = tmp_path / "task-82.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 82,\n'
            '  "task_type": "drive_to_google_doc",\n'
            '  "instruction": "Create a client summary doc from Google Drive notes.",\n'
            '  "constraints": "Use only read-only Drive input and create a new Google Doc only.",\n'
            '  "success_criteria": "New Google Doc created from external file content.",\n'
            '  "external_context": {"google_drive": ["drive-file-missing"]},\n'
            '  "output_doc_title": "Client Summary",\n'
            '  "transform_mode": "plain_summary"\n'
            "}\n"
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr("control.context_builder.read_google_drive_file", lambda payload: None)
    monkeypatch.setattr(
        "integrations.google_docs_tool.create_google_doc",
        lambda payload: (_ for _ in ()).throw(AssertionError("should not create a doc")),
    )

    _, artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert artifact_path == tmp_path / "artifacts" / "drive-to-doc-82.json"
    assert artifact["content_summary"] == ""
    assert artifact["loaded_source_file_ids"] == []
    assert artifact["output_doc_id"] == ""
    assert artifact["output_doc_title"] == "Client Summary"
    assert artifact["output_doc_url"] == ""
    assert artifact["reason"] == "no external files were loaded from Google Drive context"
    assert artifact["source_file_ids"] == ["drive-file-missing"]
    assert artifact["transform_mode"] == "plain_summary"
    assert isinstance(artifact["execution_timeline"], dict)
    assert isinstance(artifact["step_metrics"], list)
    assert isinstance(artifact["retry_info"], dict)


def test_build_subtask_task_preserves_external_context() -> None:
    task = _build_subtask_task(
        {
            "task_id": 9,
            "constraints": "Do not break existing code.",
            "success_criteria": "Execution continues normally.",
            "external_context": {"google_drive": ["drive-file-abc"]},
        },
        {
            "subtask_id": "9-1",
            "type": "analysis",
            "priority": 1,
            "instruction": "Read the attached note",
        },
    )

    assert task["external_context"] == {"google_drive": ["drive-file-abc"]}


def test_build_subtask_task_preserves_tool_call() -> None:
    task = _build_subtask_task(
        {
            "task_id": 23,
            "constraints": "Do not break existing code.",
            "success_criteria": "Execution continues normally.",
            "tool_call": {
                "tool_name": GOOGLE_DOCS_CREATE_DOCUMENT_TOOL,
                "input": {
                    "title": "DF FIRST REAL TEST VIA TOOL",
                    "content": "This document was created through Tool Layer v1.",
                },
            },
        },
        {
            "subtask_id": "23-1",
            "type": "analysis",
            "priority": 1,
            "instruction": "Create a Google Doc through Tool Layer v1",
        },
    )

    assert task["tool_call"] == {
        "tool_name": GOOGLE_DOCS_CREATE_DOCUMENT_TOOL,
        "input": {
            "title": "DF FIRST REAL TEST VIA TOOL",
            "content": "This document was created through Tool Layer v1.",
        },
    }


def test_commit_and_push_artifact_runs_expected_git_commands(monkeypatch) -> None:
    commands: list[list[str]] = []

    def fake_run(
        args: list[str],
        cwd: Path,
        capture_output: bool,
        text: bool,
        check: bool,
    ) -> SimpleNamespace:
        commands.append(args)

        outputs = {
            ("git", "rev-parse", "--abbrev-ref", "HEAD"): "issue-9\n",
            ("git", "rev-parse", "HEAD"): "abc123def456\n",
        }
        return SimpleNamespace(
            returncode=0,
            stdout=outputs.get(tuple(args), ""),
            stderr="",
        )

    monkeypatch.setattr(run_codex_task_module, "run_in_dev_env", fake_run)

    branch_name, commit_hash = commit_and_push_artifact(
        task_id=9,
        artifact_path=run_codex_task_module.REPO_ROOT / "artifacts" / "task-9.txt",
    )

    assert branch_name == "issue-9"
    assert commit_hash == "abc123def456"
    assert commands == [
        ["git", "add", "--", "artifacts/task-9.txt"],
        ["git", "commit", "-m", "DF task 9: execution result", "--", "artifacts/task-9.txt"],
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        ["git", "rev-parse", "HEAD"],
        ["git", "push"],
    ]


def test_main_updates_issue_after_successful_execution(monkeypatch, capsys, tmp_path) -> None:
    captured_metrics: dict[str, object] = {}
    task_path = tmp_path / "task-9.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 9,\n'
            '  "instruction": "Implement the task",\n'
            '  "constraints": "Do not break existing code. Modify only necessary parts.",\n'
            '  "success_criteria": "Code runs without errors and matches task description"\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        run_codex_task_module,
        "parse_args",
        lambda: SimpleNamespace(task_path=str(task_path), artifact_dir=str(tmp_path / "artifacts")),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "build_and_write_context_packet",
        lambda task, output_dir=None, repo_root=None: (
            {},
            tmp_path
            / "tasks"
            / "context"
            / f"task-{task.get('subtask_id', task['task_id'])}-context.json",
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "write_subtask_record",
        lambda record: write_subtask_record_helper(
            record,
            output_dir=tmp_path / "tasks" / "subtasks",
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "should_execute_task",
        lambda task_id, artifact_path, **kwargs: {
            "should_execute": True,
            "reason": "no final execution record found",
        },
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "validate_task_result",
        lambda artifact_path, task_instruction, required_paths=None: {
            "valid": True,
            "reason": "validation passed",
        },
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "commit_and_push_artifact",
        lambda task_id, artifact_path, **kwargs: (_ for _ in ()).throw(
            AssertionError("should not sync git during runtime completion")
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "update_issue_execution_status",
        lambda issue_number, commit_hash, artifact_path: (SimpleNamespace(number=issue_number), 321),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "mark_issue_validation_failed",
        lambda issue_number, reason: (_ for _ in ()).throw(AssertionError("should not fail")),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "log_task_run_metrics",
        lambda **kwargs: (
            captured_metrics.update(kwargs) or kwargs,
            tmp_path / "metrics" / "task-9-run-20260410T180000000Z.json",
        ),
    )

    exit_code = run_codex_task_module.main()

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "TASK_ID: 9" in output
    assert "SUBTASK_ID: 9-1" in output
    assert "ARTIFACT_WRITTEN:" in output
    assert "CONTEXT_PACKET:" in output
    assert "SUBTASKS_FILE:" in output
    assert "VALIDATION_VALID: True" in output
    assert "VALIDATION_REASON: validation passed" in output
    assert "COMMIT_HASH:" not in output
    assert "GIT_SYNC_STATUS:" not in output
    assert "issue updated" in output
    assert "comment id: 321" in output
    assert "METRICS_WRITTEN:" in output
    assert captured_metrics["status"] == "DONE"
    assert captured_metrics["instruction_text"] == "Implement the task"
    assert captured_metrics["validation_passed"] is True
    assert captured_metrics["validation_reason"] == "validation passed"
    assert captured_metrics["commit_hash"] is None
    assert captured_metrics["artifact_path"] == tmp_path / "artifacts" / "task-9-1.txt"
    assert captured_metrics["subtask_id"] == "9-1"
    assert captured_metrics["parent_task_id"] == 9


def test_main_logs_git_sync_failure_without_blocking_completion(
    monkeypatch,
    capsys,
    tmp_path,
) -> None:
    captured_metrics: dict[str, object] = {}
    task_path = tmp_path / "task-23.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 23,\n'
            '  "instruction": "Create the kickoff summary document",\n'
            '  "constraints": "Create a new doc only.",\n'
            '  "success_criteria": "Doc is created.",\n'
            '  "task_type": "external_write_google_doc",\n'
            '  "title": "Kickoff Summary",\n'
            '  "content": "Project kickoff summary text."\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setenv(run_codex_task_module.POST_EXECUTION_GIT_SYNC_ENV, "1")
    monkeypatch.setattr(
        run_codex_task_module,
        "parse_args",
        lambda: SimpleNamespace(task_path=str(task_path), artifact_dir=str(tmp_path / "artifacts")),
    )
    monkeypatch.setattr(run_codex_task_module, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(
        run_codex_task_module,
        "build_and_write_context_packet",
        lambda task, output_dir=None, repo_root=None: (
            {},
            tmp_path
            / "tasks"
            / "context"
            / f"task-{task.get('subtask_id', task['task_id'])}-context.json",
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "write_subtask_record",
        lambda record: write_subtask_record_helper(
            record,
            output_dir=tmp_path / "tasks" / "subtasks",
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "should_execute_task",
        lambda task_id, artifact_path, **kwargs: {
            "should_execute": True,
            "reason": "no final execution record found",
        },
    )
    monkeypatch.setattr(
        "integrations.google_docs_tool.create_google_doc",
        lambda payload: {
            "doc_id": "doc-abc123",
            "name": payload["title"],
            "url": "https://docs.google.com/document/d/doc-abc123",
        },
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "commit_and_push_artifact",
        lambda task_id, artifact_path, **kwargs: (_ for _ in ()).throw(
            RuntimeError("pathspec 'artifacts/doc-23.json' is ignored by one of your .gitignore files")
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "update_issue_execution_status",
        lambda issue_number, commit_hash, artifact_path: (SimpleNamespace(number=issue_number), 423),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "mark_issue_validation_failed",
        lambda issue_number, reason: (_ for _ in ()).throw(AssertionError("should not fail")),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "log_task_run_metrics",
        lambda **kwargs: (
            captured_metrics.update(kwargs) or kwargs,
            tmp_path / "metrics" / "task-23-run-20260410T180000000Z.json",
        ),
    )

    exit_code = run_codex_task_module.main()

    output = capsys.readouterr().out
    artifact_path = tmp_path / "artifacts" / "doc-23.json"
    progress_path = tmp_path / "tasks" / "subtasks" / "task-23-subtasks.json"
    progress_record = json.loads(progress_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert artifact_path.exists()
    assert progress_record["subtasks"][0]["status"] == "DONE"
    assert "issue updated" in output
    assert "comment id: 423" in output
    assert "GIT_SYNC_STATUS: failed" in output
    assert "ignored by one of your .gitignore files" in output
    assert captured_metrics["status"] == "DONE"
    assert captured_metrics["commit_hash"] is None
    assert captured_metrics["artifact_path"] == artifact_path


def test_main_marks_issue_failed_when_validation_fails(monkeypatch, capsys, tmp_path) -> None:
    captured_metrics: dict[str, object] = {}
    task_path = tmp_path / "task-9.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 9,\n'
            '  "instruction": "Implement the task",\n'
            '  "constraints": "Do not break existing code. Modify only necessary parts.",\n'
            '  "success_criteria": "Code runs without errors and matches task description"\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        run_codex_task_module,
        "parse_args",
        lambda: SimpleNamespace(task_path=str(task_path), artifact_dir=str(tmp_path / "artifacts")),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "build_and_write_context_packet",
        lambda task, output_dir=None, repo_root=None: (
            {},
            tmp_path
            / "tasks"
            / "context"
            / f"task-{task.get('subtask_id', task['task_id'])}-context.json",
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "write_subtask_record",
        lambda record: write_subtask_record_helper(
            record,
            output_dir=tmp_path / "tasks" / "subtasks",
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "should_execute_task",
        lambda task_id, artifact_path, **kwargs: {
            "should_execute": True,
            "reason": "no final execution record found",
        },
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "validate_task_result",
        lambda artifact_path, task_instruction, required_paths=None: {
            "valid": False,
            "reason": "artifact does not contain task instruction text",
        },
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "commit_and_push_artifact",
        lambda task_id, artifact_path, **kwargs: (_ for _ in ()).throw(
            AssertionError("should not commit")
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "update_issue_execution_status",
        lambda issue_number, commit_hash, artifact_path: (_ for _ in ()).throw(
            AssertionError("should not mark done")
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "mark_issue_validation_failed",
        lambda issue_number, reason: (SimpleNamespace(number=issue_number), 654),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "log_task_run_metrics",
        lambda **kwargs: (
            captured_metrics.update(kwargs) or kwargs,
            tmp_path / "metrics" / "task-9-run-20260410T180000000Z.json",
        ),
    )

    exit_code = run_codex_task_module.main()

    output = capsys.readouterr().out
    assert exit_code == 1
    assert "TASK_ID: 9" in output
    assert "SUBTASK_ID: 9-1" in output
    assert "ARTIFACT_WRITTEN:" in output
    assert "CONTEXT_PACKET:" in output
    assert "SUBTASKS_FILE:" in output
    assert "VALIDATION_VALID: False" in output
    assert "VALIDATION_REASON: artifact does not contain task instruction text" in output
    assert "issue updated" in output
    assert "comment id: 654" in output
    assert "METRICS_WRITTEN:" in output
    assert captured_metrics["status"] == "FAILED"
    assert captured_metrics["instruction_text"] == "Implement the task"
    assert captured_metrics["validation_passed"] is False
    assert captured_metrics["validation_reason"] == "artifact does not contain task instruction text"
    assert captured_metrics["commit_hash"] is None
    assert captured_metrics["artifact_path"] == tmp_path / "artifacts" / "task-9-1.txt"
    assert captured_metrics["subtask_id"] == "9-1"
    assert captured_metrics["parent_task_id"] == 9


def test_main_google_doc_task_marks_issue_failed_when_writer_fails(
    monkeypatch,
    capsys,
    tmp_path,
) -> None:
    captured_metrics: dict[str, object] = {}
    task_path = tmp_path / "task-73.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 73,\n'
            '  "instruction": "Create a Google Doc for the client summary",\n'
            '  "constraints": "Create only a new Google Doc.",\n'
            '  "success_criteria": "Document is created and URL is returned.",\n'
            '  "task_type": "external_write_google_doc",\n'
            '  "title": "Client Summary",\n'
            '  "content": "Ready-to-share client summary."\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        run_codex_task_module,
        "parse_args",
        lambda: SimpleNamespace(task_path=str(task_path), artifact_dir=str(tmp_path / "artifacts")),
    )
    monkeypatch.setattr(run_codex_task_module, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(
        run_codex_task_module,
        "build_and_write_context_packet",
        lambda task, output_dir=None, repo_root=None: (
            {},
            tmp_path
            / "tasks"
            / "context"
            / f"task-{task.get('subtask_id', task['task_id'])}-context.json",
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "write_subtask_record",
        lambda record: write_subtask_record_helper(
            record,
            output_dir=tmp_path / "tasks" / "subtasks",
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "should_execute_task",
        lambda task_id, artifact_path, **kwargs: {
            "should_execute": True,
            "reason": "no final execution record found",
        },
    )
    monkeypatch.setattr(
        "integrations.google_docs_tool.create_google_doc",
        lambda payload: (_ for _ in ()).throw(RuntimeError("google docs api failed")),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "commit_and_push_artifact",
        lambda task_id, artifact_path, **kwargs: (_ for _ in ()).throw(
            AssertionError("should not commit")
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "update_issue_execution_status",
        lambda issue_number, commit_hash, artifact_path: (_ for _ in ()).throw(
            AssertionError("should not mark done")
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "mark_issue_validation_failed",
        lambda issue_number, reason: (SimpleNamespace(number=issue_number), 909),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "log_task_run_metrics",
        lambda **kwargs: (
            captured_metrics.update(kwargs) or kwargs,
            tmp_path / "metrics" / "task-73-run-20260410T180000000Z.json",
        ),
    )

    exit_code = run_codex_task_module.main()

    output = capsys.readouterr().out
    artifact_path = tmp_path / "artifacts" / "doc-73.json"
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert exit_code == 1
    assert artifact["content_summary"] == "Ready-to-share client summary."
    assert artifact["doc_id"] == ""
    assert artifact["reason"] == "google docs api failed"
    assert artifact["url"] == ""
    assert isinstance(artifact["execution_timeline"], dict)
    assert isinstance(artifact["step_metrics"], list)
    assert isinstance(artifact["retry_info"], dict)
    assert "VALIDATION_VALID: False" in output
    assert "VALIDATION_REASON: google docs api failed" in output
    assert "issue updated" in output
    assert "comment id: 909" in output
    assert captured_metrics["status"] == "FAILED"
    assert captured_metrics["validation_passed"] is False
    assert captured_metrics["validation_reason"] == "google docs api failed"
    assert captured_metrics["artifact_path"] == artifact_path
    assert captured_metrics["subtask_id"] == "73-1"
    assert captured_metrics["parent_task_id"] == 73


def test_main_drive_to_google_doc_fails_safely_when_external_files_are_missing(
    monkeypatch,
    capsys,
    tmp_path,
) -> None:
    captured_metrics: dict[str, object] = {}
    task_path = tmp_path / "task-81.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 81,\n'
            '  "task_type": "drive_to_google_doc",\n'
            '  "instruction": "Create a client summary doc from Google Drive notes.",\n'
            '  "constraints": "Use only read-only Drive input and create a new Google Doc only.",\n'
            '  "success_criteria": "New Google Doc created from external file content.",\n'
            '  "external_context": {"google_drive": ["drive-file-missing"]},\n'
            '  "output_doc_title": "Client Summary",\n'
            '  "transform_mode": "plain_summary"\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        run_codex_task_module,
        "parse_args",
        lambda: SimpleNamespace(task_path=str(task_path), artifact_dir=str(tmp_path / "artifacts")),
    )
    monkeypatch.setattr(run_codex_task_module, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(
        run_codex_task_module,
        "build_and_write_context_packet",
        lambda task, output_dir=None, repo_root=None: build_and_write_context_packet_helper(
            task,
            output_dir=tmp_path / "tasks" / "context",
            repo_root=tmp_path,
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "write_subtask_record",
        lambda record: write_subtask_record_helper(
            record,
            output_dir=tmp_path / "tasks" / "subtasks",
        ),
    )
    monkeypatch.setattr("control.context_builder.read_google_drive_file", lambda payload: None)
    monkeypatch.setattr(
        run_codex_task_module,
        "should_execute_task",
        lambda task_id, artifact_path, **kwargs: {
            "should_execute": True,
            "reason": "no final execution record found",
        },
    )
    monkeypatch.setattr(
        "integrations.google_docs_tool.create_google_doc",
        lambda payload: (_ for _ in ()).throw(AssertionError("should not create a doc")),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "commit_and_push_artifact",
        lambda task_id, artifact_path, **kwargs: (_ for _ in ()).throw(
            AssertionError("should not commit")
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "update_issue_execution_status",
        lambda issue_number, commit_hash, artifact_path: (_ for _ in ()).throw(
            AssertionError("should not mark done")
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "mark_issue_validation_failed",
        lambda issue_number, reason: (SimpleNamespace(number=issue_number), 918),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "log_task_run_metrics",
        lambda **kwargs: (
            captured_metrics.update(kwargs) or kwargs,
            tmp_path / "metrics" / "task-81-run-20260410T180000000Z.json",
        ),
    )

    exit_code = run_codex_task_module.main()

    output = capsys.readouterr().out
    artifact_path = tmp_path / "artifacts" / "drive-to-doc-81.json"
    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert exit_code == 1
    assert artifact["content_summary"] == ""
    assert artifact["loaded_source_file_ids"] == []
    assert artifact["output_doc_id"] == ""
    assert artifact["output_doc_title"] == "Client Summary"
    assert artifact["output_doc_url"] == ""
    assert artifact["reason"] == "no external files were loaded from Google Drive context"
    assert artifact["source_file_ids"] == ["drive-file-missing"]
    assert artifact["transform_mode"] == "plain_summary"
    assert isinstance(artifact["execution_timeline"], dict)
    assert isinstance(artifact["step_metrics"], list)
    assert isinstance(artifact["retry_info"], dict)
    assert "VALIDATION_VALID: False" in output
    assert "VALIDATION_REASON: no external files were loaded from Google Drive context" in output
    assert "issue updated" in output
    assert "comment id: 918" in output
    assert captured_metrics["status"] == "FAILED"
    assert captured_metrics["validation_passed"] is False
    assert (
        captured_metrics["validation_reason"]
        == "no external files were loaded from Google Drive context"
    )
    assert captured_metrics["artifact_path"] == artifact_path
    assert captured_metrics["subtask_id"] == "81-1"
    assert captured_metrics["parent_task_id"] == 81


def test_main_skips_execution_when_guard_blocks_reprocessing(monkeypatch, capsys, tmp_path) -> None:
    captured_metrics: dict[str, object] = {}
    task_path = tmp_path / "task-9.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 9,\n'
            '  "instruction": "Implement the task",\n'
            '  "constraints": "Do not break existing code. Modify only necessary parts.",\n'
            '  "success_criteria": "Code runs without errors and matches task description"\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        run_codex_task_module,
        "parse_args",
        lambda: SimpleNamespace(task_path=str(task_path), artifact_dir=str(tmp_path / "artifacts")),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "build_and_write_context_packet",
        lambda task, output_dir=None, repo_root=None: (
            {},
            tmp_path
            / "tasks"
            / "context"
            / f"task-{task.get('subtask_id', task['task_id'])}-context.json",
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "write_subtask_record",
        lambda record: write_subtask_record_helper(
            record,
            output_dir=tmp_path / "tasks" / "subtasks",
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "should_execute_task",
        lambda task_id, artifact_path, **kwargs: {
            "should_execute": False,
            "reason": "issue #9 already has STATUS: DONE",
        },
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "run_codex_task",
        lambda task_path, artifact_dir=None: (_ for _ in ()).throw(AssertionError("should not run")),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "validate_task_result",
        lambda artifact_path, task_instruction, required_paths=None: (_ for _ in ()).throw(
            AssertionError("should not validate")
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "commit_and_push_artifact",
        lambda task_id, artifact_path, **kwargs: (_ for _ in ()).throw(
            AssertionError("should not commit")
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "update_issue_execution_status",
        lambda issue_number, commit_hash, artifact_path: (_ for _ in ()).throw(
            AssertionError("should not update issue")
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "mark_issue_validation_failed",
        lambda issue_number, reason: (_ for _ in ()).throw(
            AssertionError("should not mark failed")
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "log_task_run_metrics",
        lambda **kwargs: (
            captured_metrics.update(kwargs) or kwargs,
            tmp_path / "metrics" / "task-9-run-20260410T180000000Z.json",
        ),
    )

    exit_code = run_codex_task_module.main()

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "execution skipped: issue #9 already has STATUS: DONE" in output
    assert "METRICS_WRITTEN:" in output
    assert captured_metrics["status"] == "SKIPPED"
    assert captured_metrics["instruction_text"] == "Implement the task"
    assert captured_metrics["validation_passed"] is False
    assert captured_metrics["validation_reason"] == "issue #9 already has STATUS: DONE"
    assert captured_metrics["commit_hash"] is None
    assert captured_metrics["artifact_path"] is None
    assert captured_metrics["parent_task_id"] == 9


def test_main_skips_execution_when_final_artifact_exists_without_force_execution(
    monkeypatch, capsys, tmp_path
) -> None:
    captured_metrics: dict[str, object] = {}
    task_path = tmp_path / "task-9.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 9,\n'
            '  "instruction": "Implement the task",\n'
            '  "constraints": "Do not break existing code. Modify only necessary parts.",\n'
            '  "success_criteria": "Code runs without errors and matches task description"\n'
            "}\n"
        ),
        encoding="utf-8",
    )
    artifact_path = tmp_path / "artifacts" / "task-9-1.txt"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text("existing artifact", encoding="utf-8")

    monkeypatch.setattr(
        run_codex_task_module,
        "parse_args",
        lambda: SimpleNamespace(task_path=str(task_path), artifact_dir=str(tmp_path / "artifacts")),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "commit_and_push_artifact",
        lambda task_id, artifact_path, **kwargs: (_ for _ in ()).throw(
            AssertionError("should not commit")
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "update_issue_execution_status",
        lambda issue_number, commit_hash, artifact_path: (_ for _ in ()).throw(
            AssertionError("should not update issue")
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "log_task_run_metrics",
        lambda **kwargs: (
            captured_metrics.update(kwargs) or kwargs,
            tmp_path / "metrics" / "task-9-run-20260410T180000000Z.json",
        ),
    )

    exit_code = run_codex_task_module.main()

    output = capsys.readouterr().out
    assert exit_code == 0
    assert f"execution skipped: final artifact already exists at {artifact_path}" in output
    assert "FORCE_EXECUTION_USED:" not in output
    assert captured_metrics["status"] == "SKIPPED"
    assert captured_metrics["validation_reason"] == f"final artifact already exists at {artifact_path}"
    assert captured_metrics["artifact_path"] is None
    assert captured_metrics["force_execution_used"] is False


def test_main_force_execution_overrides_existing_final_artifact(
    monkeypatch, capsys, tmp_path
) -> None:
    captured_metrics: dict[str, object] = {}
    task_path = tmp_path / "task-9.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 9,\n'
            '  "instruction": "Implement the task",\n'
            '  "constraints": "Do not break existing code. Modify only necessary parts.",\n'
            '  "success_criteria": "Code runs without errors and matches task description",\n'
            '  "force_execution": true\n'
            "}\n"
        ),
        encoding="utf-8",
    )
    artifact_path = tmp_path / "artifacts" / "task-9-1.txt"
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_text("existing artifact", encoding="utf-8")

    monkeypatch.setattr(
        run_codex_task_module,
        "parse_args",
        lambda: SimpleNamespace(task_path=str(task_path), artifact_dir=str(tmp_path / "artifacts")),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "build_and_write_context_packet",
        lambda task, output_dir=None, repo_root=None: (
            {},
            tmp_path
            / "tasks"
            / "context"
            / f"task-{task.get('subtask_id', task['task_id'])}-context.json",
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "write_subtask_record",
        lambda record: write_subtask_record_helper(
            record,
            output_dir=tmp_path / "tasks" / "subtasks",
        ),
    )
    monkeypatch.setattr(
        task_guard_module,
        "fetch_github_issue",
        lambda issue_number, repo_name=None, token=None: SimpleNamespace(labels=[]),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "commit_and_push_artifact",
        lambda task_id, artifact_path, **kwargs: (_ for _ in ()).throw(
            AssertionError("should not sync git during runtime completion")
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "update_issue_execution_status",
        lambda issue_number, commit_hash, artifact_path: (SimpleNamespace(number=issue_number), 321),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "mark_issue_validation_failed",
        lambda issue_number, reason: (_ for _ in ()).throw(
            AssertionError("should not mark failed")
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "log_task_run_metrics",
        lambda **kwargs: (
            captured_metrics.update(kwargs) or kwargs,
            tmp_path / "metrics" / "task-9-run-20260410T180001000Z.json",
        ),
    )

    exit_code = run_codex_task_module.main()

    output = capsys.readouterr().out
    progress_path = tmp_path / "tasks" / "subtasks" / "task-9-subtasks.json"
    progress_record = json.loads(progress_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert "FORCE_EXECUTION_USED: True" in output
    assert "FORCE_EXECUTION_REASON: artifact_override" in output
    assert f"PRIOR_ARTIFACT_PATH: {artifact_path}" in output
    assert "ARTIFACT_WRITTEN:" in output
    assert captured_metrics["status"] == "DONE"
    assert captured_metrics["force_execution_used"] is True
    assert captured_metrics["force_execution_reason"] == "artifact_override"
    assert captured_metrics["prior_artifact_path"] == artifact_path
    assert captured_metrics["artifact_path"] == artifact_path
    assert progress_record["force_execution_used"] is True
    assert progress_record["force_execution_reason"] == "artifact_override"
    assert progress_record["prior_artifact_path"] == str(artifact_path)


def test_main_force_execution_without_existing_artifact_runs_normally(
    monkeypatch, capsys, tmp_path
) -> None:
    captured_metrics: dict[str, object] = {}
    task_path = tmp_path / "task-9.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 9,\n'
            '  "instruction": "Implement the task",\n'
            '  "constraints": "Do not break existing code. Modify only necessary parts.",\n'
            '  "success_criteria": "Code runs without errors and matches task description",\n'
            '  "force_execution": true\n'
            "}\n"
        ),
        encoding="utf-8",
    )
    artifact_path = tmp_path / "artifacts" / "task-9-1.txt"

    monkeypatch.setattr(
        run_codex_task_module,
        "parse_args",
        lambda: SimpleNamespace(task_path=str(task_path), artifact_dir=str(tmp_path / "artifacts")),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "build_and_write_context_packet",
        lambda task, output_dir=None, repo_root=None: (
            {},
            tmp_path
            / "tasks"
            / "context"
            / f"task-{task.get('subtask_id', task['task_id'])}-context.json",
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "write_subtask_record",
        lambda record: write_subtask_record_helper(
            record,
            output_dir=tmp_path / "tasks" / "subtasks",
        ),
    )
    monkeypatch.setattr(
        task_guard_module,
        "fetch_github_issue",
        lambda issue_number, repo_name=None, token=None: SimpleNamespace(labels=[]),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "commit_and_push_artifact",
        lambda task_id, artifact_path, **kwargs: (_ for _ in ()).throw(
            AssertionError("should not sync git during runtime completion")
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "update_issue_execution_status",
        lambda issue_number, commit_hash, artifact_path: (SimpleNamespace(number=issue_number), 654),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "mark_issue_validation_failed",
        lambda issue_number, reason: (_ for _ in ()).throw(
            AssertionError("should not mark failed")
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "log_task_run_metrics",
        lambda **kwargs: (
            captured_metrics.update(kwargs) or kwargs,
            tmp_path / "metrics" / "task-9-run-20260410T180002000Z.json",
        ),
    )

    exit_code = run_codex_task_module.main()

    output = capsys.readouterr().out
    progress_path = tmp_path / "tasks" / "subtasks" / "task-9-subtasks.json"
    progress_record = json.loads(progress_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert "FORCE_EXECUTION_USED:" not in output
    assert "ARTIFACT_WRITTEN:" in output
    assert artifact_path.exists()
    assert captured_metrics["status"] == "DONE"
    assert captured_metrics["artifact_path"] == artifact_path
    assert captured_metrics["force_execution_used"] is False
    assert captured_metrics["force_execution_reason"] is None
    assert captured_metrics["prior_artifact_path"] is None
    assert "force_execution_used" not in progress_record


def test_main_force_execution_overrides_done_issue_label(
    monkeypatch, capsys, tmp_path
) -> None:
    captured_metrics: dict[str, object] = {}
    task_path = tmp_path / "task-23.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 23,\n'
            '  "instruction": "Create a Google Doc through Tool Layer v1",\n'
            '  "force_execution": true,\n'
            '  "tool_call": {\n'
            '    "tool_name": "google_docs.create_document",\n'
            '    "input": {\n'
            '      "title": "DF FIRST REAL TEST VIA TOOL",\n'
            '      "content": "This document was created through Tool Layer v1."\n'
            "    }\n"
            "  }\n"
            "}\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        run_codex_task_module,
        "parse_args",
        lambda: SimpleNamespace(task_path=str(task_path), artifact_dir=str(tmp_path / "artifacts")),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "build_and_write_context_packet",
        lambda task, output_dir=None, repo_root=None: (
            {},
            tmp_path
            / "tasks"
            / "context"
            / f"task-{task.get('subtask_id', task['task_id'])}-context.json",
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "write_subtask_record",
        lambda record: write_subtask_record_helper(
            record,
            output_dir=tmp_path / "tasks" / "subtasks",
        ),
    )
    monkeypatch.setattr(
        task_guard_module,
        "fetch_github_issue",
        lambda issue_number, repo_name=None, token=None: SimpleNamespace(
            labels=[SimpleNamespace(name="STATUS: DONE")]
        ),
    )
    monkeypatch.setattr(
        "integrations.google_docs_tool.create_google_doc",
        lambda payload: {
            "doc_id": "doc-tool-live",
            "name": payload["title"],
            "url": "https://docs.google.com/document/d/doc-tool-live",
        },
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "commit_and_push_artifact",
        lambda task_id, artifact_path, **kwargs: (_ for _ in ()).throw(
            AssertionError("should not sync git during runtime completion")
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "update_issue_execution_status",
        lambda issue_number, commit_hash, artifact_path: (SimpleNamespace(number=issue_number), 654),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "mark_issue_validation_failed",
        lambda issue_number, reason: (_ for _ in ()).throw(
            AssertionError("should not mark failed")
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "log_task_run_metrics",
        lambda **kwargs: (
            captured_metrics.update(kwargs) or kwargs,
            tmp_path / "metrics" / "task-23-run-20260410T180003000Z.json",
        ),
    )

    exit_code = run_codex_task_module.main()

    output = capsys.readouterr().out
    progress_path = tmp_path / "tasks" / "subtasks" / "task-23-subtasks.json"
    progress_record = json.loads(progress_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert "FORCE_EXECUTION_USED: True" in output
    assert "FORCE_EXECUTION_REASON: done_label_override" in output
    assert "PRIOR_ARTIFACT_PATH:" not in output
    assert captured_metrics["status"] == "DONE"
    assert captured_metrics["force_execution_used"] is True
    assert captured_metrics["force_execution_reason"] == "done_label_override"
    assert captured_metrics["prior_artifact_path"] is None
    assert progress_record["force_execution_used"] is True
    assert progress_record["force_execution_reason"] == "done_label_override"
    assert "prior_artifact_path" not in progress_record


def test_main_executes_multiple_subtasks_sequentially(monkeypatch, capsys, tmp_path) -> None:
    metrics_calls: list[dict[str, object]] = []
    issue_updates: list[dict[str, object]] = []
    task_path = tmp_path / "task-9.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 9,\n'
            '  "instruction": "Create control/task_decomposer.py, modify scripts/run_codex_task.py, and validate the pipeline.",\n'
            '  "constraints": "Do not break existing code. Modify only necessary parts.",\n'
            '  "success_criteria": "Code runs without errors and matches task description"\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        run_codex_task_module,
        "parse_args",
        lambda: SimpleNamespace(task_path=str(task_path), artifact_dir=str(tmp_path / "artifacts")),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "decompose_task",
        lambda task: {
            "parent_task_id": 9,
            "subtasks": [
                {
                    "subtask_id": "9-1",
                    "instruction": "Create control/task_decomposer.py",
                    "type": "create_file",
                    "priority": 1,
                },
                {
                    "subtask_id": "9-2",
                    "instruction": "Modify scripts/run_codex_task.py",
                    "type": "modify_file",
                    "priority": 2,
                },
                {
                    "subtask_id": "9-3",
                    "instruction": "Validate the pipeline",
                    "type": "analysis",
                    "priority": 3,
                },
            ],
        },
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "build_and_write_context_packet",
        lambda task, output_dir=None, repo_root=None: (
            {},
            tmp_path
            / "tasks"
            / "context"
            / f"task-{task.get('subtask_id', task['task_id'])}-context.json",
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "write_subtask_record",
        lambda record: write_subtask_record_helper(
            record,
            output_dir=tmp_path / "tasks" / "subtasks",
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "should_execute_task",
        lambda task_id, artifact_path, **kwargs: {
            "should_execute": True,
            "reason": "no final execution record found",
        },
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "validate_task_result",
        lambda artifact_path, task_instruction, required_paths=None: {
            "valid": True,
            "reason": "validation passed",
        },
    )

    monkeypatch.setattr(
        run_codex_task_module,
        "commit_and_push_artifact",
        lambda task_id, artifact_path, **kwargs: (_ for _ in ()).throw(
            AssertionError("should not sync git during runtime completion")
        ),
    )

    def fake_update_issue_execution_status(issue_number: int, commit_hash: str, artifact_path: Path):
        issue_updates.append(
            {
                "issue_number": issue_number,
                "commit_hash": commit_hash,
                "artifact_path": artifact_path,
            }
        )
        return (SimpleNamespace(number=issue_number), 777)

    monkeypatch.setattr(
        run_codex_task_module,
        "update_issue_execution_status",
        fake_update_issue_execution_status,
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "mark_issue_validation_failed",
        lambda issue_number, reason: (_ for _ in ()).throw(AssertionError("should not fail")),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "log_task_run_metrics",
        lambda **kwargs: (
            metrics_calls.append(dict(kwargs)) or kwargs,
            tmp_path / "metrics" / f"task-9-run-{len(metrics_calls)}.json",
        ),
    )

    exit_code = run_codex_task_module.main()

    output = capsys.readouterr().out
    subtasks_path = tmp_path / "tasks" / "subtasks" / "task-9-subtasks.json"
    record = json.loads(subtasks_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert output.count("SUBTASK_ID:") == 3
    assert "SUBTASK_ID: 9-1" in output
    assert "SUBTASK_ID: 9-2" in output
    assert "SUBTASK_ID: 9-3" in output
    assert "issue updated" in output
    assert "comment id: 777" in output
    assert issue_updates == [
        {
            "issue_number": 9,
            "commit_hash": None,
            "artifact_path": subtasks_path,
        }
    ]
    assert [call["subtask_id"] for call in metrics_calls] == ["9-1", "9-2", "9-3"]
    assert all(call["status"] == "DONE" for call in metrics_calls)
    assert all(call["commit_hash"] is None for call in metrics_calls)
    assert [subtask["status"] for subtask in record["subtasks"]] == ["DONE", "DONE", "DONE"]


def test_main_personal_context_task_persists_updated_json_without_git_sync(
    monkeypatch, capsys, tmp_path
) -> None:
    metrics_calls: list[dict[str, object]] = []
    issue_updates: list[dict[str, object]] = []
    task_path = tmp_path / "task-61.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 61,\n'
            '  "instruction": "Update personal context with owner and DMV data",\n'
            '  "constraints": "Do not mix with business data.",\n'
            '  "success_criteria": "Personal context is updated.",\n'
            '  "task_type": "personal_context_update",\n'
            '  "personal_context_update": {\n'
            '    "owner": {"name": "Avery"},\n'
            '    "dmv": [{"id": "registration", "title": "Renew registration"}]\n'
            "  }\n"
            "}\n"
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        run_codex_task_module,
        "parse_args",
        lambda: SimpleNamespace(task_path=str(task_path), artifact_dir=str(tmp_path / "artifacts")),
    )
    monkeypatch.setattr(run_codex_task_module, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(
        run_codex_task_module,
        "build_and_write_context_packet",
        lambda task, output_dir=None, repo_root=None: (
            {},
            tmp_path
            / "tasks"
            / "context"
            / f"task-{task.get('subtask_id', task['task_id'])}-context.json",
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "write_subtask_record",
        lambda record: write_subtask_record_helper(
            record,
            output_dir=tmp_path / "tasks" / "subtasks",
        ),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "should_execute_task",
        lambda task_id, artifact_path, **kwargs: {
            "should_execute": True,
            "reason": "no final execution record found",
        },
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "validate_task_result",
        lambda artifact_path, task_instruction, required_paths=None: {
            "valid": True,
            "reason": "validation passed",
        },
    )

    monkeypatch.setattr(
        run_codex_task_module,
        "commit_and_push_artifact",
        lambda task_id, artifact_path, **kwargs: (_ for _ in ()).throw(
            AssertionError("should not sync git during runtime completion")
        ),
    )

    def fake_update_issue_execution_status(issue_number: int, commit_hash: str, artifact_path: Path):
        issue_updates.append(
            {
                "issue_number": issue_number,
                "commit_hash": commit_hash,
                "artifact_path": artifact_path,
            }
        )
        return (SimpleNamespace(number=issue_number), 881)

    monkeypatch.setattr(
        run_codex_task_module,
        "update_issue_execution_status",
        fake_update_issue_execution_status,
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "mark_issue_validation_failed",
        lambda issue_number, reason: (_ for _ in ()).throw(AssertionError("should not fail")),
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "log_task_run_metrics",
        lambda **kwargs: (
            metrics_calls.append(dict(kwargs)) or kwargs,
            tmp_path / "metrics" / f"task-61-run-{len(metrics_calls)}.json",
        ),
    )

    exit_code = run_codex_task_module.main()

    output = capsys.readouterr().out
    subtasks_path = tmp_path / "tasks" / "subtasks" / "task-61-subtasks.json"
    personal_context_path = tmp_path / "personal" / "personal_context.json"
    written_context = json.loads(personal_context_path.read_text(encoding="utf-8"))

    assert exit_code == 0
    assert "SUBTASK_ID: 61-1" in output
    assert "issue updated" in output
    assert "comment id: 881" in output
    assert written_context["owner"] == {"name": "Avery", "notes": ""}
    assert written_context["dmv"] == [{"id": "registration", "title": "Renew registration"}]
    assert issue_updates == [
        {
            "issue_number": 61,
            "commit_hash": None,
            "artifact_path": subtasks_path,
        }
    ]
    assert metrics_calls[0]["subtask_id"] == "61-1"
    assert metrics_calls[0]["commit_hash"] is None
