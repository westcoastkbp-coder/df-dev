from __future__ import annotations

import json

from control.task_to_codex import (
    build_codex_task,
    transform_task_packet_to_codex,
    write_codex_task,
)


def test_build_codex_task_is_deterministic() -> None:
    task_packet = {
        "issue_number": 9,
        "title": "DF SYSTEM TEST ISSUE 001",
        "body": "Created by github_issue_agent.py from Digital Foreman control system.",
        "labels": ["STATUS: DONE"],
    }

    first = build_codex_task(task_packet)
    second = build_codex_task(task_packet)

    assert first == second
    assert first["task_id"] == 9
    assert first["labels"] == ["STATUS: DONE"]
    assert first["instruction"]
    assert "DF SYSTEM TEST ISSUE 001" in first["instruction"]
    assert "Created by github_issue_agent.py from Digital Foreman control system." in first["instruction"]
    assert first["constraints"]
    assert "Do not break existing code" in first["constraints"]
    assert "Modify only necessary parts" in first["constraints"]
    assert first["success_criteria"]
    assert "deterministic" in first["success_criteria"].lower()


def test_write_codex_task_uses_predictable_path(tmp_path) -> None:
    codex_task = {
        "task_id": 9,
        "instruction": "DF SYSTEM TEST ISSUE 001",
        "constraints": "Do not break existing code. Modify only necessary parts.",
        "success_criteria": "Code runs without errors and matches task description",
        "labels": ["STATUS: DONE"],
    }

    path = write_codex_task(codex_task, output_dir=tmp_path / "tasks" / "codex")

    assert path == tmp_path / "tasks" / "codex" / "task-9.json"
    assert json.loads(path.read_text(encoding="utf-8")) == codex_task


def test_transform_task_packet_to_codex_writes_context_packet_for_fresh_task(tmp_path) -> None:
    packet_path = tmp_path / "tasks" / "github" / "issue-12.json"
    packet_path.parent.mkdir(parents=True, exist_ok=True)
    packet_path.write_text(
        json.dumps(
            {
                "issue_number": 12,
                "title": "Add deterministic context",
                "body": "Use a small local context packet before execution.",
                "labels": ["codex"],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "product_box_manifest.json").write_text("{}", encoding="utf-8")
    (tmp_path / "README.md").write_text("Digital Foreman", encoding="utf-8")
    (tmp_path / "control").mkdir()
    (tmp_path / "control" / "task_to_codex.py").write_text("pass\n", encoding="utf-8")
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "run_codex_task.py").write_text("pass\n", encoding="utf-8")

    codex_task, codex_task_path = transform_task_packet_to_codex(
        packet_path,
        output_dir=tmp_path / "tasks" / "codex",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    context_path = tmp_path / "tasks" / "context" / "task-12-context.json"
    context_packet = json.loads(context_path.read_text(encoding="utf-8"))

    assert codex_task_path == tmp_path / "tasks" / "codex" / "task-12.json"
    assert codex_task["context_packet_path"] == str(context_path)
    assert context_packet["task_id"] == 12
    assert context_packet["title"] == "Add deterministic context"
    assert "Add deterministic context" in context_packet["instruction"]
    assert "Use a small local context packet before execution." in context_packet["instruction"]
    assert context_packet["config_files"] == [
        {
            "content": "{}",
            "content_mode": "full_text",
            "relative_path": "config/product_box_manifest.json",
            "size_bytes": 2,
            "truncated": False,
        }
    ]


def test_build_codex_task_preserves_personal_context_payload() -> None:
    task_packet = {
        "issue_number": 44,
        "title": "Update personal reminder",
        "body": "Add DMV renewal reminder to the owner context.",
        "task_type": "personal_context_update",
        "personal_context_update": {
            "reminders": [
                {
                    "id": "dmv-renewal",
                    "title": "Renew registration",
                }
            ]
        },
    }

    codex_task = build_codex_task(task_packet)

    assert codex_task["task_type"] == "personal_context_update"
    assert codex_task["personal_context_update"] == task_packet["personal_context_update"]
    assert codex_task["file_paths"] == [
        "modules/personal/schema.json",
        "personal/personal_context.json",
        "scripts/update_personal_context.py",
    ]


def test_build_codex_task_preserves_external_context() -> None:
    task_packet = {
        "issue_number": 72,
        "title": "Read external notes",
        "body": "Use Google Drive as supporting context.",
        "external_context": {"google_drive": ["drive-file-123"]},
    }

    codex_task = build_codex_task(task_packet)

    assert codex_task["external_context"] == {"google_drive": ["drive-file-123"]}


def test_build_codex_task_preserves_google_doc_payload() -> None:
    task_packet = {
        "issue_number": 74,
        "title": "Kickoff Summary",
        "body": "Create a Google Doc for the kickoff.",
        "task_type": "external_write_google_doc",
        "content": "Kickoff content for the new document.",
    }

    codex_task = build_codex_task(task_packet)

    assert codex_task["task_type"] == "external_write_google_doc"
    assert codex_task["title"] == "Kickoff Summary"
    assert codex_task["content"] == "Kickoff content for the new document."


def test_build_codex_task_preserves_tool_call() -> None:
    task_packet = {
        "issue_number": 75,
        "title": "Create a Google Doc through Tool Layer v1",
        "body": "Use the standardized tool path.",
        "tool_call": {
            "tool_name": "google_docs.create_document",
            "input": {
                "title": "DF FIRST REAL TEST VIA TOOL",
                "content": "This document was created through Tool Layer v1.",
            },
        },
    }

    codex_task = build_codex_task(task_packet)

    assert codex_task["tool_call"] == task_packet["tool_call"]
