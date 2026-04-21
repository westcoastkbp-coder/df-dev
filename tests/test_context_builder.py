from __future__ import annotations

from control.context_builder import (
    MAX_INCLUDED_FILES,
    build_context_packet,
    write_context_packet,
)


def test_build_context_packet_respects_fixed_file_limit(tmp_path) -> None:
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "a.json").write_text('{"a":1}\n', encoding="utf-8")
    (tmp_path / "config" / "b.json").write_text('{"b":1}\n', encoding="utf-8")
    (tmp_path / "README.md").write_text("README", encoding="utf-8")
    (tmp_path / "control").mkdir()
    (tmp_path / "control" / "task_to_codex.py").write_text("pass\n", encoding="utf-8")
    (tmp_path / "control" / "task_guard.py").write_text("pass\n", encoding="utf-8")
    (tmp_path / "control" / "validate_task.py").write_text("pass\n", encoding="utf-8")
    (tmp_path / "control" / "metrics_logger.py").write_text("pass\n", encoding="utf-8")
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "run_codex_task.py").write_text("pass\n", encoding="utf-8")

    packet = build_context_packet(
        {
            "task_id": 22,
            "instruction": "Keep the context deterministic",
            "constraints": "Do not break existing code.",
            "success_criteria": "Context packet is small.",
        },
        repo_root=tmp_path,
    )

    included_paths = [
        entry["relative_path"]
        for entry in packet["config_files"] + packet["related_files"]
    ]

    assert len(included_paths) == MAX_INCLUDED_FILES
    assert included_paths == [
        "config/a.json",
        "config/b.json",
        "README.md",
        "control/task_to_codex.py",
        "scripts/run_codex_task.py",
        "control/task_guard.py",
    ]


def test_write_context_packet_uses_subtask_identifier_when_present(tmp_path) -> None:
    path = write_context_packet(
        {
            "task_id": 9,
            "subtask_id": "9-2",
            "instruction": "Modify scripts/run_codex_task.py",
            "constraints": "Do not break existing code.",
            "success_criteria": "Code runs without errors.",
            "config_files": [],
            "related_files": [],
            "notes": "Selection mode: deterministic.",
        },
        output_dir=tmp_path / "tasks" / "context",
    )

    assert path == tmp_path / "tasks" / "context" / "task-9-2-context.json"


def test_build_context_packet_uses_personal_fallback_for_personal_tasks(
    tmp_path,
) -> None:
    (tmp_path / "modules" / "personal").mkdir(parents=True)
    (tmp_path / "modules" / "personal" / "schema.json").write_text(
        "{}", encoding="utf-8"
    )
    (tmp_path / "personal").mkdir()
    (tmp_path / "personal" / "personal_context.json").write_text("{}", encoding="utf-8")
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "update_personal_context.py").write_text(
        "pass\n", encoding="utf-8"
    )

    packet = build_context_packet(
        {
            "task_id": 31,
            "instruction": "Update personal context with a reminder",
            "task_type": "personal_context_update",
            "personal_context_update": {
                "reminders": [{"id": "registration", "title": "Renew registration"}],
            },
        },
        repo_root=tmp_path,
    )

    included_paths = [entry["relative_path"] for entry in packet["related_files"]]
    assert included_paths == [
        "modules/personal/schema.json",
        "personal/personal_context.json",
        "scripts/update_personal_context.py",
    ]


def test_build_context_packet_includes_google_drive_external_files(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "control.context_builder.read_google_drive_file",
        lambda payload: {
            "file_id": payload["drive_file_id"],
            "name": "Scope Notes",
            "content": "Read-only content",
            "size": 17,
        },
    )

    packet = build_context_packet(
        {
            "task_id": 52,
            "instruction": "Use the external project notes",
            "external_context": {"google_drive": ["drive-file-001"]},
        },
        repo_root=tmp_path,
    )

    assert packet["external_files"] == [
        {
            "file_id": "drive-file-001",
            "name": "Scope Notes",
            "content": "Read-only content",
            "size": 17,
        }
    ]


def test_build_context_packet_returns_empty_external_files_on_google_drive_failure(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "control.context_builder.read_google_drive_file", lambda payload: None
    )

    packet = build_context_packet(
        {
            "task_id": 53,
            "instruction": "Use the external project notes",
            "external_context": {"google_drive": ["drive-file-002"]},
        },
        repo_root=tmp_path,
    )

    assert packet["external_files"] == []
