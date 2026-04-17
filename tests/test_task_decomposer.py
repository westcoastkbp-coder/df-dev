from __future__ import annotations

import json

from control.task_decomposer import decompose_task, write_subtask_record


def test_decompose_task_returns_single_subtask_for_simple_instruction() -> None:
    decomposition = decompose_task(
        {
            "task_id": 9,
            "instruction": "Implement the task",
        }
    )

    assert decomposition == {
        "parent_task_id": 9,
        "subtasks": [
            {
                "subtask_id": "9-1",
                "instruction": "Implement the task",
                "type": "modify_file",
                "priority": 1,
            }
        ],
    }


def test_decompose_task_splits_multiple_actions_in_order() -> None:
    decomposition = decompose_task(
        {
            "task_id": 11,
            "instruction": (
                "Create control/task_decomposer.py, modify scripts/run_codex_task.py, "
                "and validate the pipeline."
            ),
        }
    )

    assert decomposition == {
        "parent_task_id": 11,
        "subtasks": [
            {
                "subtask_id": "11-1",
                "instruction": "Create control/task_decomposer.py",
                "type": "create_file",
                "priority": 1,
            },
            {
                "subtask_id": "11-2",
                "instruction": "modify scripts/run_codex_task.py",
                "type": "modify_file",
                "priority": 2,
            },
            {
                "subtask_id": "11-3",
                "instruction": "validate the pipeline",
                "type": "analysis",
                "priority": 3,
            },
        ],
    }


def test_decompose_task_splits_multiple_files_when_action_is_shared() -> None:
    decomposition = decompose_task(
        {
            "task_id": 15,
            "instruction": (
                "Modify control/context_builder.py and control/validate_task.py "
                "to support subtask ids."
            ),
        }
    )

    assert decomposition["subtasks"] == [
        {
            "subtask_id": "15-1",
            "instruction": "Modify control/context_builder.py to support subtask ids",
            "type": "modify_file",
            "priority": 1,
        },
        {
            "subtask_id": "15-2",
            "instruction": "Modify control/validate_task.py to support subtask ids",
            "type": "modify_file",
            "priority": 2,
        },
    ]


def test_decompose_task_limits_total_subtasks_to_five() -> None:
    decomposition = decompose_task(
        {
            "task_id": 21,
            "instruction": (
                "Create a.py, create b.py, create c.py, create d.py, create e.py, "
                "and validate the result."
            ),
        }
    )

    assert len(decomposition["subtasks"]) == 5
    assert decomposition["subtasks"][-1]["instruction"] == "create e.py; validate the result"


def test_write_subtask_record_writes_expected_json(tmp_path) -> None:
    record = {
        "parent_task_id": 9,
        "subtasks": [
            {
                "subtask_id": "9-1",
                "instruction": "Create control/task_decomposer.py",
                "type": "create_file",
                "priority": 1,
                "status": "DONE",
                "artifact_path": "D:/digital_foreman/artifacts/task-9-1.txt",
                "context_packet_path": "D:/digital_foreman/tasks/context/task-9-1-context.json",
                "validation": {
                    "valid": True,
                    "reason": "validation passed",
                },
            }
        ],
    }

    path = write_subtask_record(record, output_dir=tmp_path / "subtasks")

    assert path == tmp_path / "subtasks" / "task-9-subtasks.json"
    assert json.loads(path.read_text(encoding="utf-8")) == record


def test_decompose_task_keeps_personal_context_updates_as_single_subtask() -> None:
    decomposition = decompose_task(
        {
            "task_id": 50,
            "instruction": "Update personal context with owner and reminder records",
            "task_type": "personal_context_update",
            "personal_context_update": {
                "owner": {"name": "Alex"},
                "reminders": [{"id": "dmv", "title": "Renew registration"}],
            },
        }
    )

    assert decomposition == {
        "parent_task_id": 50,
        "subtasks": [
            {
                "subtask_id": "50-1",
                "instruction": "Update personal context with owner and reminder records",
                "type": "modify_file",
                "priority": 1,
            }
        ],
    }


def test_decompose_task_keeps_google_doc_write_as_single_subtask() -> None:
    decomposition = decompose_task(
        {
            "task_id": 51,
            "instruction": "Create a Google Doc for the kickoff summary",
            "task_type": "external_write_google_doc",
            "title": "Kickoff Summary",
            "content": "Kickoff summary text.",
        }
    )

    assert decomposition == {
        "parent_task_id": 51,
        "subtasks": [
            {
                "subtask_id": "51-1",
                "instruction": "Create a Google Doc for the kickoff summary",
                "type": "create_file",
                "priority": 1,
            }
        ],
    }


def test_decompose_task_keeps_drive_to_google_doc_as_single_subtask() -> None:
    decomposition = decompose_task(
        {
            "task_id": 81,
            "instruction": "Create a client summary doc from Google Drive notes.",
            "task_type": "drive_to_google_doc",
            "external_context": {"google_drive": ["drive-file-001"]},
            "output_doc_title": "Client Summary",
            "transform_mode": "plain_summary",
        }
    )

    assert decomposition == {
        "parent_task_id": 81,
        "subtasks": [
            {
                "subtask_id": "81-1",
                "instruction": "Create a client summary doc from Google Drive notes",
                "type": "create_file",
                "priority": 1,
            }
        ],
    }
