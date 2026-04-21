from __future__ import annotations

import json

from control.validate_task import validate_task_result


def test_validate_task_result_returns_valid_for_matching_artifact(tmp_path) -> None:
    artifact_path = tmp_path / "task-9.txt"
    artifact_path.write_text("Implement the task", encoding="utf-8")

    result = validate_task_result(
        artifact_path=artifact_path,
        task_instruction="Implement the task",
    )

    assert result == {
        "valid": True,
        "reason": "validation passed",
    }


def test_validate_task_result_requires_existing_artifact() -> None:
    result = validate_task_result(
        artifact_path="D:/missing/task-9.txt",
        task_instruction="Implement the task",
    )

    assert result == {
        "valid": False,
        "reason": "artifact file does not exist",
    }


def test_validate_task_result_requires_non_empty_artifact(tmp_path) -> None:
    artifact_path = tmp_path / "task-9.txt"
    artifact_path.write_text("", encoding="utf-8")

    result = validate_task_result(
        artifact_path=artifact_path,
        task_instruction="Implement the task",
    )

    assert result == {
        "valid": False,
        "reason": "artifact file is empty",
    }


def test_validate_task_result_requires_instruction_text(tmp_path) -> None:
    artifact_path = tmp_path / "task-9.txt"
    artifact_path.write_text("Different content", encoding="utf-8")

    result = validate_task_result(
        artifact_path=artifact_path,
        task_instruction="Implement the task",
    )

    assert result == {
        "valid": False,
        "reason": "artifact does not contain task instruction text",
    }


def test_validate_task_result_requires_additional_output_paths(tmp_path) -> None:
    artifact_path = tmp_path / "task-9.txt"
    artifact_path.write_text("Implement the task", encoding="utf-8")

    result = validate_task_result(
        artifact_path=artifact_path,
        task_instruction="Implement the task",
        required_paths=[tmp_path / "personal" / "personal_context.json"],
    )

    assert result == {
        "valid": False,
        "reason": f"required output does not exist: {tmp_path / 'personal' / 'personal_context.json'}",
    }


def test_validate_task_result_accepts_google_doc_artifact(tmp_path) -> None:
    artifact_path = tmp_path / "doc-9.json"
    artifact_path.write_text(
        json.dumps(
            {
                "doc_id": "doc-xyz789",
                "url": "https://docs.google.com/document/d/doc-xyz789",
                "content_summary": "Project kickoff summary",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = validate_task_result(
        artifact_path=artifact_path,
        task_instruction="Create the kickoff summary document",
    )

    assert result == {
        "valid": True,
        "reason": "validation passed",
    }


def test_validate_task_result_returns_google_doc_failure_reason(tmp_path) -> None:
    artifact_path = tmp_path / "doc-10.json"
    artifact_path.write_text(
        json.dumps(
            {
                "doc_id": "",
                "url": "",
                "content_summary": "Project kickoff summary",
                "reason": "google docs api failed",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = validate_task_result(
        artifact_path=artifact_path,
        task_instruction="Create the kickoff summary document",
    )

    assert result == {
        "valid": False,
        "reason": "google docs api failed",
    }


def test_validate_task_result_accepts_drive_to_google_doc_artifact(tmp_path) -> None:
    artifact_path = tmp_path / "drive-to-doc-81.json"
    artifact_path.write_text(
        json.dumps(
            {
                "source_file_ids": ["drive-file-001"],
                "loaded_source_file_ids": ["drive-file-001"],
                "output_doc_id": "doc-xyz789",
                "output_doc_title": "Client Summary",
                "output_doc_url": "https://docs.google.com/document/d/doc-xyz789",
                "content_summary": "Document Title: Client Summary",
                "transform_mode": "plain_summary",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = validate_task_result(
        artifact_path=artifact_path,
        task_instruction="Create a client summary doc from Google Drive notes.",
    )

    assert result == {
        "valid": True,
        "reason": "validation passed",
    }


def test_validate_task_result_returns_drive_to_google_doc_failure_reason(
    tmp_path,
) -> None:
    artifact_path = tmp_path / "drive-to-doc-82.json"
    artifact_path.write_text(
        json.dumps(
            {
                "source_file_ids": ["drive-file-002"],
                "loaded_source_file_ids": [],
                "output_doc_id": "",
                "output_doc_title": "Client Summary",
                "output_doc_url": "",
                "content_summary": "",
                "transform_mode": "plain_summary",
                "reason": "no external files were loaded from Google Drive context",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = validate_task_result(
        artifact_path=artifact_path,
        task_instruction="Create a client summary doc from Google Drive notes.",
    )

    assert result == {
        "valid": False,
        "reason": "no external files were loaded from Google Drive context",
    }


def test_validate_task_result_accepts_email_pipeline_artifact(tmp_path) -> None:
    artifact_path = tmp_path / "pipeline-91.json"
    artifact_path.write_text(
        json.dumps(
            {
                "pipeline_steps": [
                    {
                        "step_index": 1,
                        "tool_name": "gmail.read_latest",
                        "output_key": "email_data",
                    },
                    {
                        "step_index": 2,
                        "tool_name": "claude.analyze",
                        "output_key": "analysis_data",
                    },
                    {"step_index": 3, "tool_name": "gmail.create_draft"},
                ],
                "pipeline_trace": [
                    {"step_index": 1, "success": True},
                    {"step_index": 2, "success": True},
                    {"step_index": 3, "success": True},
                ],
                "final_output": {
                    "draft_id": "draft-xyz789",
                    "subject": "Re: Client follow-up",
                    "source_subject": "Client follow-up",
                    "draft_created": True,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = validate_task_result(
        artifact_path=artifact_path,
        task_instruction="Create a reply draft from the latest email.",
    )

    assert result == {
        "valid": True,
        "reason": "validation passed",
    }
