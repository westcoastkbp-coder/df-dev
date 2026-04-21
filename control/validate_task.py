from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable


def _validate_google_doc_artifact(artifact_text: str) -> dict[str, object] | None:
    try:
        payload = json.loads(artifact_text)
    except json.JSONDecodeError:
        return None

    if not isinstance(payload, dict):
        return None
    if not {"doc_id", "url", "content_summary"}.issubset(payload):
        return None

    doc_id = str(payload.get("doc_id") or "").strip()
    url = str(payload.get("url") or "").strip()
    reason = str(payload.get("reason") or "").strip()

    if not doc_id:
        return {
            "valid": False,
            "reason": reason or "google doc artifact doc_id is empty",
        }

    expected_prefix = f"https://docs.google.com/document/d/{doc_id}"
    if not url.startswith(expected_prefix):
        return {
            "valid": False,
            "reason": reason or "google doc artifact url is invalid",
        }

    return {
        "valid": True,
        "reason": "validation passed",
    }


def _normalized_string_list(value) -> list[str]:
    if isinstance(value, str):
        candidate = value.strip()
        return [candidate] if candidate else []
    if not isinstance(value, list):
        return []
    normalized: list[str] = []
    for item in value:
        candidate = str(item or "").strip()
        if candidate:
            normalized.append(candidate)
    return normalized


def _validate_drive_to_google_doc_artifact(
    artifact_text: str,
) -> dict[str, object] | None:
    try:
        payload = json.loads(artifact_text)
    except json.JSONDecodeError:
        return None

    if not isinstance(payload, dict):
        return None
    if not {
        "source_file_ids",
        "output_doc_id",
        "output_doc_url",
        "output_doc_title",
        "content_summary",
    }.issubset(payload):
        return None

    reason = str(payload.get("reason") or "").strip()
    loaded_source_file_ids = _normalized_string_list(
        payload.get("loaded_source_file_ids")
    )
    source_file_ids = loaded_source_file_ids or _normalized_string_list(
        payload.get("source_file_ids")
    )

    if not source_file_ids and not reason:
        return {
            "valid": False,
            "reason": "drive_to_google_doc artifact missing loaded external files",
        }

    output_doc_title = str(payload.get("output_doc_title") or "").strip()
    if not output_doc_title:
        return {
            "valid": False,
            "reason": reason
            or "drive_to_google_doc artifact output_doc_title is empty",
        }

    output_doc_id = str(payload.get("output_doc_id") or "").strip()
    if not output_doc_id:
        return {
            "valid": False,
            "reason": reason or "drive_to_google_doc artifact output_doc_id is empty",
        }

    output_doc_url = str(payload.get("output_doc_url") or "").strip()
    expected_prefix = f"https://docs.google.com/document/d/{output_doc_id}"
    if not output_doc_url.startswith(expected_prefix):
        return {
            "valid": False,
            "reason": reason
            or "drive_to_google_doc artifact output_doc_url is invalid",
        }

    return {
        "valid": True,
        "reason": "validation passed",
    }


def _validate_google_drive_read_artifact(
    artifact_text: str,
) -> dict[str, object] | None:
    try:
        payload = json.loads(artifact_text)
    except json.JSONDecodeError:
        return None

    if not isinstance(payload, dict):
        return None
    if payload.get("tool_name") != "google_drive.read_file":
        return None

    tool_ok = bool(payload.get("tool_ok"))
    reason = str(payload.get("reason") or "").strip()

    if not tool_ok:
        return {
            "valid": False,
            "reason": reason
            or str(payload.get("tool_error_code") or "").strip()
            or "drive read failed",
        }

    if not {"file_id", "name", "mime_type", "content_text"}.issubset(payload):
        return {
            "valid": False,
            "reason": "google_drive.read_file artifact is missing output fields",
        }

    if not str(payload.get("file_id") or "").strip():
        return {
            "valid": False,
            "reason": "google_drive.read_file artifact file_id is empty",
        }

    return {
        "valid": True,
        "reason": "validation passed",
    }


def _validate_claude_analyze_artifact(artifact_text: str) -> dict[str, object] | None:
    try:
        payload = json.loads(artifact_text)
    except json.JSONDecodeError:
        return None

    if not isinstance(payload, dict):
        return None
    if payload.get("tool_name") != "claude.analyze":
        return None

    tool_ok = bool(payload.get("tool_ok"))
    reason = str(payload.get("reason") or "").strip()

    if not tool_ok:
        return {
            "valid": False,
            "reason": (
                reason
                or str(payload.get("tool_error_code") or "").strip()
                or "claude analyze failed"
            ),
        }

    analysis = str(payload.get("analysis") or "").strip()
    if not analysis:
        return {
            "valid": False,
            "reason": "claude.analyze artifact analysis is empty",
        }

    return {
        "valid": True,
        "reason": "validation passed",
    }


def _validate_pipeline_artifact(artifact_text: str) -> dict[str, object] | None:
    try:
        payload = json.loads(artifact_text)
    except json.JSONDecodeError:
        return None

    if not isinstance(payload, dict):
        return None
    if not {"pipeline_steps", "pipeline_trace", "final_output"}.issubset(payload):
        return None

    reason = str(payload.get("reason") or "").strip()
    pipeline_trace = payload.get("pipeline_trace")
    if not isinstance(pipeline_trace, list) or not pipeline_trace:
        return {
            "valid": False,
            "reason": reason or "pipeline artifact is missing trace data",
        }

    final_output = payload.get("final_output")
    if not isinstance(final_output, dict):
        return {
            "valid": False,
            "reason": reason or "pipeline artifact final_output is invalid",
        }

    if any(
        not bool(step.get("success"))
        for step in pipeline_trace
        if isinstance(step, dict)
    ):
        return {
            "valid": False,
            "reason": reason or "pipeline artifact contains a failed step",
        }

    doc_id = str(final_output.get("doc_id") or "").strip()
    if doc_id:
        url = str(final_output.get("url") or "").strip()
        expected_prefix = f"https://docs.google.com/document/d/{doc_id}"
        if not url.startswith(expected_prefix):
            return {
                "valid": False,
                "reason": reason or "pipeline artifact final_output.url is invalid",
            }
        return {
            "valid": True,
            "reason": "validation passed",
        }

    draft_id = str(final_output.get("draft_id") or "").strip()
    if draft_id:
        subject = str(
            final_output.get("source_subject") or final_output.get("subject") or ""
        ).strip()
        if not subject:
            return {
                "valid": False,
                "reason": reason or "pipeline artifact final_output.subject is empty",
            }
        if final_output.get("draft_created") is not True:
            return {
                "valid": False,
                "reason": reason
                or "pipeline artifact final_output.draft_created is false",
            }
        return {
            "valid": True,
            "reason": "validation passed",
        }

    if not final_output:
        return {
            "valid": False,
            "reason": reason or "pipeline artifact final_output is empty",
        }

    return {
        "valid": True,
        "reason": "validation passed",
    }


def validate_task_result(
    artifact_path: Path | str,
    task_instruction: str,
    required_paths: Iterable[Path | str] | None = None,
) -> dict[str, object]:
    path = Path(artifact_path)
    if not path.exists():
        return {
            "valid": False,
            "reason": "artifact file does not exist",
        }

    artifact_text = path.read_text(encoding="utf-8")
    if not artifact_text.strip():
        return {
            "valid": False,
            "reason": "artifact file is empty",
        }

    google_doc_validation = _validate_google_doc_artifact(artifact_text)
    if google_doc_validation is not None:
        return google_doc_validation

    drive_to_google_doc_validation = _validate_drive_to_google_doc_artifact(
        artifact_text
    )
    if drive_to_google_doc_validation is not None:
        return drive_to_google_doc_validation

    google_drive_read_validation = _validate_google_drive_read_artifact(artifact_text)
    if google_drive_read_validation is not None:
        return google_drive_read_validation

    claude_analyze_validation = _validate_claude_analyze_artifact(artifact_text)
    if claude_analyze_validation is not None:
        return claude_analyze_validation

    pipeline_validation = _validate_pipeline_artifact(artifact_text)
    if pipeline_validation is not None:
        return pipeline_validation

    if str(task_instruction) not in artifact_text:
        return {
            "valid": False,
            "reason": "artifact does not contain task instruction text",
        }

    for required_path in required_paths or ():
        required = Path(required_path)
        if not required.exists():
            return {
                "valid": False,
                "reason": f"required output does not exist: {required}",
            }

    return {
        "valid": True,
        "reason": "validation passed",
    }
