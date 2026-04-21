from __future__ import annotations

import json
from datetime import datetime, timezone

from control.metrics_logger import log_task_run_metrics


def test_log_task_run_metrics_writes_predictable_json(tmp_path) -> None:
    started_at = datetime(2026, 4, 10, 18, 0, 0, 123000, tzinfo=timezone.utc)
    finished_at = datetime(2026, 4, 10, 18, 0, 1, 456000, tzinfo=timezone.utc)

    metrics, path = log_task_run_metrics(
        task_id=9,
        status="DONE",
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=1333,
        instruction_text="Implement the task",
        validation_passed=True,
        validation_reason="validation passed",
        commit_hash="abc123def456",
        artifact_path="D:/digital_foreman/artifacts/task-9.txt",
        output_dir=tmp_path / "metrics",
    )

    assert path == tmp_path / "metrics" / "task-9-run-20260410T180001456Z.json"
    assert metrics == {
        "task_id": 9,
        "status": "DONE",
        "started_at": "2026-04-10T18:00:00.123Z",
        "finished_at": "2026-04-10T18:00:01.456Z",
        "duration_ms": 1333,
        "estimated_tokens": 5,
        "actual_tokens": None,
        "cost_usd": None,
        "validation_passed": True,
        "validation_reason": "validation passed",
        "commit_hash": "abc123def456",
        "artifact_path": "D:/digital_foreman/artifacts/task-9.txt",
    }
    assert json.loads(path.read_text(encoding="utf-8")) == metrics


def test_log_task_run_metrics_includes_subtask_metadata_when_present(tmp_path) -> None:
    started_at = datetime(2026, 4, 10, 18, 0, 0, tzinfo=timezone.utc)
    finished_at = datetime(2026, 4, 10, 18, 0, 1, tzinfo=timezone.utc)

    metrics, _ = log_task_run_metrics(
        task_id=9,
        status="DONE",
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=1000,
        instruction_text="Modify scripts/run_codex_task.py",
        validation_passed=True,
        validation_reason="validation passed",
        commit_hash="abc123def456",
        artifact_path="D:/digital_foreman/artifacts/task-9-2.txt",
        output_dir=tmp_path / "metrics",
        subtask_id="9-2",
        parent_task_id=9,
    )

    assert metrics["subtask_id"] == "9-2"
    assert metrics["parent_task_id"] == 9


def test_log_task_run_metrics_includes_tool_trace_when_present(tmp_path) -> None:
    started_at = datetime(2026, 4, 10, 18, 0, 0, tzinfo=timezone.utc)
    finished_at = datetime(2026, 4, 10, 18, 0, 1, tzinfo=timezone.utc)

    metrics, _ = log_task_run_metrics(
        task_id=23,
        status="DONE",
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=1000,
        instruction_text="Create a Google Doc through Tool Layer v1",
        validation_passed=True,
        validation_reason="validation passed",
        commit_hash=None,
        artifact_path="D:/digital_foreman/artifacts/doc-23.json",
        output_dir=tmp_path / "metrics",
        subtask_id="23-1",
        parent_task_id=23,
        tool_name="google_docs.create_document",
        tool_ok=True,
        tool_input_summary={
            "title": "DF FIRST REAL TEST VIA TOOL",
            "content_chars": 48,
        },
        tool_output_summary={
            "doc_id": "doc-tool-456",
            "url": "https://docs.google.com/document/d/doc-tool-456",
        },
    )

    assert metrics["tool_name"] == "google_docs.create_document"
    assert metrics["tool_ok"] is True
    assert metrics["tool_input_summary"] == {
        "title": "DF FIRST REAL TEST VIA TOOL",
        "content_chars": 48,
    }
    assert metrics["tool_output_summary"] == {
        "doc_id": "doc-tool-456",
        "url": "https://docs.google.com/document/d/doc-tool-456",
    }
