from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_METRICS_DIR = REPO_ROOT / "metrics"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _isoformat_utc(value: datetime) -> str:
    normalized = _coerce_utc(value)
    return normalized.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _filename_timestamp(value: datetime) -> str:
    normalized = _coerce_utc(value)
    return normalized.strftime("%Y%m%dT%H%M%S") + f"{normalized.microsecond // 1000:03d}Z"


def _normalize_path(value: Path | str | None) -> str | None:
    if value is None:
        return None
    return str(value).replace("\\", "/")


def metrics_output_path(
    task_id: int,
    finished_at: datetime,
    output_dir: Path | str | None = None,
) -> Path:
    base_dir = Path(output_dir) if output_dir is not None else DEFAULT_METRICS_DIR
    timestamp = _filename_timestamp(finished_at)
    return base_dir / f"task-{task_id}-run-{timestamp}.json"


def build_task_run_metrics(
    task_id: int,
    status: str,
    started_at: datetime,
    finished_at: datetime,
    duration_ms: int,
    instruction_text: str,
    validation_passed: bool,
    validation_reason: str,
    commit_hash: str | None,
    artifact_path: Path | str | None,
    subtask_id: str | None = None,
    parent_task_id: int | None = None,
    force_execution_used: bool = False,
    force_execution_reason: str | None = None,
    prior_artifact_path: Path | str | None = None,
    tool_name: str | None = None,
    tool_ok: bool | None = None,
    tool_input_summary: dict[str, object] | None = None,
    tool_output_summary: dict[str, object] | None = None,
    tool_error_code: str | None = None,
    pipeline_trace: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    normalized_instruction = str(instruction_text)
    metrics = {
        "task_id": task_id,
        "status": status,
        "started_at": _isoformat_utc(started_at),
        "finished_at": _isoformat_utc(finished_at),
        "duration_ms": int(duration_ms),
        "estimated_tokens": (len(normalized_instruction) + 3) // 4,
        "actual_tokens": None,
        "cost_usd": None,
        "validation_passed": bool(validation_passed),
        "validation_reason": str(validation_reason),
        "commit_hash": commit_hash,
        "artifact_path": _normalize_path(artifact_path),
    }
    if subtask_id is not None:
        metrics["subtask_id"] = str(subtask_id)
    if parent_task_id is not None:
        metrics["parent_task_id"] = int(parent_task_id)
    if force_execution_used:
        metrics["force_execution_used"] = True
        metrics["force_execution_reason"] = str(force_execution_reason or "")
        metrics["prior_artifact_path"] = _normalize_path(prior_artifact_path)
    normalized_tool_name = str(tool_name or "").strip()
    if normalized_tool_name:
        metrics["tool_name"] = normalized_tool_name
    if tool_ok is not None:
        metrics["tool_ok"] = bool(tool_ok)
    if isinstance(tool_input_summary, dict):
        metrics["tool_input_summary"] = dict(tool_input_summary)
    if isinstance(tool_output_summary, dict):
        metrics["tool_output_summary"] = dict(tool_output_summary)
    normalized_tool_error_code = str(tool_error_code or "").strip()
    if normalized_tool_error_code:
        metrics["tool_error_code"] = normalized_tool_error_code
    if isinstance(pipeline_trace, list):
        metrics["pipeline_trace"] = [
            dict(step_trace)
            for step_trace in pipeline_trace
            if isinstance(step_trace, dict)
        ]
    return metrics


def log_task_run_metrics(
    task_id: int,
    status: str,
    started_at: datetime,
    finished_at: datetime,
    duration_ms: int,
    instruction_text: str,
    validation_passed: bool,
    validation_reason: str,
    commit_hash: str | None,
    artifact_path: Path | str | None,
    output_dir: Path | str | None = None,
    subtask_id: str | None = None,
    parent_task_id: int | None = None,
    force_execution_used: bool = False,
    force_execution_reason: str | None = None,
    prior_artifact_path: Path | str | None = None,
    tool_name: str | None = None,
    tool_ok: bool | None = None,
    tool_input_summary: dict[str, object] | None = None,
    tool_output_summary: dict[str, object] | None = None,
    tool_error_code: str | None = None,
    pipeline_trace: list[dict[str, object]] | None = None,
) -> tuple[dict[str, object], Path]:
    metrics = build_task_run_metrics(
        task_id=task_id,
        status=status,
        started_at=started_at,
        finished_at=finished_at,
        duration_ms=duration_ms,
        instruction_text=instruction_text,
        validation_passed=validation_passed,
        validation_reason=validation_reason,
        commit_hash=commit_hash,
        artifact_path=artifact_path,
        subtask_id=subtask_id,
        parent_task_id=parent_task_id,
        force_execution_used=force_execution_used,
        force_execution_reason=force_execution_reason,
        prior_artifact_path=prior_artifact_path,
        tool_name=tool_name,
        tool_ok=tool_ok,
        tool_input_summary=tool_input_summary,
        tool_output_summary=tool_output_summary,
        tool_error_code=tool_error_code,
        pipeline_trace=pipeline_trace,
    )
    path = metrics_output_path(task_id, finished_at=finished_at, output_dir=output_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(metrics, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return metrics, path
