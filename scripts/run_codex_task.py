from __future__ import annotations

import os
from pathlib import Path


_REPO_ENV_PATH = Path(__file__).resolve().parents[1] / ".env"


def _bootstrap_repo_env() -> None:
    if not _REPO_ENV_PATH.is_file():
        return

    for raw_line in _REPO_ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in raw_line:
            continue

        name, value = raw_line.split("=", 1)
        env_name = name.strip()
        if not env_name or env_name in os.environ:
            continue
        os.environ[env_name] = value.strip()


_bootstrap_repo_env()

import argparse
import copy
import json
import re
import subprocess
import sys
from time import perf_counter
from typing import Any, Sequence

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from control.context_builder import build_and_write_context_packet
from control.dev_runtime import run_in_dev_env
from control.external_transformer import (
    DEFAULT_TRANSFORM_MODE,
    build_drive_to_google_doc_content,
)
from control.github_issue_status_update import (
    mark_issue_validation_failed,
    update_issue_execution_status,
)
from control.metrics_logger import log_task_run_metrics, utc_now
from control.task_decomposer import decompose_task, write_subtask_record
from control.task_guard import should_execute_task
from control.task_to_codex import (
    DEFAULT_CONSTRAINTS,
    DEFAULT_SUCCESS_CRITERIA,
    read_task_packet,
)
from control.tool_executor import execute_tool_call
from control.tool_registry import (
    CLAUDE_ANALYZE_TOOL,
    GOOGLE_DOCS_CREATE_DOCUMENT_TOOL,
    GOOGLE_DRIVE_READ_FILE_TOOL,
)
from control.validate_task import validate_task_result
from app.memory import resolve_memory
from app.policy.cross_domain_conflict_gate import evaluate_cross_domain_conflict
from app.policy.execution_invariants import check_invariants
from app.policy.memory_policy_gate import evaluate_memory_policy
from app.policy.replay_protection import check_replay
from app.state.state_store import set_state
from app.storage import StorageAdapterError, save_artifact
from app.trace.execution_trace import create_execution_trace
from memory.memory_store import load_required_execution_system_context
from modules.personal.context_store import (
    DEFAULT_PERSONAL_CONTEXT_PATH,
    PERSONAL_CONTEXT_TASK_TYPE,
    extract_personal_context_update,
    update_personal_context_file,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARTIFACT_DIR = REPO_ROOT / "artifacts"
EXTERNAL_WRITE_GOOGLE_DOC_TASK_TYPE = "external_write_google_doc"
DRIVE_TO_GOOGLE_DOC_TASK_TYPE = "drive_to_google_doc"
GOOGLE_DOC_ARTIFACT_PREFIX = "doc"
DRIVE_TO_DOC_ARTIFACT_PREFIX = "drive-to-doc"
DRIVE_READ_ARTIFACT_PREFIX = "drive-read"
CLAUDE_ANALYZE_ARTIFACT_PREFIX = "claude-analyze"
PIPELINE_ARTIFACT_PREFIX = "pipeline"
GOOGLE_DOC_SUMMARY_CHARS = 200
POST_EXECUTION_GIT_SYNC_ENV = "DIGITAL_FOREMAN_ENABLE_POST_EXECUTION_GIT_SYNC"
PIPELINE_VARIABLE_PATTERN = re.compile(
    r"\{\{\s*([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)\s*\}\}"
)
FULL_PIPELINE_VARIABLE_PATTERN = re.compile(
    r"^\{\{\s*([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)\s*\}\}$"
)
EXECUTION_LOG_FILE_NAME = "execution.log"


def _load_repo_env(env_path: Path | str | None = None) -> None:
    path = Path(env_path) if env_path is not None else _REPO_ENV_PATH
    if not path.is_file():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in raw_line:
            continue

        name, value = raw_line.split("=", 1)
        env_name = name.strip()
        if not env_name or env_name in os.environ:
            continue
        os.environ[env_name] = value.strip()


def _load_task_source(task_path: Path | str | dict[str, Any]) -> dict[str, Any]:
    if isinstance(task_path, dict):
        task = dict(task_path)
    else:
        task = read_task_packet(task_path)

    normalized_task = dict(task)
    normalized_task["constraints"] = str(
        normalized_task.get("constraints") or DEFAULT_CONSTRAINTS
    )
    normalized_task["success_criteria"] = str(
        normalized_task.get("success_criteria") or DEFAULT_SUCCESS_CRITERIA
    )
    return normalized_task


def load_codex_task(
    task_path: Path | str | dict[str, Any],
    *,
    context_output_dir: Path | str | None = None,
    repo_root: Path | str | None = None,
) -> dict[str, object]:
    _load_repo_env()
    task = _load_task_source(task_path)
    _, context_path = build_and_write_context_packet(
        task,
        output_dir=context_output_dir,
        repo_root=repo_root,
    )

    loaded_task: dict[str, object] = {
        "task_id": int(task["task_id"]),
        "instruction": str(task["instruction"]),
        "constraints": str(task.get("constraints") or DEFAULT_CONSTRAINTS),
        "success_criteria": str(
            task.get("success_criteria") or DEFAULT_SUCCESS_CRITERIA
        ),
        "context_packet_path": str(context_path),
    }
    if task.get("parent_task_id") is not None:
        loaded_task["parent_task_id"] = int(task["parent_task_id"])
    if task.get("subtask_id") is not None:
        loaded_task["subtask_id"] = str(task["subtask_id"])
    if task.get("subtask_type") is not None:
        loaded_task["subtask_type"] = str(task["subtask_type"])
    if task.get("priority") is not None:
        loaded_task["priority"] = int(task["priority"])
    if task.get("task_type") is not None:
        loaded_task["task_type"] = str(task["task_type"])
    if task.get("personal_context_update") is not None:
        loaded_task["personal_context_update"] = copy.deepcopy(
            task["personal_context_update"]
        )
    if task.get("file_paths") is not None:
        loaded_task["file_paths"] = copy.deepcopy(task["file_paths"])
    if task.get("external_context") is not None:
        loaded_task["external_context"] = copy.deepcopy(task["external_context"])
    if task.get("memory_context") is not None:
        loaded_task["memory_context"] = copy.deepcopy(task["memory_context"])
    if task.get("payload") is not None:
        loaded_task["payload"] = copy.deepcopy(task["payload"])
    if task.get("title") is not None:
        loaded_task["title"] = str(task["title"])
    if task.get("document_title") is not None:
        loaded_task["document_title"] = str(task["document_title"])
    if task.get("output_doc_title") is not None:
        loaded_task["output_doc_title"] = str(task["output_doc_title"])
    if task.get("transform_mode") is not None:
        loaded_task["transform_mode"] = str(task["transform_mode"])
    if task.get("content") is not None:
        loaded_task["content"] = str(task["content"])
    if task.get("force_execution") is not None:
        loaded_task["force_execution"] = bool(task["force_execution"])
    if task.get("domain") is not None:
        loaded_task["domain"] = str(task["domain"])
    if task.get("resource_id") is not None:
        loaded_task["resource_id"] = str(task["resource_id"])
    if task.get("logical_id") is not None:
        loaded_task["logical_id"] = str(task["logical_id"])
    if task.get("idempotency_key") is not None:
        loaded_task["idempotency_key"] = str(task["idempotency_key"])
    if task.get("status") is not None:
        loaded_task["status"] = str(task["status"])
    if task.get("tool_call") is not None:
        loaded_task["tool_call"] = copy.deepcopy(task["tool_call"])
    if isinstance(task.get("pipeline"), list):
        loaded_task["pipeline"] = copy.deepcopy(task["pipeline"])
    return loaded_task


def _attach_resolved_memory(task: dict[str, Any]) -> dict[str, Any]:
    memory_context = task.get("memory_context")
    if not isinstance(memory_context, dict):
        task["resolved_memory"] = []
        return task

    task["resolved_memory"] = resolve_memory(memory_context)
    return task


def artifact_path_for_task(
    task_id: int,
    artifact_dir: Path | str | None = None,
    *,
    subtask_id: str | None = None,
    task_type: str | None = None,
    tool_name: str | None = None,
    has_pipeline: bool = False,
) -> Path:
    base_dir = Path(artifact_dir) if artifact_dir is not None else DEFAULT_ARTIFACT_DIR
    normalized_task_type = str(task_type or "").strip()
    normalized_tool_name = str(tool_name or "").strip()
    if has_pipeline:
        return base_dir / f"{PIPELINE_ARTIFACT_PREFIX}-{task_id}.json"
    if normalized_task_type == EXTERNAL_WRITE_GOOGLE_DOC_TASK_TYPE:
        return base_dir / f"{GOOGLE_DOC_ARTIFACT_PREFIX}-{task_id}.json"
    if normalized_tool_name == GOOGLE_DOCS_CREATE_DOCUMENT_TOOL:
        return base_dir / f"{GOOGLE_DOC_ARTIFACT_PREFIX}-{task_id}.json"
    if normalized_tool_name == GOOGLE_DRIVE_READ_FILE_TOOL:
        return base_dir / f"{DRIVE_READ_ARTIFACT_PREFIX}-{task_id}.json"
    if normalized_tool_name == CLAUDE_ANALYZE_TOOL:
        return base_dir / f"{CLAUDE_ANALYZE_ARTIFACT_PREFIX}-{task_id}.json"
    if normalized_task_type == DRIVE_TO_GOOGLE_DOC_TASK_TYPE:
        return base_dir / f"{DRIVE_TO_DOC_ARTIFACT_PREFIX}-{task_id}.json"
    identifier = str(subtask_id or task_id)
    return base_dir / f"task-{identifier}.txt"


def write_task_artifact(
    task: dict[str, object],
    artifact_dir: Path | str | None = None,
    *,
    artifact_text: str | None = None,
) -> Path:
    path = artifact_path_for_task(
        int(task["task_id"]),
        artifact_dir=artifact_dir,
        subtask_id=(
            str(task["subtask_id"]) if task.get("subtask_id") is not None else None
        ),
        task_type=str(task.get("task_type") or ""),
        tool_name=_tool_name(task),
        has_pipeline=_is_pipeline_task(task),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        artifact_text if artifact_text is not None else str(task["instruction"]),
        encoding="utf-8",
    )
    return path


def _is_personal_context_task(task: dict[str, Any]) -> bool:
    return str(
        task.get("task_type") or ""
    ).strip() == PERSONAL_CONTEXT_TASK_TYPE or isinstance(
        task.get("personal_context_update"), dict
    )


def _is_google_doc_write_task(task: dict[str, Any]) -> bool:
    return (
        str(task.get("task_type") or "").strip() == EXTERNAL_WRITE_GOOGLE_DOC_TASK_TYPE
    )


def _is_drive_to_google_doc_task(task: dict[str, Any]) -> bool:
    return str(task.get("task_type") or "").strip() == DRIVE_TO_GOOGLE_DOC_TASK_TYPE


def _is_tool_call_task(task: dict[str, Any]) -> bool:
    return isinstance(task.get("tool_call"), dict)


def _is_pipeline_task(task: dict[str, Any]) -> bool:
    return isinstance(task.get("pipeline"), list)


def _tool_name(task: dict[str, Any]) -> str:
    tool_call = task.get("tool_call")
    if not isinstance(tool_call, dict):
        return ""
    return str(tool_call.get("tool_name") or "").strip()


def _tool_input_payload(task: dict[str, Any]) -> dict[str, Any]:
    tool_call = task.get("tool_call")
    if not isinstance(tool_call, dict):
        return {}

    input_payload = tool_call.get("input")
    if not isinstance(input_payload, dict):
        return {}

    return copy.deepcopy(input_payload)


def _google_doc_title(task: dict[str, Any]) -> str:
    explicit_title = str(task.get("document_title") or "").strip()
    if explicit_title:
        return explicit_title
    task_title = str(task.get("title") or "").strip()
    if task_title:
        return task_title
    return f"DF Task {task['task_id']}"


def _google_doc_content(task: dict[str, Any]) -> str:
    return str(task.get("content") or "")


def _google_doc_artifact_payload(
    task: dict[str, Any],
    *,
    doc_id: str = "",
    url: str = "",
    reason: str = "",
    content: str | None = None,
) -> dict[str, str]:
    payload = {
        "doc_id": str(doc_id or "").strip(),
        "url": str(url or "").strip(),
        "content_summary": str(
            _google_doc_content(task) if content is None else content
        )[:GOOGLE_DOC_SUMMARY_CHARS],
    }
    if str(reason or "").strip():
        payload["reason"] = str(reason).strip()
    return payload


def _utc_now_iso() -> str:
    return utc_now().isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _duration_ms(start_perf: float) -> int:
    return max(0, int((perf_counter() - start_perf) * 1000))


def _retry_info(step_metrics: Sequence[dict[str, object]]) -> dict[str, Any]:
    return {
        "total_retry_count": sum(
            int(step.get("retry_count") or 0) for step in step_metrics
        ),
        "steps": [
            {
                "step_index": int(step.get("step_index") or 0),
                "tool_name": str(step.get("tool_name") or "").strip(),
                "retry_count": int(step.get("retry_count") or 0),
            }
            for step in step_metrics
            if isinstance(step, dict)
        ],
    }


def _run_execution_timeline(
    *,
    start_time: str,
    start_perf: float,
    step_metrics: Sequence[dict[str, object]],
) -> dict[str, Any]:
    end_time = _utc_now_iso()
    total_duration_ms = _duration_ms(start_perf)
    return {
        "start_time": str(start_time),
        "end_time": end_time,
        "total_duration": int(total_duration_ms),
        "total_duration_ms": int(total_duration_ms),
        "steps": [
            {
                "step_index": int(step.get("step_index") or 0),
                "tool_name": str(step.get("tool_name") or "").strip(),
                "step_start_time": str(step.get("step_start_time") or "").strip(),
                "step_end_time": str(step.get("step_end_time") or "").strip(),
                "step_duration": int(step.get("step_duration") or 0),
                "step_duration_ms": int(step.get("step_duration_ms") or 0),
            }
            for step in step_metrics
            if isinstance(step, dict)
        ],
    }


def _execution_log_path(repo_root: Path | str | None = None) -> Path:
    base_dir = Path(repo_root) if repo_root is not None else REPO_ROOT
    return base_dir / "logs" / EXECUTION_LOG_FILE_NAME


def _append_execution_log(
    *,
    repo_root: Path | str | None,
    task: dict[str, Any],
    status: str,
    execution_timeline: dict[str, Any],
    step_metrics: Sequence[dict[str, object]],
    retry_info: dict[str, Any],
    artifact_path: Path,
) -> Path:
    log_path = _execution_log_path(repo_root)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "task_id": int(task["task_id"]),
        "command": str(task.get("instruction") or "").strip(),
        "status": str(status),
        "duration": int(execution_timeline.get("total_duration_ms") or 0),
        "steps": [dict(step) for step in step_metrics if isinstance(step, dict)],
        "execution_timeline": dict(execution_timeline),
        "retry_info": dict(retry_info),
        "artifact_path": str(artifact_path),
    }
    with log_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True) + "\n")
    return log_path


def _debug_print(enabled: bool, message: str) -> None:
    if enabled:
        print(f"DEBUG: {message}")


def _step_metric(
    *,
    step_index: int,
    tool_name: str,
    step_start_time: str,
    step_duration_ms: int,
    success: bool,
    retry_count: int = 0,
    failure_reason: str = "",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "step_index": int(step_index),
        "tool_name": str(tool_name or "").strip(),
        "success": bool(success),
        "status": "success" if success else "failure",
        "step_start_time": str(step_start_time),
        "step_end_time": _utc_now_iso(),
        "step_duration": int(step_duration_ms),
        "step_duration_ms": int(step_duration_ms),
        "retry_count": int(retry_count),
    }
    if str(failure_reason or "").strip():
        payload["failure_reason"] = str(failure_reason).strip()
    return payload


def _tool_retry_count(tool_result: dict[str, Any]) -> int:
    retry_info = tool_result.get("retry_info")
    if not isinstance(retry_info, dict):
        return 0
    return int(retry_info.get("total_retry_count") or 0)


def _tool_failure_reason(tool_result: dict[str, Any]) -> str:
    error_payload = tool_result.get("error")
    if not isinstance(error_payload, dict):
        return ""
    return str(error_payload.get("message") or error_payload.get("code") or "").strip()


def _decorate_trace_with_observability(
    trace: dict[str, object],
    *,
    step_start_time: str,
    step_duration_ms: int,
    success: bool,
    retry_count: int = 0,
    failure_reason: str = "",
    tool_result: dict[str, Any] | None = None,
) -> dict[str, object]:
    enriched_trace = dict(trace)
    enriched_trace["success"] = bool(success)
    enriched_trace["status"] = "success" if success else "failure"
    enriched_trace["step_start_time"] = str(step_start_time)
    enriched_trace["step_end_time"] = _utc_now_iso()
    enriched_trace["step_duration"] = int(step_duration_ms)
    enriched_trace["step_duration_ms"] = int(step_duration_ms)
    enriched_trace["retry_count"] = int(retry_count)
    if str(failure_reason or "").strip():
        enriched_trace["failure_reason"] = str(failure_reason).strip()
    if isinstance(tool_result, dict):
        execution_timeline = tool_result.get("execution_timeline")
        if isinstance(execution_timeline, dict):
            enriched_trace["tool_execution_timeline"] = copy.deepcopy(
                execution_timeline
            )
        tool_step_metrics = tool_result.get("step_metrics")
        if isinstance(tool_step_metrics, list):
            enriched_trace["tool_step_metrics"] = [
                dict(step) for step in tool_step_metrics if isinstance(step, dict)
            ]
        retry_info = tool_result.get("retry_info")
        if isinstance(retry_info, dict):
            enriched_trace["tool_retry_info"] = dict(retry_info)
        network_diagnostics = tool_result.get("network_diagnostics")
        if isinstance(network_diagnostics, dict):
            enriched_trace["network_diagnostics"] = copy.deepcopy(network_diagnostics)
    return enriched_trace


def _step_metrics_from_trace(
    pipeline_trace: Sequence[dict[str, object]],
) -> list[dict[str, object]]:
    return [
        {
            key: copy.deepcopy(value)
            for key, value in step_trace.items()
            if key
            in {
                "step_index",
                "tool_name",
                "success",
                "status",
                "step_start_time",
                "step_end_time",
                "step_duration",
                "step_duration_ms",
                "retry_count",
                "failure_reason",
                "tool_error_code",
                "tool_source",
                "network_diagnostics",
            }
        }
        for step_trace in pipeline_trace
        if isinstance(step_trace, dict)
    ]


def _tool_input_summary(
    tool_name: str, input_payload: dict[str, Any]
) -> dict[str, object]:
    if tool_name == CLAUDE_ANALYZE_TOOL:
        return {
            "text_chars": len(str(input_payload.get("text") or "")),
        }
    if tool_name == GOOGLE_DOCS_CREATE_DOCUMENT_TOOL:
        return {
            "title": str(input_payload.get("title") or "").strip(),
            "content_chars": len(str(input_payload.get("content") or "")),
        }
    if tool_name == GOOGLE_DRIVE_READ_FILE_TOOL:
        return {
            "file_id": str(input_payload.get("file_id") or "").strip(),
        }

    return {
        "input_keys": sorted(str(key) for key in input_payload),
    }


def _tool_output_summary(
    tool_name: str, output_payload: dict[str, Any]
) -> dict[str, object]:
    if tool_name == CLAUDE_ANALYZE_TOOL:
        return {
            "analysis_chars": len(str(output_payload.get("analysis") or "")),
        }
    if tool_name == GOOGLE_DOCS_CREATE_DOCUMENT_TOOL:
        return {
            "doc_id": str(output_payload.get("doc_id") or "").strip(),
            "url": str(output_payload.get("url") or "").strip(),
        }
    if tool_name == GOOGLE_DRIVE_READ_FILE_TOOL:
        return {
            "name": str(output_payload.get("name") or "").strip(),
            "mime_type": str(output_payload.get("mime_type") or "").strip(),
        }

    return {
        "output_keys": sorted(str(key) for key in output_payload),
    }


def _build_tool_trace(
    tool_name: str,
    input_payload: dict[str, Any],
    tool_result: dict[str, Any],
) -> dict[str, object]:
    trace: dict[str, object] = {
        "tool_name": tool_name,
        "tool_ok": bool(tool_result.get("ok")),
        "tool_input_summary": _tool_input_summary(tool_name, input_payload),
    }
    tool_source = str(tool_result.get("source") or "").strip()
    if tool_source:
        trace["tool_source"] = tool_source

    if bool(tool_result.get("ok")):
        output_payload = tool_result.get("output")
        if isinstance(output_payload, dict):
            trace["tool_output_summary"] = _tool_output_summary(
                tool_name, output_payload
            )
    else:
        error_payload = tool_result.get("error")
        if isinstance(error_payload, dict):
            tool_error_code = str(error_payload.get("code") or "").strip()
            if tool_error_code:
                trace["tool_error_code"] = tool_error_code
            failure_reason = str(error_payload.get("message") or "").strip()
            if failure_reason:
                trace["failure_reason"] = failure_reason

    trace["retry_count"] = _tool_retry_count(tool_result)
    execution_timeline = tool_result.get("execution_timeline")
    if isinstance(execution_timeline, dict):
        trace["execution_timeline"] = copy.deepcopy(execution_timeline)
    step_metrics = tool_result.get("step_metrics")
    if isinstance(step_metrics, list):
        trace["step_metrics"] = [
            dict(step) for step in step_metrics if isinstance(step, dict)
        ]
    retry_info = tool_result.get("retry_info")
    if isinstance(retry_info, dict):
        trace["retry_info"] = dict(retry_info)
    network_diagnostics = tool_result.get("network_diagnostics")
    if isinstance(network_diagnostics, dict):
        trace["network_diagnostics"] = copy.deepcopy(network_diagnostics)
    return trace


def _pipeline_trace_entry(
    step_index: int,
    tool_name: str,
    input_payload: dict[str, Any],
    tool_result: dict[str, Any],
) -> dict[str, object]:
    trace = _build_tool_trace(tool_name, input_payload, tool_result)
    trace["step_index"] = int(step_index)
    return trace


def _empty_input_step_trace(
    step_index: int,
    tool_name: str,
    input_payload: dict[str, Any],
) -> dict[str, object]:
    return {
        "step_index": int(step_index),
        "tool_name": tool_name,
        "tool_ok": False,
        "tool_error_code": "EMPTY_INPUT",
        "tool_input_summary": _tool_input_summary(tool_name, input_payload),
    }


def _resolve_pipeline_reference(reference: str, context: dict[str, Any]) -> Any:
    parts = [part.strip() for part in str(reference or "").split(".") if part.strip()]
    if not parts:
        raise ValueError("Pipeline variable reference is empty.")

    value: Any = context
    for part in parts:
        if not isinstance(value, dict) or part not in value:
            raise ValueError(f"Pipeline variable not found: {'.'.join(parts)}")
        value = value[part]
    return copy.deepcopy(value)


def _resolve_pipeline_string(value: str, context: dict[str, Any]) -> Any:
    full_match = FULL_PIPELINE_VARIABLE_PATTERN.fullmatch(value)
    if full_match:
        return _resolve_pipeline_reference(full_match.group(1), context)

    def replace(match: re.Match[str]) -> str:
        resolved_value = _resolve_pipeline_reference(match.group(1), context)
        if isinstance(resolved_value, (dict, list)):
            raise ValueError(
                "Pipeline variable must resolve to a scalar for string interpolation: "
                f"{match.group(1)}"
            )
        return str(resolved_value)

    return PIPELINE_VARIABLE_PATTERN.sub(replace, value)


def _resolve_pipeline_value(value: Any, context: dict[str, Any]) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _resolve_pipeline_value(item, context)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_resolve_pipeline_value(item, context) for item in value]
    if isinstance(value, str):
        return _resolve_pipeline_string(value, context)
    return copy.deepcopy(value)


def _pipeline_steps_payload(
    pipeline: Sequence[dict[str, Any]],
) -> list[dict[str, object]]:
    steps_payload: list[dict[str, object]] = []
    for step_index, step in enumerate(pipeline, start=1):
        step_payload: dict[str, object] = {
            "step_index": step_index,
            "tool_name": str(step.get("tool_name") or "").strip(),
        }
        output_key = str(step.get("output_key") or "").strip()
        if output_key:
            step_payload["output_key"] = output_key
        steps_payload.append(step_payload)
    return steps_payload


def _pipeline_artifact_payload(
    pipeline: Sequence[dict[str, Any]],
    pipeline_trace: Sequence[dict[str, object]],
    *,
    final_output: dict[str, Any] | None = None,
    reason: str = "",
    execution_timeline: dict[str, Any] | None = None,
    step_metrics: Sequence[dict[str, object]] | None = None,
    retry_info: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "pipeline_steps": _pipeline_steps_payload(pipeline),
        "pipeline_trace": [
            dict(step_trace)
            for step_trace in pipeline_trace
            if isinstance(step_trace, dict)
        ],
        "final_output": dict(final_output or {}),
    }
    if isinstance(execution_timeline, dict):
        payload["execution_timeline"] = copy.deepcopy(execution_timeline)
    if isinstance(step_metrics, Sequence):
        payload["step_metrics"] = [
            dict(step) for step in step_metrics if isinstance(step, dict)
        ]
    if isinstance(retry_info, dict):
        payload["retry_info"] = dict(retry_info)
    if str(reason or "").strip():
        payload["reason"] = str(reason).strip()
        payload["failure_reason"] = str(reason).strip()
    return payload


def _tool_artifact_payload(
    task: dict[str, Any],
    tool_result: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, object]]:
    tool_name = str(tool_result.get("tool_name") or _tool_name(task)).strip()
    input_payload = _tool_input_payload(task)
    trace = _build_tool_trace(tool_name, input_payload, tool_result)

    if tool_name == GOOGLE_DOCS_CREATE_DOCUMENT_TOOL:
        output_payload = tool_result.get("output")
        if not isinstance(output_payload, dict):
            output_payload = {}

        error_payload = tool_result.get("error")
        if not isinstance(error_payload, dict):
            error_payload = {}

        artifact_payload = _google_doc_artifact_payload(
            task,
            doc_id=str(output_payload.get("doc_id") or ""),
            url=str(output_payload.get("url") or ""),
            reason=str(error_payload.get("message") or ""),
            content=str(input_payload.get("content") or ""),
        )
        artifact_payload.update(trace)
        return artifact_payload, trace

    if tool_name == GOOGLE_DRIVE_READ_FILE_TOOL:
        output_payload = tool_result.get("output")
        if not isinstance(output_payload, dict):
            output_payload = {}

        error_payload = tool_result.get("error")
        if not isinstance(error_payload, dict):
            error_payload = {}

        artifact_payload = {
            "file_id": str(
                output_payload.get("file_id") or input_payload.get("file_id") or ""
            ).strip(),
            "name": str(output_payload.get("name") or "").strip(),
            "mime_type": str(output_payload.get("mime_type") or "").strip(),
            "content_text": str(output_payload.get("content_text") or ""),
        }
        reason = str(error_payload.get("message") or "").strip()
        if reason:
            artifact_payload["reason"] = reason
        artifact_payload.update(trace)
        return artifact_payload, trace

    if tool_name == CLAUDE_ANALYZE_TOOL:
        output_payload = tool_result.get("output")
        if not isinstance(output_payload, dict):
            output_payload = {}

        error_payload = tool_result.get("error")
        if not isinstance(error_payload, dict):
            error_payload = {}

        artifact_payload = {
            "analysis": str(output_payload.get("analysis") or ""),
        }
        reason = str(error_payload.get("message") or "").strip()
        if reason:
            artifact_payload["reason"] = reason
        artifact_payload.update(trace)
        return artifact_payload, trace

    artifact_payload: dict[str, Any] = dict(trace)
    error_payload = tool_result.get("error")
    if isinstance(error_payload, dict):
        artifact_payload["reason"] = str(error_payload.get("message") or "").strip()
    return artifact_payload, trace


def _requested_google_drive_file_ids(task: dict[str, Any]) -> list[str]:
    external_context = task.get("external_context")
    if not isinstance(external_context, dict):
        return []

    raw_value = external_context.get("google_drive")
    raw_items: list[Any]
    if isinstance(raw_value, str):
        raw_items = [raw_value]
    elif isinstance(raw_value, (list, tuple)):
        raw_items = list(raw_value)
    else:
        return []

    source_file_ids: list[str] = []
    seen: set[str] = set()
    for item in raw_items:
        candidate = ""
        if isinstance(item, str):
            candidate = item
        elif isinstance(item, dict):
            candidate = str(item.get("drive_file_id") or item.get("file_id") or "")

        normalized = candidate.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        source_file_ids.append(normalized)

    return source_file_ids


def _drive_to_google_doc_title(task: dict[str, Any]) -> str:
    explicit_title = str(task.get("output_doc_title") or "").strip()
    if explicit_title:
        return explicit_title
    return _google_doc_title(task)


def _drive_to_google_doc_transform_mode(task: dict[str, Any]) -> str:
    return (
        str(task.get("transform_mode") or DEFAULT_TRANSFORM_MODE).strip()
        or DEFAULT_TRANSFORM_MODE
    )


def _load_context_packet(context_packet_path: str | Path) -> dict[str, Any]:
    return json.loads(Path(context_packet_path).read_text(encoding="utf-8"))


def _load_required_system_context(task: dict[str, Any] | None = None) -> dict[str, Any]:
    provided_context = task.get("system_context") if isinstance(task, dict) else None
    if isinstance(provided_context, dict):
        return copy.deepcopy(provided_context)
    return load_required_execution_system_context()


def _trace_memory_policy(decision: dict[str, Any]) -> None:
    if bool(decision.get("allowed", True)):
        print(
            f"[MEMORY_POLICY] {str(decision.get('reason') or 'no_recent_duplicate').strip()}"
        )
        return

    matched_artifact_id = str(decision.get("matched_artifact_id") or "").strip()
    suffix = f" artifact={matched_artifact_id}" if matched_artifact_id else ""
    print(f"[MEMORY_POLICY] blocked_recent_duplicate{suffix}")


def _cross_domain_conflict_resource(task: dict[str, Any]) -> str:
    payload = (
        dict(task.get("payload") or {}) if isinstance(task.get("payload"), dict) else {}
    )
    memory_context = (
        dict(task.get("memory_context") or {})
        if isinstance(task.get("memory_context"), dict)
        else {}
    )
    return (
        str(
            payload.get("resource_id")
            or task.get("resource_id")
            or payload.get("logical_id")
            or task.get("logical_id")
            or payload.get("idempotency_key")
            or task.get("idempotency_key")
            or ""
        ).strip()
        or str(memory_context.get("logical_id") or "").strip()
    )


def _cross_domain_conflict_domain(task: dict[str, Any]) -> str:
    payload = (
        dict(task.get("payload") or {}) if isinstance(task.get("payload"), dict) else {}
    )
    memory_context = (
        dict(task.get("memory_context") or {})
        if isinstance(task.get("memory_context"), dict)
        else {}
    )
    return str(
        payload.get("domain")
        or task.get("domain")
        or memory_context.get("domain")
        or ""
    ).strip()


def _cross_domain_conflict_artifact(
    decision: dict[str, Any],
    resolved_memory: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    conflict_with = str(decision.get("conflict_with") or "").strip()
    if not conflict_with:
        return {}

    for raw_artifact in resolved_memory:
        if not isinstance(raw_artifact, dict):
            continue
        artifact_id = str(raw_artifact.get("id") or "").strip()
        if artifact_id == conflict_with:
            return dict(raw_artifact)
    return {}


def _trace_cross_domain_conflict(
    task: dict[str, Any],
    decision: dict[str, Any],
    resolved_memory: Sequence[dict[str, Any]],
) -> None:
    if bool(decision.get("allowed", True)):
        if str(decision.get("reason") or "").strip() == "previously_resolved_conflict":
            print("[CONFLICT] previously_resolved -> allow")
        return

    conflict_artifact = _cross_domain_conflict_artifact(decision, resolved_memory)
    resource_id = _cross_domain_conflict_resource(task) or "unknown"
    current_domain = _cross_domain_conflict_domain(task) or "unknown"
    other_domain = str(conflict_artifact.get("domain") or "").strip() or "unknown"
    print(
        f"[CONFLICT] blocked resource={resource_id} domain={current_domain} other_domain={other_domain}"
    )


def _cross_domain_conflict_type(task: dict[str, Any]) -> str:
    payload = (
        dict(task.get("payload") or {}) if isinstance(task.get("payload"), dict) else {}
    )
    memory_context = (
        dict(task.get("memory_context") or {})
        if isinstance(task.get("memory_context"), dict)
        else {}
    )
    return str(
        task.get("type")
        or payload.get("type")
        or task.get("task_type")
        or memory_context.get("type")
        or ""
    ).strip()


def _record_conflict_escalation_artifact(
    task: dict[str, Any],
    decision: dict[str, Any],
    resolved_memory: Sequence[dict[str, Any]],
) -> dict[str, str]:
    resource_id = _cross_domain_conflict_resource(task)
    if not resource_id:
        return {}

    conflict_artifact = _cross_domain_conflict_artifact(decision, resolved_memory)
    current_domain = _cross_domain_conflict_domain(task)
    other_domain = str(conflict_artifact.get("domain") or "").strip()
    artifact_id = f"conflict-{resource_id}"
    payload = {
        "id": artifact_id,
        "logical_id": resource_id,
        "resource_id": resource_id,
        "requesting_domain": current_domain,
        "other_domain": other_domain,
        "task_type": _cross_domain_conflict_type(task),
        "conflict_with": str(decision.get("conflict_with") or "").strip(),
    }
    artifact_path = save_artifact(
        "ownerbox",
        "conflict_escalation",
        payload,
        artifact_status="pending_resolution",
        resolution=None,
    )
    set_state(
        "conflict",
        resource_id,
        "conflict_active",
        str(artifact_path),
        domain="ownerbox",
    )
    return {
        "artifact_id": artifact_id,
        "artifact_path": str(artifact_path),
    }


def _memory_policy_blocked_artifact_payload(
    task: dict[str, Any],
    decision: dict[str, Any],
    *,
    execution_timeline: dict[str, Any],
    step_metrics: Sequence[dict[str, object]],
    retry_info: dict[str, Any],
) -> dict[str, Any]:
    return {
        "instruction": str(task["instruction"]),
        "result_type": "memory_policy_blocked",
        "reason": str(decision.get("reason") or "").strip(),
        "action": str(decision.get("action") or "block").strip(),
        "matched_artifact_id": str(decision.get("matched_artifact_id") or "").strip(),
        "memory_policy_decision": copy.deepcopy(decision),
        "execution_timeline": copy.deepcopy(execution_timeline),
        "step_metrics": [dict(step) for step in step_metrics if isinstance(step, dict)],
        "retry_info": dict(retry_info),
    }


def _cross_domain_conflict_blocked_artifact_payload(
    task: dict[str, Any],
    decision: dict[str, Any],
    *,
    escalation_artifact: dict[str, str],
    execution_timeline: dict[str, Any],
    step_metrics: Sequence[dict[str, object]],
    retry_info: dict[str, Any],
) -> dict[str, Any]:
    return {
        "instruction": str(task["instruction"]),
        "result_type": "cross_domain_conflict_blocked",
        "reason": str(decision.get("reason") or "").strip(),
        "action": str(decision.get("action") or "block").strip(),
        "conflict_with": str(decision.get("conflict_with") or "").strip(),
        "escalation_artifact_id": str(
            escalation_artifact.get("artifact_id") or ""
        ).strip(),
        "cross_domain_conflict_decision": copy.deepcopy(decision),
        "execution_timeline": copy.deepcopy(execution_timeline),
        "step_metrics": [dict(step) for step in step_metrics if isinstance(step, dict)],
        "retry_info": dict(retry_info),
    }


def _replay_blocked_artifact_payload(
    task: dict[str, Any],
    decision: dict[str, Any],
    *,
    execution_timeline: dict[str, Any],
    step_metrics: Sequence[dict[str, object]],
    retry_info: dict[str, Any],
) -> dict[str, Any]:
    return {
        "instruction": str(task["instruction"]),
        "result_type": "replay_blocked",
        "reason": str(decision.get("reason") or "").strip(),
        "previous_trace_id": str(decision.get("previous_trace_id") or "").strip(),
        "action": str(decision.get("action") or "block").strip(),
        "replay_protection_decision": copy.deepcopy(decision),
        "execution_timeline": copy.deepcopy(execution_timeline),
        "step_metrics": [dict(step) for step in step_metrics if isinstance(step, dict)],
        "retry_info": dict(retry_info),
    }


def _execution_invariant_reason(decision: dict[str, Any]) -> str:
    violations = decision.get("violations")
    if not isinstance(violations, Sequence) or isinstance(violations, (str, bytes)):
        return ""
    for violation in violations:
        if isinstance(violation, dict):
            reason = str(violation.get("type") or "").strip()
            if reason:
                return reason
    return ""


def _execution_invariants_blocked_artifact_payload(
    task: dict[str, Any],
    decision: dict[str, Any],
    *,
    execution_timeline: dict[str, Any],
    step_metrics: Sequence[dict[str, object]],
    retry_info: dict[str, Any],
) -> dict[str, Any]:
    violations = decision.get("violations")
    if not isinstance(violations, Sequence) or isinstance(violations, (str, bytes)):
        violations = []
    return {
        "instruction": str(task["instruction"]),
        "result_type": "execution_invariants_blocked",
        "reason": _execution_invariant_reason(decision),
        "action": str(decision.get("action") or "block").strip(),
        "violations": [dict(item) for item in violations if isinstance(item, dict)],
        "execution_invariants_decision": copy.deepcopy(decision),
        "execution_timeline": copy.deepcopy(execution_timeline),
        "step_metrics": [dict(step) for step in step_metrics if isinstance(step, dict)],
        "retry_info": dict(retry_info),
    }


def _contextual_tool_call(
    tool_name: str,
    input_payload: dict[str, Any],
    *,
    system_context: dict[str, Any],
) -> dict[str, Any]:
    return execute_tool_call(
        {
            "tool_name": str(tool_name or "").strip(),
            "context": copy.deepcopy(system_context),
            "input": copy.deepcopy(input_payload),
        }
    )


def _drive_to_google_doc_artifact_payload(
    task: dict[str, Any],
    *,
    source_file_ids: Sequence[str],
    loaded_source_file_ids: Sequence[str],
    output_doc_id: str = "",
    output_doc_url: str = "",
    content_summary: str = "",
    reason: str = "",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "source_file_ids": [
            str(item).strip() for item in source_file_ids if str(item).strip()
        ],
        "loaded_source_file_ids": [
            str(item).strip() for item in loaded_source_file_ids if str(item).strip()
        ],
        "output_doc_id": str(output_doc_id or "").strip(),
        "output_doc_title": _drive_to_google_doc_title(task),
        "output_doc_url": str(output_doc_url or "").strip(),
        "content_summary": str(content_summary or "")[:GOOGLE_DOC_SUMMARY_CHARS],
        "transform_mode": _drive_to_google_doc_transform_mode(task),
    }
    if str(reason or "").strip():
        payload["reason"] = str(reason).strip()
    return payload


def _personal_context_sections(task: dict[str, Any]) -> list[str]:
    update_payload = extract_personal_context_update(task)
    return [
        section
        for section in ("owner", "vehicles", "dmv", "immigration", "reminders")
        if section in update_payload
    ]


def _personal_context_artifact_text(
    task: dict[str, object],
    context_path: Path,
) -> str:
    lines = [
        str(task["instruction"]),
        f"PERSONAL_CONTEXT: {context_path}",
    ]
    sections = _personal_context_sections(task)
    if sections:
        lines.append(f"UPDATED_SECTIONS: {', '.join(sections)}")
    return "\n".join(lines)


def _execution_trace_domain(task: dict[str, Any]) -> str:
    normalized_domain = _cross_domain_conflict_domain(task).lower()
    if normalized_domain in {"owner", "ownerbox"}:
        return "ownerbox"
    return "dev"


def _execution_trace_relative_path(task: dict[str, Any]) -> str:
    return f"traces/{str(task.get('task_id') or '').strip()}.json"


def _replay_blocked_trace_relative_path(task: dict[str, Any]) -> str:
    return f"traces/{str(task.get('task_id') or '').strip()}-replay-blocked.json"


def _invariant_blocked_trace_relative_path(task: dict[str, Any]) -> str:
    return f"traces/{str(task.get('task_id') or '').strip()}-invariant-blocked.json"


def _execution_trace_domain_root(task: dict[str, Any], repo_root: Path | str) -> Path:
    base_root = Path(repo_root)
    if _execution_trace_domain(task) == "ownerbox":
        return base_root / "ownerbox" / "artifacts"
    return base_root / "df-dev" / "artifacts"


def _execution_result_artifact_id(
    task: dict[str, Any],
    artifact_path: Path,
    *,
    execution_status: str,
) -> str | None:
    if execution_status != "executed":
        return None
    artifact_id = str(task.get("doc_id") or "").strip()
    if artifact_id:
        return artifact_id
    artifact_name = artifact_path.name.strip()
    return artifact_name or None


def _persist_execution_trace(
    task: dict[str, Any],
    artifact_path: Path,
    *,
    repo_root: Path | str | None,
    execution_status: str,
    final_decision: dict[str, Any],
    trace_relative_path: Path | str | None = None,
) -> Path:
    final_decision_payload = copy.deepcopy(final_decision)
    if "artifact_path" not in final_decision_payload:
        final_decision_payload["artifact_path"] = str(artifact_path)
    if "artifact_id" not in final_decision_payload:
        artifact_id = _execution_result_artifact_id(
            task,
            artifact_path,
            execution_status=execution_status,
        )
        if artifact_id is not None:
            final_decision_payload["artifact_id"] = artifact_id

    trace_payload = create_execution_trace(
        task,
        {
            "resolver": {
                "resolved_memory": copy.deepcopy(
                    task.get("resolved_memory")
                    if isinstance(task.get("resolved_memory"), list)
                    else []
                ),
            },
            "memory_policy": copy.deepcopy(
                task.get("memory_policy_decision")
                if isinstance(task.get("memory_policy_decision"), dict)
                else {}
            ),
            "conflict_gate": copy.deepcopy(
                task.get("cross_domain_conflict_decision")
                if isinstance(task.get("cross_domain_conflict_decision"), dict)
                else {}
            ),
            "replay_protection": copy.deepcopy(
                task.get("replay_protection_decision")
                if isinstance(task.get("replay_protection_decision"), dict)
                else {}
            ),
            "execution_invariants": copy.deepcopy(
                task.get("execution_invariants_decision")
                if isinstance(task.get("execution_invariants_decision"), dict)
                else {}
            ),
            "final_decision": final_decision_payload,
            "execution_status": execution_status,
        },
    )
    trace_domain = _execution_trace_domain(task)
    trace_kwargs = {
        "overwrite": True,
        "relative_path": (
            str(trace_relative_path).strip()
            if trace_relative_path is not None
            else _execution_trace_relative_path(task)
        ),
    }
    try:
        trace_path = save_artifact(
            trace_domain,
            "execution_trace",
            trace_payload,
            **trace_kwargs,
        )
    except (OSError, StorageAdapterError):
        if repo_root is None:
            raise
        trace_path = save_artifact(
            trace_domain,
            "execution_trace",
            trace_payload,
            domain_root_override=_execution_trace_domain_root(task, repo_root),
            **trace_kwargs,
        )
    task["execution_trace"] = copy.deepcopy(trace_payload)
    task["execution_trace_artifact_path"] = str(trace_path)
    print(f"[TRACE] created task={task['task_id']}")
    return trace_path


def _persist_task_state(
    task: dict[str, Any],
    trace_path: Path,
    *,
    state: str,
) -> None:
    task_id = str(task.get("task_id") or "").strip()
    if not task_id:
        return
    set_state(
        "task",
        task_id,
        state,
        str(trace_path),
        domain=_execution_trace_domain(task),
    )


def _trace_replay(decision: dict[str, Any], task: dict[str, Any]) -> None:
    if bool(decision.get("allowed", True)):
        return

    previous_trace_id = (
        str(decision.get("previous_trace_id") or "").strip() or "unknown"
    )
    print(f"[REPLAY] blocked task={task['task_id']} trace={previous_trace_id}")


def run_codex_task(
    task_path: Path | str | dict[str, Any],
    artifact_dir: Path | str | None = None,
    context_output_dir: Path | str | None = None,
    repo_root: Path | str | None = None,
    debug: bool = False,
) -> tuple[dict[str, object], Path]:
    run_start_time = _utc_now_iso()
    run_start_perf = perf_counter()
    task = load_codex_task(
        task_path,
        context_output_dir=context_output_dir,
        repo_root=repo_root,
    )
    task = _attach_resolved_memory(task)
    memory_policy_decision = evaluate_memory_policy(
        task,
        task.get("resolved_memory")
        if isinstance(task.get("resolved_memory"), list)
        else [],
    )
    task["memory_policy_decision"] = copy.deepcopy(memory_policy_decision)
    _trace_memory_policy(memory_policy_decision)

    if not bool(memory_policy_decision.get("allowed", True)):
        step_metric = _step_metric(
            step_index=1,
            tool_name="memory_policy.evaluate",
            step_start_time=run_start_time,
            step_duration_ms=_duration_ms(run_start_perf),
            success=False,
            failure_reason=str(memory_policy_decision.get("reason") or "").strip(),
        )
        step_metric["status"] = "blocked"
        step_metrics = [step_metric]
        execution_timeline = _run_execution_timeline(
            start_time=run_start_time,
            start_perf=run_start_perf,
            step_metrics=step_metrics,
        )
        retry_info = _retry_info(step_metrics)
        task["failure_reason"] = str(memory_policy_decision.get("reason") or "").strip()
        task["execution_timeline"] = copy.deepcopy(execution_timeline)
        task["step_metrics"] = copy.deepcopy(step_metrics)
        task["retry_info"] = copy.deepcopy(retry_info)
        artifact_payload = _memory_policy_blocked_artifact_payload(
            task,
            memory_policy_decision,
            execution_timeline=execution_timeline,
            step_metrics=step_metrics,
            retry_info=retry_info,
        )
        artifact_path = write_task_artifact(
            task,
            artifact_dir=artifact_dir,
            artifact_text=json.dumps(artifact_payload, indent=2, sort_keys=True) + "\n",
        )
        _append_execution_log(
            repo_root=repo_root,
            task=task,
            status="blocked",
            execution_timeline=execution_timeline,
            step_metrics=step_metrics,
            retry_info=retry_info,
            artifact_path=artifact_path,
        )
        _persist_execution_trace(
            task,
            artifact_path,
            repo_root=repo_root,
            execution_status="blocked",
            final_decision={
                "action": "block",
                "gate": "memory_policy",
                "reason": str(memory_policy_decision.get("reason") or "").strip(),
            },
        )
        return task, artifact_path

    cross_domain_conflict_decision = evaluate_cross_domain_conflict(
        task,
        task.get("resolved_memory")
        if isinstance(task.get("resolved_memory"), list)
        else [],
    )
    task["cross_domain_conflict_decision"] = copy.deepcopy(
        cross_domain_conflict_decision
    )
    _trace_cross_domain_conflict(
        task,
        cross_domain_conflict_decision,
        task.get("resolved_memory")
        if isinstance(task.get("resolved_memory"), list)
        else [],
    )

    if not bool(cross_domain_conflict_decision.get("allowed", True)):
        step_metric = _step_metric(
            step_index=1,
            tool_name="cross_domain_conflict.evaluate",
            step_start_time=run_start_time,
            step_duration_ms=_duration_ms(run_start_perf),
            success=False,
            failure_reason=str(
                cross_domain_conflict_decision.get("reason") or ""
            ).strip(),
        )
        step_metric["status"] = "blocked"
        step_metrics = [step_metric]
        execution_timeline = _run_execution_timeline(
            start_time=run_start_time,
            start_perf=run_start_perf,
            step_metrics=step_metrics,
        )
        retry_info = _retry_info(step_metrics)
        task["failure_reason"] = str(
            cross_domain_conflict_decision.get("reason") or ""
        ).strip()
        task["execution_timeline"] = copy.deepcopy(execution_timeline)
        task["step_metrics"] = copy.deepcopy(step_metrics)
        task["retry_info"] = copy.deepcopy(retry_info)
        escalation_artifact = _record_conflict_escalation_artifact(
            task,
            cross_domain_conflict_decision,
            task.get("resolved_memory")
            if isinstance(task.get("resolved_memory"), list)
            else [],
        )
        artifact_payload = _cross_domain_conflict_blocked_artifact_payload(
            task,
            cross_domain_conflict_decision,
            escalation_artifact=escalation_artifact,
            execution_timeline=execution_timeline,
            step_metrics=step_metrics,
            retry_info=retry_info,
        )
        artifact_path = write_task_artifact(
            task,
            artifact_dir=artifact_dir,
            artifact_text=json.dumps(artifact_payload, indent=2, sort_keys=True) + "\n",
        )
        _append_execution_log(
            repo_root=repo_root,
            task=task,
            status="blocked",
            execution_timeline=execution_timeline,
            step_metrics=step_metrics,
            retry_info=retry_info,
            artifact_path=artifact_path,
        )
        _persist_execution_trace(
            task,
            artifact_path,
            repo_root=repo_root,
            execution_status="blocked",
            final_decision={
                "action": "block",
                "gate": "cross_domain_conflict",
                "reason": str(
                    cross_domain_conflict_decision.get("reason") or ""
                ).strip(),
                "conflict_with": str(
                    cross_domain_conflict_decision.get("conflict_with") or ""
                ).strip(),
                "escalation_artifact_id": str(
                    escalation_artifact.get("artifact_id") or ""
                ).strip()
                or None,
            },
        )
        return task, artifact_path

    replay_protection_decision = check_replay(task)
    task["replay_protection_decision"] = copy.deepcopy(replay_protection_decision)
    _trace_replay(replay_protection_decision, task)

    if not bool(replay_protection_decision.get("allowed", True)):
        step_metric = _step_metric(
            step_index=1,
            tool_name="replay_protection.evaluate",
            step_start_time=run_start_time,
            step_duration_ms=_duration_ms(run_start_perf),
            success=False,
            failure_reason=str(replay_protection_decision.get("reason") or "").strip(),
        )
        step_metric["status"] = "blocked"
        step_metrics = [step_metric]
        execution_timeline = _run_execution_timeline(
            start_time=run_start_time,
            start_perf=run_start_perf,
            step_metrics=step_metrics,
        )
        retry_info = _retry_info(step_metrics)
        task["failure_reason"] = str(
            replay_protection_decision.get("reason") or ""
        ).strip()
        task["execution_timeline"] = copy.deepcopy(execution_timeline)
        task["step_metrics"] = copy.deepcopy(step_metrics)
        task["retry_info"] = copy.deepcopy(retry_info)
        artifact_payload = _replay_blocked_artifact_payload(
            task,
            replay_protection_decision,
            execution_timeline=execution_timeline,
            step_metrics=step_metrics,
            retry_info=retry_info,
        )
        artifact_path = write_task_artifact(
            task,
            artifact_dir=artifact_dir,
            artifact_text=json.dumps(artifact_payload, indent=2, sort_keys=True) + "\n",
        )
        _append_execution_log(
            repo_root=repo_root,
            task=task,
            status="blocked",
            execution_timeline=execution_timeline,
            step_metrics=step_metrics,
            retry_info=retry_info,
            artifact_path=artifact_path,
        )
        _persist_execution_trace(
            task,
            artifact_path,
            repo_root=repo_root,
            execution_status="blocked",
            final_decision={
                "action": "block",
                "gate": "replay_protection",
                "reason": str(replay_protection_decision.get("reason") or "").strip(),
                "previous_trace_id": str(
                    replay_protection_decision.get("previous_trace_id") or ""
                ).strip()
                or None,
            },
            trace_relative_path=_replay_blocked_trace_relative_path(task),
        )
        return task, artifact_path

    execution_invariants_decision = check_invariants(task)
    task["execution_invariants_decision"] = copy.deepcopy(execution_invariants_decision)

    if not bool(execution_invariants_decision.get("allowed", True)):
        failure_reason = _execution_invariant_reason(execution_invariants_decision)
        step_metric = _step_metric(
            step_index=1,
            tool_name="execution_invariants.evaluate",
            step_start_time=run_start_time,
            step_duration_ms=_duration_ms(run_start_perf),
            success=False,
            failure_reason=failure_reason,
        )
        step_metric["status"] = "blocked"
        step_metrics = [step_metric]
        execution_timeline = _run_execution_timeline(
            start_time=run_start_time,
            start_perf=run_start_perf,
            step_metrics=step_metrics,
        )
        retry_info = _retry_info(step_metrics)
        task["failure_reason"] = failure_reason
        task["execution_timeline"] = copy.deepcopy(execution_timeline)
        task["step_metrics"] = copy.deepcopy(step_metrics)
        task["retry_info"] = copy.deepcopy(retry_info)
        artifact_payload = _execution_invariants_blocked_artifact_payload(
            task,
            execution_invariants_decision,
            execution_timeline=execution_timeline,
            step_metrics=step_metrics,
            retry_info=retry_info,
        )
        artifact_path = write_task_artifact(
            task,
            artifact_dir=artifact_dir,
            artifact_text=json.dumps(artifact_payload, indent=2, sort_keys=True) + "\n",
        )
        _append_execution_log(
            repo_root=repo_root,
            task=task,
            status="blocked",
            execution_timeline=execution_timeline,
            step_metrics=step_metrics,
            retry_info=retry_info,
            artifact_path=artifact_path,
        )
        _persist_execution_trace(
            task,
            artifact_path,
            repo_root=repo_root,
            execution_status="blocked",
            final_decision={
                "action": "block",
                "gate": "execution_invariants",
                "reason": failure_reason,
                "violations": copy.deepcopy(
                    execution_invariants_decision.get("violations")
                    if isinstance(execution_invariants_decision.get("violations"), list)
                    else []
                ),
            },
            trace_relative_path=_invariant_blocked_trace_relative_path(task),
        )
        return task, artifact_path

    execution_system_context = _load_required_system_context(
        task_path if isinstance(task_path, dict) else None
    )
    if _is_personal_context_task(task):
        update_payload = extract_personal_context_update(task)
        if not update_payload:
            raise ValueError(
                "personal context task requires personal_context_update data"
            )

        personal_context_path = (
            Path(repo_root) / "personal" / "personal_context.json"
            if repo_root is not None
            else DEFAULT_PERSONAL_CONTEXT_PATH
        )
        _, context_path = update_personal_context_file(
            update_payload,
            context_path=personal_context_path,
        )
        step_metrics = [
            _step_metric(
                step_index=1,
                tool_name="personal_context.update",
                step_start_time=run_start_time,
                step_duration_ms=_duration_ms(run_start_perf),
                success=True,
            )
        ]
        execution_timeline = _run_execution_timeline(
            start_time=run_start_time,
            start_perf=run_start_perf,
            step_metrics=step_metrics,
        )
        retry_info = _retry_info(step_metrics)
        task["commit_paths"] = [str(context_path)]
        task["required_validation_paths"] = [str(context_path)]
        task["execution_timeline"] = copy.deepcopy(execution_timeline)
        task["step_metrics"] = copy.deepcopy(step_metrics)
        task["retry_info"] = copy.deepcopy(retry_info)
        artifact_path = write_task_artifact(
            task,
            artifact_dir=artifact_dir,
            artifact_text=_personal_context_artifact_text(task, context_path),
        )
        _append_execution_log(
            repo_root=repo_root,
            task=task,
            status="success",
            execution_timeline=execution_timeline,
            step_metrics=step_metrics,
            retry_info=retry_info,
            artifact_path=artifact_path,
        )
        _persist_execution_trace(
            task,
            artifact_path,
            repo_root=repo_root,
            execution_status="executed",
            final_decision={
                "action": "execute",
                "result_type": PERSONAL_CONTEXT_TASK_TYPE,
                "outcome": "success",
            },
        )
        _persist_task_state(
            task, Path(str(task["execution_trace_artifact_path"])), state="completed"
        )
        return task, artifact_path

    if _is_pipeline_task(task):
        pipeline = copy.deepcopy(task.get("pipeline") or [])
        pipeline_context: dict[str, Any] = {}
        pipeline_trace: list[dict[str, object]] = []
        final_output: dict[str, Any] = {}
        failure_reason = ""

        for step_index, raw_step in enumerate(pipeline, start=1):
            step_start_time = _utc_now_iso()
            step_start_perf = perf_counter()
            tool_name = ""
            resolved_input: dict[str, Any] = {}
            try:
                if not isinstance(raw_step, dict):
                    raise ValueError(f"Pipeline step {step_index} must be an object.")

                tool_name = str(raw_step.get("tool_name") or "").strip()
                resolved_value = _resolve_pipeline_value(
                    raw_step.get("input"), pipeline_context
                )
                if not isinstance(resolved_value, dict):
                    raise ValueError(
                        f"Pipeline step {step_index} input must resolve to an object."
                    )
                resolved_input = resolved_value

                _debug_print(
                    debug,
                    f"pipeline step {step_index} start tool={tool_name} retries=0",
                )

                if (
                    tool_name == CLAUDE_ANALYZE_TOOL
                    and not str(resolved_input.get("text") or "").strip()
                ):
                    failure_reason = "EMPTY_INPUT"
                    step_trace = _decorate_trace_with_observability(
                        _empty_input_step_trace(
                            step_index,
                            tool_name,
                            resolved_input,
                        ),
                        step_start_time=step_start_time,
                        step_duration_ms=_duration_ms(step_start_perf),
                        success=False,
                        retry_count=0,
                        failure_reason=failure_reason,
                    )
                    pipeline_trace.append(step_trace)
                    _debug_print(
                        debug,
                        (
                            f"pipeline step {step_index} finish tool={tool_name} "
                            f"status=failure duration_ms={step_trace['step_duration_ms']} retries=0 "
                            f"reason={failure_reason}"
                        ),
                    )
                    break

                tool_result = _contextual_tool_call(
                    tool_name,
                    resolved_input,
                    system_context=execution_system_context,
                )
                retry_count = _tool_retry_count(tool_result)
                step_success = bool(tool_result.get("ok"))
                step_failure_reason = _tool_failure_reason(tool_result)
                step_trace = _decorate_trace_with_observability(
                    _pipeline_trace_entry(
                        step_index,
                        tool_name,
                        resolved_input,
                        tool_result,
                    ),
                    step_start_time=step_start_time,
                    step_duration_ms=_duration_ms(step_start_perf),
                    success=step_success,
                    retry_count=retry_count,
                    failure_reason=step_failure_reason,
                    tool_result=tool_result,
                )
                pipeline_trace.append(step_trace)
                _debug_print(
                    debug,
                    (
                        f"pipeline step {step_index} finish tool={tool_name} "
                        f"status={'success' if step_success else 'failure'} "
                        f"duration_ms={step_trace['step_duration_ms']} retries={retry_count}"
                    ),
                )
                for tool_step in step_trace.get("tool_step_metrics", []):
                    if not isinstance(tool_step, dict):
                        continue
                    _debug_print(
                        debug,
                        (
                            f"  tool step {tool_step.get('step_name')} "
                            f"status={tool_step.get('status')} "
                            f"duration_ms={tool_step.get('step_duration_ms')} "
                            f"retries={tool_step.get('retry_count')}"
                        ),
                    )

                if not step_success:
                    failure_reason = (
                        step_failure_reason or f"Pipeline step {step_index} failed."
                    )
                    break

                output_payload = tool_result.get("output")
                if not isinstance(output_payload, dict):
                    raise ValueError(
                        f"Pipeline step {step_index} returned invalid output."
                    )

                output_key = str(raw_step.get("output_key") or "").strip()
                if output_key:
                    pipeline_context[output_key] = copy.deepcopy(output_payload)
                final_output = copy.deepcopy(output_payload)
            except Exception as error:
                failure_reason = str(error).strip() or "Pipeline execution failed."
                pipeline_trace.append(
                    _decorate_trace_with_observability(
                        {
                            "step_index": int(step_index),
                            "tool_name": tool_name,
                            "tool_ok": False,
                            "tool_error_code": "PIPELINE_STEP_FAILED",
                            "tool_input_summary": _tool_input_summary(
                                tool_name, resolved_input
                            ),
                        },
                        step_start_time=step_start_time,
                        step_duration_ms=_duration_ms(step_start_perf),
                        success=False,
                        retry_count=0,
                        failure_reason=failure_reason,
                    )
                )
                _debug_print(
                    debug,
                    (
                        f"pipeline step {step_index} finish tool={tool_name or '<unresolved>'} "
                        f"status=failure duration_ms={pipeline_trace[-1]['step_duration_ms']} "
                        f"retries=0 reason={failure_reason}"
                    ),
                )
                break

        if failure_reason:
            final_output = {}
            task["failure_reason"] = failure_reason

        step_metrics = _step_metrics_from_trace(pipeline_trace)
        execution_timeline = _run_execution_timeline(
            start_time=run_start_time,
            start_perf=run_start_perf,
            step_metrics=step_metrics,
        )
        retry_info = _retry_info(step_metrics)
        task["pipeline_trace"] = copy.deepcopy(pipeline_trace)
        task["execution_timeline"] = copy.deepcopy(execution_timeline)
        task["step_metrics"] = copy.deepcopy(step_metrics)
        task["retry_info"] = copy.deepcopy(retry_info)
        if pipeline_trace:
            task["tool_trace"] = dict(pipeline_trace[-1])

        if not failure_reason and final_output:
            task["doc_id"] = str(final_output.get("doc_id") or "").strip()
            task["doc_url"] = str(final_output.get("url") or "").strip()

        artifact_payload = _pipeline_artifact_payload(
            pipeline,
            pipeline_trace,
            final_output=final_output,
            reason=failure_reason,
            execution_timeline=execution_timeline,
            step_metrics=step_metrics,
            retry_info=retry_info,
        )
        artifact_path = write_task_artifact(
            task,
            artifact_dir=artifact_dir,
            artifact_text=json.dumps(artifact_payload, indent=2, sort_keys=True) + "\n",
        )
        log_path = _append_execution_log(
            repo_root=repo_root,
            task=task,
            status="failed" if failure_reason else "success",
            execution_timeline=execution_timeline,
            step_metrics=step_metrics,
            retry_info=retry_info,
            artifact_path=artifact_path,
        )
        _debug_print(debug, f"execution log appended path={log_path}")
        _persist_execution_trace(
            task,
            artifact_path,
            repo_root=repo_root,
            execution_status="executed",
            final_decision={
                "action": "execute",
                "result_type": "pipeline",
                "outcome": "failed" if failure_reason else "success",
                "reason": failure_reason,
            },
        )
        _persist_task_state(
            task,
            Path(str(task["execution_trace_artifact_path"])),
            state="failed" if failure_reason else "completed",
        )
        return task, artifact_path

    if _is_tool_call_task(task):
        step_start_time = _utc_now_iso()
        step_start_perf = perf_counter()
        _debug_print(debug, f"tool task start tool={_tool_name(task)} retries=0")
        tool_call_payload = dict(task.get("tool_call") or {})
        tool_result = _contextual_tool_call(
            str(tool_call_payload.get("tool_name") or "").strip(),
            _tool_input_payload(task),
            system_context=execution_system_context,
        )
        artifact_payload, tool_trace = _tool_artifact_payload(task, tool_result)
        step_metrics = [
            _step_metric(
                step_index=1,
                tool_name=str(tool_trace.get("tool_name") or _tool_name(task)).strip(),
                step_start_time=step_start_time,
                step_duration_ms=_duration_ms(step_start_perf),
                success=bool(tool_result.get("ok")),
                retry_count=_tool_retry_count(tool_result),
                failure_reason=_tool_failure_reason(tool_result),
            )
        ]
        execution_timeline = _run_execution_timeline(
            start_time=run_start_time,
            start_perf=run_start_perf,
            step_metrics=step_metrics,
        )
        retry_info = _retry_info(step_metrics)
        task["tool_trace"] = tool_trace
        task["execution_timeline"] = copy.deepcopy(execution_timeline)
        task["step_metrics"] = copy.deepcopy(step_metrics)
        task["retry_info"] = copy.deepcopy(retry_info)
        artifact_payload["execution_timeline"] = copy.deepcopy(execution_timeline)
        artifact_payload["step_metrics"] = copy.deepcopy(step_metrics)
        artifact_payload["retry_info"] = copy.deepcopy(retry_info)

        if (
            bool(tool_result.get("ok"))
            and _tool_name(task) == GOOGLE_DOCS_CREATE_DOCUMENT_TOOL
        ):
            output_payload = tool_result.get("output")
            if isinstance(output_payload, dict):
                task["doc_id"] = str(output_payload.get("doc_id") or "").strip()
                task["doc_url"] = str(output_payload.get("url") or "").strip()

        artifact_path = write_task_artifact(
            task,
            artifact_dir=artifact_dir,
            artifact_text=json.dumps(artifact_payload, indent=2, sort_keys=True) + "\n",
        )
        log_path = _append_execution_log(
            repo_root=repo_root,
            task=task,
            status="success" if bool(tool_result.get("ok")) else "failed",
            execution_timeline=execution_timeline,
            step_metrics=step_metrics,
            retry_info=retry_info,
            artifact_path=artifact_path,
        )
        _debug_print(
            debug,
            (
                f"tool task finish tool={tool_trace.get('tool_name')} "
                f"status={'success' if bool(tool_result.get('ok')) else 'failure'} "
                f"duration_ms={step_metrics[0]['step_duration_ms']} "
                f"retries={step_metrics[0]['retry_count']}"
            ),
        )
        for tool_step in tool_trace.get("step_metrics", []):
            if not isinstance(tool_step, dict):
                continue
            _debug_print(
                debug,
                (
                    f"  tool step {tool_step.get('step_name')} "
                    f"status={tool_step.get('status')} "
                    f"duration_ms={tool_step.get('step_duration_ms')} "
                    f"retries={tool_step.get('retry_count')}"
                ),
            )
        _debug_print(debug, f"execution log appended path={log_path}")
        _persist_execution_trace(
            task,
            artifact_path,
            repo_root=repo_root,
            execution_status="executed",
            final_decision={
                "action": "execute",
                "result_type": "tool_call",
                "tool_name": str(
                    tool_trace.get("tool_name") or _tool_name(task)
                ).strip(),
                "outcome": "success" if bool(tool_result.get("ok")) else "failed",
                "reason": _tool_failure_reason(tool_result),
            },
        )
        _persist_task_state(
            task,
            Path(str(task["execution_trace_artifact_path"])),
            state="failed" if not bool(tool_result.get("ok")) else "completed",
        )
        return task, artifact_path

    if _is_google_doc_write_task(task):
        step_start_time = _utc_now_iso()
        step_start_perf = perf_counter()
        success = False
        failure_reason = ""
        try:
            tool_result = _contextual_tool_call(
                GOOGLE_DOCS_CREATE_DOCUMENT_TOOL,
                {
                    "title": _google_doc_title(task),
                    "content": _google_doc_content(task),
                },
                system_context=execution_system_context,
            )
            if not bool(tool_result.get("ok")):
                raise RuntimeError(
                    _tool_failure_reason(tool_result) or "GOOGLE_DOCS_CREATE_FAILED"
                )
            result = dict(tool_result.get("output") or {})
            task["doc_id"] = result["doc_id"]
            task["doc_url"] = result["url"]
            artifact_payload = _google_doc_artifact_payload(
                task,
                doc_id=result["doc_id"],
                url=result["url"],
            )
            success = True
        except Exception as error:
            failure_reason = str(error).strip()
            artifact_payload = _google_doc_artifact_payload(task, reason=failure_reason)

        step_metrics = [
            _step_metric(
                step_index=1,
                tool_name=GOOGLE_DOCS_CREATE_DOCUMENT_TOOL,
                step_start_time=step_start_time,
                step_duration_ms=_duration_ms(step_start_perf),
                success=success,
                retry_count=0,
                failure_reason=failure_reason,
            )
        ]
        execution_timeline = _run_execution_timeline(
            start_time=run_start_time,
            start_perf=run_start_perf,
            step_metrics=step_metrics,
        )
        retry_info = _retry_info(step_metrics)
        task["execution_timeline"] = copy.deepcopy(execution_timeline)
        task["step_metrics"] = copy.deepcopy(step_metrics)
        task["retry_info"] = copy.deepcopy(retry_info)
        artifact_payload["execution_timeline"] = copy.deepcopy(execution_timeline)
        artifact_payload["step_metrics"] = copy.deepcopy(step_metrics)
        artifact_payload["retry_info"] = copy.deepcopy(retry_info)

        artifact_path = write_task_artifact(
            task,
            artifact_dir=artifact_dir,
            artifact_text=json.dumps(artifact_payload, indent=2, sort_keys=True) + "\n",
        )
        _append_execution_log(
            repo_root=repo_root,
            task=task,
            status="success" if success else "failed",
            execution_timeline=execution_timeline,
            step_metrics=step_metrics,
            retry_info=retry_info,
            artifact_path=artifact_path,
        )
        _persist_execution_trace(
            task,
            artifact_path,
            repo_root=repo_root,
            execution_status="executed",
            final_decision={
                "action": "execute",
                "result_type": EXTERNAL_WRITE_GOOGLE_DOC_TASK_TYPE,
                "outcome": "success" if success else "failed",
                "reason": failure_reason,
            },
        )
        _persist_task_state(
            task,
            Path(str(task["execution_trace_artifact_path"])),
            state="failed" if not success else "completed",
        )
        return task, artifact_path

    if _is_drive_to_google_doc_task(task):
        requested_source_file_ids = _requested_google_drive_file_ids(task)
        step_start_time = _utc_now_iso()
        step_start_perf = perf_counter()
        success = False
        failure_reason = ""
        try:
            context_packet = _load_context_packet(str(task["context_packet_path"]))
            transformed_content = build_drive_to_google_doc_content(
                context_packet,
                output_doc_title=_drive_to_google_doc_title(task),
                transform_mode=_drive_to_google_doc_transform_mode(task),
                generated_at=utc_now(),
            )
            tool_result = _contextual_tool_call(
                GOOGLE_DOCS_CREATE_DOCUMENT_TOOL,
                {
                    "title": _drive_to_google_doc_title(task),
                    "content": str(transformed_content["content"]),
                },
                system_context=execution_system_context,
            )
            if not bool(tool_result.get("ok")):
                raise RuntimeError(
                    _tool_failure_reason(tool_result) or "GOOGLE_DOCS_CREATE_FAILED"
                )
            result = dict(tool_result.get("output") or {})
            task["doc_id"] = result["doc_id"]
            task["doc_url"] = result["url"]
            artifact_payload = _drive_to_google_doc_artifact_payload(
                task,
                source_file_ids=requested_source_file_ids
                or transformed_content["source_file_ids"],
                loaded_source_file_ids=transformed_content["source_file_ids"],
                output_doc_id=result["doc_id"],
                output_doc_url=result["url"],
                content_summary=str(transformed_content["content_summary"]),
            )
            success = True
        except Exception as error:
            failure_reason = str(error).strip()
            artifact_payload = _drive_to_google_doc_artifact_payload(
                task,
                source_file_ids=requested_source_file_ids,
                loaded_source_file_ids=[],
                reason=failure_reason,
            )

        step_metrics = [
            _step_metric(
                step_index=1,
                tool_name="drive_to_google_doc",
                step_start_time=step_start_time,
                step_duration_ms=_duration_ms(step_start_perf),
                success=success,
                retry_count=0,
                failure_reason=failure_reason,
            )
        ]
        execution_timeline = _run_execution_timeline(
            start_time=run_start_time,
            start_perf=run_start_perf,
            step_metrics=step_metrics,
        )
        retry_info = _retry_info(step_metrics)
        task["execution_timeline"] = copy.deepcopy(execution_timeline)
        task["step_metrics"] = copy.deepcopy(step_metrics)
        task["retry_info"] = copy.deepcopy(retry_info)
        artifact_payload["execution_timeline"] = copy.deepcopy(execution_timeline)
        artifact_payload["step_metrics"] = copy.deepcopy(step_metrics)
        artifact_payload["retry_info"] = copy.deepcopy(retry_info)

        artifact_path = write_task_artifact(
            task,
            artifact_dir=artifact_dir,
            artifact_text=json.dumps(artifact_payload, indent=2, sort_keys=True) + "\n",
        )
        _append_execution_log(
            repo_root=repo_root,
            task=task,
            status="success" if success else "failed",
            execution_timeline=execution_timeline,
            step_metrics=step_metrics,
            retry_info=retry_info,
            artifact_path=artifact_path,
        )
        _persist_execution_trace(
            task,
            artifact_path,
            repo_root=repo_root,
            execution_status="executed",
            final_decision={
                "action": "execute",
                "result_type": DRIVE_TO_GOOGLE_DOC_TASK_TYPE,
                "outcome": "success" if success else "failed",
                "reason": failure_reason,
            },
        )
        _persist_task_state(
            task,
            Path(str(task["execution_trace_artifact_path"])),
            state="failed" if not success else "completed",
        )
        return task, artifact_path

    step_metrics = [
        _step_metric(
            step_index=1,
            tool_name="local.artifact_write",
            step_start_time=run_start_time,
            step_duration_ms=_duration_ms(run_start_perf),
            success=True,
        )
    ]
    execution_timeline = _run_execution_timeline(
        start_time=run_start_time,
        start_perf=run_start_perf,
        step_metrics=step_metrics,
    )
    retry_info = _retry_info(step_metrics)
    task["execution_timeline"] = copy.deepcopy(execution_timeline)
    task["step_metrics"] = copy.deepcopy(step_metrics)
    task["retry_info"] = copy.deepcopy(retry_info)
    artifact_path = write_task_artifact(task, artifact_dir=artifact_dir)
    _append_execution_log(
        repo_root=repo_root,
        task=task,
        status="success",
        execution_timeline=execution_timeline,
        step_metrics=step_metrics,
        retry_info=retry_info,
        artifact_path=artifact_path,
    )
    _persist_execution_trace(
        task,
        artifact_path,
        repo_root=repo_root,
        execution_status="executed",
        final_decision={
            "action": "execute",
            "result_type": "local_artifact_write",
            "outcome": "success",
        },
    )
    _persist_task_state(
        task, Path(str(task["execution_trace_artifact_path"])), state="completed"
    )
    return task, artifact_path


def _run_git_command(args: list[str]) -> str:
    result = run_in_dev_env(
        ["git", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        message = result.stderr.strip() or result.stdout.strip() or "git command failed"
        raise RuntimeError(message)
    return result.stdout.strip() or result.stderr.strip()


def _git_relative_path(path: Path) -> str:
    return path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()


def _commit_message_for_task(
    task_id: int,
    *,
    subtask_id: str | None = None,
    final_commit: bool = False,
) -> str:
    if subtask_id is None or final_commit:
        if subtask_id is None:
            return f"DF task {task_id}: execution result"
        return f"DF task {task_id}: execution result ({subtask_id})"
    return f"DF task {task_id} subtask {subtask_id}: execution result"


def commit_and_push_artifact(
    task_id: int,
    artifact_path: Path,
    *,
    subtask_id: str | None = None,
    additional_paths: Sequence[Path | str] | None = None,
    final_commit: bool = False,
) -> tuple[str, str]:
    git_paths = [_git_relative_path(artifact_path)]
    for extra_path in additional_paths or ():
        git_paths.append(_git_relative_path(Path(extra_path)))

    commit_message = _commit_message_for_task(
        task_id,
        subtask_id=subtask_id,
        final_commit=final_commit,
    )

    _run_git_command(["add", "--", *git_paths])
    _run_git_command(["commit", "-m", commit_message, "--", *git_paths])
    branch_name = _run_git_command(["rev-parse", "--abbrev-ref", "HEAD"])
    commit_hash = _run_git_command(["rev-parse", "HEAD"])
    _run_git_command(["push"])
    return branch_name, commit_hash


def _is_truthy_env(name: str) -> bool:
    return str(os.environ.get(name, "")).strip().lower() in {"1", "true", "yes", "on"}


def _dedupe_paths(
    artifact_path: Path,
    additional_paths: Sequence[Path | str] | None = None,
) -> list[Path]:
    ordered_paths: list[Path] = []
    seen: set[Path] = set()

    for candidate in (artifact_path, *(additional_paths or ())):
        path = Path(candidate).resolve()
        if path in seen:
            continue
        seen.add(path)
        ordered_paths.append(path)

    return ordered_paths


def best_effort_post_execution_git_sync(
    task_id: int,
    artifact_path: Path,
    *,
    subtask_id: str | None = None,
    additional_paths: Sequence[Path | str] | None = None,
) -> dict[str, object]:
    if not _is_truthy_env(POST_EXECUTION_GIT_SYNC_ENV):
        return {
            "attempted": False,
            "succeeded": False,
            "branch_name": None,
            "commit_hash": None,
            "reason": "",
        }

    sync_paths = _dedupe_paths(artifact_path, additional_paths)
    try:
        branch_name, commit_hash = commit_and_push_artifact(
            task_id=task_id,
            artifact_path=sync_paths[0],
            subtask_id=subtask_id,
            additional_paths=sync_paths[1:],
            final_commit=True,
        )
    except Exception as error:
        return {
            "attempted": True,
            "succeeded": False,
            "branch_name": None,
            "commit_hash": None,
            "reason": str(error),
        }

    return {
        "attempted": True,
        "succeeded": True,
        "branch_name": branch_name,
        "commit_hash": commit_hash,
        "reason": "",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a Codex task JSON by writing deterministic local artifacts.",
    )
    parser.add_argument(
        "task_path",
        help="Path to the Codex task JSON file.",
    )
    parser.add_argument(
        "--artifact-dir",
        default=None,
        help="Optional output directory for the generated task artifact.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print step-by-step execution timing and retry diagnostics.",
    )
    return parser.parse_args()


def _print_execution_summary(
    task: dict[str, object],
    artifact_path: Path,
    validation: dict[str, object],
    *,
    branch_name: str | None = None,
    commit_hash: str | None = None,
    subtasks_path: Path | None = None,
    comment_id: int | None = None,
    issue_updated: bool = False,
) -> None:
    tool_trace = task.get("tool_trace")
    print(f"TASK_ID: {task['task_id']}")
    if task.get("subtask_id") is not None:
        print(f"SUBTASK_ID: {task['subtask_id']}")
    print(f"INSTRUCTION: {task['instruction']}")
    print(f"CONTEXT_PACKET: {task['context_packet_path']}")
    print(f"ARTIFACT_WRITTEN: {artifact_path}")
    print(f"VALIDATION_VALID: {validation['valid']}")
    print(f"VALIDATION_REASON: {validation['reason']}")
    if isinstance(tool_trace, dict):
        tool_name = str(tool_trace.get("tool_name") or "").strip()
        if tool_name:
            print(f"TOOL_NAME: {tool_name}")
        if tool_trace.get("tool_ok") is not None:
            print(f"TOOL_OK: {bool(tool_trace['tool_ok'])}")
        tool_error_code = str(tool_trace.get("tool_error_code") or "").strip()
        if tool_error_code:
            print(f"TOOL_ERROR_CODE: {tool_error_code}")
    pipeline_trace = task.get("pipeline_trace")
    if isinstance(pipeline_trace, list):
        print(f"PIPELINE_STEPS: {len(pipeline_trace)}")
    if subtasks_path is not None:
        print(f"SUBTASKS_FILE: {subtasks_path}")
    if branch_name is not None:
        print(f"BRANCH_NAME: {branch_name}")
    if commit_hash is not None:
        print(f"COMMIT_HASH: {commit_hash}")
    if issue_updated:
        print("issue updated")
        if comment_id is not None:
            print(f"comment id: {comment_id}")
        else:
            print("comment id: unavailable")


def _print_git_sync_summary(result: dict[str, object]) -> None:
    if not bool(result.get("attempted")):
        return

    if bool(result.get("succeeded")):
        print("GIT_SYNC_STATUS: completed")
        branch_name = str(result.get("branch_name") or "").strip()
        commit_hash = str(result.get("commit_hash") or "").strip()
        if branch_name:
            print(f"GIT_SYNC_BRANCH: {branch_name}")
        if commit_hash:
            print(f"GIT_SYNC_COMMIT: {commit_hash}")
        return

    print("GIT_SYNC_STATUS: failed")
    print(f"GIT_SYNC_REASON: {str(result.get('reason') or 'git sync failed').strip()}")


def _log_metrics(
    task_id: int,
    status: str,
    started_at,
    start_perf: float,
    instruction_text: str,
    validation_passed: bool,
    validation_reason: str,
    commit_hash: str | None,
    artifact_path: Path | None,
    *,
    subtask_id: str | None = None,
    parent_task_id: int | None = None,
    force_execution_used: bool = False,
    force_execution_reason: str | None = None,
    prior_artifact_path: Path | None = None,
    tool_trace: dict[str, Any] | None = None,
    pipeline_trace: list[dict[str, object]] | None = None,
) -> Path:
    _, metrics_path = log_task_run_metrics(
        task_id=task_id,
        status=status,
        started_at=started_at,
        finished_at=utc_now(),
        duration_ms=max(0, int((perf_counter() - start_perf) * 1000)),
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
        tool_name=(
            str(tool_trace.get("tool_name") or "").strip()
            if isinstance(tool_trace, dict)
            else None
        ),
        tool_ok=tool_trace.get("tool_ok") if isinstance(tool_trace, dict) else None,
        tool_input_summary=(
            dict(tool_trace["tool_input_summary"])
            if isinstance(tool_trace, dict)
            and isinstance(tool_trace.get("tool_input_summary"), dict)
            else None
        ),
        tool_output_summary=(
            dict(tool_trace["tool_output_summary"])
            if isinstance(tool_trace, dict)
            and isinstance(tool_trace.get("tool_output_summary"), dict)
            else None
        ),
        tool_error_code=(
            str(tool_trace.get("tool_error_code") or "").strip()
            if isinstance(tool_trace, dict)
            else None
        ),
        pipeline_trace=(
            [dict(step_trace) for step_trace in pipeline_trace]
            if isinstance(pipeline_trace, list)
            else None
        ),
    )
    print(f"METRICS_WRITTEN: {metrics_path}")
    return metrics_path


def _build_subtask_task(
    base_task: dict[str, Any],
    subtask: dict[str, Any],
) -> dict[str, Any]:
    task = {
        "task_id": int(base_task["task_id"]),
        "parent_task_id": int(base_task["task_id"]),
        "subtask_id": str(subtask["subtask_id"]),
        "subtask_type": str(subtask["type"]),
        "priority": int(subtask["priority"]),
        "instruction": str(subtask["instruction"]),
        "constraints": str(base_task["constraints"]),
        "success_criteria": str(base_task["success_criteria"]),
    }
    if base_task.get("task_type") is not None:
        task["task_type"] = str(base_task["task_type"])
    if base_task.get("personal_context_update") is not None:
        task["personal_context_update"] = copy.deepcopy(
            base_task["personal_context_update"]
        )
    if base_task.get("file_paths") is not None:
        task["file_paths"] = copy.deepcopy(base_task["file_paths"])
    if base_task.get("external_context") is not None:
        task["external_context"] = copy.deepcopy(base_task["external_context"])
    if base_task.get("memory_context") is not None:
        task["memory_context"] = copy.deepcopy(base_task["memory_context"])
    if base_task.get("title") is not None:
        task["title"] = str(base_task["title"])
    if base_task.get("document_title") is not None:
        task["document_title"] = str(base_task["document_title"])
    if base_task.get("output_doc_title") is not None:
        task["output_doc_title"] = str(base_task["output_doc_title"])
    if base_task.get("transform_mode") is not None:
        task["transform_mode"] = str(base_task["transform_mode"])
    if base_task.get("content") is not None:
        task["content"] = str(base_task["content"])
    if base_task.get("force_execution") is not None:
        task["force_execution"] = bool(base_task["force_execution"])
    if base_task.get("tool_call") is not None:
        task["tool_call"] = copy.deepcopy(base_task["tool_call"])
    if isinstance(base_task.get("pipeline"), list):
        task["pipeline"] = copy.deepcopy(base_task["pipeline"])
    return task


def _build_subtask_progress(
    base_task: dict[str, Any],
    decomposition: dict[str, Any],
) -> dict[str, Any]:
    return {
        "parent_task_id": int(base_task["task_id"]),
        "original_instruction": str(base_task["instruction"]),
        "subtasks": [
            {
                **dict(subtask),
                "status": "PENDING",
                "artifact_path": None,
                "context_packet_path": None,
                "validation": None,
            }
            for subtask in decomposition["subtasks"]
        ],
    }


def _force_execution_requested(task: dict[str, Any]) -> bool:
    return task.get("force_execution") is True


def _record_force_execution_trace(
    progress_record: dict[str, Any],
    *,
    reason: str,
    prior_artifact_path: Path | None = None,
) -> None:
    progress_record["force_execution_used"] = True
    progress_record["force_execution_reason"] = str(reason)
    if prior_artifact_path is not None:
        progress_record["prior_artifact_path"] = str(prior_artifact_path)


def _update_subtask_progress(
    progress_record: dict[str, Any],
    task: dict[str, object],
    artifact_path: Path,
    validation: dict[str, object],
    *,
    status: str,
) -> None:
    subtask_id = str(task["subtask_id"])
    for subtask in progress_record["subtasks"]:
        if subtask["subtask_id"] != subtask_id:
            continue
        subtask["status"] = status
        subtask["artifact_path"] = str(artifact_path)
        subtask["context_packet_path"] = str(task["context_packet_path"])
        subtask["validation"] = {
            "valid": bool(validation["valid"]),
            "reason": str(validation["reason"]),
        }
        return


def main() -> int:
    args = parse_args()
    debug_enabled = bool(getattr(args, "debug", False))
    base_task = _load_task_source(args.task_path)
    task_id = int(base_task["task_id"])
    decomposition = decompose_task(base_task)
    final_subtask = dict(decomposition["subtasks"][-1])

    started_at = utc_now()
    start_perf = perf_counter()
    guard_artifact_path = artifact_path_for_task(
        task_id=task_id,
        artifact_dir=args.artifact_dir,
        subtask_id=str(final_subtask["subtask_id"]),
        task_type=str(base_task.get("task_type") or ""),
        tool_name=(
            str(base_task.get("tool_call", {}).get("tool_name") or "").strip()
            if isinstance(base_task.get("tool_call"), dict)
            else None
        ),
        has_pipeline=isinstance(base_task.get("pipeline"), list),
    )
    force_execution_requested = _force_execution_requested(base_task)
    prior_artifact_path = (
        guard_artifact_path
        if force_execution_requested and guard_artifact_path.exists()
        else None
    )
    guard = should_execute_task(
        task_id=task_id,
        artifact_path=guard_artifact_path,
        allow_existing_artifact=force_execution_requested,
    )
    force_execution_used = bool(guard.get("force_execution_used"))
    force_execution_reason = (
        str(guard.get("force_execution_reason") or "").strip() or None
    )
    if (
        not force_execution_used
        and prior_artifact_path is not None
        and bool(guard["should_execute"])
    ):
        force_execution_used = True
        force_execution_reason = "artifact_override"
    if not bool(guard["should_execute"]):
        print(f"execution skipped: {guard['reason']}")
        _log_metrics(
            task_id=task_id,
            status="SKIPPED",
            started_at=started_at,
            start_perf=start_perf,
            instruction_text=str(base_task["instruction"]),
            validation_passed=False,
            validation_reason=str(guard["reason"]),
            commit_hash=None,
            artifact_path=None,
            parent_task_id=task_id,
            force_execution_used=force_execution_used,
            force_execution_reason=force_execution_reason,
            prior_artifact_path=prior_artifact_path,
        )
        return 0

    progress_record = _build_subtask_progress(base_task, decomposition)
    if force_execution_used:
        print("FORCE_EXECUTION_USED: True")
        if force_execution_reason is not None:
            print(f"FORCE_EXECUTION_REASON: {force_execution_reason}")
        if prior_artifact_path is not None:
            print(f"PRIOR_ARTIFACT_PATH: {prior_artifact_path}")
        _record_force_execution_trace(
            progress_record,
            reason=str(force_execution_reason or ""),
            prior_artifact_path=prior_artifact_path,
        )
    progress_path = write_subtask_record(progress_record)

    final_task: dict[str, object] | None = None
    final_artifact_path: Path | None = None
    git_sync_paths: list[Path] = []

    for subtask in decomposition["subtasks"]:
        subtask_started_at = utc_now()
        subtask_start_perf = perf_counter()
        subtask_task = _build_subtask_task(base_task, dict(subtask))
        task, artifact_path = run_codex_task(
            subtask_task,
            artifact_dir=args.artifact_dir,
            repo_root=REPO_ROOT,
            debug=debug_enabled,
        )
        validation = validate_task_result(
            artifact_path=artifact_path,
            task_instruction=str(task["instruction"]),
            required_paths=task.get("required_validation_paths"),
        )

        status = "DONE" if bool(validation["valid"]) else "FAILED"
        _update_subtask_progress(
            progress_record,
            task,
            artifact_path,
            validation,
            status=status,
        )
        progress_path = write_subtask_record(progress_record)

        if not bool(validation["valid"]):
            _, comment_id = mark_issue_validation_failed(
                issue_number=task_id,
                reason=f"subtask {task['subtask_id']} failed validation: {validation['reason']}",
            )
            _print_execution_summary(
                task=task,
                artifact_path=artifact_path,
                validation=validation,
                subtasks_path=progress_path,
                comment_id=comment_id,
                issue_updated=True,
            )
            _log_metrics(
                task_id=task_id,
                status="FAILED",
                started_at=subtask_started_at,
                start_perf=subtask_start_perf,
                instruction_text=str(task["instruction"]),
                validation_passed=bool(validation["valid"]),
                validation_reason=str(validation["reason"]),
                commit_hash=None,
                artifact_path=artifact_path,
                subtask_id=str(task["subtask_id"]),
                parent_task_id=task_id,
                force_execution_used=force_execution_used,
                force_execution_reason=force_execution_reason,
                prior_artifact_path=prior_artifact_path,
                tool_trace=(
                    task.get("tool_trace")
                    if isinstance(task.get("tool_trace"), dict)
                    else None
                ),
                pipeline_trace=(
                    task.get("pipeline_trace")
                    if isinstance(task.get("pipeline_trace"), list)
                    else None
                ),
            )
            return 1

        git_sync_paths.append(artifact_path)
        git_sync_paths.extend(Path(path) for path in task.get("commit_paths", []))
        _print_execution_summary(
            task=task,
            artifact_path=artifact_path,
            validation=validation,
            subtasks_path=progress_path,
        )
        _log_metrics(
            task_id=task_id,
            status="DONE",
            started_at=subtask_started_at,
            start_perf=subtask_start_perf,
            instruction_text=str(task["instruction"]),
            validation_passed=bool(validation["valid"]),
            validation_reason=str(validation["reason"]),
            commit_hash=None,
            artifact_path=artifact_path,
            subtask_id=str(task["subtask_id"]),
            parent_task_id=task_id,
            force_execution_used=force_execution_used,
            force_execution_reason=force_execution_reason,
            prior_artifact_path=prior_artifact_path,
            tool_trace=task.get("tool_trace")
            if isinstance(task.get("tool_trace"), dict)
            else None,
            pipeline_trace=(
                task.get("pipeline_trace")
                if isinstance(task.get("pipeline_trace"), list)
                else None
            ),
        )
        final_task = task
        final_artifact_path = artifact_path

    if final_task is None or final_artifact_path is None:
        raise RuntimeError("no subtasks were executed")

    _, comment_id = update_issue_execution_status(
        issue_number=task_id,
        commit_hash=None,
        artifact_path=progress_path,
    )
    print("issue updated")
    if comment_id is not None:
        print(f"comment id: {comment_id}")
    else:
        print("comment id: unavailable")

    git_sync_paths.append(progress_path)
    git_sync_result = best_effort_post_execution_git_sync(
        task_id=task_id,
        artifact_path=final_artifact_path,
        subtask_id=str(final_task.get("subtask_id"))
        if final_task.get("subtask_id")
        else None,
        additional_paths=git_sync_paths,
    )
    _print_git_sync_summary(git_sync_result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
