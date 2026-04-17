from __future__ import annotations

import copy
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import sleep
from typing import Any, Callable, Sequence
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import scripts.run_codex_task as run_codex_task_module
import app.execution.decision_engine as decision_engine_module
from app.execution.decision_trace import build_decision_trace, summarize_context_reference
from app.execution.vendor_router import DEFAULT_VENDOR, route as route_vendor
from control.context_store import (
    load_context,
    set_active_mode,
)
from control.tool_executor import execute_tool
from control.tool_registry import HTTP_REQUEST_TOOL
from control.state_store import (
    STATE_FILE,
    append_command_state,
)
from memory.memory_store import (
    append_execution_system_context_recent_action,
    build_execution_system_context_summary,
    build_memory_snapshot,
    build_memory_summary,
    get_project_state,
    load_required_execution_system_context,
    read_memory,
    read_execution_system_context,
    write_memory,
)

TASK_OUTPUT_PATH = REPO_ROOT / "tasks" / "codex" / "auto-task.json"
ARTIFACT_DIR = REPO_ROOT / "artifacts"
EXECUTION_LOG_PATH = REPO_ROOT / "logs" / "execution_log.jsonl"
DEFAULT_TEST_FILE_ID = "1M-pWmYBp7ZvnRECe1l7VBBaeD787Asj9tCBo6R8sGfU"
PIPELINE_TASK_INSTRUCTION = "Run explicit Tool Layer v1 pipeline"
ANALYSIS_SAMPLE_TEXT = (
    "Digital Foreman converts exact commands into deterministic task packets. "
    "Each route triggers a fixed sequence of tool calls so execution stays "
    "explicit, static, and testable. The command layer intentionally avoids "
    "fuzzy parsing, autonomous planning, and chat-style interpretation."
)
EMAIL_ANALYZE_INSTRUCTION = "Summarize email and suggest reply"
OWNER_ANALYZE_PREAMBLE = """You are assisting the owner:
Anton Vorontsov.

Focus:
- immigration cases
- business development
- system growth

Respond in a structured, actionable way."""
OWNER_TASK_INSTRUCTION = (
    "Use owner context to answer this request. "
    "Return sections exactly named: Action Steps, Priorities, Next Moves."
)
LINKEDIN_POST_DRAFT_SUGGESTION = "Review and approve manually before posting."
COMMAND_RETRY_DELAYS_SECONDS = (1.0, 2.0)
RETRIABLE_FAILURE_REASONS = {
    "CLAUDE_API_FAILED",
    "DRIVE_READ_FAILED",
    "GMAIL_API_FAILED",
    "GMAIL_DRAFT_FAILED",
    "GMAIL_READ_FAILED",
    "GOOGLE_DOCS_CREATE_FAILED",
    "TOOL_EXECUTION_FAILED",
}
COMMAND_STATE_FILE = STATE_FILE
CONTEXT_DIR = REPO_ROOT / "context"
VALID_MODES = ("dev", "owner", "business")
CONFLICT_PREFIXES = ("do not ", "don't ", "never ", "avoid ")
ROLE_PIPELINE = ("planner", "executor", "verifier")


def _timestamp_task_id() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _normalize_command(command_text: str) -> str:
    return " ".join(str(command_text or "").lower().split())


def _normalize_mode(mode: str) -> str:
    normalized_mode = str(mode or "").strip().lower()
    if normalized_mode not in VALID_MODES:
        return "dev"
    return normalized_mode


def get_relevant_context(
    mode: str,
    *,
    context_payload: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any] | None:
    normalized_mode = _normalize_mode(mode)
    if normalized_mode == "dev":
        return None

    payload = context_payload or load_context(context_dir=CONTEXT_DIR)
    selected_context = payload.get(f"{normalized_mode}_context")
    if not isinstance(selected_context, dict):
        return {}
    return copy.deepcopy(selected_context)


def _load_memory_summary() -> dict[str, Any]:
    return build_memory_summary()


def _load_system_context_summary() -> dict[str, Any]:
    system_context = read_execution_system_context()
    return build_execution_system_context_summary(system_context)


def _context_with_memory(
    selected_context: dict[str, Any] | None,
    *,
    context_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    memory_summary = _load_memory_summary()
    system_context_summary = (
        copy.deepcopy(context_summary)
        if isinstance(context_summary, dict)
        else _load_system_context_summary()
    )
    if isinstance(selected_context, dict):
        updated_context = copy.deepcopy(selected_context)
        updated_context["memory_summary"] = memory_summary
        updated_context["context_summary"] = system_context_summary
        return updated_context
    return {
        "context_summary": system_context_summary,
        "memory_summary": memory_summary,
    }


def _linkedin_context_text(
    selected_context: dict[str, Any] | None,
    *,
    mode: str = "dev",
) -> str:
    if not isinstance(selected_context, dict):
        return ""

    context_summary = selected_context.get("context_summary")
    summary_text = (
        json.dumps(context_summary, ensure_ascii=False, sort_keys=True)
        if isinstance(context_summary, dict)
        else "{}"
    )
    if _normalize_mode(mode) == "owner":
        return "\n".join(
            [
                "Owner context summary:",
                f"- System context: {summary_text}",
                f"- Active owner context: {json.dumps(selected_context, ensure_ascii=False, sort_keys=True)}",
            ]
        )

    return summary_text

    if _normalize_mode(mode) == "owner":
        return "\n".join(
            [
                "Owner context:",
                "- твою систему: Digital Foreman execution control system built from real operating work.",
                f"- текущую стадию: {json.dumps(selected_context, ensure_ascii=False, sort_keys=True)}",
                "- реальные действия: active immigration, business, and system-building work already happening now.",
            ]
        )

    return json.dumps(selected_context, sort_keys=True)


def _inject_context_into_pipeline(
    task_payload: dict[str, Any],
    selected_context: dict[str, Any] | None,
    *,
    mode: str = "dev",
) -> dict[str, Any]:
    pipeline = task_payload.get("pipeline")
    if not isinstance(pipeline, list):
        return task_payload

    updated_task = copy.deepcopy(task_payload)
    for step in updated_task["pipeline"]:
        tool_name = str(step.get("tool_name") or "").strip()
        if tool_name == "linkedin.create_post_draft":
            input_payload = step.get("input")
            if not isinstance(input_payload, dict):
                continue
            input_payload["context"] = _linkedin_context_text(
                selected_context,
                mode=mode,
            )
            continue

        if tool_name != "claude.analyze":
            continue
        input_payload = step.get("input")
        if not isinstance(input_payload, dict):
            continue
        if isinstance(selected_context, dict):
            input_payload["context"] = copy.deepcopy(selected_context)
        if _normalize_mode(mode) == "owner":
            instruction = str(input_payload.get("instruction") or "").strip()
            if instruction:
                input_payload["instruction"] = (
                    f"{OWNER_ANALYZE_PREAMBLE}\n\n{instruction}"
                )
    return updated_task


def _build_pipeline_task(pipeline: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "task_id": _timestamp_task_id(),
        "instruction": PIPELINE_TASK_INSTRUCTION,
        "force_execution": True,
        "pipeline": pipeline,
    }


def _build_summarize_drive_file_task() -> dict[str, Any]:
    return _build_pipeline_task(
        [
            {
                "tool_name": "google_drive.read_file",
                "input": {
                    "file_id": DEFAULT_TEST_FILE_ID,
                },
                "output_key": "drive_data",
            },
            {
                "tool_name": "claude.analyze",
                "input": {
                    "instruction": "Summarize this document in 3 sentences",
                    "text": "{{drive_data.content_text}}",
                },
                "output_key": "analysis_data",
            },
            {
                "tool_name": "google_docs.create_document",
                "input": {
                    "title": "DF COMMAND RESULT",
                    "content": "{{analysis_data.analysis}}",
                },
            },
        ]
    )


def _build_analyze_drive_file_task() -> dict[str, Any]:
    return _build_pipeline_task(
        [
            {
                "tool_name": "google_drive.read_file",
                "input": {
                    "file_id": DEFAULT_TEST_FILE_ID,
                },
                "output_key": "drive_data",
            },
            {
                "tool_name": "claude.analyze",
                "input": {
                    "instruction": "Analyze this document and produce a concise structured summary",
                    "text": "{{drive_data.content_text}}",
                },
                "output_key": "analysis_data",
            },
        ]
    )


def _build_create_doc_from_analysis_task() -> dict[str, Any]:
    return _build_pipeline_task(
        [
            {
                "tool_name": "claude.analyze",
                "input": {
                    "instruction": "Summarize this text in 3 sentences",
                    "text": ANALYSIS_SAMPLE_TEXT,
                },
                "output_key": "analysis_data",
            },
            {
                "tool_name": "google_docs.create_document",
                "input": {
                    "title": "DF ANALYSIS DOC",
                    "content": "{{analysis_data.analysis}}",
                },
            },
        ]
    )


def _build_process_email_task() -> dict[str, Any]:
    return _build_pipeline_task(
        [
            {
                "tool_name": "gmail.read_latest",
                "input": {},
                "output_key": "email_data",
            },
            {
                "tool_name": "claude.analyze",
                "input": {
                    "instruction": EMAIL_ANALYZE_INSTRUCTION,
                    "text": (
                        "Subject: {{email_data.subject}}\n"
                        "Sender: {{email_data.sender}}\n\n"
                        "Body:\n{{email_data.body_text}}"
                    ),
                },
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
        ]
    )


def _is_process_email_task(task_payload: dict[str, Any], command_name: str) -> bool:
    return (
        str(command_name or "").strip() == "process email"
        or str(task_payload.get("pipeline_route") or "").strip() == "process email"
    )


def _build_owner_task(owner_request: str) -> dict[str, Any]:
    normalized_request = str(owner_request or "").strip()
    return _build_pipeline_task(
        [
            {
                "tool_name": "claude.analyze",
                "input": {
                    "instruction": OWNER_TASK_INSTRUCTION,
                    "text": normalized_request,
                },
                "output_key": "analysis_data",
            },
        ]
    )


def _build_linkedin_post_task(topic: str) -> dict[str, Any]:
    normalized_topic = str(topic or "").strip()
    return _build_pipeline_task(
        [
            {
                "tool_name": "linkedin.create_post_draft",
                "input": {
                    "topic": normalized_topic,
                    "context": "",
                },
                "output_key": "linkedin_post",
            },
        ]
    )


def _build_http_request_task(method: str, url: str) -> dict[str, Any]:
    task_payload = _build_pipeline_task([])
    task_payload["command_name"] = _normalize_command(f"http {method} {url}")
    task_payload["pipeline_route"] = "http request"
    task_payload["requires_planning"] = True
    task_payload["roles"] = list(ROLE_PIPELINE)
    task_payload["planning_route"] = "http_request"
    task_payload["planning_input"] = {
        "method": str(method or "GET").strip().upper() or "GET",
        "url": str(url or "").strip(),
    }
    return task_payload


CommandRouteBuilder = Callable[[], dict[str, Any]]
COMMAND_ROUTES: dict[str, CommandRouteBuilder] = {
    "summarize drive file": _build_summarize_drive_file_task,
    "analyze drive file": _build_analyze_drive_file_task,
    "create doc from analysis": _build_create_doc_from_analysis_task,
    "process email": _build_process_email_task,
}


def _parse_http_command(command_text: str) -> tuple[str, str] | None:
    parts = str(command_text or "").strip().split()
    if len(parts) < 3:
        return None
    if str(parts[0]).lower() != "http":
        return None
    method = str(parts[1]).strip().upper()
    if method not in {"GET", "POST"}:
        return None
    url = str(parts[2]).strip()
    if not url:
        return None
    return method, url


def parse_command(command_text: str) -> dict[str, Any] | None:
    normalized_command = _normalize_command(command_text)
    if not normalized_command:
        return None

    http_command = _parse_http_command(command_text)
    if http_command is not None:
        method, url = http_command
        return _build_http_request_task(method, url)

    if normalized_command.startswith("owner task "):
        owner_request = str(command_text or "").strip()[len("owner task ") :].strip()
        if not owner_request:
            return None
        task_payload = _build_owner_task(owner_request)
        task_payload["command_name"] = normalized_command
        task_payload["pipeline_route"] = "owner task"
        task_payload["context_mode"] = "owner"
        task_payload["print_analysis"] = True
        return task_payload

    if normalized_command.startswith("linkedin post "):
        topic = str(command_text or "").strip()[len("linkedin post ") :].strip()
        if not topic:
            return None
        task_payload = _build_linkedin_post_task(topic)
        task_payload["command_name"] = f"linkedin post {' '.join(topic.lower().split())}"
        task_payload["pipeline_route"] = "linkedin post"
        task_payload["print_linkedin_post"] = True
        return task_payload

    if "email" in normalized_command or "client" in normalized_command:
        task_payload = _build_process_email_task()
        task_payload["command_name"] = normalized_command
        task_payload["pipeline_route"] = "process email"
        return task_payload

    route_builder = COMMAND_ROUTES.get(normalized_command)
    if route_builder is None:
        return None
    task_payload = route_builder()
    task_payload["command_name"] = normalized_command
    task_payload["pipeline_route"] = normalized_command
    return task_payload


def _parse_context_command(command_text: str) -> tuple[str, str] | None:
    normalized_command = _normalize_command(command_text)
    if normalized_command == "show context":
        return "show_context", ""

    for mode in VALID_MODES:
        if normalized_command == f"set mode {mode}":
            return "set_mode", mode
    return None


def _parse_save_decision_payload(command_text: str) -> tuple[str, str]:
    decision_text = str(command_text or "").strip()
    if "|" in decision_text:
        decision, reason = decision_text.split("|", 1)
        return decision.strip(), reason.strip()

    lowered_text = decision_text.lower()
    marker = " because "
    marker_index = lowered_text.find(marker)
    if marker_index >= 0:
        return (
            decision_text[:marker_index].strip(),
            decision_text[marker_index + len(marker) :].strip(),
        )
    return decision_text, ""


def _parse_memory_command(command_text: str) -> tuple[str, str, str] | None:
    normalized_command = _normalize_command(command_text)
    if normalized_command == "show memory":
        return "show_memory", "", ""
    if normalized_command == "show state":
        return "show_state", "", ""
    if normalized_command == "show decisions":
        return "show_decisions", "", ""
    if normalized_command == "show architecture":
        return "show_architecture", "", ""
    if normalized_command.startswith("save decision "):
        raw_payload = str(command_text or "").strip()[len("save decision ") :].strip()
        if not raw_payload:
            return None
        decision, reason = _parse_save_decision_payload(raw_payload)
        if not decision:
            return None
        return "save_decision", decision, reason
    return None


def write_task_file(task_payload: dict[str, Any], task_path: Path | None = None) -> Path:
    output_path = task_path or TASK_OUTPUT_PATH
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(task_payload, indent=2) + "\n", encoding="utf-8")
    return output_path


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _console_safe_text(value: Any) -> str:
    return str(value or "").encode("ascii", errors="replace").decode("ascii")


def _artifact_url(artifact_path: Path) -> str:
    if not artifact_path.is_file():
        return ""

    try:
        payload = _read_json(artifact_path)
    except (json.JSONDecodeError, OSError):
        return ""

    direct_url = str(payload.get("url") or "").strip()
    if direct_url:
        return direct_url

    final_output = payload.get("final_output")
    if isinstance(final_output, dict):
        return str(final_output.get("url") or "").strip()

    return ""


def _doc_id_from_url(doc_url: str) -> str:
    normalized_url = str(doc_url or "").strip()
    if "/document/d/" not in normalized_url:
        return ""
    return normalized_url.split("/document/d/", 1)[1].split("/", 1)[0].strip()


def _doc_id_from_artifact(artifact_path: Path) -> str:
    if not artifact_path.is_file():
        return ""

    try:
        payload = _read_json(artifact_path)
    except (json.JSONDecodeError, OSError):
        return ""

    direct_doc_id = str(payload.get("doc_id") or "").strip()
    if direct_doc_id:
        return direct_doc_id

    final_output = payload.get("final_output")
    if isinstance(final_output, dict):
        nested_doc_id = str(final_output.get("doc_id") or "").strip()
        if nested_doc_id:
            return nested_doc_id

        nested_url = str(final_output.get("url") or "").strip()
        if nested_url:
            return _doc_id_from_url(nested_url)

    direct_url = str(payload.get("url") or "").strip()
    if direct_url:
        return _doc_id_from_url(direct_url)

    return ""


def _final_output_from_artifact(artifact_path: Path) -> dict[str, Any]:
    if not artifact_path.is_file():
        return {}

    try:
        payload = _read_json(artifact_path)
    except (json.JSONDecodeError, OSError):
        return {}

    final_output = payload.get("final_output")
    if not isinstance(final_output, dict):
        return {}
    return final_output


def _analysis_from_artifact(artifact_path: Path) -> str:
    final_output = _final_output_from_artifact(artifact_path)
    return str(final_output.get("analysis") or "").strip()


def _email_subject_from_artifact(artifact_path: Path) -> str:
    final_output = _final_output_from_artifact(artifact_path)
    return str(final_output.get("source_subject") or final_output.get("subject") or "").strip()


def _email_sent_from_artifact(artifact_path: Path) -> bool:
    final_output = _final_output_from_artifact(artifact_path)
    return bool(final_output.get("email_sent") or final_output.get("draft_created"))


def _process_email_result_from_artifact(artifact_path: Path) -> dict[str, str]:
    final_output = _final_output_from_artifact(artifact_path)
    recipient = str(final_output.get("to") or "").strip()
    subject = str(final_output.get("subject") or "").strip()
    output_parts = [part for part in (recipient, subject) if part]
    return {
        "action": "email sent",
        "status": "success",
        "output": " | ".join(output_parts),
    }


def _linkedin_post_from_artifact(artifact_path: Path) -> dict[str, Any]:
    final_output = _final_output_from_artifact(artifact_path)
    if not isinstance(final_output, dict):
        return {}
    return {
        "post_text": str(final_output.get("post_text") or "").strip(),
        "tone": str(final_output.get("tone") or "").strip(),
        "intent": str(final_output.get("intent") or "").strip(),
    }


def _tool_source_from_artifact(artifact_path: Path) -> str:
    if not artifact_path.is_file():
        return ""

    try:
        payload = _read_json(artifact_path)
    except (json.JSONDecodeError, OSError):
        return ""

    tool_trace = payload.get("tool_trace")
    if isinstance(tool_trace, dict):
        return str(tool_trace.get("tool_source") or tool_trace.get("source") or "").strip()

    pipeline_trace = payload.get("pipeline_trace")
    if isinstance(pipeline_trace, list):
        for step_trace in reversed(pipeline_trace):
            if not isinstance(step_trace, dict):
                continue
            tool_source = str(step_trace.get("tool_source") or step_trace.get("source") or "").strip()
            if tool_source:
                return tool_source
    return ""


def _task_failure_reason(task_result: dict[str, Any], artifact_path: Path) -> str:
    if not artifact_path.is_file():
        return "MISSING_ARTIFACT"

    direct_reason = str(task_result.get("failure_reason") or "").strip()
    if direct_reason:
        return direct_reason

    tool_trace = task_result.get("tool_trace")
    if isinstance(tool_trace, dict) and tool_trace.get("tool_ok") is False:
        return str(tool_trace.get("tool_error_code") or "").strip() or "TOOL_FAILURE"

    pipeline_trace = task_result.get("pipeline_trace")
    if isinstance(pipeline_trace, list):
        for step_trace in pipeline_trace:
            if isinstance(step_trace, dict) and step_trace.get("tool_ok") is False:
                return str(step_trace.get("tool_error_code") or "").strip() or "PIPELINE_FAILURE"

    try:
        artifact_payload = _read_json(artifact_path)
    except (json.JSONDecodeError, OSError):
        return "INVALID_ARTIFACT"

    return (
        str(artifact_payload.get("failure_reason") or "").strip()
        or str(artifact_payload.get("reason") or "").strip()
    )


def execute_command_task(task_path: Path) -> tuple[bool, str, Path, str]:
    task_result, artifact_path = run_codex_task_module.run_codex_task(
        task_path,
        artifact_dir=ARTIFACT_DIR,
        repo_root=REPO_ROOT,
    )
    resolved_artifact_path = Path(artifact_path)

    doc_url = str(task_result.get("doc_url") or "").strip()
    if doc_url:
        return True, doc_url, resolved_artifact_path, ""

    failure_reason = _task_failure_reason(task_result, resolved_artifact_path)
    if failure_reason:
        return False, "", resolved_artifact_path, failure_reason

    return (
        True,
        _artifact_url(resolved_artifact_path),
        resolved_artifact_path,
        "",
    )


def _record_command_state(
    command_name: str,
    *,
    succeeded: bool,
    doc_url: str,
    artifact_path: Path,
    failure_reason: str,
    state_path: Path | str | None = None,
) -> tuple[dict[str, Any], Path]:
    result = "SUCCESS" if succeeded else str(failure_reason or "FAILURE").strip() or "FAILURE"
    doc_id = _doc_id_from_url(doc_url) or _doc_id_from_artifact(artifact_path)
    return append_command_state(
        command_name=command_name,
        result=result,
        artifact=artifact_path,
        doc_id=doc_id,
        state_path=state_path,
    )


def execute_command_task_with_retry(task_path: Path) -> tuple[bool, str, Path, str]:
    attempts = (0.0, *COMMAND_RETRY_DELAYS_SECONDS)
    last_result: tuple[bool, str, Path, str] | None = None

    for attempt_index, delay_seconds in enumerate(attempts, start=1):
        if delay_seconds > 0:
            sleep(delay_seconds)

        last_result = execute_command_task(task_path)
        succeeded, _doc_url, _artifact_path, failure_reason = last_result
        if succeeded:
            return last_result
        if failure_reason not in RETRIABLE_FAILURE_REASONS:
            return last_result
        if attempt_index == len(attempts):
            return last_result

    if last_result is None:
        raise RuntimeError("COMMAND_EXECUTION_FAILED")
    return last_result


def _load_context_payload() -> dict[str, dict[str, Any]]:
    return load_context(context_dir=CONTEXT_DIR)


def _load_required_execution_context() -> dict[str, Any]:
    return load_required_execution_system_context()


def _recent_action_summary(command_name: str, normalized_command: str) -> str:
    action_name = _resolve_last_action(command_name, normalized_command)
    if not action_name:
        action_name = normalized_command or "execution"
    return f"{action_name}: completed"


def _failed_action_summary(
    command_name: str,
    normalized_command: str,
    failure_reason: str,
) -> str:
    action_name = _resolve_last_action(command_name, normalized_command)
    if not action_name:
        action_name = normalized_command or "execution"
    normalized_reason = " ".join(str(failure_reason or "").split()).strip() or "execution failed"
    return f"{action_name}: failed ({normalized_reason})"


def _record_system_context_execution_summary(summary: str) -> None:
    try:
        append_execution_system_context_recent_action(summary)
    except Exception:
        pass


def _record_system_context_success(command_name: str, normalized_command: str) -> None:
    _record_system_context_execution_summary(
        _recent_action_summary(command_name, normalized_command)
    )


def _handle_context_command(command_type: str, mode: str = "") -> tuple[int, Any]:
    if command_type == "set_mode":
        updated_system_context = set_active_mode(mode, context_dir=CONTEXT_DIR)
        return 0, {
            "active_mode": updated_system_context["active_mode"],
            "last_update": updated_system_context["last_update"],
        }

    if command_type == "show_context":
        return 0, load_context(context_dir=CONTEXT_DIR)

    return 1, {
        "status": "error",
        "message": "UNKNOWN COMMAND",
    }


def _handle_memory_command(command_type: str, value: str = "", reason: str = "") -> tuple[int, Any]:
    if command_type == "show_memory":
        return 0, build_memory_snapshot()

    if command_type == "show_decisions":
        return 0, read_memory("decisions")

    if command_type == "show_architecture":
        return 0, read_memory("architecture")

    if command_type == "show_state":
        return 0, get_project_state()

    if command_type == "save_decision":
        decision_entry = write_memory(
            "decisions",
            {
                "decision": value,
                "reason": reason,
            },
        )
        return 0, decision_entry

    return 1, {
        "status": "error",
        "message": "UNKNOWN COMMAND",
    }


def _conflicting_decision_terms(decision_text: str) -> list[str]:
    normalized_decision = " ".join(str(decision_text or "").lower().split())
    for prefix in CONFLICT_PREFIXES:
        if normalized_decision.startswith(prefix):
            phrase = normalized_decision[len(prefix) :].strip(" .,!?:;")
            if phrase:
                return [phrase]
    return []


def _memory_conflict_detected(suggestion_text: str) -> bool:
    normalized_suggestion = " ".join(str(suggestion_text or "").lower().split())
    if not normalized_suggestion:
        return False

    decisions_payload = read_memory("decisions")
    decisions = decisions_payload.get("decisions")
    if not isinstance(decisions, list):
        return False

    for entry in reversed(decisions):
        if not isinstance(entry, dict):
            continue
        decision_text = str(entry.get("decision") or "").strip()
        if not decision_text:
            continue
        for forbidden_phrase in _conflicting_decision_terms(decision_text):
            if forbidden_phrase in normalized_suggestion:
                return True
    return False


def _resolve_last_action(command_name: str, normalized_command: str) -> str:
    return str(command_name or normalized_command).strip()


def _build_last_result_summary(last_action: str, *, tool_source: str = "") -> str:
    normalized_action = _normalize_command(last_action)
    normalized_source = _normalize_command(tool_source)
    if normalized_source == "fallback":
        return "fallback executed"
    if "email" in normalized_action:
        return "email sent"
    if "http" in normalized_action:
        return "http request success"
    return "fallback executed"


def _write_success_project_state(
    command_name: str,
    normalized_command: str,
    *,
    tool_source: str = "",
) -> dict[str, str]:
    last_action = _resolve_last_action(command_name, normalized_command)
    project_state_update = {
        "last_action": last_action,
        "last_result_summary": _build_last_result_summary(
            last_action,
            tool_source=tool_source,
        ),
    }
    write_memory("project_state", project_state_update)
    return project_state_update


def _memory_state_from_summary(memory_summary: dict[str, Any] | None = None) -> dict[str, str]:
    summary = memory_summary if isinstance(memory_summary, dict) else _load_memory_summary()
    return {
        "core_status": str(summary.get("core_status") or "").strip(),
        "operating_phase": str(summary.get("operating_phase") or "").strip(),
        "system_mode": str(summary.get("system_mode") or "").strip(),
    }


def _normalize_tool_source(tool_source: str) -> str:
    return "fallback" if _normalize_command(tool_source) == "fallback" else "external"


def _normalize_output_contract(
    payload: dict[str, Any] | None,
    *,
    result: Any = None,
    memory_summary: dict[str, Any] | None = None,
    tool_source: str = "",
    execution_id: str = "",
    timestamp: str = "",
    decision_trace: dict[str, Any] | None = None,
) -> dict[str, Any]:
    candidate_payload = dict(payload or {})
    candidate_trace = candidate_payload.get("execution_trace")
    if not isinstance(candidate_trace, dict):
        candidate_trace = {}
    candidate_decision_trace = candidate_payload.get("decision_trace")
    if not isinstance(candidate_decision_trace, dict):
        candidate_decision_trace = {}
    normalized_payload = {
        "result": candidate_payload.get("result", result),
        "execution_trace": {
            "execution_id": str(
                candidate_trace.get("execution_id") or execution_id or _timestamp_task_id()
            ),
            "memory_state": _memory_state_from_summary(
                memory_summary
                if memory_summary is not None
                else dict(candidate_trace.get("memory_state") or {})
            ),
            "timestamp": str(
                candidate_trace.get("timestamp")
                or timestamp
                or datetime.now(timezone.utc).isoformat()
            ),
            "tool_source": _normalize_tool_source(
                str(candidate_trace.get("tool_source") or tool_source)
            ),
        },
        "decision_trace": dict(decision_trace or candidate_decision_trace),
    }
    return normalized_payload


def _emit_output_contract(
    result: Any,
    *,
    memory_summary: dict[str, Any] | None = None,
    tool_source: str = "",
    execution_id: str = "",
    timestamp: str = "",
    decision_trace: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_payload = _normalize_output_contract(
        {
            "result": result,
            "execution_trace": {
                "execution_id": execution_id,
                "timestamp": timestamp,
                "tool_source": tool_source,
            },
            "decision_trace": dict(decision_trace or {}),
        },
        result=result,
        memory_summary=memory_summary,
        tool_source=tool_source,
        execution_id=execution_id,
        timestamp=timestamp,
        decision_trace=decision_trace,
    )
    print(
        json.dumps(
            normalized_payload,
            sort_keys=True,
            ensure_ascii=False,
        )
    )
    return normalized_payload


def _emit_error_output(
    message: str,
    *,
    memory_summary: dict[str, Any] | None = None,
    tool_source: str = "",
    execution_id: str = "",
    timestamp: str = "",
    decision_trace: dict[str, Any] | None = None,
) -> int:
    _emit_output_contract(
        {
            "status": "error",
            "message": _console_safe_text(message),
        },
        memory_summary=memory_summary,
        tool_source=tool_source,
        execution_id=execution_id,
        timestamp=timestamp,
        decision_trace=decision_trace,
    )
    return 1


def _requires_role_planning(task_payload: dict[str, Any]) -> bool:
    return bool(task_payload.get("requires_planning"))


def _planned_http_step(method: str, url: str) -> dict[str, Any]:
    parsed_url = urlparse(str(url or "").strip())
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
        raise ValueError("HTTP_REQUEST_INVALID_URL")
    resolved_method = str(method or "GET").strip().upper() or "GET"
    return {
        "step": f"http.request {resolved_method} {url}",
        "tool_name": HTTP_REQUEST_TOOL,
        "input": {
            "method": resolved_method,
            "url": str(url or "").strip(),
        },
    }


def _planner_steps(task_payload: dict[str, Any]) -> list[dict[str, Any]]:
    planning_route = str(task_payload.get("planning_route") or "").strip()
    planning_input = task_payload.get("planning_input")
    if not isinstance(planning_input, dict):
        raise ValueError("PLANNER_INPUT_INVALID")

    if planning_route == "http_request":
        return [
            _planned_http_step(
                str(planning_input.get("method") or "GET"),
                str(planning_input.get("url") or ""),
            )
        ]

    raise ValueError("PLANNER_ROUTE_UNSUPPORTED")


def _execute_planned_step(
    step_payload: dict[str, Any],
    *,
    system_context: dict[str, Any],
) -> dict[str, Any]:
    internal_executor = step_payload.get("internal_function")
    if callable(internal_executor):
        return {
            "status": "success",
            "data": internal_executor(dict(step_payload.get("input") or {})),
            "error": None,
            "source": "internal",
        }

    tool_name = str(step_payload.get("tool_name") or "").strip()
    return execute_tool(
        tool_name,
        {
            "context": copy.deepcopy(system_context),
            "input": dict(step_payload.get("input") or {}),
        },
    )


def _verify_planned_step(
    step_payload: dict[str, Any],
    execution_result: dict[str, Any],
) -> dict[str, Any]:
    if str(execution_result.get("status") or "").strip() != "success":
        error_payload = dict(execution_result.get("error") or {})
        return {
            "valid": False,
            "error": str(error_payload.get("message") or "ROLE_EXECUTION_FAILED").strip()
            or "ROLE_EXECUTION_FAILED",
            "retry_hint": "retry once",
        }

    tool_name = str(step_payload.get("tool_name") or "").strip()
    if tool_name == HTTP_REQUEST_TOOL:
        data = execution_result.get("data")
        if not isinstance(data, dict):
            return {
                "valid": False,
                "error": "HTTP verification failed: response payload missing",
                "retry_hint": "retry once",
            }
        try:
            status_code = int(data.get("status_code") or 0)
        except (TypeError, ValueError):
            status_code = 0
        if status_code < 200 or status_code >= 400:
            return {
                "valid": False,
                "error": f"HTTP verification failed: status {status_code or 'unknown'}",
                "retry_hint": "retry once",
            }

    return {
        "valid": True,
        "error": "",
        "retry_hint": "",
    }


def _execute_role_pipeline(
    task_payload: dict[str, Any],
    *,
    artifact_path: Path,
    system_context: dict[str, Any],
) -> tuple[bool, str, Path, str, Any, dict[str, Any], str]:
    planned_steps = _planner_steps(task_payload)
    role_trace: dict[str, Any] = {
        "roles": list(task_payload.get("roles") or list(ROLE_PIPELINE)),
        "planner": {
            "status": "success",
            "steps": [str(step.get("step") or "").strip() for step in planned_steps],
        },
        "steps": [],
    }
    final_result: Any = ""
    tool_source = ""

    for step_index, step_payload in enumerate(planned_steps, start=1):
        step_name = str(step_payload.get("step") or "").strip() or f"step-{step_index}"
        step_trace = {
            "step": step_name,
            "tool_name": str(step_payload.get("tool_name") or "").strip(),
            "attempts": [],
            "status": "failed",
        }
        verification: dict[str, Any] = {
            "valid": False,
            "error": "ROLE_EXECUTION_FAILED",
            "retry_hint": "retry once",
        }
        execution_result: dict[str, Any] = {
            "status": "error",
            "data": None,
            "error": {"message": "ROLE_EXECUTION_FAILED"},
            "source": "",
        }

        for attempt in range(1, 3):
            execution_result = _execute_planned_step(
                step_payload,
                system_context=system_context,
            )
            verification = _verify_planned_step(step_payload, execution_result)
            step_trace["attempts"].append(
                {
                    "attempt": attempt,
                    "executor_status": str(execution_result.get("status") or "").strip(),
                    "verifier_status": "pass" if verification["valid"] else "fail",
                    "error": str(verification.get("error") or "").strip(),
                    "retry_hint": str(verification.get("retry_hint") or "").strip(),
                }
            )
            if verification["valid"]:
                step_trace["status"] = "passed"
                break

        role_trace["steps"].append(step_trace)
        if step_trace["status"] != "passed":
            role_trace["verifier"] = {
                "status": "failed",
                "error": str(verification.get("error") or "ROLE_VERIFICATION_FAILED").strip(),
                "retry_hint": str(verification.get("retry_hint") or "retry once").strip(),
            }
            return (
                False,
                "",
                artifact_path,
                role_trace["verifier"]["error"],
                "",
                role_trace,
                str(execution_result.get("source") or "").strip(),
            )

        final_result = execution_result.get("data")
        if not tool_source:
            tool_source = str(execution_result.get("source") or "").strip()

    role_trace["verifier"] = {
        "status": "passed",
    }
    return True, "", artifact_path, "", final_result, role_trace, tool_source


def _execute_task_request(
    task_payload: dict[str, Any],
    *,
    command_text: str,
    system_context: dict[str, Any],
) -> dict[str, Any]:
    command_name = str(task_payload.get("command_name") or "").strip()
    is_process_email_task = _is_process_email_task(task_payload, command_name)
    uses_role_pipeline = _requires_role_planning(task_payload)
    execution_state: dict[str, Any] = {
        "command_name": command_name,
        "is_process_email_task": is_process_email_task,
        "uses_role_pipeline": uses_role_pipeline,
        "planned_result": "",
        "planned_trace": None,
        "planned_tool_source": "",
    }

    task_path = write_task_file(task_payload)
    execution_state["task_path"] = task_path

    try:
        if uses_role_pipeline:
            (
                succeeded,
                doc_url,
                artifact_path,
                failure_reason,
                execution_state["planned_result"],
                execution_state["planned_trace"],
                execution_state["planned_tool_source"],
            ) = _execute_role_pipeline(
                task_payload,
                artifact_path=task_path,
                system_context=system_context,
            )
        else:
            succeeded, doc_url, artifact_path, failure_reason = execute_command_task_with_retry(task_path)
    except Exception as error:
        failure_reason = str(error).strip() or "COMMAND_EXECUTION_FAILED"
        execution_state.update(
            {
                "succeeded": False,
                "doc_url": "",
                "artifact_path": task_path,
                "failure_reason": failure_reason,
            }
        )
        return execution_state

    execution_state.update(
        {
            "succeeded": succeeded,
            "doc_url": doc_url,
            "artifact_path": artifact_path,
            "failure_reason": failure_reason,
        }
    )
    return execution_state


def _build_task_result(
    task_payload: dict[str, Any],
    execution_state: dict[str, Any],
) -> tuple[Any, str]:
    artifact_path = Path(execution_state["artifact_path"])
    uses_role_pipeline = bool(execution_state.get("uses_role_pipeline"))
    is_process_email_task = bool(execution_state.get("is_process_email_task"))

    tool_source = ""
    result: Any = ""
    if uses_role_pipeline:
        result = execution_state.get("planned_result", "")
        tool_source = str(execution_state.get("planned_tool_source") or "").strip()
    elif bool(task_payload.get("print_analysis")):
        result = _analysis_from_artifact(artifact_path)
    elif is_process_email_task:
        result = _process_email_result_from_artifact(artifact_path)
    elif bool(task_payload.get("print_linkedin_post")):
        result = _linkedin_post_from_artifact(artifact_path)
    elif str(execution_state.get("doc_url") or "").strip():
        result = str(execution_state.get("doc_url") or "").strip()

    if not tool_source:
        tool_source = _tool_source_from_artifact(artifact_path)
    return result, tool_source


def _build_task_output_payload(
    result: Any,
    *,
    memory_summary: dict[str, Any],
    tool_source: str,
    execution_id: str,
    timestamp: str,
    decision_trace: dict[str, Any],
) -> dict[str, Any]:
    return _normalize_output_contract(
        {
            "result": result,
            "execution_trace": {},
            "decision_trace": dict(decision_trace),
        },
        result=result,
        memory_summary=memory_summary,
        tool_source=tool_source,
        execution_id=execution_id,
        timestamp=timestamp,
        decision_trace=decision_trace,
    )


def _command_context_reference(
    *,
    command_name: str,
    mode: str,
    context_summary: dict[str, Any] | None,
    task_payload: dict[str, Any] | None = None,
) -> str:
    return summarize_context_reference(
        command_name=command_name,
        mode=mode,
        payload={
            "operation": str((task_payload or {}).get("pipeline_route") or "").strip(),
        },
        context_summary=context_summary,
        source="run_command",
    )


def _command_decision_trace(
    *,
    command_name: str,
    mode: str,
    context_summary: dict[str, Any] | None,
    task_payload: dict[str, Any] | None,
    success: bool,
    failure_reason: str = "",
    vendor: str = DEFAULT_VENDOR,
) -> dict[str, str]:
    normalized_command = str(command_name or "").strip() or "execution"
    normalized_reason = str(failure_reason or "").strip()
    return build_decision_trace(
        reason=(
            f"{normalized_command} executed successfully"
            if success
            else normalized_reason or f"{normalized_command} failed"
        ),
        context_used=_command_context_reference(
            command_name=normalized_command,
            mode=mode,
            context_summary=context_summary,
            task_payload=task_payload,
        ),
        action_type=normalized_command,
        policy_result="allowed: context loaded and command routed" if success else f"blocked: {normalized_reason or 'execution failed'}",
        confidence="high" if success else "medium",
        vendor=vendor,
    )


def _persist_success_output(
    command_name: str,
    normalized_command: str,
    *,
    tool_source: str,
    result_payload: dict[str, Any],
) -> None:
    decision_trace = dict(result_payload.get("decision_trace") or {})
    _write_success_project_state(
        command_name,
        normalized_command,
        tool_source=tool_source,
    )
    _record_system_context_execution_summary(
        _recent_action_summary(command_name, normalized_command)
        + (
            f" | {str(decision_trace.get('reason') or '').strip()}"
            if str(decision_trace.get("reason") or "").strip()
            else ""
        )
    )
    try:
        EXECUTION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with EXECUTION_LOG_PATH.open("a", encoding="utf-8") as execution_log:
            execution_log.write(json.dumps(result_payload) + "\n")
    except Exception:
        pass


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    command_text = " ".join(str(argument) for argument in arguments).strip()
    normalized_command = _normalize_command(command_text)
    if normalized_command.startswith("owner input "):
        from owner_entry import handle_owner_input

        owner_payload = handle_owner_input(
            str(command_text or "").strip()[len("owner input ") :].strip()
        )
        print(json.dumps(owner_payload, sort_keys=True, ensure_ascii=False))
        return 0 if owner_payload.get("status") == "success" else 1

    context_command = _parse_context_command(command_text)
    memory_command = _parse_memory_command(command_text)
    task_payload = parse_command(command_text)
    output_execution_id = str(_timestamp_task_id())
    output_timestamp = datetime.now(timezone.utc).isoformat()
    output_memory_summary: dict[str, Any] = {}
    if context_command is None and memory_command is None and task_payload is None:
        output_memory_summary = _load_memory_summary()
        _emit_output_contract(
            {
                "status": "error",
                "message": "UNKNOWN COMMAND",
            },
            memory_summary=output_memory_summary,
            tool_source="external",
            execution_id=output_execution_id,
            timestamp=output_timestamp,
            decision_trace=build_decision_trace(
                reason="command not recognized",
                context_used="command routing without execution context",
                action_type=normalized_command or "unknown_command",
                policy_result="blocked: unknown command",
                confidence="low",
            ),
        )
        return 1

    if context_command is not None:
        command_type, mode = context_command
        exit_code, result = _handle_context_command(command_type, mode)
        _emit_output_contract(
            result,
            memory_summary=_load_memory_summary(),
            tool_source="external",
            execution_id=output_execution_id,
            timestamp=output_timestamp,
            decision_trace=build_decision_trace(
                reason=f"{command_type} completed",
                context_used="context command routing",
                action_type=command_type,
                policy_result="allowed: local context command",
                confidence="high",
            ),
        )
        return exit_code
    if memory_command is not None:
        command_type, value, reason = memory_command
        exit_code, result = _handle_memory_command(command_type, value, reason)
        _emit_output_contract(
            result,
            memory_summary=_load_memory_summary(),
            tool_source="external",
            execution_id=output_execution_id,
            timestamp=output_timestamp,
            decision_trace=build_decision_trace(
                reason=f"{command_type} completed",
                context_used="memory command routing",
                action_type=command_type,
                policy_result="allowed: local memory command",
                confidence="high",
            ),
        )
        return exit_code

    try:
        execution_system_context = _load_required_execution_context()
    except RuntimeError as error:
        return _emit_error_output(
            str(error).strip() or "CONTEXT_NOT_LOADED",
            memory_summary=output_memory_summary,
            tool_source="external",
            execution_id=output_execution_id,
            timestamp=output_timestamp,
            decision_trace=build_decision_trace(
                reason=str(error).strip() or "context not loaded",
                context_used="execution context bootstrap",
                action_type=normalized_command or "execution",
                policy_result="blocked: required execution context missing",
                confidence="low",
            ),
        )

    context_payload = _load_context_payload()
    active_mode = _normalize_mode(context_payload["system_context"].get("active_mode"))
    effective_mode = _normalize_mode(task_payload.get("context_mode") or active_mode)
    context_summary = _load_system_context_summary()
    selected_context = get_relevant_context(
        effective_mode,
        context_payload=context_payload,
    )
    selected_context = _context_with_memory(
        selected_context,
        context_summary=context_summary,
    )
    output_memory_summary = dict(selected_context.get("memory_summary") or {})
    task_payload = _inject_context_into_pipeline(
        task_payload,
        selected_context,
        mode=effective_mode,
    )

    command_name = str(task_payload.get("command_name") or "").strip()
    decision_context = {
        "task_id": str(task_payload.get("task_id", "")).strip(),
        "task_state": {
            "task_id": str(task_payload.get("task_id", "")).strip(),
            "status": "ready",
        },
        "command_name": command_name,
        "mode": effective_mode,
        "context_summary": context_summary,
        "system_context": execution_system_context,
        "selected_context": selected_context,
    }
    try:
        action_plan = decision_engine_module.validate_action_plan(
            decision_engine_module.decide(task_payload, decision_context),
            expected_task_id=str(task_payload.get("task_id", "")).strip(),
        )
        action_plan["vendor"] = route_vendor(task_payload, decision_context, action_plan)
        action_plan = decision_engine_module.validate_action_plan(
            action_plan,
            expected_task_id=str(task_payload.get("task_id", "")).strip(),
        )
    except Exception as error:
        reason = str(error).strip() or "missing decision before command execution"
        return _emit_error_output(
            reason,
            memory_summary=output_memory_summary,
            tool_source="external",
            execution_id=output_execution_id,
            timestamp=output_timestamp,
            decision_trace=decision_engine_module.failure_trace_for_context(
                task=task_payload,
                context=decision_context,
                reason=reason,
                source="run_command",
            ),
        )
    if action_plan["requires_confirmation"]:
        return _emit_error_output(
            str(action_plan["reason"]).strip() or "decision requires confirmation",
            memory_summary=output_memory_summary,
            tool_source="external",
            execution_id=output_execution_id,
            timestamp=output_timestamp,
            decision_trace=decision_engine_module.decision_trace_for_plan(
                action_plan,
                task=task_payload,
                context=decision_context,
                source="run_command",
            ),
        )

    execution_state = _execute_task_request(
        task_payload,
        command_text=command_text,
        system_context=execution_system_context,
    )
    if not bool(execution_state.get("succeeded")):
        failure_reason = str(execution_state.get("failure_reason") or "").strip()
        failure_trace = _command_decision_trace(
            command_name=command_name,
            mode=effective_mode,
            context_summary=context_summary,
            task_payload=task_payload,
            success=False,
            failure_reason=failure_reason,
            vendor=str(action_plan.get("vendor") or DEFAULT_VENDOR).strip() or DEFAULT_VENDOR,
        )
        _record_system_context_execution_summary(
            _failed_action_summary(command_name, normalized_command, failure_reason)
            + f" | {failure_trace['reason']}"
        )
        failure_tool_source = "fallback" if bool(execution_state.get("fallback_error")) else str(
            execution_state.get("planned_tool_source") or ""
        ).strip()
        return _emit_error_output(
            failure_reason,
            memory_summary=output_memory_summary,
            tool_source=failure_tool_source,
            execution_id=output_execution_id,
            timestamp=output_timestamp,
            decision_trace=failure_trace,
        )

    _record_command_state(
        command_name,
        succeeded=True,
        doc_url=str(execution_state.get("doc_url") or ""),
        artifact_path=Path(execution_state["artifact_path"]),
        failure_reason="",
        state_path=COMMAND_STATE_FILE,
    )

    result, tool_source = _build_task_result(task_payload, execution_state)
    success_trace = _command_decision_trace(
        command_name=command_name,
        mode=effective_mode,
        context_summary=context_summary,
        task_payload=task_payload,
        success=True,
        vendor=str(action_plan.get("vendor") or DEFAULT_VENDOR).strip() or DEFAULT_VENDOR,
    )
    result_payload = _build_task_output_payload(
        result,
        memory_summary=output_memory_summary,
        tool_source=tool_source,
        execution_id=output_execution_id,
        timestamp=output_timestamp,
        decision_trace=success_trace,
    )
    _persist_success_output(
        command_name,
        normalized_command,
        tool_source=tool_source,
        result_payload=result_payload,
    )
    _emit_output_contract(
        result_payload.get("result"),
        memory_summary=output_memory_summary,
        tool_source=result_payload["execution_trace"].get("tool_source", ""),
        execution_id=result_payload["execution_trace"].get("execution_id", ""),
        timestamp=result_payload["execution_trace"].get("timestamp", ""),
        decision_trace=result_payload.get("decision_trace"),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
