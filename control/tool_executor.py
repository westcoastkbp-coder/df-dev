from __future__ import annotations

from datetime import datetime, timezone
import json
import socket
from time import perf_counter
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from integrations.claude_tool import run_claude_analyze_fallback
from integrations.gmail_tool import (
    run_gmail_create_draft_fallback,
    run_gmail_read_latest_fallback,
)
from integrations.google_drive_tool import run_google_drive_read_file_fallback
from integrations.linkedin_tool import run_linkedin_create_post_draft_fallback
from control.tool_registry import (
    CLAUDE_WEB_OPERATOR_TOOL,
    GMAIL_CREATE_DRAFT_TOOL,
    GMAIL_READ_LATEST_TOOL,
    GEMINI_GOOGLE_OPERATOR_TOOL,
    GOOGLE_LAYER_TOOL,
    GOOGLE_GMAIL_SEND_TOOL,
    GOOGLE_DOCS_CREATE_TOOL,
    GOOGLE_DRIVE_READ_FILE_TOOL,
    GOOGLE_DRIVE_READ_FILE_LAYER_TOOL,
    EMAIL_SEND_TOOL,
    HTTP_REQUEST_TOOL,
    LINKEDIN_CREATE_POST_DRAFT_TOOL,
    resolve_tool_executor,
)
from control.tool_validator import ToolValidationError, validate_tool_call

_NETWORK_HOSTS_BY_TOOL: dict[str, tuple[str, ...]] = {
    "claude.analyze": ("api.anthropic.com",),
    CLAUDE_WEB_OPERATOR_TOOL: ("api.anthropic.com",),
    EMAIL_SEND_TOOL: (),
    GEMINI_GOOGLE_OPERATOR_TOOL: ("generativelanguage.googleapis.com",),
    GOOGLE_LAYER_TOOL: (),
    GMAIL_CREATE_DRAFT_TOOL: ("gmail.googleapis.com", "oauth2.googleapis.com"),
    GMAIL_READ_LATEST_TOOL: ("gmail.googleapis.com", "oauth2.googleapis.com"),
    GOOGLE_GMAIL_SEND_TOOL: ("gmail.googleapis.com", "oauth2.googleapis.com"),
    GOOGLE_DOCS_CREATE_TOOL: ("docs.googleapis.com", "oauth2.googleapis.com"),
    "google_docs.create_document": ("docs.googleapis.com",),
    GOOGLE_DRIVE_READ_FILE_TOOL: ("www.googleapis.com", "drive.googleapis.com"),
    GOOGLE_DRIVE_READ_FILE_LAYER_TOOL: ("www.googleapis.com", "drive.googleapis.com"),
    HTTP_REQUEST_TOOL: (),
    LINKEDIN_CREATE_POST_DRAFT_TOOL: ("api.anthropic.com",),
}

_TOOL_INTERFACE_NAMES = {
    EMAIL_SEND_TOOL,
    HTTP_REQUEST_TOOL,
}


class ToolExecutionError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = str(code)
        self.message = str(message)


def _tool_name_from_call(tool_call: Any) -> str:
    if not isinstance(tool_call, dict):
        return ""
    return str(tool_call.get("tool_name") or "").strip()


def _unwrap_execution_payload(payload: dict[str, Any] | Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    if "input" not in payload or "context" not in payload:
        return dict(payload)

    context_payload = payload.get("context")
    if not isinstance(context_payload, dict):
        raise ToolExecutionError("CONTEXT_NOT_LOADED", "CONTEXT_NOT_LOADED")

    input_payload = payload.get("input")
    if not isinstance(input_payload, dict):
        raise ToolExecutionError("TOOL_INPUT_INVALID", "Tool execution payload input must be an object.")
    return dict(input_payload)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _duration_ms(start_perf: float) -> int:
    return max(0, int((perf_counter() - start_perf) * 1000))


def _step_metric(
    *,
    step_name: str,
    tool_name: str,
    step_start_time: str,
    step_end_time: str,
    step_duration_ms: int,
    success: bool,
    retry_count: int = 0,
    failure_reason: str = "",
    execution_source: str = "",
) -> dict[str, Any]:
    metric: dict[str, Any] = {
        "step_name": str(step_name),
        "tool_name": str(tool_name or "").strip(),
        "success": bool(success),
        "status": "success" if success else "failure",
        "step_start_time": str(step_start_time),
        "step_end_time": str(step_end_time),
        "step_duration": int(step_duration_ms),
        "step_duration_ms": int(step_duration_ms),
        "retry_count": int(retry_count),
    }
    if str(failure_reason or "").strip():
        metric["failure_reason"] = str(failure_reason).strip()
    if str(execution_source or "").strip():
        metric["source"] = str(execution_source).strip()
    return metric


def _retry_info(step_metrics: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "total_retry_count": sum(int(step.get("retry_count") or 0) for step in step_metrics),
        "steps": [
            {
                "step_name": str(step.get("step_name") or "").strip(),
                "retry_count": int(step.get("retry_count") or 0),
            }
            for step in step_metrics
        ],
    }


def _execution_timeline(
    *,
    start_time: str,
    start_perf: float,
    step_metrics: list[dict[str, Any]],
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
                "step_name": str(step.get("step_name") or "").strip(),
                "step_start_time": str(step.get("step_start_time") or "").strip(),
                "step_end_time": str(step.get("step_end_time") or "").strip(),
                "step_duration": int(step.get("step_duration") or 0),
                "step_duration_ms": int(step.get("step_duration_ms") or 0),
            }
            for step in step_metrics
        ],
    }


def _network_failure_type(code: str, message: str) -> str:
    combined = f"{code} {message}".strip().lower()
    if any(term in combined for term in ("dns", "getaddrinfo", "name resolution", "name or service")):
        return "dns_resolution_failed"
    if any(term in combined for term in ("timeout", "timed out", "deadline exceeded")):
        return "timeout"
    if "refused" in combined:
        return "connection_refused"
    if any(term in combined for term in ("unreachable", "no route to host")):
        return "network_unreachable"
    if any(term in combined for term in ("connection", "socket", "ssl", "tls")):
        return "connection_failed"
    return "application_error"


def _network_diagnostics(tool_name: str, code: str, message: str) -> dict[str, Any] | None:
    hosts = _NETWORK_HOSTS_BY_TOOL.get(str(tool_name or "").strip(), ())
    failure_type = _network_failure_type(code, message)
    if not hosts and failure_type == "application_error":
        return None

    host_checks: list[dict[str, Any]] = []
    for host in hosts:
        dns_start = perf_counter()
        dns_resolved = False
        dns_error = ""
        try:
            socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
            dns_resolved = True
        except OSError as error:
            dns_error = str(error).strip()
        dns_duration_ms = _duration_ms(dns_start)
        host_payload: dict[str, Any] = {
            "host": str(host),
            "dns_resolve_time_ms": int(dns_duration_ms),
            "dns_resolved": bool(dns_resolved),
        }
        if dns_error:
            host_payload["dns_error"] = dns_error
        host_checks.append(host_payload)

    return {
        "connection_attempted": bool(hosts),
        "failure_type": failure_type,
        "retry_attempts": 0,
        "failure_reason": str(message or "").strip(),
        "hosts": host_checks,
    }


def _supports_execute_tool(tool_name: str) -> bool:
    normalized_tool_name = str(tool_name or "").strip()
    if normalized_tool_name in _TOOL_INTERFACE_NAMES:
        return True
    try:
        resolve_tool_executor(normalized_tool_name)
    except KeyError:
        return False
    return True


def _execute_email_send(payload: dict[str, Any]) -> dict[str, Any]:
    recipient = str(payload.get("to") or "").strip()
    subject = str(payload.get("subject") or "").strip()
    body = str(payload.get("body") or "").strip()
    if not recipient:
        raise ToolExecutionError("EMAIL_SEND_FAILED", "Email recipient is required.")
    if not subject:
        raise ToolExecutionError("EMAIL_SEND_FAILED", "Email subject is required.")
    if not body:
        raise ToolExecutionError("EMAIL_SEND_FAILED", "Email body is required.")
    structured_result = payload.get("structured_result")
    if isinstance(structured_result, dict):
        return dict(structured_result)
    return {
        "to": recipient,
        "subject": subject,
        "body": body,
        "delivery": "queued",
    }


def _execute_http_request(payload: dict[str, Any]) -> dict[str, Any]:
    url = str(payload.get("url") or "").strip()
    if not url:
        raise ToolExecutionError("HTTP_REQUEST_FAILED", "HTTP request URL is required.")

    method = str(payload.get("method") or "GET").strip().upper() or "GET"
    headers_payload = payload.get("headers")
    headers = dict(headers_payload) if isinstance(headers_payload, dict) else {}
    body = payload.get("body")
    request_body = None
    if body is not None:
        if isinstance(body, (dict, list)):
            request_body = json.dumps(body).encode("utf-8")
            headers.setdefault("Content-Type", "application/json")
        else:
            request_body = str(body).encode("utf-8")

    request = Request(url, headers=headers, data=request_body, method=method)
    timeout_seconds = float(payload.get("timeout_seconds") or 15)
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            raw_body = response.read()
    except HTTPError as error:
        message = str(error.reason or "HTTP request failed.").strip() or "HTTP request failed."
        raise ToolExecutionError("HTTP_REQUEST_FAILED", message) from error
    except (URLError, OSError, TimeoutError, ValueError) as error:
        raise ToolExecutionError(
            "HTTP_REQUEST_FAILED",
            str(error).strip() or "HTTP request failed.",
        ) from error

    decoded_body = raw_body.decode("utf-8", errors="replace")
    parsed_body: Any
    try:
        parsed_body = json.loads(decoded_body)
    except ValueError:
        parsed_body = decoded_body

    return {
        "status_code": int(getattr(response, "status", 200)),
        "headers": dict(response.headers.items()),
        "body": parsed_body,
        "url": str(response.geturl()),
        "method": method,
    }


def _fallback_executor(tool_name: str):
    executors = {
        EMAIL_SEND_TOOL: _execute_email_send,
        "claude.analyze": run_claude_analyze_fallback,
        GMAIL_CREATE_DRAFT_TOOL: run_gmail_create_draft_fallback,
        GMAIL_READ_LATEST_TOOL: run_gmail_read_latest_fallback,
        GOOGLE_DRIVE_READ_FILE_TOOL: run_google_drive_read_file_fallback,
        GOOGLE_DRIVE_READ_FILE_LAYER_TOOL: run_google_drive_read_file_fallback,
        LINKEDIN_CREATE_POST_DRAFT_TOOL: run_linkedin_create_post_draft_fallback,
    }
    return executors.get(str(tool_name or "").strip())


def _error_payload(error: Exception, default_type: str = "TOOL_EXECUTION_FAILED") -> dict[str, str]:
    error_type = (
        str(getattr(error, "code", "") or getattr(error, "type", "") or default_type).strip()
        or default_type
    )
    return {
        "type": error_type,
        "message": str(error).strip() or "Tool execution failed.",
    }


def _success_execution_result(data: Any, *, source: str) -> dict[str, Any]:
    return {
        "status": "success",
        "data": dict(data) if isinstance(data, dict) else data,
        "error": None,
        "source": str(source),
    }


def _error_execution_result(error: dict[str, str], *, source: str) -> dict[str, Any]:
    return {
        "status": "error",
        "data": None,
        "error": {
            "type": str(error.get("type") or "TOOL_EXECUTION_FAILED").strip()
            or "TOOL_EXECUTION_FAILED",
            "message": str(error.get("message") or "Tool execution failed.").strip()
            or "Tool execution failed.",
        },
        "source": str(source),
    }


def _execute_external_tool(tool_name: str, payload: dict[str, Any]) -> Any:
    normalized_tool_name = str(tool_name or "").strip()
    if normalized_tool_name == EMAIL_SEND_TOOL:
        raise ToolExecutionError(
            "EXTERNAL_EXECUTION_UNAVAILABLE",
            "External execution is not available for email.send.",
        )
    if normalized_tool_name == HTTP_REQUEST_TOOL:
        return _execute_http_request(payload)
    executor = resolve_tool_executor(normalized_tool_name)
    return executor(payload)


def _execute_tool_with_trace(
    tool_name: str,
    payload: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, str] | None]:
    normalized_tool_name = str(tool_name or "").strip()
    input_payload = _unwrap_execution_payload(payload)
    step_metrics: list[dict[str, Any]] = []

    external_start_time = _utc_now_iso()
    external_start_perf = perf_counter()
    try:
        data = _execute_external_tool(normalized_tool_name, input_payload)
        step_metrics.append(
            _step_metric(
                step_name="execute_external_tool",
                tool_name=normalized_tool_name,
                step_start_time=external_start_time,
                step_end_time=_utc_now_iso(),
                step_duration_ms=_duration_ms(external_start_perf),
                success=True,
                execution_source="external",
            )
        )
        return _success_execution_result(data, source="external"), step_metrics, None
    except Exception as external_error:
        external_error_payload = _error_payload(external_error)
        step_metrics.append(
            _step_metric(
                step_name="execute_external_tool",
                tool_name=normalized_tool_name,
                step_start_time=external_start_time,
                step_end_time=_utc_now_iso(),
                step_duration_ms=_duration_ms(external_start_perf),
                success=False,
                failure_reason=external_error_payload["message"],
                execution_source="external",
            )
        )

    fallback_executor = _fallback_executor(normalized_tool_name)
    if fallback_executor is None:
        return (
            _error_execution_result(external_error_payload, source="external"),
            step_metrics,
            external_error_payload,
        )

    fallback_start_time = _utc_now_iso()
    fallback_start_perf = perf_counter()
    try:
        data = fallback_executor(input_payload)
        step_metrics.append(
            _step_metric(
                step_name="execute_fallback_tool",
                tool_name=normalized_tool_name,
                step_start_time=fallback_start_time,
                step_end_time=_utc_now_iso(),
                step_duration_ms=_duration_ms(fallback_start_perf),
                success=True,
                retry_count=1,
                execution_source="fallback",
            )
        )
        return _success_execution_result(data, source="fallback"), step_metrics, external_error_payload
    except Exception as fallback_error:
        fallback_error_payload = _error_payload(fallback_error)
        step_metrics.append(
            _step_metric(
                step_name="execute_fallback_tool",
                tool_name=normalized_tool_name,
                step_start_time=fallback_start_time,
                step_end_time=_utc_now_iso(),
                step_duration_ms=_duration_ms(fallback_start_perf),
                success=False,
                retry_count=1,
                failure_reason=fallback_error_payload["message"],
                execution_source="fallback",
            )
        )
        final_error = fallback_error_payload
        final_source = "fallback"
        if external_error_payload["type"] != "EXTERNAL_EXECUTION_UNAVAILABLE":
            final_error = external_error_payload
            final_source = "external"
        return (
            _error_execution_result(final_error, source=final_source),
            step_metrics,
            external_error_payload,
        )


def execute_tool(tool_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    result, _step_metrics, _external_error = _execute_tool_with_trace(tool_name, payload)
    return result


def _failure_result(
    tool_name: str,
    code: str,
    message: str,
    *,
    source: str,
    execution_timeline: dict[str, Any],
    step_metrics: list[dict[str, Any]],
    retry_info: dict[str, Any],
    network_diagnostics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result = {
        "ok": False,
        "tool_name": str(tool_name or "").strip(),
        "output": None,
        "error": {
            "code": str(code),
            "message": str(message),
        },
        "source": str(source or "").strip(),
        "execution_timeline": dict(execution_timeline),
        "step_metrics": [dict(step) for step in step_metrics],
        "retry_info": dict(retry_info),
    }
    if isinstance(network_diagnostics, dict):
        result["network_diagnostics"] = dict(network_diagnostics)
    return result


def _success_result(
    tool_name: str,
    output: dict[str, Any],
    *,
    source: str,
    execution_timeline: dict[str, Any],
    step_metrics: list[dict[str, Any]],
    retry_info: dict[str, Any],
) -> dict[str, Any]:
    return {
        "ok": True,
        "tool_name": str(tool_name or "").strip(),
        "output": dict(output),
        "error": None,
        "source": str(source or "").strip(),
        "execution_timeline": dict(execution_timeline),
        "step_metrics": [dict(step) for step in step_metrics],
        "retry_info": dict(retry_info),
    }


def execute_tool_call(tool_call: Any) -> dict[str, Any]:
    run_start_time = _utc_now_iso()
    run_start_perf = perf_counter()
    step_metrics: list[dict[str, Any]] = []
    tool_name = _tool_name_from_call(tool_call)

    try:
        validation_start_time = _utc_now_iso()
        validation_start_perf = perf_counter()
        validated_tool_call = validate_tool_call(tool_call)
        tool_name = validated_tool_call["tool_name"]
        step_metrics.append(
            _step_metric(
                step_name="validate_tool_call",
                tool_name=tool_name,
                step_start_time=validation_start_time,
                step_end_time=_utc_now_iso(),
                step_duration_ms=_duration_ms(validation_start_perf),
                success=True,
            )
        )
    except ToolValidationError as error:
        step_metrics.append(
            _step_metric(
                step_name="validate_tool_call",
                tool_name=tool_name,
                step_start_time=validation_start_time,
                step_end_time=_utc_now_iso(),
                step_duration_ms=_duration_ms(validation_start_perf),
                success=False,
                failure_reason=error.message,
            )
        )
        execution_timeline = _execution_timeline(
            start_time=run_start_time,
            start_perf=run_start_perf,
            step_metrics=step_metrics,
        )
        return _failure_result(
            tool_name,
            error.code,
            error.message,
            source="external",
            execution_timeline=execution_timeline,
            step_metrics=step_metrics,
            retry_info=_retry_info(step_metrics),
        )

    try:
        resolve_start_time = _utc_now_iso()
        resolve_start_perf = perf_counter()
        if not _supports_execute_tool(tool_name):
            raise KeyError(tool_name)
        step_metrics.append(
            _step_metric(
                step_name="resolve_tool_executor",
                tool_name=tool_name,
                step_start_time=resolve_start_time,
                step_end_time=_utc_now_iso(),
                step_duration_ms=_duration_ms(resolve_start_perf),
                success=True,
            )
        )
    except KeyError:
        message = f"Tool is not registered: {tool_name or '<missing>'}."
        step_metrics.append(
            _step_metric(
                step_name="resolve_tool_executor",
                tool_name=tool_name,
                step_start_time=resolve_start_time,
                step_end_time=_utc_now_iso(),
                step_duration_ms=_duration_ms(resolve_start_perf),
                success=False,
                failure_reason=message,
            )
        )
        execution_timeline = _execution_timeline(
            start_time=run_start_time,
            start_perf=run_start_perf,
            step_metrics=step_metrics,
        )
        return _failure_result(
            tool_name,
            "TOOL_NOT_FOUND",
            message,
            source="external",
            execution_timeline=execution_timeline,
            step_metrics=step_metrics,
            retry_info=_retry_info(step_metrics),
        )

    try:
        tool_result, execution_steps, external_error = _execute_tool_with_trace(
            tool_name,
            validated_tool_call["input"],
        )
        step_metrics.extend(execution_steps)
        if str(tool_result.get("status") or "").strip() != "success":
            error_payload = tool_result.get("error")
            if not isinstance(error_payload, dict):
                error_payload = {}
            message = str(error_payload.get("message") or "Tool execution failed.").strip()
            error_code = str(error_payload.get("type") or "TOOL_EXECUTION_FAILED").strip()
            if error_code == "TOOL_EXECUTION_FAILED" and tool_name in {
                GOOGLE_DRIVE_READ_FILE_TOOL,
                GOOGLE_DRIVE_READ_FILE_LAYER_TOOL,
            }:
                error_code = "DRIVE_READ_FAILED"
            network_error_payload = external_error or {
                "type": error_code,
                "message": message,
            }
            execution_timeline = _execution_timeline(
                start_time=run_start_time,
                start_perf=run_start_perf,
                step_metrics=step_metrics,
            )
            return _failure_result(
                tool_name,
                error_code,
                message,
                source=str(tool_result.get("source") or "").strip() or "external",
                execution_timeline=execution_timeline,
                step_metrics=step_metrics,
                retry_info=_retry_info(step_metrics),
                network_diagnostics=_network_diagnostics(
                    tool_name,
                    str(network_error_payload.get("type") or error_code),
                    str(network_error_payload.get("message") or message),
                ),
            )
        output = tool_result.get("data")
    except Exception as error:
        message = str(error).strip() or "Tool execution failed."
        error_code = str(getattr(error, "code", "") or "").strip() or "TOOL_EXECUTION_FAILED"
        if error_code == "TOOL_EXECUTION_FAILED" and tool_name in {
            GOOGLE_DRIVE_READ_FILE_TOOL,
            GOOGLE_DRIVE_READ_FILE_LAYER_TOOL,
        }:
            error_code = "DRIVE_READ_FAILED"
        execution_timeline = _execution_timeline(
            start_time=run_start_time,
            start_perf=run_start_perf,
            step_metrics=step_metrics,
        )
        return _failure_result(
            tool_name,
            error_code,
            message,
            source="external",
            execution_timeline=execution_timeline,
            step_metrics=step_metrics,
            retry_info=_retry_info(step_metrics),
            network_diagnostics=_network_diagnostics(tool_name, error_code, message),
        )

    output_validation_start_time = _utc_now_iso()
    output_validation_start_perf = perf_counter()
    if not isinstance(output, dict):
        message = "Tool output must be an object."
        step_metrics.append(
            _step_metric(
                step_name="validate_tool_output",
                tool_name=tool_name,
                step_start_time=output_validation_start_time,
                step_end_time=_utc_now_iso(),
                step_duration_ms=_duration_ms(output_validation_start_perf),
                success=False,
                failure_reason=message,
            )
        )
        execution_timeline = _execution_timeline(
            start_time=run_start_time,
            start_perf=run_start_perf,
            step_metrics=step_metrics,
        )
        return _failure_result(
            tool_name,
            "TOOL_OUTPUT_INVALID",
            message,
            source=str(tool_result.get("source") or "").strip() or "external",
            execution_timeline=execution_timeline,
            step_metrics=step_metrics,
            retry_info=_retry_info(step_metrics),
        )

    step_metrics.append(
        _step_metric(
            step_name="validate_tool_output",
            tool_name=tool_name,
            step_start_time=output_validation_start_time,
            step_end_time=_utc_now_iso(),
            step_duration_ms=_duration_ms(output_validation_start_perf),
            success=True,
        )
    )
    execution_timeline = _execution_timeline(
        start_time=run_start_time,
        start_perf=run_start_perf,
        step_metrics=step_metrics,
    )
    return _success_result(
        tool_name,
        output,
        source=str(tool_result.get("source") or "").strip() or "external",
        execution_timeline=execution_timeline,
        step_metrics=step_metrics,
        retry_info=_retry_info(step_metrics),
    )
