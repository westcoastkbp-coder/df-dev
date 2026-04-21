from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from app.execution.action_result import build_action_result
from app.execution.decision_trace import (
    build_decision_trace,
    summarize_context_reference,
)
from app.execution.vendor_router import route as route_vendor
from app.execution.browser_tool import (
    BROWSER_TOOL_ACTION,
    execute_browser_action,
    validate_browser_tool_payload,
)
from app.execution.execution_boundary import (
    ExecutionBoundaryViolationError,
    build_boundary_violation_signal,
    require_execution_boundary,
)
from app.execution.product_runtime import (
    ProductRuntimeBoundaryError,
    assert_product_runtime_action,
    assert_product_runtime_output_path,
)
from app.execution.paths import LOGS_DIR, OUTPUT_DIR, ROOT_DIR, RUNTIME_DIR, STATE_DIR
from app.policy.policy_gate import evaluate_policy
from app.product.intake import build_product_task_request
from runtime.resource_probe import get_resource_snapshot
from runtime.system_log import log_event

RUNTIME_OUT_DIR = OUTPUT_DIR
ALLOWED_DISPATCH_ACTIONS = {"WRITE_FILE", "READ_FILE", BROWSER_TOOL_ACTION}


@dataclass(slots=True)
class ProductTaskPayloadAdapter:
    task_id: str = ""
    objective: str = ""
    scope_files: list[str] | None = None
    descriptor_path: str = ""
    descriptor_action: str = ""
    user_id: str = ""
    user_role: str = ""

    def __post_init__(self) -> None:
        if self.scope_files is None:
            self.scope_files = []


SingleTaskExecutor = Callable[..., dict[str, object]]


def _error_result(
    *,
    error_type: str,
    error_message: str,
    recoverable: bool,
    task_id: str = "",
) -> dict[str, object]:
    return {
        "status": "error",
        "error_type": str(error_type).strip() or "UNKNOWN_ERROR",
        "error_message": str(error_message).strip() or "unknown error",
        "recoverable": bool(recoverable),
        "task_id": str(task_id).strip(),
    }


def _completed_result(result: dict[str, object]) -> dict[str, object]:
    updated = dict(result)
    if str(updated.get("status", "")).strip() == "error":
        return updated
    updated["status"] = "completed"
    return updated


def build_typed_action_result(
    *,
    status: str,
    action_type: str,
    result_type: str,
    result_summary: str,
    task_id: str,
    result_payload: dict[str, object] | None = None,
    decision_trace: dict[str, object] | None = None,
) -> dict[str, object]:
    payload = {"result_type": str(result_type).strip()}
    if result_payload:
        payload.update(dict(result_payload))
    normalized_status = str(status).strip() or "error"
    normalized_summary = str(result_summary).strip()
    action_result = build_action_result(
        status=normalized_status,
        action_type=str(action_type).strip(),
        result_payload=payload,
        error_code="" if normalized_status == "completed" else str(result_type).strip(),
        error_message="" if normalized_status == "completed" else normalized_summary,
        source="product_runner",
        task_id=str(task_id).strip(),
        diagnostic_message=normalized_summary,
        decision_trace=decision_trace,
    )
    compatibility_result = dict(action_result)
    compatibility_result["result_type"] = str(result_type).strip()
    compatibility_result["result_summary"] = normalized_summary
    for key, value in payload.items():
        if key == "result_type":
            continue
        compatibility_result[key] = value
    return compatibility_result


def build_resource_summary(snapshot: dict[str, object]) -> str:
    cpu = dict(snapshot.get("cpu", {}) or {})
    ram = dict(snapshot.get("ram", {}) or {})
    gpu = dict(snapshot.get("gpu", {}) or {})
    gpu_name = str(gpu.get("name", "")).strip() or "none"
    return (
        f"CPU {int(cpu.get('count', 0) or 0)} cores | "
        f"RAM {float(ram.get('available_gb', 0.0) or 0.0):.2f}/"
        f"{float(ram.get('total_gb', 0.0) or 0.0):.2f} GB available | "
        f"GPU {gpu_name} | "
        f"mode={str(snapshot.get('mode', '')).strip() or 'cpu-only'}"
    )


def build_policy_blocked_result(
    *,
    action_type: str,
    reason: str,
    task_id: str,
    policy_trace: dict[str, object],
) -> dict[str, object]:
    vendor = route_vendor(
        {"task_id": task_id},
        {"task_state": {"task_id": task_id}},
        {
            "action_type": action_type,
            "parameters": policy_trace,
        },
    )
    return build_typed_action_result(
        status="policy_blocked",
        action_type=action_type,
        result_type="POLICY_VIOLATION",
        result_summary=str(reason).strip() or "execution blocked by policy gate",
        task_id=task_id,
        result_payload={"policy_trace": dict(policy_trace)},
        decision_trace=build_decision_trace(
            reason=str(reason).strip() or "execution blocked by policy gate",
            context_used=summarize_context_reference(
                task_id=task_id,
                payload=policy_trace,
                source="product_runner.dispatch_action_trigger",
            ),
            action_type=action_type,
            policy_result=f"blocked: {str(reason).strip() or 'policy violation'}",
            confidence="high",
            vendor=vendor,
        ),
    )


def _default_output_filename() -> str:
    return "task-dispatch.log"


def _policy_task_state(task_state: dict[str, object]) -> dict[str, object]:
    normalized_task_state = dict(task_state)
    raw_status = str(normalized_task_state.get("status", "")).strip().upper()
    status_map = {
        "CREATED": "pending",
        "VALIDATED": "pending",
        "EXECUTING": "running",
        "DEFERRED": "pending",
        "COMPLETED": "completed",
        "FAILED": "failed",
    }
    normalized_task_state["status"] = status_map.get(
        raw_status, str(normalized_task_state.get("status", "")).strip()
    )
    return normalized_task_state


def _runtime_out_root_path() -> Path:
    return (ROOT_DIR / RUNTIME_OUT_DIR).resolve(strict=False)


def _runtime_fs_path(relative_path: Path) -> Path:
    normalized_path = Path(str(relative_path).replace("\\", "/"))
    return (ROOT_DIR / normalized_path).resolve(strict=False)


def _display_runtime_path(path: Path | str) -> str:
    return str(path).replace("\\", "/")


def _resolve_runtime_out_path(raw_path: object) -> Path:
    normalized = str(raw_path or "").strip().replace("\\", "/")
    if not normalized:
        return RUNTIME_OUT_DIR / "task.log"
    candidate = Path(normalized)
    if candidate.is_absolute():
        return candidate
    if not candidate.is_relative_to(RUNTIME_OUT_DIR):
        candidate = RUNTIME_OUT_DIR / candidate

    absolute_candidate = _runtime_fs_path(candidate)
    try:
        absolute_candidate.relative_to(_runtime_out_root_path())
    except ValueError:
        return absolute_candidate
    return absolute_candidate.relative_to(ROOT_DIR)


def _boundary_error_result(
    *,
    task_id: str,
    detail: str,
) -> dict[str, object]:
    vendor = route_vendor(
        {"task_id": task_id},
        {"task_state": {"task_id": task_id}},
        {"action_type": "PRODUCT_RUNTIME_BOUNDARY"},
    )
    return build_typed_action_result(
        status="error",
        action_type="PRODUCT_RUNTIME_BOUNDARY",
        result_type="runtime_boundary_blocked",
        result_summary=str(detail).strip() or "product runtime boundary blocked",
        task_id=task_id,
        decision_trace=build_decision_trace(
            reason=str(detail).strip() or "product runtime boundary blocked",
            context_used=summarize_context_reference(
                task_id=task_id,
                source="product_runner.request",
            ),
            action_type="PRODUCT_RUNTIME_BOUNDARY",
            policy_result=f"blocked: {str(detail).strip() or 'runtime boundary blocked'}",
            confidence="high",
            vendor=vendor,
        ),
    )


def _trigger_vendor(
    *,
    action_type: object,
    payload: object,
    task_state: object,
) -> str:
    return route_vendor(
        {
            "action_type": action_type,
            "payload": payload,
        },
        {
            "task_state": task_state,
        },
        {
            "action_type": action_type,
            "target": dict(payload).get("path") if isinstance(payload, dict) else "",
            "parameters": payload if isinstance(payload, dict) else {},
        },
    )


def _validate_product_action_or_error(
    *,
    action_type: object,
    descriptor_path: object,
    request_source: object,
    task_id: object,
) -> dict[str, object] | None:
    normalized_task_id = str(task_id or "").strip()
    try:
        assert_product_runtime_action(
            descriptor_action=action_type,
            descriptor_path=descriptor_path,
            request_source=request_source,
            context="product_runner.request",
        )
    except ProductRuntimeBoundaryError as exc:
        return _boundary_error_result(
            task_id=normalized_task_id,
            detail=str(exc),
        )
    return None


def build_execution_ready(payload: object) -> dict[str, object]:
    task_id = str(getattr(payload, "task_id", "") or "").strip()
    action_type = str(getattr(payload, "descriptor_action", "") or "").strip().upper()
    if action_type == BROWSER_TOOL_ACTION:
        base_payload = {
            "task_id": task_id,
            "steps": [],
        }
    else:
        target_path = _resolve_runtime_out_path(_default_output_filename())
        base_payload = {
            "task_id": task_id,
            "path": str(target_path),
            "filename": target_path.name,
        }
        if action_type == "WRITE_FILE":
            base_payload["content"] = f"task_id={task_id}\naction_type={action_type}\n"
    return {
        "task_id": task_id,
        "descriptor_path": str(getattr(payload, "descriptor_path", "") or "").strip(),
        "action_type": action_type,
        "payload": base_payload,
    }


def validate_action_trigger(trigger: object) -> dict[str, object]:
    if not isinstance(trigger, dict):
        return {"valid": False, "reason": "trigger must be a dict", "trigger": {}}

    action_type = str(trigger.get("action_type", "")).strip().upper()
    if action_type not in ALLOWED_DISPATCH_ACTIONS:
        log_event("validation", f"unknown action_type: {action_type or '(empty)'}")
        return {
            "valid": False,
            "reason": f"unknown action_type: {action_type or '(empty)'}",
            "trigger": dict(trigger),
        }

    payload = trigger.get("payload")
    if not isinstance(payload, dict):
        log_event("validation", "payload must be a dict")
        return {
            "valid": False,
            "reason": "payload must be a dict",
            "trigger": dict(trigger),
        }

    task_id = str(payload.get("task_id", "")).strip()
    if not task_id:
        log_event("validation", "payload.task_id is required")
        return {
            "valid": False,
            "reason": "payload.task_id is required",
            "trigger": dict(trigger),
        }

    if action_type == BROWSER_TOOL_ACTION:
        try:
            normalized_browser_payload = validate_browser_tool_payload(payload)
        except ValueError as exc:
            log_event("validation", str(exc))
            return {
                "valid": False,
                "reason": str(exc),
                "trigger": dict(trigger),
            }
        normalized_trigger = dict(trigger)
        normalized_trigger["action_type"] = action_type
        normalized_trigger["payload"] = normalized_browser_payload
        return {"valid": True, "reason": "", "trigger": normalized_trigger}

    raw_path = payload.get("path") or payload.get("filename")
    if not str(raw_path or "").strip():
        log_event("validation", "payload.path or payload.filename is required")
        return {
            "valid": False,
            "reason": "payload.path or payload.filename is required",
            "trigger": dict(trigger),
        }

    resolved_path = _resolve_runtime_out_path(raw_path)
    try:
        _runtime_fs_path(resolved_path).relative_to(_runtime_out_root_path())
    except ValueError:
        log_event("validation", "payload path must stay under runtime/out")
        return {
            "valid": False,
            "reason": "payload path must stay under runtime/out",
            "trigger": dict(trigger),
        }

    normalized_trigger = dict(trigger)
    normalized_payload = dict(payload)
    normalized_payload["task_id"] = task_id
    normalized_payload["path"] = str(resolved_path)
    normalized_payload["filename"] = resolved_path.name
    normalized_trigger["action_type"] = action_type
    normalized_trigger["payload"] = normalized_payload

    if (
        action_type == "WRITE_FILE"
        and not str(normalized_payload.get("content", "")).strip()
    ):
        log_event("validation", "payload.content is required for WRITE_FILE")
        return {
            "valid": False,
            "reason": "payload.content is required for WRITE_FILE",
            "trigger": normalized_trigger,
        }

    return {"valid": True, "reason": "", "trigger": normalized_trigger}


def dispatch_action_trigger(
    trigger: dict[str, object],
    *,
    task_state: dict[str, object] | None = None,
) -> dict[str, object]:
    action_type = str(trigger.get("action_type", "")).strip().upper()
    payload = dict(trigger.get("payload", {}) or {})
    task_id = str(payload.get("task_id", "")).strip()
    normalized_task_state = (
        dict(task_state)
        if isinstance(task_state, dict)
        else {"task_id": task_id, "status": ""}
    )
    vendor = _trigger_vendor(
        action_type=action_type,
        payload=payload,
        task_state=normalized_task_state,
    )
    try:
        require_execution_boundary(
            component="product_runner.dispatch_action_trigger",
            task_id=task_id,
            reason="direct_action_call_blocked",
        )
    except ExecutionBoundaryViolationError as exc:
        violation = build_boundary_violation_signal(
            task_id=task_id,
            component="product_runner.dispatch_action_trigger",
            reason=str(exc.signal.get("reason", "")),
        )
        return {
            **build_action_result(
                status="execution_boundary_violation",
                action_type=action_type,
                result_payload={"result_type": "execution_boundary_violation"},
                error_code="execution_boundary_violation",
                error_message=str(violation["reason"]),
                source="product_runner",
                task_id=task_id,
                diagnostic_message=str(violation["reason"]),
            ),
            "reason": str(violation["reason"]),
        }
    policy_result = evaluate_policy(trigger, _policy_task_state(normalized_task_state))
    if not policy_result.execution_allowed:
        log_event("validation", f"policy gate blocked action: {policy_result.reason}")
        return build_policy_blocked_result(
            action_type=action_type,
            reason=policy_result.reason,
            task_id=task_id,
            policy_trace=policy_result.policy_trace,
        )
    try:
        assert_product_runtime_action(
            descriptor_action=action_type,
            descriptor_path="",
            request_source="dispatch_action_trigger",
            context="product_runner.dispatch_action_trigger",
        )
        assert_product_runtime_output_path(
            payload.get("path") or payload.get("filename"),
            context="product_runner.dispatch_action_trigger",
        )
    except ProductRuntimeBoundaryError as exc:
        return _boundary_error_result(
            task_id=task_id,
            detail=str(exc),
        )
    if action_type == BROWSER_TOOL_ACTION:
        try:
            normalized_browser_payload = validate_browser_tool_payload(payload)
        except ValueError as exc:
            return build_typed_action_result(
                status="error",
                action_type=action_type,
                result_type="dispatch_error",
                result_summary=str(exc),
                task_id=task_id,
            )
        return execute_browser_action(normalized_browser_payload)
    target_path = _resolve_runtime_out_path(
        payload.get("path") or payload.get("filename")
    )
    filesystem_path = _runtime_fs_path(target_path)
    display_path = _display_runtime_path(target_path)
    filesystem_path.parent.mkdir(parents=True, exist_ok=True)

    if action_type == "WRITE_FILE":
        content = str(payload.get("content", ""))
        filesystem_path.write_text(content, encoding="utf-8")
        log_event("action", f"completed WRITE_FILE -> {display_path}")
        return build_typed_action_result(
            status="completed",
            action_type=action_type,
            result_type="file_written",
            result_summary=f"wrote {display_path}",
            task_id=task_id,
            result_payload={"path": str(target_path)},
            decision_trace=build_decision_trace(
                reason=f"write completed for {display_path}",
                context_used=summarize_context_reference(
                    task_id=task_id,
                    payload={"path": display_path},
                    source="product_runner.dispatch_action_trigger",
                ),
                action_type=action_type,
                policy_result="allowed: policy gate passed",
                confidence="high",
                vendor=vendor,
            ),
        )

    if action_type == "READ_FILE":
        if not filesystem_path.exists():
            log_event("validation", f"missing file: {display_path}")
            return build_typed_action_result(
                status="error",
                action_type=action_type,
                result_type="dispatch_error",
                result_summary=f"missing file: {display_path}",
                task_id=task_id,
                result_payload={"path": str(target_path)},
                decision_trace=build_decision_trace(
                    reason=f"read failed because {display_path} is missing",
                    context_used=summarize_context_reference(
                        task_id=task_id,
                        payload={"path": display_path},
                        source="product_runner.dispatch_action_trigger",
                    ),
                    action_type=action_type,
                    policy_result="allowed_then_failed: missing file",
                    confidence="medium",
                    vendor=vendor,
                ),
            )
        content = filesystem_path.read_text(encoding="utf-8").strip()
        log_event("action", f"completed READ_FILE -> {display_path}")
        return build_typed_action_result(
            status="completed",
            action_type=action_type,
            result_type="file_read",
            result_summary=f"read {display_path}: {content}",
            task_id=task_id,
            result_payload={"path": str(target_path), "content": content},
            decision_trace=build_decision_trace(
                reason=f"read completed for {display_path}",
                context_used=summarize_context_reference(
                    task_id=task_id,
                    payload={"path": display_path},
                    source="product_runner.dispatch_action_trigger",
                ),
                action_type=action_type,
                policy_result="allowed: policy gate passed",
                confidence="high",
                vendor=vendor,
            ),
        )

    return build_typed_action_result(
        status="error",
        action_type=action_type,
        result_type="dispatch_error",
        result_summary=f"unsupported action_type: {action_type or '(empty)'}",
        task_id=task_id,
    )


def dispatch_development_action(*, action_type: str, task_id: str) -> dict[str, object]:
    normalized_action = str(action_type or "").strip().upper()
    normalized_task_id = str(task_id or "").strip()
    try:
        require_execution_boundary(
            component="product_runner.dispatch_development_action",
            task_id=normalized_task_id,
            reason="direct_action_call_blocked",
        )
    except ExecutionBoundaryViolationError as exc:
        violation = build_boundary_violation_signal(
            task_id=normalized_task_id,
            component="product_runner.dispatch_development_action",
            reason=str(exc.signal.get("reason", "")),
        )
        return {
            **build_action_result(
                status="execution_boundary_violation",
                action_type=normalized_action,
                result_payload={"result_type": "execution_boundary_violation"},
                error_code="execution_boundary_violation",
                error_message=str(violation["reason"]),
                source="product_runner",
                task_id=normalized_task_id,
                diagnostic_message=str(violation["reason"]),
            ),
            "reason": str(violation["reason"]),
        }

    if normalized_action == "RUN_TESTS":
        boundary_error = _validate_product_action_or_error(
            action_type=normalized_action,
            descriptor_path="tasks/active/run-tests.yaml",
            request_source="dispatch_development_action",
            task_id=normalized_task_id,
        )
        return boundary_error or _boundary_error_result(
            task_id=normalized_task_id,
            detail="product runtime blocked RUN_TESTS",
        )

    if normalized_action == "BUILD_WEBSITE":
        boundary_error = _validate_product_action_or_error(
            action_type=normalized_action,
            descriptor_path="tasks/active/build-website.yaml",
            request_source="dispatch_development_action",
            task_id=normalized_task_id,
        )
        return boundary_error or _boundary_error_result(
            task_id=normalized_task_id,
            detail="product runtime blocked BUILD_WEBSITE",
        )

    if normalized_action == "SYSTEM_STATUS":
        boundary_error = _validate_product_action_or_error(
            action_type=normalized_action,
            descriptor_path="tasks/active/system-status.yaml",
            request_source="dispatch_development_action",
            task_id=normalized_task_id,
        )
        if boundary_error is not None:
            return boundary_error
        preview_path = ROOT_DIR / OUTPUT_DIR / "landing" / "index.html"
        checks = {
            "runtime": (ROOT_DIR / RUNTIME_DIR).exists(),
            "memory": (ROOT_DIR / STATE_DIR / "task_memory.json").exists(),
            "preview": preview_path.exists(),
        }
        summary = ", ".join(
            f"{name}={'ok' if ok else 'missing'}" for name, ok in checks.items()
        )
        if all(checks.values()):
            log_event("action", f"completed SYSTEM_STATUS -> {summary}")
            return build_typed_action_result(
                status="completed",
                action_type=normalized_action,
                result_type="system_status",
                result_summary=summary,
                task_id=normalized_task_id,
                result_payload={"checks": checks},
            )
        log_event("validation", f"SYSTEM_STATUS failed: {summary}")
        return build_typed_action_result(
            status="error",
            action_type=normalized_action,
            result_type="system_status",
            result_summary=summary,
            task_id=normalized_task_id,
            result_payload={"checks": checks},
        )

    if normalized_action == "RESOURCES":
        boundary_error = _validate_product_action_or_error(
            action_type=normalized_action,
            descriptor_path="tasks/active/resources.yaml",
            request_source="dispatch_development_action",
            task_id=normalized_task_id,
        )
        if boundary_error is not None:
            return boundary_error
        try:
            snapshot = get_resource_snapshot()
        except Exception as exc:
            summary = str(exc).strip() or "resource check failed"
            log_event("validation", f"RESOURCES failed: {summary}")
            return build_typed_action_result(
                status="error",
                action_type=normalized_action,
                result_type="resource_snapshot",
                result_summary=summary,
                task_id=normalized_task_id,
            )
        summary = build_resource_summary(snapshot)
        log_event("action", f"completed RESOURCES -> {summary}")
        return build_typed_action_result(
            status="completed",
            action_type=normalized_action,
            result_type="resource_snapshot",
            result_summary=summary,
            task_id=normalized_task_id,
            result_payload={"resource_snapshot": snapshot},
        )

    log_event(
        "validation",
        f"unsupported development action: {normalized_action or '(empty)'}",
    )
    return build_typed_action_result(
        status="error",
        action_type=normalized_action,
        result_type="dispatch_error",
        result_summary=f"unsupported action_type: {normalized_action or '(empty)'}",
        task_id=normalized_task_id,
    )


def _payload_from_request(
    request: dict[str, object],
    *,
    user_id: str,
    user_role: str,
) -> ProductTaskPayloadAdapter:
    return ProductTaskPayloadAdapter(
        task_id=str(request.get("task_id", "")).strip(),
        objective=str(request.get("objective", "")).strip(),
        scope_files=[
            str(item).strip()
            for item in list(request.get("scope_files", []) or [])
            if str(item).strip()
        ],
        descriptor_path=str(request.get("descriptor_path", "")).strip(),
        descriptor_action=str(request.get("descriptor_action", "")).strip(),
        user_id=user_id,
        user_role=user_role,
    )


def _composed_step_result(
    request: dict[str, object],
    *,
    request_source: str,
) -> dict[str, object]:
    task_id = str(request.get("task_id", "")).strip()
    descriptor_path = str(request.get("descriptor_path", "")).strip()
    descriptor_action = str(request.get("descriptor_action", "")).strip()
    return _completed_result(
        {
            "task_id": task_id,
            "selected_agent": "orchestrated",
            "acceptance_status": "orchestrated",
            "validation_status": "orchestrated",
            "lifecycle_state": "completed",
            "lifecycle_history": ("created", "running", "completed"),
            "objective": str(request.get("objective", "")).strip(),
            "system_status_text": "orchestrated",
            "user_summary": task_id,
            "system_report": {
                "descriptor_path": descriptor_path,
                "descriptor_action": descriptor_action,
                "scope_files": list(request.get("scope_files", []) or []),
                "request_source": str(request_source).strip() or "api",
                "api_called": True,
                "lifecycle_state": "completed",
                "attempted": descriptor_action == "CONTROL_HEALTH",
            },
        }
    )


def execute_product_task_request(
    payload: object,
    *,
    principal: dict[str, str],
    request_source: str,
    execute_single: SingleTaskExecutor,
) -> dict[str, object] | list[dict[str, object]]:
    request = build_product_task_request(payload)
    if str(request.get("status", "")).strip() == "error":
        return _error_result(
            error_type=str(request.get("error_type", "")).strip(),
            error_message=str(request.get("error_message", "")).strip(),
            recoverable=bool(request.get("recoverable", False)),
            task_id=str(request.get("task_id", "")).strip(),
        )
    user_id = str(getattr(payload, "user_id", "") or "").strip()
    user_role = str(getattr(payload, "user_role", "") or "").strip()

    if str(request.get("action", "")).strip() == "execute_chain":
        results: list[dict[str, object]] = []
        for task_request in list(request.get("requests", []) or []):
            normalized_task_request = dict(task_request)
            boundary_error = _validate_product_action_or_error(
                action_type=normalized_task_request.get("descriptor_action", ""),
                descriptor_path=normalized_task_request.get("descriptor_path", ""),
                request_source=request_source,
                task_id=normalized_task_request.get("task_id", ""),
            )
            if boundary_error is not None:
                results.append(boundary_error)
                continue
            try:
                results.append(
                    _completed_result(
                        execute_single(
                            _payload_from_request(
                                normalized_task_request,
                                user_id=user_id,
                                user_role=user_role,
                            ),
                            principal=principal,
                            request_source=request_source,
                        )
                    )
                )
            except Exception as exc:
                results.append(
                    _error_result(
                        error_type="UNKNOWN_ERROR",
                        error_message=str(exc),
                        recoverable=False,
                        task_id=str(normalized_task_request.get("task_id", "")).strip(),
                    )
                )
        return results

    boundary_error = _validate_product_action_or_error(
        action_type=request.get("descriptor_action", ""),
        descriptor_path=request.get("descriptor_path", ""),
        request_source=request_source,
        task_id=request.get("task_id", ""),
    )
    if boundary_error is not None:
        return boundary_error

    normalized_payload = _payload_from_request(
        request,
        user_id=user_id,
        user_role=user_role,
    )
    try:
        return _completed_result(
            execute_single(
                normalized_payload,
                principal=principal,
                request_source=request_source,
            )
        )
    except Exception as exc:
        return _error_result(
            error_type="UNKNOWN_ERROR",
            error_message=str(exc),
            recoverable=False,
            task_id=str(getattr(normalized_payload, "task_id", "")).strip(),
        )
