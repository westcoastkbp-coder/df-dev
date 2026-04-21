from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator

from app.execution.paths import LOGS_DIR, ROOT_DIR
from runtime.system_log import log_event, write_json_log


EXECUTION_BOUNDARY_VIOLATIONS_LOG = (
    ROOT_DIR / LOGS_DIR / "execution_boundary_violations.jsonl"
)
_CURRENT_EXECUTION_SCOPE: ContextVar["ExecutionScope | None"] = ContextVar(
    "df_execution_scope",
    default=None,
)


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


@dataclass(frozen=True, slots=True)
class ExecutionScope:
    task_id: str
    intent: str
    policy_validated: bool


class ExecutionBoundaryViolationError(RuntimeError):
    def __init__(
        self,
        *,
        task_id: str,
        component: str,
        reason: str,
    ) -> None:
        self.signal = build_boundary_violation_signal(
            task_id=task_id,
            component=component,
            reason=reason,
        )
        super().__init__(str(self.signal["reason"]))


def build_boundary_violation_signal(
    *,
    task_id: object = "",
    component: object = "",
    reason: object = "direct_action_call_blocked",
) -> dict[str, object]:
    normalized_task_id = _normalize_text(task_id)
    normalized_component = _normalize_text(component)
    normalized_reason = _normalize_text(reason) or "direct_action_call_blocked"
    signal = {
        "status": "execution_boundary_violation",
        "reason": normalized_reason,
    }
    if normalized_task_id:
        signal["task_id"] = normalized_task_id
    if normalized_component:
        signal["component"] = normalized_component
    return signal


def _append_violation_log(entry: dict[str, object]) -> None:
    normalized_entry = dict(entry)
    write_json_log(
        EXECUTION_BOUNDARY_VIOLATIONS_LOG,
        task_id=normalized_entry.get("task_id", ""),
        event_type="execution_boundary_violation",
        status=normalized_entry.get("status", "execution_boundary_violation"),
        details=normalized_entry,
    )


def log_execution_boundary_violation(
    *,
    task_id: object = "",
    component: object = "",
    reason: object = "direct_action_call_blocked",
    scope: ExecutionScope | None = None,
) -> dict[str, object]:
    signal = build_boundary_violation_signal(
        task_id=task_id,
        component=component,
        reason=reason,
    )
    entry = {
        "timestamp": _timestamp(),
        **signal,
    }
    if scope is not None and scope.intent:
        entry["intent"] = scope.intent
    _append_violation_log(entry)
    log_event(
        "validation",
        signal,
        task_id=signal.get("task_id", ""),
        status="execution_boundary_violation",
    )
    return signal


def current_execution_scope() -> ExecutionScope | None:
    return _CURRENT_EXECUTION_SCOPE.get()


@contextmanager
def execution_boundary(
    task_data: dict[str, object],
    *,
    policy_validated: bool,
) -> Iterator[ExecutionScope]:
    scope = ExecutionScope(
        task_id=_normalize_text(task_data.get("task_id")),
        intent=_normalize_text(task_data.get("intent")),
        policy_validated=bool(policy_validated),
    )
    token: Token[ExecutionScope | None] = _CURRENT_EXECUTION_SCOPE.set(scope)
    try:
        yield scope
    finally:
        _CURRENT_EXECUTION_SCOPE.reset(token)


def require_execution_boundary(
    *,
    component: str,
    task_id: object = "",
    reason: str = "direct_action_call_blocked",
) -> ExecutionScope:
    scope = current_execution_scope()
    normalized_task_id = _normalize_text(task_id)
    if scope is None:
        log_execution_boundary_violation(
            task_id=normalized_task_id,
            component=component,
            reason=reason,
        )
        raise ExecutionBoundaryViolationError(
            task_id=normalized_task_id,
            component=component,
            reason=reason,
        )
    if not scope.policy_validated:
        log_execution_boundary_violation(
            task_id=normalized_task_id or scope.task_id,
            component=component,
            reason="execution_without_policy_blocked",
            scope=scope,
        )
        raise ExecutionBoundaryViolationError(
            task_id=normalized_task_id or scope.task_id,
            component=component,
            reason="execution_without_policy_blocked",
        )
    if not scope.task_id:
        log_execution_boundary_violation(
            task_id=normalized_task_id,
            component=component,
            reason="execution_without_task_blocked",
            scope=scope,
        )
        raise ExecutionBoundaryViolationError(
            task_id=normalized_task_id,
            component=component,
            reason="execution_without_task_blocked",
        )
    if normalized_task_id and scope.task_id != normalized_task_id:
        log_execution_boundary_violation(
            task_id=normalized_task_id,
            component=component,
            reason="execution_task_context_mismatch",
            scope=scope,
        )
        raise ExecutionBoundaryViolationError(
            task_id=normalized_task_id,
            component=component,
            reason="execution_task_context_mismatch",
        )
    return scope
