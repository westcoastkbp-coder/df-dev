from __future__ import annotations

import sys
from collections.abc import Mapping

from app.execution.paths import LOGS_DIR, ROOT_DIR
from app.execution.task_schema import TaskEvent
from runtime.system_log import write_json_log

TASK_LIFECYCLE_LOG_FILE = ROOT_DIR / LOGS_DIR / "task_lifecycle.jsonl"
TASK_STATUSES = (
    "AWAITING_APPROVAL",
    "CREATED",
    "VALIDATED",
    "EXECUTING",
    "COMPLETED",
    "FAILED",
    "DEFERRED",
)
TASK_STATUS_ALIASES = {
    "AWAITING_APPROVAL": "AWAITING_APPROVAL",
    "awaiting_approval": "AWAITING_APPROVAL",
    "CREATED": "CREATED",
    "created": "CREATED",
    "VALIDATED": "VALIDATED",
    "validated": "VALIDATED",
    "confirmed": "VALIDATED",
    "pending": "VALIDATED",
    "EXECUTING": "EXECUTING",
    "executing": "EXECUTING",
    "running": "EXECUTING",
    "COMPLETED": "COMPLETED",
    "completed": "COMPLETED",
    "FAILED": "FAILED",
    "failed": "FAILED",
    "DEFERRED": "DEFERRED",
    "deferred": "DEFERRED",
}
OPEN_TASK_STATUSES = {
    "AWAITING_APPROVAL",
    "CREATED",
    "VALIDATED",
    "EXECUTING",
    "DEFERRED",
}
TASK_STATUS_TRANSITIONS = {
    "AWAITING_APPROVAL": {"VALIDATED", "FAILED"},
    "CREATED": {"AWAITING_APPROVAL", "VALIDATED", "FAILED"},
    "VALIDATED": {"EXECUTING", "FAILED"},
    "EXECUTING": {"COMPLETED", "FAILED", "DEFERRED"},
    "COMPLETED": set(),
    "FAILED": set(),
    "DEFERRED": {"EXECUTING", "FAILED"},
}


class InvalidTaskStateTransitionError(ValueError):
    def __init__(
        self, *, task_id: object, from_state: object, to_state: object
    ) -> None:
        normalized_task_id = str(task_id or "").strip()
        normalized_from_state = normalize_task_status(from_state)
        normalized_to_state = normalize_task_status(to_state)
        self.signal = {
            "status": "invalid_state_transition",
            "task_id": normalized_task_id,
            "from": normalized_from_state,
            "to": normalized_to_state,
        }
        super().__init__(
            "invalid task status transition: "
            f"{normalized_from_state} -> {normalized_to_state}"
        )


def _append_lifecycle_log(entry: dict[str, object]) -> None:
    normalized_entry = dict(entry)
    write_json_log(
        TASK_LIFECYCLE_LOG_FILE,
        task_id=str(normalized_entry.get("task_id", "")).strip(),
        event_type=str(
            normalized_entry.get("phase")
            or normalized_entry.get("event_type")
            or "task_lifecycle"
        ).strip(),
        status=str(normalized_entry.get("result", "")).strip() or "observed",
        details=normalized_entry,
    )


def _caller_name() -> str:
    frame = sys._getframe(2)
    while frame is not None:
        module_name = str(frame.f_globals.get("__name__", "") or "").strip()
        if module_name == __name__:
            frame = frame.f_back
            continue
        function_name = str(frame.f_code.co_name or "").strip()
        if module_name:
            return f"{module_name}.{function_name}"
        if function_name:
            return function_name
        frame = frame.f_back
    return "unknown"


def _transition_step_index(task_data: Mapping[str, object]) -> int:
    return len(list(task_data.get("history", []))) + 1


def _log_transition_phase(
    *,
    phase: str,
    task_data: Mapping[str, object],
    from_state: object,
    to_state: object,
    result: str,
) -> None:
    _append_lifecycle_log(
        {
            "phase": str(phase).strip(),
            "task_id": str(task_data.get("task_id", "")).strip(),
            "from_state": normalize_task_status(from_state),
            "to_state": normalize_task_status(to_state),
            "caller": _caller_name(),
            "step_index": _transition_step_index(task_data),
            "result": str(result).strip(),
        }
    )


def normalize_task_status(value: object) -> str:
    normalized = str(value or "").strip()
    return TASK_STATUS_ALIASES.get(normalized, "")


def validate_transition(from_state: object, to_state: object) -> bool:
    normalized_from = normalize_task_status(from_state)
    normalized_to = normalize_task_status(to_state)
    if not normalized_from or not normalized_to:
        return False
    if normalized_from == normalized_to:
        return False
    return normalized_to in TASK_STATUS_TRANSITIONS.get(normalized_from, set())


def can_transition_task_status(current_status: object, next_status: object) -> bool:
    return validate_transition(current_status, next_status)


def record_task_event(
    task_data: dict[str, object],
    *,
    timestamp: str,
    event: str,
    from_status: object = "",
    to_status: object = "",
    details: object = None,
) -> dict[str, object]:
    normalized_details: object
    if isinstance(details, Mapping):
        normalized_details = dict(details)
    elif details is None:
        normalized_details = {}
    else:
        normalized_details = {"message": details}

    if isinstance(normalized_details, dict):
        normalized_details.setdefault(
            "interaction_id",
            str(task_data.get("interaction_id", "")).strip(),
        )
        normalized_details.setdefault(
            "job_id",
            str(task_data.get("job_id") or task_data.get("task_id", "")).strip(),
        )
        normalized_details.setdefault(
            "trace_id",
            str(task_data.get("trace_id") or task_data.get("task_id", "")).strip(),
        )

    history = list(task_data.get("history", []))
    history.append(
        TaskEvent(
            timestamp=str(timestamp).strip(),
            event=str(event).strip() or "updated",
            from_status=str(from_status or "").strip(),
            to_status=str(to_status or "").strip(),
            details=normalized_details,
        ).as_dict()
    )
    task_data["history"] = history
    task_data["last_updated_at"] = str(timestamp).strip()
    return task_data


def set_task_state(
    task_data: dict[str, object],
    new_state: object,
    *,
    timestamp: str,
    details: object = None,
) -> dict[str, object]:
    current_state = normalize_task_status(task_data.get("status"))
    normalized_next = normalize_task_status(new_state)
    task_id = task_data.get("task_id")

    if not current_state:
        _log_transition_phase(
            phase="before_transition",
            task_data=task_data,
            from_state=task_data.get("status"),
            to_state=new_state,
            result="invalid_current_state",
        )
        raise InvalidTaskStateTransitionError(
            task_id=task_id,
            from_state=task_data.get("status"),
            to_state=new_state,
        )
    if not normalized_next:
        _log_transition_phase(
            phase="before_transition",
            task_data=task_data,
            from_state=current_state,
            to_state=new_state,
            result="invalid_next_state",
        )
        raise InvalidTaskStateTransitionError(
            task_id=task_id,
            from_state=current_state,
            to_state=new_state,
        )
    _log_transition_phase(
        phase="before_transition",
        task_data=task_data,
        from_state=current_state,
        to_state=normalized_next,
        result="pending",
    )
    if not validate_transition(current_state, normalized_next):
        _log_transition_phase(
            phase="after_transition",
            task_data=task_data,
            from_state=current_state,
            to_state=normalized_next,
            result="rejected",
        )
        _append_lifecycle_log(
            {
                "task_id": str(task_id or "").strip(),
                "from_state": current_state,
                "to_state": normalized_next,
                "result": "rejected",
            }
        )
        raise InvalidTaskStateTransitionError(
            task_id=task_id,
            from_state=current_state,
            to_state=normalized_next,
        )

    task_data["status"] = normalized_next
    if normalized_next == "EXECUTING":
        task_data["started_at"] = str(timestamp).strip()
    elif normalized_next == "COMPLETED":
        task_data["completed_at"] = str(timestamp).strip()
        task_data["error"] = ""
    elif normalized_next == "FAILED":
        task_data["failed_at"] = str(timestamp).strip()
        task_data["result"] = None
    _log_transition_phase(
        phase="after_transition",
        task_data=task_data,
        from_state=current_state,
        to_state=normalized_next,
        result="allowed",
    )
    _append_lifecycle_log(
        {
            "task_id": str(task_id or "").strip(),
            "from_state": current_state,
            "to_state": normalized_next,
            "result": "allowed",
        }
    )
    task_data["last_updated_at"] = str(timestamp).strip()

    return record_task_event(
        task_data,
        timestamp=str(timestamp).strip(),
        event="status_transition",
        from_status=current_state,
        to_status=normalized_next,
        details=details,
    )


def transition_task_status(
    task_data: dict[str, object],
    next_status: object,
    *,
    timestamp: str,
    details: object = None,
) -> dict[str, object]:
    return set_task_state(
        task_data,
        next_status,
        timestamp=timestamp,
        details=details,
    )


def task_history_snapshot(task_data: Mapping[str, object]) -> list[dict[str, object]]:
    return [dict(item) for item in task_data.get("history", [])]
