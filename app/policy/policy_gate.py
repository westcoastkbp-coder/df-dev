from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Mapping

from app.execution.lead_estimate_contract import (
    WORKFLOW_TYPE as LEAD_ESTIMATE_WORKFLOW_TYPE,
    validate_decision_contract,
    validate_input_payload,
)
from app.execution.paths import LOGS_DIR, ROOT_DIR
from app.execution.task_schema import validate_task_contract, validate_task_lineage
from app.orchestrator.task_lifecycle import OPEN_TASK_STATUSES, normalize_task_status
from runtime.system_log import write_json_log


ALLOWED_ACTION_TYPES = {"WRITE_FILE", "READ_FILE", "BROWSER_TOOL"}
ALLOWED_EXTERNAL_ACTION_TYPES = {
    "SEND_SMS",
    "MAKE_CALL",
    "SEND_EMAIL",
    "API_REQUEST",
    "OPENAI_REQUEST",
    "BROWSER_ACTION",
    "EMAIL_ACTION",
    "PRINT_DOCUMENT",
}
ALLOWED_EXTERNAL_ACTION_DESTINATIONS = {
    "SEND_SMS": {"sms_gateway"},
    "MAKE_CALL": {"phone_gateway"},
    "SEND_EMAIL": {"gmail_gateway"},
    "API_REQUEST": {"estimate_service"},
    "OPENAI_REQUEST": {"openai"},
    "BROWSER_ACTION": {"browser", "browser_adapter"},
    "EMAIL_ACTION": {"email", "email_adapter"},
    "PRINT_DOCUMENT": {"printer", "printer_adapter"},
}
ALLOWED_TASK_STATUSES = {"pending", "running"}
REQUIRED_DESCRIPTOR_FIELDS = ("action_type", "payload")
REQUIRED_PAYLOAD_FIELDS = ("task_id",)
POLICY_LOG_FILE = ROOT_DIR / LOGS_DIR / "policy.log"
CORE_ZONE_FILES = frozenset(
    {
        "app/orchestrator/execution_runner.py",
        "app/orchestrator/task_lifecycle.py",
        "app/policy/policy_gate.py",
        "app/execution/execution_boundary.py",
    }
)
SYSTEM_IMPROVEMENT_INTENT = "system_improvement_task"
POLICY_ACTION_TYPES = {"read", "write", "external", "critical"}
CRITICAL_CONFIRMATION_FIELDS = (
    "confirmed",
    "confirmation_received",
    "confirmation_granted",
    "policy_confirmation",
    "policy_confirmed",
)
EXTERNAL_MUTATION_HINTS = (
    "create",
    "update",
    "delete",
    "submit",
    "send",
    "write",
    "modify",
    "patch",
    "post",
    "put",
    "approve",
    "reject",
)


@dataclass(frozen=True, slots=True)
class PolicyResult:
    execution_allowed: bool
    reason: str
    policy_trace: dict[str, object]


@dataclass(frozen=True, slots=True)
class PolicyAction:
    action_name: str
    action_type: str
    payload: dict[str, object]
    task_id: str
    confirmation_required: bool
    confirmation_received: bool


def _normalize_mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_list(value: object) -> list[object]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    return []


def _normalize_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return _normalize_text(value).lower() in {"true", "1", "yes"}


def _first_non_empty(*values: object) -> str:
    for value in values:
        normalized = _normalize_text(value)
        if normalized:
            return normalized
    return ""


def _sort_key(value: object) -> str:
    if isinstance(value, Mapping):
        mapped = _normalize_mapping(value)
        return "|".join(
            [
                _first_non_empty(mapped.get("id"), mapped.get("resource_id")),
                _first_non_empty(mapped.get("type"), mapped.get("resource_type")),
                _first_non_empty(
                    mapped.get("availability"), mapped.get("resource_availability")
                ),
            ]
        )
    return _normalize_text(value)


def _decision_summary(task_data: object) -> dict[str, str]:
    task = _normalize_mapping(task_data)
    payload = _normalize_mapping(task.get("payload"))
    decision = _normalize_mapping(payload.get("decision"))
    actions = _normalize_list(decision.get("recommended_actions"))
    primary_action = _first_non_empty(
        payload.get("recommended_action"),
        payload.get("action"),
        actions[0] if actions else "",
        decision.get("action"),
    )
    return {
        "task_id": _normalize_text(task.get("task_id")),
        "status": normalize_task_status(task.get("status")),
        "decision_id": _first_non_empty(
            decision.get("decision_id"), payload.get("decision_id")
        ),
        "decision_type": _first_non_empty(
            decision.get("decision_type"),
            payload.get("decision_type"),
        ),
        "domain": _first_non_empty(
            payload.get("domain"),
            decision.get("domain"),
            payload.get("signal_type"),
            task.get("task_type"),
            task.get("intent"),
        ),
        "resource_id": _first_non_empty(
            payload.get("resource_id"),
            decision.get("resource_id"),
            payload.get("lead_id"),
            task.get("source_lead_id"),
        ),
        "priority": _first_non_empty(
            payload.get("priority"), decision.get("priority")
        ).lower(),
        "action": primary_action.lower(),
    }


def _has_decision_lock_scope(task_data: object) -> bool:
    task = _normalize_mapping(task_data)
    payload = _normalize_mapping(task.get("payload"))
    decision = _normalize_mapping(payload.get("decision"))
    return any(
        _normalize_text(value)
        for value in (
            decision.get("decision_id"),
            payload.get("decision_id"),
            decision.get("decision_type"),
            payload.get("decision_type"),
            payload.get("domain"),
            decision.get("domain"),
            payload.get("signal_type"),
            payload.get("recommended_action"),
            payload.get("action"),
            decision.get("action"),
            payload.get("resource_id"),
            decision.get("resource_id"),
            payload.get("lead_id"),
            task.get("source_lead_id"),
        )
    )


def _resource_summary(task_data: object) -> dict[str, object]:
    task = _normalize_mapping(task_data)
    payload = _normalize_mapping(task.get("payload"))
    resource = _normalize_mapping(payload.get("resource"))
    has_explicit_resource_model = bool(resource) or any(
        key in payload
        for key in (
            "resource_type",
            "resource_availability",
            "current_load",
            "max_load",
            "schedule_slot",
            "scheduled_for",
        )
    )
    availability = _first_non_empty(
        resource.get("availability"),
        payload.get("resource_availability"),
    ).lower()
    resource_type = _first_non_empty(
        resource.get("type"),
        payload.get("resource_type"),
    ).lower()
    resource_id = _first_non_empty(
        resource.get("id"),
        payload.get("resource_id"),
    )
    current_load = resource.get("current_load", payload.get("current_load", 0))
    max_load = resource.get("max_load", payload.get("max_load", 1))
    schedule_slot = _first_non_empty(
        payload.get("schedule_slot"),
        payload.get("scheduled_for"),
    )
    return {
        "task_id": _normalize_text(task.get("task_id")),
        "status": normalize_task_status(task.get("status")),
        "id": resource_id if has_explicit_resource_model else "",
        "type": resource_type,
        "availability": availability,
        "current_load": int(current_load) if str(current_load).strip() else 0,
        "max_load": int(max_load) if str(max_load).strip() else 1,
        "schedule_slot": schedule_slot,
    }


def _resource_from_candidate(candidate: object) -> dict[str, object]:
    candidate_map = _normalize_mapping(candidate)
    resource_id = _first_non_empty(
        candidate_map.get("id"), candidate_map.get("resource_id")
    )
    resource_type = _first_non_empty(
        candidate_map.get("type"), candidate_map.get("resource_type")
    ).lower()
    availability = _first_non_empty(
        candidate_map.get("availability"),
        candidate_map.get("resource_availability"),
        "available",
    ).lower()
    current_load = candidate_map.get("current_load", 0)
    max_load = candidate_map.get("max_load", 1)
    return {
        "id": resource_id,
        "type": resource_type,
        "availability": availability,
        "current_load": int(current_load) if str(current_load).strip() else 0,
        "max_load": int(max_load) if str(max_load).strip() else 1,
    }


def _resource_candidates(task_data: object) -> list[dict[str, object]]:
    task = _normalize_mapping(task_data)
    payload = _normalize_mapping(task.get("payload"))
    candidates = [
        _resource_from_candidate(candidate)
        for candidate in sorted(
            _normalize_list(payload.get("candidate_resources")), key=_sort_key
        )
    ]
    contractors = [
        _resource_from_candidate(candidate)
        for candidate in sorted(
            _normalize_list(payload.get("alternative_contractors")), key=_sort_key
        )
    ]
    return [
        candidate
        for candidate in candidates + contractors
        if _normalize_text(candidate.get("id"))
    ]


def _time_slot_candidates(task_data: object) -> list[str]:
    task = _normalize_mapping(task_data)
    payload = _normalize_mapping(task.get("payload"))
    return [
        _normalize_text(slot)
        for slot in sorted(
            _normalize_list(payload.get("candidate_time_slots")), key=_sort_key
        )
        if _normalize_text(slot)
    ]


def _contradictory_actions() -> set[frozenset[str]]:
    return {
        frozenset({"approve", "reject"}),
        frozenset({"start", "stop"}),
        frozenset({"enable", "disable"}),
        frozenset({"send_email", "do_not_send_email"}),
        frozenset({"make_call", "do_not_call"}),
        frozenset({"request_more_reviews", "pause_review_requests"}),
    }


def _find_decision_conflict(
    new_task: dict[str, object],
    *,
    existing_tasks: list[dict[str, object]],
) -> str:
    if not _has_decision_lock_scope(new_task):
        return ""
    new_summary = _decision_summary(new_task)
    if not any(
        new_summary.get(field)
        for field in (
            "decision_id",
            "decision_type",
            "resource_id",
            "priority",
            "action",
        )
    ):
        return ""

    for existing_task in existing_tasks:
        if not _has_decision_lock_scope(existing_task):
            continue
        existing_summary = _decision_summary(existing_task)
        if existing_summary["status"] not in OPEN_TASK_STATUSES:
            continue
        if existing_summary["task_id"] == new_summary["task_id"]:
            continue

        if (
            new_summary["resource_id"]
            and existing_summary["resource_id"]
            and new_summary["resource_id"] == existing_summary["resource_id"]
        ):
            return (
                "decision conflict flagged: resource conflict with active decision "
                f"{existing_summary['task_id']}"
            )

        if (
            new_summary["priority"]
            and existing_summary["priority"]
            and new_summary["domain"]
            and new_summary["domain"] == existing_summary["domain"]
            and new_summary["priority"] != existing_summary["priority"]
        ):
            return (
                "decision conflict flagged: priority conflict with active decision "
                f"{existing_summary['task_id']}"
            )

        if (
            new_summary["action"]
            and existing_summary["action"]
            and frozenset({new_summary["action"], existing_summary["action"]})
            in _contradictory_actions()
        ):
            same_domain = (
                not new_summary["domain"]
                or not existing_summary["domain"]
                or new_summary["domain"] == existing_summary["domain"]
            )
            same_resource = (
                not new_summary["resource_id"]
                or not existing_summary["resource_id"]
                or new_summary["resource_id"] == existing_summary["resource_id"]
            )
            if same_domain and same_resource:
                return (
                    "decision conflict flagged: contradictory actions with active decision "
                    f"{existing_summary['task_id']}"
                )

    return ""


def _find_resource_conflict(
    new_task: dict[str, object],
    *,
    existing_tasks: list[dict[str, object]],
) -> tuple[str, str]:
    new_resource = _resource_summary(new_task)
    if not new_resource["id"]:
        return ("", "")

    if new_resource["type"] not in {"contractor", "crew"}:
        return ("resource_conflict", "")

    if new_resource["availability"] and new_resource["availability"] != "available":
        return ("resource_conflict", "")

    if new_resource["current_load"] >= new_resource["max_load"]:
        return ("resource_conflict", "")

    for existing_task in existing_tasks:
        existing_resource = _resource_summary(existing_task)
        if existing_resource["status"] not in OPEN_TASK_STATUSES:
            continue
        if existing_resource["task_id"] == new_resource["task_id"]:
            continue
        if existing_resource["id"] != new_resource["id"]:
            continue
        if (
            new_resource["schedule_slot"]
            and existing_resource["schedule_slot"]
            and new_resource["schedule_slot"] == existing_resource["schedule_slot"]
        ):
            return ("schedule_conflict", _normalize_text(existing_resource["task_id"]))

    return ("", "")


def suggest_alternative(
    new_task: dict[str, object],
    *,
    existing_tasks: list[dict[str, object]],
    conflict_type: str,
) -> dict[str, object]:
    normalized_conflict_type = _normalize_text(conflict_type)
    new_resource = _resource_summary(new_task)
    schedule_slot = _normalize_text(new_resource.get("schedule_slot"))

    for candidate in _resource_candidates(new_task):
        candidate_id = _normalize_text(candidate.get("id"))
        if not candidate_id:
            continue
        if _normalize_text(candidate.get("availability")).lower() != "available":
            continue
        if int(candidate.get("current_load", 0)) >= int(candidate.get("max_load", 1)):
            continue
        has_schedule_conflict = False
        for existing_task in existing_tasks:
            existing_resource = _resource_summary(existing_task)
            if existing_resource["status"] not in OPEN_TASK_STATUSES:
                continue
            if _normalize_text(existing_resource["id"]) != candidate_id:
                continue
            if (
                schedule_slot
                and _normalize_text(existing_resource["schedule_slot"]) == schedule_slot
            ):
                has_schedule_conflict = True
                break
        if has_schedule_conflict:
            continue
        option_kind = (
            "alternative_contractor"
            if _normalize_text(candidate.get("type")) == "contractor"
            else "next_available_resource"
        )
        return {
            "option_type": option_kind,
            "resource": {
                "id": candidate_id,
                "type": _normalize_text(candidate.get("type")),
            },
        }

    if (
        normalized_conflict_type in {"resource_conflict", "schedule_conflict"}
        and new_resource["id"]
    ):
        for slot in _time_slot_candidates(new_task):
            has_schedule_conflict = False
            for existing_task in existing_tasks:
                existing_resource = _resource_summary(existing_task)
                if existing_resource["status"] not in OPEN_TASK_STATUSES:
                    continue
                if _normalize_text(existing_resource["id"]) != _normalize_text(
                    new_resource["id"]
                ):
                    continue
                if _normalize_text(existing_resource["schedule_slot"]) == slot:
                    has_schedule_conflict = True
                    break
            if has_schedule_conflict:
                continue
            return {
                "option_type": "next_time_slot",
                "resource": {
                    "id": _normalize_text(new_resource["id"]),
                    "type": _normalize_text(new_resource["type"]),
                },
                "schedule_slot": slot,
            }

    return {}


def _task_id_from(descriptor: dict[str, object], task_state: dict[str, object]) -> str:
    payload = _normalize_mapping(descriptor.get("payload"))
    return (
        str(payload.get("task_id", "")).strip()
        or str(task_state.get("task_id", "")).strip()
        or "(unknown)"
    )


def _normalized_repo_path(value: object) -> str:
    normalized = _normalize_text(value).replace("\\", "/").lstrip("./").lower()
    if len(normalized) >= 2 and normalized[1] == ":":
        marker = "/app/"
        index = normalized.find(marker)
        if index >= 0:
            normalized = normalized[index + 1 :]
    return normalized


def _system_improvement_targets(task_data: Mapping[str, object]) -> list[str]:
    payload = _normalize_mapping(task_data.get("payload"))
    candidates: list[object] = []
    for key in ("affected_files", "target_files", "target_file", "module_path", "path"):
        value = payload.get(key)
        if isinstance(value, (list, tuple)):
            candidates.extend(value)
        elif _normalize_text(value):
            candidates.append(value)
    return [
        normalized
        for normalized in (_normalized_repo_path(value) for value in candidates)
        if normalized
    ]


def _core_zone_impact(task_data: Mapping[str, object]) -> tuple[bool, list[str]]:
    impacted = sorted(
        {
            target
            for target in _system_improvement_targets(task_data)
            if target in CORE_ZONE_FILES
        }
    )
    return (bool(impacted), impacted)


def _write_policy_log(task_id: str, *, allowed: bool, reason: str) -> None:
    decision = "allowed" if allowed else "blocked"
    normalized_reason = str(reason).strip() or "-"
    write_json_log(
        POLICY_LOG_FILE,
        task_id=task_id,
        event_type="policy_decision",
        status=decision,
        details={"reason": normalized_reason},
    )


def record_policy_decision(task_id: str, *, allowed: bool, reason: str) -> None:
    _write_policy_log(
        str(task_id or "").strip() or "(unknown)", allowed=allowed, reason=reason
    )


def _return_result(
    *,
    descriptor: dict[str, object],
    task_state: dict[str, object],
    execution_allowed: bool,
    reason: str,
    policy_trace: dict[str, object],
) -> PolicyResult:
    task_id = _task_id_from(descriptor, task_state)
    _write_policy_log(task_id, allowed=execution_allowed, reason=reason)
    return PolicyResult(
        execution_allowed=execution_allowed,
        reason=reason,
        policy_trace=policy_trace,
    )


def _confirmation_received(*values: object) -> bool:
    for value in values:
        normalized = _normalize_mapping(value)
        if not normalized:
            continue
        for field_name in CRITICAL_CONFIRMATION_FIELDS:
            if _normalize_bool(normalized.get(field_name)):
                return True
        if _normalize_text(normalized.get("approval_status")).lower() == "approved":
            return True
    return False


def _browser_policy_action_type(payload: object) -> str:
    normalized_payload = _normalize_mapping(payload)
    steps = _normalize_list(normalized_payload.get("steps"))
    if not steps:
        return "read"

    current_url = ""
    saw_form_fill = False
    try:
        import app.execution.browser_tool as browser_tool_module
    except ImportError:
        browser_tool_module = None

    for raw_step in steps:
        step = _normalize_mapping(raw_step)
        operation = _normalize_text(step.get("operation")).lower()
        if operation == "open_url":
            current_url = _normalize_text(step.get("url"))
            continue
        if operation == "get_page_text":
            continue
        if operation == "fill_form":
            saw_form_fill = True
            continue
        if operation == "click":
            if browser_tool_module is not None and current_url:
                try:
                    page_config = browser_tool_module._page_config(current_url)
                    click_targets = _normalize_mapping(page_config.get("click_targets"))
                    click_target = _normalize_mapping(
                        click_targets.get(_normalize_text(step.get("selector")))
                    )
                    method = (
                        _normalize_text(click_target.get("method")).upper() or "GET"
                    )
                    if method == "POST":
                        return "critical"
                except ValueError:
                    return "critical" if saw_form_fill else "external"
            if saw_form_fill:
                return "critical"
            return "external"
        return "external"

    return "external" if saw_form_fill else "read"


def _api_request_is_critical(payload: object) -> bool:
    normalized_payload = _normalize_mapping(payload)
    if _normalize_bool(normalized_payload.get("modifies_external_data")):
        return True
    if _normalize_bool(normalized_payload.get("submit")):
        return True
    operation = _normalize_text(normalized_payload.get("operation")).lower()
    return any(hint in operation for hint in EXTERNAL_MUTATION_HINTS)


def build_policy_action(
    action_name: object,
    payload: object,
    *,
    descriptor: object = None,
    task_state: object = None,
) -> PolicyAction:
    normalized_action_name = _normalize_text(action_name).upper()
    normalized_payload = _normalize_mapping(payload)
    normalized_descriptor = _normalize_mapping(descriptor)
    normalized_task_state = _normalize_mapping(task_state)
    task_id = _first_non_empty(
        normalized_payload.get("task_id"),
        normalized_descriptor.get("task_id"),
        normalized_task_state.get("task_id"),
    )

    policy_action_type = "external"
    if normalized_action_name == "READ_FILE":
        policy_action_type = "read"
    elif normalized_action_name == "WRITE_FILE":
        policy_action_type = "write"
    elif normalized_action_name == "BROWSER_TOOL":
        policy_action_type = _browser_policy_action_type(normalized_payload)
    elif normalized_action_name == "SEND_EMAIL":
        policy_action_type = "critical"
    elif normalized_action_name == "API_REQUEST":
        policy_action_type = (
            "critical" if _api_request_is_critical(normalized_payload) else "external"
        )

    confirmation_required = policy_action_type == "critical"
    confirmation_received = _confirmation_received(
        normalized_payload,
        normalized_descriptor,
        normalized_task_state,
    )
    return PolicyAction(
        action_name=normalized_action_name,
        action_type=policy_action_type,
        payload=normalized_payload,
        task_id=task_id,
        confirmation_required=confirmation_required,
        confirmation_received=confirmation_received,
    )


class ExecutionPolicy:
    def validate(self, action: PolicyAction, task_state: object) -> PolicyResult:
        normalized_task_state = _normalize_mapping(task_state)
        task_status = str(normalized_task_state.get("status", "")).strip().lower()
        policy_trace = {
            "action_name": action.action_name,
            "policy_action_type": action.action_type,
            "task_status": task_status,
            "task_id": action.task_id,
            "confirmation_required": action.confirmation_required,
            "confirmation_received": action.confirmation_received,
            "known_action_type": action.action_name
            in (ALLOWED_ACTION_TYPES | ALLOWED_EXTERNAL_ACTION_TYPES),
            "task_state_allows_execution": task_status in ALLOWED_TASK_STATUSES,
        }

        if action.action_type not in POLICY_ACTION_TYPES:
            return _return_result(
                descriptor={
                    "action_type": action.action_name,
                    "payload": action.payload,
                },
                task_state=normalized_task_state,
                execution_allowed=False,
                reason=f"unknown policy action_type: {action.action_type or '(empty)'}",
                policy_trace=policy_trace,
            )

        if task_status not in ALLOWED_TASK_STATUSES:
            return _return_result(
                descriptor={
                    "action_type": action.action_name,
                    "payload": action.payload,
                },
                task_state=normalized_task_state,
                execution_allowed=False,
                reason=f"task_state does not allow execution: {task_status or '(empty)'}",
                policy_trace=policy_trace,
            )

        if action.confirmation_required and not action.confirmation_received:
            return _return_result(
                descriptor={
                    "action_type": action.action_name,
                    "payload": action.payload,
                },
                task_state=normalized_task_state,
                execution_allowed=False,
                reason=f"critical action requires confirmation: {action.action_name}",
                policy_trace=policy_trace,
            )

        return _return_result(
            descriptor={"action_type": action.action_name, "payload": action.payload},
            task_state=normalized_task_state,
            execution_allowed=True,
            reason="",
            policy_trace=policy_trace,
        )


policy = ExecutionPolicy()


def evaluate_policy(descriptor: object, task_state: object) -> PolicyResult:
    normalized_descriptor = _normalize_mapping(descriptor)
    normalized_task_state = _normalize_mapping(task_state)
    action_type = str(normalized_descriptor.get("action_type", "")).strip().upper()
    payload = _normalize_mapping(normalized_descriptor.get("payload"))
    task_status = str(normalized_task_state.get("status", "")).strip().lower()

    policy_trace: dict[str, object] = {
        "action_type": action_type,
        "descriptor_fields_present": {
            field: field in normalized_descriptor
            for field in REQUIRED_DESCRIPTOR_FIELDS
        },
        "payload_fields_present": {
            field: bool(str(payload.get(field, "")).strip())
            for field in REQUIRED_PAYLOAD_FIELDS
        },
        "task_status": task_status,
        "known_action_type": action_type in ALLOWED_ACTION_TYPES,
        "task_state_allows_execution": task_status in ALLOWED_TASK_STATUSES,
    }

    if not normalized_descriptor:
        return _return_result(
            descriptor=normalized_descriptor,
            task_state=normalized_task_state,
            execution_allowed=False,
            reason="descriptor must be a dict",
            policy_trace=policy_trace,
        )

    missing_descriptor_fields = [
        field
        for field in REQUIRED_DESCRIPTOR_FIELDS
        if field not in normalized_descriptor
    ]
    if missing_descriptor_fields:
        return _return_result(
            descriptor=normalized_descriptor,
            task_state=normalized_task_state,
            execution_allowed=False,
            reason=f"descriptor missing required fields: {', '.join(missing_descriptor_fields)}",
            policy_trace=policy_trace,
        )

    if action_type not in ALLOWED_ACTION_TYPES:
        return _return_result(
            descriptor=normalized_descriptor,
            task_state=normalized_task_state,
            execution_allowed=False,
            reason=f"unknown action_type: {action_type or '(empty)'}",
            policy_trace=policy_trace,
        )

    if not payload:
        return _return_result(
            descriptor=normalized_descriptor,
            task_state=normalized_task_state,
            execution_allowed=False,
            reason="descriptor.payload must be a dict",
            policy_trace=policy_trace,
        )

    missing_payload_fields = [
        field
        for field in REQUIRED_PAYLOAD_FIELDS
        if not str(payload.get(field, "")).strip()
    ]
    if missing_payload_fields:
        return _return_result(
            descriptor=normalized_descriptor,
            task_state=normalized_task_state,
            execution_allowed=False,
            reason=f"descriptor payload missing required fields: {', '.join(missing_payload_fields)}",
            policy_trace=policy_trace,
        )

    if not normalized_task_state:
        return _return_result(
            descriptor=normalized_descriptor,
            task_state=normalized_task_state,
            execution_allowed=False,
            reason="task_state must be a dict",
            policy_trace=policy_trace,
        )

    if task_status not in ALLOWED_TASK_STATUSES:
        return _return_result(
            descriptor=normalized_descriptor,
            task_state=normalized_task_state,
            execution_allowed=False,
            reason=f"task_state does not allow execution: {task_status or '(empty)'}",
            policy_trace=policy_trace,
        )

    action = build_policy_action(
        action_type,
        payload,
        descriptor=normalized_descriptor,
        task_state=normalized_task_state,
    )
    result = policy.validate(action, normalized_task_state)
    return PolicyResult(
        execution_allowed=result.execution_allowed,
        reason=result.reason,
        policy_trace={
            **policy_trace,
            "policy_action_type": action.action_type,
            "confirmation_required": action.confirmation_required,
            "confirmation_received": action.confirmation_received,
            **dict(result.policy_trace),
        },
    )


def policy_result_as_dict(result: PolicyResult) -> dict[str, object]:
    return asdict(result)


def evaluate_workflow_policy(payload: object, task_state: object) -> PolicyResult:
    normalized_task_state = _normalize_mapping(task_state)
    task_status = str(normalized_task_state.get("status", "")).strip().lower()
    valid, reason, validation_trace = validate_input_payload(payload)
    policy_trace: dict[str, object] = {
        "workflow_type": str(
            _normalize_mapping(payload).get("workflow_type", "")
        ).strip(),
        "task_status": task_status,
        "workflow_validation": dict(validation_trace),
        "task_state_allows_execution": task_status in ALLOWED_TASK_STATUSES,
    }

    if task_status not in ALLOWED_TASK_STATUSES:
        return _return_result(
            descriptor={"payload": _normalize_mapping(payload)},
            task_state=normalized_task_state,
            execution_allowed=False,
            reason=f"task_state does not allow execution: {task_status or '(empty)'}",
            policy_trace=policy_trace,
        )

    return _return_result(
        descriptor={"payload": _normalize_mapping(payload)},
        task_state=normalized_task_state,
        execution_allowed=bool(valid),
        reason=str(reason).strip(),
        policy_trace=policy_trace,
    )


def evaluate_workflow_contract(payload: object, task_state: object) -> PolicyResult:
    normalized_task_state = _normalize_mapping(task_state)
    task_status = str(normalized_task_state.get("status", "")).strip().lower()
    valid, reason, validation_trace = validate_decision_contract(payload)
    policy_trace: dict[str, object] = {
        "workflow_type": LEAD_ESTIMATE_WORKFLOW_TYPE,
        "task_status": task_status,
        "workflow_validation": dict(validation_trace),
        "task_state_allows_execution": task_status in ALLOWED_TASK_STATUSES,
    }

    if task_status not in ALLOWED_TASK_STATUSES:
        return _return_result(
            descriptor={"payload": _normalize_mapping(payload)},
            task_state=normalized_task_state,
            execution_allowed=False,
            reason=f"task_state does not allow execution: {task_status or '(empty)'}",
            policy_trace=policy_trace,
        )

    return _return_result(
        descriptor={"payload": _normalize_mapping(payload)},
        task_state=normalized_task_state,
        execution_allowed=bool(valid),
        reason=str(reason).strip(),
        policy_trace=policy_trace,
    )


def evaluate_external_action_policy(
    action_type: object,
    payload: object,
    task_state: object,
) -> PolicyResult:
    normalized_action_type = str(action_type or "").strip().upper()
    normalized_payload = _normalize_mapping(payload)
    normalized_task_state = _normalize_mapping(task_state)
    task_status = str(normalized_task_state.get("status", "")).strip().lower()
    destination = str(normalized_payload.get("destination", "")).strip()
    policy_trace: dict[str, object] = {
        "action_type": normalized_action_type,
        "task_status": task_status,
        "destination": destination,
        "known_action_type": normalized_action_type in ALLOWED_EXTERNAL_ACTION_TYPES,
        "known_destination": destination
        in ALLOWED_EXTERNAL_ACTION_DESTINATIONS.get(normalized_action_type, set()),
        "task_state_allows_execution": task_status in ALLOWED_TASK_STATUSES,
    }
    if normalized_action_type not in ALLOWED_EXTERNAL_ACTION_TYPES:
        return _return_result(
            descriptor={
                "action_type": normalized_action_type,
                "payload": normalized_payload,
            },
            task_state=normalized_task_state,
            execution_allowed=False,
            reason=f"unknown external action_type: {normalized_action_type or '(empty)'}",
            policy_trace=policy_trace,
        )
    if task_status not in ALLOWED_TASK_STATUSES:
        return _return_result(
            descriptor={
                "action_type": normalized_action_type,
                "payload": normalized_payload,
            },
            task_state=normalized_task_state,
            execution_allowed=False,
            reason=f"task_state does not allow execution: {task_status or '(empty)'}",
            policy_trace=policy_trace,
        )
    if destination not in ALLOWED_EXTERNAL_ACTION_DESTINATIONS.get(
        normalized_action_type, set()
    ):
        return _return_result(
            descriptor={
                "action_type": normalized_action_type,
                "payload": normalized_payload,
            },
            task_state=normalized_task_state,
            execution_allowed=False,
            reason=(
                f"destination `{destination or '(empty)'}` is not allowed for "
                f"{normalized_action_type}"
            ),
            policy_trace=policy_trace,
        )
    action = build_policy_action(
        normalized_action_type,
        normalized_payload,
        task_state=normalized_task_state,
    )
    result = policy.validate(action, normalized_task_state)
    return PolicyResult(
        execution_allowed=result.execution_allowed,
        reason=result.reason,
        policy_trace={
            **policy_trace,
            "policy_action_type": action.action_type,
            "confirmation_required": action.confirmation_required,
            "confirmation_received": action.confirmation_received,
            **dict(result.policy_trace),
        },
    )


def evaluate_task_creation_policy(
    task_data: object,
    *,
    parent_task: object = None,
    existing_tasks: object = None,
) -> PolicyResult:
    normalized_task = _normalize_mapping(task_data)
    normalized_parent = _normalize_mapping(parent_task)
    normalized_existing = []
    if isinstance(existing_tasks, list):
        normalized_existing = [_normalize_mapping(item) for item in existing_tasks]

    policy_trace: dict[str, object] = {
        "task_id": str(normalized_task.get("task_id", "")).strip(),
        "intent": str(normalized_task.get("intent", "")).strip(),
        "task_type": str(normalized_task.get("task_type", "")).strip(),
        "parent_task_id": str(
            normalized_task.get("parent_task_id")
            or _normalize_mapping(normalized_task.get("payload")).get("parent_task_id")
            or ""
        ).strip(),
        "parent_task_present": bool(normalized_parent),
        "existing_task_count": len(normalized_existing),
    }

    try:
        validated_task = validate_task_contract(normalized_task)
        validate_task_lineage(
            validated_task,
            parent_task=normalized_parent or None,
            existing_tasks=normalized_existing,
        )
        core_impact, impacted_core_files = _core_zone_impact(validated_task)
        validated_payload = _normalize_mapping(validated_task.get("payload"))
        validated_payload["core_impact"] = core_impact
        validated_task["payload"] = validated_payload
        policy_trace["core_impact"] = core_impact
        policy_trace["affected_files"] = _system_improvement_targets(validated_task)
        if impacted_core_files:
            policy_trace["core_zone_files"] = impacted_core_files
        is_system_improvement = (
            _normalize_text(validated_task.get("intent")) == SYSTEM_IMPROVEMENT_INTENT
        )
        if is_system_improvement and core_impact:
            requires_high_approval = (
                _normalize_bool(validated_payload.get("requires_approval"))
                and _normalize_text(validated_payload.get("route_target"))
                == "approval_queue"
                and _normalize_text(validated_payload.get("priority")).upper() == "HIGH"
                and _normalize_text(validated_task.get("approval_status")) == "pending"
            )
            policy_trace["requires_high_approval"] = requires_high_approval
            policy_trace[
                "execution_blocked_until_approval"
            ] = not requires_high_approval
        resource_conflict_reason, resource_conflict_task_id = _find_resource_conflict(
            validated_task,
            existing_tasks=normalized_existing,
        )
        if resource_conflict_reason:
            alternative_option = suggest_alternative(
                validated_task,
                existing_tasks=normalized_existing,
                conflict_type=resource_conflict_reason,
            )
            if alternative_option:
                policy_trace["conflict_type"] = resource_conflict_reason
                if resource_conflict_task_id:
                    policy_trace["conflict_task_id"] = resource_conflict_task_id
                policy_trace["alternative_option"] = alternative_option
                raise ValueError("alternative_option")
            raise ValueError(resource_conflict_reason)
        if not (is_system_improvement and not core_impact):
            conflict_reason = _find_decision_conflict(
                validated_task,
                existing_tasks=normalized_existing,
            )
            if conflict_reason:
                raise ValueError(conflict_reason)
    except ValueError as exc:
        return _return_result(
            descriptor={"payload": normalized_task},
            task_state={
                "task_id": str(normalized_task.get("task_id", "")).strip(),
                "status": "pending",
            },
            execution_allowed=False,
            reason=str(exc).strip(),
            policy_trace=policy_trace,
        )

    return _return_result(
        descriptor={"payload": normalized_task},
        task_state={
            "task_id": str(normalized_task.get("task_id", "")).strip(),
            "status": "pending",
        },
        execution_allowed=True,
        reason="",
        policy_trace=policy_trace,
    )
