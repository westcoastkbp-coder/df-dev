from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence

import app.execution.orchestrator_client as orchestrator_client_module
from app.execution.decision_trace import build_decision_trace, summarize_context_reference
from app.execution.vendor_router import DEFAULT_VENDOR, normalize_vendor, route as route_vendor
from app.policy.policy_gate import PolicyAction, policy


ACTION_PLAN_REQUIRED_FIELDS = {
    "task_id",
    "action",
    "action_type",
    "requires_confirmation",
    "reason",
}
AI_DECISION_SOURCES = {"rule", "ai"}
AI_POLICY_ACTION_TYPES = {"read", "write", "external", "critical"}
DEFAULT_ALLOWED_ACTIONS = (
    "read_file",
    "write_file",
    "browser_tool",
    "send_sms",
    "make_call",
    "send_email",
    "api_request",
)
CONFIRMATION_REQUIRED_STATUSES = {
    "awaiting_approval",
    "pending",
    "required",
    "requested",
    "unapproved",
}
EXECUTION_READY_STATUSES = {
    "created",
    "validated",
    "executing",
    "running",
    "deferred",
    "ready",
}
AI_CONFIRMATION_FIELDS = (
    "confirmed",
    "confirmation_received",
    "confirmation_granted",
    "policy_confirmation",
    "policy_confirmed",
)


class DecisionExecutionBlockedError(RuntimeError):
    def __init__(
        self,
        *,
        code: str,
        reason: str,
        decision_trace: dict[str, str],
        action_plan: dict[str, object] | None = None,
    ) -> None:
        self.code = str(code).strip()
        self.reason = _normalize_text(reason)
        self.decision_trace = dict(decision_trace)
        self.action_plan = dict(action_plan) if isinstance(action_plan, Mapping) else None
        super().__init__(self.reason)

    def __str__(self) -> str:
        return self.reason


def _normalize_text(value: object) -> str:
    return " ".join(str(value or "").strip().split())


def _normalize_mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _normalize_sequence(value: object) -> list[object]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return list(value)
    return []


def _normalize_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    normalized = _normalize_text(value).lower()
    return normalized in {"1", "true", "yes", "y", "on"}


def _first_non_empty(*values: object) -> str:
    for value in values:
        normalized = _normalize_text(value)
        if normalized:
            return normalized
    return ""


def _json_ready(value: object) -> object:
    if isinstance(value, Mapping):
        return {
            _normalize_text(key): _json_ready(item)
            for key, item in value.items()
            if _normalize_text(key)
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_ready(item) for item in value]
    return value


def _context_mode(context: Mapping[str, object]) -> str:
    normalized_context = _normalize_mapping(context)
    context_summary = _normalize_mapping(normalized_context.get("context_summary"))
    return _first_non_empty(
        normalized_context.get("mode"),
        context_summary.get("active_mode"),
    )


def _decision_mode(context: Mapping[str, object]) -> str:
    normalized_context = _normalize_mapping(context)
    context_summary = _normalize_mapping(normalized_context.get("context_summary"))
    global_context = _normalize_mapping(normalized_context.get("global_context"))
    configured = _first_non_empty(
        normalized_context.get("decision_mode"),
        context_summary.get("decision_mode"),
        global_context.get("decision_mode"),
        os.getenv("DF_DECISION_MODE"),
    ).lower()
    return "ai" if configured == "ai" else "rule"


def _task_id(task: Mapping[str, object], context: Mapping[str, object]) -> str:
    normalized_task = _normalize_mapping(task)
    normalized_context = _normalize_mapping(context)
    task_state = _normalize_mapping(normalized_context.get("task_state"))
    return _first_non_empty(
        normalized_task.get("task_id"),
        task_state.get("task_id"),
        normalized_context.get("task_id"),
    )


def _task_state(task: Mapping[str, object], context: Mapping[str, object]) -> str:
    normalized_task = _normalize_mapping(task)
    normalized_context = _normalize_mapping(context)
    task_state = _normalize_mapping(normalized_context.get("task_state"))
    return _first_non_empty(
        normalized_task.get("status"),
        task_state.get("status"),
        "ready",
    ).lower()


def _action(task: Mapping[str, object], context: Mapping[str, object]) -> str:
    normalized_task = _normalize_mapping(task)
    normalized_context = _normalize_mapping(context)
    return _first_non_empty(
        normalized_task.get("command_name"),
        normalized_task.get("pipeline_route"),
        normalized_task.get("intent"),
        normalized_task.get("action"),
        normalized_task.get("action_type"),
        normalized_context.get("command_name"),
        normalized_context.get("action"),
    )


def _action_type(task: Mapping[str, object], context: Mapping[str, object], *, action: str) -> str:
    normalized_task = _normalize_mapping(task)
    explicit_action_type = _first_non_empty(
        normalized_task.get("intent"),
        normalized_task.get("action_type"),
    )
    if explicit_action_type:
        return explicit_action_type.upper()
    return _normalize_text(action).upper().replace(" ", "_")


def _decision_target(task: Mapping[str, object], context: Mapping[str, object], *, action: str) -> str:
    normalized_task = _normalize_mapping(task)
    normalized_context = _normalize_mapping(context)
    payload = _normalize_mapping(normalized_task.get("payload"))
    return _first_non_empty(
        normalized_task.get("target"),
        payload.get("path"),
        payload.get("destination"),
        payload.get("resource_id"),
        normalized_context.get("command_name"),
        action,
        "execution",
    )


def _decision_parameters(task: Mapping[str, object]) -> dict[str, object]:
    return _normalize_mapping(_normalize_mapping(task).get("payload"))


def _requires_confirmation(task: Mapping[str, object], context: Mapping[str, object], *, task_state: str) -> bool:
    normalized_task = _normalize_mapping(task)
    normalized_context = _normalize_mapping(context)
    task_requires = _normalize_bool(normalized_task.get("requires_confirmation"))
    context_requires = _normalize_bool(normalized_context.get("requires_confirmation"))
    approval_status = _first_non_empty(
        normalized_task.get("approval_status"),
        normalized_context.get("approval_status"),
    ).lower()
    return (
        task_requires
        or context_requires
        or approval_status in CONFIRMATION_REQUIRED_STATUSES
        or task_state in CONFIRMATION_REQUIRED_STATUSES
    )


def _context_summary_text(context: Mapping[str, object]) -> str:
    normalized_context = _normalize_mapping(context)
    candidate = _normalize_mapping(normalized_context.get("context_summary"))
    if not candidate:
        candidate = _normalize_mapping(normalized_context.get("global_context"))
    if not candidate:
        candidate = {
            key: value
            for key, value in normalized_context.items()
            if key in {"task_id", "command_name", "mode", "decision_mode"}
        }
    return json.dumps(_json_ready(candidate), ensure_ascii=True, separators=(",", ":"))


def _input_text(task: Mapping[str, object], context: Mapping[str, object], *, action: str) -> str:
    normalized_task = _normalize_mapping(task)
    normalized_context = _normalize_mapping(context)
    payload = _normalize_mapping(normalized_task.get("payload"))
    return _first_non_empty(
        normalized_task.get("input_text"),
        normalized_task.get("instruction"),
        payload.get("instruction"),
        payload.get("request"),
        payload.get("summary"),
        normalized_task.get("goal"),
        normalized_context.get("command_name"),
        action,
    )


def _allowed_actions(task: Mapping[str, object], context: Mapping[str, object], *, action: str) -> list[str]:
    normalized_task = _normalize_mapping(task)
    normalized_context = _normalize_mapping(context)
    raw_allowed = (
        _normalize_sequence(normalized_context.get("allowed_actions"))
        or _normalize_sequence(normalized_task.get("allowed_actions"))
    )
    normalized = [
        _normalize_text(candidate).lower().replace(" ", "_")
        for candidate in raw_allowed
        if _normalize_text(candidate)
    ]
    if normalized:
        return list(dict.fromkeys(normalized))
    if _normalize_text(action):
        return [_normalize_text(action).lower().replace(" ", "_")]
    return list(DEFAULT_ALLOWED_ACTIONS)


def _policy_rules() -> list[str]:
    return [
        "Return exactly one action object.",
        "Choose action only from allowed_actions.",
        "action_type must be one of: read, write, external, critical.",
        "requires_confirmation must be true only when human confirmation is still required.",
        "Never bypass Digital Foreman policy or execution boundaries.",
    ]


def _confirmation_received(
    task: Mapping[str, object],
    context: Mapping[str, object],
    parameters: Mapping[str, object],
) -> bool:
    candidates = (
        _normalize_mapping(parameters),
        _normalize_mapping(task),
        _normalize_mapping(_normalize_mapping(task).get("payload")),
        _normalize_mapping(context),
        _normalize_mapping(_normalize_mapping(context).get("task_state")),
    )
    for candidate in candidates:
        for field_name in AI_CONFIRMATION_FIELDS:
            if _normalize_bool(candidate.get(field_name)):
                return True
        if _normalize_text(candidate.get("approval_status")).lower() == "approved":
            return True
    return False


def _policy_result_text(
    *,
    execution_allowed: bool,
    reason: object,
) -> str:
    normalized_reason = _normalize_text(reason)
    if execution_allowed:
        return "allowed: policy gate passed"
    return f"blocked: {normalized_reason or 'policy rejected'}"


def _policy_task_state(context: Mapping[str, object]) -> dict[str, object]:
    normalized_task_state = _normalize_mapping(_normalize_mapping(context).get("task_state"))
    raw_status = _normalize_text(normalized_task_state.get("status")).upper()
    status_map = {
        "CREATED": "pending",
        "VALIDATED": "pending",
        "READY": "pending",
        "EXECUTING": "running",
        "RUNNING": "running",
        "DEFERRED": "pending",
        "COMPLETED": "completed",
        "FAILED": "failed",
    }
    normalized_task_state["status"] = status_map.get(
        raw_status,
        _normalize_text(normalized_task_state.get("status")).lower(),
    )
    return normalized_task_state


def _validate_ai_policy(
    action_plan: Mapping[str, object],
    *,
    task: Mapping[str, object],
    context: Mapping[str, object],
) -> str:
    normalized_parameters = _normalize_mapping(action_plan.get("parameters"))
    confirmation_received = _confirmation_received(task, context, normalized_parameters)
    normalized_action_type = _normalize_text(action_plan.get("action_type")).lower()
    policy_action = PolicyAction(
        action_name=_normalize_text(action_plan.get("action")).upper(),
        action_type=normalized_action_type,
        payload={
            "task_id": _normalize_text(action_plan.get("task_id")),
            "target": _normalize_text(action_plan.get("target")),
            **normalized_parameters,
        },
        task_id=_normalize_text(action_plan.get("task_id")),
        confirmation_required=(
            bool(action_plan.get("requires_confirmation")) or normalized_action_type == "critical"
        ),
        confirmation_received=confirmation_received,
    )
    policy_result = policy.validate(
        policy_action,
        _policy_task_state(context),
    )
    if (
        not policy_result.execution_allowed
        and not (policy_action.confirmation_required and not policy_action.confirmation_received)
    ):
        raise ValueError(policy_result.reason or "policy rejected ai decision")
    return _policy_result_text(
        execution_allowed=policy_result.execution_allowed,
        reason=policy_result.reason,
    )


def _rule_based_plan(
    task: Mapping[str, object],
    context: Mapping[str, object],
    *,
    reason_prefix: object = "",
) -> dict[str, object]:
    normalized_task = _normalize_mapping(task)
    normalized_context = _normalize_mapping(context)
    task_id = _task_id(normalized_task, normalized_context)
    action = _action(normalized_task, normalized_context)
    task_state = _task_state(normalized_task, normalized_context)
    mode = _context_mode(normalized_context)
    requires_confirmation = _requires_confirmation(
        normalized_task,
        normalized_context,
        task_state=task_state,
    )
    if not task_id:
        raise ValueError("decision context must include task_id")
    if not action:
        raise ValueError("decision context must include action")

    readiness = (
        f"state={task_state or 'unknown'}"
        if task_state in EXECUTION_READY_STATUSES
        else f"state={task_state or 'unknown'} requires review"
    )
    mode_fragment = f"; mode={mode}" if mode else ""
    base_reason = (
        f"rule-based confirmation required for {action}; {readiness}{mode_fragment}"
        if requires_confirmation
        else f"rule-based decision approved {action}; {readiness}{mode_fragment}"
    )
    prefix = _normalize_text(reason_prefix)
    reason = f"{prefix}; {base_reason}" if prefix else base_reason
    return validate_action_plan(
        {
            "task_id": task_id,
            "action": action,
            "action_type": _action_type(
                normalized_task,
                normalized_context,
                action=action,
            ),
            "target": _decision_target(
                normalized_task,
                normalized_context,
                action=action,
            ),
            "parameters": _decision_parameters(normalized_task),
            "decision_source": "rule",
            "requires_confirmation": requires_confirmation,
            "reason": reason,
        },
        expected_task_id=task_id,
    )


def validate_action_plan(
    plan: object,
    *,
    expected_task_id: object = "",
) -> dict[str, object]:
    if not isinstance(plan, Mapping):
        raise ValueError("decision engine must return an action plan dict")
    candidate = dict(plan)
    missing_fields = sorted(ACTION_PLAN_REQUIRED_FIELDS - set(candidate))
    if missing_fields:
        raise ValueError("action plan missing required fields: " + ", ".join(missing_fields))
    if not _normalize_text(candidate.get("task_id")):
        raise ValueError("action plan task_id must not be empty")
    if not _normalize_text(candidate.get("action")):
        raise ValueError("action plan action must not be empty")
    if not _normalize_text(candidate.get("action_type")):
        raise ValueError("action plan action_type must not be empty")
    if not isinstance(candidate.get("requires_confirmation"), bool):
        raise ValueError("action plan requires_confirmation must be a bool")
    if not _normalize_text(candidate.get("reason")):
        raise ValueError("action plan reason must not be empty")

    normalized_expected_task_id = _normalize_text(expected_task_id)
    normalized_task_id = _normalize_text(candidate.get("task_id"))
    if normalized_expected_task_id and normalized_task_id != normalized_expected_task_id:
        raise ValueError("action plan task_id does not match execution task")

    decision_source = _normalize_text(candidate.get("decision_source")).lower() or "rule"
    if decision_source not in AI_DECISION_SOURCES:
        raise ValueError(f"unsupported decision source: {decision_source or '(empty)'}")

    normalized_action_type = _normalize_text(candidate.get("action_type"))
    if decision_source == "ai":
        normalized_action_type = normalized_action_type.lower()
        if normalized_action_type not in AI_POLICY_ACTION_TYPES:
            raise ValueError(
                "invalid ai action_type: "
                f"{normalized_action_type or '(empty)'}"
            )
        if not _normalize_text(candidate.get("target")):
            raise ValueError("ai action plan target must not be empty")
        if "parameters" not in candidate or not isinstance(candidate.get("parameters"), Mapping):
            raise ValueError("ai action plan parameters must be a dict")

    return {
        "task_id": normalized_task_id,
        "action": _normalize_text(candidate.get("action")),
        "action_type": normalized_action_type,
        "target": _normalize_text(candidate.get("target")),
        "parameters": _normalize_mapping(candidate.get("parameters")),
        "decision_source": decision_source,
        "vendor": normalize_vendor(candidate.get("vendor")),
        "policy_result": _normalize_text(candidate.get("policy_result")),
        "requires_confirmation": bool(candidate.get("requires_confirmation")),
        "reason": _normalize_text(candidate.get("reason")),
    }


def _decide_with_ai(
    task: Mapping[str, object],
    context: Mapping[str, object],
    *,
    task_id: str,
    action: str,
) -> dict[str, object]:
    normalized_task = _normalize_mapping(task)
    normalized_context = _normalize_mapping(context)
    allowed_actions = _allowed_actions(normalized_task, normalized_context, action=action)
    raw_plan = orchestrator_client_module.call_orchestrator(
        normalized_task,
        _context_summary_text(normalized_context),
        _input_text(normalized_task, normalized_context, action=action),
        allowed_actions=allowed_actions,
        policy_rules=_policy_rules(),
    )
    if not isinstance(raw_plan, Mapping):
        raise ValueError("orchestrator client must return a dict")
    candidate = dict(raw_plan)
    candidate["task_id"] = task_id
    candidate["decision_source"] = "ai"
    normalized_plan = validate_action_plan(
        candidate,
        expected_task_id=task_id,
    )
    normalized_action = normalized_plan["action"].lower().replace(" ", "_")
    if normalized_action not in allowed_actions:
        raise ValueError(f"ai action is not allowed: {normalized_plan['action']}")
    expected_action = _normalize_text(action).lower().replace(" ", "_")
    if expected_action and normalized_action != expected_action:
        raise ValueError("ai action does not match execution task")
    normalized_plan["policy_result"] = _validate_ai_policy(
        normalized_plan,
        task=normalized_task,
        context=normalized_context,
    )
    return normalized_plan


def decide(task: object, context: object) -> dict[str, object]:
    normalized_task = _normalize_mapping(task)
    normalized_context = _normalize_mapping(context)
    task_id = _task_id(normalized_task, normalized_context)
    action = _action(normalized_task, normalized_context)
    if not task_id:
        raise ValueError("decision context must include task_id")
    if not action:
        raise ValueError("decision context must include action")

    if _decision_mode(normalized_context) != "ai":
        routed_plan = _rule_based_plan(normalized_task, normalized_context)
        routed_plan["vendor"] = route_vendor(normalized_task, normalized_context, routed_plan)
        return validate_action_plan(routed_plan, expected_task_id=task_id)

    try:
        routed_plan = _decide_with_ai(
            normalized_task,
            normalized_context,
            task_id=task_id,
            action=action,
        )
        routed_plan["vendor"] = route_vendor(normalized_task, normalized_context, routed_plan)
        return validate_action_plan(routed_plan, expected_task_id=task_id)
    except orchestrator_client_module.OrchestratorUnavailableError as exc:
        routed_plan = _rule_based_plan(
            normalized_task,
            normalized_context,
            reason_prefix=(
                "ai orchestrator unavailable, using rule-based fallback: "
                f"{_normalize_text(exc) or 'unavailable'}"
            ),
        )
        routed_plan["vendor"] = route_vendor(normalized_task, normalized_context, routed_plan)
        return validate_action_plan(routed_plan, expected_task_id=task_id)


def _context_used_for_plan(
    action_plan: Mapping[str, object],
    *,
    task: Mapping[str, object],
    context: Mapping[str, object],
    source: object,
) -> str:
    normalized_context_used = summarize_context_reference(
        task_id=action_plan["task_id"],
        intent=action_plan["action"],
        payload={
            "operation": action_plan["action"],
            "target": _first_non_empty(action_plan.get("target"), _task_state(task, context), "execution"),
            **_normalize_mapping(action_plan.get("parameters")),
        },
        source=source,
        context_summary=_normalize_mapping(context.get("context_summary")),
        command_name=_first_non_empty(
            context.get("command_name"),
            action_plan["action"],
        ),
        mode=_context_mode(context),
    )
    decision_source = _normalize_text(action_plan.get("decision_source")).lower()
    if decision_source and f"source={decision_source}" not in normalized_context_used:
        return f"source={decision_source}; {normalized_context_used}"
    return normalized_context_used


def decision_trace_for_plan(
    action_plan: object,
    *,
    task: object = None,
    context: object = None,
    source: object = "decision_engine",
    policy_result: object = "",
) -> dict[str, str]:
    normalized_plan = validate_action_plan(action_plan)
    normalized_task = _normalize_mapping(task)
    normalized_context = _normalize_mapping(context)
    normalized_policy_result = _normalize_text(policy_result) or _normalize_text(
        normalized_plan.get("policy_result")
    )
    if not normalized_policy_result:
        normalized_policy_result = (
            "blocked: confirmation required"
            if normalized_plan["requires_confirmation"]
            else "allowed: decision recorded"
        )
    return build_decision_trace(
        reason=normalized_plan["reason"],
        context_used=_context_used_for_plan(
            normalized_plan,
            task=normalized_task,
            context=normalized_context,
            source=source,
        ),
        action_type=normalized_plan["action_type"],
        policy_result=normalized_policy_result,
        confidence="medium" if normalized_plan["requires_confirmation"] else "high",
        vendor=normalized_plan.get("vendor", DEFAULT_VENDOR),
    )


def failure_trace_for_context(
    *,
    task: object,
    context: object,
    reason: object,
    source: object = "decision_engine",
) -> dict[str, str]:
    normalized_task = _normalize_mapping(task)
    normalized_context = _normalize_mapping(context)
    task_id = _task_id(normalized_task, normalized_context)
    action = _action(normalized_task, normalized_context) or "execution"
    vendor = route_vendor(normalized_task, normalized_context, {"action": action})
    return build_decision_trace(
        reason=_normalize_text(reason) or "execution blocked before decision",
        context_used=summarize_context_reference(
            task_id=task_id,
            intent=action,
            payload={"operation": action},
            source=source,
            context_summary=_normalize_mapping(normalized_context.get("context_summary")),
            command_name=_first_non_empty(
                normalized_context.get("command_name"),
                action,
            ),
            mode=_context_mode(normalized_context),
        ),
        action_type=_action_type(normalized_task, normalized_context, action=action) or "EXECUTION",
        policy_result=f"blocked: {_normalize_text(reason) or 'decision missing'}",
        confidence="high",
        vendor=vendor,
    )
