from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from types import MappingProxyType
from typing import Final, TypeAlias, cast


BusinessScalar: TypeAlias = str | int | float | bool | None
BusinessValue: TypeAlias = (
    BusinessScalar | tuple["BusinessValue", ...] | Mapping[str, "BusinessValue"]
)

ALLOWED_SIGNAL_SEVERITIES: Final[set[str]] = {"low", "medium", "high", "critical"}
REQUIRED_SIGNAL_FIELDS: Final[set[str]] = {
    "signal_type",
    "severity",
    "source",
    "metrics",
    "impact",
    "recommended_action",
}
ALLOWED_RECOMMENDED_ACTIONS: Final[dict[str, dict[str, str]]] = {
    "request_more_reviews": {
        "task_intent": "business_signal_response",
        "task_type": "lead",
    },
    "investigate_visibility_drop": {
        "task_intent": "business_signal_response",
        "task_type": "lead",
    },
    "refresh_business_profile": {
        "task_intent": "business_signal_response",
        "task_type": "project",
    },
    "follow_up_missed_calls": {
        "task_intent": "business_signal_response",
        "task_type": "lead",
    },
}
ALLOWED_DECISION_PRIORITIES: Final[set[str]] = {"low", "medium", "high", "urgent"}
ALLOWED_EXECUTION_MODES: Final[set[str]] = {"auto", "confirmation", "strict"}
ALLOWED_DECISION_OUTCOME_STATUSES: Final[set[str]] = {
    "success",
    "no_effect",
    "negative",
}
PRIORITY_ORDER: Final[tuple[str, ...]] = ("low", "medium", "high", "urgent")
ALTERNATIVE_ACTIONS_BY_ACTION: Final[dict[str, tuple[str, ...]]] = {
    "request_more_reviews": ("investigate_visibility_drop", "refresh_business_profile"),
    "investigate_visibility_drop": ("refresh_business_profile", "request_more_reviews"),
    "refresh_business_profile": ("investigate_visibility_drop",),
    "follow_up_missed_calls": ("request_more_reviews",),
}


class BusinessSignalValidationError(ValueError):
    """Raised when a business signal is not structured or mappable."""


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _validate_identifier(value: object, *, field_name: str) -> str:
    normalized = _normalize_text(value).lower()
    if not normalized:
        raise BusinessSignalValidationError(f"{field_name} must not be empty")
    if any(char not in "abcdefghijklmnopqrstuvwxyz0123456789_" for char in normalized):
        raise BusinessSignalValidationError(
            f"{field_name} must use lowercase snake_case identifiers"
        )
    return normalized


def _normalize_mapping(value: object, *, field_name: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise BusinessSignalValidationError(f"{field_name} must be a dict")
    return dict(value)


def _deep_clone_json_like(value: object, *, field_name: str) -> BusinessValue:
    if value is None or isinstance(value, (str, int, float, bool)):
        return cast(BusinessValue, value)
    if isinstance(value, Mapping):
        cloned: dict[str, BusinessValue] = {}
        for key, item in value.items():
            normalized_key = _validate_identifier(key, field_name=f"{field_name}.key")
            cloned[normalized_key] = _deep_clone_json_like(
                item,
                field_name=f"{field_name}.{normalized_key}",
            )
        return MappingProxyType(cloned)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(
            _deep_clone_json_like(item, field_name=f"{field_name}[]") for item in value
        )
    raise BusinessSignalValidationError(
        f"{field_name} must contain only structured JSON-like values"
    )


def _deep_unfreeze(value: BusinessValue) -> object:
    if isinstance(value, Mapping):
        return {key: _deep_unfreeze(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_deep_unfreeze(item) for item in value]
    return value


@dataclass(frozen=True, slots=True)
class BusinessSignal:
    signal_type: str
    severity: str
    source: str
    metrics: Mapping[str, BusinessValue]
    impact: str
    recommended_action: str

    def to_dict(self) -> dict[str, object]:
        return {
            "signal_type": self.signal_type,
            "severity": self.severity,
            "source": self.source,
            "metrics": _deep_unfreeze(cast(BusinessValue, self.metrics)),
            "impact": self.impact,
            "recommended_action": self.recommended_action,
        }


@dataclass(frozen=True, slots=True)
class Decision:
    decision_id: str
    decision_type: str
    priority: str
    confidence: float
    reason: str
    evidence: Mapping[str, BusinessValue]
    recommended_actions: tuple[str, ...]
    execution_mode: str

    def to_dict(self) -> dict[str, object]:
        return {
            "decision_id": self.decision_id,
            "decision_type": self.decision_type,
            "priority": self.priority,
            "confidence": self.confidence,
            "reason": self.reason,
            "evidence": _deep_unfreeze(cast(BusinessValue, self.evidence)),
            "recommended_actions": list(self.recommended_actions),
            "execution_mode": self.execution_mode,
        }


@dataclass(frozen=True, slots=True)
class DecisionOutcome:
    decision_type: str
    decision_id: str
    actions_executed: tuple[str, ...]
    timestamp: str
    outcome_status: str
    metrics_delta: Mapping[str, BusinessValue]
    confidence: float

    def to_dict(self) -> dict[str, object]:
        return {
            "decision_type": self.decision_type,
            "decision_id": self.decision_id,
            "actions_executed": list(self.actions_executed),
            "timestamp": self.timestamp,
            "outcome_status": self.outcome_status,
            "metrics_delta": _deep_unfreeze(cast(BusinessValue, self.metrics_delta)),
            "confidence": self.confidence,
        }


@dataclass(frozen=True, slots=True)
class DecisionAdjustment:
    decision: Decision
    explanation: Mapping[str, BusinessValue]

    def to_dict(self) -> dict[str, object]:
        return {
            "decision": self.decision.to_dict(),
            "explanation": _deep_unfreeze(cast(BusinessValue, self.explanation)),
        }


@dataclass(frozen=True, slots=True)
class ExecutiveState:
    lead_status: str
    project_status: str
    contractor_status: str
    risks: tuple[str, ...]
    pending_approvals: tuple[str, ...]
    top_priority: str
    leads: tuple[str, ...]
    projects: tuple[str, ...]
    contractors: tuple[str, ...]
    actions_required: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "lead_status": self.lead_status,
            "project_status": self.project_status,
            "contractor_status": self.contractor_status,
            "risks": list(self.risks),
            "pending_approvals": list(self.pending_approvals),
            "top_priority": self.top_priority,
            "sections": {
                "Leads": list(self.leads),
                "Projects": list(self.projects),
                "Contractors": list(self.contractors),
                "Risks": list(self.risks),
                "Actions Required": list(self.actions_required),
            },
        }


def build_business_signal(payload: object) -> BusinessSignal:
    normalized = _normalize_mapping(payload, field_name="business_signal")
    missing_fields = sorted(REQUIRED_SIGNAL_FIELDS - set(normalized))
    if missing_fields:
        raise BusinessSignalValidationError(
            "business_signal missing required fields: " + ", ".join(missing_fields)
        )
    unexpected_fields = sorted(set(normalized) - REQUIRED_SIGNAL_FIELDS)
    if unexpected_fields:
        raise BusinessSignalValidationError(
            "business_signal contains unsupported fields: "
            + ", ".join(unexpected_fields)
        )

    severity = _validate_identifier(normalized.get("severity"), field_name="severity")
    if severity not in ALLOWED_SIGNAL_SEVERITIES:
        raise BusinessSignalValidationError(
            f"unsupported business signal severity: {severity or '(empty)'}"
        )
    recommended_action = _validate_identifier(
        normalized.get("recommended_action"),
        field_name="recommended_action",
    )
    if recommended_action not in ALLOWED_RECOMMENDED_ACTIONS:
        raise BusinessSignalValidationError(
            f"unsupported recommended_action: {recommended_action}"
        )

    metrics = _normalize_mapping(normalized.get("metrics"), field_name="metrics")
    if not metrics:
        raise BusinessSignalValidationError("metrics must not be empty")

    return BusinessSignal(
        signal_type=_validate_identifier(
            normalized.get("signal_type"), field_name="signal_type"
        ),
        severity=severity,
        source=_validate_identifier(normalized.get("source"), field_name="source"),
        metrics=cast(
            Mapping[str, BusinessValue],
            _deep_clone_json_like(metrics, field_name="metrics"),
        ),
        impact=_validate_identifier(normalized.get("impact"), field_name="impact"),
        recommended_action=recommended_action,
    )


def build_decision_from_business_signal(signal: BusinessSignal) -> Decision:
    priority_by_severity = {
        "low": "low",
        "medium": "medium",
        "high": "high",
        "critical": "urgent",
    }
    execution_mode_by_severity = {
        "low": "confirmation",
        "medium": "confirmation",
        "high": "auto",
        "critical": "strict",
    }
    decision = Decision(
        decision_id=f"{signal.signal_type}:{signal.source}:{signal.recommended_action}",
        decision_type=f"{signal.signal_type}_decision",
        priority=priority_by_severity[signal.severity],
        confidence=1.0,
        reason=signal.impact,
        evidence=cast(
            Mapping[str, BusinessValue],
            _deep_clone_json_like(dict(signal.metrics), field_name="evidence"),
        ),
        recommended_actions=(signal.recommended_action,),
        execution_mode=execution_mode_by_severity[signal.severity],
    )
    if decision.priority not in ALLOWED_DECISION_PRIORITIES:
        raise BusinessSignalValidationError(
            f"unsupported decision priority: {decision.priority}"
        )
    if decision.execution_mode not in ALLOWED_EXECUTION_MODES:
        raise BusinessSignalValidationError(
            f"unsupported execution mode: {decision.execution_mode}"
        )
    return decision


def _numeric_metric(value: object, *, field_name: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise BusinessSignalValidationError(
            f"{field_name} must be numeric when provided"
        )
    if isinstance(value, (int, float)):
        return float(value)
    raise BusinessSignalValidationError(f"{field_name} must be numeric when provided")


def evaluate_decision_outcome(
    decision: Decision,
    *,
    before_metrics: Mapping[str, object],
    after_metrics: Mapping[str, object],
    actions_executed: Sequence[object],
    timestamp: object = "",
) -> DecisionOutcome:
    normalized_before = _normalize_mapping(before_metrics, field_name="before_metrics")
    normalized_after = _normalize_mapping(after_metrics, field_name="after_metrics")
    if set(normalized_before) != set(normalized_after):
        raise BusinessSignalValidationError(
            "before_metrics and after_metrics must use identical keys"
        )
    deltas: dict[str, BusinessValue] = {}
    positive_delta_count = 0
    negative_delta_count = 0
    for key in sorted(normalized_before):
        before_value = _numeric_metric(
            normalized_before.get(key), field_name=f"before_metrics.{key}"
        )
        after_value = _numeric_metric(
            normalized_after.get(key), field_name=f"after_metrics.{key}"
        )
        if before_value is None or after_value is None:
            raise BusinessSignalValidationError(
                f"metrics comparison requires numeric values for `{key}`"
            )
        deltas[_validate_identifier(key, field_name="metrics_delta.key")] = (
            after_value - before_value
        )
        if after_value > before_value:
            positive_delta_count += 1
        elif after_value < before_value:
            negative_delta_count += 1
    if positive_delta_count > 0 and negative_delta_count == 0:
        outcome_status = "success"
    elif negative_delta_count > 0 and positive_delta_count == 0:
        outcome_status = "negative"
    else:
        outcome_status = "no_effect"
    if outcome_status not in ALLOWED_DECISION_OUTCOME_STATUSES:
        raise BusinessSignalValidationError(
            f"unsupported decision outcome status: {outcome_status}"
        )
    action_list = tuple(
        _validate_identifier(item, field_name="actions_executed[]")
        for item in actions_executed
    )
    return DecisionOutcome(
        decision_type=decision.decision_type,
        decision_id=decision.decision_id,
        actions_executed=action_list,
        timestamp=_normalize_text(timestamp) or _timestamp(),
        outcome_status=outcome_status,
        metrics_delta=cast(
            Mapping[str, BusinessValue],
            _deep_clone_json_like(deltas, field_name="metrics_delta"),
        ),
        confidence=1.0 if deltas else 0.0,
    )


def _lower_priority(priority: str) -> str:
    if priority not in PRIORITY_ORDER:
        raise BusinessSignalValidationError(
            f"unsupported decision priority: {priority}"
        )
    current_index = PRIORITY_ORDER.index(priority)
    return PRIORITY_ORDER[max(0, current_index - 1)]


def adjust_decision_with_outcomes(
    decision: Decision,
    outcomes: Sequence[DecisionOutcome | Mapping[str, object]],
) -> DecisionAdjustment:
    normalized_outcomes: list[DecisionOutcome] = []
    for raw_outcome in outcomes:
        if isinstance(raw_outcome, DecisionOutcome):
            outcome = raw_outcome
        else:
            payload = _normalize_mapping(raw_outcome, field_name="decision_outcome")
            outcome = DecisionOutcome(
                decision_type=_validate_identifier(
                    payload.get("decision_type"), field_name="decision_type"
                ),
                decision_id=_validate_identifier(
                    payload.get("decision_id"), field_name="decision_id"
                ),
                actions_executed=tuple(
                    _validate_identifier(item, field_name="actions_executed[]")
                    for item in payload.get("actions_executed", [])
                ),
                timestamp=_normalize_text(payload.get("timestamp")) or _timestamp(),
                outcome_status=_validate_identifier(
                    payload.get("outcome_status"), field_name="outcome_status"
                ),
                metrics_delta=cast(
                    Mapping[str, BusinessValue],
                    _deep_clone_json_like(
                        _normalize_mapping(
                            payload.get("metrics_delta"), field_name="metrics_delta"
                        ),
                        field_name="metrics_delta",
                    ),
                ),
                confidence=float(payload.get("confidence", 0.0)),
            )
        if outcome.decision_type == decision.decision_type:
            normalized_outcomes.append(outcome)

    failure_count = sum(
        1
        for outcome in normalized_outcomes
        if outcome.outcome_status in {"no_effect", "negative"}
    )
    alternative_actions = list(decision.recommended_actions)
    adjusted_priority = decision.priority
    adjusted_confidence = decision.confidence
    changed = False
    if failure_count >= 3:
        adjusted_priority = _lower_priority(decision.priority)
        adjusted_confidence = max(0.1, round(decision.confidence - 0.3, 2))
        primary_action = (
            decision.recommended_actions[0] if decision.recommended_actions else ""
        )
        for alternative_action in ALTERNATIVE_ACTIONS_BY_ACTION.get(primary_action, ()):
            if alternative_action not in alternative_actions:
                alternative_actions.append(alternative_action)
        changed = (
            adjusted_priority != decision.priority
            or adjusted_confidence != decision.confidence
        )

    adjusted_decision = Decision(
        decision_id=decision.decision_id,
        decision_type=decision.decision_type,
        priority=adjusted_priority,
        confidence=adjusted_confidence,
        reason=decision.reason,
        evidence=decision.evidence,
        recommended_actions=tuple(alternative_actions),
        execution_mode=decision.execution_mode,
    )
    explanation = {
        "decision_type": decision.decision_type,
        "matched_outcome_count": len(normalized_outcomes),
        "non_success_count": failure_count,
        "change_applied": changed,
        "change_reason": (
            "repeated_no_effect_or_negative_outcomes"
            if failure_count >= 3
            else "insufficient_negative_history"
        ),
    }
    return DecisionAdjustment(
        decision=adjusted_decision,
        explanation=cast(
            Mapping[str, BusinessValue],
            _deep_clone_json_like(
                explanation, field_name="decision_adjustment_explanation"
            ),
        ),
    )


def decision_to_task_input(
    signal: BusinessSignal,
    decision: Decision,
    *,
    source: str = "internal",
) -> dict[str, object]:
    if not decision.recommended_actions:
        raise BusinessSignalValidationError(
            "decision.recommended_actions must not be empty"
        )
    primary_action = _validate_identifier(
        decision.recommended_actions[0],
        field_name="decision.recommended_actions[0]",
    )
    if primary_action not in ALLOWED_RECOMMENDED_ACTIONS:
        raise BusinessSignalValidationError(
            f"unsupported decision recommended action: {primary_action}"
        )
    action_mapping = dict(ALLOWED_RECOMMENDED_ACTIONS[primary_action])
    return {
        "source": _normalize_text(source).lower() or "internal",
        "status": "awaiting_approval"
        if decision.execution_mode == "confirmation"
        else "created",
        "intent": action_mapping["task_intent"],
        "task_type": action_mapping["task_type"],
        "execution_mode": decision.execution_mode,
        "approval_status": "pending"
        if decision.execution_mode in {"confirmation", "strict"}
        else "approved",
        "payload": {
            "business_signal": signal.to_dict(),
            "decision": decision.to_dict(),
            "recommended_action": primary_action,
            "signal_type": signal.signal_type,
            "impact": signal.impact,
            "severity": signal.severity,
            "metrics": signal.to_dict()["metrics"],
            "priority": decision.priority,
            "execution_mode": decision.execution_mode,
        },
        "notes": [
            f"business_signal:{signal.signal_type}",
            f"decision:{decision.decision_type}",
            f"recommended_action:{primary_action}",
        ],
    }


def _task_mapping(task: object) -> dict[str, object]:
    return dict(task) if isinstance(task, Mapping) else {}


def _decision_mapping(decision: Decision | Mapping[str, object]) -> dict[str, object]:
    if isinstance(decision, Decision):
        return decision.to_dict()
    return _normalize_mapping(decision, field_name="decision")


def _outcome_mapping(
    outcome: DecisionOutcome | Mapping[str, object],
) -> dict[str, object]:
    if isinstance(outcome, DecisionOutcome):
        return outcome.to_dict()
    return _normalize_mapping(outcome, field_name="decision_outcome")


def _signal_mapping(signal: BusinessSignal | Mapping[str, object]) -> dict[str, object]:
    if isinstance(signal, BusinessSignal):
        return signal.to_dict()
    return _normalize_mapping(signal, field_name="business_signal")


def _first_non_empty(values: Sequence[object], fallback: str) -> str:
    for value in values:
        normalized = _normalize_text(value)
        if normalized:
            return normalized
    return fallback


def _summarize_pending_approval(task: Mapping[str, object]) -> str:
    payload = _normalize_mapping(task.get("payload", {}), field_name="task.payload")
    decision = _normalize_mapping(
        payload.get("decision", {}), field_name="task.payload.decision"
    )
    return _first_non_empty(
        (
            decision.get("decision_type"),
            payload.get("signal_type"),
            task.get("intent"),
            task.get("task_id"),
        ),
        "approval_required",
    )


def build_executive_state(
    *,
    business_signals: Sequence[BusinessSignal | Mapping[str, object]],
    decisions: Sequence[Decision | Mapping[str, object]],
    tasks: Sequence[Mapping[str, object] | object],
    decision_outcomes: Sequence[DecisionOutcome | Mapping[str, object]],
) -> ExecutiveState:
    normalized_signals = [_signal_mapping(signal) for signal in business_signals]
    normalized_decisions = [_decision_mapping(decision) for decision in decisions]
    normalized_tasks = [_task_mapping(task) for task in tasks if _task_mapping(task)]
    normalized_outcomes = [_outcome_mapping(outcome) for outcome in decision_outcomes]

    lead_signal_count = sum(
        1
        for signal in normalized_signals
        if _normalize_text(signal.get("recommended_action"))
        in {
            "request_more_reviews",
            "investigate_visibility_drop",
            "follow_up_missed_calls",
        }
    )
    project_signal_count = sum(
        1
        for signal in normalized_signals
        if _normalize_text(signal.get("recommended_action"))
        == "refresh_business_profile"
    )
    open_project_tasks = sum(
        1
        for task in normalized_tasks
        if _normalize_text(task.get("task_type")) == "project"
        and _normalize_text(task.get("status")) not in {"COMPLETED", "FAILED"}
    )
    contractor_tasks = sum(
        1
        for task in normalized_tasks
        if _normalize_text(task.get("task_type"))
        in {"procurement", "permit", "payment"}
        and _normalize_text(task.get("status")) not in {"COMPLETED", "FAILED"}
    )
    pending_approval_tasks = [
        task
        for task in normalized_tasks
        if _normalize_text(task.get("approval_status")).lower() == "pending"
    ]
    negative_outcomes = [
        outcome
        for outcome in normalized_outcomes
        if _normalize_text(outcome.get("outcome_status")) == "negative"
    ]
    no_effect_outcomes = [
        outcome
        for outcome in normalized_outcomes
        if _normalize_text(outcome.get("outcome_status")) == "no_effect"
    ]
    urgent_decisions = [
        decision
        for decision in normalized_decisions
        if _normalize_text(decision.get("priority")) == "urgent"
    ]

    lead_status = (
        "needs attention"
        if negative_outcomes or lead_signal_count > 0 or pending_approval_tasks
        else "stable"
    )
    project_status = (
        "active" if open_project_tasks > 0 or project_signal_count > 0 else "stable"
    )
    contractor_status = "attention needed" if contractor_tasks > 0 else "stable"

    risks: list[str] = []
    if negative_outcomes:
        risks.append(f"{len(negative_outcomes)} decision outcome(s) turned negative")
    if len(no_effect_outcomes) >= 2:
        risks.append(
            f"{len(no_effect_outcomes)} recent decisions had no measurable effect"
        )
    if pending_approval_tasks:
        risks.append(
            f"{len(pending_approval_tasks)} important item(s) are waiting on approval"
        )
    if contractor_tasks:
        risks.append(f"{contractor_tasks} contractor-facing task(s) remain open")
    if not risks and urgent_decisions:
        risks.append(f"{len(urgent_decisions)} urgent decision(s) need follow-through")
    risks = risks[:3]

    pending_approvals = [
        _summarize_pending_approval(task) for task in pending_approval_tasks[:3]
    ]

    top_priority = "stable"
    if pending_approval_tasks:
        top_priority = _first_non_empty(
            (
                _normalize_mapping(
                    _normalize_mapping(
                        pending_approval_tasks[0].get("payload", {}),
                        field_name="task.payload",
                    ).get("decision", {}),
                    field_name="task.payload.decision",
                ).get("decision_type"),
                pending_approval_tasks[0].get("intent"),
            ),
            "approval_required",
        )
    elif urgent_decisions:
        top_priority = _first_non_empty(
            (
                urgent_decisions[0].get("decision_type"),
                urgent_decisions[0].get("decision_id"),
            ),
            "urgent_decision",
        )
    elif risks:
        top_priority = risks[0]

    leads = [
        (
            f"{lead_signal_count} lead signal(s) changed"
            if lead_signal_count
            else "Lead flow stable"
        ),
        (
            f"{len(negative_outcomes) + len(no_effect_outcomes)} recent lead-related decisions underperformed"
            if negative_outcomes or no_effect_outcomes
            else "Recent lead decisions are producing measurable progress"
        ),
    ]
    projects = [
        (
            f"{open_project_tasks} project task(s) currently open"
            if open_project_tasks
            else "No project backlog spike detected"
        ),
        (
            f"{project_signal_count} project-level signal(s) require follow-up"
            if project_signal_count
            else "Project-level signals remain quiet"
        ),
    ]
    contractors = [
        (
            f"{contractor_tasks} contractor-facing task(s) pending"
            if contractor_tasks
            else "Contractor pipeline stable"
        )
    ]
    actions_required = []
    if pending_approvals:
        actions_required.append(f"Approve or reject: {pending_approvals[0]}")
    if negative_outcomes:
        actions_required.append(
            "Review negative decision outcomes before repeating the same action"
        )
    if contractor_tasks:
        actions_required.append(
            "Clear contractor-related open work blocking downstream execution"
        )
    if not actions_required:
        actions_required.append("No immediate owner action required")

    return ExecutiveState(
        lead_status=lead_status,
        project_status=project_status,
        contractor_status=contractor_status,
        risks=tuple(risks),
        pending_approvals=tuple(pending_approvals),
        top_priority=top_priority,
        leads=tuple(leads[:2]),
        projects=tuple(projects[:2]),
        contractors=tuple(contractors[:1]),
        actions_required=tuple(actions_required[:3]),
    )
