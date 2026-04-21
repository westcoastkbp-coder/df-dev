from __future__ import annotations

from collections.abc import Mapping

TASK_INTAKE_CONTRACT_FIELDS = (
    "linear_task_id",
    "linear_task_title",
    "mvp_priority",
    "expected_result",
    "done_condition",
)
VALID_MVP_PRIORITIES = ("P0", "P1", "P2")
MVP_PRIORITY_TO_PRIORITY = {
    "P0": "high",
    "P1": "medium",
    "P2": "low",
}
WORKFLOW_PHASES = ("intake", "implement", "validate", "report")
LINEAR_STATUS_BY_PHASE = {
    "intake": "Todo",
    "implement": "In Progress",
    "validate": "In Review",
    "report": "Done",
}
VALID_LINEAR_STATUSES = tuple(dict.fromkeys(LINEAR_STATUS_BY_PHASE.values()))
IMPLEMENT_PHASE_ROLES = {"Architect", "Implementer", "Reviewer"}


def clean_text(value: object) -> str:
    return str(value or "").strip()


def format_linear_task_reference(
    linear_task_id: object, linear_task_title: object
) -> str:
    task_id = clean_text(linear_task_id)
    task_title = clean_text(linear_task_title)
    if task_id and task_title:
        return f"{task_id} - {task_title}"
    return task_id or task_title or "unknown Linear task"


def normalize_mvp_priority(priority: object) -> str:
    normalized = clean_text(priority).upper()
    if normalized not in VALID_MVP_PRIORITIES:
        raise ValueError(
            "mvp_priority must be one of: " + ", ".join(VALID_MVP_PRIORITIES)
        )
    return normalized


def priority_for_mvp_priority(mvp_priority: object) -> str:
    return MVP_PRIORITY_TO_PRIORITY[normalize_mvp_priority(mvp_priority)]


def validate_task_intake_contract(payload: Mapping[str, object]) -> tuple[str, ...]:
    errors: list[str] = []

    for field in TASK_INTAKE_CONTRACT_FIELDS:
        if not clean_text(payload.get(field)):
            errors.append(f"missing `{field}` in Task Intake Contract")

    if clean_text(payload.get("mvp_priority")):
        try:
            normalize_mvp_priority(payload.get("mvp_priority"))
        except ValueError as exc:
            errors.append(str(exc))

    return tuple(errors)


def normalize_task_intake_payload(payload: Mapping[str, object]) -> dict:
    errors = validate_task_intake_contract(payload)
    if errors:
        raise ValueError("; ".join(errors))

    normalized = dict(payload)
    normalized["linear_task_id"] = clean_text(payload.get("linear_task_id"))
    normalized["linear_task_title"] = clean_text(payload.get("linear_task_title"))
    normalized["mvp_priority"] = normalize_mvp_priority(payload.get("mvp_priority"))
    normalized["expected_result"] = clean_text(payload.get("expected_result"))
    normalized["done_condition"] = clean_text(payload.get("done_condition"))

    if not clean_text(payload.get("goal")):
        normalized["goal"] = normalized["linear_task_title"]

    if not clean_text(payload.get("expected_output")):
        normalized["expected_output"] = normalized["expected_result"]

    if not clean_text(payload.get("priority")):
        normalized["priority"] = priority_for_mvp_priority(normalized["mvp_priority"])

    return normalized


def workflow_phase_for_logical_role(logical_role: object) -> str:
    role = clean_text(logical_role)
    if role == "QA":
        return "validate"
    if role == "Reporter":
        return "report"
    if role in IMPLEMENT_PHASE_ROLES:
        return "implement"
    return "implement"


def normalize_done_condition_met(value: object) -> bool:
    if isinstance(value, bool):
        return value

    normalized = clean_text(value).lower()
    return normalized in {"true", "1", "yes"}


def linear_status_for_phase(phase: object, *, done_condition_met: bool) -> str:
    normalized_phase = clean_text(phase).lower()
    if normalized_phase not in WORKFLOW_PHASES:
        raise ValueError(f"unsupported workflow phase: {phase}")

    if normalized_phase == "report" and not done_condition_met:
        return LINEAR_STATUS_BY_PHASE["validate"]

    return LINEAR_STATUS_BY_PHASE[normalized_phase]


def next_step_for_phase(phase: object, *, done_condition_met: bool) -> str:
    normalized_phase = clean_text(phase).lower()
    if normalized_phase == "intake":
        return "Start implementation from the Linear task contract."
    if normalized_phase == "implement":
        return "Move the task into validation after implementation is ready."
    if normalized_phase == "validate":
        if done_condition_met:
            return "Sync the validated result and close the task in Linear."
        return "Finish the remaining done condition before moving the task to Done."
    if normalized_phase == "report":
        if done_condition_met:
            return "No next step. The task can remain in Done."
        return (
            "Complete the remaining done condition, then rerun validation/report sync."
        )
    return "Review the task state and continue from Linear."
