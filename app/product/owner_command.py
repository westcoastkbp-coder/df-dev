from __future__ import annotations

from dataclasses import asdict, dataclass
from collections.abc import Mapping, Sequence
from pathlib import Path

from app.execution.paths import LOGS_DIR, ROOT_DIR
from app.policy.policy_gate import evaluate_task_creation_policy, policy_result_as_dict
from app.orchestrator import task_factory
from runtime.system_log import log_event, write_json_log


ALLOWED_OWNER_COMMANDS = {
    "override_decision",
    "change_priority",
    "approve_all_pending",
    "reject_task",
    "force_recompute",
}
ALLOWED_DECISION_PRIORITIES = {"low", "medium", "high", "urgent"}
ALLOWED_OVERRIDE_FIELDS = {
    "reason",
    "recommended_action",
    "recommended_actions",
    "confidence",
}
OWNER_COMMAND_LOG_FILE = ROOT_DIR / LOGS_DIR / "owner_commands.log"


class OwnerCommandError(ValueError):
    """Raised when an owner command is invalid or unsupported."""


@dataclass(frozen=True, slots=True)
class CommandLog:
    user: str
    command_type: str
    target: str
    change_applied: dict[str, object]

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_mapping(value: object, *, field_name: str) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise OwnerCommandError(f"{field_name} must be a dict")
    return dict(value)


def _normalize_actor(command: Mapping[str, object]) -> str:
    actor = _normalize_text(
        command.get("who") or command.get("actor") or command.get("owner")
    )
    if not actor:
        raise OwnerCommandError("who must not be empty")
    return actor


def _normalize_reason(command: Mapping[str, object]) -> str:
    reason = _normalize_text(command.get("why") or command.get("reason"))
    if not reason:
        raise OwnerCommandError("why must not be empty")
    return reason


def _task_payload(task: Mapping[str, object]) -> dict[str, object]:
    payload = task.get("payload")
    return dict(payload) if isinstance(payload, Mapping) else {}


def _task_decision(task: Mapping[str, object]) -> dict[str, object]:
    payload = _task_payload(task)
    decision = payload.get("decision")
    return dict(decision) if isinstance(decision, Mapping) else {}


def _resolve_task(
    *,
    task_id: object = "",
    decision_id: object = "",
    store_path: Path | None = None,
) -> dict[str, object]:
    normalized_task_id = _normalize_text(task_id)
    if normalized_task_id:
        task = task_factory.get_task(normalized_task_id, store_path)
        if task is None:
            raise OwnerCommandError(f"task not found: {normalized_task_id}")
        return dict(task)

    normalized_decision_id = _normalize_text(decision_id)
    if not normalized_decision_id:
        raise OwnerCommandError("task_id or decision_id is required")
    for task in task_factory.load_tasks(store_path):
        decision = _task_decision(task)
        if _normalize_text(decision.get("decision_id")) == normalized_decision_id:
            return dict(task)
    raise OwnerCommandError(f"decision not found: {normalized_decision_id}")


def _write_owner_command_log(
    *,
    command_type: str,
    actor: str,
    reason: str,
    changed: Sequence[dict[str, object]],
    target: str,
    task_id: str = "",
) -> None:
    change_applied = {"reason": reason, "changes": [dict(item) for item in changed]}
    command_log = CommandLog(
        user=actor,
        command_type=command_type,
        target=target,
        change_applied=change_applied,
    )
    write_json_log(
        OWNER_COMMAND_LOG_FILE,
        task_id=task_id,
        event_type="command_log",
        status="recorded",
        details=command_log.as_dict(),
    )
    log_event(
        "owner_command",
        {
            "user": actor,
            "command_type": command_type,
            "target": target,
            "change_applied": change_applied,
        },
        task_id=task_id,
    )


def _policy_validate_task_update(
    task: Mapping[str, object],
    *,
    store_path: Path | None,
) -> dict[str, object]:
    parent_task_id = _normalize_text(
        task.get("parent_task_id") or _task_payload(task).get("parent_task_id")
    )
    existing_tasks = task_factory.load_tasks(store_path)
    parent_task = {}
    if parent_task_id:
        parent_task = next(
            (
                dict(existing_task)
                for existing_task in existing_tasks
                if _normalize_text(existing_task.get("task_id")) == parent_task_id
            ),
            {},
        )
    policy_result = evaluate_task_creation_policy(
        dict(task),
        parent_task=parent_task or None,
        existing_tasks=existing_tasks,
    )
    if not policy_result.execution_allowed:
        raise OwnerCommandError(
            f"policy gate blocked owner command change: {policy_result.reason}"
        )
    return policy_result_as_dict(policy_result)


def _record_task_command(
    task: dict[str, object],
    *,
    event: str,
    actor: str,
    reason: str,
    changes: Mapping[str, object],
    store_path: Path | None = None,
) -> dict[str, object]:
    policy_trace = _policy_validate_task_update(task, store_path=store_path)
    task_factory.append_history_event(
        task,
        action=event,
        data={
            "who": actor,
            "reason": reason,
            "changes": dict(changes),
            "policy_gate": policy_trace,
            "status": _normalize_text(task.get("status")),
        },
    )
    return task_factory.save_task(task, store_path)


def _handle_override_decision(
    command: Mapping[str, object],
    *,
    actor: str,
    reason: str,
    store_path: Path | None,
) -> dict[str, object]:
    changes = _normalize_mapping(command.get("changes"), field_name="changes")
    unsupported = sorted(set(changes) - ALLOWED_OVERRIDE_FIELDS)
    if unsupported:
        raise OwnerCommandError(
            "override_decision contains unsupported fields: " + ", ".join(unsupported)
        )
    task = _resolve_task(
        task_id=command.get("task_id"),
        decision_id=command.get("decision_id"),
        store_path=store_path,
    )
    payload = _task_payload(task)
    decision = _task_decision(task)
    if not decision:
        raise OwnerCommandError("target task does not contain a decision payload")

    applied: dict[str, object] = {}
    if "reason" in changes:
        decision["reason"] = _normalize_text(changes.get("reason"))
        applied["reason"] = decision["reason"]
    if "recommended_action" in changes:
        recommended_action = _normalize_text(changes.get("recommended_action"))
        if not recommended_action:
            raise OwnerCommandError("recommended_action must not be empty")
        decision["recommended_actions"] = [recommended_action]
        payload["recommended_action"] = recommended_action
        applied["recommended_actions"] = [recommended_action]
    if "recommended_actions" in changes:
        raw_actions = changes.get("recommended_actions")
        if not isinstance(raw_actions, Sequence) or isinstance(
            raw_actions, (str, bytes, bytearray)
        ):
            raise OwnerCommandError("recommended_actions must be a list")
        recommended_actions = [
            _normalize_text(item) for item in raw_actions if _normalize_text(item)
        ]
        if not recommended_actions:
            raise OwnerCommandError("recommended_actions must not be empty")
        decision["recommended_actions"] = recommended_actions
        payload["recommended_action"] = recommended_actions[0]
        applied["recommended_actions"] = list(recommended_actions)
    if "confidence" in changes:
        try:
            confidence = float(changes.get("confidence"))
        except (TypeError, ValueError) as exc:
            raise OwnerCommandError("confidence must be numeric") from exc
        decision["confidence"] = round(confidence, 2)
        applied["confidence"] = decision["confidence"]

    if not applied:
        raise OwnerCommandError(
            "override_decision requires at least one supported change"
        )

    payload["decision"] = decision
    task["payload"] = payload
    updated_task = _record_task_command(
        task,
        event="owner_override_decision",
        actor=actor,
        reason=reason,
        changes=applied,
        store_path=store_path,
    )
    _write_owner_command_log(
        command_type="override_decision",
        actor=actor,
        reason=reason,
        changed=[applied],
        target=_normalize_text(decision.get("decision_id"))
        or _normalize_text(updated_task.get("task_id")),
        task_id=_normalize_text(updated_task.get("task_id")),
    )
    return {
        "status": "accepted",
        "command_type": "override_decision",
        "task_id": _normalize_text(updated_task.get("task_id")),
        "decision_id": _normalize_text(decision.get("decision_id")),
        "changed": applied,
    }


def _handle_change_priority(
    command: Mapping[str, object],
    *,
    actor: str,
    reason: str,
    store_path: Path | None,
) -> dict[str, object]:
    priority = _normalize_text(command.get("priority")).lower()
    if priority not in ALLOWED_DECISION_PRIORITIES:
        raise OwnerCommandError(
            "priority must be one of: " + ", ".join(sorted(ALLOWED_DECISION_PRIORITIES))
        )
    task = _resolve_task(
        task_id=command.get("task_id"),
        decision_id=command.get("decision_id"),
        store_path=store_path,
    )
    payload = _task_payload(task)
    payload["priority"] = priority
    decision = _task_decision(task)
    changed: dict[str, object] = {"priority": priority}
    if decision:
        decision["priority"] = priority
        payload["decision"] = decision
        changed["decision_id"] = _normalize_text(decision.get("decision_id"))
    task["payload"] = payload
    updated_task = _record_task_command(
        task,
        event="owner_change_priority",
        actor=actor,
        reason=reason,
        changes=changed,
        store_path=store_path,
    )
    _write_owner_command_log(
        command_type="change_priority",
        actor=actor,
        reason=reason,
        changed=[changed],
        target=_normalize_text(decision.get("decision_id"))
        or _normalize_text(updated_task.get("task_id")),
        task_id=_normalize_text(updated_task.get("task_id")),
    )
    return {
        "status": "accepted",
        "command_type": "change_priority",
        "task_id": _normalize_text(updated_task.get("task_id")),
        "changed": changed,
    }


def _handle_approve_all_pending(
    command: Mapping[str, object],
    *,
    actor: str,
    reason: str,
    store_path: Path | None,
) -> dict[str, object]:
    approved_tasks: list[dict[str, object]] = []
    for task in task_factory.load_tasks(store_path):
        if _normalize_text(task.get("approval_status")).lower() != "pending":
            continue
        if _normalize_text(task.get("status")) != "AWAITING_APPROVAL":
            continue
        approved = task_factory.apply_task_approval(
            task.get("task_id"),
            approved=True,
            approved_by=actor,
            store_path=store_path,
        )
        approved_tasks.append(
            {
                "task_id": _normalize_text(approved.get("task_id")),
                "status": _normalize_text(approved.get("status")),
                "approval_status": _normalize_text(approved.get("approval_status")),
            }
        )
    _write_owner_command_log(
        command_type="approve_all_pending",
        actor=actor,
        reason=reason,
        changed=approved_tasks,
        target="all_pending_approvals",
    )
    return {
        "status": "accepted",
        "command_type": "approve_all_pending",
        "approved_count": len(approved_tasks),
        "changed": approved_tasks,
    }


def _handle_reject_task(
    command: Mapping[str, object],
    *,
    actor: str,
    reason: str,
    store_path: Path | None,
) -> dict[str, object]:
    task = _resolve_task(
        task_id=command.get("task_id"),
        decision_id=command.get("decision_id"),
        store_path=store_path,
    )
    if (
        _normalize_text(task.get("status")) == "AWAITING_APPROVAL"
        and _normalize_text(task.get("approval_status")).lower() == "pending"
    ):
        rejected = task_factory.apply_task_approval(
            task.get("task_id"),
            approved=False,
            approved_by=actor,
            store_path=store_path,
        )
    else:
        current_status = task_factory.normalize_task_status(task.get("status"))
        if current_status in {"COMPLETED", "FAILED"}:
            raise OwnerCommandError("reject_task cannot change a terminal task")
        task["approval_status"] = "rejected"
        task["rejected_at"] = task_factory.now()
        task["rejected_by"] = actor
        task_factory.transition_task_status(
            task,
            "FAILED",
            reason=f"owner_rejected:{reason}",
        )
        rejected = _record_task_command(
            task,
            event="owner_reject_task",
            actor=actor,
            reason=reason,
            changes={"approval_status": "rejected"},
            store_path=store_path,
        )
    changed = {
        "task_id": _normalize_text(rejected.get("task_id")),
        "status": _normalize_text(rejected.get("status")),
        "approval_status": _normalize_text(rejected.get("approval_status")),
    }
    _write_owner_command_log(
        command_type="reject_task",
        actor=actor,
        reason=reason,
        changed=[changed],
        target=changed["task_id"],
        task_id=changed["task_id"],
    )
    return {
        "status": "accepted",
        "command_type": "reject_task",
        "task_id": changed["task_id"],
        "changed": changed,
    }


def _handle_force_recompute(
    command: Mapping[str, object],
    *,
    actor: str,
    reason: str,
    store_path: Path | None,
) -> dict[str, object]:
    task = _resolve_task(
        task_id=command.get("task_id"),
        decision_id=command.get("decision_id"),
        store_path=store_path,
    )
    timestamp = task_factory.now()
    payload = _task_payload(task)
    payload["force_recompute"] = {
        "requested": True,
        "requested_by": actor,
        "reason": reason,
        "requested_at": timestamp,
    }
    decision = _task_decision(task)
    if decision:
        decision["recompute_requested"] = True
        decision["recompute_requested_by"] = actor
        decision["recompute_reason"] = reason
        decision["recompute_requested_at"] = timestamp
        payload["decision"] = decision
    task["payload"] = payload
    current_status = task_factory.normalize_task_status(task.get("status"))
    if current_status == "EXECUTING":
        task_factory.transition_task_status(
            task,
            "DEFERRED",
            reason="owner_force_recompute_requested",
        )
    updated_task = _record_task_command(
        task,
        event="owner_force_recompute",
        actor=actor,
        reason=reason,
        changes={"force_recompute": True},
        store_path=store_path,
    )
    changed = {
        "task_id": _normalize_text(updated_task.get("task_id")),
        "status": _normalize_text(updated_task.get("status")),
        "decision_id": _normalize_text(decision.get("decision_id")) if decision else "",
        "force_recompute": True,
    }
    _write_owner_command_log(
        command_type="force_recompute",
        actor=actor,
        reason=reason,
        changed=[changed],
        target=changed["decision_id"] or changed["task_id"],
        task_id=changed["task_id"],
    )
    return {
        "status": "accepted",
        "command_type": "force_recompute",
        "task_id": changed["task_id"],
        "changed": changed,
    }


def owner_command(
    input_data: Mapping[str, object] | object,
    *,
    store_path: Path | None = None,
) -> dict[str, object]:
    command = _normalize_mapping(input_data, field_name="owner_command")
    command_type = _normalize_text(command.get("command_type")).lower()
    if command_type not in ALLOWED_OWNER_COMMANDS:
        raise OwnerCommandError(
            "command_type must be one of: " + ", ".join(sorted(ALLOWED_OWNER_COMMANDS))
        )
    actor = _normalize_actor(command)
    reason = _normalize_reason(command)

    if command_type == "override_decision":
        return _handle_override_decision(
            command,
            actor=actor,
            reason=reason,
            store_path=store_path,
        )
    if command_type == "change_priority":
        return _handle_change_priority(
            command,
            actor=actor,
            reason=reason,
            store_path=store_path,
        )
    if command_type == "approve_all_pending":
        return _handle_approve_all_pending(
            command,
            actor=actor,
            reason=reason,
            store_path=store_path,
        )
    if command_type == "reject_task":
        return _handle_reject_task(
            command,
            actor=actor,
            reason=reason,
            store_path=store_path,
        )
    if command_type == "force_recompute":
        return _handle_force_recompute(
            command,
            actor=actor,
            reason=reason,
            store_path=store_path,
        )
    raise OwnerCommandError(f"unsupported command_type: {command_type}")
