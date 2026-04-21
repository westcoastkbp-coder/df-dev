from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Iterable, Mapping
from pathlib import Path

from app.context.shared_context_store import append_event, get_context, set_context
from app.execution.input_normalizer import normalize_input
from app.execution.paths import TASK_SYSTEM_FILE
from app.execution.task_schema import (
    TASK_CONTRACT_VERSION,
    Task,
    validate_task_contract,
    validate_task_lineage,
)
from app.orchestrator.task_lifecycle import (
    OPEN_TASK_STATUSES,
    can_transition_task_status as lifecycle_can_transition_task_status,
    normalize_task_status,
    transition_task_status as lifecycle_transition_task_status,
)
from app.orchestrator.task_state_store import (
    initialize_database,
    read_all_tasks,
    read_task,
    run_in_transaction,
    task_row,
    write_task,
)

ALLOWED_SOURCES = {"voice", "api", "internal"}
ALLOWED_EXECUTION_MODES = {"auto", "confirmation", "strict"}
ALLOWED_APPROVAL_STATUSES = {"pending", "approved", "rejected"}

_TASK_STORE: list[dict[str, object]] = []
_TASK_INDEX: dict[str, dict[str, object]] = {}
_STORE_SOURCE: str | None = None
_TASK_SEQUENCE = 0


def now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def next_task_id() -> str:
    global _TASK_SEQUENCE
    _TASK_SEQUENCE += 1
    return f"{int(time.time() * 1000)}-{_TASK_SEQUENCE}"


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_payload(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _normalize_idempotency_value(value: object) -> object:
    if isinstance(value, Mapping):
        return {
            _normalize_text(key): _normalize_idempotency_value(item)
            for key, item in sorted(
                dict(value).items(), key=lambda item: _normalize_text(item[0])
            )
        }
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, Mapping)):
        return [_normalize_idempotency_value(item) for item in value]
    if isinstance(value, str):
        return _normalize_text(value)
    return value


def _idempotency_payload_hash(payload: Mapping[str, object] | object) -> str:
    normalized_payload = _normalize_idempotency_value(payload)
    serialized_payload = json.dumps(
        normalized_payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(serialized_payload.encode("utf-8")).hexdigest()


def _normalize_notes(value: object) -> list[str]:
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes, Mapping)):
        return [_normalize_text(item) for item in value if _normalize_text(item)]
    normalized = _normalize_text(value)
    return [normalized] if normalized else []


def _build_text_signal_idempotency_key(
    *,
    intent: object,
    payload: Mapping[str, object],
    task_type: object,
    parent_task_id: object,
    execution_mode: object,
) -> str:
    normalized_intent = _normalize_text(intent) or "generic_task"
    normalized_task_type = _normalize_text(task_type)
    normalized_parent_task_id = _normalize_text(parent_task_id)
    normalized_execution_mode = _normalize_text(execution_mode)
    identity_payload = {
        "intent": normalized_intent,
        "task_type": normalized_task_type,
        "parent_task_id": normalized_parent_task_id,
        "execution_mode": normalized_execution_mode,
        "payload": _normalize_idempotency_value(payload),
    }
    return f"text_signal:{_idempotency_payload_hash(identity_payload)}"


def _normalize_source(value: object) -> str:
    normalized = _normalize_text(value).lower()
    return normalized if normalized in ALLOWED_SOURCES else "internal"


def _normalize_execution_mode(value: object) -> str:
    normalized = _normalize_text(value).lower()
    return normalized if normalized in ALLOWED_EXECUTION_MODES else ""


def _normalize_approval_status(value: object) -> str:
    normalized = _normalize_text(value).lower()
    return normalized if normalized in ALLOWED_APPROVAL_STATUSES else ""


def _default_approval_status(*, execution_mode: str, requested_status: str) -> str:
    if requested_status == "AWAITING_APPROVAL":
        return "pending"
    if execution_mode in {"confirmation", "strict"}:
        return "pending"
    return "approved"


def _constraint_items(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        items = value.replace("\n", ";").split(";")
        return tuple(item.strip() for item in items if item.strip())
    if isinstance(value, Iterable) and not isinstance(value, (bytes, Mapping)):
        return tuple(_normalize_text(item) for item in value if _normalize_text(item))
    return ()


def build_idempotency_key(
    *,
    lead_id: object,
    workflow_type: object,
    step_name: object,
    payload: Mapping[str, object] | object,
) -> str:
    normalized_lead_id = _normalize_text(lead_id)
    normalized_workflow_type = _normalize_text(workflow_type)
    normalized_step_name = _normalize_text(step_name)
    payload_hash = _idempotency_payload_hash(payload)
    return f"{normalized_lead_id}:{normalized_workflow_type}:{normalized_step_name}:{payload_hash}"


def find_task_by_idempotency_key(
    idempotency_key: object,
    *,
    store_path: Path | None = None,
) -> dict[str, object] | None:
    normalized_key = _normalize_text(idempotency_key)
    if not normalized_key:
        return None
    for task in load_tasks(store_path):
        if _normalize_text(task.get("idempotency_key")) == normalized_key:
            return dict(task)
        payload = _normalize_payload(task.get("payload"))
        if _normalize_text(payload.get("idempotency_key")) == normalized_key:
            return dict(task)
    return None


def normalize_task_input(input_data: Mapping[str, object]) -> dict[str, object]:
    payload = _normalize_payload(input_data.get("payload"))
    explicit_text = _normalize_text(input_data.get("text"))
    execution_mode = _normalize_execution_mode(
        input_data.get("execution_mode") or payload.get("execution_mode")
    )
    requested_status = normalize_task_status(input_data.get("status"))
    approval_status = _normalize_approval_status(
        input_data.get("approval_status")
    ) or _default_approval_status(
        execution_mode=execution_mode,
        requested_status=requested_status,
    )
    normalized_intent, normalized_payload = normalize_input(
        text=input_data.get("text"),
        intent=input_data.get("intent"),
        payload=payload,
    )
    parent_task_id = _normalize_text(
        input_data.get("parent_task_id") or normalized_payload.get("parent_task_id")
    )
    task_type = _normalize_text(input_data.get("task_type"))
    idempotency_key = _normalize_text(
        input_data.get("idempotency_key") or normalized_payload.get("idempotency_key")
    )
    has_explicit_task_id = bool(_normalize_text(input_data.get("task_id")))
    if not idempotency_key and not has_explicit_task_id and explicit_text:
        idempotency_key = _build_text_signal_idempotency_key(
            intent=normalized_intent,
            payload=normalized_payload,
            task_type=task_type,
            parent_task_id=parent_task_id,
            execution_mode=execution_mode,
        )
        normalized_payload["idempotency_key"] = idempotency_key
    task_id = _normalize_text(input_data.get("task_id")) or next_task_id()
    interaction_id = _normalize_text(
        input_data.get("interaction_id")
        or normalized_payload.get("interaction_id")
        or normalized_payload.get("session_id")
        or task_id
    )
    job_id = _normalize_text(input_data.get("job_id")) or task_id
    trace_id = _normalize_text(
        input_data.get("trace_id") or normalized_payload.get("trace_id") or job_id
    )
    return {
        "task_id": task_id,
        "created_at": _normalize_text(input_data.get("created_at")) or now(),
        "last_updated_at": _normalize_text(input_data.get("last_updated_at"))
        or _normalize_text(input_data.get("created_at"))
        or now(),
        "source": _normalize_source(input_data.get("source")),
        "intent": normalized_intent,
        "payload": normalized_payload,
        "status": requested_status or "CREATED",
        "approval_status": approval_status,
        "notes": _normalize_notes(input_data.get("notes")),
        "interaction_id": interaction_id,
        "job_id": job_id,
        "trace_id": trace_id,
        "task_type": task_type,
        "parent_task_id": parent_task_id,
        "parent_task_type": _normalize_text(
            input_data.get("parent_task_type")
            or normalized_payload.get("parent_task_type")
        ),
        "execution_mode": execution_mode,
        "approved_at": _normalize_text(input_data.get("approved_at")),
        "approved_by": _normalize_text(input_data.get("approved_by")),
        "rejected_at": _normalize_text(input_data.get("rejected_at")),
        "rejected_by": _normalize_text(input_data.get("rejected_by")),
        "execution_location": _normalize_text(input_data.get("execution_location")),
        "offload_latency": input_data.get("offload_latency"),
        "routing_reason": (
            dict(input_data.get("routing_reason", {}))
            if isinstance(input_data.get("routing_reason"), Mapping)
            else None
        ),
        "telemetry_snapshot": (
            dict(input_data.get("telemetry_snapshot", {}))
            if isinstance(input_data.get("telemetry_snapshot"), Mapping)
            else None
        ),
        "safety_override": (
            dict(input_data.get("safety_override", {}))
            if isinstance(input_data.get("safety_override"), Mapping)
            else None
        ),
        "network_snapshot": (
            dict(input_data.get("network_snapshot", {}))
            if isinstance(input_data.get("network_snapshot"), Mapping)
            else None
        ),
        "network_policy": (
            dict(input_data.get("network_policy", {}))
            if isinstance(input_data.get("network_policy"), Mapping)
            else None
        ),
        "constraints": input_data.get(
            "constraints", normalized_payload.get("constraints", "")
        ),
        "target_environment": input_data.get("target_environment"),
        "treat_as_dev_environment": input_data.get("treat_as_dev_environment"),
        "allow_code_generation": input_data.get("allow_code_generation"),
    }


def policy_input_from_task_input(input_data: Mapping[str, object]) -> dict[str, object]:
    payload = _normalize_payload(input_data.get("payload"))
    policy_input: dict[str, object] = {
        "target_environment": _normalize_text(
            input_data.get("target_environment") or payload.get("target_environment")
        ),
        "treat_as_dev_environment": bool(
            input_data.get("treat_as_dev_environment")
            or payload.get("treat_as_dev_environment")
        ),
        "allow_code_generation": bool(
            input_data.get("allow_code_generation")
            or payload.get("allow_code_generation")
        ),
        "context_scope": [],
        "assumptions": {},
        "session_context": {},
    }

    raw_constraints = input_data.get("constraints", payload.get("constraints", ""))
    for raw_item in _constraint_items(raw_constraints):
        key, separator, value = raw_item.partition("=")
        if separator != "=":
            continue
        normalized_key = key.strip().lower()
        normalized_value = value.strip()
        lowered_value = normalized_value.lower()
        bool_value = lowered_value in {"true", "1", "yes"}
        if normalized_key == "target_environment":
            policy_input["target_environment"] = normalized_value
        elif normalized_key == "treat_as_dev_environment":
            policy_input["treat_as_dev_environment"] = bool_value
        elif normalized_key == "allow_code_generation":
            policy_input["allow_code_generation"] = bool_value
    return policy_input


def _ensure_store(store_path: Path) -> None:
    initialize_database(store_path)


def _sync_memory_index(
    tasks: list[dict[str, object]],
    store_path: Path | None = None,
) -> list[dict[str, object]]:
    global _TASK_STORE, _TASK_INDEX, _STORE_SOURCE
    _TASK_STORE = [dict(task) for task in tasks]
    _TASK_INDEX = {
        _normalize_text(task.get("task_id")): task
        for task in _TASK_STORE
        if _normalize_text(task.get("task_id"))
    }
    _STORE_SOURCE = str(store_path or TASK_SYSTEM_FILE)
    return _TASK_STORE


def clear_task_runtime_store() -> None:
    global _TASK_STORE, _TASK_INDEX, _STORE_SOURCE, _TASK_SEQUENCE
    _TASK_STORE = []
    _TASK_INDEX = {}
    _STORE_SOURCE = None
    _TASK_SEQUENCE = 0


def _store_source_key(store_path: Path | None = None) -> str:
    return str(store_path or TASK_SYSTEM_FILE)


def _cached_tasks(store_path: Path | None = None) -> list[dict[str, object]] | None:
    if _STORE_SOURCE != _store_source_key(store_path) or not _TASK_STORE:
        return None
    return [dict(task) for task in _TASK_STORE]


def _update_cached_task(
    task_data: dict[str, object], store_path: Path | None = None
) -> None:
    target = store_path or TASK_SYSTEM_FILE
    cached_tasks = _cached_tasks(target)
    if cached_tasks is None:
        return
    normalized_task_id = _normalize_text(task_data.get("task_id"))
    updated_tasks: list[dict[str, object]] = []
    replaced = False
    for existing_task in cached_tasks:
        if _normalize_text(existing_task.get("task_id")) == normalized_task_id:
            updated_tasks.append(dict(task_data))
            replaced = True
        else:
            updated_tasks.append(existing_task)
    if not replaced:
        updated_tasks.append(dict(task_data))
    _sync_memory_index(updated_tasks, target)


def load_tasks(store_path: Path | None = None) -> list[dict[str, object]]:
    target = store_path or TASK_SYSTEM_FILE
    cached_tasks = _cached_tasks(target)
    if cached_tasks is not None:
        return cached_tasks
    _ensure_store(target)
    tasks = read_all_tasks(target)
    validated_tasks = [validate_task_contract(dict(task)) for task in tasks]
    return [dict(task) for task in _sync_memory_index(validated_tasks, target)]


def _save_store(tasks: list[dict[str, object]], store_path: Path | None = None) -> None:
    target = store_path or TASK_SYSTEM_FILE
    _ensure_store(target)

    def write_all(connection) -> None:
        connection.execute("DELETE FROM Task WHERE memory_ref = ''")
        for task in tasks:
            row = task_row(dict(task))
            connection.execute(
                """
                INSERT OR REPLACE INTO Task (
                    task_id, status, descriptor, result, created_at,
                    updated_at, control_fields, memory_ref
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["task_id"],
                    row["status"],
                    row["descriptor"],
                    row["result"],
                    row["created_at"],
                    row["updated_at"],
                    row["control_fields"],
                    row["memory_ref"],
                ),
            )

    run_in_transaction(
        write_all, store_path=target, operation_name="replace_task_store"
    )
    _sync_memory_index(tasks, target)


def save_task(
    task_data: dict[str, object], store_path: Path | None = None
) -> dict[str, object]:
    from app.policy.policy_gate import evaluate_task_creation_policy

    existing_tasks = load_tasks(store_path)
    persisted_task = dict(task_data)
    persisted_task.setdefault(
        "last_updated_at",
        _normalize_text(persisted_task.get("created_at")) or now(),
    )
    persisted_task = validate_task_contract(persisted_task)
    parent_task_id = _normalize_text(
        persisted_task.get("parent_task_id")
        or _normalize_payload(persisted_task.get("payload")).get("parent_task_id")
    )
    parent_task = None
    if parent_task_id:
        for existing_task in existing_tasks:
            if _normalize_text(existing_task.get("task_id")) == parent_task_id:
                parent_task = dict(existing_task)
                break
    creation_policy = evaluate_task_creation_policy(
        persisted_task,
        parent_task=parent_task,
        existing_tasks=existing_tasks,
    )
    if not creation_policy.execution_allowed:
        raise ValueError(f"policy gate blocked task creation: {creation_policy.reason}")
    payload = _normalize_payload(persisted_task.get("payload"))
    if _normalize_text(
        persisted_task.get("intent")
    ) == "system_improvement_task" and bool(
        creation_policy.policy_trace.get("core_impact")
    ):
        # Core-targeted improvement tasks may be created, but they must remain
        # parked in the approval queue until a high-approval path unlocks them.
        payload["core_impact"] = True
        payload["requires_approval"] = True
        payload["route_target"] = "approval_queue"
        payload["auto_task_mode"] = "approval_queue"
        payload["priority"] = "HIGH"
        persisted_task["payload"] = payload
        persisted_task["status"] = "AWAITING_APPROVAL"
        persisted_task["approval_status"] = "pending"
        persisted_task["approved_at"] = ""
        persisted_task["approved_by"] = ""
    persisted_task = validate_task_lineage(
        persisted_task,
        parent_task=parent_task,
        existing_tasks=existing_tasks,
    )
    persisted_task = write_task(persisted_task, store_path or TASK_SYSTEM_FILE)
    _update_cached_task(persisted_task, store_path)
    return dict(persisted_task)


def append_history_event(
    task_data: dict[str, object],
    *,
    action: str,
    data: Mapping[str, object] | None = None,
) -> dict[str, object]:
    history = list(task_data.get("history", []))
    payload = dict(data or {})
    payload.setdefault(
        "interaction_id", _normalize_text(task_data.get("interaction_id"))
    )
    payload.setdefault(
        "job_id", _normalize_text(task_data.get("job_id") or task_data.get("task_id"))
    )
    payload.setdefault(
        "trace_id",
        _normalize_text(task_data.get("trace_id") or task_data.get("task_id")),
    )
    history.append(
        {
            "timestamp": now(),
            "event": _normalize_text(action) or "updated",
            "from_status": "",
            "to_status": _normalize_text(
                payload.get("status") or task_data.get("status")
            ),
            "details": payload,
        }
    )
    task_data["history"] = history
    task_data["last_updated_at"] = history[-1]["timestamp"]
    return task_data


def _build_summary(task: Mapping[str, object]) -> str:
    payload = _normalize_payload(task.get("payload"))
    summary = _normalize_text(payload.get("summary"))
    if summary:
        return summary
    text_summary = _normalize_text(payload.get("text"))
    if text_summary:
        return text_summary
    notes = _normalize_notes(task.get("notes"))
    if notes:
        return notes[-1]
    return f"{_normalize_text(task.get('intent')) or 'task'} [{_normalize_text(task.get('status')) or 'created'}]"


def _active_task_context_key(task_id: object) -> str:
    return f"active_task:{_normalize_text(task_id)}"


def _previous_context_snapshot(value: object) -> dict[str, object]:
    if not isinstance(value, Mapping):
        return {}
    return {
        "task_id": _normalize_text(value.get("task_id")),
        "interaction_id": _normalize_text(value.get("interaction_id")),
        "intent": _normalize_text(value.get("intent")),
        "status": _normalize_text(value.get("status")),
        "approval_status": _normalize_text(value.get("approval_status")),
        "summary": _normalize_text(value.get("summary")),
        "updated_at": _normalize_text(
            value.get("updated_at") or value.get("timestamp")
        ),
    }


def _sync_task_context(task: Mapping[str, object], *, event_type: str) -> None:
    normalized_task = dict(task)
    task_id = _normalize_text(normalized_task.get("task_id"))
    if not task_id:
        return
    interaction_id = _normalize_text(normalized_task.get("interaction_id"))
    current_context = get_context(_active_task_context_key(task_id))
    set_context(
        _active_task_context_key(task_id),
        {
            "task_id": task_id,
            "interaction_id": interaction_id,
            "intent": _normalize_text(normalized_task.get("intent")),
            "status": _normalize_text(normalized_task.get("status")),
            "approval_status": _normalize_text(normalized_task.get("approval_status")),
            "summary": _build_summary(normalized_task),
            "payload": dict(normalized_task.get("payload", {}) or {}),
            "history": list(normalized_task.get("history", [])),
            "updated_at": _normalize_text(normalized_task.get("last_updated_at"))
            or now(),
            "previous_context": _previous_context_snapshot(current_context),
        },
        task_id=task_id,
        interaction_id=interaction_id,
        timestamp=_normalize_text(normalized_task.get("last_updated_at")) or now(),
    )
    append_event(
        event_type,
        {
            "task_id": task_id,
            "interaction_id": interaction_id,
            "intent": _normalize_text(normalized_task.get("intent")),
            "status": _normalize_text(normalized_task.get("status")),
            "approval_status": _normalize_text(normalized_task.get("approval_status")),
            "summary": _build_summary(normalized_task),
        },
        task_id=task_id,
        interaction_id=interaction_id,
        timestamp=_normalize_text(normalized_task.get("last_updated_at")) or now(),
    )


def build_task_summary(task: Mapping[str, object]) -> str:
    return _build_summary(task)


def create_task(
    input_data: Mapping[str, object],
    *,
    store_path: Path | None = None,
) -> dict[str, object]:
    requested_status_value = _normalize_text(input_data.get("status"))
    requested_status = normalize_task_status(requested_status_value)
    if requested_status not in {"CREATED", "AWAITING_APPROVAL"}:
        raise ValueError(
            "task must be created with status `created` or `awaiting_approval`"
        )

    normalized = normalize_task_input(input_data)
    existing_task = find_task_by_idempotency_key(
        normalized.get("idempotency_key")
        or dict(normalized.get("payload", {})).get("idempotency_key"),
        store_path=store_path,
    )
    if existing_task is not None:
        return existing_task
    task = Task(
        task_contract_version=TASK_CONTRACT_VERSION,
        task_id=str(normalized["task_id"]),
        created_at=str(normalized["created_at"]),
        last_updated_at=str(normalized["last_updated_at"]),
        intent=str(normalized["intent"]),
        payload=dict(normalized["payload"]),
        status=str(normalized["status"]),
        approval_status=str(normalized["approval_status"]),
        notes=list(normalized["notes"]),
        history=[
            {
                "timestamp": normalized["created_at"],
                "event": (
                    "awaiting_approval"
                    if str(normalized["status"]) == "AWAITING_APPROVAL"
                    else "created"
                ),
                "from_status": "",
                "to_status": str(normalized["status"]),
                "details": {
                    "intent": normalized["intent"],
                    "summary": _build_summary(normalized),
                    "approval_status": normalized["approval_status"],
                    "interaction_id": normalized["interaction_id"],
                    "job_id": normalized["job_id"],
                    "trace_id": normalized["trace_id"],
                },
            }
        ],
        interaction_id=str(normalized.get("interaction_id", "")),
        job_id=str(normalized.get("job_id", "")),
        trace_id=str(normalized.get("trace_id", "")),
        task_type=str(normalized.get("task_type", "")),
        parent_task_id=str(normalized.get("parent_task_id", "")),
        parent_task_type=str(normalized.get("parent_task_type", "")),
        execution_mode=str(normalized.get("execution_mode", "")),
        approved_at=str(normalized.get("approved_at", "")),
        approved_by=str(normalized.get("approved_by", "")),
        rejected_at=str(normalized.get("rejected_at", "")),
        rejected_by=str(normalized.get("rejected_by", "")),
        execution_location=str(normalized.get("execution_location", "")),
        offload_latency=normalized.get("offload_latency"),
        routing_reason=normalized.get("routing_reason"),
        telemetry_snapshot=normalized.get("telemetry_snapshot"),
        safety_override=normalized.get("safety_override"),
        network_snapshot=normalized.get("network_snapshot"),
        network_policy=normalized.get("network_policy"),
    ).as_dict()
    persisted_task = save_task(task, store_path)
    _sync_task_context(persisted_task, event_type="task_created")
    return persisted_task


def get_task(task_id: str, store_path: Path | None = None) -> dict[str, object] | None:
    normalized_task_id = _normalize_text(task_id)
    if not normalized_task_id:
        return None
    task = read_task(normalized_task_id, store_path or TASK_SYSTEM_FILE)
    if task is None:
        load_tasks(store_path)
        task = _TASK_INDEX.get(normalized_task_id)
    return validate_task_contract(dict(task)) if task else None


def get_task_status(
    task_id: str, store_path: Path | None = None
) -> dict[str, object] | None:
    task = get_task(task_id, store_path)
    if task is None:
        return None
    history = list(task.get("history", []))
    return {
        "task_id": task["task_id"],
        "status": _normalize_text(task.get("status")) or "created",
        "summary": _build_summary(task),
        "notes": list(task.get("notes", [])),
        "recent_changes": history[-3:],
    }


def get_recent_tasks(
    limit: int = 10, store_path: Path | None = None
) -> list[dict[str, object]]:
    tasks = load_tasks(store_path)
    normalized_limit = max(0, int(limit))
    if normalized_limit == 0:
        return []
    recent = sorted(
        tasks,
        key=lambda item: (
            _normalize_text(item.get("created_at")),
            _normalize_text(item.get("task_id")),
        ),
        reverse=True,
    )
    return [dict(task) for task in recent[:normalized_limit]]


def get_tasks_by_contact(
    contact_id: str,
    *,
    store_path: Path | None = None,
) -> list[dict[str, object]]:
    normalized_contact_id = _normalize_text(contact_id)
    if not normalized_contact_id:
        return []
    tasks = load_tasks(store_path)
    matches = [
        dict(task)
        for task in tasks
        if _normalize_text(task.get("contact_id")) == normalized_contact_id
        or _normalize_text(_normalize_payload(task.get("payload")).get("contact_id"))
        == normalized_contact_id
    ]
    return sorted(
        matches,
        key=lambda item: (
            _normalize_text(item.get("created_at")),
            _normalize_text(item.get("task_id")),
        ),
        reverse=True,
    )


def get_open_tasks(store_path: Path | None = None) -> list[dict[str, object]]:
    tasks = load_tasks(store_path)
    open_tasks = [
        dict(task)
        for task in tasks
        if normalize_task_status(task.get("status")) in OPEN_TASK_STATUSES
    ]
    return sorted(
        open_tasks,
        key=lambda item: (
            _normalize_text(item.get("created_at")),
            _normalize_text(item.get("task_id")),
        ),
        reverse=True,
    )


def append_note(
    task_id: str, note: object, store_path: Path | None = None
) -> dict[str, object]:
    task = get_task(task_id, store_path)
    if task is None:
        raise ValueError(f"task not found: {task_id}")

    normalized_note = _normalize_text(note)
    if not normalized_note:
        raise ValueError("note must not be empty")

    notes = list(task.get("notes", []))
    notes.append(normalized_note)
    task["notes"] = notes
    history = list(task.get("history", []))
    history.append(
        {
            "timestamp": now(),
            "event": "note_appended",
            "from_status": "",
            "to_status": _normalize_text(task.get("status")),
            "details": {
                "note": normalized_note,
                "notes_count": len(notes),
                "interaction_id": _normalize_text(task.get("interaction_id")),
                "job_id": _normalize_text(task.get("job_id") or task.get("task_id")),
                "trace_id": _normalize_text(
                    task.get("trace_id") or task.get("task_id")
                ),
            },
        }
    )
    task["history"] = history
    return save_task(task, store_path)


def can_transition_task_status(current_status: object, next_status: object) -> bool:
    return lifecycle_can_transition_task_status(current_status, next_status)


def transition_task_status(
    task_data: dict[str, object],
    next_status: object,
    *,
    reason: object = "",
) -> dict[str, object]:
    return lifecycle_transition_task_status(
        task_data,
        next_status,
        timestamp=now(),
        details=_normalize_text(reason),
    )


def close_task(
    task_id: str,
    *,
    status: str = "completed",
    note: object = "",
    store_path: Path | None = None,
) -> dict[str, object]:
    task = get_task(task_id, store_path)
    if task is None:
        raise ValueError(f"task not found: {task_id}")

    normalized_status = normalize_task_status(status)
    if normalized_status not in {"COMPLETED", "FAILED"}:
        raise ValueError("close_task status must be completed or failed")

    lifecycle_transition_task_status(
        task,
        normalized_status,
        timestamp=now(),
        details="close_task",
    )
    normalized_note = _normalize_text(note)
    if normalized_note:
        notes = list(task.get("notes", []))
        notes.append(normalized_note)
        task["notes"] = notes
    persisted_task = save_task(task, store_path)
    if normalized_status == "COMPLETED":
        from app.execution.real_lead_contract import build_followup_context_payload
        from app.execution.followup_reentry import reenter_completed_followup

        reenter_completed_followup(
            build_followup_context_payload(persisted_task), store_path=store_path
        )
    return persisted_task


def apply_task_approval(
    task_id: object,
    *,
    approved: bool,
    approved_by: object,
    store_path: Path | None = None,
) -> dict[str, object]:
    task = get_task(_normalize_text(task_id), store_path)
    if task is None:
        raise ValueError(f"task not found: {task_id}")
    if normalize_task_status(task.get("status")) != "AWAITING_APPROVAL":
        raise ValueError("task is not awaiting approval")
    if _normalize_text(task.get("approval_status")).lower() != "pending":
        raise ValueError("task approval is not pending")

    timestamp = now()
    approver = _normalize_text(approved_by)
    if not approver:
        raise ValueError("approved_by must not be empty")

    if approved:
        task["approval_status"] = "approved"
        task["approved_at"] = timestamp
        task["approved_by"] = approver
        lifecycle_transition_task_status(
            task,
            "VALIDATED",
            timestamp=timestamp,
            details="explicit human approval recorded",
        )
        append_history_event(
            task,
            action="approval_granted",
            data={"approved_by": approver, "status": "VALIDATED"},
        )
    else:
        task["approval_status"] = "rejected"
        task["rejected_at"] = timestamp
        task["rejected_by"] = approver
        lifecycle_transition_task_status(
            task,
            "FAILED",
            timestamp=timestamp,
            details="explicit human rejection recorded",
        )
        append_history_event(
            task,
            action="approval_rejected",
            data={"rejected_by": approver, "status": "FAILED"},
        )
    persisted_task = save_task(task, store_path)
    _sync_task_context(
        persisted_task,
        event_type="approval_granted" if approved else "approval_rejected",
    )
    return persisted_task
