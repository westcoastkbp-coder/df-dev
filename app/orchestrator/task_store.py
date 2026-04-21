from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path

import app.orchestrator.task_factory as task_factory_module
from app.context.shared_context_store import SYSTEM_CONTEXT_KEY, set_context
from app.execution.task_schema import validate_task_contract
from app.orchestrator.task_lifecycle import normalize_task_status


ACTIVE_TASK_LIMIT = 25
TERMINAL_TASK_STATUSES = {"COMPLETED", "FAILED"}


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _root_dir_for_store(store_path: Path | None) -> Path | None:
    if store_path is None:
        return None
    target = Path(store_path)
    parent = target.parent
    if parent.name.lower() == "data":
        return parent.parent
    return parent


def _task_timestamps(task_data: Mapping[str, object]) -> dict[str, str]:
    return {
        "created_at": _normalize_text(task_data.get("created_at")),
        "updated_at": _normalize_text(task_data.get("last_updated_at"))
        or _normalize_text(task_data.get("created_at")),
        "started_at": _normalize_text(task_data.get("started_at")),
        "completed_at": _normalize_text(task_data.get("completed_at")),
        "failed_at": _normalize_text(task_data.get("failed_at")),
    }


def _public_task(task_data: Mapping[str, object]) -> dict[str, object]:
    normalized_task = dict(task_data)
    return {
        "task_id": _normalize_text(normalized_task.get("task_id")),
        "type": _normalize_text(normalized_task.get("intent")),
        "status": _normalize_text(normalized_task.get("status")),
        "input": _normalize_mapping(normalized_task.get("payload")),
        "result": _normalize_mapping(normalized_task.get("result")),
        "history": list(normalized_task.get("history", [])),
        "timestamps": _task_timestamps(normalized_task),
    }


def _task_summary(task_data: Mapping[str, object]) -> dict[str, str]:
    normalized_task = dict(task_data)
    return {
        "task_id": _normalize_text(normalized_task.get("task_id")),
        "type": _normalize_text(normalized_task.get("intent")),
        "status": _normalize_text(normalized_task.get("status")),
        "summary": task_factory_module.build_task_summary(normalized_task),
        "updated_at": _normalize_text(normalized_task.get("last_updated_at"))
        or _normalize_text(normalized_task.get("created_at")),
    }


def sync_active_tasks_context(
    *,
    store_path: Path | None = None,
    task_overrides: Sequence[Mapping[str, object]] | None = None,
    timestamp: object = "",
) -> list[dict[str, str]]:
    tasks_by_id: dict[str, dict[str, object]] = {
        _normalize_text(task.get("task_id")): dict(task)
        for task in task_factory_module.load_tasks(store_path)
        if _normalize_text(task.get("task_id"))
    }
    for task in list(task_overrides or []):
        normalized_task = dict(task)
        task_id = _normalize_text(normalized_task.get("task_id"))
        if not task_id:
            continue
        tasks_by_id[task_id] = normalized_task
    active_tasks = [
        _task_summary(task)
        for task in tasks_by_id.values()
        if _normalize_text(task.get("status")).upper() not in TERMINAL_TASK_STATUSES
    ]
    active_tasks.sort(
        key=lambda item: (
            item["updated_at"],
            item["task_id"],
        ),
        reverse=True,
    )
    summaries = active_tasks[:ACTIVE_TASK_LIMIT]
    set_context(
        SYSTEM_CONTEXT_KEY,
        {"active_tasks": summaries},
        root_dir=_root_dir_for_store(store_path),
        timestamp=_normalize_text(timestamp),
    )
    return summaries


def create_task(
    *,
    task_type: object,
    task_input: Mapping[str, object] | None = None,
    task_id: object = "",
    status: object = "CREATED",
    source: object = "internal",
    store_path: Path | None = None,
) -> dict[str, object]:
    normalized_type = _normalize_text(task_type)
    normalized_input = _normalize_mapping(task_input)
    persisted = task_factory_module.create_task(
        {
            "task_id": _normalize_text(task_id),
            "status": _normalize_text(status) or "CREATED",
            "source": _normalize_text(source) or "internal",
            "intent": normalized_type,
            "payload": normalized_input,
        },
        store_path=store_path,
    )
    persisted["intent"] = normalized_type
    persisted["payload"] = normalized_input
    persisted = task_factory_module.save_task(persisted, store_path=store_path)
    sync_active_tasks_context(
        store_path=store_path,
        task_overrides=[persisted],
        timestamp=persisted.get("last_updated_at") or persisted.get("created_at"),
    )
    return _public_task(persisted)


def get_task(
    task_id: object, *, store_path: Path | None = None
) -> dict[str, object] | None:
    persisted = task_factory_module.get_task(
        _normalize_text(task_id), store_path=store_path
    )
    if persisted is None:
        return None
    return _public_task(persisted)


def update_task(
    task_id: object,
    *,
    status: object = "",
    task_input: Mapping[str, object] | None = None,
    result: Mapping[str, object] | None = None,
    history_event: object = "task_updated",
    decision_trace: Mapping[str, object] | None = None,
    store_path: Path | None = None,
) -> dict[str, object]:
    persisted = task_factory_module.get_task(
        _normalize_text(task_id), store_path=store_path
    )
    if persisted is None:
        raise ValueError(f"task not found: {task_id}")

    next_status = normalize_task_status(status)
    if next_status and next_status != _normalize_text(persisted.get("status")):
        task_factory_module.transition_task_status(
            persisted,
            next_status,
            reason="task_store.update_task",
        )
    if task_input is not None:
        persisted["payload"] = _normalize_mapping(task_input)
    if result is not None:
        persisted["result"] = _normalize_mapping(result)

    details: dict[str, object] = {}
    if task_input is not None:
        details["input_updated"] = True
    if result is not None:
        details["result_updated"] = True
    if isinstance(decision_trace, Mapping):
        details["decision_trace"] = dict(decision_trace)
    if details:
        task_factory_module.append_history_event(
            persisted,
            action=_normalize_text(history_event) or "task_updated",
            data=details,
        )

    saved_task = task_factory_module.save_task(persisted, store_path=store_path)
    sync_active_tasks_context(
        store_path=store_path,
        task_overrides=[saved_task],
        timestamp=saved_task.get("last_updated_at") or saved_task.get("created_at"),
    )
    return _public_task(saved_task)


def complete_task(
    task_id: object,
    *,
    result: Mapping[str, object] | None = None,
    decision_trace: Mapping[str, object] | None = None,
    store_path: Path | None = None,
) -> dict[str, object]:
    persisted = task_factory_module.get_task(
        _normalize_text(task_id), store_path=store_path
    )
    if persisted is None:
        raise ValueError(f"task not found: {task_id}")

    task_factory_module.transition_task_status(
        persisted,
        "COMPLETED",
        reason="task_store.complete_task",
    )
    persisted["result"] = _normalize_mapping(result)
    details = {"result": dict(persisted.get("result", {}) or {})}
    if isinstance(decision_trace, Mapping):
        details["decision_trace"] = dict(decision_trace)
    task_factory_module.append_history_event(
        persisted,
        action="task_completed",
        data=details,
    )
    saved_task = task_factory_module.save_task(persisted, store_path=store_path)
    sync_active_tasks_context(
        store_path=store_path,
        task_overrides=[saved_task],
        timestamp=saved_task.get("last_updated_at") or saved_task.get("created_at"),
    )
    return _public_task(saved_task)


def fail_task(
    task_id: object,
    *,
    error: object,
    result: Mapping[str, object] | None = None,
    decision_trace: Mapping[str, object] | None = None,
    store_path: Path | None = None,
) -> dict[str, object]:
    persisted = task_factory_module.get_task(
        _normalize_text(task_id), store_path=store_path
    )
    if persisted is None:
        raise ValueError(f"task not found: {task_id}")

    task_factory_module.transition_task_status(
        persisted,
        "FAILED",
        reason=_normalize_text(error) or "task_store.fail_task",
    )
    persisted["error"] = _normalize_text(error)
    persisted["result"] = _normalize_mapping(result)
    details = {
        "error": persisted["error"],
        "result": dict(persisted.get("result", {}) or {}),
    }
    if isinstance(decision_trace, Mapping):
        details["decision_trace"] = dict(decision_trace)
    task_factory_module.append_history_event(
        persisted,
        action="task_failed",
        data=details,
    )
    saved_task = task_factory_module.save_task(persisted, store_path=store_path)
    sync_active_tasks_context(
        store_path=store_path,
        task_overrides=[saved_task],
        timestamp=saved_task.get("last_updated_at") or saved_task.get("created_at"),
    )
    return _public_task(saved_task)


def ensure_task_for_execution(
    task_data: Mapping[str, object],
    *,
    store_path: Path | None = None,
) -> dict[str, object]:
    validated_task = validate_task_contract(dict(task_data))
    task_id = _normalize_text(validated_task.get("task_id"))
    if not task_id:
        raise ValueError("execution requires task_id")
    sync_active_tasks_context(
        store_path=store_path,
        task_overrides=[validated_task],
        timestamp=validated_task.get("last_updated_at")
        or validated_task.get("created_at"),
    )
    return dict(validated_task)


__all__ = [
    "complete_task",
    "create_task",
    "ensure_task_for_execution",
    "fail_task",
    "get_task",
    "sync_active_tasks_context",
    "update_task",
]
