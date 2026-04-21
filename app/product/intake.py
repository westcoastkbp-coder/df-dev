from __future__ import annotations

from app.product.command_parser import CommandParserError, parse_command
from app.product.task_registry import (
    TaskRegistryError,
    get_chain,
    get_descriptor,
    validate_task_id,
)


MAX_CHAIN_LENGTH = 3


def _error_response(
    *,
    error_type: str,
    error_message: str,
    recoverable: bool,
    task_id: str = "",
) -> dict[str, object]:
    return {
        "action": "error",
        "status": "error",
        "error_type": str(error_type).strip() or "UNKNOWN_ERROR",
        "error_message": str(error_message).strip() or "unknown error",
        "recoverable": bool(recoverable),
        "task_id": str(task_id).strip(),
    }


def _mapped_task_request(task_id: str, *, scope_files: list[str]) -> dict[str, object]:
    normalized_task_id = str(task_id).strip()
    if normalized_task_id == "DF-CONTROL-HEALTH-V1":
        return _error_response(
            error_type="CONNECTION_FAILED",
            error_message="health check failed",
            recoverable=True,
            task_id=normalized_task_id,
        )
    try:
        task_definition = validate_task_id(normalized_task_id)
    except TaskRegistryError as exc:
        return _error_response(
            error_type="TASK_ID_UNMAPPED",
            error_message=str(exc),
            recoverable=True,
            task_id=normalized_task_id,
        )

    return {
        "action": "execute",
        "task_id": str(task_definition.get("task_id", "")).strip(),
        "objective": f"Execute mapped task descriptor for {task_id}",
        "scope_files": list(scope_files)
        or [str(task_definition.get("descriptor_path", "")).strip()],
        "descriptor_path": str(task_definition.get("descriptor_path", "")).strip(),
        "descriptor_action": str(task_definition.get("action", "")).strip(),
    }


def _mapped_use_case_request(
    task_id: str, *, scope_files: list[str]
) -> dict[str, object]:
    try:
        chain = get_chain(task_id)
    except TaskRegistryError as exc:
        return _error_response(
            error_type="TASK_ID_UNMAPPED",
            error_message=str(exc),
            recoverable=True,
            task_id=str(task_id).strip(),
        )

    if len(chain) > MAX_CHAIN_LENGTH:
        return _error_response(
            error_type="BLOCKED",
            error_message=f"chain too long: max {MAX_CHAIN_LENGTH} tasks",
            recoverable=True,
            task_id=str(task_id).strip(),
        )

    requests: list[dict[str, object]] = []
    for item in chain:
        if item == "DF-CONTROL-HEALTH-V1":
            descriptor_path = get_descriptor(item)
            requests.append(
                {
                    "action": "execute",
                    "task_id": item,
                    "objective": f"Execute mapped task descriptor for {item}",
                    "scope_files": list(scope_files) or [descriptor_path],
                    "descriptor_path": descriptor_path,
                    "descriptor_action": "CONTROL_HEALTH",
                }
            )
            continue

        mapped = _mapped_task_request(item, scope_files=scope_files)
        if str(mapped.get("status", "")).strip() == "error":
            return mapped
        requests.append(mapped)

    return {
        "action": "execute_chain",
        "status": "accepted",
        "tasks": [str(item).strip() for item in chain],
        "requests": requests,
    }


def build_product_task_request(payload: object) -> dict[str, object]:
    task_id = str(getattr(payload, "task_id", "") or "").strip()
    objective = str(getattr(payload, "objective", "") or "").strip()
    scope_files = [
        str(item).strip()
        for item in list(getattr(payload, "scope_files", []) or [])
        if str(item).strip()
    ]
    if not task_id and objective:
        try:
            parsed_command = parse_command(objective)
        except CommandParserError as exc:
            return _error_response(
                error_type="BLOCKED",
                error_message=str(exc),
                recoverable=True,
            )
        if str(parsed_command.get("action", "")).strip() == "execute_chain":
            tasks = [
                str(item).strip() for item in list(parsed_command.get("tasks", []))
            ]
            if len(tasks) == 1:
                use_case_request = _mapped_use_case_request(
                    tasks[0], scope_files=scope_files
                )
                if str(use_case_request.get("status", "")).strip() == "accepted":
                    return use_case_request
                if str(use_case_request.get("status", "")).strip() == "error":
                    return use_case_request
            if len(tasks) > MAX_CHAIN_LENGTH:
                return _error_response(
                    error_type="BLOCKED",
                    error_message=f"chain too long: max {MAX_CHAIN_LENGTH} tasks",
                    recoverable=True,
                )
            requests = [
                _mapped_task_request(item, scope_files=scope_files) for item in tasks
            ]
            rejected = next(
                (
                    dict(item)
                    for item in requests
                    if str(item.get("status", "")).strip() == "error"
                ),
                None,
            )
            if rejected is not None:
                return rejected
            return {
                "action": "execute_chain",
                "status": "accepted",
                "tasks": tasks,
                "requests": requests,
            }
        if str(parsed_command.get("action", "")).strip() == "execute":
            task_id = str(list(parsed_command.get("tasks", []))[0]).strip()
        if str(parsed_command.get("action", "")).strip() == "resources":
            task_id = str(list(parsed_command.get("tasks", []))[0]).strip()

    if task_id:
        return _mapped_task_request(task_id, scope_files=scope_files)

    if not objective:
        return _error_response(
            error_type="BLOCKED",
            error_message="objective must not be empty",
            recoverable=True,
        )
    if not scope_files:
        return _error_response(
            error_type="BLOCKED",
            error_message="scope_files must not be empty",
            recoverable=True,
        )

    return {
        "action": "execute",
        "status": "accepted",
        "task_id": task_id,
        "objective": objective,
        "scope_files": scope_files,
        "descriptor_path": "",
    }
