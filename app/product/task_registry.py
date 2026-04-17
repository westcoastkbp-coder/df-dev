from __future__ import annotations

from pathlib import Path


ALLOWED_ACTIONS = {
    "WRITE_FILE",
    "READ_FILE",
    "BROWSER_TOOL",
    "RUN_TESTS",
    "BUILD_WEBSITE",
    "SYSTEM_STATUS",
    "RESOURCES",
}
TASK_CHAINS = {
    "DF-USE-CASE-OPERATOR-V1": (
        "DF-CONTROL-HEALTH-V1",
        "DF-CREATE-FILE-V1",
        "DF-READ-FILE-V1",
    ),
    "DF-REAL-USE-CASE-V1": (
        "DF-CONTROL-HEALTH-V1",
        "DF-CREATE-FILE-V1",
        "DF-READ-FILE-V1",
    ),
}
TASK_REGISTRY = {
    "DF-CREATE-FILE-V1": "tasks/active/create-test-file.yaml",
    "DF-READ-FILE-V1": "tasks/active/read-test-file.yaml",
    "DF-CONTROL-HEALTH-V1": "tasks/active/control-health-check.yaml",
    "DF-RUN-TESTS": "tasks/active/run-tests.yaml",
    "DF-BUILD-WEBSITE": "tasks/active/build-website.yaml",
    "DF-SYSTEM-STATUS": "tasks/active/system-status.yaml",
    "DF-RESOURCES": "tasks/active/resources.yaml",
    "DF-BROWSER-TOOL-V1": "tasks/active/browser-tool.yaml",
}


class TaskRegistryError(ValueError):
    pass


def get_descriptor(task_id: str) -> str:
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id:
        raise TaskRegistryError("task_id is required")

    descriptor = TASK_REGISTRY.get(normalized_task_id)
    if not descriptor:
        raise TaskRegistryError(f"unknown task_id: {normalized_task_id}")

    return str(Path(descriptor).as_posix())


def _read_descriptor_lines(descriptor_path: str) -> list[str]:
    path = Path(descriptor_path)
    if not path.is_absolute():
        path = Path.cwd() / path
    return path.read_text(encoding="utf-8").splitlines()


def get_descriptor_action(task_id: str) -> str:
    descriptor_path = get_descriptor(task_id)
    action_map = {
        "create_file": "WRITE_FILE",
        "read_file": "READ_FILE",
        "control_health": "CONTROL_HEALTH",
        "run_tests": "RUN_TESTS",
        "build_website": "BUILD_WEBSITE",
        "system_status": "SYSTEM_STATUS",
        "resources": "RESOURCES",
        "browser_tool": "BROWSER_TOOL",
    }
    for line in _read_descriptor_lines(descriptor_path):
        key, _, value = line.partition(":")
        if key.strip().lower() != "action":
            continue
        normalized = action_map.get(value.strip().lower(), value.strip().upper())
        return normalized
    raise TaskRegistryError(f"descriptor action is required: {task_id}")


def validate_task_id(task_id: str) -> dict[str, str]:
    normalized_task_id = str(task_id or "").strip()
    descriptor_path = get_descriptor(normalized_task_id)
    action = get_descriptor_action(normalized_task_id)
    if action not in ALLOWED_ACTIONS:
        raise TaskRegistryError(f"action not allowed: {action}")
    return {
        "task_id": normalized_task_id,
        "descriptor_path": descriptor_path,
        "action": action,
    }


def get_chain(task_id: str) -> tuple[str, ...]:
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id:
        raise TaskRegistryError("task_id is required")
    chain = TASK_CHAINS.get(normalized_task_id)
    if not chain:
        raise TaskRegistryError(f"unknown chain task_id: {normalized_task_id}")
    return tuple(str(item).strip() for item in chain if str(item).strip())
