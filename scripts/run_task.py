from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from scripts.execute_action import execute_action


REPO_ROOT = Path(__file__).resolve().parents[1]
TASK_PATH = REPO_ROOT / "tasks" / "sample_task.json"
TASK_QUEUE_PATH = REPO_ROOT / "tasks" / "task_queue.json"
EVENT_LOG_PATH = REPO_ROOT / "memory" / "events" / "event_log.jsonl"
DEFAULT_RETRIES = 0
DEFAULT_MAX_RETRIES = 3


def _load_task(task_path: Path | None = None) -> dict[str, Any]:
    if task_path is None:
        task_path = TASK_PATH
    return json.loads(task_path.read_text(encoding="utf-8"))


def _read_last_event_id(event_log_path: Path | None = None) -> str | None:
    last_event = _read_last_event(event_log_path)
    if last_event is None:
        return None
    return last_event.get("event_id")


def _read_last_event(event_log_path: Path | None = None) -> dict[str, Any] | None:
    if event_log_path is None:
        event_log_path = EVENT_LOG_PATH
    if not event_log_path.is_file():
        return None

    last_event: dict[str, Any] | None = None
    for raw_line in event_log_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        last_event = json.loads(line)
    return last_event


def _write_task(task: dict[str, Any], task_path: Path | None = None) -> None:
    if task_path is None:
        task_path = TASK_PATH
    task_path.write_text(json.dumps(task, indent=2) + "\n", encoding="utf-8")


def _append_task_ids(task_ids: list[str], queue_path: Path | None = None) -> None:
    if not task_ids:
        return

    if queue_path is None:
        queue_path = TASK_QUEUE_PATH

    queue: list[dict[str, str]] = []
    if queue_path.is_file():
        queue = json.loads(queue_path.read_text(encoding="utf-8"))

    for task_id in task_ids:
        queue.append({"task_id": task_id})

    queue_path.write_text(json.dumps(queue, indent=2) + "\n", encoding="utf-8")


def _normalize_retry_count(value: Any, *, default: int) -> int:
    if isinstance(value, bool):
        return default
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return default
    return max(normalized, 0)


def run_task(task_path: Path = TASK_PATH) -> dict[str, Any]:
    task = _load_task(task_path)
    task["retries"] = _normalize_retry_count(
        task.get("retries"), default=DEFAULT_RETRIES
    )
    task["max_retries"] = _normalize_retry_count(
        task.get("max_retries"),
        default=DEFAULT_MAX_RETRIES,
    )
    action = task.get("action") or {}
    action_type = str(action.get("type") or "")
    action_input = dict(action.get("input") or {})

    result = execute_action(action_type, action_input)

    last_event = _read_last_event()
    verification_status = str(
        ((last_event or {}).get("verification") or {}).get("status")
        or ("success" if result["status"] == "success" else "failed")
    )

    task["result"]["status"] = (
        "success" if verification_status == "success" else "failed"
    )
    task["result"]["output"] = result
    task["verification"]["status"] = verification_status

    last_event_id = None if last_event is None else last_event.get("event_id")
    if last_event_id is not None:
        events = list(task.get("events") or [])
        events.append(last_event_id)
        task["events"] = events

    if task["verification"]["status"] == "success":
        task["status"] = "completed"
        next_tasks = [str(task_id) for task_id in task.get("next_tasks") or []]
        _append_task_ids(next_tasks)
    else:
        task["status"] = "failed"
        if task["retries"] < task["max_retries"]:
            task["retries"] += 1
            task_id = str(task.get("task_id") or "").strip()
            if task_id:
                _append_task_ids([task_id])

    _write_task(task, task_path)
    return task


def main() -> None:
    run_task()


if __name__ == "__main__":
    main()
