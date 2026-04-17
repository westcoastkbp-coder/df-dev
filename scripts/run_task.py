from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from scripts.execute_action import execute_action


REPO_ROOT = Path(__file__).resolve().parents[1]
TASK_PATH = REPO_ROOT / "tasks" / "sample_task.json"
EVENT_LOG_PATH = REPO_ROOT / "memory" / "events" / "event_log.jsonl"


def _load_task(task_path: Path = TASK_PATH) -> dict[str, Any]:
    return json.loads(task_path.read_text(encoding="utf-8"))


def _read_last_event_id(event_log_path: Path = EVENT_LOG_PATH) -> str | None:
    if not event_log_path.is_file():
        return None

    last_event_id: str | None = None
    for raw_line in event_log_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        event = json.loads(line)
        last_event_id = event.get("event_id")
    return last_event_id


def _write_task(task: dict[str, Any], task_path: Path = TASK_PATH) -> None:
    task_path.write_text(json.dumps(task, indent=2) + "\n", encoding="utf-8")


def run_task() -> dict[str, Any]:
    task = _load_task()
    action = task.get("action") or {}
    action_type = str(action.get("type") or "")
    action_input = dict(action.get("input") or {})

    result = execute_action(action_type, action_input)

    task["status"] = "completed"
    task["result"]["status"] = result["status"]
    task["result"]["output"] = result
    task["verification"]["status"] = "success"

    last_event_id = _read_last_event_id()
    if last_event_id is not None:
        events = list(task.get("events") or [])
        events.append(last_event_id)
        task["events"] = events

    _write_task(task)
    return task


def main() -> None:
    run_task()


if __name__ == "__main__":
    main()
