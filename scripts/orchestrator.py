from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from scripts.derive_state import derive_state
from scripts.run_task import run_task


REPO_ROOT = Path(__file__).resolve().parents[1]
TASKS_DIR = REPO_ROOT / "tasks"
TASK_QUEUE_PATH = TASKS_DIR / "task_queue.json"


def _load_queue(queue_path: Path = TASK_QUEUE_PATH) -> list[dict[str, Any]]:
    return json.loads(queue_path.read_text(encoding="utf-8"))


def _resolve_task_path(task_id: str, tasks_dir: Path = TASKS_DIR) -> Path | None:
    for candidate in sorted(tasks_dir.glob("*.json")):
        if candidate.name == "task_queue.json":
            continue
        task = json.loads(candidate.read_text(encoding="utf-8"))
        if task.get("task_id") == task_id:
            return candidate
    return None


def orchestrate() -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for queued_task in _load_queue():
        task_id = str(queued_task.get("task_id") or "")
        task_path = _resolve_task_path(task_id)
        if task_path is None:
            continue

        task = json.loads(task_path.read_text(encoding="utf-8"))
        if task.get("status") == "pending":
            results.append(run_task(task_path))
            derive_state()
    return results


def main() -> None:
    orchestrate()


if __name__ == "__main__":
    main()
