from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.execution.paths import ROOT_DIR
from app.orchestrator.task_queue import task_queue
from app.orchestrator.task_worker import process_next_queued_task


TASK_ID = "DF-LEAD-INTAKE-V1"
REQUEST_TEXT = "Client wants ADU project, lot 5000 sqft, asking for price"
OUTPUT_PATH = ROOT_DIR / "runtime" / "out" / "leads" / "lead_001.txt"


def main(system_context: dict[str, object]) -> int:
    if system_context is None:
        raise RuntimeError("SYSTEM_CONTEXT_REQUIRED")

    task_data: dict[str, object] = {
        "task_id": TASK_ID,
        "job_id": TASK_ID,
        "trace_id": TASK_ID,
        "interaction_id": TASK_ID,
        "status": "pending",
        "intent": "new_lead",
        "goal": REQUEST_TEXT,
        "payload": {
            "request": REQUEST_TEXT,
            "summary": REQUEST_TEXT,
        },
        "history": [],
    }

    def fetch_task(task_id: str) -> dict[str, object] | None:
        return task_data if task_id == TASK_ID else None

    def persist(updated_task: dict[str, object]) -> None:
        task_data.update(updated_task)

    task_queue.enqueue_task(TASK_ID)
    executed_task = process_next_queued_task(
        queue=task_queue,
        fetch_task=fetch_task,
        persist=persist,
        timeout=0.0,
        system_context=system_context,
    )
    if executed_task is None:
        raise RuntimeError("lead intake task was not executed")

    print("[TRACE]")
    print(f"task: {TASK_ID}")
    print(f"status: {str(executed_task.get('status', '')).strip()}")
    print(
        f"result: {str(dict(executed_task.get('result', {})).get('summary', '')).strip()}"
    )
    print("[VERIFY]")
    print(OUTPUT_PATH.read_text(encoding="utf-8").strip())
    return 0


if __name__ == "__main__":
    raise RuntimeError("SYSTEM_CONTEXT_REQUIRED")
