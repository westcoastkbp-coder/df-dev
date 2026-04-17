from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.execution.paths import ROOT_DIR
from app.orchestrator.task_queue import task_queue
from app.orchestrator.task_worker import process_next_queued_task


TASK_ID = "DF-LEAD-FOLLOWUP-V1"
OUTPUT_PATH = ROOT_DIR / "runtime" / "out" / "leads" / "lead_001_followup.txt"


def main(system_context: dict[str, object]) -> int:
    if system_context is None:
        raise RuntimeError("SYSTEM_CONTEXT_REQUIRED")

    task_data: dict[str, object] = {
        "task_id": TASK_ID,
        "job_id": TASK_ID,
        "trace_id": TASK_ID,
        "interaction_id": TASK_ID,
        "status": "pending",
        "intent": "lead_followup",
        "goal": "Generate lead follow-up from existing lead_001",
        "payload": {
            "lead_file": r"runtime\out\leads\lead_001.txt",
            "summary": "Generate follow-up for existing lead_001",
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
        raise RuntimeError("lead follow-up task was not executed")

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
