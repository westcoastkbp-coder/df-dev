from __future__ import annotations

from pathlib import Path

from app.execution.paths import ROOT_DIR, STATE_DIR
import app.orchestrator.task_state_store as task_state_store
from runtime.system_log import log_event


TASK_MEMORY_FILE = STATE_DIR / "task_memory.json"
MAX_ENTRIES = 100


def _task_memory_path() -> Path:
    return ROOT_DIR / TASK_MEMORY_FILE


def _task_state_db_path() -> Path:
    task_state_store.ROOT_DIR = ROOT_DIR
    task_state_store.LEGACY_TASK_MEMORY_FILE = TASK_MEMORY_FILE
    return task_state_store.initialize_database()


def _ensure_task_memory_file() -> None:
    task_memory_path = _task_memory_path()
    task_memory_path.parent.mkdir(parents=True, exist_ok=True)
    _task_state_db_path()


def _load_task_memory() -> list[dict[str, object]]:
    _ensure_task_memory_file()
    return task_state_store.read_memory_entries()


def load_task_memory() -> list[dict[str, object]]:
    return _load_task_memory()


def _save_task_memory(entries: list[dict[str, object]]) -> None:
    _ensure_task_memory_file()
    task_state_store.replace_memory_entries(
        [dict(entry) for entry in entries[-MAX_ENTRIES:]]
    )


def store_task_result(result: dict[str, object]) -> dict[str, object]:
    entry = {
        "task_id": str(result.get("task_id", "")).strip(),
        "status": "completed",
        "result_type": str(result.get("result_type", "")).strip(),
        "result_summary": str(result.get("result_summary", "")).strip(),
    }
    task_state_store.append_memory_entry(entry)
    log_event("memory", f"updated task_memory for {entry['task_id']}")
    return entry


def get_task_history(task_id: str) -> list[dict[str, object]]:
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id:
        return []
    return task_state_store.get_task_history(normalized_task_id)
