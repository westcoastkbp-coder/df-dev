from __future__ import annotations

import json
import time
from threading import Lock

from app.execution.paths import LOGS_DIR, ROOT_DIR, STATE_DIR
from runtime.system_log import write_json_log

TASK_QUEUE_FILE = ROOT_DIR / STATE_DIR / "task_queue.json"
TASK_LOG_FILE = ROOT_DIR / LOGS_DIR / "tasks.log"


def _timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class InMemoryTaskQueue:
    def __init__(self) -> None:
        self._lock = Lock()
        self._in_flight_task_ids: set[str] = set()
        self._ensure_files()

    def _ensure_files(self) -> None:
        TASK_QUEUE_FILE.parent.mkdir(parents=True, exist_ok=True)
        TASK_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not TASK_QUEUE_FILE.exists():
            TASK_QUEUE_FILE.write_text("[]", encoding="utf-8")
        if not TASK_LOG_FILE.exists():
            TASK_LOG_FILE.write_text("", encoding="utf-8")

    def _read_queue(self) -> list[dict[str, str]]:
        self._ensure_files()
        try:
            payload = json.loads(TASK_QUEUE_FILE.read_text(encoding="utf-8"))
        except Exception:
            payload = []
        if not isinstance(payload, list):
            return []
        queue_entries: list[dict[str, str]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            task_id = str(item.get("task_id") or "").strip()
            status = str(item.get("status") or "").strip() or "pending"
            if task_id:
                queue_entries.append({"task_id": task_id, "status": status})
        return queue_entries

    def _write_queue(self, queue_entries: list[dict[str, str]]) -> None:
        self._ensure_files()
        TASK_QUEUE_FILE.write_text(json.dumps(queue_entries, indent=2), encoding="utf-8")

    def _log(self, action: str, task_id: str) -> None:
        self._ensure_files()
        write_json_log(
            TASK_LOG_FILE,
            task_id=task_id,
            event_type=f"queue_{str(action or '').strip()}",
            status="observed",
            details={},
        )

    def enqueue_task(self, task_id: object) -> bool:
        normalized_task_id = str(task_id or "").strip()
        if not normalized_task_id:
            raise ValueError("task_id is required for queueing")

        with self._lock:
            queue_entries = self._read_queue()
            if any(item["task_id"] == normalized_task_id for item in queue_entries):
                return False
            queue_entries.append({"task_id": normalized_task_id, "status": "pending"})
            self._write_queue(queue_entries)
            self._log("enqueue", normalized_task_id)
        return True

    def dequeue_task(self) -> str | None:
        with self._lock:
            queue_entries = self._read_queue()
            if not queue_entries:
                return None
            removed = queue_entries.pop(0)
            task_id = removed["task_id"]
            self._write_queue(queue_entries)
            self._in_flight_task_ids.discard(task_id)
            self._log("dequeue", task_id)
            return task_id

    def enqueue(self, task_id: object) -> bool:
        return self.enqueue_task(task_id)

    def get(self, *, timeout: float = 0.1) -> str | None:
        deadline = time.monotonic() + max(0.0, float(timeout))
        while True:
            with self._lock:
                queue_entries = self._read_queue()
                for entry in queue_entries:
                    task_id = entry["task_id"]
                    if task_id not in self._in_flight_task_ids:
                        self._in_flight_task_ids.add(task_id)
                        return task_id
            if time.monotonic() >= deadline:
                return None
            time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))

    def reserve_task(self, task_id: object) -> bool:
        normalized_task_id = str(task_id or "").strip()
        if not normalized_task_id:
            return False
        with self._lock:
            queue_entries = self._read_queue()
            if not any(item["task_id"] == normalized_task_id for item in queue_entries):
                return False
            if normalized_task_id in self._in_flight_task_ids:
                return False
            self._in_flight_task_ids.add(normalized_task_id)
            return True

    def complete(self, task_id: object) -> None:
        normalized_task_id = str(task_id or "").strip()
        if not normalized_task_id:
            return
        with self._lock:
            queue_entries = self._read_queue()
            for index, entry in enumerate(queue_entries):
                if entry["task_id"] != normalized_task_id:
                    continue
                updated_entries = list(queue_entries[:index]) + list(queue_entries[index + 1 :])
                self._write_queue(updated_entries)
                self._in_flight_task_ids.discard(normalized_task_id)
                self._log("dequeue", normalized_task_id)
                return
            self._in_flight_task_ids.discard(normalized_task_id)

    def qsize(self) -> int:
        with self._lock:
            return len(self._read_queue())

    def queued_task_ids(self) -> list[str]:
        with self._lock:
            return [item["task_id"] for item in self._read_queue()]

    def in_flight_task_ids(self) -> list[str]:
        with self._lock:
            return sorted(self._in_flight_task_ids)

    def is_idle(self) -> bool:
        with self._lock:
            return not self._read_queue() and not self._in_flight_task_ids

    def clear(self) -> None:
        with self._lock:
            self._write_queue([])
            self._in_flight_task_ids.clear()


task_queue = InMemoryTaskQueue()
