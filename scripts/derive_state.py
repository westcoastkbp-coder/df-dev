from __future__ import annotations

import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
EVENT_LOG_PATH = REPO_ROOT / "memory" / "events" / "event_log.jsonl"
STATE_PATH = REPO_ROOT / "memory" / "state" / "current_state.json"
DEFAULT_STATE: dict[str, Any] = {
    "system_status": "initializing",
    "memory_layer": "unknown",
    "last_event_id": None,
}


def derive_state() -> dict[str, Any]:
    state = dict(DEFAULT_STATE)
    if not EVENT_LOG_PATH.is_file():
        write_state(state)
        return state

    for raw_line in EVENT_LOG_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line:
            continue

        event = json.loads(line)
        verification = event.get("verification") or {}
        if verification.get("status") != "success":
            continue

        if event.get("event_type") == "system_init":
            state["memory_layer"] = "active"
            state["system_status"] = "running"

        state["last_event_id"] = event.get("event_id")

    write_state(state)
    return state


def write_state(state: dict[str, Any]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(
        json.dumps(state, indent=2) + "\n",
        encoding="utf-8",
    )


def main() -> None:
    derive_state()


if __name__ == "__main__":
    main()
