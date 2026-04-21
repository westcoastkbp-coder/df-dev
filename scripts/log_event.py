from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping


REPO_ROOT = Path(__file__).resolve().parents[1]
EVENT_LOG_PATH = REPO_ROOT / "memory" / "events" / "event_log.jsonl"


def log_event(event_data: Mapping[str, Any]) -> None:
    EVENT_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    event_line = json.dumps(dict(event_data), separators=(",", ":"))
    with EVENT_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(event_line)
        handle.write("\n")
