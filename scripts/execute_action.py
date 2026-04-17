from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from scripts.log_event import log_event


def _generate_event_id() -> str:
    return f"evt_{uuid4().hex}"


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00",
        "Z",
    )


def execute_action(action_type: str, input_data: dict[str, Any]) -> dict[str, str]:
    result = {
        "status": "success",
    }

    event = {
        "event_id": _generate_event_id(),
        "timestamp": _utc_now(),
        "event_type": "action_execution",
        "source": "execution_layer",
        "action": {
            "type": action_type,
            "input": input_data,
            "output": result,
        },
        "verification": {
            "status": "success",
            "details": {},
        },
        "state_update": {
            "applied": False,
            "changes": {},
        },
        "trace": {
            "task_id": "test_task",
            "session_id": "test_session",
        },
    }

    log_event(event)
    return result
