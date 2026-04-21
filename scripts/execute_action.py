from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from scripts.log_event import log_event


REPO_ROOT = Path(__file__).resolve().parents[1]


def _generate_event_id() -> str:
    return f"evt_{uuid4().hex}"


def _utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace(
            "+00:00",
            "Z",
        )
    )


def run_action(action_type: str, input_data: dict[str, Any]) -> dict[str, Any]:
    if action_type == "write_file":
        path = REPO_ROOT / str(input_data["path"])
        content = str(input_data["content"])
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        return {
            "status": "success",
            "file_written": str(path.relative_to(REPO_ROOT)),
        }

    if action_type == "open_url":
        import webbrowser

        url = str(input_data["url"])
        webbrowser.open(url)
        return {
            "status": "success",
            "url_opened": url,
        }

    return {
        "status": "success",
    }


def verify_result(
    action_type: str,
    input_data: dict[str, Any],
    result: dict[str, Any],
) -> dict[str, dict[str, Any] | str]:
    if action_type == "write_file":
        path = REPO_ROOT / str(input_data["path"])
        if path.is_file():
            return {
                "status": "success",
                "details": {},
            }
        return {
            "status": "failed",
            "details": {},
        }

    if action_type == "open_url":
        return {
            "status": "success",
            "details": {},
        }

    if result["status"] == "success":
        return {
            "status": "success",
            "details": {},
        }
    return {
        "status": "failed",
        "details": {},
    }


def execute_action(action_type: str, input_data: dict[str, Any]) -> dict[str, Any]:
    result = run_action(action_type, input_data)
    verification = verify_result(action_type, input_data, result)
    state_applied = verification["status"] == "success"

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
        "verification": verification,
        "state_update": {
            "applied": state_applied,
            "changes": (
                {}
                if not state_applied
                else {
                    "last_action_status": "success",
                    **(
                        {"file_written": str(input_data["path"])}
                        if action_type == "write_file"
                        else {}
                    ),
                }
            ),
        },
        "trace": {
            "task_id": "test_task",
            "session_id": "test_session",
        },
    }

    log_event(event)
    return result
