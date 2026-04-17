from __future__ import annotations

from pathlib import Path

from app.product.runner import (
    RUNTIME_OUT_DIR,
    build_execution_ready,
    validate_action_trigger,
)


class _Payload:
    def __init__(self, *, task_id: str, descriptor_action: str) -> None:
        self.task_id = task_id
        self.objective = f"Execute mapped task descriptor for {task_id}"
        self.scope_files = []
        self.descriptor_path = f"tasks/active/{task_id.lower()}.yaml"
        self.descriptor_action = descriptor_action


def _path_is_in_runtime_out(path: object) -> bool:
    normalized_parts = Path(str(path).replace("\\", "/")).parts
    return normalized_parts[:2] == ("runtime", "out")


def test_validate_action_trigger_accepts_valid_write_file() -> None:
    trigger = build_execution_ready(
        _Payload(task_id="DF-CREATE-FILE-V1", descriptor_action="WRITE_FILE")
    )

    result = validate_action_trigger(trigger)

    assert result["valid"] is True
    assert result["reason"] == ""
    assert result["trigger"]["action_type"] == "WRITE_FILE"
    assert _path_is_in_runtime_out(result["trigger"]["payload"]["path"])


def test_validate_action_trigger_accepts_valid_read_file() -> None:
    trigger = build_execution_ready(
        _Payload(task_id="DF-READ-FILE-V1", descriptor_action="READ_FILE")
    )

    result = validate_action_trigger(trigger)

    assert result["valid"] is True
    assert result["reason"] == ""
    assert result["trigger"]["action_type"] == "READ_FILE"
    assert _path_is_in_runtime_out(result["trigger"]["payload"]["path"])


def test_validate_action_trigger_rejects_invalid_action_type() -> None:
    trigger = {
        "action_type": "DELETE_FILE",
        "payload": {
            "task_id": "DF-DELETE-FILE-V1",
            "path": str(RUNTIME_OUT_DIR / "blocked.log"),
        },
    }

    result = validate_action_trigger(trigger)

    assert result["valid"] is False
    assert "unknown action_type" in str(result["reason"])


def test_validate_action_trigger_rejects_invalid_path() -> None:
    trigger = {
        "action_type": "WRITE_FILE",
        "payload": {
            "task_id": "DF-CREATE-FILE-V1",
            "path": "/tmp/outside.log",
            "content": "blocked",
        },
    }

    result = validate_action_trigger(trigger)

    assert result["valid"] is False
    assert "runtime/out" in str(result["reason"])


def test_validate_action_trigger_rejects_malformed_payload() -> None:
    trigger = {
        "action_type": "WRITE_FILE",
        "payload": {
            "path": str(RUNTIME_OUT_DIR / "missing-task-id.log"),
        },
    }

    result = validate_action_trigger(trigger)

    assert result["valid"] is False
    assert "payload.task_id is required" == result["reason"]


def test_validate_action_trigger_accepts_valid_browser_tool() -> None:
    trigger = {
        "action_type": "BROWSER_TOOL",
        "payload": {
            "task_id": "DF-BROWSER-VALIDATION-V1",
            "steps": [
                {"operation": "open_url", "url": "https://example.com/contact"},
                {"operation": "get_page_text"},
            ],
        },
    }

    result = validate_action_trigger(trigger)

    assert result["valid"] is True
    assert result["reason"] == ""
    assert result["trigger"]["action_type"] == "BROWSER_TOOL"


def test_validate_action_trigger_rejects_unallowlisted_browser_url() -> None:
    trigger = {
        "action_type": "BROWSER_TOOL",
        "payload": {
            "task_id": "DF-BROWSER-VALIDATION-V2",
            "steps": [
                {"operation": "open_url", "url": "https://not-allowed.example.com/"},
            ],
        },
    }

    result = validate_action_trigger(trigger)

    assert result["valid"] is False
    assert "not allowlisted" in str(result["reason"])
