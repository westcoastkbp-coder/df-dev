from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = REPO_ROOT / "state"
STATE_FILE = STATE_DIR / "state.json"
RECENT_COMMAND_WINDOW = timedelta(minutes=2)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _utc_now_iso() -> str:
    return utc_now().isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _parse_timestamp(value: Any) -> datetime | None:
    timestamp = str(value or "").strip()
    if not timestamp:
        return None
    try:
        return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


def _default_state() -> dict[str, list[dict[str, Any]]]:
    return {
        "commands": [],
    }


def load_state(state_path: Path | str | None = None) -> dict[str, list[dict[str, Any]]]:
    path = Path(state_path) if state_path is not None else STATE_FILE
    if not path.is_file():
        return _default_state()

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return _default_state()

    if not isinstance(payload, dict):
        return _default_state()

    commands = payload.get("commands")
    if not isinstance(commands, list):
        return _default_state()

    normalized_commands = [
        dict(command)
        for command in commands
        if isinstance(command, dict)
    ]
    return {
        "commands": normalized_commands,
    }


def write_state(
    state_payload: dict[str, list[dict[str, Any]]],
    state_path: Path | str | None = None,
) -> Path:
    path = Path(state_path) if state_path is not None else STATE_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(state_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path


def append_command_state(
    *,
    command_name: str,
    result: str,
    artifact: Path | str | None,
    doc_id: str = "",
    timestamp: str | None = None,
    state_path: Path | str | None = None,
) -> tuple[dict[str, Any], Path]:
    state = load_state(state_path)
    entry = {
        "command": str(command_name or "").strip(),
        "timestamp": str(timestamp or _utc_now_iso()),
        "result": str(result or "").strip(),
        "artifact": str(Path(artifact)) if artifact is not None else "",
        "doc_id": str(doc_id or "").strip(),
    }
    state["commands"].append(entry)
    written_path = write_state(state, state_path)
    return entry, written_path


def get_last_command(
    command_name: str,
    state_path: Path | str | None = None,
) -> dict[str, Any] | None:
    normalized_command = str(command_name or "").strip()
    if not normalized_command:
        return None

    state = load_state(state_path)
    for entry in reversed(state["commands"]):
        if str(entry.get("command") or "").strip() != normalized_command:
            continue
        return {
            "command": normalized_command,
            "timestamp": str(entry.get("timestamp") or "").strip(),
            "last_result": str(entry.get("result") or "").strip(),
            "last_doc_id": str(entry.get("doc_id") or "").strip(),
            "last_artifact": str(entry.get("artifact") or "").strip(),
        }
    return None


def command_executed_recently(
    command_name: str,
    *,
    now: datetime | None = None,
    state_path: Path | str | None = None,
) -> bool:
    last_command = get_last_command(command_name, state_path=state_path)
    if not isinstance(last_command, dict):
        return False

    last_timestamp = _parse_timestamp(last_command.get("timestamp"))
    if last_timestamp is None:
        return False

    reference_time = now if now is not None else utc_now()
    if reference_time.tzinfo is None:
        reference_time = reference_time.replace(tzinfo=timezone.utc)
    reference_time = reference_time.astimezone(timezone.utc)
    return (reference_time - last_timestamp) <= RECENT_COMMAND_WINDOW
