from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

from app.execution.paths import BOX_SESSIONS_FILE

SESSION_MODES = frozenset(
    {
        "idle",
        "task_run",
        "task_status",
        "history_review",
        "help",
        "secretary_confirmation",
    }
)
INTERACTION_STATES = frozenset(
    {
        "idle",
        "listening",
        "processing",
        "responding",
        "waiting_for_followup",
        "pending_confirmation",
    }
)
SESSION_TIMEOUT_SECONDS = 300
BOX_STATES = frozenset(
    {
        "idle",
        "guiding",
        "awaiting_scope",
        "processing",
        "presenting_result",
        "reviewing_task",
        "reviewing_history",
        "awaiting_confirmation",
    }
)


def now() -> str:
    return datetime.utcnow().isoformat() + "Z"


def ensure_session_storage() -> None:
    BOX_SESSIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not BOX_SESSIONS_FILE.exists():
        BOX_SESSIONS_FILE.write_text("[]\n", encoding="utf-8")


def _normalize_mode(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in SESSION_MODES:
        return "idle"
    return normalized


def _normalize_interaction_state(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in INTERACTION_STATES:
        return "idle"
    return normalized


def _normalize_box_state(value: object) -> str:
    normalized = str(value or "").strip().lower()
    if normalized not in BOX_STATES:
        return "idle"
    return normalized


def _parse_timestamp(value: object) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None


def _is_timed_out(updated_at: object, *, now_value: datetime | None = None) -> bool:
    parsed = _parse_timestamp(updated_at)
    if parsed is None:
        return False
    reference = now_value or datetime.now(timezone.utc)
    return (reference - parsed).total_seconds() >= SESSION_TIMEOUT_SECONDS


def _normalize_session(record: dict[str, object]) -> dict[str, str]:
    payload = dict(record)
    return {
        "session_id": str(payload.get("session_id", "")).strip(),
        "user_id": str(payload.get("user_id", "")).strip(),
        "user_role": str(payload.get("user_role", "")).strip().lower(),
        "current_mode": _normalize_mode(payload.get("current_mode", "idle")),
        "interaction_state": _normalize_interaction_state(
            payload.get("interaction_state", "idle")
        ),
        "last_task_id": str(payload.get("last_task_id", "")).strip(),
        "last_intent": str(payload.get("last_intent", "")).strip(),
        "last_response_summary": str(payload.get("last_response_summary", "")).strip(),
        "box_state": _normalize_box_state(payload.get("box_state", "idle")),
        "pending_objective": str(payload.get("pending_objective", "")).strip(),
        "pending_confirmation_payload": str(
            payload.get("pending_confirmation_payload", "")
        ).strip(),
        "updated_at": str(payload.get("updated_at", "")).strip() or now(),
    }


def load_sessions() -> list[dict[str, str]]:
    ensure_session_storage()
    try:
        with open(BOX_SESSIONS_FILE, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except Exception:
        return []

    if not isinstance(data, list):
        return []
    return [_normalize_session(item) for item in data if isinstance(item, dict)]


def save_sessions(records: list[dict[str, object]]) -> None:
    ensure_session_storage()
    with open(BOX_SESSIONS_FILE, "w", encoding="utf-8") as handle:
        json.dump([_normalize_session(record) for record in records], handle, indent=2)


def find_session(session_id: object) -> dict[str, str] | None:
    normalized_session_id = str(session_id or "").strip()
    if not normalized_session_id:
        return None

    for record in reversed(load_sessions()):
        if str(record.get("session_id", "")).strip() == normalized_session_id:
            if _is_timed_out(record.get("updated_at", "")):
                return update_session(
                    normalized_session_id,
                    user_id=record.get("user_id", ""),
                    user_role=record.get("user_role", ""),
                    current_mode="idle",
                    interaction_state="idle",
                    last_task_id=record.get("last_task_id", ""),
                    last_intent=record.get("last_intent", ""),
                    last_response_summary=record.get("last_response_summary", ""),
                    box_state="idle",
                    pending_objective="",
                    pending_confirmation_payload="",
                )
            return record
    return None


def create_session(
    *,
    user_id: object,
    user_role: object,
    session_id: object | None = None,
) -> dict[str, str]:
    resolved_session_id = (
        str(session_id or "").strip() or f"box-{uuid.uuid4().hex[:12]}"
    )
    session = _normalize_session(
        {
            "session_id": resolved_session_id,
            "user_id": user_id,
            "user_role": user_role,
            "current_mode": "idle",
            "interaction_state": "idle",
            "last_task_id": "",
            "last_intent": "",
            "last_response_summary": "",
            "box_state": "idle",
            "pending_objective": "",
            "pending_confirmation_payload": "",
            "updated_at": now(),
        }
    )
    records = [
        record
        for record in load_sessions()
        if record["session_id"] != resolved_session_id
    ]
    records.append(session)
    save_sessions(records)
    return session


def get_or_create_session(
    *,
    session_id: object,
    user_id: object,
    user_role: object,
) -> tuple[dict[str, str], bool]:
    existing = find_session(session_id)
    if existing is not None:
        return existing, False
    created = create_session(
        session_id=session_id,
        user_id=user_id,
        user_role=user_role,
    )
    return created, True


def update_session(
    session_id: object,
    *,
    user_id: object,
    user_role: object,
    current_mode: object,
    interaction_state: object,
    last_task_id: object = "",
    last_intent: object = "",
    last_response_summary: object = "",
    box_state: object = "idle",
    pending_objective: object = "",
    pending_confirmation_payload: object = "",
) -> dict[str, str]:
    normalized_session_id = str(session_id or "").strip()
    if not normalized_session_id:
        raise ValueError("session_id must not be empty")

    updated = _normalize_session(
        {
            "session_id": normalized_session_id,
            "user_id": user_id,
            "user_role": user_role,
            "current_mode": current_mode,
            "interaction_state": interaction_state,
            "last_task_id": last_task_id,
            "last_intent": last_intent,
            "last_response_summary": last_response_summary,
            "box_state": box_state,
            "pending_objective": pending_objective,
            "pending_confirmation_payload": pending_confirmation_payload,
            "updated_at": now(),
        }
    )
    records = [
        record
        for record in load_sessions()
        if record["session_id"] != normalized_session_id
    ]
    records.append(updated)
    save_sessions(records)
    return updated
