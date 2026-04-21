from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from app.execution.paths import VOICE_CALL_SESSIONS_FILE, VOICE_TRACE_EVENTS_FILE

_STORE_LOCK = threading.RLock()


def now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def ensure_voice_runtime_storage() -> None:
    with _STORE_LOCK:
        VOICE_CALL_SESSIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not VOICE_CALL_SESSIONS_FILE.exists():
            VOICE_CALL_SESSIONS_FILE.write_text("[]\n", encoding="utf-8")
        if not VOICE_TRACE_EVENTS_FILE.exists():
            VOICE_TRACE_EVENTS_FILE.write_text("[]\n", encoding="utf-8")


def _normalize_pending_item(item: object) -> dict[str, Any]:
    payload = dict(item or {})
    return {
        "event_id": str(payload.get("event_id", "")).strip()
        or f"voice-out-{uuid.uuid4().hex[:12]}",
        "sequence": int(payload.get("sequence", 0) or 0),
        "kind": str(payload.get("kind", "assistant_response")).strip()
        or "assistant_response",
        "text": str(payload.get("text", "")).strip(),
        "interruptible": bool(payload.get("interruptible", True)),
        "created_at": str(payload.get("created_at", "")).strip() or now(),
    }


def _normalize_processed_event_ids(value: object) -> list[str]:
    items = [str(item).strip() for item in list(value or []) if str(item).strip()]
    return items[-100:]


def _normalize_latency_metrics(value: object) -> dict[str, float]:
    metrics = dict(value or {})
    normalized: dict[str, float] = {}
    for key, raw_value in metrics.items():
        try:
            normalized[str(key).strip()] = round(float(raw_value), 3)
        except (TypeError, ValueError):
            continue
    return normalized


def _normalize_call_session(record: dict[str, object]) -> dict[str, Any]:
    payload = dict(record or {})
    return {
        "call_session_id": str(payload.get("call_session_id", "")).strip(),
        "session_id": str(payload.get("session_id", "")).strip(),
        "call_control_id": str(payload.get("call_control_id", "")).strip(),
        "telnyx_call_leg_id": str(payload.get("telnyx_call_leg_id", "")).strip(),
        "stream_id": str(payload.get("stream_id", "")).strip(),
        "from_number": str(payload.get("from_number", "")).strip(),
        "to_number": str(payload.get("to_number", "")).strip(),
        "user_id": str(payload.get("user_id", "")).strip(),
        "user_role": str(payload.get("user_role", "foreman")).strip().lower()
        or "foreman",
        "interaction_id": str(payload.get("interaction_id", "")).strip(),
        "job_id": str(payload.get("job_id", "")).strip(),
        "trace_id": str(payload.get("trace_id", "")).strip(),
        "orchestrator_task_id": str(payload.get("orchestrator_task_id", "")).strip(),
        "call_state": str(payload.get("call_state", "new")).strip() or "new",
        "media_state": str(payload.get("media_state", "idle")).strip() or "idle",
        "interaction_state": str(payload.get("interaction_state", "idle")).strip()
        or "idle",
        "current_mode": str(payload.get("current_mode", "idle")).strip() or "idle",
        "job_status": str(payload.get("job_status", "idle")).strip() or "idle",
        "last_partial_transcript": str(
            payload.get("last_partial_transcript", "")
        ).strip(),
        "last_transcript": str(payload.get("last_transcript", "")).strip(),
        "last_response_text": str(payload.get("last_response_text", "")).strip(),
        "last_ack_text": str(payload.get("last_ack_text", "")).strip(),
        "last_error": str(payload.get("last_error", "")).strip(),
        "validation_state": str(payload.get("validation_state", "UNKNOWN")).strip()
        or "UNKNOWN",
        "validation_guardrails": [
            str(item).strip()
            for item in list(payload.get("validation_guardrails", []))
            if str(item).strip()
        ],
        "validation_summary": str(payload.get("validation_summary", "")).strip(),
        "last_validation_at": str(payload.get("last_validation_at", "")).strip(),
        "active_turn_event_id": str(payload.get("active_turn_event_id", "")).strip(),
        "last_runtime_state": str(payload.get("last_runtime_state", "")).strip(),
        "last_runtime_confidence": str(
            payload.get("last_runtime_confidence", "")
        ).strip(),
        "voice_latency_metrics": _normalize_latency_metrics(
            payload.get("voice_latency_metrics", {})
        ),
        "guardrail_activation_count": int(
            payload.get("guardrail_activation_count", 0) or 0
        ),
        "runtime_transition_count": int(
            payload.get("runtime_transition_count", 0) or 0
        ),
        "runtime_verdict": str(payload.get("runtime_verdict", "")).strip(),
        "voice_runtime_verdict": str(payload.get("voice_runtime_verdict", "")).strip(),
        "runtime_verdict_score": int(payload.get("runtime_verdict_score", 0) or 0),
        "last_runtime_verdict_at": str(
            payload.get("last_runtime_verdict_at", "")
        ).strip(),
        "reconnect_count": int(payload.get("reconnect_count", 0) or 0),
        "interruption_count": int(payload.get("interruption_count", 0) or 0),
        "response_sequence": int(payload.get("response_sequence", 0) or 0),
        "processed_event_ids": _normalize_processed_event_ids(
            payload.get("processed_event_ids", [])
        ),
        "pending_outbound": [
            _normalize_pending_item(item)
            for item in list(payload.get("pending_outbound", []))
        ],
        "created_at": str(payload.get("created_at", "")).strip() or now(),
        "updated_at": str(payload.get("updated_at", "")).strip() or now(),
    }


def load_call_sessions() -> list[dict[str, Any]]:
    ensure_voice_runtime_storage()
    with _STORE_LOCK:
        try:
            with open(VOICE_CALL_SESSIONS_FILE, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except Exception:
            return []
    if not isinstance(data, list):
        return []
    return [_normalize_call_session(item) for item in data if isinstance(item, dict)]


def save_call_sessions(records: list[dict[str, object]]) -> None:
    ensure_voice_runtime_storage()
    normalized = [_normalize_call_session(record) for record in records]
    with _STORE_LOCK:
        with open(VOICE_CALL_SESSIONS_FILE, "w", encoding="utf-8") as handle:
            json.dump(normalized, handle, indent=2)


def _match_call_session(
    record: dict[str, Any],
    *,
    call_session_id: str = "",
    call_control_id: str = "",
    session_id: str = "",
) -> bool:
    if call_session_id and record["call_session_id"] == call_session_id:
        return True
    if call_control_id and record["call_control_id"] == call_control_id:
        return True
    if session_id and record["session_id"] == session_id:
        return True
    return False


def find_call_session(
    *,
    call_session_id: object = "",
    call_control_id: object = "",
    session_id: object = "",
) -> dict[str, Any] | None:
    normalized_call_session_id = str(call_session_id or "").strip()
    normalized_call_control_id = str(call_control_id or "").strip()
    normalized_session_id = str(session_id or "").strip()
    if not (
        normalized_call_session_id
        or normalized_call_control_id
        or normalized_session_id
    ):
        return None
    for record in reversed(load_call_sessions()):
        if _match_call_session(
            record,
            call_session_id=normalized_call_session_id,
            call_control_id=normalized_call_control_id,
            session_id=normalized_session_id,
        ):
            return record
    return None


def upsert_call_session(record: dict[str, object]) -> dict[str, Any]:
    normalized = _normalize_call_session(record)
    if not normalized["call_session_id"]:
        raise ValueError("call_session_id must not be empty")
    records = [
        item
        for item in load_call_sessions()
        if item["call_session_id"] != normalized["call_session_id"]
    ]
    normalized["updated_at"] = now()
    records.append(normalized)
    save_call_sessions(records)
    return normalized


def create_or_update_call_session(
    *,
    call_session_id: object,
    session_id: object,
    call_control_id: object,
    telnyx_call_leg_id: object = "",
    stream_id: object = "",
    from_number: object = "",
    to_number: object = "",
    user_id: object = "",
    user_role: object = "foreman",
    interaction_id: object = "",
    job_id: object = "",
    trace_id: object = "",
    orchestrator_task_id: object = "",
    call_state: object = "new",
    media_state: object = "idle",
    interaction_state: object = "idle",
    current_mode: object = "idle",
    job_status: object = "idle",
    last_partial_transcript: object = "",
    last_transcript: object = "",
    last_response_text: object = "",
    last_ack_text: object = "",
    last_error: object = "",
    validation_state: object = "UNKNOWN",
    validation_guardrails: object | None = None,
    validation_summary: object = "",
    last_validation_at: object = "",
    active_turn_event_id: object = "",
    last_runtime_state: object = "",
    last_runtime_confidence: object = "",
    voice_latency_metrics: object | None = None,
    guardrail_activation_count: object = 0,
    runtime_transition_count: object = 0,
    runtime_verdict: object = "",
    voice_runtime_verdict: object = "",
    runtime_verdict_score: object = 0,
    last_runtime_verdict_at: object = "",
    reconnect_count: object = 0,
    interruption_count: object = 0,
    response_sequence: object = 0,
    processed_event_ids: object | None = None,
    pending_outbound: object | None = None,
) -> dict[str, Any]:
    existing = find_call_session(
        call_session_id=call_session_id,
        call_control_id=call_control_id,
        session_id=session_id,
    )
    base = dict(existing or {})
    created_at = str(base.get("created_at", "")).strip() or now()
    payload = {
        **base,
        "call_session_id": str(call_session_id or "").strip()
        or base.get("call_session_id", ""),
        "session_id": str(session_id or "").strip() or base.get("session_id", ""),
        "call_control_id": str(call_control_id or "").strip()
        or base.get("call_control_id", ""),
        "telnyx_call_leg_id": str(telnyx_call_leg_id or "").strip()
        or base.get("telnyx_call_leg_id", ""),
        "stream_id": str(stream_id or "").strip() or base.get("stream_id", ""),
        "from_number": str(from_number or "").strip() or base.get("from_number", ""),
        "to_number": str(to_number or "").strip() or base.get("to_number", ""),
        "user_id": str(user_id or "").strip() or base.get("user_id", ""),
        "user_role": str(user_role or "").strip().lower()
        or base.get("user_role", "foreman"),
        "interaction_id": str(interaction_id or "").strip()
        or base.get("interaction_id", ""),
        "job_id": str(job_id or "").strip() or base.get("job_id", ""),
        "trace_id": str(trace_id or "").strip() or base.get("trace_id", ""),
        "orchestrator_task_id": str(orchestrator_task_id or "").strip()
        or base.get("orchestrator_task_id", ""),
        "call_state": str(call_state or "").strip() or base.get("call_state", "new"),
        "media_state": str(media_state or "").strip()
        or base.get("media_state", "idle"),
        "interaction_state": str(interaction_state or "").strip()
        or base.get("interaction_state", "idle"),
        "current_mode": str(current_mode or "").strip()
        or base.get("current_mode", "idle"),
        "job_status": str(job_status or "").strip() or base.get("job_status", "idle"),
        "last_partial_transcript": str(last_partial_transcript or "").strip()
        or base.get("last_partial_transcript", ""),
        "last_transcript": str(last_transcript or "").strip()
        or base.get("last_transcript", ""),
        "last_response_text": str(last_response_text or "").strip()
        or base.get("last_response_text", ""),
        "last_ack_text": str(last_ack_text or "").strip()
        or base.get("last_ack_text", ""),
        "last_error": str(last_error or "").strip() or base.get("last_error", ""),
        "validation_state": str(validation_state or "").strip()
        or base.get("validation_state", "UNKNOWN"),
        "validation_guardrails": validation_guardrails
        if validation_guardrails is not None
        else base.get("validation_guardrails", []),
        "validation_summary": str(validation_summary or "").strip()
        or base.get("validation_summary", ""),
        "last_validation_at": str(last_validation_at or "").strip()
        or base.get("last_validation_at", ""),
        "active_turn_event_id": str(active_turn_event_id or "").strip()
        or base.get("active_turn_event_id", ""),
        "last_runtime_state": str(last_runtime_state or "").strip()
        or base.get("last_runtime_state", ""),
        "last_runtime_confidence": str(last_runtime_confidence or "").strip()
        or base.get("last_runtime_confidence", ""),
        "voice_latency_metrics": voice_latency_metrics
        if voice_latency_metrics is not None
        else base.get("voice_latency_metrics", {}),
        "guardrail_activation_count": int(
            guardrail_activation_count or base.get("guardrail_activation_count", 0) or 0
        ),
        "runtime_transition_count": int(
            runtime_transition_count or base.get("runtime_transition_count", 0) or 0
        ),
        "runtime_verdict": str(runtime_verdict or "").strip()
        or base.get("runtime_verdict", ""),
        "voice_runtime_verdict": str(voice_runtime_verdict or "").strip()
        or base.get("voice_runtime_verdict", ""),
        "runtime_verdict_score": int(
            runtime_verdict_score or base.get("runtime_verdict_score", 0) or 0
        ),
        "last_runtime_verdict_at": str(last_runtime_verdict_at or "").strip()
        or base.get("last_runtime_verdict_at", ""),
        "reconnect_count": int(reconnect_count or base.get("reconnect_count", 0) or 0),
        "interruption_count": int(
            interruption_count or base.get("interruption_count", 0) or 0
        ),
        "response_sequence": int(
            response_sequence or base.get("response_sequence", 0) or 0
        ),
        "processed_event_ids": processed_event_ids
        if processed_event_ids is not None
        else base.get("processed_event_ids", []),
        "pending_outbound": pending_outbound
        if pending_outbound is not None
        else base.get("pending_outbound", []),
        "created_at": created_at,
    }
    return upsert_call_session(payload)


def update_call_session(call_session_id: object, **updates: object) -> dict[str, Any]:
    existing = find_call_session(call_session_id=call_session_id)
    if existing is None:
        raise ValueError("voice call session not found")
    payload = {**existing, **updates}
    payload["call_session_id"] = existing["call_session_id"]
    payload["session_id"] = (
        str(payload.get("session_id", "")).strip() or existing["session_id"]
    )
    payload["call_control_id"] = (
        str(payload.get("call_control_id", "")).strip() or existing["call_control_id"]
    )
    payload["created_at"] = existing["created_at"]
    return upsert_call_session(payload)


def register_processed_event(call_session_id: object, event_id: object) -> bool:
    normalized_event_id = str(event_id or "").strip()
    if not normalized_event_id:
        return True
    session = find_call_session(call_session_id=call_session_id)
    if session is None:
        raise ValueError("voice call session not found")
    processed_event_ids = list(session.get("processed_event_ids", []))
    if normalized_event_id in processed_event_ids:
        return False
    processed_event_ids.append(normalized_event_id)
    update_call_session(
        call_session_id,
        processed_event_ids=processed_event_ids[-100:],
    )
    return True


def queue_outbound_event(
    call_session_id: object,
    *,
    kind: object,
    text: object,
    interruptible: bool = True,
) -> dict[str, Any]:
    session = find_call_session(call_session_id=call_session_id)
    if session is None:
        raise ValueError("voice call session not found")
    next_sequence = int(session.get("response_sequence", 0)) + 1
    item = _normalize_pending_item(
        {
            "event_id": f"voice-out-{uuid.uuid4().hex[:12]}",
            "sequence": next_sequence,
            "kind": kind,
            "text": text,
            "interruptible": interruptible,
            "created_at": now(),
        }
    )
    pending = list(session.get("pending_outbound", []))
    pending.append(item)
    update_call_session(
        call_session_id,
        pending_outbound=pending,
        response_sequence=next_sequence,
    )
    return item


def list_pending_outbound(call_session_id: object) -> list[dict[str, Any]]:
    session = find_call_session(call_session_id=call_session_id)
    if session is None:
        return []
    return [
        _normalize_pending_item(item) for item in session.get("pending_outbound", [])
    ]


def remove_pending_outbound(
    call_session_id: object, event_id: object
) -> dict[str, Any]:
    normalized_event_id = str(event_id or "").strip()
    session = find_call_session(call_session_id=call_session_id)
    if session is None:
        raise ValueError("voice call session not found")
    pending = [
        _normalize_pending_item(item)
        for item in session.get("pending_outbound", [])
        if str(dict(item).get("event_id", "")).strip() != normalized_event_id
    ]
    return update_call_session(call_session_id, pending_outbound=pending)


def clear_pending_outbound(call_session_id: object) -> dict[str, Any]:
    return update_call_session(call_session_id, pending_outbound=[])


def append_trace_event(
    *,
    call_session_id: object,
    interaction_id: object,
    job_id: object,
    trace_id: object,
    event_type: object,
    payload: dict[str, object] | None = None,
) -> dict[str, Any]:
    ensure_voice_runtime_storage()
    record = {
        "event_id": f"voice-trace-{uuid.uuid4().hex[:12]}",
        "call_session_id": str(call_session_id or "").strip(),
        "interaction_id": str(interaction_id or "").strip(),
        "job_id": str(job_id or "").strip(),
        "trace_id": str(trace_id or "").strip(),
        "event_type": str(event_type or "").strip(),
        "payload": dict(payload or {}),
        "recorded_at": now(),
    }
    with _STORE_LOCK:
        try:
            with open(VOICE_TRACE_EVENTS_FILE, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except Exception:
            data = []
        if not isinstance(data, list):
            data = []
        data.append(record)
        with open(VOICE_TRACE_EVENTS_FILE, "w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2)
    return record


def load_trace_events(*, call_session_id: object = "") -> list[dict[str, Any]]:
    ensure_voice_runtime_storage()
    with _STORE_LOCK:
        try:
            with open(VOICE_TRACE_EVENTS_FILE, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except Exception:
            return []
    if not isinstance(data, list):
        return []
    normalized_call_session_id = str(call_session_id or "").strip()
    events = [dict(item) for item in data if isinstance(item, dict)]
    if not normalized_call_session_id:
        return events
    return [
        item
        for item in events
        if str(item.get("call_session_id", "")).strip() == normalized_call_session_id
    ]
