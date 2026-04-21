from __future__ import annotations

import re
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.ownerbox.domain import normalize_ownerbox_domain_binding


VOICE_SESSION_STATUSES = frozenset({"active", "paused", "completed", "failed"})
VOICE_TURN_STATUSES = frozenset({"received", "completed", "blocked", "failed"})
VOICE_RESPONSE_TYPES = frozenset(
    {"spoken_text", "confirmation_request", "input_error", "error"}
)
VOICE_EXECUTION_MODES = frozenset({"live", "dry_run"})
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._:@-]+$")
_LANGUAGE_TAG_PATTERN = re.compile(r"^[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8}){0,2}$")


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _collapse_whitespace(value: object) -> str:
    return " ".join(_normalize_text(value).split())


def _stable_identifier(value: object, *, field_name: str) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    if _IDENTIFIER_PATTERN.fullmatch(normalized) is None:
        raise ValueError(f"{field_name} must be a stable identifier")
    return normalized


def _normalize_language_tag(value: object, *, default: str) -> str:
    normalized = _normalize_text(value) or default
    if not normalized:
        raise ValueError("language tag must not be empty")
    if _LANGUAGE_TAG_PATTERN.fullmatch(normalized) is None:
        raise ValueError(f"invalid language tag: {normalized}")
    parts = normalized.split("-")
    canonical: list[str] = []
    for index, part in enumerate(parts):
        if index == 0:
            canonical.append(part.lower())
        elif len(part) == 2 and part.isalpha():
            canonical.append(part.upper())
        else:
            canonical.append(part.lower())
    return "-".join(canonical)


def _normalize_mapping(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, object] = {}
    for key in sorted(value):
        normalized_key = _normalize_text(key)
        if normalized_key:
            normalized[normalized_key] = value[key]
    return normalized


def _normalize_status(
    value: object, *, allowed: frozenset[str], field_name: str
) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in allowed:
        raise ValueError(f"{field_name} must be one of: {', '.join(sorted(allowed))}")
    return normalized


def _new_identifier(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@dataclass(frozen=True, slots=True)
class VoiceTraceMetadata:
    session_id: str
    turn_id: str
    caller_id: str
    detected_language: str
    action_id: str | None
    result_status: str
    started_at: str
    completed_at: str
    latency_ms: int
    domain_binding: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "domain_binding",
            normalize_ownerbox_domain_binding(self.domain_binding),
        )

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ResponsePlan:
    response_type: str
    target_language: str
    text_payload: str
    action_refs: tuple[str, ...] = field(default_factory=tuple)
    requires_confirmation: bool = False
    execution_mode: str = "live"
    metadata: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "response_type",
            _normalize_status(
                self.response_type,
                allowed=VOICE_RESPONSE_TYPES,
                field_name="response_type",
            ),
        )
        object.__setattr__(
            self,
            "target_language",
            _normalize_language_tag(self.target_language, default="und"),
        )
        object.__setattr__(
            self, "text_payload", _collapse_whitespace(self.text_payload)
        )
        object.__setattr__(
            self,
            "action_refs",
            tuple(
                _stable_identifier(value, field_name="action_refs item")
                for value in self.action_refs
                if _normalize_text(value)
            ),
        )
        object.__setattr__(
            self,
            "execution_mode",
            _normalize_status(
                self.execution_mode,
                allowed=VOICE_EXECUTION_MODES,
                field_name="execution_mode",
            ),
        )
        object.__setattr__(self, "metadata", _normalize_mapping(self.metadata))

    def to_dict(self) -> dict[str, object]:
        return {
            "response_type": self.response_type,
            "target_language": self.target_language,
            "text_payload": self.text_payload,
            "action_refs": list(self.action_refs),
            "requires_confirmation": self.requires_confirmation,
            "execution_mode": self.execution_mode,
            "metadata": dict(self.metadata),
        }


@dataclass(frozen=True, slots=True)
class VoiceSession:
    session_id: str
    caller_id: str
    channel_type: str
    active_language: str
    language_profile: dict[str, object] = field(default_factory=dict)
    context_ref: str | None = None
    domain_binding: dict[str, object] = field(default_factory=dict)
    started_at: str = field(default_factory=_utc_timestamp)
    last_turn_at: str = field(default_factory=_utc_timestamp)
    session_status: str = "active"

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "session_id",
            _stable_identifier(self.session_id, field_name="session_id"),
        )
        object.__setattr__(
            self,
            "caller_id",
            _stable_identifier(self.caller_id, field_name="caller_id"),
        )
        object.__setattr__(
            self,
            "channel_type",
            _stable_identifier(self.channel_type, field_name="channel_type").lower(),
        )
        object.__setattr__(
            self,
            "active_language",
            _normalize_language_tag(self.active_language, default="und"),
        )
        object.__setattr__(
            self, "language_profile", _normalize_mapping(self.language_profile)
        )
        context_ref = _normalize_text(self.context_ref)
        object.__setattr__(self, "context_ref", context_ref or None)
        object.__setattr__(
            self,
            "domain_binding",
            normalize_ownerbox_domain_binding(self.domain_binding),
        )
        object.__setattr__(
            self,
            "session_status",
            _normalize_status(
                self.session_status,
                allowed=VOICE_SESSION_STATUSES,
                field_name="session_status",
            ),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "session_id": self.session_id,
            "caller_id": self.caller_id,
            "channel_type": self.channel_type,
            "active_language": self.active_language,
            "language_profile": dict(self.language_profile),
            "context_ref": self.context_ref,
            "domain_binding": dict(self.domain_binding),
            "started_at": self.started_at,
            "last_turn_at": self.last_turn_at,
            "session_status": self.session_status,
        }


@dataclass(frozen=True, slots=True)
class VoiceTurn:
    turn_id: str
    session_id: str
    input_text: str
    detected_language: str
    normalized_intent_ref: str
    response_plan: ResponsePlan
    output_text: str
    turn_status: str
    created_at: str = field(default_factory=_utc_timestamp)
    trace_metadata: VoiceTraceMetadata | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "turn_id", _stable_identifier(self.turn_id, field_name="turn_id")
        )
        object.__setattr__(
            self,
            "session_id",
            _stable_identifier(self.session_id, field_name="session_id"),
        )
        object.__setattr__(self, "input_text", _collapse_whitespace(self.input_text))
        object.__setattr__(
            self,
            "detected_language",
            _normalize_language_tag(self.detected_language, default="und"),
        )
        intent_ref = _normalize_text(self.normalized_intent_ref) or "generic_task"
        object.__setattr__(
            self,
            "normalized_intent_ref",
            _stable_identifier(intent_ref, field_name="normalized_intent_ref").lower(),
        )
        object.__setattr__(self, "output_text", _collapse_whitespace(self.output_text))
        object.__setattr__(
            self,
            "turn_status",
            _normalize_status(
                self.turn_status,
                allowed=VOICE_TURN_STATUSES,
                field_name="turn_status",
            ),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "turn_id": self.turn_id,
            "session_id": self.session_id,
            "input_text": self.input_text,
            "detected_language": self.detected_language,
            "normalized_intent_ref": self.normalized_intent_ref,
            "response_plan": self.response_plan.to_dict(),
            "output_text": self.output_text,
            "turn_status": self.turn_status,
            "created_at": self.created_at,
            "trace_metadata": None
            if self.trace_metadata is None
            else self.trace_metadata.to_dict(),
        }


def create_voice_session(
    *,
    caller_id: object,
    channel_type: object,
    active_language: object = "und",
    language_profile: dict[str, object] | None = None,
    context_ref: object = None,
    domain_binding: dict[str, object] | None = None,
    session_id: object | None = None,
    started_at: object | None = None,
) -> VoiceSession:
    timestamp = _normalize_text(started_at) or _utc_timestamp()
    return VoiceSession(
        session_id=_normalize_text(session_id) or _new_identifier("voice-session"),
        caller_id=_normalize_text(caller_id),
        channel_type=_normalize_text(channel_type),
        active_language=_normalize_text(active_language) or "und",
        language_profile=dict(language_profile or {}),
        context_ref=_normalize_text(context_ref) or None,
        domain_binding=normalize_ownerbox_domain_binding(domain_binding),
        started_at=timestamp,
        last_turn_at=timestamp,
        session_status="active",
    )


def update_voice_session(
    session: VoiceSession,
    *,
    last_turn_at: object | None = None,
    session_status: object | None = None,
    active_language: object | None = None,
    language_profile: dict[str, object] | None = None,
    context_ref: object | None = None,
    domain_binding: dict[str, object] | None = None,
) -> VoiceSession:
    return VoiceSession(
        session_id=session.session_id,
        caller_id=session.caller_id,
        channel_type=session.channel_type,
        active_language=_normalize_text(active_language) or session.active_language,
        language_profile=(
            dict(session.language_profile)
            if language_profile is None
            else dict(language_profile)
        ),
        context_ref=session.context_ref
        if context_ref is None
        else (_normalize_text(context_ref) or None),
        domain_binding=(
            dict(session.domain_binding)
            if domain_binding is None
            else normalize_ownerbox_domain_binding(domain_binding)
        ),
        started_at=session.started_at,
        last_turn_at=_normalize_text(last_turn_at) or session.last_turn_at,
        session_status=_normalize_text(session_status) or session.session_status,
    )


def create_response_plan(
    *,
    response_type: object,
    target_language: object,
    text_payload: object,
    action_refs: list[str] | tuple[str, ...] | None = None,
    requires_confirmation: bool = False,
    execution_mode: object = "live",
    metadata: dict[str, object] | None = None,
) -> ResponsePlan:
    return ResponsePlan(
        response_type=_normalize_text(response_type),
        target_language=_normalize_text(target_language) or "und",
        text_payload=_collapse_whitespace(text_payload),
        action_refs=tuple(action_refs or ()),
        requires_confirmation=bool(requires_confirmation),
        execution_mode=_normalize_text(execution_mode) or "live",
        metadata=dict(metadata or {}),
    )


def create_voice_trace_metadata(
    *,
    session_id: object,
    turn_id: object,
    caller_id: object,
    detected_language: object,
    action_id: object = None,
    result_status: object,
    started_at: object,
    completed_at: object,
    latency_ms: object = 0,
    domain_binding: dict[str, object] | None = None,
) -> VoiceTraceMetadata:
    latency_value = int(latency_ms or 0)
    if latency_value < 0:
        latency_value = 0
    action_text = _normalize_text(action_id) or None
    return VoiceTraceMetadata(
        session_id=_stable_identifier(session_id, field_name="session_id"),
        turn_id=_stable_identifier(turn_id, field_name="turn_id"),
        caller_id=_stable_identifier(caller_id, field_name="caller_id"),
        detected_language=_normalize_language_tag(detected_language, default="und"),
        action_id=None
        if action_text is None
        else _stable_identifier(action_text, field_name="action_id"),
        result_status=_normalize_text(result_status).lower(),
        started_at=_normalize_text(started_at) or _utc_timestamp(),
        completed_at=_normalize_text(completed_at) or _utc_timestamp(),
        latency_ms=latency_value,
        domain_binding=normalize_ownerbox_domain_binding(domain_binding),
    )


def create_voice_turn(
    *,
    session_id: object,
    input_text: object,
    detected_language: object,
    normalized_intent_ref: object,
    response_plan: ResponsePlan,
    output_text: object,
    turn_status: object,
    turn_id: object | None = None,
    created_at: object | None = None,
    trace_metadata: VoiceTraceMetadata | None = None,
) -> VoiceTurn:
    return VoiceTurn(
        turn_id=_normalize_text(turn_id) or _new_identifier("voice-turn"),
        session_id=_normalize_text(session_id),
        input_text=_collapse_whitespace(input_text),
        detected_language=_normalize_text(detected_language) or "und",
        normalized_intent_ref=_normalize_text(normalized_intent_ref) or "generic_task",
        response_plan=response_plan,
        output_text=_collapse_whitespace(output_text),
        turn_status=_normalize_text(turn_status),
        created_at=_normalize_text(created_at) or _utc_timestamp(),
        trace_metadata=trace_metadata,
    )
