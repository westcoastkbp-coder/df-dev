from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


OWNER_REQUEST_STATUSES = frozenset(
    {"received", "dispatched", "completed", "blocked", "failed"}
)
OWNER_REQUEST_TYPES = frozenset(
    {"generic_task", "status_check", "approval_request", "input_error"}
)
OWNER_PRIORITY_CLASSES = frozenset({"low", "medium", "high", "urgent"})
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._:@-]+$")
_LANGUAGE_TAG_PATTERN = re.compile(r"^[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8}){0,2}$")
_MAX_REQUEST_TEXT_LENGTH = 1024
_MAX_CONTEXT_REF_LENGTH = 256


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


def _normalize_context_ref(value: object) -> str | None:
    normalized = _normalize_text(value)
    if not normalized:
        return None
    if len(normalized) > _MAX_CONTEXT_REF_LENGTH:
        raise ValueError("context_ref exceeds max length")
    return normalized


def _normalize_request_text(value: object) -> str:
    normalized = _collapse_whitespace(value)
    if len(normalized) > _MAX_REQUEST_TEXT_LENGTH:
        raise ValueError("request_text exceeds max length")
    return normalized


def _normalize_status(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in OWNER_REQUEST_STATUSES:
        raise ValueError(
            "status must be one of: " + ", ".join(sorted(OWNER_REQUEST_STATUSES))
        )
    return normalized


def _normalize_request_type(value: object, *, request_text: str) -> str:
    normalized = _normalize_text(value).lower() or infer_owner_request_type(request_text)
    if normalized not in OWNER_REQUEST_TYPES:
        raise ValueError(
            "request_type must be one of: " + ", ".join(sorted(OWNER_REQUEST_TYPES))
        )
    return normalized


def _normalize_priority_class(value: object, *, request_text: str) -> str:
    normalized = _normalize_text(value).lower() or infer_priority_class(request_text)
    if normalized not in OWNER_PRIORITY_CLASSES:
        raise ValueError(
            "priority_class must be one of: " + ", ".join(sorted(OWNER_PRIORITY_CLASSES))
        )
    return normalized


def _normalize_mapping(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    normalized: dict[str, Any] = {}
    for key in sorted(value):
        normalized_key = _normalize_text(key)
        if normalized_key:
            normalized[normalized_key] = value[key]
    return normalized


def _new_identifier(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def infer_owner_request_type(request_text: object) -> str:
    normalized = _collapse_whitespace(request_text).lower()
    if not normalized:
        return "input_error"
    if any(token in normalized for token in ("approve", "confirm", "reject")):
        return "approval_request"
    if any(
        token in normalized
        for token in ("status", "queue", "pending", "show", "list", "what is")
    ):
        return "status_check"
    return "generic_task"


def infer_priority_class(request_text: object) -> str:
    normalized = _collapse_whitespace(request_text).lower()
    if any(token in normalized for token in ("urgent", "asap", "immediately", "right now")):
        return "urgent"
    if any(token in normalized for token in ("important", "high priority", "blocker")):
        return "high"
    if any(token in normalized for token in ("low priority", "whenever", "later")):
        return "low"
    return "medium"


@dataclass(frozen=True, slots=True)
class OwnerRequest:
    request_id: str
    owner_session_id: str
    owner_id: str
    request_text: str
    detected_language: str
    request_type: str
    priority_class: str
    context_ref: str | None
    created_at: str
    status: str
    normalized_payload: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        request_text = _normalize_request_text(self.request_text)
        object.__setattr__(self, "request_id", _stable_identifier(self.request_id, field_name="request_id"))
        object.__setattr__(
            self,
            "owner_session_id",
            _stable_identifier(self.owner_session_id, field_name="owner_session_id"),
        )
        object.__setattr__(self, "owner_id", _stable_identifier(self.owner_id, field_name="owner_id"))
        object.__setattr__(self, "request_text", request_text)
        object.__setattr__(
            self,
            "detected_language",
            _normalize_language_tag(self.detected_language, default="und"),
        )
        object.__setattr__(
            self,
            "request_type",
            _normalize_request_type(self.request_type, request_text=request_text),
        )
        object.__setattr__(
            self,
            "priority_class",
            _normalize_priority_class(self.priority_class, request_text=request_text),
        )
        object.__setattr__(self, "context_ref", _normalize_context_ref(self.context_ref))
        object.__setattr__(self, "created_at", _normalize_text(self.created_at) or _utc_timestamp())
        object.__setattr__(self, "status", _normalize_status(self.status))
        object.__setattr__(self, "normalized_payload", _normalize_mapping(self.normalized_payload))

    def to_dict(self) -> dict[str, object]:
        return {
            "request_id": self.request_id,
            "owner_session_id": self.owner_session_id,
            "owner_id": self.owner_id,
            "request_text": self.request_text,
            "detected_language": self.detected_language,
            "request_type": self.request_type,
            "priority_class": self.priority_class,
            "context_ref": self.context_ref,
            "created_at": self.created_at,
            "status": self.status,
            "normalized_payload": dict(self.normalized_payload),
        }


def create_owner_request(
    *,
    owner_session_id: object,
    owner_id: object,
    request_text: object,
    detected_language: object = "und",
    request_type: object | None = None,
    priority_class: object | None = None,
    context_ref: object = None,
    created_at: object | None = None,
    status: object = "received",
    request_id: object | None = None,
    normalized_payload: dict[str, object] | None = None,
) -> OwnerRequest:
    normalized_request_text = _normalize_request_text(request_text)
    resolved_request_type = _normalize_text(request_type) or infer_owner_request_type(
        normalized_request_text
    )
    resolved_priority_class = _normalize_text(priority_class) or infer_priority_class(
        normalized_request_text
    )
    payload = {
        "summary": normalized_request_text,
        "request_type": resolved_request_type,
        "priority_class": resolved_priority_class,
    }
    payload.update(dict(normalized_payload or {}))
    return OwnerRequest(
        request_id=_normalize_text(request_id) or _new_identifier("owner-request"),
        owner_session_id=_normalize_text(owner_session_id),
        owner_id=_normalize_text(owner_id),
        request_text=normalized_request_text,
        detected_language=_normalize_text(detected_language) or "und",
        request_type=resolved_request_type,
        priority_class=resolved_priority_class,
        context_ref=context_ref,
        created_at=_normalize_text(created_at) or _utc_timestamp(),
        status=_normalize_text(status) or "received",
        normalized_payload=payload,
    )


def update_owner_request(
    request: OwnerRequest,
    *,
    request_type: object | None = None,
    priority_class: object | None = None,
    context_ref: object | None = None,
    status: object | None = None,
    normalized_payload: dict[str, object] | None = None,
) -> OwnerRequest:
    payload = dict(request.normalized_payload)
    if normalized_payload is not None:
        payload = _normalize_mapping(normalized_payload)
    return OwnerRequest(
        request_id=request.request_id,
        owner_session_id=request.owner_session_id,
        owner_id=request.owner_id,
        request_text=request.request_text,
        detected_language=request.detected_language,
        request_type=_normalize_text(request_type) or request.request_type,
        priority_class=_normalize_text(priority_class) or request.priority_class,
        context_ref=request.context_ref if context_ref is None else context_ref,
        created_at=request.created_at,
        status=_normalize_text(status) or request.status,
        normalized_payload=payload,
    )
