from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable


OWNER_ACTION_QUEUE_STATUSES = frozenset(
    {"queued", "awaiting_confirmation", "success", "blocked", "failed"}
)
OWNER_PRIORITY_CLASSES = frozenset({"low", "medium", "high", "urgent"})
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._:@-]+$")


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _stable_identifier(value: object, *, field_name: str) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    if _IDENTIFIER_PATTERN.fullmatch(normalized) is None:
        raise ValueError(f"{field_name} must be a stable identifier")
    return normalized


def _normalize_status(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in OWNER_ACTION_QUEUE_STATUSES:
        raise ValueError(
            "action_status must be one of: "
            + ", ".join(sorted(OWNER_ACTION_QUEUE_STATUSES))
        )
    return normalized


def _normalize_priority_class(value: object) -> str:
    normalized = _normalize_text(value).lower()
    if normalized not in OWNER_PRIORITY_CLASSES:
        raise ValueError(
            "priority_class must be one of: " + ", ".join(sorted(OWNER_PRIORITY_CLASSES))
        )
    return normalized


def _new_identifier(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@dataclass(frozen=True, slots=True)
class OwnerActionQueueEntry:
    queue_entry_id: str
    owner_id: str
    action_id: str
    action_type: str
    action_status: str
    requires_confirmation: bool
    requires_high_trust: bool
    priority_class: str
    created_at: str
    updated_at: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "queue_entry_id",
            _stable_identifier(self.queue_entry_id, field_name="queue_entry_id"),
        )
        object.__setattr__(self, "owner_id", _stable_identifier(self.owner_id, field_name="owner_id"))
        object.__setattr__(self, "action_id", _stable_identifier(self.action_id, field_name="action_id"))
        object.__setattr__(
            self,
            "action_type",
            _stable_identifier(self.action_type, field_name="action_type").upper(),
        )
        object.__setattr__(self, "action_status", _normalize_status(self.action_status))
        object.__setattr__(
            self,
            "priority_class",
            _normalize_priority_class(self.priority_class),
        )
        object.__setattr__(self, "created_at", _normalize_text(self.created_at) or _utc_timestamp())
        object.__setattr__(self, "updated_at", _normalize_text(self.updated_at) or self.created_at)

    def to_dict(self) -> dict[str, object]:
        return {
            "queue_entry_id": self.queue_entry_id,
            "owner_id": self.owner_id,
            "action_id": self.action_id,
            "action_type": self.action_type,
            "action_status": self.action_status,
            "requires_confirmation": self.requires_confirmation,
            "requires_high_trust": self.requires_high_trust,
            "priority_class": self.priority_class,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


def create_owner_action_queue_entry(
    *,
    owner_id: object,
    action_id: object,
    action_type: object,
    action_status: object,
    requires_confirmation: bool,
    requires_high_trust: bool,
    priority_class: object,
    created_at: object | None = None,
    updated_at: object | None = None,
    queue_entry_id: object | None = None,
) -> OwnerActionQueueEntry:
    timestamp = _normalize_text(created_at) or _utc_timestamp()
    return OwnerActionQueueEntry(
        queue_entry_id=_normalize_text(queue_entry_id) or _new_identifier("owner-queue"),
        owner_id=_normalize_text(owner_id),
        action_id=_normalize_text(action_id),
        action_type=_normalize_text(action_type),
        action_status=_normalize_text(action_status),
        requires_confirmation=bool(requires_confirmation),
        requires_high_trust=bool(requires_high_trust),
        priority_class=_normalize_text(priority_class) or "medium",
        created_at=timestamp,
        updated_at=_normalize_text(updated_at) or timestamp,
    )


class OwnerActionQueue:
    def __init__(self, entries: Iterable[OwnerActionQueueEntry] | None = None) -> None:
        self._entries = list(entries or ())

    def enqueue(self, entry: OwnerActionQueueEntry) -> OwnerActionQueueEntry:
        self._entries.append(entry)
        return entry

    def list_entries(self, *, owner_id: object | None = None) -> list[OwnerActionQueueEntry]:
        normalized_owner_id = _normalize_text(owner_id)
        if not normalized_owner_id:
            return list(self._entries)
        return [
            entry
            for entry in self._entries
            if entry.owner_id == normalized_owner_id
        ]

    def get(self, queue_entry_id: object) -> OwnerActionQueueEntry | None:
        normalized_queue_entry_id = _normalize_text(queue_entry_id)
        if not normalized_queue_entry_id:
            return None
        for entry in self._entries:
            if entry.queue_entry_id == normalized_queue_entry_id:
                return entry
        return None

    def update_status(
        self,
        *,
        queue_entry_id: object,
        action_status: object,
        updated_at: object | None = None,
    ) -> OwnerActionQueueEntry | None:
        normalized_queue_entry_id = _normalize_text(queue_entry_id)
        if not normalized_queue_entry_id:
            return None
        for index, entry in enumerate(self._entries):
            if entry.queue_entry_id != normalized_queue_entry_id:
                continue
            replacement = create_owner_action_queue_entry(
                queue_entry_id=entry.queue_entry_id,
                owner_id=entry.owner_id,
                action_id=entry.action_id,
                action_type=entry.action_type,
                action_status=action_status,
                requires_confirmation=entry.requires_confirmation,
                requires_high_trust=entry.requires_high_trust,
                priority_class=entry.priority_class,
                created_at=entry.created_at,
                updated_at=updated_at,
            )
            self._entries[index] = replacement
            return replacement
        return None
