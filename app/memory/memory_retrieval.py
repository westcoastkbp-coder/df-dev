from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from app.memory.canonical_memory import MemoryObject
from app.memory.memory_store import CanonicalMemoryStore


DEFAULT_TYPE_PRIORITY = (
    "preference",
    "fact",
    "decision",
    "document_ref",
    "relationship",
    "entity_ref",
)
STATUS_PRIORITY = {
    "active": 0,
    "superseded": 1,
    "deprecated": 2,
    "archived": 3,
}


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_identifiers(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        candidates = [value]
    elif isinstance(value, (list, tuple, set, frozenset)):
        candidates = list(value)
    else:
        return ()
    normalized: list[str] = []
    for item in candidates:
        text = _normalize_text(item).lower()
        if text and text not in normalized:
            normalized.append(text)
    return tuple(normalized)


def _parse_timestamp(value: object) -> datetime:
    normalized = _normalize_text(value)
    if not normalized:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


@dataclass(frozen=True, slots=True)
class MemoryRetrievalQuery:
    domain_scope: str = ""
    domain_type: str = ""
    memory_types: tuple[str, ...] = field(default_factory=tuple)
    owner_ref: str | None = None
    subject_ref: str | None = None
    status_filters: tuple[str, ...] = ("active",)
    trust_classes: tuple[str, ...] = field(default_factory=tuple)
    text_query: str = ""
    freshness_window_days: int | None = None
    reference_timestamp: str | None = None
    limit: int = 10
    type_priority: tuple[str, ...] = field(default_factory=lambda: DEFAULT_TYPE_PRIORITY)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "domain_scope",
            _normalize_text(self.domain_scope or self.domain_type).lower(),
        )
        object.__setattr__(self, "domain_type", self.domain_scope)
        object.__setattr__(self, "memory_types", _normalize_identifiers(self.memory_types))
        object.__setattr__(self, "owner_ref", _normalize_text(self.owner_ref) or None)
        object.__setattr__(self, "subject_ref", _normalize_text(self.subject_ref) or None)
        object.__setattr__(self, "status_filters", _normalize_identifiers(self.status_filters))
        object.__setattr__(self, "trust_classes", _normalize_identifiers(self.trust_classes))
        object.__setattr__(self, "text_query", _normalize_text(self.text_query))
        if self.freshness_window_days is None or _normalize_text(self.freshness_window_days) == "":
            object.__setattr__(self, "freshness_window_days", None)
        else:
            object.__setattr__(self, "freshness_window_days", max(0, int(self.freshness_window_days)))
        object.__setattr__(self, "reference_timestamp", _normalize_text(self.reference_timestamp) or None)
        object.__setattr__(self, "limit", max(1, int(self.limit)))
        object.__setattr__(self, "type_priority", _normalize_identifiers(self.type_priority))
        if not self.domain_scope:
            raise ValueError("domain_scope must not be empty")
        if not self.status_filters:
            object.__setattr__(self, "status_filters", ("active",))


def _updated_since(query: MemoryRetrievalQuery) -> str | None:
    if query.freshness_window_days is None:
        return None
    reference_timestamp = (
        _parse_timestamp(query.reference_timestamp)
        if query.reference_timestamp
        else datetime.now(timezone.utc)
    )
    threshold = reference_timestamp - timedelta(days=query.freshness_window_days)
    return threshold.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def retrieve_memory_objects(
    *,
    store: CanonicalMemoryStore,
    query: MemoryRetrievalQuery,
) -> list[MemoryObject]:
    base_records = store.search_memory_objects(
        query_text=query.text_query,
        domain_scope=query.domain_scope,
        owner_ref=query.owner_ref,
        subject_ref=query.subject_ref,
        status=query.status_filters,
        trust_class=query.trust_classes,
        updated_since=_updated_since(query),
        limit=max(query.limit * 5, query.limit),
    )
    filtered = [
        item for item in base_records if not query.memory_types or item.memory_type in query.memory_types
    ]
    type_rank = {
        memory_type: index for index, memory_type in enumerate(query.type_priority or DEFAULT_TYPE_PRIORITY)
    }
    filtered.sort(
        key=lambda item: (
            type_rank.get(item.memory_type, len(type_rank) + 1),
            STATUS_PRIORITY.get(item.status, len(STATUS_PRIORITY)),
            -_parse_timestamp(item.updated_at).timestamp(),
            -float(item.confidence),
            item.memory_id,
        )
    )
    constrained: list[MemoryObject] = []
    seen_active_conflict_keys: set[str] = set()
    for item in filtered:
        if item.status == "active" and item.conflict_key:
            if item.conflict_key in seen_active_conflict_keys:
                continue
            seen_active_conflict_keys.add(item.conflict_key)
        constrained.append(item)
    return constrained[: query.limit]
