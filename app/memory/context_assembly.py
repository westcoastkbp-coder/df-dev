from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone

from app.memory.canonical_memory import MemoryObject, SUPPORTED_STATUSES
from app.memory.memory_retrieval import MemoryRetrievalQuery, retrieve_memory_objects
from app.memory.memory_store import CanonicalMemoryStore


DEFAULT_PER_TYPE_LIMITS = {
    "preference": 2,
    "fact": 3,
    "decision": 2,
    "document_ref": 2,
    "relationship": 1,
    "entity_ref": 1,
}


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _parse_timestamp(value: object) -> datetime:
    normalized = _normalize_text(value)
    if not normalized:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00")).astimezone(
            timezone.utc
        )
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


def _deterministic_assembled_at(
    *,
    request: "MemoryContextRequest",
    retrieved: list[MemoryObject],
) -> str:
    if request.reference_timestamp:
        return request.reference_timestamp
    if retrieved:
        return max(
            retrieved,
            key=lambda item: (_parse_timestamp(item.updated_at), item.memory_id),
        ).updated_at
    return "1970-01-01T00:00:00Z"


def _memory_ref(memory_object: MemoryObject) -> dict[str, object]:
    return {
        "memory_id": memory_object.memory_id,
        "memory_type": memory_object.memory_type,
        "source_ref": memory_object.source_ref,
        "trust_level": memory_object.trust_level,
    }


def _summary_entry(memory_object: MemoryObject) -> dict[str, object]:
    return {
        "memory_id": memory_object.memory_id,
        "summary": memory_object.content_summary,
        "payload": dict(memory_object.structured_payload),
    }


def _normalize_type_limits(value: object) -> dict[str, int]:
    if not isinstance(value, dict):
        return dict(DEFAULT_PER_TYPE_LIMITS)
    normalized = dict(DEFAULT_PER_TYPE_LIMITS)
    for raw_key, raw_limit in value.items():
        key = _normalize_text(raw_key).lower()
        if not key:
            continue
        try:
            normalized[key] = max(0, int(raw_limit))
        except (TypeError, ValueError):
            continue
    return normalized


@dataclass(frozen=True, slots=True)
class MemoryContextRequest:
    domain_type: str
    owner_ref: str
    subject_ref: str | None = None
    text_query: str = ""
    memory_types: tuple[str, ...] = ()
    status_filters: tuple[str, ...] = ("active",)
    trust_classes: tuple[str, ...] = ()
    freshness_window_days: int | None = None
    reference_timestamp: str | None = None
    per_type_limits: dict[str, int] = field(
        default_factory=lambda: dict(DEFAULT_PER_TYPE_LIMITS)
    )
    limit: int = 8

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "domain_type", _normalize_text(self.domain_type).lower()
        )
        object.__setattr__(self, "owner_ref", _normalize_text(self.owner_ref))
        object.__setattr__(
            self, "subject_ref", _normalize_text(self.subject_ref) or None
        )
        object.__setattr__(self, "text_query", _normalize_text(self.text_query))
        normalized_memory_types = self.memory_types
        if isinstance(normalized_memory_types, str):
            normalized_memory_types = (normalized_memory_types,)
        elif not isinstance(normalized_memory_types, tuple):
            normalized_memory_types = tuple(normalized_memory_types)
        object.__setattr__(
            self,
            "memory_types",
            tuple(
                _normalize_text(item).lower()
                for item in normalized_memory_types
                if _normalize_text(item)
            ),
        )
        normalized_status_filters = self.status_filters
        if isinstance(normalized_status_filters, str):
            normalized_status_filters = (normalized_status_filters,)
        elif not isinstance(normalized_status_filters, tuple):
            normalized_status_filters = tuple(normalized_status_filters)
        object.__setattr__(
            self,
            "status_filters",
            tuple(
                _normalize_text(item).lower()
                for item in normalized_status_filters
                if _normalize_text(item)
            )
            or ("active",),
        )
        normalized_trust_classes = self.trust_classes
        if isinstance(normalized_trust_classes, str):
            normalized_trust_classes = (normalized_trust_classes,)
        elif not isinstance(normalized_trust_classes, tuple):
            normalized_trust_classes = tuple(normalized_trust_classes)
        object.__setattr__(
            self,
            "trust_classes",
            tuple(
                _normalize_text(item)
                for item in normalized_trust_classes
                if _normalize_text(item)
            ),
        )
        if (
            self.freshness_window_days is None
            or _normalize_text(self.freshness_window_days) == ""
        ):
            object.__setattr__(self, "freshness_window_days", None)
        else:
            object.__setattr__(
                self,
                "freshness_window_days",
                max(0, int(self.freshness_window_days)),
            )
        object.__setattr__(
            self,
            "reference_timestamp",
            _normalize_text(self.reference_timestamp) or None,
        )
        object.__setattr__(
            self, "per_type_limits", _normalize_type_limits(self.per_type_limits)
        )
        object.__setattr__(self, "limit", max(1, min(int(self.limit), 20)))
        if not self.domain_type:
            raise ValueError("domain_type must not be empty")
        if not self.owner_ref:
            raise ValueError("owner_ref must not be empty")


def assemble_context(
    *,
    store: CanonicalMemoryStore,
    request: MemoryContextRequest,
) -> dict[str, object]:
    explicit_status_filters = tuple(request.status_filters)
    retrieval_status_filters = (
        tuple(sorted(SUPPORTED_STATUSES))
        if explicit_status_filters == ("active",)
        else explicit_status_filters
    )
    pool_limit = max(
        request.limit * 6, sum(request.per_type_limits.values()), request.limit
    )
    retrieved = retrieve_memory_objects(
        store=store,
        query=MemoryRetrievalQuery(
            domain_scope=request.domain_type,
            memory_types=request.memory_types,
            owner_ref=request.owner_ref,
            subject_ref=request.subject_ref,
            status_filters=retrieval_status_filters,
            trust_classes=request.trust_classes,
            text_query=request.text_query,
            freshness_window_days=request.freshness_window_days,
            reference_timestamp=request.reference_timestamp,
            limit=pool_limit,
        ),
    )
    included: list[MemoryObject] = []
    excluded_memory_ids: list[str] = []
    exclusion_reasons: Counter[str] = Counter()
    counts_by_memory_type: Counter[str] = Counter()
    included_memory_ids: list[str] = []

    for item in retrieved:
        if item.status not in explicit_status_filters:
            excluded_memory_ids.append(item.memory_id)
            exclusion_reasons[f"status:{item.status}"] += 1
            continue
        per_type_limit = request.per_type_limits.get(item.memory_type, request.limit)
        if counts_by_memory_type[item.memory_type] >= per_type_limit:
            excluded_memory_ids.append(item.memory_id)
            exclusion_reasons[f"type_cap:{item.memory_type}"] += 1
            continue
        if len(included) >= request.limit:
            excluded_memory_ids.append(item.memory_id)
            exclusion_reasons["overall_limit"] += 1
            continue
        included.append(item)
        included_memory_ids.append(item.memory_id)
        counts_by_memory_type[item.memory_type] += 1

    return {
        "memory_refs": [_memory_ref(item) for item in included],
        "fact_summaries": [
            _summary_entry(item) for item in included if item.memory_type == "fact"
        ],
        "preferences": [
            _summary_entry(item)
            for item in included
            if item.memory_type == "preference"
        ],
        "relevant_decisions": [
            _summary_entry(item) for item in included if item.memory_type == "decision"
        ],
        "related_document_refs": [
            _summary_entry(item)
            for item in included
            if item.memory_type == "document_ref"
        ],
        "assembly_metadata": {
            "domain_type": request.domain_type,
            "owner_ref": request.owner_ref,
            "subject_ref": request.subject_ref,
            "text_query": request.text_query,
            "requested_limit": request.limit,
            "retrieved_count": len(retrieved),
            "included_count": len(included),
            "included_memory_ids": included_memory_ids,
            "excluded_memory_ids": excluded_memory_ids,
            "exclusion_reasons": dict(sorted(exclusion_reasons.items())),
            "counts_by_memory_type": dict(sorted(counts_by_memory_type.items())),
            "status_filters": list(explicit_status_filters),
            "per_type_limits": dict(sorted(request.per_type_limits.items())),
            "assembled_at": _deterministic_assembled_at(
                request=request,
                retrieved=retrieved,
            ),
        },
    }
