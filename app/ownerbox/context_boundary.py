from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

from app.memory.context_assembly import MemoryContextRequest, assemble_context
from app.memory.memory_store import CanonicalMemoryStore
from app.memory.memory_object import memory_object_from_mapping
from app.memory.memory_registry import list_by_domain
from app.ownerbox.domain import (
    OWNERBOX_DOMAIN_TYPE,
    OwnerActionScope,
    OwnerDomain,
    OwnerMemoryScope,
    OwnerTrustProfile,
    build_ownerbox_domain_binding,
    build_ownerbox_trace_metadata,
)


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _normalized_tags(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        candidates = [value]
    elif isinstance(value, (list, tuple, set, frozenset)):
        candidates = list(value)
    else:
        return ()
    normalized: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        tag = _normalize_text(item)
        if not tag or tag in seen:
            continue
        seen.add(tag)
        normalized.append(tag)
    return tuple(normalized)


def _parse_timestamp(value: object) -> datetime:
    normalized = _normalize_text(value)
    if not normalized:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(normalized.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


def _ref_matches(candidate: str, prefixes: tuple[str, ...]) -> bool:
    return any(candidate == prefix or candidate.startswith(f"{prefix}:") for prefix in prefixes)


def _scope_allows_record(
    record: dict[str, object],
    *,
    owner_domain: OwnerDomain,
    memory_scope: OwnerMemoryScope,
    request_memory_context: Mapping[str, object],
) -> tuple[bool, str]:
    if _normalize_text(record.get("domain")).lower() != OWNERBOX_DOMAIN_TYPE:
        return False, "cross_domain"

    memory_class = _normalize_text(record.get("memory_class"))
    if memory_class in memory_scope.blocked_memory_classes:
        return False, "blocked_memory_class"
    if memory_scope.allowed_memory_classes and memory_class not in memory_scope.allowed_memory_classes:
        return False, "disallowed_memory_class"

    refs = tuple(_normalized_tags(record.get("refs")))
    if any(_ref_matches(ref, memory_scope.blocked_refs) for ref in refs):
        return False, "blocked_ref"

    type_name = _normalize_text(record.get("type")).lower()
    allow_authoritative_transcripts = bool(
        memory_scope.truth_constraints.get("allow_authoritative_transcripts", False)
    )
    if not allow_authoritative_transcripts and (
        "transcript" in type_name or any(ref.startswith("transcript:") for ref in refs)
    ):
        return False, "blocked_transcript"

    owner_ref_prefixes = (
        f"owner:{owner_domain.owner_id}",
        f"domain:{owner_domain.domain_id}",
    )
    allowed_ref_prefixes = owner_ref_prefixes + memory_scope.allowed_refs
    if bool(memory_scope.truth_constraints.get("require_explicit_refs", True)):
        if not refs or not any(_ref_matches(ref, allowed_ref_prefixes) for ref in refs):
            return False, "missing_explicit_owner_ref"

    requested_type = _normalize_text(request_memory_context.get("type"))
    requested_memory_class = _normalize_text(request_memory_context.get("memory_class"))
    requested_tags = set(_normalized_tags(request_memory_context.get("tags")))
    requested_refs = tuple(_normalized_tags(request_memory_context.get("refs")))

    if requested_type and _normalize_text(record.get("type")) != requested_type:
        return False, "requested_type_mismatch"
    if requested_memory_class and memory_class != requested_memory_class:
        return False, "requested_memory_class_mismatch"

    artifact_tags = set(_normalized_tags(record.get("tags")))
    if requested_tags and not requested_tags.issubset(artifact_tags):
        return False, "requested_tags_mismatch"
    if requested_refs and not any(_ref_matches(ref, requested_refs) for ref in refs):
        return False, "requested_refs_mismatch"

    return True, "allowed"


def _boundary_summary_template(
    *,
    owner_domain: OwnerDomain,
    memory_scope: OwnerMemoryScope,
    action_scope: OwnerActionScope,
) -> dict[str, object]:
    return {
        "domain_type": OWNERBOX_DOMAIN_TYPE,
        "owner_id": owner_domain.owner_id,
        "memory_scope_ref": owner_domain.memory_scope_ref,
        "action_scope_ref": owner_domain.action_scope_ref,
        "policy_scope_ref": owner_domain.policy_scope_ref,
        "scope_id": memory_scope.scope_id,
        "allowed_action_types": list(action_scope.allowed_action_types),
        "blocked_cross_domain_count": 0,
        "blocked_transcript_count": 0,
        "filtered_out_count": 0,
        "resolved_memory_count": 0,
    }


@dataclass(frozen=True, slots=True)
class OwnerRequestContextRef:
    request_ref: str
    owner_id: str
    session_ref: str | None = None
    trace_id: str | None = None
    turn_ref: str | None = None
    memory_context: dict[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "request_ref", _normalize_text(self.request_ref))
        object.__setattr__(self, "owner_id", _normalize_text(self.owner_id))
        object.__setattr__(self, "session_ref", _normalize_text(self.session_ref) or None)
        object.__setattr__(self, "trace_id", _normalize_text(self.trace_id) or None)
        object.__setattr__(self, "turn_ref", _normalize_text(self.turn_ref) or None)
        object.__setattr__(self, "memory_context", _normalize_mapping(self.memory_context))

    def to_dict(self) -> dict[str, object]:
        return {
            "request_ref": self.request_ref,
            "owner_id": self.owner_id,
            "session_ref": self.session_ref,
            "trace_id": self.trace_id,
            "turn_ref": self.turn_ref,
            "memory_context": dict(self.memory_context),
        }


def assemble_owner_context(
    *,
    request_ref: OwnerRequestContextRef,
    owner_domain: OwnerDomain,
    memory_scope: OwnerMemoryScope,
    action_scope: OwnerActionScope,
    trust_profile: OwnerTrustProfile,
    memory_records: list[Mapping[str, object]] | None = None,
) -> dict[str, object]:
    records = (
        [dict(record) for record in memory_records]
        if memory_records is not None
        else list_by_domain(OWNERBOX_DOMAIN_TYPE)
    )
    boundary_application = _boundary_summary_template(
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
    )
    filtered: list[dict[str, object]] = []
    for raw_record in records:
        normalized_record = memory_object_from_mapping(raw_record).to_dict()
        allowed, reason = _scope_allows_record(
            normalized_record,
            owner_domain=owner_domain,
            memory_scope=memory_scope,
            request_memory_context=request_ref.memory_context,
        )
        if not allowed:
            boundary_application["filtered_out_count"] = int(boundary_application["filtered_out_count"]) + 1
            if reason == "cross_domain":
                boundary_application["blocked_cross_domain_count"] = (
                    int(boundary_application["blocked_cross_domain_count"]) + 1
                )
            if reason == "blocked_transcript":
                boundary_application["blocked_transcript_count"] = (
                    int(boundary_application["blocked_transcript_count"]) + 1
                )
            continue
        filtered.append(normalized_record)

    filtered.sort(key=lambda item: _normalize_text(item.get("id")))
    filtered.sort(key=lambda item: _parse_timestamp(item.get("updated_at")), reverse=True)

    max_entries = int(memory_scope.truth_constraints.get("max_resolved_entries", 5) or 0)
    selected = filtered[:max_entries] if max_entries > 0 else []
    boundary_application["resolved_memory_count"] = len(selected)

    domain_binding = build_ownerbox_domain_binding(
        owner_domain,
        trust_profile=trust_profile,
        request_ref=request_ref.request_ref,
        session_ref=request_ref.session_ref,
        trace_id=request_ref.trace_id,
    )
    trace_metadata = build_ownerbox_trace_metadata(domain_binding)
    if request_ref.turn_ref:
        trace_metadata["turn_ref"] = request_ref.turn_ref

    return {
        "request_ref": request_ref.to_dict(),
        "domain": owner_domain.to_dict(),
        "memory_scope": memory_scope.to_dict(),
        "action_scope": action_scope.to_dict(),
        "trust_profile": trust_profile.to_dict(),
        "domain_binding": domain_binding,
        "trace_metadata": trace_metadata,
        "boundary_application": boundary_application,
        "resolved_memory": selected,
    }


def assemble_owner_canonical_context(
    *,
    owner_domain: OwnerDomain,
    request_ref: OwnerRequestContextRef,
    memory_store: CanonicalMemoryStore,
    context_request: Mapping[str, object] | None = None,
) -> dict[str, object]:
    request_payload = _normalize_mapping(context_request)
    requested_domain_type = _normalize_text(request_payload.get("domain_type")).lower()
    subject_ref = _normalize_text(request_payload.get("subject_ref")) or None
    memory_types = request_payload.get("memory_types")
    status_filters = request_payload.get("status_filters")
    trust_classes = request_payload.get("trust_classes")
    per_type_limits = request_payload.get("per_type_limits")
    text_query = _normalize_text(request_payload.get("text_query"))
    freshness_window_days = request_payload.get("freshness_window_days")
    reference_timestamp = _normalize_text(request_payload.get("reference_timestamp")) or None
    limit_value = request_payload.get("limit", 5)
    try:
        limit = max(1, int(limit_value))
    except (TypeError, ValueError):
        limit = 5

    if requested_domain_type and requested_domain_type != OWNERBOX_DOMAIN_TYPE:
        return {
            "memory_refs": [],
            "fact_summaries": [],
            "preferences": [],
            "relevant_decisions": [],
            "related_document_refs": [],
            "assembly_metadata": {
                "domain_type": OWNERBOX_DOMAIN_TYPE,
                "owner_ref": owner_domain.owner_id,
                "subject_ref": subject_ref,
                "text_query": text_query,
                "requested_limit": limit,
                "retrieved_count": 0,
                "included_count": 0,
                "included_memory_ids": [],
                "excluded_memory_ids": [],
                "exclusion_reasons": {},
                "counts_by_memory_type": {},
                "assembled_at": reference_timestamp or "1970-01-01T00:00:00Z",
                "blocked_cross_domain": True,
                "request_ref": request_ref.request_ref,
            },
        }

    assembled = assemble_context(
        store=memory_store,
        request=MemoryContextRequest(
            domain_type=OWNERBOX_DOMAIN_TYPE,
            owner_ref=owner_domain.owner_id,
            subject_ref=subject_ref,
            text_query=text_query,
            memory_types=tuple(memory_types)
            if isinstance(memory_types, (list, tuple, set, frozenset))
            else (),
            status_filters=tuple(status_filters)
            if isinstance(status_filters, (list, tuple, set, frozenset))
            else ("active",),
            trust_classes=tuple(trust_classes)
            if isinstance(trust_classes, (list, tuple, set, frozenset))
            else (),
            freshness_window_days=freshness_window_days,
            reference_timestamp=reference_timestamp,
            per_type_limits=per_type_limits if isinstance(per_type_limits, dict) else {},
            limit=limit,
        ),
    )
    metadata = _normalize_mapping(assembled.get("assembly_metadata"))
    metadata["request_ref"] = request_ref.request_ref
    metadata["blocked_cross_domain"] = False
    assembled["assembly_metadata"] = metadata
    return assembled
