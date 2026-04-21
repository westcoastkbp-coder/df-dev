from __future__ import annotations

from typing import Any, Mapping
from uuid import uuid4

from app.memory.canonical_memory import (
    MemoryDecision,
    MemoryDocumentRef,
    MemoryFact,
    MemoryObject,
    MemoryPreference,
    build_audit_metadata,
)
from app.memory.memory_lifecycle import MemoryLifecycleError, plan_promotion
from app.memory.memory_policy import (
    MemoryPromotionCandidate,
    assert_memory_candidate_allowed,
)
from app.memory.memory_store import CanonicalMemoryStore


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _new_identifier() -> str:
    return str(uuid4())


def from_owner_fact(
    *,
    owner_ref: object,
    subject_ref: object,
    fact_key: object,
    content_summary: object,
    fact_value: object,
    source_ref: object,
    source_trace_id: object | None = None,
    evidence_ref: object | None = None,
    confidence: float = 1.0,
    domain_scope: str = "ownerbox",
) -> MemoryPromotionCandidate:
    return MemoryPromotionCandidate(
        candidate_id=_new_identifier(),
        candidate_kind="owner_fact",
        memory_type="fact",
        domain_scope=domain_scope,
        owner_ref=_normalize_text(owner_ref),
        subject_ref=_normalize_text(subject_ref),
        content_summary=_normalize_text(content_summary),
        structured_payload={
            "fact_key": _normalize_text(fact_key),
            "fact_value": fact_value,
            "validated": True,
        },
        trust_level="validated",
        trust_class="owner_validated",
        source_type="owner_input",
        source_ref=_normalize_text(source_ref),
        source_trace_id=_normalize_text(source_trace_id),
        evidence_ref=_normalize_text(evidence_ref),
        confidence=confidence,
        validation_passed=True,
        trace_complete=True,
        evidence_exists=True,
    )


def from_owner_preference(
    *,
    owner_ref: object,
    subject_ref: object,
    preference_key: object,
    content_summary: object,
    preference_value: object,
    source_ref: object,
    source_trace_id: object | None = None,
    evidence_ref: object | None = None,
    confidence: float = 1.0,
    domain_scope: str = "ownerbox",
) -> MemoryPromotionCandidate:
    return MemoryPromotionCandidate(
        candidate_id=_new_identifier(),
        candidate_kind="owner_preference",
        memory_type="preference",
        domain_scope=domain_scope,
        owner_ref=_normalize_text(owner_ref),
        subject_ref=_normalize_text(subject_ref),
        content_summary=_normalize_text(content_summary),
        structured_payload={
            "preference_key": _normalize_text(preference_key),
            "preference_value": preference_value,
            "validated": True,
        },
        trust_level="validated",
        trust_class="owner_validated",
        source_type="owner_input",
        source_ref=_normalize_text(source_ref),
        source_trace_id=_normalize_text(source_trace_id),
        evidence_ref=_normalize_text(evidence_ref),
        confidence=confidence,
        validation_passed=True,
        trace_complete=True,
        evidence_exists=True,
    )


def from_execution_result(
    *,
    owner_ref: object,
    subject_ref: object,
    fact_key: object,
    content_summary: object,
    result_payload: Mapping[str, Any],
    source_ref: object,
    source_trace_id: object | None = None,
    evidence_ref: object | None = None,
    confidence: float = 1.0,
    domain_scope: str = "ownerbox",
) -> MemoryPromotionCandidate:
    normalized_payload = dict(result_payload)
    return MemoryPromotionCandidate(
        candidate_id=_new_identifier(),
        candidate_kind="execution_result",
        memory_type="fact",
        domain_scope=domain_scope,
        owner_ref=_normalize_text(owner_ref),
        subject_ref=_normalize_text(subject_ref),
        content_summary=_normalize_text(content_summary),
        structured_payload={
            "fact_key": _normalize_text(fact_key),
            "result_status": _normalize_text(normalized_payload.get("result_status")) or "approved",
            "approved": bool(normalized_payload.get("approved", True)),
            "result_summary": _normalize_text(normalized_payload.get("result_summary")),
            "result_payload": normalized_payload,
        },
        trust_level="canonical",
        trust_class="approved_execution",
        source_type="execution_result",
        source_ref=_normalize_text(source_ref),
        source_trace_id=_normalize_text(source_trace_id),
        evidence_ref=_normalize_text(evidence_ref),
        confidence=confidence,
        validation_passed=True,
        trace_complete=True,
        evidence_exists=True,
    )


def from_decision(
    *,
    owner_ref: object,
    subject_ref: object,
    decision_ref: object,
    content_summary: object,
    decision_payload: Mapping[str, Any],
    source_ref: object,
    source_trace_id: object | None = None,
    evidence_ref: object | None = None,
    confidence: float = 1.0,
    domain_scope: str = "ownerbox",
) -> MemoryPromotionCandidate:
    return MemoryPromotionCandidate(
        candidate_id=_new_identifier(),
        candidate_kind="decision",
        memory_type="decision",
        domain_scope=domain_scope,
        owner_ref=_normalize_text(owner_ref),
        subject_ref=_normalize_text(subject_ref),
        content_summary=_normalize_text(content_summary),
        structured_payload={
            "decision_ref": _normalize_text(decision_ref),
            "decision_payload": dict(decision_payload),
        },
        trust_level="validated",
        trust_class="structured_decision",
        source_type="decision_record",
        source_ref=_normalize_text(source_ref),
        source_trace_id=_normalize_text(source_trace_id),
        evidence_ref=_normalize_text(evidence_ref),
        confidence=confidence,
        validation_passed=True,
        trace_complete=True,
        evidence_exists=True,
    )


def from_document_reference(
    *,
    owner_ref: object,
    subject_ref: object,
    document_id: object,
    document_locator: object,
    content_summary: object,
    source_ref: object,
    source_trace_id: object | None = None,
    evidence_ref: object | None = None,
    confidence: float = 1.0,
    domain_scope: str = "ownerbox",
) -> MemoryPromotionCandidate:
    return MemoryPromotionCandidate(
        candidate_id=_new_identifier(),
        candidate_kind="document_reference",
        memory_type="document_ref",
        domain_scope=domain_scope,
        owner_ref=_normalize_text(owner_ref),
        subject_ref=_normalize_text(subject_ref),
        content_summary=_normalize_text(content_summary),
        structured_payload={
            "document_id": _normalize_text(document_id),
            "document_locator": _normalize_text(document_locator),
            "controlled_summary": True,
        },
        trust_level="validated",
        trust_class="document_controlled",
        source_type="document_reference",
        source_ref=_normalize_text(source_ref),
        source_trace_id=_normalize_text(source_trace_id),
        evidence_ref=_normalize_text(evidence_ref),
        confidence=confidence,
        validation_passed=True,
        trace_complete=True,
        evidence_exists=True,
    )


def from_execution_evidence_summary(
    *,
    owner_ref: object,
    subject_ref: object,
    fact_key: object,
    content_summary: object,
    evidence_payload: Mapping[str, Any],
    source_ref: object,
    source_trace_id: object | None = None,
    evidence_ref: object | None = None,
    confidence: float = 1.0,
    domain_scope: str = "ownerbox",
) -> MemoryPromotionCandidate:
    return MemoryPromotionCandidate(
        candidate_id=_new_identifier(),
        candidate_kind="evidence_summary",
        memory_type="fact",
        domain_scope=domain_scope,
        owner_ref=_normalize_text(owner_ref),
        subject_ref=_normalize_text(subject_ref),
        content_summary=_normalize_text(content_summary),
        structured_payload={
            "fact_key": _normalize_text(fact_key),
            "bounded_summary": True,
            "evidence_payload": dict(evidence_payload),
        },
        trust_level="validated",
        trust_class="bounded_evidence",
        source_type="evidence_summary",
        source_ref=_normalize_text(source_ref),
        source_trace_id=_normalize_text(source_trace_id),
        evidence_ref=_normalize_text(evidence_ref),
        confidence=confidence,
        validation_passed=True,
        trace_complete=True,
        evidence_exists=True,
    )


def candidate_to_memory_object(
    candidate: MemoryPromotionCandidate,
    *,
    conflict_key: str | None = None,
    prior_memory_id: str | None = None,
    previous_version_id: str | None = None,
    version: int = 1,
    actor_scope: str | None = None,
) -> MemoryObject:
    common_kwargs = {
        "memory_id": candidate.candidate_id,
        "domain_scope": candidate.domain_scope,
        "owner_ref": candidate.owner_ref,
        "subject_ref": candidate.subject_ref,
        "content_summary": candidate.content_summary,
        "structured_payload": dict(candidate.structured_payload),
        "status": "active",
        "created_at": candidate.created_at,
        "updated_at": candidate.updated_at,
        "source_trace_id": candidate.source_trace_id,
        "evidence_ref": candidate.evidence_ref,
        "version": version,
        "confidence": candidate.confidence,
        "trust_level": candidate.trust_level,
        "trust_class": candidate.trust_class,
        "source_type": candidate.source_type,
        "source_ref": candidate.source_ref,
        "audit_metadata": build_audit_metadata(
            operation_type="create",
            source_type=candidate.source_type,
            source_ref=candidate.source_ref,
            timestamp=candidate.updated_at,
            prior_memory_id=prior_memory_id,
            actor_scope=actor_scope,
        ),
        "previous_version_id": previous_version_id,
        "conflict_key": conflict_key,
    }
    if candidate.memory_type == "fact":
        return MemoryFact(**common_kwargs)
    if candidate.memory_type == "decision":
        return MemoryDecision(**common_kwargs)
    if candidate.memory_type == "preference":
        return MemoryPreference(**common_kwargs)
    if candidate.memory_type == "document_ref":
        return MemoryDocumentRef(**common_kwargs)
    raise ValueError(f"unsupported candidate memory_type: {candidate.memory_type}")


def promote_candidate(
    candidate: MemoryPromotionCandidate,
    *,
    store: CanonicalMemoryStore,
    actor_scope: str | None = None,
) -> MemoryObject:
    assert_memory_candidate_allowed(candidate)
    promotion_plan = plan_promotion(candidate, store=store)
    memory_object = candidate_to_memory_object(
        candidate,
        conflict_key=promotion_plan.conflict_key,
        prior_memory_id=promotion_plan.prior_memory_id,
        previous_version_id=promotion_plan.previous_version_id,
        version=promotion_plan.next_version,
        actor_scope=actor_scope,
    )
    try:
        return store.create_memory_object(
            memory_object,
            supersede_prior=promotion_plan.prior_active_records,
            promotion_gate_passed=True,
        )
    except MemoryLifecycleError:
        raise
