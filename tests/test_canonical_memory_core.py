from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from app.execution.action_contract import build_action_result_contract
from app.memory.canonical_memory import (
    CanonicalMemoryError,
    MemoryDecision,
    MemoryDocumentRef,
    MemoryEntityRef,
    MemoryFact,
    MemoryPreference,
    MemoryRelationship,
)
from app.memory.context_assembly import MemoryContextRequest, assemble_context
from app.memory.memory_lifecycle import MemoryLifecycleError
from app.memory.memory_policy import (
    MemoryPolicyError,
    MemoryPromotionCandidate,
    evaluate_memory_candidate,
)
from app.memory.memory_promotion import (
    from_decision,
    from_document_reference,
    from_execution_result,
    from_owner_preference,
    promote_candidate,
)
from app.memory.memory_retrieval import MemoryRetrievalQuery, retrieve_memory_objects
from app.memory.memory_store import CanonicalMemoryStore, CanonicalMemoryStoreError
from app.ownerbox.context_boundary import (
    OwnerRequestContextRef,
    assemble_owner_canonical_context,
)
from app.ownerbox.domain import (
    create_owner_action_scope,
    create_owner_domain,
    create_owner_memory_scope,
    create_owner_trust_profile,
)
from app.ownerbox.owner_orchestrator import OwnerOrchestrator
from app.ownerbox.workflow_state_store import WorkflowStateStore


def _canonical_store(tmp_path: Path) -> CanonicalMemoryStore:
    return CanonicalMemoryStore(
        db_path=tmp_path / "runtime" / "state" / "canonical_memory.sqlite3"
    )


def _owner_boundary_bundle() -> tuple[object, object, object, object]:
    memory_scope = create_owner_memory_scope(
        scope_id="owner-memory-scope-v1",
        allowed_refs=["artifact", "state"],
    )
    action_scope = create_owner_action_scope(scope_id="owner-action-scope-v1")
    trust_profile = create_owner_trust_profile(
        trust_profile_id="owner-trust-v1",
        owner_id="owner-001",
        confirmation_policy_ref="owner-confirmation-v1",
    )
    owner_domain = create_owner_domain(
        domain_id="ownerbox-main",
        owner_id="owner-001",
        trust_level="high",
        memory_scope_ref=memory_scope.scope_id,
        action_scope_ref=action_scope.scope_id,
        policy_scope_ref="owner-policy-scope-v1",
    )
    return owner_domain, memory_scope, action_scope, trust_profile


def _persist_owner_memory_set(store: CanonicalMemoryStore) -> list[str]:
    created_ids: list[str] = []
    created_ids.append(
        promote_candidate(
            from_owner_preference(
                owner_ref="owner-001",
                subject_ref="printer-profile",
                preference_key="printer.duplex",
                content_summary="Owner prefers duplex printing by default.",
                preference_value=True,
                source_ref="owner-input:pref-001",
            ),
            store=store,
        ).memory_id
    )
    created_ids.append(
        promote_candidate(
            from_execution_result(
                owner_ref="owner-001",
                subject_ref="printer-profile",
                fact_key="printer.last_success",
                content_summary="Printer adapter completed the approved print job.",
                result_payload={
                    "approved": True,
                    "result_status": "approved",
                    "result_summary": "Printed estimate packet successfully.",
                },
                source_ref="execution:print-001",
            ),
            store=store,
        ).memory_id
    )
    created_ids.append(
        promote_candidate(
            from_decision(
                owner_ref="owner-001",
                subject_ref="printer-profile",
                decision_ref="decision:printer-route-001",
                content_summary="Route estimate packets through the office printer.",
                decision_payload={"selected_path": "office-printer"},
                source_ref="decision:printer-route-001",
            ),
            store=store,
        ).memory_id
    )
    created_ids.append(
        promote_candidate(
            from_document_reference(
                owner_ref="owner-001",
                subject_ref="printer-profile",
                document_id="doc-001",
                document_locator="ownerbox/docs/printer-runbook.md",
                content_summary="Controlled runbook reference for printer operations.",
                source_ref="document:printer-runbook",
            ),
            store=store,
        ).memory_id
    )
    return created_ids


def test_memory_object_creation_by_type() -> None:
    fact = MemoryFact(
        memory_id="memory-fact-001",
        domain_type="ownerbox",
        owner_ref="owner-001",
        subject_ref="printer-profile",
        content_summary="Owner printer location is upstairs.",
        structured_payload={"fact_key": "printer.location", "fact_value": "upstairs"},
        trust_level="validated",
        trust_class="owner_validated",
        source_type="owner_input",
        source_ref="owner-input:fact-001",
    )
    decision = MemoryDecision(
        memory_id="memory-decision-001",
        domain_type="ownerbox",
        owner_ref="owner-001",
        subject_ref="printer-profile",
        content_summary="Use the office printer for estimate packets.",
        structured_payload={
            "decision_ref": "decision:001",
            "selected_path": "office-printer",
        },
        trust_level="validated",
        trust_class="structured_decision",
        source_type="decision_record",
        source_ref="decision:001",
    )
    preference = MemoryPreference(
        memory_id="memory-preference-001",
        domain_type="ownerbox",
        owner_ref="owner-001",
        subject_ref="printer-profile",
        content_summary="Prefer duplex printing.",
        structured_payload={
            "preference_key": "printer.duplex",
            "preference_value": True,
        },
        trust_level="validated",
        trust_class="owner_validated",
        source_type="owner_input",
        source_ref="owner-input:pref-001",
    )
    entity_ref = MemoryEntityRef(
        memory_id="memory-entity-001",
        domain_type="ownerbox",
        owner_ref="owner-001",
        subject_ref="printer-profile",
        content_summary="Office printer entity.",
        structured_payload={
            "entity_type": "printer",
            "entity_ref": "printer:office-main",
        },
        trust_level="validated",
        trust_class="bounded_reference",
        source_type="document_reference",
        source_ref="document:printer-inventory",
    )
    document_ref = MemoryDocumentRef(
        memory_id="memory-document-001",
        domain_type="ownerbox",
        owner_ref="owner-001",
        subject_ref="printer-profile",
        content_summary="Printer runbook document reference.",
        structured_payload={
            "document_id": "doc-001",
            "document_locator": "ownerbox/docs/runbook.md",
        },
        trust_level="validated",
        trust_class="document_controlled",
        source_type="document_reference",
        source_ref="document:runbook",
    )
    relationship = MemoryRelationship(
        memory_id="memory-relationship-001",
        domain_type="ownerbox",
        owner_ref="owner-001",
        subject_ref="printer-profile",
        content_summary="Printer profile is related to the office printer.",
        structured_payload={
            "relationship_type": "uses_device",
            "related_ref": "printer:office-main",
        },
        trust_level="validated",
        trust_class="bounded_reference",
        source_type="evidence_summary",
        source_ref="execution:evidence-001",
    )

    assert fact.memory_type == "fact"
    assert decision.memory_type == "decision"
    assert preference.memory_type == "preference"
    assert entity_ref.memory_type == "entity_ref"
    assert document_ref.memory_type == "document_ref"
    assert relationship.memory_type == "relationship"


def test_memory_policy_accepts_valid_canonical_candidates() -> None:
    candidate = from_owner_preference(
        owner_ref="owner-001",
        subject_ref="printer-profile",
        preference_key="printer.duplex",
        content_summary="Owner prefers duplex printing.",
        preference_value=True,
        source_ref="owner-input:pref-001",
    )

    decision = evaluate_memory_candidate(candidate)

    assert decision.allowed is True
    assert decision.policy_code == "allowed"


@pytest.mark.parametrize(
    ("candidate_kind", "memory_type", "source_type", "structured_payload"),
    [
        (
            "execution_result",
            "fact",
            "trace_entry",
            {"fact_key": "trace.raw", "approved": True},
        ),
        (
            "owner_fact",
            "fact",
            "raw_transcript",
            {"fact_key": "call.raw", "validated": True},
        ),
        (
            "owner_fact",
            "fact",
            "raw_browser_dump",
            {"fact_key": "browser.raw", "validated": True},
        ),
        (
            "document_reference",
            "document_ref",
            "raw_mailbox_dump",
            {"document_id": "mail-001"},
        ),
    ],
)
def test_memory_policy_rejects_raw_trace_transcript_browser_and_mailbox(
    candidate_kind: str,
    memory_type: str,
    source_type: str,
    structured_payload: dict[str, object],
) -> None:
    candidate = MemoryPromotionCandidate(
        candidate_id="candidate-raw-001",
        candidate_kind=candidate_kind,
        memory_type=memory_type,
        domain_type="ownerbox",
        owner_ref="owner-001",
        subject_ref="printer-profile",
        content_summary="Raw payload should be rejected.",
        structured_payload=structured_payload,
        trust_level="validated",
        trust_class="raw_source",
        source_type=source_type,
        source_ref=f"{source_type}:001",
    )

    decision = evaluate_memory_candidate(candidate)

    assert decision.allowed is False


def test_owner_preference_promotion_works(tmp_path: Path) -> None:
    store = _canonical_store(tmp_path)

    created = promote_candidate(
        from_owner_preference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            preference_key="printer.duplex",
            content_summary="Owner prefers duplex printing.",
            preference_value=True,
            source_ref="owner-input:pref-001",
        ),
        store=store,
    )

    assert created.memory_type == "preference"
    assert created.source_ref == "owner-input:pref-001"
    assert store.get_memory_object(created.memory_id) is not None


def test_execution_result_promotion_works(tmp_path: Path) -> None:
    store = _canonical_store(tmp_path)

    created = promote_candidate(
        from_execution_result(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            fact_key="printer.last_success",
            content_summary="Printer adapter completed the approved print job.",
            result_payload={"approved": True, "result_status": "approved"},
            source_ref="execution:print-001",
        ),
        store=store,
    )

    assert created.memory_type == "fact"
    assert created.structured_payload["result_status"] == "approved"
    assert created.source_ref == "execution:print-001"


def test_decision_promotion_works(tmp_path: Path) -> None:
    store = _canonical_store(tmp_path)

    created = promote_candidate(
        from_decision(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            decision_ref="decision:printer-route-001",
            content_summary="Route packets through the office printer.",
            decision_payload={"selected_path": "office-printer"},
            source_ref="decision:printer-route-001",
        ),
        store=store,
    )

    assert created.memory_type == "decision"
    assert created.structured_payload["decision_ref"] == "decision:printer-route-001"


def test_memory_store_persists_objects_durably(tmp_path: Path) -> None:
    store = _canonical_store(tmp_path)
    created = promote_candidate(
        from_document_reference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            document_id="doc-001",
            document_locator="ownerbox/docs/runbook.md",
            content_summary="Controlled printer runbook reference.",
            source_ref="document:runbook",
        ),
        store=store,
    )

    reloaded_store = CanonicalMemoryStore(db_path=store.db_path)
    reloaded = reloaded_store.get_memory_object(created.memory_id)

    assert reloaded is not None
    assert reloaded.to_dict() == created.to_dict()


def test_memory_retrieval_is_domain_scoped(tmp_path: Path) -> None:
    store = _canonical_store(tmp_path)
    _persist_owner_memory_set(store)

    owner_results = retrieve_memory_objects(
        store=store,
        query=MemoryRetrievalQuery(
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="printer-profile",
            limit=10,
        ),
    )
    blocked_context = assemble_owner_canonical_context(
        owner_domain=_owner_boundary_bundle()[0],
        request_ref=OwnerRequestContextRef(
            request_ref="owner-request-001",
            owner_id="owner-001",
            session_ref="owner-session-001",
            trace_id="owner-trace-001",
        ),
        memory_store=store,
        context_request={"domain_type": "dev", "subject_ref": "printer-profile"},
    )

    assert len(owner_results) == 4
    assert blocked_context["memory_refs"] == []
    assert blocked_context["assembly_metadata"]["blocked_cross_domain"] is True


def test_retrieval_ranking_respects_trust_freshness_and_type_filters(
    tmp_path: Path,
) -> None:
    store = _canonical_store(tmp_path)
    store.seed_memory_object(
        MemoryFact(
            memory_id="memory-fact-low-trust",
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="subject-001",
            content_summary="Working fact should rank below validated preference.",
            structured_payload={"fact_key": "printer.mode", "fact_value": "draft"},
            trust_level="working",
            trust_class="working_source",
            source_type="evidence_summary",
            source_ref="execution:evidence-001",
            updated_at="2026-04-15T10:00:00Z",
        )
    )
    store.seed_memory_object(
        MemoryPreference(
            memory_id="memory-preference-high-trust",
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="subject-001",
            content_summary="Validated preference should rank first.",
            structured_payload={
                "preference_key": "printer.duplex",
                "preference_value": True,
            },
            trust_level="validated",
            trust_class="owner_validated",
            source_type="owner_input",
            source_ref="owner-input:pref-002",
            updated_at="2026-04-15T09:59:00Z",
        )
    )
    store.seed_memory_object(
        MemoryFact(
            memory_id="memory-fact-high-trust-newer",
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="subject-001",
            content_summary="Canonical fact should rank above validated preference.",
            structured_payload={"fact_key": "printer.last_success", "fact_value": True},
            trust_level="canonical",
            trust_class="approved_execution",
            source_type="execution_result",
            source_ref="execution:print-002",
            updated_at="2026-04-15T10:01:00Z",
        )
    )

    results = retrieve_memory_objects(
        store=store,
        query=MemoryRetrievalQuery(
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="subject-001",
            memory_types=("fact", "preference"),
            text_query="printer",
            limit=3,
        ),
    )
    preference_only = retrieve_memory_objects(
        store=store,
        query=MemoryRetrievalQuery(
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="subject-001",
            memory_types=("preference",),
            limit=3,
        ),
    )

    assert [item.memory_id for item in results] == [
        "memory-preference-high-trust",
        "memory-fact-high-trust-newer",
        "memory-fact-low-trust",
    ]
    assert [item.memory_id for item in preference_only] == [
        "memory-preference-high-trust"
    ]


def test_context_assembly_returns_bounded_structured_context(tmp_path: Path) -> None:
    store = _canonical_store(tmp_path)
    _persist_owner_memory_set(store)

    context_pack = assemble_context(
        store=store,
        request=MemoryContextRequest(
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="printer-profile",
            text_query="printer",
            limit=2,
        ),
    )

    assert len(context_pack["memory_refs"]) == 2
    assert len(context_pack["fact_summaries"]) <= 2
    assert len(context_pack["preferences"]) <= 2
    assert "assembly_metadata" in context_pack
    assert context_pack["assembly_metadata"]["included_count"] == 2


def test_memory_store_remains_separate_from_durable_workflow_state(
    tmp_path: Path,
) -> None:
    store = _canonical_store(tmp_path)
    workflow_store = WorkflowStateStore(
        db_path=tmp_path / "runtime" / "state" / "ownerbox_workflow_state.sqlite3"
    )
    promote_candidate(
        from_owner_preference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            preference_key="printer.duplex",
            content_summary="Owner prefers duplex printing.",
            preference_value=True,
            source_ref="owner-input:pref-001",
        ),
        store=store,
    )
    with workflow_store._connect() as workflow_connection:  # noqa: SLF001
        workflow_connection.execute("SELECT 1")
    with sqlite3.connect(str(store.db_path)) as connection:
        memory_tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }

    assert store.db_path.name == "canonical_memory.sqlite3"
    assert workflow_store._db_path.name == "ownerbox_workflow_state.sqlite3"  # noqa: SLF001
    assert store.db_path != workflow_store._db_path  # noqa: SLF001
    assert "canonical_memory_objects" in memory_tables
    assert "WorkflowState" not in memory_tables


def test_memory_store_remains_separate_from_trace_semantics(tmp_path: Path) -> None:
    store = _canonical_store(tmp_path)
    store.list_memory_objects(domain_type="ownerbox")
    candidate = MemoryPromotionCandidate(
        candidate_id="candidate-trace-001",
        candidate_kind="execution_result",
        memory_type="fact",
        domain_type="ownerbox",
        owner_ref="owner-001",
        subject_ref="printer-profile",
        content_summary="Raw trace payload should not be canonical memory.",
        structured_payload={
            "fact_key": "trace.raw",
            "approved": True,
            "result_status": "approved",
            "raw_trace": {"step": "dispatch"},
        },
        trust_level="validated",
        trust_class="trace_source",
        source_type="execution_result",
        source_ref="trace:execution-001",
    )

    with pytest.raises(MemoryPolicyError, match="raw_content_blocked"):
        promote_candidate(candidate, store=store)

    with sqlite3.connect(str(store.db_path)) as connection:
        rows = connection.execute(
            "SELECT COUNT(1) FROM canonical_memory_objects"
        ).fetchone()

    assert rows is not None
    assert int(rows[0]) == 0


def test_malformed_invalid_memory_candidates_fail_closed(tmp_path: Path) -> None:
    store = _canonical_store(tmp_path)

    with pytest.raises(MemoryPolicyError):
        MemoryPromotionCandidate(
            candidate_id="",
            candidate_kind="owner_preference",
            memory_type="preference",
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="printer-profile",
            content_summary="Broken candidate.",
            structured_payload={"preference_key": "printer.duplex"},
            trust_level="validated",
            trust_class="owner_validated",
            source_type="owner_input",
            source_ref="owner-input:broken",
        )

    with pytest.raises(CanonicalMemoryError):
        MemoryFact(
            memory_id="memory-bad-001",
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="printer-profile",
            content_summary="Broken fact.",
            structured_payload={},
            trust_level="validated",
            trust_class="owner_validated",
            source_type="owner_input",
            source_ref="owner-input:bad-fact",
        )

    assert store.list_memory_objects(domain_type="ownerbox") == []


def test_no_cross_domain_leakage(tmp_path: Path) -> None:
    store = _canonical_store(tmp_path)
    _persist_owner_memory_set(store)
    owner_domain, _memory_scope, _action_scope, _trust_profile = (
        _owner_boundary_bundle()
    )

    blocked_pack = assemble_owner_canonical_context(
        owner_domain=owner_domain,
        request_ref=OwnerRequestContextRef(
            request_ref="owner-request-001",
            owner_id="owner-001",
            session_ref="owner-session-001",
            trace_id="owner-trace-001",
        ),
        memory_store=store,
        context_request={"domain_type": "dev", "text_query": "printer"},
    )

    assert blocked_pack["memory_refs"] == []
    assert blocked_pack["assembly_metadata"]["blocked_cross_domain"] is True
    assert blocked_pack["assembly_metadata"]["domain_type"] == "ownerbox"


def test_ownerbox_can_consume_assembled_canonical_memory_context_without_bypassing_boundaries(
    tmp_path: Path,
) -> None:
    store = _canonical_store(tmp_path)
    _persist_owner_memory_set(store)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    captured: dict[str, object] = {}

    def dispatcher(action_contract: object, **kwargs: object) -> dict[str, object]:
        captured["action_contract"] = dict(action_contract)
        captured["dispatch_kwargs"] = dict(kwargs)
        return build_action_result_contract(
            action_id=str(dict(action_contract)["action_id"]),
            status="success",
            result_type="text_generation",
            payload={"text": "Owner-facing bounded response."},
        )

    result = OwnerOrchestrator(dispatcher=dispatcher).process_request(
        request_text="Summarize printer memory for the owner",
        owner_id="owner-001",
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-us",
        canonical_memory_store=store,
        canonical_memory_context_request={
            "domain_type": "ownerbox",
            "subject_ref": "printer-profile",
            "text_query": "printer",
            "limit": 3,
        },
    )

    prompt = str(dict(captured["action_contract"])["parameters"]["prompt"])

    assert result.owner_context["canonical_memory_context"]["memory_refs"] != []
    assert (
        result.owner_context["canonical_memory_context"]["assembly_metadata"][
            "domain_type"
        ]
        == "ownerbox"
    )
    assert result.owner_context["trace_metadata"]["domain_type"] == "ownerbox"
    assert dict(captured["dispatch_kwargs"])["memory_domain"] == "ownerbox"
    assert "canonical_memory_count=" in prompt
    assert "canonical_preference_" in prompt or "canonical_fact_" in prompt


def test_memory_lifecycle_status_creation_and_transition(tmp_path: Path) -> None:
    store = _canonical_store(tmp_path)
    created = promote_candidate(
        from_owner_preference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            preference_key="printer.color",
            content_summary="Owner wants color printing enabled.",
            preference_value=True,
            source_ref="owner-input:pref-color-001",
        ),
        store=store,
        actor_scope="ownerbox",
    )

    deprecated = store.transition_memory_object(
        created.memory_id,
        status="deprecated",
        operation_type="deprecate",
        source_type="owner_input",
        source_ref="owner-input:pref-color-002",
        timestamp="2026-04-15T12:00:00Z",
        actor_scope="ownerbox",
    )

    assert created.status == "active"
    assert created.audit_metadata["operation_type"] == "create"
    assert deprecated.status == "deprecated"
    assert deprecated.audit_metadata == {
        "operation_type": "deprecate",
        "source_type": "owner_input",
        "source_ref": "owner-input:pref-color-002",
        "prior_memory_id": created.memory_id,
        "actor_scope": "ownerbox",
        "timestamp": "2026-04-15T12:00:00Z",
        "lifecycle_from": "active",
        "lifecycle_to": "deprecated",
    }


def test_superseding_a_preference_marks_prior_record_non_active(tmp_path: Path) -> None:
    store = _canonical_store(tmp_path)
    first = promote_candidate(
        from_owner_preference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            preference_key="printer.duplex",
            content_summary="Owner prefers duplex printing.",
            preference_value=True,
            source_ref="owner-input:pref-001",
        ),
        store=store,
        actor_scope="ownerbox",
    )
    second = promote_candidate(
        from_owner_preference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            preference_key="printer.duplex",
            content_summary="Owner now prefers simplex printing.",
            preference_value=False,
            source_ref="owner-input:pref-002",
        ),
        store=store,
        actor_scope="ownerbox",
    )

    first_reloaded = store.get_memory_object(first.memory_id)
    assert first_reloaded is not None
    assert first_reloaded.status == "superseded"
    assert first_reloaded.superseded_by_memory_id == second.memory_id
    assert first_reloaded.audit_metadata["operation_type"] == "supersede"
    assert second.audit_metadata["prior_memory_id"] == first.memory_id
    assert [
        item.memory_id
        for item in retrieve_memory_objects(
            store=store,
            query=MemoryRetrievalQuery(
                domain_type="ownerbox",
                owner_ref="owner-001",
                subject_ref="printer-profile",
                memory_types=("preference",),
                limit=5,
            ),
        )
    ] == [second.memory_id]


def test_superseding_a_decision_preserves_prior_record_and_provenance(
    tmp_path: Path,
) -> None:
    store = _canonical_store(tmp_path)
    first = promote_candidate(
        MemoryPromotionCandidate(
            candidate_id="candidate-decision-001",
            candidate_kind="decision",
            memory_type="decision",
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="printer-profile",
            content_summary="Route packets through printer lane A.",
            structured_payload={
                "decision_ref": "decision:printer-route-001",
                "decision_scope": "printer-routing",
                "decision_payload": {"selected_path": "lane-a"},
            },
            trust_level="validated",
            trust_class="structured_decision",
            source_type="decision_record",
            source_ref="decision:printer-route-001",
        ),
        store=store,
        actor_scope="ownerbox",
    )
    second = promote_candidate(
        MemoryPromotionCandidate(
            candidate_id="candidate-decision-002",
            candidate_kind="decision",
            memory_type="decision",
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="printer-profile",
            content_summary="Route packets through printer lane B.",
            structured_payload={
                "decision_ref": "decision:printer-route-002",
                "decision_scope": "printer-routing",
                "decision_payload": {"selected_path": "lane-b"},
            },
            trust_level="validated",
            trust_class="structured_decision",
            source_type="decision_record",
            source_ref="decision:printer-route-002",
        ),
        store=store,
        actor_scope="ownerbox",
    )

    first_reloaded = store.get_memory_object(first.memory_id)
    assert first_reloaded is not None
    assert first_reloaded.status == "superseded"
    assert first_reloaded.source_ref == "decision:printer-route-001"
    assert first_reloaded.superseded_by_memory_id == second.memory_id
    assert second.audit_metadata["prior_memory_id"] == first.memory_id


def test_conflicting_fact_promotion_is_handled_explicitly(tmp_path: Path) -> None:
    store = _canonical_store(tmp_path)
    first = promote_candidate(
        from_execution_result(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            fact_key="printer.last_success",
            content_summary="Printer job completed on lane A.",
            result_payload={
                "approved": True,
                "result_status": "approved",
                "result_summary": "Lane A completed the job.",
            },
            source_ref="execution:print-001",
        ),
        store=store,
        actor_scope="ownerbox",
    )
    second = promote_candidate(
        from_execution_result(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            fact_key="printer.last_success",
            content_summary="Printer job completed on lane B.",
            result_payload={
                "approved": True,
                "result_status": "approved",
                "result_summary": "Lane B completed the job.",
            },
            source_ref="execution:print-002",
        ),
        store=store,
        actor_scope="ownerbox",
    )

    first_reloaded = store.get_memory_object(first.memory_id)
    assert first_reloaded is not None
    assert first_reloaded.status == "superseded"
    assert first_reloaded.superseded_by_memory_id == second.memory_id
    assert second.audit_metadata["prior_memory_id"] == first.memory_id


def test_duplicate_active_conflicts_are_rejected_explicitly(tmp_path: Path) -> None:
    store = _canonical_store(tmp_path)
    promote_candidate(
        from_owner_preference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            preference_key="printer.duplex",
            content_summary="Owner prefers duplex printing.",
            preference_value=True,
            source_ref="owner-input:pref-001",
        ),
        store=store,
    )

    with pytest.raises(MemoryLifecycleError, match="duplicate_active_memory"):
        promote_candidate(
            from_owner_preference(
                owner_ref="owner-001",
                subject_ref="printer-profile",
                preference_key="printer.duplex",
                content_summary="Owner prefers duplex printing.",
                preference_value=True,
                source_ref="owner-input:pref-002",
            ),
            store=store,
        )


def test_malformed_candidate_rejected_with_explicit_reason() -> None:
    with pytest.raises(MemoryPolicyError, match="source_ref must not be empty"):
        MemoryPromotionCandidate(
            candidate_id="candidate-bad-001",
            candidate_kind="owner_preference",
            memory_type="preference",
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="printer-profile",
            content_summary="Broken candidate.",
            structured_payload={"preference_key": "printer.duplex", "validated": True},
            trust_level="validated",
            trust_class="owner_validated",
            source_type="owner_input",
            source_ref="",
        )


@pytest.mark.parametrize(
    ("source_type", "structured_payload"),
    [
        ("raw_printer_payload", {"fact_key": "printer.payload", "validated": True}),
        ("raw_email_body", {"document_id": "mail-001"}),
    ],
)
def test_raw_external_payloads_remain_rejected_with_explicit_reason(
    source_type: str,
    structured_payload: dict[str, object],
) -> None:
    candidate = MemoryPromotionCandidate(
        candidate_id=f"candidate-{source_type}",
        candidate_kind="owner_fact"
        if source_type == "raw_printer_payload"
        else "document_reference",
        memory_type="fact" if source_type == "raw_printer_payload" else "document_ref",
        domain_type="ownerbox",
        owner_ref="owner-001",
        subject_ref="printer-profile",
        content_summary="Raw payload should be rejected.",
        structured_payload=structured_payload,
        trust_level="validated",
        trust_class="raw_source",
        source_type=source_type,
        source_ref=f"{source_type}:001",
    )

    decision = evaluate_memory_candidate(candidate)

    assert decision.allowed is False
    assert decision.policy_code == "disallowed_source_type"


def test_retrieval_returns_active_only_by_default(tmp_path: Path) -> None:
    store = _canonical_store(tmp_path)
    first = promote_candidate(
        from_owner_preference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            preference_key="printer.duplex",
            content_summary="Owner prefers duplex printing.",
            preference_value=True,
            source_ref="owner-input:pref-001",
        ),
        store=store,
    )
    second = promote_candidate(
        from_owner_preference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            preference_key="printer.duplex",
            content_summary="Owner now prefers simplex printing.",
            preference_value=False,
            source_ref="owner-input:pref-002",
        ),
        store=store,
    )
    archived = promote_candidate(
        from_document_reference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            document_id="doc-archive-001",
            document_locator="ownerbox/docs/old-runbook.md",
            content_summary="Archived runbook reference.",
            source_ref="document:old-runbook",
        ),
        store=store,
    )
    store.transition_memory_object(
        archived.memory_id,
        status="archived",
        operation_type="archive",
        source_type="document_reference",
        source_ref="document:old-runbook",
        timestamp="2026-04-15T12:05:00Z",
        actor_scope="ownerbox",
    )

    results = retrieve_memory_objects(
        store=store,
        query=MemoryRetrievalQuery(
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="printer-profile",
            limit=10,
        ),
    )

    assert [item.memory_id for item in results] == [second.memory_id]
    assert first.memory_id not in [item.memory_id for item in results]


def test_retrieval_can_include_superseded_and_archived_only_when_explicitly_requested(
    tmp_path: Path,
) -> None:
    store = _canonical_store(tmp_path)
    first = promote_candidate(
        from_owner_preference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            preference_key="printer.duplex",
            content_summary="Owner prefers duplex printing.",
            preference_value=True,
            source_ref="owner-input:pref-001",
        ),
        store=store,
    )
    second = promote_candidate(
        from_owner_preference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            preference_key="printer.duplex",
            content_summary="Owner now prefers simplex printing.",
            preference_value=False,
            source_ref="owner-input:pref-002",
        ),
        store=store,
    )
    archived = promote_candidate(
        from_document_reference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            document_id="doc-archive-001",
            document_locator="ownerbox/docs/old-runbook.md",
            content_summary="Archived runbook reference.",
            source_ref="document:old-runbook",
        ),
        store=store,
    )
    store.transition_memory_object(
        archived.memory_id,
        status="archived",
        operation_type="archive",
        source_type="document_reference",
        source_ref="document:old-runbook",
        timestamp="2026-04-15T12:05:00Z",
        actor_scope="ownerbox",
    )

    results = retrieve_memory_objects(
        store=store,
        query=MemoryRetrievalQuery(
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="printer-profile",
            status_filters=("active", "superseded", "archived"),
            limit=10,
        ),
    )

    assert [item.memory_id for item in results] == [
        second.memory_id,
        first.memory_id,
        archived.memory_id,
    ]


def test_retrieval_ranking_remains_deterministic_across_equal_score_candidates(
    tmp_path: Path,
) -> None:
    store = _canonical_store(tmp_path)
    store.seed_memory_object(
        MemoryFact(
            memory_id="memory-fact-alpha",
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="subject-001",
            content_summary="Alpha fact.",
            structured_payload={"fact_key": "printer.alpha", "fact_value": True},
            trust_level="validated",
            trust_class="owner_validated",
            source_type="owner_input",
            source_ref="owner-input:alpha",
            updated_at="2026-04-15T10:00:00Z",
        )
    )
    store.seed_memory_object(
        MemoryFact(
            memory_id="memory-fact-beta",
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="subject-001",
            content_summary="Beta fact.",
            structured_payload={"fact_key": "printer.beta", "fact_value": True},
            trust_level="validated",
            trust_class="owner_validated",
            source_type="owner_input",
            source_ref="owner-input:beta",
            updated_at="2026-04-15T10:00:00Z",
        )
    )

    results = retrieve_memory_objects(
        store=store,
        query=MemoryRetrievalQuery(
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="subject-001",
            memory_types=("fact",),
            limit=10,
        ),
    )

    assert [item.memory_id for item in results] == [
        "memory-fact-alpha",
        "memory-fact-beta",
    ]


def test_context_assembly_excludes_deprecated_and_superseded_by_default(
    tmp_path: Path,
) -> None:
    store = _canonical_store(tmp_path)
    first = promote_candidate(
        from_owner_preference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            preference_key="printer.duplex",
            content_summary="Owner prefers duplex printing.",
            preference_value=True,
            source_ref="owner-input:pref-001",
        ),
        store=store,
    )
    second = promote_candidate(
        from_owner_preference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            preference_key="printer.duplex",
            content_summary="Owner now prefers simplex printing.",
            preference_value=False,
            source_ref="owner-input:pref-002",
        ),
        store=store,
    )
    deprecated = promote_candidate(
        from_document_reference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            document_id="doc-001",
            document_locator="ownerbox/docs/runbook.md",
            content_summary="Current runbook reference.",
            source_ref="document:runbook",
        ),
        store=store,
    )
    store.transition_memory_object(
        deprecated.memory_id,
        status="deprecated",
        operation_type="deprecate",
        source_type="document_reference",
        source_ref="document:runbook:v2",
        timestamp="2026-04-15T12:00:00Z",
        actor_scope="ownerbox",
    )

    context_pack = assemble_context(
        store=store,
        request=MemoryContextRequest(
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="printer-profile",
            limit=10,
        ),
    )

    assert [entry["memory_id"] for entry in context_pack["memory_refs"]] == [
        second.memory_id
    ]
    assert sorted(context_pack["assembly_metadata"]["excluded_memory_ids"]) == sorted(
        [first.memory_id, deprecated.memory_id]
    )
    assert context_pack["assembly_metadata"]["exclusion_reasons"] == {
        "status:deprecated": 1,
        "status:superseded": 1,
    }


def test_context_assembly_enforces_bounded_per_type_caps(tmp_path: Path) -> None:
    store = _canonical_store(tmp_path)
    for index in range(3):
        promote_candidate(
            from_owner_preference(
                owner_ref="owner-001",
                subject_ref="printer-profile",
                preference_key=f"printer.preference.{index}",
                content_summary=f"Preference {index}.",
                preference_value=bool(index % 2),
                source_ref=f"owner-input:pref-{index:03d}",
            ),
            store=store,
        )

    context_pack = assemble_context(
        store=store,
        request=MemoryContextRequest(
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="printer-profile",
            per_type_limits={"preference": 1},
            limit=5,
        ),
    )

    assert len(context_pack["preferences"]) == 1
    assert context_pack["assembly_metadata"]["counts_by_memory_type"] == {
        "preference": 1
    }
    assert context_pack["assembly_metadata"]["exclusion_reasons"] == {
        "type_cap:preference": 2
    }


def test_memory_write_audit_metadata_is_preserved(tmp_path: Path) -> None:
    store = _canonical_store(tmp_path)
    created = promote_candidate(
        from_owner_preference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            preference_key="printer.duplex",
            content_summary="Owner prefers duplex printing.",
            preference_value=True,
            source_ref="owner-input:pref-001",
        ),
        store=store,
        actor_scope="ownerbox",
    )

    reloaded = store.get_memory_object(created.memory_id)

    assert reloaded is not None
    assert reloaded.audit_metadata == {
        "operation_type": "create",
        "source_type": "owner_input",
        "source_ref": "owner-input:pref-001",
        "prior_memory_id": None,
        "actor_scope": "ownerbox",
        "timestamp": created.updated_at,
    }


def test_direct_memory_writes_are_rejected_without_promotion_gate(
    tmp_path: Path,
) -> None:
    store = _canonical_store(tmp_path)

    with pytest.raises(CanonicalMemoryStoreError, match="direct_memory_write_rejected"):
        store.create_memory_object(
            MemoryFact(
                memory_id="memory-direct-write-001",
                domain_type="ownerbox",
                owner_ref="owner-001",
                subject_ref="printer-profile",
                content_summary="Direct writes must be rejected.",
                structured_payload={"fact_key": "printer.direct", "fact_value": True},
                source_type="owner_input",
                source_ref="owner-input:direct-write-001",
            )
        )


def test_audit_layer_reports_active_truths_lineage_and_conflicts(
    tmp_path: Path,
) -> None:
    store = _canonical_store(tmp_path)
    first = promote_candidate(
        from_owner_preference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            preference_key="printer.duplex",
            content_summary="Owner prefers duplex printing.",
            preference_value=True,
            source_ref="owner-input:pref-001",
        ),
        store=store,
        actor_scope="ownerbox",
    )
    second = promote_candidate(
        from_owner_preference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            preference_key="printer.duplex",
            content_summary="Owner now prefers simplex printing.",
            preference_value=False,
            source_ref="owner-input:pref-002",
        ),
        store=store,
        actor_scope="ownerbox",
    )

    active_truths = store.list_active_truths(
        domain_scope="ownerbox",
        owner_ref="owner-001",
        subject_ref="printer-profile",
    )
    supersession_chain = store.get_supersession_chain(second.memory_id)
    lineage = store.trace_memory_lineage(second.memory_id)
    conflicts = store.detect_conflicts(
        domain_scope="ownerbox",
        owner_ref="owner-001",
        subject_ref="printer-profile",
    )

    assert [item.memory_id for item in active_truths] == [second.memory_id]
    assert [item.memory_id for item in supersession_chain] == [
        second.memory_id,
        first.memory_id,
    ]
    assert lineage["memory_id"] == second.memory_id
    assert lineage["source_trace_id"] == second.source_trace_id
    assert lineage["evidence_ref"] == second.evidence_ref
    assert len(conflicts) == 1
    assert conflicts[0]["memory_ids"] == [second.memory_id, first.memory_id]


def test_duplicate_active_fact_with_same_typed_key_and_value_is_rejected(
    tmp_path: Path,
) -> None:
    store = _canonical_store(tmp_path)
    promote_candidate(
        from_execution_result(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            fact_key="printer.last_success",
            content_summary="Printer lane A completed successfully.",
            result_payload={
                "approved": True,
                "result_status": "approved",
                "result_summary": "Printer lane A completed successfully.",
            },
            source_ref="execution:print-001",
        ),
        store=store,
    )

    with pytest.raises(MemoryLifecycleError, match="duplicate_active_memory"):
        promote_candidate(
            from_execution_result(
                owner_ref="owner-001",
                subject_ref="printer-profile",
                fact_key="printer.last_success",
                content_summary="Printer lane A completed successfully.",
                result_payload={
                    "approved": True,
                    "result_status": "approved",
                    "result_summary": "Printer lane A completed successfully.",
                },
                source_ref="execution:print-002",
            ),
            store=store,
        )


def test_duplicate_active_document_ref_identity_is_rejected(tmp_path: Path) -> None:
    store = _canonical_store(tmp_path)
    promote_candidate(
        from_document_reference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            document_id="doc-runbook-001",
            document_locator="ownerbox/docs/runbook.md",
            content_summary="Controlled printer runbook reference.",
            source_ref="document:runbook-001",
        ),
        store=store,
    )

    with pytest.raises(MemoryLifecycleError, match="duplicate_active_memory"):
        promote_candidate(
            from_document_reference(
                owner_ref="owner-001",
                subject_ref="printer-profile",
                document_id="doc-runbook-001",
                document_locator="ownerbox/docs/runbook.md",
                content_summary="Controlled printer runbook reference.",
                source_ref="document:runbook-002",
            ),
            store=store,
        )


@pytest.mark.parametrize(
    (
        "candidate_kind",
        "memory_type",
        "source_type",
        "structured_payload",
        "expected_code",
    ),
    [
        (
            "owner_fact",
            "fact",
            "speculative_inference",
            {"fact_key": "printer.location", "validated": True},
            "disallowed_source_type",
        ),
        (
            "owner_preference",
            "preference",
            "owner_input",
            {"preference_key": "printer.duplex", "validated": False},
            "owner_preference_not_validated",
        ),
        (
            "owner_fact",
            "fact",
            "owner_input",
            {"fact_key": "printer.location", "validated": False},
            "owner_fact_not_validated",
        ),
    ],
)
def test_speculative_or_ungrounded_memory_candidates_fail_closed(
    candidate_kind: str,
    memory_type: str,
    source_type: str,
    structured_payload: dict[str, object],
    expected_code: str,
) -> None:
    candidate = MemoryPromotionCandidate(
        candidate_id=f"candidate-{expected_code}",
        candidate_kind=candidate_kind,
        memory_type=memory_type,
        domain_type="ownerbox",
        owner_ref="owner-001",
        subject_ref="printer-profile",
        content_summary="Ungrounded memory should be rejected.",
        structured_payload=structured_payload,
        trust_level="validated",
        trust_class="owner_validated",
        source_type=source_type,
        source_ref=f"{source_type or 'missing'}:001",
    )

    decision = evaluate_memory_candidate(candidate)

    assert decision.allowed is False
    assert decision.policy_code == expected_code


def test_retrieval_filter_integrity_respects_combined_scope_constraints(
    tmp_path: Path,
) -> None:
    store = _canonical_store(tmp_path)
    store.seed_memory_object(
        MemoryFact(
            memory_id="memory-owner-001-current",
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="printer-profile",
            content_summary="Current validated printer profile fact.",
            structured_payload={"fact_key": "printer.profile", "fact_value": "current"},
            trust_level="validated",
            trust_class="owner_validated",
            source_type="owner_input",
            source_ref="owner-input:filter-001",
            updated_at="2026-04-14T12:00:00Z",
        )
    )
    store.seed_memory_object(
        MemoryFact(
            memory_id="memory-owner-001-stale",
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="printer-profile",
            content_summary="Old validated printer profile fact.",
            structured_payload={
                "fact_key": "printer.profile.old",
                "fact_value": "stale",
            },
            trust_level="validated",
            trust_class="owner_validated",
            source_type="owner_input",
            source_ref="owner-input:filter-002",
            updated_at="2026-03-01T12:00:00Z",
        )
    )
    store.seed_memory_object(
        MemoryFact(
            memory_id="memory-owner-001-working",
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="printer-profile",
            content_summary="Working printer profile fact.",
            structured_payload={
                "fact_key": "printer.profile.working",
                "fact_value": "draft",
            },
            trust_level="working",
            trust_class="working_source",
            source_type="evidence_summary",
            source_ref="execution:filter-003",
            updated_at="2026-04-14T12:00:00Z",
        )
    )
    store.seed_memory_object(
        MemoryFact(
            memory_id="memory-owner-002-current",
            domain_type="ownerbox",
            owner_ref="owner-002",
            subject_ref="printer-profile",
            content_summary="Other owner printer profile fact.",
            structured_payload={
                "fact_key": "printer.profile",
                "fact_value": "other-owner",
            },
            trust_level="validated",
            trust_class="owner_validated",
            source_type="owner_input",
            source_ref="owner-input:filter-004",
            updated_at="2026-04-14T12:00:00Z",
        )
    )

    results = retrieve_memory_objects(
        store=store,
        query=MemoryRetrievalQuery(
            domain_type="ownerbox",
            memory_types=("fact",),
            owner_ref="owner-001",
            subject_ref="printer-profile",
            status_filters=("active",),
            trust_classes=("owner_validated",),
            text_query="printer profile",
            freshness_window_days=7,
            reference_timestamp="2026-04-15T12:00:00Z",
            limit=10,
        ),
    )

    assert [item.memory_id for item in results] == ["memory-owner-001-current"]


def test_ranking_order_is_stable_across_repeated_runs(tmp_path: Path) -> None:
    store = _canonical_store(tmp_path)
    store.seed_memory_object(
        MemoryFact(
            memory_id="memory-rank-alpha",
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="subject-001",
            content_summary="Alpha ranking fact.",
            structured_payload={"fact_key": "printer.alpha", "fact_value": True},
            trust_level="validated",
            trust_class="owner_validated",
            source_type="owner_input",
            source_ref="owner-input:rank-alpha",
            updated_at="2026-04-15T10:00:00Z",
        )
    )
    store.seed_memory_object(
        MemoryFact(
            memory_id="memory-rank-beta",
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="subject-001",
            content_summary="Beta ranking fact.",
            structured_payload={"fact_key": "printer.beta", "fact_value": True},
            trust_level="validated",
            trust_class="owner_validated",
            source_type="owner_input",
            source_ref="owner-input:rank-beta",
            updated_at="2026-04-15T10:00:00Z",
        )
    )

    observed_orders = [
        [
            item.memory_id
            for item in retrieve_memory_objects(
                store=store,
                query=MemoryRetrievalQuery(
                    domain_type="ownerbox",
                    owner_ref="owner-001",
                    subject_ref="subject-001",
                    memory_types=("fact",),
                    limit=10,
                ),
            )
        ]
        for _ in range(5)
    ]

    assert observed_orders == [
        ["memory-rank-alpha", "memory-rank-beta"],
        ["memory-rank-alpha", "memory-rank-beta"],
        ["memory-rank-alpha", "memory-rank-beta"],
        ["memory-rank-alpha", "memory-rank-beta"],
        ["memory-rank-alpha", "memory-rank-beta"],
    ]


def test_context_assembly_is_deterministic_for_same_store_state(tmp_path: Path) -> None:
    store = _canonical_store(tmp_path)
    _persist_owner_memory_set(store)

    first = assemble_context(
        store=store,
        request=MemoryContextRequest(
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="printer-profile",
            text_query="printer",
            limit=4,
        ),
    )
    second = assemble_context(
        store=store,
        request=MemoryContextRequest(
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="printer-profile",
            text_query="printer",
            limit=4,
        ),
    )

    assert second == first


def test_ownerbox_does_not_receive_canonical_memory_without_explicit_assembly_request(
    tmp_path: Path,
) -> None:
    store = _canonical_store(tmp_path)
    _persist_owner_memory_set(store)
    owner_domain, memory_scope, action_scope, trust_profile = _owner_boundary_bundle()
    captured: dict[str, object] = {}

    def dispatcher(action_contract: object, **kwargs: object) -> dict[str, object]:
        captured["action_contract"] = dict(action_contract)
        captured["dispatch_kwargs"] = dict(kwargs)
        return build_action_result_contract(
            action_id=str(dict(action_contract)["action_id"]),
            status="success",
            result_type="text_generation",
            payload={"text": "Owner-facing bounded response."},
        )

    result = OwnerOrchestrator(dispatcher=dispatcher).process_request(
        request_text="Summarize printer memory for the owner",
        owner_id="owner-001",
        owner_domain=owner_domain,
        memory_scope=memory_scope,
        action_scope=action_scope,
        trust_profile=trust_profile,
        detected_language="en-us",
        canonical_memory_store=store,
    )

    prompt = str(dict(captured["action_contract"])["parameters"]["prompt"])

    assert "canonical_memory_context" not in result.owner_context
    assert "canonical_memory_count=0" in prompt
    assert dict(captured["dispatch_kwargs"])["memory_domain"] == "ownerbox"


def test_malformed_store_rows_fail_closed_with_explicit_store_error(
    tmp_path: Path,
) -> None:
    store = _canonical_store(tmp_path)
    store.db_path.parent.mkdir(parents=True, exist_ok=True)
    store.list_memory_objects(domain_type="ownerbox")
    with sqlite3.connect(str(store.db_path)) as connection:
        connection.execute(
            """
            INSERT INTO canonical_memory_objects (
                memory_id,
                memory_type,
                domain_type,
                owner_ref,
                subject_ref,
                content_summary,
                structured_payload_json,
                trust_level,
                trust_class,
                source_type,
                source_ref,
                created_at,
                updated_at,
                status,
                audit_metadata_json,
                superseded_by_memory_id,
                conflict_key
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "memory-malformed-001",
                "fact",
                "ownerbox",
                "owner-001",
                "printer-profile",
                "Malformed record.",
                "{bad-json",
                "validated",
                "owner_validated",
                "owner_input",
                "owner-input:malformed-001",
                "2026-04-15T12:00:00Z",
                "2026-04-15T12:00:00Z",
                "active",
                "{}",
                None,
                "fact/ownerbox/owner-001/printer-profile/printer.malformed",
            ),
        )
        connection.commit()

    with pytest.raises(
        CanonicalMemoryStoreError, match="malformed canonical memory record"
    ):
        store.list_memory_objects(domain_type="ownerbox")


def test_preexisting_duplicate_active_rows_are_constrained_on_retrieval(
    tmp_path: Path,
) -> None:
    store = _canonical_store(tmp_path)
    store.db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(store.db_path)) as connection:
        connection.execute(
            """
            CREATE TABLE canonical_memory_objects (
                memory_id TEXT PRIMARY KEY,
                memory_type TEXT NOT NULL,
                domain_type TEXT NOT NULL,
                owner_ref TEXT,
                subject_ref TEXT,
                content_summary TEXT NOT NULL,
                structured_payload_json TEXT NOT NULL,
                trust_level TEXT NOT NULL,
                trust_class TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_ref TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                status TEXT NOT NULL,
                audit_metadata_json TEXT NOT NULL DEFAULT '{}',
                superseded_by_memory_id TEXT,
                conflict_key TEXT
            )
            """
        )
        duplicate_rows = [
            (
                "memory-legacy-001",
                "fact",
                "ownerbox",
                "owner-001",
                "printer-profile",
                "Legacy printer lane A fact.",
                '{"fact_key":"printer.last_success","fact_value":"lane-a"}',
                "validated",
                "owner_validated",
                "owner_input",
                "owner-input:legacy-001",
                "2026-04-15T10:00:00Z",
                "2026-04-15T10:00:00Z",
                "active",
                "{}",
                None,
                "fact/ownerbox/owner-001/printer-profile/printer.last_success",
            ),
            (
                "memory-legacy-002",
                "fact",
                "ownerbox",
                "owner-001",
                "printer-profile",
                "Legacy printer lane B fact.",
                '{"fact_key":"printer.last_success","fact_value":"lane-b"}',
                "validated",
                "owner_validated",
                "owner_input",
                "owner-input:legacy-002",
                "2026-04-15T11:00:00Z",
                "2026-04-15T11:00:00Z",
                "active",
                "{}",
                None,
                "fact/ownerbox/owner-001/printer-profile/printer.last_success",
            ),
        ]
        connection.executemany(
            """
            INSERT INTO canonical_memory_objects (
                memory_id,
                memory_type,
                domain_type,
                owner_ref,
                subject_ref,
                content_summary,
                structured_payload_json,
                trust_level,
                trust_class,
                source_type,
                source_ref,
                created_at,
                updated_at,
                status,
                audit_metadata_json,
                superseded_by_memory_id,
                conflict_key
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            duplicate_rows,
        )
        connection.commit()

    results = retrieve_memory_objects(
        store=store,
        query=MemoryRetrievalQuery(
            domain_type="ownerbox",
            owner_ref="owner-001",
            subject_ref="printer-profile",
            memory_types=("fact",),
            limit=10,
        ),
    )

    assert [item.memory_id for item in results] == ["memory-legacy-002"]


def test_partial_write_failure_rolls_back_supersession_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _canonical_store(tmp_path)
    first = promote_candidate(
        from_owner_preference(
            owner_ref="owner-001",
            subject_ref="printer-profile",
            preference_key="printer.duplex",
            content_summary="Owner prefers duplex printing.",
            preference_value=True,
            source_ref="owner-input:pref-001",
        ),
        store=store,
    )

    original_insert = store._insert_memory_object  # noqa: SLF001

    def failing_insert(connection: sqlite3.Connection, memory_object: object) -> None:
        if str(getattr(memory_object, "memory_id", "")) == "memory-failure-001":
            raise sqlite3.OperationalError("forced insert failure")
        original_insert(connection, memory_object)

    monkeypatch.setattr(store, "_insert_memory_object", failing_insert)

    candidate = MemoryPromotionCandidate(
        candidate_id="memory-failure-001",
        candidate_kind="owner_preference",
        memory_type="preference",
        domain_type="ownerbox",
        owner_ref="owner-001",
        subject_ref="printer-profile",
        content_summary="Owner now prefers simplex printing.",
        structured_payload={
            "preference_key": "printer.duplex",
            "preference_value": False,
            "validated": True,
        },
        trust_level="validated",
        trust_class="owner_validated",
        source_type="owner_input",
        source_ref="owner-input:pref-002",
    )

    with pytest.raises(CanonicalMemoryStoreError, match="forced insert failure"):
        promote_candidate(candidate, store=store)

    reloaded = store.get_memory_object(first.memory_id)

    assert reloaded is not None
    assert reloaded.status == "active"
    assert reloaded.superseded_by_memory_id is None
