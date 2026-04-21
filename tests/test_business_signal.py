from __future__ import annotations

from pathlib import Path

import pytest

from app.execution.business_signal import (
    BusinessSignalValidationError,
    adjust_decision_with_outcomes,
    build_business_signal,
    build_decision_from_business_signal,
    build_executive_state,
    decision_to_task_input,
    evaluate_decision_outcome,
)
from app.orchestrator import orchestrator as orchestrator_module
from app.orchestrator import task_factory
from app.orchestrator import task_state_store


def _configure_state_backend(monkeypatch, tmp_path: Path) -> Path:
    monkeypatch.setattr(task_state_store, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(
        task_factory,
        "TASK_SYSTEM_FILE",
        tmp_path / "data" / "task_system.json",
    )
    monkeypatch.setattr(
        orchestrator_module,
        "TASKS_FILE",
        tmp_path / "data" / "task_system.json",
    )
    task_factory.clear_task_runtime_store()
    return tmp_path / "data" / "task_system.json"


def _payload() -> dict[str, object]:
    return {
        "signal_type": "low_review_count",
        "severity": "high",
        "source": "reputation_monitor",
        "metrics": {
            "current": 12,
            "competitor_avg": 35,
        },
        "impact": "reduced_visibility",
        "recommended_action": "request_more_reviews",
    }


def test_business_signal_is_built_as_structured_contract() -> None:
    signal = build_business_signal(_payload())

    assert signal.signal_type == "low_review_count"
    assert signal.severity == "high"
    assert signal.impact == "reduced_visibility"
    assert signal.recommended_action == "request_more_reviews"
    assert dict(signal.metrics)["current"] == 12


def test_business_signal_rejects_unstructured_fields() -> None:
    payload = _payload()
    payload["summary"] = "please just do something about reviews"

    with pytest.raises(
        BusinessSignalValidationError,
        match="business_signal contains unsupported fields: summary",
    ):
        build_business_signal(payload)


def test_business_signal_maps_to_task_input() -> None:
    signal = build_business_signal(_payload())
    decision = build_decision_from_business_signal(signal)
    task_input = decision_to_task_input(signal, decision)

    assert task_input["intent"] == "business_signal_response"
    assert task_input["task_type"] == "lead"
    assert (
        dict(task_input["payload"])["decision"]["decision_type"]
        == "low_review_count_decision"
    )
    assert dict(task_input["payload"])["decision"]["reason"] == "reduced_visibility"
    assert dict(task_input["payload"])["recommended_action"] == "request_more_reviews"
    assert dict(task_input["payload"])["impact"] == "reduced_visibility"
    assert task_input["execution_mode"] == "auto"
    assert task_input["approval_status"] == "approved"


def test_business_signal_builds_structured_decision() -> None:
    signal = build_business_signal(_payload())
    decision = build_decision_from_business_signal(signal)

    assert (
        decision.decision_id
        == "low_review_count:reputation_monitor:request_more_reviews"
    )
    assert decision.decision_type == "low_review_count_decision"
    assert decision.priority == "high"
    assert decision.confidence == 1.0
    assert decision.reason == "reduced_visibility"
    assert dict(decision.evidence)["current"] == 12
    assert list(decision.recommended_actions) == ["request_more_reviews"]
    assert decision.execution_mode == "auto"


def test_decision_outcome_is_structured_from_real_metric_comparison() -> None:
    signal = build_business_signal(_payload())
    decision = build_decision_from_business_signal(signal)
    outcome = evaluate_decision_outcome(
        decision,
        before_metrics={"lead_count": 10, "review_count": 12},
        after_metrics={"lead_count": 13, "review_count": 16},
        actions_executed=["request_more_reviews"],
        timestamp="2026-04-06T03:00:00Z",
    )

    assert outcome.decision_type == "low_review_count_decision"
    assert outcome.decision_id == decision.decision_id
    assert outcome.actions_executed == ("request_more_reviews",)
    assert outcome.timestamp == "2026-04-06T03:00:00Z"
    assert outcome.outcome_status == "success"
    assert dict(outcome.metrics_delta)["lead_count"] == 3.0
    assert dict(outcome.metrics_delta)["review_count"] == 4.0
    assert outcome.confidence == 1.0


def test_decision_adjustment_reduces_priority_and_confidence_after_repeated_failures() -> (
    None
):
    signal = build_business_signal(_payload())
    decision = build_decision_from_business_signal(signal)
    outcomes = [
        evaluate_decision_outcome(
            decision,
            before_metrics={"review_count": 12},
            after_metrics={"review_count": 12},
            actions_executed=["request_more_reviews"],
            timestamp="2026-04-06T03:00:00Z",
        ),
        evaluate_decision_outcome(
            decision,
            before_metrics={"review_count": 12},
            after_metrics={"review_count": 11},
            actions_executed=["request_more_reviews"],
            timestamp="2026-04-06T03:10:00Z",
        ),
        evaluate_decision_outcome(
            decision,
            before_metrics={"review_count": 11},
            after_metrics={"review_count": 11},
            actions_executed=["request_more_reviews"],
            timestamp="2026-04-06T03:20:00Z",
        ),
    ]

    adjustment = adjust_decision_with_outcomes(decision, outcomes)

    assert adjustment.decision.priority == "medium"
    assert adjustment.decision.confidence == 0.7
    assert list(adjustment.decision.recommended_actions) == [
        "request_more_reviews",
        "investigate_visibility_drop",
        "refresh_business_profile",
    ]
    assert dict(adjustment.explanation)["non_success_count"] == 3
    assert (
        dict(adjustment.explanation)["change_reason"]
        == "repeated_no_effect_or_negative_outcomes"
    )


def test_orchestrator_creates_task_from_business_signal(
    monkeypatch, tmp_path: Path
) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)

    created = orchestrator_module.create_task_from_business_signal(_payload())
    restored = task_factory.get_task(
        str(created.get("task_id", "")).strip(), store_path
    )

    assert restored is not None
    assert restored["intent"] == "business_signal_response"
    assert restored["task_type"] == "lead"
    assert restored["approval_status"] == "approved"
    assert restored["execution_mode"] == "auto"
    assert (
        dict(restored["payload"])["business_signal"]["signal_type"]
        == "low_review_count"
    )
    assert (
        dict(restored["payload"])["decision"]["decision_id"]
        == "low_review_count:reputation_monitor:request_more_reviews"
    )
    assert (
        dict(restored["payload"])["decision"]["decision_type"]
        == "low_review_count_decision"
    )
    assert dict(restored["payload"])["recommended_action"] == "request_more_reviews"


def test_confirmation_mode_creates_task_awaiting_approval(
    monkeypatch, tmp_path: Path
) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    payload = _payload()
    payload["severity"] = "low"

    created = orchestrator_module.create_task_from_business_signal(payload)
    restored = task_factory.get_task(
        str(created.get("task_id", "")).strip(), store_path
    )

    assert restored is not None
    assert restored["status"] == "AWAITING_APPROVAL"
    assert restored["approval_status"] == "pending"
    assert restored["execution_mode"] == "confirmation"


def test_strict_mode_blocks_task_creation_until_explicit_approval(
    monkeypatch, tmp_path: Path
) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    payload = _payload()
    payload["severity"] = "critical"

    blocked = orchestrator_module.create_task_from_business_signal(payload)

    assert blocked["task_created"] is False
    assert blocked["approval_status"] == "pending"
    assert blocked["execution_mode"] == "strict"
    assert task_factory.load_tasks(store_path) == []


def test_strict_mode_creates_task_only_after_explicit_approval(
    monkeypatch, tmp_path: Path
) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    payload = _payload()
    payload["severity"] = "critical"
    signal = build_business_signal(payload)
    decision = build_decision_from_business_signal(signal)

    created = orchestrator_module.approve_decision_task_creation(
        signal,
        decision,
        approved=True,
        approved_by="dev_officer",
    )
    restored = task_factory.get_task(
        str(created.get("task_id", "")).strip(), store_path
    )

    assert restored is not None
    assert restored["status"] == "CREATED"
    assert restored["approval_status"] == "approved"
    assert restored["approved_by"] == "dev_officer"


def test_build_executive_state_aggregates_pending_approvals_and_risks() -> None:
    signal = build_business_signal(_payload())
    decision = build_decision_from_business_signal(signal)
    confirmation_decision = build_decision_from_business_signal(
        build_business_signal({**_payload(), "severity": "low"})
    )
    negative_outcome = evaluate_decision_outcome(
        decision,
        before_metrics={"lead_count": 10, "review_count": 12},
        after_metrics={"lead_count": 8, "review_count": 11},
        actions_executed=["request_more_reviews"],
        timestamp="2026-04-06T03:00:00Z",
    )

    executive_state = build_executive_state(
        business_signals=[signal],
        decisions=[decision, confirmation_decision],
        tasks=[
            {
                "task_id": "DF-APPROVAL-1",
                "task_type": "lead",
                "status": "AWAITING_APPROVAL",
                "approval_status": "pending",
                "intent": "business_signal_response",
                "payload": {"decision": confirmation_decision.to_dict()},
            },
            {
                "task_id": "DF-PROJECT-1",
                "task_type": "project",
                "status": "CREATED",
                "approval_status": "approved",
                "intent": "business_signal_response",
                "payload": {},
            },
            {
                "task_id": "DF-CONTRACTOR-1",
                "task_type": "procurement",
                "status": "VALIDATED",
                "approval_status": "approved",
                "intent": "procurement_followup",
                "payload": {},
            },
        ],
        decision_outcomes=[negative_outcome],
    )

    assert executive_state.lead_status == "needs attention"
    assert executive_state.project_status == "active"
    assert executive_state.contractor_status == "attention needed"
    assert executive_state.pending_approvals == ("low_review_count_decision",)
    assert executive_state.top_priority == "low_review_count_decision"
    assert "decision outcome(s) turned negative" in executive_state.risks[0]
    assert (
        executive_state.actions_required[0]
        == "Approve or reject: low_review_count_decision"
    )


def test_build_executive_state_returns_compact_sections_when_stable() -> None:
    signal = build_business_signal(_payload())
    decision = build_decision_from_business_signal(signal)
    success_outcome = evaluate_decision_outcome(
        decision,
        before_metrics={"lead_count": 10, "review_count": 12},
        after_metrics={"lead_count": 12, "review_count": 15},
        actions_executed=["request_more_reviews"],
        timestamp="2026-04-06T03:00:00Z",
    )

    executive_state = build_executive_state(
        business_signals=[],
        decisions=[decision],
        tasks=[],
        decision_outcomes=[success_outcome],
    )

    payload = executive_state.to_dict()

    assert executive_state.lead_status == "stable"
    assert executive_state.project_status == "stable"
    assert executive_state.contractor_status == "stable"
    assert executive_state.pending_approvals == ()
    assert executive_state.actions_required == ("No immediate owner action required",)
    assert payload["sections"]["Leads"][0] == "Lead flow stable"
    assert payload["sections"]["Projects"][0] == "No project backlog spike detected"
    assert payload["sections"]["Contractors"][0] == "Contractor pipeline stable"
