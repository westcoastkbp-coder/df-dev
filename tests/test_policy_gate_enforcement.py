from __future__ import annotations

from app.execution.lead_estimate_contract import WORKFLOW_TYPE
from app.policy.policy_gate import (
    evaluate_policy,
    evaluate_task_creation_policy,
    evaluate_workflow_contract,
    evaluate_workflow_policy,
)


def test_policy_gate_allows_only_supported_action_in_executable_state() -> None:
    result = evaluate_policy(
        {
            "action_type": "WRITE_FILE",
            "payload": {
                "task_id": "DF-POLICY-ENFORCEMENT-V1",
                "path": r"runtime\out\a.log",
                "content": "ok",
            },
        },
        {"task_id": "DF-POLICY-ENFORCEMENT-V1", "status": "running"},
    )

    assert result.execution_allowed is True
    assert result.reason == ""


def test_policy_gate_allows_browser_tool_in_executable_state() -> None:
    result = evaluate_policy(
        {
            "action_type": "BROWSER_TOOL",
            "payload": {
                "task_id": "DF-POLICY-BROWSER-V1",
                "steps": [
                    {"operation": "open_url", "url": "https://example.com/contact"},
                ],
            },
        },
        {"task_id": "DF-POLICY-BROWSER-V1", "status": "running"},
    )

    assert result.execution_allowed is True
    assert result.reason == ""


def test_policy_gate_blocks_malformed_descriptor_payload_and_state() -> None:
    malformed_payload = evaluate_policy(
        {"action_type": "WRITE_FILE", "payload": "bad"},
        {"task_id": "DF-POLICY-ENFORCEMENT-V2", "status": "running"},
    )
    invalid_state = evaluate_policy(
        {
            "action_type": "READ_FILE",
            "payload": {"task_id": "DF-POLICY-ENFORCEMENT-V2"},
        },
        {"task_id": "DF-POLICY-ENFORCEMENT-V2", "status": "completed"},
    )

    assert malformed_payload.execution_allowed is False
    assert malformed_payload.reason == "descriptor.payload must be a dict"
    assert invalid_state.execution_allowed is False
    assert invalid_state.reason == "task_state does not allow execution: completed"


def test_workflow_policy_blocks_malformed_payload() -> None:
    result = evaluate_workflow_policy(
        {
            "workflow_type": WORKFLOW_TYPE,
            "lead_id": "",
            "lead_data": {"lead_exists": True},
        },
        {"task_id": "DF-POLICY-WORKFLOW-V1", "status": "pending"},
    )

    assert result.execution_allowed is False
    assert result.reason == "lead_id must not be empty"


def test_workflow_contract_blocks_invalid_decision_and_status() -> None:
    invalid_decision = evaluate_workflow_contract(
        {
            "decision": "create_estimate",
            "confidence": "high",
            "next_step": "unsupported_action",
        },
        {"task_id": "DF-POLICY-WORKFLOW-V2", "status": "running"},
    )
    invalid_status = evaluate_workflow_contract(
        {
            "decision": "create_estimate",
            "confidence": "high",
            "next_step": "create_estimate_task",
        },
        {"task_id": "DF-POLICY-WORKFLOW-V3", "status": "created"},
    )

    assert invalid_decision.execution_allowed is False
    assert invalid_decision.reason == "invalid next_step"
    assert invalid_status.execution_allowed is False
    assert invalid_status.reason == "task_state does not allow execution: created"


def test_workflow_contract_and_policy_block_malformed_payloads() -> None:
    malformed_payload = evaluate_workflow_policy(
        {
            "workflow_type": WORKFLOW_TYPE,
            "lead_id": "lead-001",
            "lead_data": {"lead_exists": True},
            "unexpected_field": "blocked",
        },
        {"task_id": "DF-POLICY-WORKFLOW-V5", "status": "pending"},
    )

    assert malformed_payload.execution_allowed is False
    assert (
        malformed_payload.reason
        == "workflow payload contains unsupported fields: unexpected_field"
    )


def test_task_creation_policy_allows_valid_office_branching() -> None:
    lead_task = {
        "task_contract_version": 1,
        "task_id": "DF-POLICY-LINEAGE-LEAD-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "generic_task",
        "task_type": "project",
        "payload": {"summary": "project"},
        "status": "created",
        "notes": [],
        "history": [],
    }
    procurement_task = {
        "task_contract_version": 1,
        "task_id": "DF-POLICY-LINEAGE-PROCUREMENT-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "generic_task",
        "task_type": "procurement",
        "parent_task_id": "DF-POLICY-LINEAGE-LEAD-V1",
        "payload": {
            "summary": "procurement",
            "parent_task_id": "DF-POLICY-LINEAGE-LEAD-V1",
        },
        "status": "created",
        "notes": [],
        "history": [],
    }
    payment_task = {
        "task_contract_version": 1,
        "task_id": "DF-POLICY-LINEAGE-PAYMENT-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "generic_task",
        "task_type": "payment",
        "parent_task_id": "DF-POLICY-LINEAGE-LEAD-V1",
        "payload": {
            "summary": "payment",
            "parent_task_id": "DF-POLICY-LINEAGE-LEAD-V1",
        },
        "status": "created",
        "notes": [],
        "history": [],
    }

    procurement_result = evaluate_task_creation_policy(
        procurement_task,
        parent_task=lead_task,
        existing_tasks=[lead_task],
    )
    payment_result = evaluate_task_creation_policy(
        payment_task,
        parent_task=lead_task,
        existing_tasks=[lead_task, procurement_task],
    )

    assert procurement_result.execution_allowed is True
    assert payment_result.execution_allowed is True


def test_task_creation_policy_blocks_invalid_transition_and_duplicate() -> None:
    estimate_task = {
        "task_contract_version": 1,
        "task_id": "DF-POLICY-LINEAGE-ESTIMATE-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "generic_task",
        "task_type": "estimate",
        "payload": {"summary": "estimate"},
        "status": "created",
        "notes": [],
        "history": [],
    }
    invalid_payment = {
        "task_contract_version": 1,
        "task_id": "DF-POLICY-LINEAGE-INVALID-PAYMENT-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "generic_task",
        "task_type": "payment",
        "parent_task_id": "DF-POLICY-LINEAGE-ESTIMATE-V1",
        "payload": {
            "summary": "payment",
            "parent_task_id": "DF-POLICY-LINEAGE-ESTIMATE-V1",
        },
        "status": "created",
        "notes": [],
        "history": [],
    }
    existing_follow_up = {
        "task_contract_version": 1,
        "task_id": "DF-POLICY-LINEAGE-FOLLOWUP-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "generic_task",
        "task_type": "follow_up",
        "parent_task_id": "DF-POLICY-LINEAGE-ESTIMATE-V1",
        "payload": {
            "summary": "follow_up",
            "parent_task_id": "DF-POLICY-LINEAGE-ESTIMATE-V1",
        },
        "status": "created",
        "notes": [],
        "history": [],
    }
    duplicate_follow_up = dict(existing_follow_up)
    duplicate_follow_up["task_id"] = "DF-POLICY-LINEAGE-FOLLOWUP-V2"

    invalid_transition_result = evaluate_task_creation_policy(
        invalid_payment,
        parent_task=estimate_task,
        existing_tasks=[estimate_task],
    )
    duplicate_result = evaluate_task_creation_policy(
        duplicate_follow_up,
        parent_task=estimate_task,
        existing_tasks=[estimate_task, existing_follow_up],
    )

    assert invalid_transition_result.execution_allowed is False
    assert (
        invalid_transition_result.reason
        == "invalid office lineage transition: estimate -> payment"
    )
    assert duplicate_result.execution_allowed is False
    assert (
        duplicate_result.reason
        == "duplicate office child task type not allowed: estimate -> follow_up"
    )


def test_task_creation_policy_blocks_resource_conflict_with_active_decision() -> None:
    active_task = {
        "task_contract_version": 1,
        "task_id": "DF-DECISION-RESOURCE-ACTIVE-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "business_signal_response",
        "task_type": "lead",
        "payload": {
            "summary": "active decision",
            "decision": {
                "decision_id": "dec-1",
                "decision_type": "resource_lock",
                "priority": "high",
            },
            "recommended_action": "request_more_reviews",
            "resource_id": "crew-west",
            "priority": "high",
            "domain": "operations",
        },
        "status": "created",
        "notes": [],
        "history": [],
    }
    new_task = {
        "task_contract_version": 1,
        "task_id": "DF-DECISION-RESOURCE-NEW-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "business_signal_response",
        "task_type": "lead",
        "payload": {
            "summary": "new decision",
            "decision": {
                "decision_id": "dec-2",
                "decision_type": "resource_lock",
                "priority": "high",
            },
            "recommended_action": "request_more_reviews",
            "resource_id": "crew-west",
            "priority": "high",
            "domain": "operations",
        },
        "status": "created",
        "notes": [],
        "history": [],
    }

    result = evaluate_task_creation_policy(
        new_task,
        existing_tasks=[active_task],
    )

    assert result.execution_allowed is False
    assert (
        result.reason
        == "decision conflict flagged: resource conflict with active decision DF-DECISION-RESOURCE-ACTIVE-V1"
    )


def test_task_creation_policy_blocks_priority_conflict_with_active_decision() -> None:
    active_task = {
        "task_contract_version": 1,
        "task_id": "DF-DECISION-PRIORITY-ACTIVE-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "business_signal_response",
        "task_type": "lead",
        "payload": {
            "summary": "active priority",
            "decision": {
                "decision_id": "dec-3",
                "decision_type": "queue_review",
                "priority": "urgent",
            },
            "recommended_action": "request_more_reviews",
            "priority": "urgent",
            "domain": "reputation",
        },
        "status": "validated",
        "notes": [],
        "history": [],
    }
    new_task = {
        "task_contract_version": 1,
        "task_id": "DF-DECISION-PRIORITY-NEW-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "business_signal_response",
        "task_type": "lead",
        "payload": {
            "summary": "new priority",
            "decision": {
                "decision_id": "dec-4",
                "decision_type": "queue_review",
                "priority": "low",
            },
            "recommended_action": "request_more_reviews",
            "priority": "low",
            "domain": "reputation",
        },
        "status": "created",
        "notes": [],
        "history": [],
    }

    result = evaluate_task_creation_policy(
        new_task,
        existing_tasks=[active_task],
    )

    assert result.execution_allowed is False
    assert (
        result.reason
        == "decision conflict flagged: priority conflict with active decision DF-DECISION-PRIORITY-ACTIVE-V1"
    )


def test_task_creation_policy_blocks_contradictory_action_with_active_decision() -> (
    None
):
    active_task = {
        "task_contract_version": 1,
        "task_id": "DF-DECISION-ACTION-ACTIVE-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "business_signal_response",
        "task_type": "lead",
        "payload": {
            "summary": "active action",
            "decision": {
                "decision_id": "dec-5",
                "decision_type": "contact_policy",
                "priority": "high",
            },
            "recommended_action": "send_email",
            "priority": "high",
            "domain": "communications",
        },
        "status": "executing",
        "notes": [],
        "history": [],
    }
    new_task = {
        "task_contract_version": 1,
        "task_id": "DF-DECISION-ACTION-NEW-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "business_signal_response",
        "task_type": "lead",
        "payload": {
            "summary": "new action",
            "decision": {
                "decision_id": "dec-6",
                "decision_type": "contact_policy",
                "priority": "high",
            },
            "recommended_action": "do_not_send_email",
            "priority": "high",
            "domain": "communications",
        },
        "status": "created",
        "notes": [],
        "history": [],
    }

    result = evaluate_task_creation_policy(
        new_task,
        existing_tasks=[active_task],
    )

    assert result.execution_allowed is False
    assert (
        result.reason
        == "decision conflict flagged: contradictory actions with active decision DF-DECISION-ACTION-ACTIVE-V1"
    )


def test_task_creation_policy_allows_non_conflicting_decision() -> None:
    active_task = {
        "task_contract_version": 1,
        "task_id": "DF-DECISION-ALLOW-ACTIVE-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "business_signal_response",
        "task_type": "lead",
        "payload": {
            "summary": "active decision",
            "decision": {
                "decision_id": "dec-7",
                "decision_type": "resource_lock",
                "priority": "high",
            },
            "recommended_action": "request_more_reviews",
            "resource_id": "crew-west",
            "priority": "high",
            "domain": "operations",
        },
        "status": "completed",
        "notes": [],
        "history": [],
    }
    new_task = {
        "task_contract_version": 1,
        "task_id": "DF-DECISION-ALLOW-NEW-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "business_signal_response",
        "task_type": "lead",
        "payload": {
            "summary": "new decision",
            "decision": {
                "decision_id": "dec-8",
                "decision_type": "resource_lock",
                "priority": "high",
            },
            "recommended_action": "request_more_reviews",
            "resource_id": "crew-east",
            "priority": "high",
            "domain": "operations",
        },
        "status": "created",
        "notes": [],
        "history": [],
    }

    result = evaluate_task_creation_policy(
        new_task,
        existing_tasks=[active_task],
    )

    assert result.execution_allowed is True
    assert result.reason == ""


def test_task_creation_policy_blocks_unavailable_resource() -> None:
    new_task = {
        "task_contract_version": 1,
        "task_id": "DF-RESOURCE-UNAVAILABLE-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "generic_task",
        "payload": {
            "summary": "resource check",
            "resource": {
                "id": "crew-west",
                "type": "crew",
                "availability": "unavailable",
                "current_load": 0,
                "max_load": 2,
            },
            "candidate_resources": [
                {
                    "id": "crew-west",
                    "type": "crew",
                    "availability": "unavailable",
                    "current_load": 0,
                    "max_load": 2,
                },
                {
                    "id": "crew-east",
                    "type": "crew",
                    "availability": "available",
                    "current_load": 0,
                    "max_load": 2,
                },
            ],
        },
        "status": "created",
        "notes": [],
        "history": [],
    }

    result = evaluate_task_creation_policy(new_task, existing_tasks=[])

    assert result.execution_allowed is False
    assert result.reason == "alternative_option"
    assert result.policy_trace["conflict_type"] == "resource_conflict"
    assert result.policy_trace["alternative_option"] == {
        "option_type": "next_available_resource",
        "resource": {"id": "crew-east", "type": "crew"},
    }


def test_task_creation_policy_blocks_overloaded_resource() -> None:
    new_task = {
        "task_contract_version": 1,
        "task_id": "DF-RESOURCE-OVERLOAD-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "generic_task",
        "payload": {
            "summary": "resource overload",
            "resource": {
                "id": "contractor-1",
                "type": "contractor",
                "availability": "available",
                "current_load": 2,
                "max_load": 2,
            },
            "alternative_contractors": [
                {
                    "id": "contractor-3",
                    "type": "contractor",
                    "availability": "available",
                    "current_load": 1,
                    "max_load": 1,
                },
                {
                    "id": "contractor-2",
                    "type": "contractor",
                    "availability": "available",
                    "current_load": 0,
                    "max_load": 2,
                },
            ],
        },
        "status": "created",
        "notes": [],
        "history": [],
    }

    result = evaluate_task_creation_policy(new_task, existing_tasks=[])

    assert result.execution_allowed is False
    assert result.reason == "alternative_option"
    assert result.policy_trace["alternative_option"] == {
        "option_type": "alternative_contractor",
        "resource": {"id": "contractor-2", "type": "contractor"},
    }


def test_task_creation_policy_blocks_resource_scheduling_conflict() -> None:
    active_task = {
        "task_contract_version": 1,
        "task_id": "DF-RESOURCE-SLOT-ACTIVE-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "generic_task",
        "payload": {
            "summary": "active slot",
            "schedule_slot": "2026-04-06T09:00:00Z",
            "resource": {
                "id": "crew-west",
                "type": "crew",
                "availability": "available",
                "current_load": 1,
                "max_load": 2,
            },
        },
        "status": "validated",
        "notes": [],
        "history": [],
    }
    new_task = {
        "task_contract_version": 1,
        "task_id": "DF-RESOURCE-SLOT-NEW-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "generic_task",
        "payload": {
            "summary": "new slot",
            "schedule_slot": "2026-04-06T09:00:00Z",
            "resource": {
                "id": "crew-west",
                "type": "crew",
                "availability": "available",
                "current_load": 0,
                "max_load": 2,
            },
            "candidate_time_slots": [
                "2026-04-06T09:00:00Z",
                "2026-04-06T11:00:00Z",
            ],
        },
        "status": "created",
        "notes": [],
        "history": [],
    }

    result = evaluate_task_creation_policy(new_task, existing_tasks=[active_task])

    assert result.execution_allowed is False
    assert result.reason == "alternative_option"
    assert result.policy_trace["conflict_type"] == "schedule_conflict"
    assert result.policy_trace["alternative_option"] == {
        "option_type": "next_time_slot",
        "resource": {"id": "crew-west", "type": "crew"},
        "schedule_slot": "2026-04-06T11:00:00Z",
    }


def test_task_creation_policy_allows_available_resource_with_no_conflict() -> None:
    new_task = {
        "task_contract_version": 1,
        "task_id": "DF-RESOURCE-ALLOW-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "generic_task",
        "payload": {
            "summary": "resource available",
            "schedule_slot": "2026-04-06T10:00:00Z",
            "resource": {
                "id": "crew-east",
                "type": "crew",
                "availability": "available",
                "current_load": 1,
                "max_load": 2,
            },
        },
        "status": "created",
        "notes": [],
        "history": [],
    }

    result = evaluate_task_creation_policy(new_task, existing_tasks=[])

    assert result.execution_allowed is True
    assert result.reason == ""


def test_task_creation_policy_blocks_when_no_alternative_exists() -> None:
    active_task = {
        "task_contract_version": 1,
        "task_id": "DF-RESOURCE-NO-ALT-ACTIVE-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "generic_task",
        "payload": {
            "summary": "active slot",
            "schedule_slot": "2026-04-06T09:00:00Z",
            "resource": {
                "id": "crew-west",
                "type": "crew",
                "availability": "available",
                "current_load": 1,
                "max_load": 2,
            },
        },
        "status": "validated",
        "notes": [],
        "history": [],
    }
    new_task = {
        "task_contract_version": 1,
        "task_id": "DF-RESOURCE-NO-ALT-NEW-V1",
        "created_at": "2026-04-04T00:00:00Z",
        "intent": "generic_task",
        "payload": {
            "summary": "no fallback",
            "schedule_slot": "2026-04-06T09:00:00Z",
            "resource": {
                "id": "crew-west",
                "type": "crew",
                "availability": "available",
                "current_load": 0,
                "max_load": 2,
            },
            "candidate_time_slots": ["2026-04-06T09:00:00Z"],
        },
        "status": "created",
        "notes": [],
        "history": [],
    }

    result = evaluate_task_creation_policy(new_task, existing_tasks=[active_task])

    assert result.execution_allowed is False
    assert result.reason == "schedule_conflict"
    assert "alternative_option" not in result.policy_trace


def test_system_improvement_task_creation_allows_core_zone_but_flags_approval_requirement() -> (
    None
):
    task = {
        "task_contract_version": 1,
        "task_id": "DF-CORE-LOCK-BLOCK-V1",
        "created_at": "2026-04-06T00:00:00Z",
        "intent": "system_improvement_task",
        "payload": {
            "summary": "Update execution runner logic",
            "affected_files": ["app/orchestrator/execution_runner.py"],
            "core_impact": True,
            "requires_approval": False,
            "route_target": "execution",
            "priority": "NORMAL",
        },
        "status": "created",
        "approval_status": "approved",
        "notes": [],
        "history": [],
    }

    result = evaluate_task_creation_policy(task, existing_tasks=[])

    assert result.execution_allowed is True
    assert result.reason == ""
    assert result.policy_trace["core_impact"] is True
    assert result.policy_trace["core_zone_files"] == [
        "app/orchestrator/execution_runner.py"
    ]
    assert result.policy_trace["requires_high_approval"] is False
    assert result.policy_trace["execution_blocked_until_approval"] is True


def test_system_improvement_task_allows_core_zone_with_high_approval_queue() -> None:
    task = {
        "task_contract_version": 1,
        "task_id": "DF-CORE-LOCK-ALLOW-V1",
        "created_at": "2026-04-06T00:00:00Z",
        "intent": "system_improvement_task",
        "payload": {
            "summary": "Update execution runner logic",
            "affected_files": ["app/orchestrator/execution_runner.py"],
            "core_impact": True,
            "requires_approval": True,
            "route_target": "approval_queue",
            "priority": "HIGH",
        },
        "status": "awaiting_approval",
        "approval_status": "pending",
        "notes": [],
        "history": [],
    }

    result = evaluate_task_creation_policy(task, existing_tasks=[])

    assert result.execution_allowed is True
    assert result.reason == ""
    assert result.policy_trace["core_impact"] is True
    assert result.policy_trace["requires_high_approval"] is True


def test_non_core_system_improvement_tasks_do_not_conflict_on_priority_only() -> None:
    existing = {
        "task_contract_version": 1,
        "task_id": "DF-IMPROVEMENT-OPEN-V1",
        "created_at": "2026-04-06T00:00:00Z",
        "intent": "system_improvement_task",
        "payload": {
            "summary": "Improve non-core logging",
            "affected_files": ["app/system/analyzer.py"],
            "core_impact": False,
            "requires_approval": False,
            "route_target": "execution",
            "priority": "NORMAL",
        },
        "status": "validated",
        "approval_status": "approved",
        "notes": [],
        "history": [],
    }
    candidate = {
        "task_contract_version": 1,
        "task_id": "DF-IMPROVEMENT-OPEN-V2",
        "created_at": "2026-04-06T00:00:01Z",
        "intent": "system_improvement_task",
        "payload": {
            "summary": "Improve non-core retries",
            "affected_files": ["app/system/gap_tasks.py"],
            "core_impact": False,
            "requires_approval": False,
            "route_target": "execution",
            "priority": "HIGH",
        },
        "status": "created",
        "approval_status": "approved",
        "notes": [],
        "history": [],
    }

    result = evaluate_task_creation_policy(candidate, existing_tasks=[existing])

    assert result.execution_allowed is True
    assert result.reason == ""
    assert result.policy_trace["core_impact"] is False
