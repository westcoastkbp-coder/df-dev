from __future__ import annotations

import pytest

from app.execution.lead_estimate_decision import (
    build_action_payload,
    build_decision_payload,
)
from app.execution.lead_estimate_contract import build_execution_result_payload, build_input_payload, validate_input_payload
from app.execution.real_lead_contract import (
    build_followup_payload,
    build_lead_input_payload,
    validate_followup_payload,
)
from app.execution.real_lead_reporting import build_reporting_payload, report_workflow_execution


def test_valid_lead_input_payload_passes() -> None:
    payload = build_lead_input_payload(
        {
            "lead_id": "lead-001",
            "contact_info": {"phone": "555-0100"},
            "project_type": "ADU",
            "scope_summary": "Detached ADU",
        }
    )

    assert payload["lead_id"] == "lead-001"
    assert payload["project_type"] == "adu"
    assert payload["contact_info"] == {"phone": "555-0100", "email": None}


def test_missing_required_lead_input_field_fails() -> None:
    with pytest.raises(ValueError, match="lead_input_payload missing required fields: scope_summary"):
        from app.execution.real_lead_contract import validate_lead_input_payload

        validate_lead_input_payload(
            {
                "lead_id": "lead-001",
                "contact_info": {"phone": "555-0100"},
                "project_type": "adu",
                "urgency_level": None,
                "location": None,
                "notes": None,
                "qualification_flags": {},
                "lead_invalid": False,
                "unsupported_request": False,
                "lead_exists": True,
            }
        )


def test_extra_lead_input_field_fails() -> None:
    with pytest.raises(ValueError, match="lead_input_payload contains unsupported fields: extra_field"):
        from app.execution.real_lead_contract import validate_lead_input_payload

        validate_lead_input_payload(
            {
                "lead_id": "lead-001",
                "contact_info": {"phone": "555-0100"},
                "project_type": "adu",
                "scope_summary": "Detached ADU",
                "urgency_level": None,
                "location": None,
                "notes": None,
                "qualification_flags": {},
                "lead_invalid": False,
                "unsupported_request": False,
                "lead_exists": True,
                "extra_field": "nope",
            }
        )


def test_decision_payload_shape_enforced() -> None:
    with pytest.raises(ValueError, match="decision payload contains unsupported fields: extra_field"):
        build_decision_payload(
            {
                "decision": "create_estimate",
                "confidence": "high",
                "next_step": "create_estimate_task",
                "extra_field": "nope",
            }
        )


def test_action_payload_shape_enforced() -> None:
    with pytest.raises(ValueError, match="action payload contains unsupported fields: extra_field"):
        build_action_payload(
            {
                "binding_action": "create_estimate_task",
                "binding_status": "child_task_created",
                "child_task_created": True,
                "child_task_id": "child-001",
                "child_task_intent": "estimate_task",
                "parent_task_id": "DF-1",
                "source_lead_id": "lead-001",
                "action_source": "lead_estimate_decision",
                "reason_code": "project_defined",
                "extra_field": "nope",
            }
        )


def test_input_payload_trims_redundant_and_empty_fields() -> None:
    payload = build_input_payload(
        {
            "workflow_type": "lead_estimate_decision",
            "lead_id": " lead-001 ",
            "lead_data": {
                "project_type": " ADU ",
                "scope_summary": " Detached ADU ",
                "contact_info": {"phone": " 555-0100 ", "email": " "},
                "qualification_flags": {"invalid": False, "priority": True},
                "lead_invalid": False,
                "unsupported_request": False,
                "lead_exists": True,
            },
        }
    )

    assert payload == {
        "workflow_type": "lead_estimate_decision",
        "lead_id": "lead-001",
        "lead_data": {
            "project_type": "ADU",
            "scope_summary": "Detached ADU",
            "contact_info": {"phone": "555-0100"},
            "qualification_flags": {"priority": True},
        },
    }


def test_input_payload_rejects_nested_scalar_structure() -> None:
    valid, reason, _ = validate_input_payload(
        {
            "workflow_type": "lead_estimate_decision",
            "lead_id": "lead-001",
            "lead_data": {
                "project_type": {"kind": "adu"},
            },
        }
    )

    assert valid is False
    assert reason == "workflow payload field lead_data.project_type must not be nested"


def test_followup_payload_shape_enforced() -> None:
    with pytest.raises(ValueError, match="followup_payload contains unsupported fields: extra_field"):
        validate_followup_payload(
            {
                "workflow_type": "missing_input_followup",
                "parent_lead_id": "lead-001",
                "missing_fields": ["contact_info"],
                "required_action": "request_input_completion",
                "status": "pending",
                "extra_field": "nope",
            }
        )


def test_reporting_payload_shape_enforced() -> None:
    with pytest.raises(ValueError, match="reporting_payload contains unsupported fields: extra_field"):
        report_workflow_execution(
            {
                "task_id": "DF-1",
                "decision_payload": {},
                "action_payload": {},
                "extra_field": "nope",
            }
        )


def test_execution_result_builder_trims_empty_optional_fields() -> None:
    payload = build_execution_result_payload(
        {
            "result": "ok",
            "task_type": "lead_estimate_decision",
            "result_type": "lead_estimate_decision",
            "result_summary": "summary",
            "summary": "summary",
            "decision": {
                "decision": "create_estimate",
                "confidence": "high",
                "next_step": "create_estimate_task",
            },
            "binding": {
                "binding_action": "create_estimate_task",
                "binding_status": "child_task_created",
                "child_task_created": True,
                "child_task_id": "child-001",
                "child_task_intent": "",
                "archive_status": "",
                "parent_task_id": "DF-1",
                "source_lead_id": "lead-001",
                "action_source": "lead_estimate_decision",
                "reason_code": "project_defined",
            },
        }
    )

    assert payload["binding"] == {
        "binding_action": "create_estimate_task",
        "binding_status": "child_task_created",
        "child_task_created": True,
        "child_task_id": "child-001",
        "parent_task_id": "DF-1",
        "source_lead_id": "lead-001",
        "action_source": "lead_estimate_decision",
        "reason_code": "project_defined",
    }


def test_no_in_place_mutation_across_modules() -> None:
    raw_lead_input = {
        "lead_id": " lead-001 ",
        "contact_info": {"phone": " 555-0100 "},
        "project_type": " ADU ",
        "scope_summary": " Detached ADU ",
    }
    raw_followup_payload = {
        "workflow_type": "missing_input_followup",
        "parent_lead_id": "lead-001",
        "missing_fields": ["contact_info"],
        "required_action": "request_input_completion",
        "status": "pending",
    }
    raw_decision_payload = {
        "decision": "create_estimate",
        "confidence": "high",
        "next_step": "create_estimate_task",
    }
    raw_action_payload = {
        "binding_action": "create_estimate_task",
        "binding_status": "child_task_created",
        "child_task_created": True,
        "child_task_id": "child-001",
        "child_task_intent": "estimate_task",
        "parent_task_id": "DF-1",
        "source_lead_id": "lead-001",
        "action_source": "lead_estimate_decision",
        "reason_code": "project_defined",
    }

    lead_input_copy = dict(raw_lead_input)
    contact_info_copy = dict(raw_lead_input["contact_info"])
    followup_copy = dict(raw_followup_payload)
    decision_copy = dict(raw_decision_payload)
    action_copy = dict(raw_action_payload)

    build_lead_input_payload(raw_lead_input)
    build_followup_payload(
        parent_lead_id=raw_followup_payload["parent_lead_id"],
        missing_fields=raw_followup_payload["missing_fields"],
        required_action=raw_followup_payload["required_action"],
        status=raw_followup_payload["status"],
    )
    build_decision_payload(raw_decision_payload)
    build_action_payload(raw_action_payload)
    build_reporting_payload(
        task_id="DF-1",
        decision_payload=raw_decision_payload,
        action_payload=raw_action_payload,
    )

    assert raw_lead_input == lead_input_copy
    assert raw_lead_input["contact_info"] == contact_info_copy
    assert raw_followup_payload == followup_copy
    assert raw_decision_payload == decision_copy
    assert raw_action_payload == action_copy
