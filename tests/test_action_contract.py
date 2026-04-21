from __future__ import annotations

import json

import pytest

from app.execution.action_contract import (
    ActionContractViolation,
    build_action_contract,
    build_action_result_contract,
    precheck_action_contract,
    serialize_action_contract,
    serialize_action_result_contract,
    validate_action_contract,
    validate_action_result_contract,
)


def _valid_action_payload() -> dict[str, object]:
    return {
        "action_id": "act-001",
        "action_type": "SEND_EMAIL",
        "target_type": "gateway",
        "target_ref": "gmail_gateway",
        "parameters": {
            "template_id": "lead_followup_v1",
            "lead_id": "lead-001",
            "metadata": {
                "priority": "high",
            },
        },
        "execution_mode": "dry_run",
        "confirmation_policy": "required",
        "idempotency_key": "lead-001:workflow:send_email:abcd1234",
        "requested_by": "df_core",
        "timestamp": "2026-04-14T12:00:00Z",
        "schema_version": "v1",
    }


def _valid_action_result_payload() -> dict[str, object]:
    return {
        "action_id": "act-001",
        "status": "blocked",
        "result_type": "precheck",
        "payload": {
            "reason": "confirmation required",
            "confirmation_policy": "required",
        },
        "error_code": "confirmation_required",
        "error_message": "manual confirmation required",
        "timestamp": "2026-04-14T12:00:01Z",
        "schema_version": "v1",
    }


def _valid_printer_action_payload() -> dict[str, object]:
    return {
        "action_id": "act-print-001",
        "action_type": "print_document",
        "target_type": "adapter",
        "target_ref": "printer",
        "parameters": {
            "operation": "print_document",
            "document_title": "Owner Review",
            "document_text": "Bounded printable document text.",
            "copies": 1,
            "printer_name": "Zephyrus_Main",
        },
        "execution_mode": "live",
        "confirmation_policy": "required",
        "idempotency_key": "owner-print:001",
        "requested_by": "ownerbox_interaction_v1",
        "timestamp": "2026-04-14T12:00:00Z",
        "schema_version": "v1",
    }


def test_valid_action_contract_passes() -> None:
    validated = validate_action_contract(_valid_action_payload())

    assert validated == {
        "action_id": "act-001",
        "action_type": "SEND_EMAIL",
        "target_type": "gateway",
        "target_ref": "gmail_gateway",
        "parameters": {
            "lead_id": "lead-001",
            "metadata": {
                "priority": "high",
            },
            "template_id": "lead_followup_v1",
        },
        "execution_mode": "dry_run",
        "confirmation_policy": "required",
        "idempotency_key": "lead-001:workflow:send_email:abcd1234",
        "requested_by": "df_core",
        "timestamp": "2026-04-14T12:00:00Z",
        "schema_version": "v1",
    }


def test_print_document_action_contract_passes() -> None:
    validated = validate_action_contract(_valid_printer_action_payload())

    assert validated == {
        "action_id": "act-print-001",
        "action_type": "PRINT_DOCUMENT",
        "target_type": "adapter",
        "target_ref": "printer",
        "parameters": {
            "copies": 1,
            "document_text": "Bounded printable document text.",
            "document_title": "Owner Review",
            "operation": "print_document",
            "printer_name": "Zephyrus_Main",
        },
        "execution_mode": "live",
        "confirmation_policy": "required",
        "idempotency_key": "owner-print:001",
        "requested_by": "ownerbox_interaction_v1",
        "timestamp": "2026-04-14T12:00:00Z",
        "schema_version": "v1",
    }


def test_malformed_action_contract_rejected() -> None:
    with pytest.raises(
        ActionContractViolation, match="action contract missing required fields"
    ):
        validate_action_contract(
            {
                "action_id": "act-001",
                "action_type": "SEND_EMAIL",
            }
        )


def test_unsupported_schema_version_rejected() -> None:
    payload = _valid_action_payload()
    payload["schema_version"] = "v2"

    with pytest.raises(ActionContractViolation, match="unsupported schema_version: v2"):
        validate_action_contract(payload)


def test_unsupported_execution_mode_rejected() -> None:
    payload = _valid_action_payload()
    payload["execution_mode"] = "auto"

    with pytest.raises(
        ActionContractViolation, match="unsupported execution_mode: auto"
    ):
        validate_action_contract(payload)


def test_missing_idempotency_key_rejected() -> None:
    payload = _valid_action_payload()
    payload["idempotency_key"] = ""

    with pytest.raises(
        ActionContractViolation, match="idempotency_key must not be empty"
    ):
        validate_action_contract(payload)


def test_precheck_blocks_duplicate_idempotency_key() -> None:
    result = precheck_action_contract(
        _valid_action_payload(),
        confirmation_received=True,
        existing_idempotency_keys={"lead-001:workflow:send_email:abcd1234"},
    )

    assert result == {
        "allowed": False,
        "status": "blocked",
        "reason": "duplicate idempotency_key: lead-001:workflow:send_email:abcd1234",
        "action": validate_action_contract(_valid_action_payload()),
    }


def test_precheck_blocks_missing_confirmation() -> None:
    result = precheck_action_contract(
        _valid_action_payload(), confirmation_received=False
    )

    assert result["allowed"] is False
    assert result["status"] == "blocked"
    assert result["reason"] == "confirmation required: SEND_EMAIL"


def test_valid_action_result_contract_passes() -> None:
    validated = validate_action_result_contract(_valid_action_result_payload())

    assert validated == {
        "action_id": "act-001",
        "status": "blocked",
        "result_type": "precheck",
        "payload": {
            "confirmation_policy": "required",
            "reason": "confirmation required",
        },
        "error_code": "confirmation_required",
        "error_message": "manual confirmation required",
        "timestamp": "2026-04-14T12:00:01Z",
        "schema_version": "v1",
    }


def test_malformed_action_result_rejected() -> None:
    payload = _valid_action_result_payload()
    payload["payload"] = "raw tool output"

    with pytest.raises(ActionContractViolation, match="payload must be a dict"):
        validate_action_result_contract(payload)


def test_trace_serialization_is_deterministic() -> None:
    built_action = build_action_contract(**_valid_action_payload())
    built_result = build_action_result_contract(**_valid_action_result_payload())

    action_variant = {
        "schema_version": "v1",
        "timestamp": "2026-04-14T12:00:00Z",
        "requested_by": "df_core",
        "idempotency_key": "lead-001:workflow:send_email:abcd1234",
        "confirmation_policy": "required",
        "execution_mode": "dry_run",
        "parameters": {
            "metadata": {"priority": "high"},
            "template_id": "lead_followup_v1",
            "lead_id": "lead-001",
        },
        "target_ref": "gmail_gateway",
        "target_type": "gateway",
        "action_type": "SEND_EMAIL",
        "action_id": "act-001",
    }
    result_variant = {
        "schema_version": "v1",
        "timestamp": "2026-04-14T12:00:01Z",
        "error_message": "manual confirmation required",
        "error_code": "confirmation_required",
        "payload": {
            "reason": "confirmation required",
            "confirmation_policy": "required",
        },
        "result_type": "precheck",
        "status": "blocked",
        "action_id": "act-001",
    }

    assert serialize_action_contract(built_action) == serialize_action_contract(
        action_variant
    )
    assert serialize_action_result_contract(
        built_result
    ) == serialize_action_result_contract(result_variant)
    assert json.loads(
        serialize_action_contract(built_action)
    ) == validate_action_contract(_valid_action_payload())
    assert json.loads(
        serialize_action_result_contract(built_result)
    ) == validate_action_result_contract(_valid_action_result_payload())
