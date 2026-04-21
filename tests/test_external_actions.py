from __future__ import annotations

from pathlib import Path

import pytest

from app.execution.execution_boundary import (
    ExecutionBoundaryViolationError,
    execution_boundary,
)
from app.execution.external_actions import (
    API_REQUEST,
    MAKE_CALL,
    SEND_SMS,
    ExternalActionValidationError,
    build_external_action_request,
    execute_external,
)
from app.execution.external_modules import (
    ExternalModuleRegistry,
    ExternalModuleRequest,
    ExternalModuleValidationError,
)
from app.policy.policy_gate import evaluate_external_action_policy


class EstimateAdapter:
    module_type = "estimate_service"

    def validate_request_payload(
        self, operation: str, payload: dict[str, object]
    ) -> None:
        if operation != "generate_estimate":
            raise ExternalModuleValidationError("unsupported operation")
        if "scope" not in payload:
            raise ExternalModuleValidationError(
                "payload missing required fields: scope"
            )

    def validate_result_payload(
        self, operation: str, payload: dict[str, object]
    ) -> None:
        if operation != "generate_estimate":
            raise ExternalModuleValidationError("unsupported operation")
        if "estimate_id" not in payload:
            raise ExternalModuleValidationError(
                "result_payload missing required fields: estimate_id"
            )

    def execute(self, request: ExternalModuleRequest) -> dict[str, object]:
        return {
            "request_id": request.request_id,
            "status": "success",
            "module_type": request.module_type,
            "operation": request.operation,
            "result_payload": {"estimate_id": "est-001"},
            "duration_ms": 15,
        }


def test_external_action_request_rejects_unknown_destination() -> None:
    with pytest.raises(
        ExternalActionValidationError,
        match="destination `public_sms_gateway` is not allowed for SEND_SMS",
    ):
        build_external_action_request(
            SEND_SMS,
            {
                "destination": "public_sms_gateway",
                "contact_id": "contact-001",
                "text": "hello",
            },
        )


def test_external_action_policy_blocks_unknown_destination() -> None:
    result = evaluate_external_action_policy(
        MAKE_CALL,
        {
            "destination": "public_phone_gateway",
            "contact_id": "contact-001",
            "phone_number": "555-0100",
            "script": "hello",
        },
        {"task_id": "DF-TASK-001", "status": "running"},
    )

    assert result.execution_allowed is False
    assert "public_phone_gateway" in result.reason


def test_execute_external_send_sms_returns_typed_action_result(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr("app.execution.paths.OUTPUT_DIR", tmp_path / "out")
    monkeypatch.setattr("integrations.sms_gateway.OUTPUT_DIR", tmp_path / "out")
    with execution_boundary(
        {"task_id": "DF-TASK-001", "intent": "outbound_message"},
        policy_validated=True,
    ):
        result = execute_external(
            SEND_SMS,
            {
                "destination": "sms_gateway",
                "contact_id": "contact-001",
                "text": "hello",
            },
        )

    assert result["status"] == "completed"
    assert result["action_type"] == SEND_SMS
    assert result["task_id"] == "DF-TASK-001"
    assert "provider_result" in result["result_payload"]


def test_execute_external_api_request_is_centralized_through_module_registry() -> None:
    registry = ExternalModuleRegistry()
    registry.register(EstimateAdapter())

    with execution_boundary(
        {"task_id": "DF-TASK-002", "intent": "estimate"},
        policy_validated=True,
    ):
        result = execute_external(
            API_REQUEST,
            {
                "destination": "estimate_service",
                "request_id": "EXT-ACT-001",
                "operation": "generate_estimate",
                "request_payload": {"scope": "detached_adu"},
                "timeout_ms": 15000,
            },
            registry=registry,
        )

    assert result["status"] == "completed"
    assert result["action_type"] == API_REQUEST
    assert (
        result["result_payload"]["external_module_result"]["module_type"]
        == "estimate_service"
    )


def test_execute_external_requires_execution_boundary() -> None:
    with pytest.raises(
        ExecutionBoundaryViolationError,
        match="direct_external_action_call_blocked",
    ):
        execute_external(
            SEND_SMS,
            {
                "destination": "sms_gateway",
                "contact_id": "contact-001",
                "text": "hello",
            },
        )
