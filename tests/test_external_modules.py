from __future__ import annotations

import pytest

from app.execution.execution_boundary import (
    ExecutionBoundaryViolationError,
    execution_boundary,
)
from app.execution.external_modules import (
    ExternalModuleRequest,
    ExternalModuleResult,
    ExternalModuleValidationError,
    ExternalModuleRegistry,
    build_external_module_request,
    build_external_module_result,
    execute_external_module,
    validate_external_module_result_for_df,
)


class EstimateModuleAdapter:
    module_type = "estimate_service"

    def __init__(
        self, *, raw_result: object | None = None, error: Exception | None = None
    ) -> None:
        self.raw_result = raw_result
        self.error = error
        self.last_request: ExternalModuleRequest | None = None

    def validate_request_payload(
        self, operation: str, payload: dict[str, object]
    ) -> None:
        if operation != "generate_estimate":
            raise ExternalModuleValidationError(
                "unsupported operation for estimate_service"
            )
        required_fields = {"estimate_scope", "address"}
        missing_fields = sorted(required_fields - set(payload))
        if missing_fields:
            raise ExternalModuleValidationError(
                "payload missing required fields: " + ", ".join(missing_fields)
            )

    def validate_result_payload(
        self, operation: str, payload: dict[str, object]
    ) -> None:
        if operation != "generate_estimate":
            raise ExternalModuleValidationError(
                "unsupported operation for estimate_service"
            )
        required_fields = {"estimate_id", "price_band"}
        missing_fields = sorted(required_fields - set(payload))
        if missing_fields:
            raise ExternalModuleValidationError(
                "result_payload missing required fields: " + ", ".join(missing_fields)
            )

    def execute(self, request: ExternalModuleRequest) -> object:
        self.last_request = request
        if self.error is not None:
            raise self.error
        return self.raw_result or {
            "request_id": request.request_id,
            "status": "success",
            "module_type": request.module_type,
            "operation": request.operation,
            "result_payload": {
                "estimate_id": "est-001",
                "price_band": "250k-300k",
            },
            "duration_ms": 42,
        }


def _build_valid_request() -> ExternalModuleRequest:
    return build_external_module_request(
        {
            "request_id": "EXT-REQ-001",
            "task_id": "DF-TASK-001",
            "task_type": "estimate",
            "module_type": "estimate_service",
            "operation": "generate_estimate",
            "payload": {
                "estimate_scope": "detached_adu",
                "address": "123 Main St",
            },
            "correlation_id": "corr-001",
            "timeout_ms": 15000,
            "metadata": {
                "schema_version": "v1",
                "request_source": "df_core",
                "capability": "estimate_generation",
                "priority": "normal",
                "tags": ["estimate", "external"],
            },
        }
    )


def test_valid_external_module_request_passes() -> None:
    request = _build_valid_request()

    assert request.request_id == "EXT-REQ-001"
    assert request.task_id == "DF-TASK-001"
    assert request.payload["estimate_scope"] == "detached_adu"
    assert request.metadata["capability"] == "estimate_generation"


def test_malformed_external_module_request_fails() -> None:
    with pytest.raises(
        ExternalModuleValidationError,
        match="external_module_request contains unsupported fields: freeform_prompt",
    ):
        build_external_module_request(
            {
                "request_id": "EXT-REQ-001",
                "task_id": "DF-TASK-001",
                "task_type": "estimate",
                "module_type": "estimate_service",
                "operation": "generate_estimate",
                "payload": {
                    "estimate_scope": "detached_adu",
                    "address": "123 Main St",
                },
                "correlation_id": "corr-001",
                "timeout_ms": 15000,
                "metadata": {},
                "freeform_prompt": "do whatever seems best",
            }
        )


def test_valid_typed_result_passes() -> None:
    result = build_external_module_result(
        {
            "request_id": "EXT-REQ-001",
            "status": "success",
            "module_type": "estimate_service",
            "operation": "generate_estimate",
            "result_payload": {
                "estimate_id": "est-001",
                "price_band": "250k-300k",
            },
            "duration_ms": 27,
        }
    )

    assert isinstance(result, ExternalModuleResult)
    assert result.status == "success"
    assert result.result_payload["estimate_id"] == "est-001"


def test_malformed_result_is_rejected_as_signal() -> None:
    registry = ExternalModuleRegistry()
    registry.register(EstimateModuleAdapter())
    request = _build_valid_request()

    result = validate_external_module_result_for_df(
        {
            "request_id": request.request_id,
            "status": "success",
            "module_type": request.module_type,
            "operation": request.operation,
            "result_payload": {
                "estimate_id": "est-001",
                "price_band": "250k-300k",
            },
            "duration_ms": 10,
            "next_step": "skip_df_and_create_permit",
        },
        request=request,
        registry=registry,
    )

    assert result.status == "invalid_result"
    assert result.error_code == "external_module_invalid_result"
    assert "unsupported fields: next_step" in result.error_message


def test_timeout_and_unavailable_statuses_are_handled_as_signals() -> None:
    timeout_registry = ExternalModuleRegistry()
    timeout_registry.register(
        EstimateModuleAdapter(error=TimeoutError("estimate call timed out"))
    )
    request = _build_valid_request()

    with execution_boundary(
        {"task_id": request.task_id, "intent": request.task_type},
        policy_validated=True,
    ):
        timeout_result = execute_external_module(request, registry=timeout_registry)
    with execution_boundary(
        {"task_id": request.task_id, "intent": request.task_type},
        policy_validated=True,
    ):
        unavailable_result = execute_external_module(
            request,
            registry=ExternalModuleRegistry(),
        )

    assert timeout_result.status == "timeout"
    assert timeout_result.error_code == "external_module_timeout"
    assert unavailable_result.status == "unavailable"
    assert unavailable_result.error_code == "external_module_unavailable"


def test_adapter_cannot_set_next_workflow_step() -> None:
    registry = ExternalModuleRegistry()
    registry.register(
        EstimateModuleAdapter(
            raw_result={
                "request_id": "EXT-REQ-001",
                "status": "success",
                "module_type": "estimate_service",
                "operation": "generate_estimate",
                "result_payload": {
                    "estimate_id": "est-001",
                    "price_band": "250k-300k",
                },
                "duration_ms": 5,
                "next_step": "permit_submission",
            }
        )
    )

    request = _build_valid_request()
    with execution_boundary(
        {"task_id": request.task_id, "intent": request.task_type},
        policy_validated=True,
    ):
        result = execute_external_module(request, registry=registry)

    assert result.status == "invalid_result"
    assert "next_step" in result.error_message


def test_adapter_cannot_mutate_task_state_directly() -> None:
    task_state = {
        "task_id": "DF-TASK-001",
        "status": "pending",
        "next_step": "await_estimate",
    }
    adapter = EstimateModuleAdapter()
    registry = ExternalModuleRegistry()
    registry.register(adapter)
    request = _build_valid_request()

    with execution_boundary(
        {"task_id": request.task_id, "intent": request.task_type},
        policy_validated=True,
    ):
        result = execute_external_module(request, registry=registry)

    assert result.status == "success"
    assert adapter.last_request is not None
    assert "next_step" not in adapter.last_request.to_dict()
    assert task_state == {
        "task_id": "DF-TASK-001",
        "status": "pending",
        "next_step": "await_estimate",
    }
    with pytest.raises(TypeError):
        adapter.last_request.payload["estimate_scope"] = "changed"  # type: ignore[index]


def test_df_remains_owner_of_retries_cancel_and_next_step_decisions() -> None:
    registry = ExternalModuleRegistry()
    registry.register(
        EstimateModuleAdapter(
            raw_result={
                "request_id": "EXT-REQ-001",
                "status": "failed",
                "module_type": "estimate_service",
                "operation": "generate_estimate",
                "result_payload": {},
                "error_code": "vendor_failed",
                "error_message": "temporary upstream issue",
                "duration_ms": 12,
                "retry_decision": "retry_now",
                "cancel_task": True,
            }
        )
    )

    request = _build_valid_request()
    with execution_boundary(
        {"task_id": request.task_id, "intent": request.task_type},
        policy_validated=True,
    ):
        result = execute_external_module(request, registry=registry)

    assert result.status == "invalid_result"
    assert "retry_decision" in result.error_message
    assert "cancel_task" in result.error_message


def test_external_modules_cannot_be_called_outside_execution_boundary() -> None:
    registry = ExternalModuleRegistry()
    registry.register(EstimateModuleAdapter())

    with pytest.raises(
        ExecutionBoundaryViolationError, match="direct_external_module_call_blocked"
    ):
        execute_external_module(_build_valid_request(), registry=registry)


def test_external_modules_execute_inside_valid_boundary() -> None:
    registry = ExternalModuleRegistry()
    registry.register(EstimateModuleAdapter())

    with execution_boundary(
        {"task_id": "DF-TASK-001", "intent": "estimate"},
        policy_validated=True,
    ):
        result = execute_external_module(_build_valid_request(), registry=registry)

    assert result.status == "success"
