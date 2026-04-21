from __future__ import annotations

import copy

import pytest

from app.adapters.openai_adapter import (
    OpenAIAdapterConfig,
    OpenAIAdapterError,
    OpenAITextResponse,
    execute_openai_action,
)
from app.execution.action_contract import (
    ActionContractViolation,
    validate_action_contract,
    validate_action_result_contract,
)


def _valid_openai_action_contract(*, execution_mode: str = "live") -> dict[str, object]:
    return {
        "action_id": "act-openai-001",
        "action_type": "openai_request",
        "target_type": "adapter",
        "target_ref": "openai",
        "parameters": {
            "model": "gpt-5-mini",
            "prompt": "Summarize the change in one sentence.",
            "max_tokens": 64,
            "temperature": 0.2,
        },
        "execution_mode": execution_mode,
        "confirmation_policy": "not_required",
        "idempotency_key": "df:openai:act-openai-001",
        "requested_by": "df_core",
        "timestamp": "2026-04-14T12:00:00Z",
        "schema_version": "v1",
    }


class StubClient:
    def __init__(
        self,
        *,
        responses: list[OpenAITextResponse | Exception] | None = None,
    ) -> None:
        self.responses = list(responses or [OpenAITextResponse(text="bounded output")])
        self.calls: list[dict[str, object]] = []

    def generate_text(
        self,
        *,
        model: str,
        prompt: str,
        max_tokens: int,
        temperature: float,
        timeout_seconds: int,
    ) -> OpenAITextResponse:
        self.calls.append(
            {
                "model": model,
                "prompt": prompt,
                "max_tokens": max_tokens,
                "temperature": temperature,
                "timeout_seconds": timeout_seconds,
            }
        )
        current = self.responses.pop(0)
        if isinstance(current, Exception):
            raise current
        return current


def test_valid_openai_request_contract_passes() -> None:
    validated = validate_action_contract(_valid_openai_action_contract())

    assert validated["action_type"] == "OPENAI_REQUEST"
    assert validated["parameters"] == {
        "model": "gpt-5-mini",
        "prompt": "Summarize the change in one sentence.",
        "max_tokens": 64,
        "temperature": 0.2,
    }


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("extra", "nope", "parameters contains unsupported fields: extra"),
        ("max_tokens", 0, "parameters.max_tokens must be between 1 and 4096"),
        ("temperature", 1.5, "parameters.temperature must be between 0 and 1"),
    ],
)
def test_invalid_openai_request_parameters_are_rejected(
    field: str,
    value: object,
    message: str,
) -> None:
    payload = _valid_openai_action_contract()
    payload_parameters = dict(payload["parameters"])
    payload_parameters[field] = value
    payload["parameters"] = payload_parameters

    with pytest.raises(ActionContractViolation, match=message):
        validate_action_contract(payload)


def test_invalid_parameters_return_normalized_validation_failure() -> None:
    payload = _valid_openai_action_contract()
    payload["parameters"] = {
        "model": "gpt-5-mini",
        "prompt": "",
        "max_tokens": 64,
        "temperature": 0.2,
    }
    client = StubClient()

    result = execute_openai_action(payload, client=client)

    assert client.calls == []
    assert validate_action_result_contract(result) == result
    assert result["status"] == "failed"
    assert result["error_code"] == "validation_error"
    assert result["error_message"] == "parameters.prompt must not be empty"


def test_unknown_parameters_return_normalized_validation_failure() -> None:
    payload = _valid_openai_action_contract()
    payload_parameters = dict(payload["parameters"])
    payload_parameters["extra"] = "nope"
    payload["parameters"] = payload_parameters

    result = execute_openai_action(payload)

    assert validate_action_result_contract(result) == result
    assert result["status"] == "failed"
    assert result["error_code"] == "validation_error"
    assert result["error_message"] == "parameters contains unsupported fields: extra"


def test_dry_run_returns_simulation_without_calling_api() -> None:
    client = StubClient()

    result = execute_openai_action(
        _valid_openai_action_contract(execution_mode="dry_run"),
        client=client,
    )

    assert client.calls == []
    assert validate_action_result_contract(result) == result
    assert result["status"] == "success"
    assert result["result_type"] == "simulation"
    assert result["payload"]["note"] == "dry run"
    assert result["payload"]["metadata"]["dry_run"] is True
    assert result["payload"]["metadata"]["simulation_mode"] == "dry_run"


def test_live_request_returns_usage_and_latency_metadata() -> None:
    client = StubClient(
        responses=[
            OpenAITextResponse(
                text="Normalized model output",
                input_tokens=11,
                output_tokens=7,
                total_tokens=18,
            )
        ]
    )

    result = execute_openai_action(
        _valid_openai_action_contract(),
        client=client,
        config=OpenAIAdapterConfig(timeout_seconds=9, max_retries=1),
    )

    assert len(client.calls) == 1
    assert validate_action_result_contract(result) == result
    assert result["status"] == "success"
    assert result["result_type"] == "text_generation"
    assert result["payload"]["text"] == "Normalized model output"
    metadata = result["payload"]["metadata"]
    assert metadata["input_tokens"] == 11
    assert metadata["output_tokens"] == 7
    assert metadata["total_tokens"] == 18
    assert metadata["latency_ms"] >= 0
    assert metadata["attempt_count"] == 1
    assert metadata["timeout_seconds"] == 9
    assert client.calls[0]["timeout_seconds"] == 9


def test_timeout_returns_normalized_failed_result() -> None:
    client = StubClient(
        responses=[
            OpenAIAdapterError(
                "timeout",
                "OpenAI request timed out",
                diagnostic={"reason": "timed out"},
            )
        ]
    )

    result = execute_openai_action(
        _valid_openai_action_contract(),
        client=client,
        config=OpenAIAdapterConfig(timeout_seconds=2, max_retries=0),
    )

    assert validate_action_result_contract(result) == result
    assert result["status"] == "failed"
    assert result["error_code"] == "timeout"
    assert result["error_message"] == "OpenAI request timed out"
    assert result["payload"]["diagnostic"]["reason"] == "timed out"


def test_transient_failure_respects_retry_cap() -> None:
    client = StubClient(
        responses=[
            OpenAIAdapterError(
                "transport_error",
                "OpenAI transport request failed",
                diagnostic={"reason": "connection reset by peer"},
                retryable=True,
            ),
            OpenAIAdapterError(
                "transport_error",
                "OpenAI transport request failed",
                diagnostic={"reason": "connection reset by peer"},
                retryable=True,
            ),
            OpenAIAdapterError(
                "transport_error",
                "OpenAI transport request failed",
                diagnostic={"reason": "connection reset by peer"},
                retryable=True,
            ),
        ]
    )

    result = execute_openai_action(
        _valid_openai_action_contract(),
        client=client,
        config=OpenAIAdapterConfig(timeout_seconds=5, max_retries=2),
    )

    assert validate_action_result_contract(result) == result
    assert len(client.calls) == 3
    assert result["status"] == "failed"
    assert result["error_code"] == "transport_error"
    assert result["payload"]["metadata"]["attempt_count"] == 3


def test_validation_failure_does_not_retry() -> None:
    payload = _valid_openai_action_contract()
    payload["parameters"] = {
        "model": "gpt-5-mini",
        "prompt": "",
        "max_tokens": 64,
        "temperature": 0.2,
    }
    client = StubClient(
        responses=[
            OpenAIAdapterError(
                "transport_error",
                "OpenAI transport request failed",
                diagnostic={"reason": "connection reset by peer"},
                retryable=True,
            )
        ]
    )

    result = execute_openai_action(
        payload,
        client=client,
        config=OpenAIAdapterConfig(timeout_seconds=5, max_retries=2),
    )

    assert validate_action_result_contract(result) == result
    assert client.calls == []
    assert result["error_code"] == "validation_error"
    assert result["payload"]["metadata"]["attempt_count"] == 0


def test_supported_model_allowlist_is_enforced() -> None:
    client = StubClient()

    result = execute_openai_action(
        _valid_openai_action_contract(),
        client=client,
        config=OpenAIAdapterConfig(
            timeout_seconds=5,
            max_retries=0,
            supported_models=("gpt-4.1-mini",),
        ),
    )

    assert client.calls == []
    assert validate_action_result_contract(result) == result
    assert result["status"] == "failed"
    assert result["error_code"] == "validation_error"
    assert result["error_message"] == "parameters.model is not supported: gpt-5-mini"


def test_unknown_adapter_config_fields_are_rejected() -> None:
    result = execute_openai_action(
        _valid_openai_action_contract(),
        config={"timeout_seconds": 5, "max_retries": 0, "unexpected": True},
    )

    assert validate_action_result_contract(result) == result
    assert result["status"] == "failed"
    assert result["error_code"] == "validation_error"
    assert result["error_message"] == "adapter config contains unsupported fields: unexpected"


def test_adapter_does_not_mutate_input_contract_or_external_state() -> None:
    action_contract = _valid_openai_action_contract()
    original_contract = copy.deepcopy(action_contract)
    state = {
        "task_id": "DF-TASK-001",
        "status": "running",
        "memory_version": 7,
    }
    original_state = copy.deepcopy(state)
    client = StubClient(responses=[OpenAITextResponse(text="stable output")])

    result = execute_openai_action(action_contract, client=client)

    assert result["status"] == "success"
    assert action_contract == original_contract
    assert state == original_state


def test_adapter_never_leaks_raw_exception_outside_boundary() -> None:
    client = StubClient(responses=[RuntimeError("upstream exploded")])

    result = execute_openai_action(
        _valid_openai_action_contract(),
        client=client,
        config=OpenAIAdapterConfig(timeout_seconds=5, max_retries=0),
    )

    assert validate_action_result_contract(result) == result
    assert result["status"] == "failed"
    assert result["error_code"] == "unknown_error"
    assert result["error_message"] == "OpenAI adapter encountered an unexpected error"
    assert result["payload"]["diagnostic"]["exception_type"] == "RuntimeError"
