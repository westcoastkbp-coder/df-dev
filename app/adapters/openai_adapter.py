from __future__ import annotations

import json
import math
import os
import re
import socket
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Protocol
from urllib import error, request

from app.execution.action_contract import (
    ActionContractViolation,
    build_action_result_contract,
    validate_action_contract,
)


SUPPORTED_ACTION_TYPE = "OPENAI_REQUEST"
SUPPORTED_TARGET_REF = "openai"
SUPPORTED_PARAMETER_FIELDS = frozenset({"model", "prompt", "max_tokens", "temperature"})
SUPPORTED_CONFIG_FIELDS = frozenset({"timeout_seconds", "max_retries", "supported_models"})
MAX_TOKENS_LIMIT = 4096
DEFAULT_OPENAI_BASE_URL = "https://api.openai.com"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_MAX_RETRIES = 1
MAX_TIMEOUT_SECONDS = 120
MAX_RETRY_LIMIT = 2
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._:@-]+$")


@dataclass(frozen=True, slots=True)
class OpenAIAdapterConfig:
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS
    max_retries: int = DEFAULT_MAX_RETRIES
    supported_models: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class OpenAITextResponse:
    text: str
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None


class OpenAIAdapterError(RuntimeError):
    def __init__(
        self,
        error_code: str,
        message: str,
        *,
        diagnostic: Mapping[str, object] | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.error_code = error_code
        self.message = message
        self.diagnostic = _normalize_diagnostic_mapping(diagnostic)
        self.retryable = retryable


class OpenAITextClient(Protocol):
    def generate_text(
        self,
        *,
        model: str,
        prompt: str,
        max_tokens: int,
        temperature: float,
        timeout_seconds: int,
    ) -> OpenAITextResponse:
        ...


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _truncate_text(value: object, *, limit: int = 240) -> str:
    normalized = _normalize_text(value)
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."


def _normalize_mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _normalize_sequence(value: object) -> list[object]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return list(value)
    return []


def _normalize_diagnostic_mapping(value: Mapping[str, object] | None) -> dict[str, object]:
    normalized: dict[str, object] = {}
    if value is None:
        return normalized
    for key in sorted(value):
        if _IDENTIFIER_PATTERN.fullmatch(key) is None:
            continue
        item = value[key]
        if item is None or isinstance(item, (bool, int)):
            normalized[key] = item
            continue
        if isinstance(item, float):
            if math.isfinite(item):
                normalized[key] = item
            continue
        if isinstance(item, str):
            normalized[key] = _truncate_text(item)
    return normalized


def _bounded_text(value: object, *, field_name: str) -> str:
    normalized = _normalize_text(value)
    if not normalized:
        raise ActionContractViolation(f"{field_name} must not be empty")
    return normalized


def _normalize_supported_models(value: object) -> tuple[str, ...] | None:
    if value is None:
        return None
    if isinstance(value, str):
        items = [item for item in (_normalize_text(part) for part in value.split(",")) if item]
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray, str)):
        items = [_bounded_text(item, field_name="supported_models item") for item in value]
    else:
        raise OpenAIAdapterError(
            "validation_error",
            "supported_models must be a sequence of model identifiers",
        )

    if not items:
        return None

    normalized: list[str] = []
    for item in items:
        if item not in normalized:
            normalized.append(item)
    return tuple(normalized)


def validate_openai_action_parameters(
    parameters: Mapping[str, object],
    *,
    supported_models: Sequence[str] | None = None,
) -> dict[str, object]:
    normalized = dict(parameters)
    missing_fields = sorted(SUPPORTED_PARAMETER_FIELDS - set(normalized))
    if missing_fields:
        raise ActionContractViolation(
            "parameters missing required fields: " + ", ".join(missing_fields)
        )
    unexpected_fields = sorted(set(normalized) - SUPPORTED_PARAMETER_FIELDS)
    if unexpected_fields:
        raise ActionContractViolation(
            "parameters contains unsupported fields: " + ", ".join(unexpected_fields)
        )

    max_tokens = normalized.get("max_tokens")
    if not isinstance(max_tokens, int) or isinstance(max_tokens, bool):
        raise ActionContractViolation("parameters.max_tokens must be an integer")
    if max_tokens <= 0 or max_tokens > MAX_TOKENS_LIMIT:
        raise ActionContractViolation(
            f"parameters.max_tokens must be between 1 and {MAX_TOKENS_LIMIT}"
        )

    temperature = normalized.get("temperature")
    if not isinstance(temperature, (int, float)) or isinstance(temperature, bool):
        raise ActionContractViolation("parameters.temperature must be a finite number")
    if not math.isfinite(float(temperature)):
        raise ActionContractViolation("parameters.temperature must be a finite number")
    normalized_temperature = float(temperature)
    if normalized_temperature < 0.0 or normalized_temperature > 1.0:
        raise ActionContractViolation("parameters.temperature must be between 0 and 1")

    model = _bounded_text(normalized.get("model"), field_name="parameters.model")
    prompt = _bounded_text(normalized.get("prompt"), field_name="parameters.prompt")
    normalized_supported_models = tuple(supported_models or ())
    if normalized_supported_models and model not in normalized_supported_models:
        raise ActionContractViolation(f"parameters.model is not supported: {model}")

    return {
        "model": model,
        "prompt": prompt,
        "max_tokens": max_tokens,
        "temperature": normalized_temperature,
    }


def _responses_url() -> str:
    base_url = _normalize_text(os.getenv("OPENAI_BASE_URL")) or DEFAULT_OPENAI_BASE_URL
    return base_url.rstrip("/") + "/v1/responses"


def _api_key() -> str:
    api_key = _normalize_text(os.getenv("OPENAI_API_KEY"))
    if not api_key:
        raise OpenAIAdapterError("validation_error", "OPENAI_API_KEY is not configured")
    return api_key


def _parse_optional_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and math.isfinite(value) and value.is_integer():
        return int(value)
    return None


def _extract_usage(payload: Mapping[str, object]) -> tuple[int | None, int | None, int | None]:
    usage = _normalize_mapping(payload.get("usage"))
    input_tokens = _parse_optional_int(usage.get("input_tokens"))
    if input_tokens is None:
        input_tokens = _parse_optional_int(usage.get("prompt_tokens"))

    output_tokens = _parse_optional_int(usage.get("output_tokens"))
    if output_tokens is None:
        output_tokens = _parse_optional_int(usage.get("completion_tokens"))

    total_tokens = _parse_optional_int(usage.get("total_tokens"))
    if total_tokens is None and input_tokens is not None and output_tokens is not None:
        total_tokens = input_tokens + output_tokens

    return input_tokens, output_tokens, total_tokens


def _extract_provider_error(detail_text: str) -> tuple[str, dict[str, object]]:
    diagnostic: dict[str, object] = {}
    try:
        payload = json.loads(detail_text)
    except json.JSONDecodeError:
        message = _truncate_text(detail_text)
        if message:
            diagnostic["provider_message"] = message
        return message, diagnostic

    error_payload = _normalize_mapping(_normalize_mapping(payload).get("error"))
    message = _truncate_text(error_payload.get("message"))
    error_type = _normalize_text(error_payload.get("type"))
    error_code = _normalize_text(error_payload.get("code"))
    if message:
        diagnostic["provider_message"] = message
    if error_type:
        diagnostic["provider_error_type"] = error_type
    if error_code:
        diagnostic["provider_error_code"] = error_code
    return message, diagnostic


def _extract_response_text(payload: Mapping[str, object]) -> str:
    direct_text = _normalize_text(payload.get("output_text"))
    if direct_text:
        return direct_text

    for item in _normalize_sequence(payload.get("output")):
        normalized_item = _normalize_mapping(item)
        for content in _normalize_sequence(normalized_item.get("content")):
            normalized_content = _normalize_mapping(content)
            content_type = _normalize_text(normalized_content.get("type"))
            if content_type in {"output_text", "text"}:
                candidate = _normalize_text(normalized_content.get("text"))
                if candidate:
                    return candidate

    for choice in _normalize_sequence(payload.get("choices")):
        normalized_choice = _normalize_mapping(choice)
        message = _normalize_mapping(normalized_choice.get("message"))
        content = message.get("content")
        if isinstance(content, str):
            candidate = _normalize_text(content)
            if candidate:
                return candidate
        for item in _normalize_sequence(content):
            normalized_item = _normalize_mapping(item)
            candidate = _normalize_text(
                normalized_item.get("text") or normalized_item.get("content")
            )
            if candidate:
                return candidate

    raise OpenAIAdapterError(
        "provider_error",
        "OpenAI response did not include generated text",
    )


def _is_timeout_reason(reason: object) -> bool:
    if isinstance(reason, TimeoutError | socket.timeout):
        return True
    return "timed out" in _normalize_text(reason).lower()


def _is_transient_transport_reason(reason: object) -> bool:
    normalized_reason = _normalize_text(reason).lower()
    if not normalized_reason:
        return False
    transient_markers = (
        "connection reset",
        "connection aborted",
        "connection refused",
        "connection closed",
        "network is unreachable",
        "temporarily unavailable",
        "temporary failure",
        "remote end closed connection",
        "server disconnected",
    )
    return any(marker in normalized_reason for marker in transient_markers)


def _read_supported_models_from_env() -> tuple[str, ...] | None:
    raw_value = _normalize_text(os.getenv("OPENAI_ADAPTER_SUPPORTED_MODELS"))
    if not raw_value:
        return None
    return _normalize_supported_models(raw_value)


def _resolve_adapter_config(
    config: OpenAIAdapterConfig | Mapping[str, object] | None,
) -> OpenAIAdapterConfig:
    if config is None:
        raw_timeout = _normalize_text(os.getenv("OPENAI_ADAPTER_TIMEOUT_SECONDS"))
        raw_retries = _normalize_text(os.getenv("OPENAI_ADAPTER_MAX_RETRIES"))
        timeout_seconds: object = DEFAULT_TIMEOUT_SECONDS if not raw_timeout else raw_timeout
        max_retries: object = DEFAULT_MAX_RETRIES if not raw_retries else raw_retries
        supported_models = _read_supported_models_from_env()
    elif isinstance(config, OpenAIAdapterConfig):
        timeout_seconds = config.timeout_seconds
        max_retries = config.max_retries
        supported_models = config.supported_models
    elif isinstance(config, Mapping):
        unexpected_fields = sorted(set(config) - SUPPORTED_CONFIG_FIELDS)
        if unexpected_fields:
            raise OpenAIAdapterError(
                "validation_error",
                "adapter config contains unsupported fields: " + ", ".join(unexpected_fields),
            )
        timeout_seconds = config.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS)
        max_retries = config.get("max_retries", DEFAULT_MAX_RETRIES)
        supported_models = config.get("supported_models")
    else:
        raise OpenAIAdapterError("validation_error", "adapter config must be a mapping")

    if isinstance(timeout_seconds, bool):
        raise OpenAIAdapterError("validation_error", "timeout_seconds must be an integer")
    try:
        normalized_timeout = int(timeout_seconds)
    except (TypeError, ValueError) as exc:
        raise OpenAIAdapterError(
            "validation_error",
            "timeout_seconds must be an integer",
        ) from exc
    if normalized_timeout <= 0 or normalized_timeout > MAX_TIMEOUT_SECONDS:
        raise OpenAIAdapterError(
            "validation_error",
            f"timeout_seconds must be between 1 and {MAX_TIMEOUT_SECONDS}",
        )

    if isinstance(max_retries, bool):
        raise OpenAIAdapterError("validation_error", "max_retries must be an integer")
    try:
        normalized_retries = int(max_retries)
    except (TypeError, ValueError) as exc:
        raise OpenAIAdapterError(
            "validation_error",
            "max_retries must be an integer",
        ) from exc
    if normalized_retries < 0 or normalized_retries > MAX_RETRY_LIMIT:
        raise OpenAIAdapterError(
            "validation_error",
            f"max_retries must be between 0 and {MAX_RETRY_LIMIT}",
        )

    return OpenAIAdapterConfig(
        timeout_seconds=normalized_timeout,
        max_retries=normalized_retries,
        supported_models=_normalize_supported_models(supported_models),
    )


class DefaultOpenAITextClient:
    def generate_text(
        self,
        *,
        model: str,
        prompt: str,
        max_tokens: int,
        temperature: float,
        timeout_seconds: int,
    ) -> OpenAITextResponse:
        payload = {
            "model": model,
            "input": [
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "input_text",
                            "text": prompt,
                        }
                    ],
                }
            ],
            "max_output_tokens": max_tokens,
            "temperature": temperature,
        }
        http_request = request.Request(
            _responses_url(),
            data=json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8"),
            headers={
                "Authorization": f"Bearer {_api_key()}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with request.urlopen(http_request, timeout=timeout_seconds) as response:
                response_payload = json.loads(response.read().decode("utf-8"))
        except error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="ignore")
            provider_message, diagnostic = _extract_provider_error(detail)
            raise OpenAIAdapterError(
                "provider_error",
                _truncate_text(provider_message) or f"OpenAI provider returned HTTP {exc.code}",
                diagnostic={
                    "http_status": exc.code,
                    **diagnostic,
                },
            ) from exc
        except error.URLError as exc:
            reason = getattr(exc, "reason", exc)
            if _is_timeout_reason(reason):
                raise OpenAIAdapterError(
                    "timeout",
                    "OpenAI request timed out",
                    diagnostic={"reason": _truncate_text(reason)},
                    retryable=True,
                ) from exc
            raise OpenAIAdapterError(
                "transport_error",
                "OpenAI transport request failed",
                diagnostic={"reason": _truncate_text(reason)},
                retryable=_is_transient_transport_reason(reason),
            ) from exc
        except (TimeoutError, socket.timeout) as exc:
            raise OpenAIAdapterError(
                "timeout",
                "OpenAI request timed out",
                diagnostic={"reason": _truncate_text(exc)},
                retryable=True,
            ) from exc
        except json.JSONDecodeError as exc:
            raise OpenAIAdapterError(
                "provider_error",
                "OpenAI response was not valid JSON",
                diagnostic={"reason": _truncate_text(exc)},
            ) from exc

        input_tokens, output_tokens, total_tokens = _extract_usage(_normalize_mapping(response_payload))
        return OpenAITextResponse(
            text=_extract_response_text(_normalize_mapping(response_payload)),
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            total_tokens=total_tokens,
        )


def _validate_openai_action_contract(
    action_contract: object,
    *,
    config: OpenAIAdapterConfig,
) -> dict[str, object]:
    validated_contract = validate_action_contract(action_contract)
    if validated_contract["action_type"] != SUPPORTED_ACTION_TYPE:
        raise ActionContractViolation(
            f"unsupported action_type for openai adapter: {validated_contract['action_type']}"
        )
    if _normalize_text(validated_contract.get("target_ref")).lower() != SUPPORTED_TARGET_REF:
        raise ActionContractViolation(f"target_ref must be {SUPPORTED_TARGET_REF}")
    normalized_parameters = validate_openai_action_parameters(
        _normalize_mapping(validated_contract.get("parameters")),
        supported_models=config.supported_models,
    )
    return {
        **validated_contract,
        "parameters": normalized_parameters,
    }


def _fallback_action_id(action_contract: object) -> str:
    candidate = ""
    if isinstance(action_contract, Mapping):
        candidate = _normalize_text(action_contract.get("action_id"))
    if candidate and _IDENTIFIER_PATTERN.fullmatch(candidate):
        return candidate
    return "unknown_action"


def _build_metadata(
    *,
    request_start_time: str,
    started_at_monotonic: float,
    attempt_count: int,
    config: OpenAIAdapterConfig | None,
    dry_run: bool,
    usage: OpenAITextResponse | None = None,
) -> dict[str, object]:
    request_end_time = _utc_timestamp()
    latency_ms = max(0, int(round((time.monotonic() - started_at_monotonic) * 1000)))
    metadata: dict[str, object] = {
        "provider": "openai",
        "dry_run": dry_run,
        "request_start_time": request_start_time,
        "request_end_time": request_end_time,
        "latency_ms": latency_ms,
        "attempt_count": attempt_count,
        "timeout_seconds": config.timeout_seconds if config is not None else None,
        "max_retries": config.max_retries if config is not None else None,
        "input_tokens": None if usage is None else usage.input_tokens,
        "output_tokens": None if usage is None else usage.output_tokens,
        "total_tokens": None if usage is None else usage.total_tokens,
    }
    if dry_run:
        metadata["simulation_mode"] = "dry_run"
    return metadata


def _failed_action_result(
    *,
    action_id: str,
    error_code: str,
    error_message: str,
    metadata: Mapping[str, object],
    diagnostic: Mapping[str, object] | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {"metadata": dict(metadata)}
    normalized_diagnostic = _normalize_diagnostic_mapping(diagnostic)
    if normalized_diagnostic:
        payload["diagnostic"] = normalized_diagnostic
    return build_action_result_contract(
        action_id=action_id,
        status="failed",
        result_type="text_generation",
        payload=payload,
        error_code=error_code,
        error_message=error_message,
    )


def execute_openai_action(
    action_contract: object,
    *,
    client: OpenAITextClient | None = None,
    config: OpenAIAdapterConfig | Mapping[str, object] | None = None,
) -> dict[str, object]:
    request_start_time = _utc_timestamp()
    started_at_monotonic = time.monotonic()
    action_id = _fallback_action_id(action_contract)
    effective_config: OpenAIAdapterConfig | None = None
    attempt_count = 0

    try:
        effective_config = _resolve_adapter_config(config)
        validated_contract = _validate_openai_action_contract(action_contract, config=effective_config)
        action_id = str(validated_contract["action_id"])
        parameters = dict(validated_contract["parameters"])

        if validated_contract["execution_mode"] == "dry_run":
            return build_action_result_contract(
                action_id=action_id,
                status="success",
                result_type="simulation",
                payload={
                    "note": "dry run",
                    "metadata": _build_metadata(
                        request_start_time=request_start_time,
                        started_at_monotonic=started_at_monotonic,
                        attempt_count=0,
                        config=effective_config,
                        dry_run=True,
                    ),
                },
            )

        effective_client = client or DefaultOpenAITextClient()
        last_failure: OpenAIAdapterError | None = None

        for attempt_number in range(1, effective_config.max_retries + 2):
            attempt_count = attempt_number
            try:
                response = effective_client.generate_text(
                    model=str(parameters["model"]),
                    prompt=str(parameters["prompt"]),
                    max_tokens=int(parameters["max_tokens"]),
                    temperature=float(parameters["temperature"]),
                    timeout_seconds=effective_config.timeout_seconds,
                )
                return build_action_result_contract(
                    action_id=action_id,
                    status="success",
                    result_type="text_generation",
                    payload={
                        "text": response.text,
                        "metadata": _build_metadata(
                            request_start_time=request_start_time,
                            started_at_monotonic=started_at_monotonic,
                            attempt_count=attempt_count,
                            config=effective_config,
                            dry_run=False,
                            usage=response,
                        ),
                    },
                )
            except OpenAIAdapterError as exc:
                last_failure = exc
            except Exception as exc:
                last_failure = OpenAIAdapterError(
                    "unknown_error",
                    "OpenAI adapter encountered an unexpected error",
                    diagnostic={
                        "exception_type": type(exc).__name__,
                        "reason": _truncate_text(exc),
                    },
                )

            if last_failure is None or not last_failure.retryable:
                break
            if attempt_number > effective_config.max_retries:
                break

        failure = last_failure or OpenAIAdapterError(
            "unknown_error",
            "OpenAI adapter encountered an unexpected error",
        )
        return _failed_action_result(
            action_id=action_id,
            error_code=failure.error_code,
            error_message=failure.message,
            metadata=_build_metadata(
                request_start_time=request_start_time,
                started_at_monotonic=started_at_monotonic,
                attempt_count=attempt_count,
                config=effective_config,
                dry_run=False,
            ),
            diagnostic=failure.diagnostic,
        )
    except ActionContractViolation as exc:
        return _failed_action_result(
            action_id=action_id,
            error_code="validation_error",
            error_message=str(exc),
            metadata=_build_metadata(
                request_start_time=request_start_time,
                started_at_monotonic=started_at_monotonic,
                attempt_count=attempt_count,
                config=effective_config,
                dry_run=False,
            ),
        )
    except OpenAIAdapterError as exc:
        return _failed_action_result(
            action_id=action_id,
            error_code=exc.error_code,
            error_message=exc.message,
            metadata=_build_metadata(
                request_start_time=request_start_time,
                started_at_monotonic=started_at_monotonic,
                attempt_count=attempt_count,
                config=effective_config,
                dry_run=False,
            ),
            diagnostic=exc.diagnostic,
        )
    except Exception as exc:
        return _failed_action_result(
            action_id=action_id,
            error_code="unknown_error",
            error_message="OpenAI adapter encountered an unexpected error",
            metadata=_build_metadata(
                request_start_time=request_start_time,
                started_at_monotonic=started_at_monotonic,
                attempt_count=attempt_count,
                config=effective_config,
                dry_run=False,
            ),
            diagnostic={
                "exception_type": type(exc).__name__,
                "reason": _truncate_text(exc),
            },
        )
