from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from urllib import error, request


DEFAULT_OPENAI_BASE_URL = "https://api.openai.com"
DEFAULT_ORCHESTRATOR_MODEL = "gpt-5-mini"
DEFAULT_TIMEOUT_SECONDS = 20


class OrchestratorClientError(RuntimeError):
    pass


class OrchestratorUnavailableError(OrchestratorClientError):
    pass


class OrchestratorInvalidResponseError(OrchestratorClientError):
    pass


def _normalize_text(value: object) -> str:
    return " ".join(str(value or "").strip().split())


def _normalize_mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _normalize_sequence(value: object) -> list[object]:
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return list(value)
    return []


def _json_ready(value: object) -> object:
    if isinstance(value, Mapping):
        return {
            _normalize_text(key): _json_ready(item)
            for key, item in value.items()
            if _normalize_text(key)
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_ready(item) for item in value]
    return value


def _responses_url() -> str:
    base_url = _normalize_text(os.getenv("OPENAI_BASE_URL")) or DEFAULT_OPENAI_BASE_URL
    return base_url.rstrip("/") + "/v1/responses"


def _model_name() -> str:
    return (
        _normalize_text(os.getenv("DF_ORCHESTRATOR_MODEL"))
        or _normalize_text(os.getenv("OPENAI_MODEL"))
        or DEFAULT_ORCHESTRATOR_MODEL
    )


def _api_key() -> str:
    api_key = _normalize_text(os.getenv("OPENAI_API_KEY"))
    if not api_key:
        raise OrchestratorUnavailableError("OPENAI_API_KEY is not configured")
    return api_key


def _output_schema() -> dict[str, object]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "action",
            "action_type",
            "target",
            "parameters",
            "requires_confirmation",
            "reason",
        ],
        "properties": {
            "action": {"type": "string"},
            "action_type": {
                "type": "string",
                "enum": ["read", "write", "external", "critical"],
            },
            "target": {"type": "string"},
            "parameters": {
                "type": "object",
                "additionalProperties": True,
            },
            "requires_confirmation": {"type": "boolean"},
            "reason": {"type": "string"},
        },
    }


def _instructions() -> str:
    return (
        "You are the Digital Foreman AI orchestrator. "
        "Return exactly one JSON object that matches the provided schema. "
        "Use only allowed_actions. "
        "Choose action_type from read, write, external, critical. "
        "Set requires_confirmation to true only when human confirmation is still required. "
        "Do not add extra keys."
    )


def _extract_output_text(response_payload: Mapping[str, object]) -> str:
    direct_output = _normalize_text(response_payload.get("output_text"))
    if direct_output:
        return direct_output

    for item in _normalize_sequence(response_payload.get("output")):
        normalized_item = _normalize_mapping(item)
        for content in _normalize_sequence(normalized_item.get("content")):
            normalized_content = _normalize_mapping(content)
            if _normalize_text(normalized_content.get("type")) in {"output_text", "text"}:
                candidate = _normalize_text(normalized_content.get("text"))
                if candidate:
                    return candidate

    raise OrchestratorInvalidResponseError("OpenAI response did not include output_text")


def call_orchestrator(
    task: object,
    context_summary: object,
    input_text: object,
    *,
    allowed_actions: Sequence[object] | None = None,
    policy_rules: Sequence[object] | None = None,
) -> dict[str, object]:
    request_payload = {
        "task": _json_ready(_normalize_mapping(task)),
        "context_summary": _normalize_text(context_summary),
        "input": _normalize_text(input_text),
        "constraints": {
            "allowed_actions": [
                _normalize_text(action).lower().replace(" ", "_")
                for action in (allowed_actions or [])
                if _normalize_text(action)
            ],
            "policy_rules": [
                _normalize_text(rule)
                for rule in (policy_rules or [])
                if _normalize_text(rule)
            ],
        },
    }
    payload = {
        "model": _model_name(),
        "instructions": _instructions(),
        "input": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "input_text",
                        "text": json.dumps(
                            request_payload,
                            ensure_ascii=True,
                            separators=(",", ":"),
                        ),
                    }
                ],
            }
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "df_action_plan",
                "strict": True,
                "schema": _output_schema(),
            }
        },
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
    timeout_seconds = int(
        _normalize_text(os.getenv("DF_ORCHESTRATOR_TIMEOUT_SECONDS"))
        or DEFAULT_TIMEOUT_SECONDS
    )
    try:
        with request.urlopen(http_request, timeout=timeout_seconds) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise OrchestratorUnavailableError(
            f"OpenAI request failed with HTTP {exc.code}: {_normalize_text(detail) or exc.reason}"
        ) from exc
    except error.URLError as exc:
        raise OrchestratorUnavailableError(
            f"OpenAI request failed: {_normalize_text(getattr(exc, 'reason', exc)) or 'network error'}"
        ) from exc
    except TimeoutError as exc:
        raise OrchestratorUnavailableError("OpenAI request timed out") from exc
    except json.JSONDecodeError as exc:
        raise OrchestratorUnavailableError("OpenAI response was not valid JSON") from exc

    output_text = _extract_output_text(response_payload)
    try:
        parsed = json.loads(output_text)
    except json.JSONDecodeError as exc:
        raise OrchestratorInvalidResponseError("OpenAI output was not valid JSON") from exc
    if not isinstance(parsed, Mapping):
        raise OrchestratorInvalidResponseError("OpenAI output must be a JSON object")
    return dict(parsed)
