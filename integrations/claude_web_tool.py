from __future__ import annotations

import json
from typing import Any

from integrations.claude_tool import (
    CLAUDE_ANALYZE_MODEL,
    MAX_OUTPUT_TOKENS,
    _analysis_from_response,
    _api_key,
    _messages_response_with_retry,
)


def _validated_task_and_context(
    input_payload: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    if not isinstance(input_payload, dict):
        raise RuntimeError("Claude web operator input must be an object.")

    task = str(input_payload.get("task") or "").strip()
    context = input_payload.get("context")
    if not task:
        raise RuntimeError("Claude web operator task is required.")
    if not isinstance(context, dict):
        raise RuntimeError("Claude web operator context must be an object.")
    return task, dict(context)


def _request_message_content(task: str, context: dict[str, Any]) -> str:
    return "\n\n".join(
        [
            "Return ONLY valid JSON with no preamble or markdown fences.",
            "You are claude.web_operator.",
            "Handle web-native tasks at a high level, including browsing, LinkedIn workflows, and form completion planning.",
            "Operate like a web operator, not a low-level HTTP client.",
            "Do not describe manual browser implementation or raw HTTP steps.",
            'Respond using exactly this shape:\n{\n  "result": {}\n}',
            f"Task:\n{task}",
            f"Context:\n{json.dumps(context, indent=2, sort_keys=True)}",
        ]
    )


def run_claude_web_operator_external(input_payload: dict[str, Any]) -> dict[str, Any]:
    task, context = _validated_task_and_context(input_payload)
    request_payload = {
        "model": CLAUDE_ANALYZE_MODEL,
        "max_tokens": MAX_OUTPUT_TOKENS,
        "temperature": 0,
        "messages": [
            {
                "role": "user",
                "content": _request_message_content(task, context),
            }
        ],
    }
    response_payload = _messages_response_with_retry(request_payload, _api_key())
    analysis_text = _analysis_from_response(response_payload)
    try:
        parsed_payload = json.loads(analysis_text)
    except json.JSONDecodeError:
        parsed_payload = {"result": analysis_text}

    if not isinstance(parsed_payload, dict):
        parsed_payload = {"result": parsed_payload}

    return {
        "result": parsed_payload.get("result"),
        "execution_trace": {
            "tool_source": "external",
            "model": "claude",
        },
    }
