from __future__ import annotations

import copy
from typing import Any

from control.tool_registry import (
    CLAUDE_ANALYZE_TOOL,
    CLAUDE_WEB_OPERATOR_TOOL,
    EMAIL_SEND_TOOL,
    GEMINI_GOOGLE_OPERATOR_TOOL,
    GMAIL_CREATE_DRAFT_TOOL,
    GMAIL_READ_LATEST_TOOL,
    GOOGLE_LAYER_TOOL,
    GOOGLE_GMAIL_SEND_TOOL,
    GOOGLE_DOCS_CREATE_TOOL,
    GOOGLE_DOCS_CREATE_DOCUMENT_TOOL,
    GOOGLE_DRIVE_READ_FILE_TOOL,
    GOOGLE_DRIVE_READ_FILE_LAYER_TOOL,
    HTTP_REQUEST_TOOL,
    LINKEDIN_CREATE_POST_DRAFT_TOOL,
)

_REQUIRED_INPUT_FIELDS: dict[str, tuple[str, ...]] = {
    CLAUDE_ANALYZE_TOOL: ("text", "instruction"),
    CLAUDE_WEB_OPERATOR_TOOL: ("task", "context"),
    EMAIL_SEND_TOOL: ("to", "subject", "body"),
    GEMINI_GOOGLE_OPERATOR_TOOL: ("task", "context"),
    GMAIL_CREATE_DRAFT_TOOL: ("subject", "body"),
    GMAIL_READ_LATEST_TOOL: (),
    GOOGLE_GMAIL_SEND_TOOL: ("to", "subject", "body"),
    GOOGLE_DOCS_CREATE_TOOL: ("title", "content"),
    GOOGLE_DOCS_CREATE_DOCUMENT_TOOL: ("title", "content"),
    GOOGLE_DRIVE_READ_FILE_TOOL: ("file_id",),
    GOOGLE_DRIVE_READ_FILE_LAYER_TOOL: ("file_id",),
    HTTP_REQUEST_TOOL: ("url",),
    LINKEDIN_CREATE_POST_DRAFT_TOOL: ("topic",),
}
_REQUIRED_CONTEXT_FIELDS: dict[str, tuple[str, ...]] = {
    GEMINI_GOOGLE_OPERATOR_TOOL: ("owner", "business", "product", "policies"),
}
_OBJECT_INPUT_FIELDS: dict[str, tuple[str, ...]] = {
    CLAUDE_WEB_OPERATOR_TOOL: ("context",),
    GEMINI_GOOGLE_OPERATOR_TOOL: ("context",),
}


class ToolValidationError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = str(code)
        self.message = str(message)


def _has_required_value(value: Any) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return value is not None


def validate_tool_call(tool_call: Any) -> dict[str, Any]:
    if not isinstance(tool_call, dict):
        raise ToolValidationError("TOOL_CALL_INVALID", "tool_call must be an object.")

    tool_name = str(tool_call.get("tool_name") or "").strip()
    if not tool_name:
        raise ToolValidationError("TOOL_NAME_REQUIRED", "tool_call.tool_name is required.")

    context_payload = tool_call.get("context")
    if context_payload is not None and not isinstance(context_payload, dict):
        raise ToolValidationError("TOOL_INPUT_INVALID", "tool_call.context must be an object.")

    input_payload = tool_call.get("input")
    if not isinstance(input_payload, dict):
        raise ToolValidationError("TOOL_INPUT_INVALID", "tool_call.input must be an object.")

    missing_fields = [
        field
        for field in _REQUIRED_INPUT_FIELDS.get(tool_name, ())
        if not str(input_payload.get(field) or "").strip()
    ]
    if missing_fields:
        missing_fields_csv = ", ".join(missing_fields)
        raise ToolValidationError(
            "TOOL_INPUT_MISSING_FIELDS",
            f"Missing required tool input fields: {missing_fields_csv}.",
        )

    object_fields = _OBJECT_INPUT_FIELDS.get(tool_name, ())
    invalid_object_fields = [
        field for field in object_fields if not isinstance(input_payload.get(field), dict)
    ]
    if invalid_object_fields:
        invalid_fields_csv = ", ".join(f"tool_call.input.{field}" for field in invalid_object_fields)
        raise ToolValidationError(
            "TOOL_INPUT_INVALID",
            f"{invalid_fields_csv} must be an object.",
        )

    required_context_fields = _REQUIRED_CONTEXT_FIELDS.get(tool_name, ())
    if required_context_fields:
        context_payload = input_payload.get("context")
        missing_context_fields = [
            f"context.{field}"
            for field in required_context_fields
            if not _has_required_value(context_payload.get(field))
        ]
        if missing_context_fields:
            missing_fields_csv = ", ".join(missing_context_fields)
            raise ToolValidationError(
                "TOOL_INPUT_MISSING_FIELDS",
                f"Missing required tool input fields: {missing_fields_csv}.",
            )

    return {
        "tool_name": tool_name,
        "context": copy.deepcopy(context_payload) if isinstance(context_payload, dict) else None,
        "input": copy.deepcopy(input_payload),
    }
