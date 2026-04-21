from __future__ import annotations

from typing import Any

GOOGLE_LAYER_TOOL = "google.layer"
GOOGLE_GMAIL_SEND_TOOL = "google.gmail.send"
GOOGLE_DOCS_CREATE_TOOL = "google.docs.create"
GOOGLE_DRIVE_READ_FILE_TOOL = "google.drive.read_file"

INTEGRATED_GOOGLE_TOOLS = (
    GOOGLE_GMAIL_SEND_TOOL,
    GOOGLE_DOCS_CREATE_TOOL,
    GOOGLE_DRIVE_READ_FILE_TOOL,
)

_ROUTE_ALIASES = {
    GOOGLE_GMAIL_SEND_TOOL: GOOGLE_GMAIL_SEND_TOOL,
    "gmail.send": GOOGLE_GMAIL_SEND_TOOL,
    "send_email": GOOGLE_GMAIL_SEND_TOOL,
    "send email": GOOGLE_GMAIL_SEND_TOOL,
    GOOGLE_DOCS_CREATE_TOOL: GOOGLE_DOCS_CREATE_TOOL,
    "docs.create": GOOGLE_DOCS_CREATE_TOOL,
    "create_doc": GOOGLE_DOCS_CREATE_TOOL,
    "create doc": GOOGLE_DOCS_CREATE_TOOL,
    GOOGLE_DRIVE_READ_FILE_TOOL: GOOGLE_DRIVE_READ_FILE_TOOL,
    "google_drive.read_file": GOOGLE_DRIVE_READ_FILE_TOOL,
    "drive.read_file": GOOGLE_DRIVE_READ_FILE_TOOL,
    "read_drive_file": GOOGLE_DRIVE_READ_FILE_TOOL,
    "read drive file": GOOGLE_DRIVE_READ_FILE_TOOL,
}
_ROUTE_HINT_FIELDS = ("tool_name", "tool", "target_tool", "operation", "action", "route")
_NESTED_INPUT_FIELDS = ("input", "payload", "arguments", "args")


class GoogleLayerError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = str(code)
        self.message = str(message)


def _has_value(value: Any) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return value is not None


def _normalize_route_name(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    return _ROUTE_ALIASES.get(normalized, "")


def _delegate_input(input_payload: dict[str, Any]) -> dict[str, Any]:
    for field_name in _NESTED_INPUT_FIELDS:
        candidate = input_payload.get(field_name)
        if isinstance(candidate, dict):
            return dict(candidate)

    return {
        key: value
        for key, value in input_payload.items()
        if key not in _ROUTE_HINT_FIELDS and key not in _NESTED_INPUT_FIELDS
    }


def _route_from_explicit_hint(input_payload: dict[str, Any]) -> str:
    for field_name in _ROUTE_HINT_FIELDS:
        normalized = _normalize_route_name(input_payload.get(field_name))
        if normalized:
            return normalized
    return ""


def _route_from_payload_shape(delegate_input: dict[str, Any]) -> str:
    if all(_has_value(delegate_input.get(field)) for field in ("to", "subject", "body")):
        return GOOGLE_GMAIL_SEND_TOOL
    if all(_has_value(delegate_input.get(field)) for field in ("title", "content")):
        return GOOGLE_DOCS_CREATE_TOOL
    if _has_value(delegate_input.get("file_id")):
        return GOOGLE_DRIVE_READ_FILE_TOOL
    return ""


def _document_input_from_result(result: Any) -> dict[str, str] | None:
    if not isinstance(result, dict):
        return None

    document_payload = result.get("document")
    if isinstance(document_payload, dict):
        title = str(document_payload.get("title") or "").strip()
        content = str(document_payload.get("content") or "")
        if title and content.strip():
            return {
                "title": title,
                "content": content,
            }

    title = str(result.get("title") or "").strip()
    content = str(result.get("content") or "")
    if title and content.strip():
        return {
            "title": title,
            "content": content,
        }
    return None


def _email_input_from_result(result: Any, fallback_input: dict[str, Any]) -> dict[str, str] | None:
    if not isinstance(result, dict):
        return None

    recipient = str(result.get("to") or result.get("email_to") or fallback_input.get("to") or "").strip()
    subject = str(
        result.get("subject") or result.get("email_subject") or fallback_input.get("subject") or ""
    ).strip()
    body = str(result.get("body") or result.get("email_body") or fallback_input.get("body") or "")
    if recipient and subject and body.strip():
        return {
            "to": recipient,
            "subject": subject,
            "body": body,
        }
    return None


def _drive_input_from_result(result: Any, fallback_input: dict[str, Any]) -> dict[str, str] | None:
    if not isinstance(result, dict):
        result = {}

    file_id = str(result.get("file_id") or fallback_input.get("file_id") or "").strip()
    if not file_id:
        return None
    return {"file_id": file_id}


def _prepared_route(input_payload: dict[str, Any]) -> tuple[str, dict[str, Any]] | None:
    task = str(input_payload.get("task") or "").strip()
    context = input_payload.get("context")
    if not task or not isinstance(context, dict):
        return None

    from integrations.gemini_tool import call_gemini_google_operator

    response_payload = call_gemini_google_operator(task, context)
    result = response_payload.get("result") if isinstance(response_payload, dict) else response_payload

    document_input = _document_input_from_result(result)
    if document_input is not None:
        return GOOGLE_DOCS_CREATE_TOOL, document_input

    delegate_input = _delegate_input(input_payload)

    email_input = _email_input_from_result(result, delegate_input)
    if email_input is not None:
        return GOOGLE_GMAIL_SEND_TOOL, email_input

    drive_input = _drive_input_from_result(result, delegate_input)
    if drive_input is not None:
        return GOOGLE_DRIVE_READ_FILE_TOOL, drive_input

    return None


def _resolved_route(input_payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    explicit_route = _route_from_explicit_hint(input_payload)
    delegate_input = _delegate_input(input_payload)
    if explicit_route:
        return explicit_route, delegate_input

    inferred_route = _route_from_payload_shape(delegate_input)
    if inferred_route:
        return inferred_route, delegate_input

    prepared_route = _prepared_route(input_payload)
    if prepared_route is not None:
        return prepared_route

    raise GoogleLayerError(
        "GOOGLE_LAYER_ROUTE_FAILED",
        "Google Layer could not determine a target tool.",
    )


def run_google_layer_external(input_payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(input_payload, dict):
        raise GoogleLayerError("GOOGLE_LAYER_INPUT_INVALID", "Google Layer input must be an object.")

    target_tool_name, delegate_input = _resolved_route(input_payload)
    if target_tool_name == GOOGLE_LAYER_TOOL:
        raise GoogleLayerError(
            "GOOGLE_LAYER_ROUTE_FAILED",
            "Google Layer cannot route to itself.",
        )

    from control.tool_executor import execute_tool

    delegated_result = execute_tool(target_tool_name, delegate_input)
    if str(delegated_result.get("status") or "").strip() != "success":
        error_payload = delegated_result.get("error")
        if not isinstance(error_payload, dict):
            error_payload = {}
        raise GoogleLayerError(
            str(error_payload.get("type") or "GOOGLE_LAYER_ROUTE_FAILED").strip()
            or "GOOGLE_LAYER_ROUTE_FAILED",
            str(error_payload.get("message") or "Google Layer delegation failed.").strip()
            or "Google Layer delegation failed.",
        )

    delegated_data = delegated_result.get("data")
    if not isinstance(delegated_data, dict):
        raise GoogleLayerError("GOOGLE_LAYER_OUTPUT_INVALID", "Google Layer delegate output must be an object.")
    return dict(delegated_data)
