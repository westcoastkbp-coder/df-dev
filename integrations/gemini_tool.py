from __future__ import annotations

from typing import Any

from control.gemini_executor import call_gemini_google_operator

_REQUIRED_CONTEXT_FIELDS = ("owner", "business", "product", "policies")


def _has_value(value: Any) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return value is not None


def _validated_task_and_context(
    input_payload: dict[str, Any],
) -> tuple[str, dict[str, Any]]:
    if not isinstance(input_payload, dict):
        raise RuntimeError("Gemini tool input must be an object.")

    task = str(input_payload.get("task") or "").strip()
    context = input_payload.get("context")
    if not task:
        raise RuntimeError("Gemini task is required.")
    if not isinstance(context, dict):
        raise RuntimeError("Gemini context must be an object.")

    missing_fields = [
        field
        for field in _REQUIRED_CONTEXT_FIELDS
        if not _has_value(context.get(field))
    ]
    if missing_fields:
        missing_fields_csv = ", ".join(f"context.{field}" for field in missing_fields)
        raise RuntimeError(f"Missing Gemini context fields: {missing_fields_csv}.")

    normalized_context = {
        field: context.get(field) for field in _REQUIRED_CONTEXT_FIELDS
    }
    return task, normalized_context


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


def run_gemini_google_operator_external(
    input_payload: dict[str, Any],
) -> dict[str, Any]:
    task, context = _validated_task_and_context(input_payload)
    response_payload = call_gemini_google_operator(task, context)
    result = (
        response_payload.get("result")
        if isinstance(response_payload, dict)
        else response_payload
    )
    document_input = _document_input_from_result(result)
    if document_input is not None:
        from control.tool_executor import execute_tool
        from control.tool_registry import GOOGLE_DOCS_CREATE_TOOL

        doc_result = execute_tool(GOOGLE_DOCS_CREATE_TOOL, document_input)
        if str(doc_result.get("status") or "").strip() != "success":
            error_payload = dict(doc_result.get("error") or {})
            raise RuntimeError(
                str(
                    error_payload.get("message") or "Google Docs tool execution failed."
                ).strip()
                or "Google Docs tool execution failed."
            )
        doc_data = dict(doc_result.get("data") or {})
        return {
            "result": dict(doc_data.get("result") or {}),
            "execution_trace": dict(doc_data.get("execution_trace") or {}),
        }

    return {
        "result": result,
        "execution_trace": {
            "tool_source": "external",
            "model": "gemini",
        },
    }
