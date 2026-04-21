from __future__ import annotations

from collections.abc import Callable
from typing import Any

from integrations.claude_tool import run_claude_analyze_external
from integrations.claude_web_tool import run_claude_web_operator_external
from integrations.gemini_tool import run_gemini_google_operator_external
from integrations.gmail_tool import (
    run_gmail_create_draft_external,
    run_gmail_read_latest_external,
    run_google_gmail_send_external,
)
from integrations.google_layer_tool import run_google_layer_external
from integrations.google_docs_tool import (
    run_google_docs_create_document_external,
    run_google_docs_create_external,
)
from integrations.google_drive_tool import run_google_drive_read_file_external
from integrations.linkedin_tool import run_linkedin_create_post_draft_external

ToolExecutor = Callable[[dict[str, Any]], dict[str, Any]]

CLAUDE_ANALYZE_TOOL = "claude.analyze"
CLAUDE_WEB_OPERATOR_TOOL = "claude.web_operator"
EMAIL_SEND_TOOL = "email.send"
GMAIL_CREATE_DRAFT_TOOL = "gmail.create_draft"
GMAIL_READ_LATEST_TOOL = "gmail.read_latest"
GEMINI_GOOGLE_OPERATOR_TOOL = "gemini.google_operator"
GOOGLE_LAYER_TOOL = "google.layer"
GOOGLE_GMAIL_SEND_TOOL = "google.gmail.send"
GOOGLE_DOCS_CREATE_TOOL = "google.docs.create"
GOOGLE_DOCS_CREATE_DOCUMENT_TOOL = "google_docs.create_document"
GOOGLE_DRIVE_READ_FILE_TOOL = "google_drive.read_file"
GOOGLE_DRIVE_READ_FILE_LAYER_TOOL = "google.drive.read_file"
HTTP_REQUEST_TOOL = "http.request"
LINKEDIN_CREATE_POST_DRAFT_TOOL = "linkedin.create_post_draft"

_TOOL_REGISTRY: dict[str, ToolExecutor] = {
    CLAUDE_ANALYZE_TOOL: run_claude_analyze_external,
    CLAUDE_WEB_OPERATOR_TOOL: run_claude_web_operator_external,
    GEMINI_GOOGLE_OPERATOR_TOOL: run_gemini_google_operator_external,
    GMAIL_CREATE_DRAFT_TOOL: run_gmail_create_draft_external,
    GMAIL_READ_LATEST_TOOL: run_gmail_read_latest_external,
    GOOGLE_LAYER_TOOL: run_google_layer_external,
    GOOGLE_GMAIL_SEND_TOOL: run_google_gmail_send_external,
    GOOGLE_DOCS_CREATE_TOOL: run_google_docs_create_external,
    GOOGLE_DOCS_CREATE_DOCUMENT_TOOL: run_google_docs_create_document_external,
    GOOGLE_DRIVE_READ_FILE_TOOL: run_google_drive_read_file_external,
    GOOGLE_DRIVE_READ_FILE_LAYER_TOOL: run_google_drive_read_file_external,
    LINKEDIN_CREATE_POST_DRAFT_TOOL: run_linkedin_create_post_draft_external,
}


def registered_tool_names() -> list[str]:
    return sorted(_TOOL_REGISTRY)


def has_registered_tool(tool_name: str) -> bool:
    return str(tool_name or "").strip() in _TOOL_REGISTRY


def resolve_tool_executor(tool_name: str) -> ToolExecutor:
    normalized_tool_name = str(tool_name or "").strip()
    executor = _TOOL_REGISTRY.get(normalized_tool_name)
    if executor is None:
        raise KeyError(normalized_tool_name)
    return executor
