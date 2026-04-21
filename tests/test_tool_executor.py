from __future__ import annotations

import json
from urllib.error import URLError

import scripts.run_codex_task as run_codex_task_module
from control.tool_executor import execute_tool_call
from control.tool_registry import (
    CLAUDE_ANALYZE_TOOL,
    CLAUDE_WEB_OPERATOR_TOOL,
    EMAIL_SEND_TOOL,
    GEMINI_GOOGLE_OPERATOR_TOOL,
    GMAIL_CREATE_DRAFT_TOOL,
    GMAIL_READ_LATEST_TOOL,
    GOOGLE_GMAIL_SEND_TOOL,
    GOOGLE_DOCS_CREATE_DOCUMENT_TOOL,
    GOOGLE_DRIVE_READ_FILE_TOOL,
    HTTP_REQUEST_TOOL,
    LINKEDIN_CREATE_POST_DRAFT_TOOL,
    has_registered_tool,
    resolve_tool_executor,
)
from integrations.claude_tool import CLAUDE_ANALYZE_MODEL, ClaudeToolError, run_claude_analyze
from scripts.run_codex_task import run_codex_task


def test_execute_tool_returns_structured_email_send_result() -> None:
    from control.tool_executor import execute_tool

    result = execute_tool(
        EMAIL_SEND_TOOL,
        {
            "to": "client@example.com",
            "subject": "ADU project",
            "body": "Thanks for reaching out.",
        },
    )

    assert result == {
        "status": "success",
        "data": {
            "to": "client@example.com",
            "subject": "ADU project",
            "body": "Thanks for reaching out.",
            "delivery": "queued",
        },
        "error": None,
        "source": "fallback",
    }


def test_execute_tool_returns_structured_http_request_result(monkeypatch) -> None:
    from control.tool_executor import execute_tool

    class _FakeResponse:
        status = 200

        def __init__(self) -> None:
            self.headers = {"Content-Type": "application/json"}

        def read(self) -> bytes:
            return b'{\"ok\": true}'

        def geturl(self) -> str:
            return "https://example.com/ping"

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

    monkeypatch.setattr("control.tool_executor.urlopen", lambda request, timeout=15: _FakeResponse())

    result = execute_tool(
        HTTP_REQUEST_TOOL,
        {
            "url": "https://example.com/ping",
            "method": "GET",
        },
    )

    assert result == {
        "status": "success",
        "data": {
            "status_code": 200,
            "headers": {"Content-Type": "application/json"},
            "body": {"ok": True},
            "url": "https://example.com/ping",
            "method": "GET",
        },
        "error": None,
        "source": "external",
    }


def test_execute_tool_call_supports_email_send() -> None:
    result = execute_tool_call(
        {
            "tool_name": EMAIL_SEND_TOOL,
            "input": {
                "to": "client@example.com",
                "subject": "ADU project",
                "body": "Thanks for reaching out.",
            },
        }
    )

    assert result["ok"] is True
    assert result["tool_name"] == EMAIL_SEND_TOOL
    assert result["output"] == {
        "to": "client@example.com",
        "subject": "ADU project",
        "body": "Thanks for reaching out.",
        "delivery": "queued",
    }
    assert result["source"] == "fallback"
    assert result["retry_info"]["total_retry_count"] == 1
    assert [step["step_name"] for step in result["step_metrics"]] == [
        "validate_tool_call",
        "resolve_tool_executor",
        "execute_external_tool",
        "execute_fallback_tool",
        "validate_tool_output",
    ]
    _assert_execution_observability(result)


def test_execute_tool_returns_structured_error_when_http_request_fails(monkeypatch) -> None:
    from control.tool_executor import execute_tool

    monkeypatch.setattr(
        "control.tool_executor.urlopen",
        lambda request, timeout=15: (_ for _ in ()).throw(URLError("network down")),
    )

    result = execute_tool(
        HTTP_REQUEST_TOOL,
        {
            "url": "https://example.com/ping",
            "method": "GET",
        },
    )

    assert result == {
        "status": "error",
        "data": None,
        "error": {
            "type": "HTTP_REQUEST_FAILED",
            "message": "<urlopen error network down>",
        },
        "source": "external",
    }


def test_execute_tool_returns_fallback_source_when_claude_api_fails(monkeypatch) -> None:
    from control.tool_executor import execute_tool

    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("CLAUDE_API_KEY", "bad-key")
    monkeypatch.setattr(
        "integrations.claude_tool._messages_response",
        lambda request_payload, api_key: (_ for _ in ()).throw(
            ClaudeToolError("CLAUDE_API_FAILED", "invalid x-api-key")
        ),
    )

    result = execute_tool(
        CLAUDE_ANALYZE_TOOL,
        {
            "instruction": "Summarize this text in 2 sentences",
            "text": "Digital Foreman manages tasks.",
        },
    )

    assert result == {
        "status": "success",
        "data": {
            "analysis": "Digital Foreman manages tasks.",
        },
        "error": None,
        "source": "fallback",
    }


def _assert_execution_observability(result: dict[str, object]) -> None:
    execution_timeline = result.get("execution_timeline")
    assert isinstance(execution_timeline, dict)
    assert isinstance(execution_timeline.get("start_time"), str)
    assert isinstance(execution_timeline.get("end_time"), str)
    assert isinstance(execution_timeline.get("total_duration_ms"), int)

    step_metrics = result.get("step_metrics")
    assert isinstance(step_metrics, list)
    assert step_metrics
    for step in step_metrics:
        assert isinstance(step, dict)
        assert isinstance(step.get("step_name"), str)
        assert isinstance(step.get("tool_name"), str)
        assert isinstance(step.get("success"), bool)
        assert isinstance(step.get("step_start_time"), str)
        assert isinstance(step.get("step_end_time"), str)
        assert isinstance(step.get("step_duration_ms"), int)
        assert isinstance(step.get("retry_count"), int)

    retry_info = result.get("retry_info")
    assert isinstance(retry_info, dict)
    assert isinstance(retry_info.get("total_retry_count"), int)


def test_claude_tool_resolves_from_registry() -> None:
    executor = resolve_tool_executor(CLAUDE_ANALYZE_TOOL)

    assert has_registered_tool(CLAUDE_ANALYZE_TOOL) is True
    assert callable(executor)


def test_claude_web_operator_tool_resolves_from_registry() -> None:
    executor = resolve_tool_executor(CLAUDE_WEB_OPERATOR_TOOL)

    assert has_registered_tool(CLAUDE_WEB_OPERATOR_TOOL) is True
    assert callable(executor)


def test_google_docs_tool_resolves_from_registry() -> None:
    executor = resolve_tool_executor(GOOGLE_DOCS_CREATE_DOCUMENT_TOOL)

    assert has_registered_tool(GOOGLE_DOCS_CREATE_DOCUMENT_TOOL) is True
    assert callable(executor)


def test_google_drive_tool_resolves_from_registry() -> None:
    executor = resolve_tool_executor(GOOGLE_DRIVE_READ_FILE_TOOL)

    assert has_registered_tool(GOOGLE_DRIVE_READ_FILE_TOOL) is True
    assert callable(executor)


def test_gemini_google_operator_tool_resolves_from_registry() -> None:
    executor = resolve_tool_executor(GEMINI_GOOGLE_OPERATOR_TOOL)

    assert has_registered_tool(GEMINI_GOOGLE_OPERATOR_TOOL) is True
    assert callable(executor)


def test_gmail_read_latest_tool_resolves_from_registry() -> None:
    executor = resolve_tool_executor(GMAIL_READ_LATEST_TOOL)

    assert has_registered_tool(GMAIL_READ_LATEST_TOOL) is True
    assert callable(executor)


def test_gmail_create_draft_tool_resolves_from_registry() -> None:
    executor = resolve_tool_executor(GMAIL_CREATE_DRAFT_TOOL)

    assert has_registered_tool(GMAIL_CREATE_DRAFT_TOOL) is True
    assert callable(executor)


def test_google_gmail_send_tool_resolves_from_registry() -> None:
    executor = resolve_tool_executor(GOOGLE_GMAIL_SEND_TOOL)

    assert has_registered_tool(GOOGLE_GMAIL_SEND_TOOL) is True
    assert callable(executor)


def test_linkedin_create_post_draft_tool_resolves_from_registry() -> None:
    executor = resolve_tool_executor(LINKEDIN_CREATE_POST_DRAFT_TOOL)

    assert has_registered_tool(LINKEDIN_CREATE_POST_DRAFT_TOOL) is True
    assert callable(executor)


def test_execute_tool_call_rejects_invalid_tool_name() -> None:
    result = execute_tool_call(
        {
            "tool_name": "google_docs.missing_tool",
            "input": {
                "title": "Kickoff Summary",
                "content": "Create the summary document.",
            },
        }
    )

    assert result["ok"] is False
    assert result["tool_name"] == "google_docs.missing_tool"
    assert result["output"] is None
    assert result["error"] == {
        "code": "TOOL_NOT_FOUND",
        "message": "Tool is not registered: google_docs.missing_tool.",
    }
    _assert_execution_observability(result)


def test_execute_tool_call_rejects_missing_required_input() -> None:
    result = execute_tool_call(
        {
            "tool_name": GOOGLE_DOCS_CREATE_DOCUMENT_TOOL,
            "input": {
                "title": "Kickoff Summary",
            },
        }
    )

    assert result["ok"] is False
    assert result["tool_name"] == GOOGLE_DOCS_CREATE_DOCUMENT_TOOL
    assert result["output"] is None
    assert result["error"] == {
        "code": "TOOL_INPUT_MISSING_FIELDS",
        "message": "Missing required tool input fields: content.",
    }
    _assert_execution_observability(result)


def test_execute_tool_call_rejects_missing_claude_text() -> None:
    result = execute_tool_call(
        {
            "tool_name": CLAUDE_ANALYZE_TOOL,
            "input": {
                "instruction": "Summarize this text in 2 sentences",
            },
        }
    )

    assert result["ok"] is False
    assert result["tool_name"] == CLAUDE_ANALYZE_TOOL
    assert result["output"] is None
    assert result["error"] == {
        "code": "TOOL_INPUT_MISSING_FIELDS",
        "message": "Missing required tool input fields: text.",
    }
    _assert_execution_observability(result)


def test_execute_tool_call_rejects_missing_claude_instruction() -> None:
    result = execute_tool_call(
        {
            "tool_name": CLAUDE_ANALYZE_TOOL,
            "input": {
                "text": "Digital Foreman is an execution control system.",
            },
        }
    )

    assert result["ok"] is False
    assert result["tool_name"] == CLAUDE_ANALYZE_TOOL
    assert result["output"] is None
    assert result["error"] == {
        "code": "TOOL_INPUT_MISSING_FIELDS",
        "message": "Missing required tool input fields: instruction.",
    }
    _assert_execution_observability(result)


def test_execute_tool_call_rejects_non_object_claude_web_context() -> None:
    result = execute_tool_call(
        {
            "tool_name": CLAUDE_WEB_OPERATOR_TOOL,
            "input": {
                "task": "Review the LinkedIn profile draft and suggest edits before posting.",
                "context": "owner=Anton",
            },
        }
    )

    assert result["ok"] is False
    assert result["tool_name"] == CLAUDE_WEB_OPERATOR_TOOL
    assert result["output"] is None
    assert result["error"] == {
        "code": "TOOL_INPUT_INVALID",
        "message": "tool_call.input.context must be an object.",
    }
    _assert_execution_observability(result)


def test_execute_tool_call_rejects_missing_gemini_context_fields() -> None:
    result = execute_tool_call(
        {
            "tool_name": GEMINI_GOOGLE_OPERATOR_TOOL,
            "input": {
                "task": "Draft a follow-up email based on the meeting notes.",
                "context": {
                    "owner": "Anton",
                    "business": "Digital Foreman",
                    "product": "Operations system",
                },
            },
        }
    )

    assert result["ok"] is False
    assert result["tool_name"] == GEMINI_GOOGLE_OPERATOR_TOOL
    assert result["output"] is None
    assert result["error"] == {
        "code": "TOOL_INPUT_MISSING_FIELDS",
        "message": "Missing required tool input fields: context.policies.",
    }
    _assert_execution_observability(result)


def test_run_claude_analyze_returns_clean_analysis(monkeypatch) -> None:
    captured_request: dict[str, object] = {}
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("CLAUDE_API_KEY", "test-claude-key")
    monkeypatch.setattr(
        "integrations.claude_tool._messages_response",
        lambda request_payload, api_key: (
            captured_request.update(
                {
                    "request_payload": dict(request_payload),
                    "api_key": api_key,
                }
            )
            or {
                "content": [
                    {
                        "type": "text",
                        "text": "Digital Foreman manages tasks through a deterministic pipeline.",
                    }
                ]
            }
        ),
    )

    result = run_claude_analyze(
        {
            "instruction": "Summarize this text in 2 sentences",
            "text": (
                "Digital Foreman is an execution control system that manages tasks, "
                "validates them, and executes external tools through a deterministic pipeline."
            ),
        }
    )

    assert result == {
        "analysis": "Digital Foreman manages tasks through a deterministic pipeline.",
    }
    assert captured_request["api_key"] == "test-claude-key"
    assert captured_request["request_payload"] == {
        "model": CLAUDE_ANALYZE_MODEL,
        "max_tokens": 1024,
        "temperature": 0,
        "messages": [
            {
                "role": "user",
                "content": (
                    "Return only the final analysis text with no preamble.\n\n"
                    "Instruction:\nSummarize this text in 2 sentences\n\n"
                    "Text:\nDigital Foreman is an execution control system that manages "
                    "tasks, validates them, and executes external tools through a deterministic "
                    "pipeline."
                ),
            }
        ],
    }


def test_run_claude_analyze_includes_context_in_request_payload(monkeypatch) -> None:
    captured_request: dict[str, object] = {}
    context = {
        "identity": {"name": "Alex Owner"},
        "memory_summary": {
            "architecture_rules": ["Keep deterministic routing"],
            "current_stage": "memory control",
            "last_decisions": ["Do not use DB memory"],
            "owner_priorities": ["EB1"],
        },
        "notes": "Prioritize permit deadlines.",
    }
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("CLAUDE_API_KEY", "test-claude-key")
    monkeypatch.setattr(
        "integrations.claude_tool._messages_response",
        lambda request_payload, api_key: (
            captured_request.update(
                {
                    "request_payload": dict(request_payload),
                    "api_key": api_key,
                }
            )
            or {
                "content": [
                    {
                        "type": "text",
                        "text": "Owner-aware summary.",
                    }
                ]
            }
        ),
    )

    result = run_claude_analyze(
        {
            "instruction": "Summarize this text in 2 sentences",
            "text": "Permit renewal status is pending city review.",
            "context": context,
        }
    )

    assert result == {
        "analysis": "Owner-aware summary.",
    }
    assert captured_request["api_key"] == "test-claude-key"
    assert captured_request["request_payload"] == {
        "model": CLAUDE_ANALYZE_MODEL,
        "max_tokens": 1024,
        "temperature": 0,
        "messages": [
            {
                "role": "user",
                "content": (
                    "Return only the final analysis text with no preamble.\n\n"
                    "Context:\n"
                    "{\n"
                    '  "identity": {\n'
                    '    "name": "Alex Owner"\n'
                    "  },\n"
                    '  "memory_summary": {\n'
                    '    "architecture_rules": [\n'
                    '      "Keep deterministic routing"\n'
                    "    ],\n"
                    '    "current_stage": "memory control",\n'
                    '    "last_decisions": [\n'
                    '      "Do not use DB memory"\n'
                    "    ],\n"
                    '    "owner_priorities": [\n'
                    '      "EB1"\n'
                    "    ]\n"
                    "  },\n"
                    '  "notes": "Prioritize permit deadlines."\n'
                    "}\n\n"
                    "Memory control rules:\n"
                    "- Respect memory_summary.current_stage.\n"
                    "- Respect memory_summary.architecture_rules.\n"
                    "- Do not contradict memory_summary.last_decisions.\n"
                    "- Align with memory_summary.owner_priorities when present.\n\n"
                    "Instruction:\nSummarize this text in 2 sentences\n\n"
                    "Text:\nPermit renewal status is pending city review."
                ),
            }
        ],
    }


def test_execute_tool_call_returns_standard_google_docs_result(monkeypatch) -> None:
    monkeypatch.setattr(
        "integrations.google_docs_tool.create_google_doc",
        lambda payload: {
            "doc_id": "doc-tool-123",
            "name": payload["title"],
            "url": "https://docs.google.com/document/d/doc-tool-123",
        },
    )

    result = execute_tool_call(
        {
            "tool_name": GOOGLE_DOCS_CREATE_DOCUMENT_TOOL,
            "input": {
                "title": "Kickoff Summary",
                "content": "Create the summary document.",
            },
        }
    )

    assert result["ok"] is True
    assert result["tool_name"] == GOOGLE_DOCS_CREATE_DOCUMENT_TOOL
    assert result["output"] == {
        "doc_id": "doc-tool-123",
        "url": "https://docs.google.com/document/d/doc-tool-123",
    }
    assert result["error"] is None
    assert result["source"] == "external"
    _assert_execution_observability(result)


def test_execute_tool_call_returns_standard_gemini_google_operator_result(monkeypatch) -> None:
    monkeypatch.setattr(
        "integrations.gemini_tool.call_gemini_google_operator",
        lambda task, context: {
            "result": {
                "email_subject": "Re: Q2 partner sync",
                "email_body": "Hi team, sharing the doc summary and next steps.",
                "doc_outline": [
                    "Summary",
                    "Decisions",
                    "Next steps",
                ],
            }
        },
    )

    result = execute_tool_call(
        {
            "tool_name": GEMINI_GOOGLE_OPERATOR_TOOL,
            "input": {
                "task": "Draft the client follow-up email and a Google Doc outline from the notes.",
                "context": {
                    "owner": "Anton Vorontsov",
                    "business": "Digital Foreman",
                    "product": "Execution control system",
                    "policies": [
                        "Do not send or publish automatically",
                        "Prefer concise owner-ready language",
                    ],
                },
            },
        }
    )

    assert result["ok"] is True
    assert result["tool_name"] == GEMINI_GOOGLE_OPERATOR_TOOL
    assert result["output"] == {
        "result": {
            "email_subject": "Re: Q2 partner sync",
            "email_body": "Hi team, sharing the doc summary and next steps.",
            "doc_outline": [
                "Summary",
                "Decisions",
                "Next steps",
            ],
        },
        "execution_trace": {
            "tool_source": "external",
            "model": "gemini",
        },
    }
    assert result["error"] is None
    assert result["source"] == "external"
    _assert_execution_observability(result)


def test_execute_tool_call_returns_standard_claude_web_operator_result(monkeypatch) -> None:
    monkeypatch.setattr(
        "integrations.claude_web_tool.run_claude_web_operator_external",
        lambda payload: {
            "result": {
                "channel": "linkedin",
                "action": "prepare_profile_update",
                "summary": "Refresh the headline, tighten the about section, and queue a draft post for manual review.",
            },
            "execution_trace": {
                "tool_source": "external",
                "model": "claude",
            },
        },
    )
    monkeypatch.setattr(
        "control.tool_registry._TOOL_REGISTRY",
        {
            **__import__("control.tool_registry", fromlist=["_TOOL_REGISTRY"])._TOOL_REGISTRY,
            CLAUDE_WEB_OPERATOR_TOOL: __import__(
                "integrations.claude_web_tool",
                fromlist=["run_claude_web_operator_external"],
            ).run_claude_web_operator_external,
        },
    )

    result = execute_tool_call(
        {
            "tool_name": CLAUDE_WEB_OPERATOR_TOOL,
            "input": {
                "task": "Update the LinkedIn founder profile plan and prepare the next web actions for manual review.",
                "context": {
                    "owner": "Anton Vorontsov",
                    "business": "Digital Foreman",
                    "constraints": [
                        "Manual review before publishing",
                        "Keep tone practical and specific",
                    ],
                },
            },
        }
    )

    assert result["ok"] is True
    assert result["tool_name"] == CLAUDE_WEB_OPERATOR_TOOL
    assert result["output"] == {
        "result": {
            "channel": "linkedin",
            "action": "prepare_profile_update",
            "summary": "Refresh the headline, tighten the about section, and queue a draft post for manual review.",
        },
        "execution_trace": {
            "tool_source": "external",
            "model": "claude",
        },
    }
    assert result["error"] is None
    assert result["source"] == "external"
    _assert_execution_observability(result)


def test_execute_tool_call_rejects_missing_google_drive_file_id() -> None:
    result = execute_tool_call(
        {
            "tool_name": GOOGLE_DRIVE_READ_FILE_TOOL,
            "input": {},
        }
    )

    assert result["ok"] is False
    assert result["tool_name"] == GOOGLE_DRIVE_READ_FILE_TOOL
    assert result["output"] is None
    assert result["error"] == {
        "code": "TOOL_INPUT_MISSING_FIELDS",
        "message": "Missing required tool input fields: file_id.",
    }
    _assert_execution_observability(result)


def test_execute_tool_call_rejects_missing_gmail_draft_body() -> None:
    result = execute_tool_call(
        {
            "tool_name": GMAIL_CREATE_DRAFT_TOOL,
            "input": {
                "subject": "Re: Client follow-up",
            },
        }
    )

    assert result["ok"] is False
    assert result["tool_name"] == GMAIL_CREATE_DRAFT_TOOL
    assert result["output"] is None
    assert result["error"] == {
        "code": "TOOL_INPUT_MISSING_FIELDS",
        "message": "Missing required tool input fields: body.",
    }
    _assert_execution_observability(result)


def test_execute_tool_call_rejects_missing_google_gmail_send_to() -> None:
    result = execute_tool_call(
        {
            "tool_name": GOOGLE_GMAIL_SEND_TOOL,
            "input": {
                "subject": "Re: Client follow-up",
                "body": "Thanks for the note.",
            },
        }
    )

    assert result["ok"] is False
    assert result["tool_name"] == GOOGLE_GMAIL_SEND_TOOL
    assert result["output"] is None
    assert result["error"] == {
        "code": "TOOL_INPUT_MISSING_FIELDS",
        "message": "Missing required tool input fields: to.",
    }
    _assert_execution_observability(result)


def test_execute_tool_call_rejects_missing_linkedin_topic() -> None:
    result = execute_tool_call(
        {
            "tool_name": LINKEDIN_CREATE_POST_DRAFT_TOOL,
            "input": {
                "context": "Digital Foreman founder update",
            },
        }
    )

    assert result["ok"] is False
    assert result["tool_name"] == LINKEDIN_CREATE_POST_DRAFT_TOOL
    assert result["output"] is None
    assert result["error"] == {
        "code": "TOOL_INPUT_MISSING_FIELDS",
        "message": "Missing required tool input fields: topic.",
    }
    _assert_execution_observability(result)


def test_execute_tool_call_returns_standard_linkedin_draft_result(monkeypatch) -> None:
    monkeypatch.setattr(
        "integrations.linkedin_tool.run_claude_analyze_external",
        lambda payload: {
            "analysis": json.dumps(
                {
                    "post_text": "Digital Foreman is becoming a real operating system for execution.",
                    "tone": "grounded and specific",
                    "intent": "share progress and invite feedback",
                }
            )
        },
    )

    result = execute_tool_call(
        {
            "tool_name": LINKEDIN_CREATE_POST_DRAFT_TOOL,
            "input": {
                "topic": "first DF system explanation",
                "context": "real progress update",
            },
        }
    )

    assert result["ok"] is True
    assert result["tool_name"] == LINKEDIN_CREATE_POST_DRAFT_TOOL
    assert result["output"] == {
        "post_text": "Digital Foreman is becoming a real operating system for execution.",
        "tone": "grounded and specific",
        "intent": "share progress and invite feedback",
    }
    assert result["error"] is None
    _assert_execution_observability(result)


def test_execute_tool_call_returns_standard_gmail_read_result(monkeypatch) -> None:
    monkeypatch.setattr(
        "integrations.gmail_tool.run_gmail_read_latest",
        lambda payload: {
            "subject": "Client follow-up",
            "sender": "Jamie Client <jamie@example.com>",
            "body_text": "Can you send a reply draft today?",
            "message_id": "msg-001",
            "thread_id": "thread-001",
            "reply_to_email": "jamie@example.com",
        },
    )
    monkeypatch.setattr(
        "control.tool_registry._TOOL_REGISTRY",
        {
            GMAIL_READ_LATEST_TOOL: __import__(
                "integrations.gmail_tool",
                fromlist=["run_gmail_read_latest"],
            ).run_gmail_read_latest,
        },
    )

    result = execute_tool_call(
        {
            "tool_name": GMAIL_READ_LATEST_TOOL,
            "input": {},
        }
    )

    assert result["ok"] is True
    assert result["tool_name"] == GMAIL_READ_LATEST_TOOL
    assert result["output"] == {
        "subject": "Client follow-up",
        "sender": "Jamie Client <jamie@example.com>",
        "body_text": "Can you send a reply draft today?",
        "message_id": "msg-001",
        "thread_id": "thread-001",
        "reply_to_email": "jamie@example.com",
    }
    assert result["error"] is None
    _assert_execution_observability(result)


def test_execute_tool_call_returns_standard_google_gmail_send_result(monkeypatch) -> None:
    monkeypatch.setattr(
        "integrations.gmail_tool.run_google_gmail_send_external",
        lambda payload: {
            "message_id": "msg-sent-001",
            "thread_id": "thread-sent-001",
            "to": payload["to"],
            "subject": payload["subject"],
            "email_sent": True,
            "mode": "real",
        },
    )
    monkeypatch.setattr(
        "control.tool_registry._TOOL_REGISTRY",
        {
            GOOGLE_GMAIL_SEND_TOOL: __import__(
                "integrations.gmail_tool",
                fromlist=["run_google_gmail_send_external"],
            ).run_google_gmail_send_external,
        },
    )

    result = execute_tool_call(
        {
            "tool_name": GOOGLE_GMAIL_SEND_TOOL,
            "input": {
                "to": "jamie@example.com",
                "subject": "Re: Client follow-up",
                "body": "Thanks for the follow-up.",
            },
        }
    )

    assert result["ok"] is True
    assert result["tool_name"] == GOOGLE_GMAIL_SEND_TOOL
    assert result["output"] == {
        "message_id": "msg-sent-001",
        "thread_id": "thread-sent-001",
        "to": "jamie@example.com",
        "subject": "Re: Client follow-up",
        "email_sent": True,
        "mode": "real",
    }
    assert result["error"] is None
    _assert_execution_observability(result)


def test_run_codex_task_executes_email_pipeline_successfully(tmp_path, monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_execute_tool_call(tool_call):
        calls.append(dict(tool_call))
        if tool_call["tool_name"] == GMAIL_READ_LATEST_TOOL:
            return {
                "ok": True,
                "tool_name": GMAIL_READ_LATEST_TOOL,
                "output": {
                    "subject": "Client follow-up",
                    "sender": "Jamie Client <jamie@example.com>",
                    "body_text": "Can you send a reply draft today?",
                    "reply_to_email": "jamie@example.com",
                },
                "error": None,
            }
        if tool_call["tool_name"] == CLAUDE_ANALYZE_TOOL:
            return {
                "ok": True,
                "tool_name": CLAUDE_ANALYZE_TOOL,
                "output": {
                    "analysis": "Reply draft:\nHi Jamie,\n\nThanks for the follow-up.\n\nBest,",
                },
                "error": None,
            }
        if tool_call["tool_name"] == GOOGLE_GMAIL_SEND_TOOL:
            return {
                "ok": True,
                "tool_name": GOOGLE_GMAIL_SEND_TOOL,
                "output": {
                    "message_id": "msg-sent-001",
                    "to": "jamie@example.com",
                    "subject": "Re: Client follow-up",
                    "email_sent": True,
                },
                "error": None,
            }
        raise AssertionError(f"unexpected tool call: {tool_call}")

    monkeypatch.setattr(run_codex_task_module, "execute_tool_call", fake_execute_tool_call)

    task, artifact_path = run_codex_task(
        {
            "task_id": 42,
            "instruction": "Run explicit Tool Layer v1 pipeline",
            "pipeline": [
                {
                    "tool_name": GMAIL_READ_LATEST_TOOL,
                    "input": {},
                    "output_key": "email_data",
                },
                {
                    "tool_name": CLAUDE_ANALYZE_TOOL,
                    "input": {
                        "instruction": "Summarize email and suggest reply",
                        "text": (
                            "Subject: {{email_data.subject}}\n"
                            "Sender: {{email_data.sender}}\n\n"
                            "Body:\n{{email_data.body_text}}"
                        ),
                    },
                    "output_key": "analysis_data",
                },
                {
                    "tool_name": GOOGLE_GMAIL_SEND_TOOL,
                    "input": {
                        "to": "{{email_data.reply_to_email}}",
                        "subject": "Re: {{email_data.subject}}",
                        "body": "{{analysis_data.analysis}}",
                    },
                },
            ],
        },
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert [call["tool_name"] for call in calls] == [
        GMAIL_READ_LATEST_TOOL,
        CLAUDE_ANALYZE_TOOL,
        GOOGLE_GMAIL_SEND_TOOL,
    ]
    assert calls[1]["input"] == {
        "instruction": "Summarize email and suggest reply",
        "text": (
            "Subject: Client follow-up\n"
            "Sender: Jamie Client <jamie@example.com>\n\n"
            "Body:\nCan you send a reply draft today?"
        ),
    }
    assert calls[2]["input"] == {
        "to": "jamie@example.com",
        "subject": "Re: Client follow-up",
        "body": "Reply draft:\nHi Jamie,\n\nThanks for the follow-up.\n\nBest,",
    }
    assert task["pipeline_trace"][-1]["tool_name"] == GOOGLE_GMAIL_SEND_TOOL
    assert artifact["final_output"] == {
        "message_id": "msg-sent-001",
        "to": "jamie@example.com",
        "subject": "Re: Client follow-up",
        "email_sent": True,
    }
    assert artifact["retry_info"]["total_retry_count"] == 0


def test_execute_tool_call_returns_standard_google_drive_result(monkeypatch) -> None:
    monkeypatch.setattr(
        "integrations.google_drive_tool.run_google_drive_read_file",
        lambda payload: {
            "file_id": payload["file_id"],
            "name": "Project Notes",
            "mime_type": "text/plain",
            "content_text": "Line one\nLine two",
        },
    )
    monkeypatch.setattr(
        "control.tool_registry._TOOL_REGISTRY",
        {
            GOOGLE_DOCS_CREATE_DOCUMENT_TOOL: resolve_tool_executor(
                GOOGLE_DOCS_CREATE_DOCUMENT_TOOL
            ),
            GOOGLE_DRIVE_READ_FILE_TOOL: __import__(
                "integrations.google_drive_tool",
                fromlist=["run_google_drive_read_file"],
            ).run_google_drive_read_file,
        },
    )

    result = execute_tool_call(
        {
            "tool_name": GOOGLE_DRIVE_READ_FILE_TOOL,
            "input": {
                "file_id": "file-live-001",
            },
        }
    )

    assert result["ok"] is True
    assert result["tool_name"] == GOOGLE_DRIVE_READ_FILE_TOOL
    assert result["output"] == {
        "file_id": "file-live-001",
        "name": "Project Notes",
        "mime_type": "text/plain",
        "content_text": "Line one\nLine two",
    }
    assert result["error"] is None
    assert result["source"] == "external"
    _assert_execution_observability(result)


def test_execute_tool_call_returns_drive_read_failed_for_invalid_file_id(monkeypatch) -> None:
    monkeypatch.setattr(
        "integrations.google_drive_tool.run_google_drive_read_file",
        lambda payload: (_ for _ in ()).throw(RuntimeError("File not found: bad-file-id")),
    )
    monkeypatch.setattr(
        "control.tool_registry._TOOL_REGISTRY",
        {
            GOOGLE_DOCS_CREATE_DOCUMENT_TOOL: resolve_tool_executor(
                GOOGLE_DOCS_CREATE_DOCUMENT_TOOL
            ),
            GOOGLE_DRIVE_READ_FILE_TOOL: __import__(
                "integrations.google_drive_tool",
                fromlist=["run_google_drive_read_file"],
            ).run_google_drive_read_file,
        },
    )

    result = execute_tool_call(
        {
            "tool_name": GOOGLE_DRIVE_READ_FILE_TOOL,
            "input": {
                "file_id": "bad-file-id",
            },
        }
    )

    assert result["ok"] is False
    assert result["tool_name"] == GOOGLE_DRIVE_READ_FILE_TOOL
    assert result["output"] is None
    assert result["error"] == {
        "code": "DRIVE_READ_FAILED",
        "message": "File not found: bad-file-id",
    }
    assert result["source"] == "external"
    _assert_execution_observability(result)


def test_execute_tool_call_returns_offline_claude_analysis_when_api_fails(monkeypatch) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("CLAUDE_API_KEY", "bad-key")
    monkeypatch.setattr(
        "integrations.claude_tool._messages_response",
        lambda request_payload, api_key: (_ for _ in ()).throw(
            ClaudeToolError("CLAUDE_API_FAILED", "invalid x-api-key")
        ),
    )

    result = execute_tool_call(
        {
            "tool_name": CLAUDE_ANALYZE_TOOL,
            "input": {
                "instruction": "Summarize this text in 2 sentences",
                "text": "Digital Foreman manages tasks.",
            },
        }
    )

    assert result["ok"] is True
    assert result["tool_name"] == CLAUDE_ANALYZE_TOOL
    assert result["output"] == {
        "analysis": "Digital Foreman manages tasks.",
    }
    assert result["error"] is None
    assert result["source"] == "fallback"
    assert result["retry_info"]["total_retry_count"] == 1
    _assert_execution_observability(result)


def test_run_claude_analyze_returns_structured_owner_fallback(monkeypatch) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setenv("CLAUDE_API_KEY", "bad-key")
    monkeypatch.setattr(
        "integrations.claude_tool._messages_response",
        lambda request_payload, api_key: (_ for _ in ()).throw(
            ClaudeToolError("CLAUDE_API_FAILED", "invalid x-api-key")
        ),
    )

    result = run_claude_analyze(
        {
            "instruction": (
                "You are assisting the owner:\n"
                "Anton Vorontsov.\n\n"
                "Focus:\n"
                "- immigration cases\n"
                "- business development\n"
                "- system growth\n\n"
                "Respond in a structured, actionable way.\n\n"
                "Use owner context to answer this request. "
                "Return sections exactly named: Action Steps, Priorities, Next Moves."
            ),
            "text": "what should I do next for EB1",
            "context": {
                "identity": {"name": "Anton Vorontsov"},
                "immigration": {"active_cases": ["EB1"]},
                "memory_summary": {
                    "architecture_rules": ["Keep deterministic routing"],
                    "current_stage": "evidence packaging",
                    "last_decisions": ["Do not use database storage"],
                    "owner_priorities": ["EB1"],
                },
                "notes": "Digital Foreman system used as proof base",
            },
        }
    )

    assert result["analysis"] == (
        "Action Steps:\n"
        "1. Make a gap list for the EB1 criteria you can credibly satisfy right now.\n"
        "2. Gather strongest proof of original work, system growth, business traction, and public recognition.\n"
        "3. Turn each proof item into a dated evidence packet with links, metrics, and a one-line significance note.\n"
        "4. Keep the next move aligned with the current stage: evidence packaging.\n\n"
        "Priorities:\n"
        "1. Immigration evidence and filing readiness.\n"
        "2. Concrete proof of business and technical impact.\n"
        "3. Preserve evidence trail in Digital Foreman for reuse.\n"
        "4. EB1.\n\n"
        "Next Moves:\n"
        "- Book a focused review of the current EB1 evidence set this week.\n"
        "- Draft the next 30-day evidence-building plan around publications, impact metrics, and reference letters.\n"
        "- Capture each result inside Digital Foreman as dated proof.\n"
        "- Keep the latest system decision intact: Do not use database storage."
    )


def test_run_codex_task_executes_pipeline_successfully(tmp_path, monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_execute_tool_call(tool_call):
        calls.append(dict(tool_call))
        if tool_call["tool_name"] == GOOGLE_DRIVE_READ_FILE_TOOL:
            return {
                "ok": True,
                "tool_name": GOOGLE_DRIVE_READ_FILE_TOOL,
                "output": {
                    "file_id": "file-live-001",
                    "name": "Project Notes",
                    "mime_type": "text/plain",
                    "content_text": "Source text",
                },
                "error": None,
            }
        if tool_call["tool_name"] == CLAUDE_ANALYZE_TOOL:
            return {
                "ok": True,
                "tool_name": CLAUDE_ANALYZE_TOOL,
                "output": {
                    "analysis": "Pipeline summary.",
                },
                "error": None,
            }
        if tool_call["tool_name"] == GOOGLE_DOCS_CREATE_DOCUMENT_TOOL:
            return {
                "ok": True,
                "tool_name": GOOGLE_DOCS_CREATE_DOCUMENT_TOOL,
                "output": {
                    "doc_id": "doc-pipeline-001",
                    "url": "https://docs.google.com/document/d/doc-pipeline-001",
                },
                "error": None,
            }
        raise AssertionError(f"unexpected tool call: {tool_call}")

    monkeypatch.setattr(run_codex_task_module, "execute_tool_call", fake_execute_tool_call)

    task, artifact_path = run_codex_task(
        {
            "task_id": 24,
            "instruction": "Run explicit Tool Layer v1 pipeline",
            "pipeline": [
                {
                    "tool_name": GOOGLE_DRIVE_READ_FILE_TOOL,
                    "input": {
                        "file_id": "file-live-001",
                    },
                    "output_key": "drive_data",
                },
                {
                    "tool_name": CLAUDE_ANALYZE_TOOL,
                    "input": {
                        "instruction": "Summarize this document in 3 sentences",
                        "text": "{{drive_data.content_text}}",
                    },
                    "output_key": "analysis_data",
                },
                {
                    "tool_name": GOOGLE_DOCS_CREATE_DOCUMENT_TOOL,
                    "input": {
                        "title": "DF PIPELINE RESULT",
                        "content": "{{analysis_data.analysis}}",
                    },
                },
            ],
        },
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert [call["tool_name"] for call in calls] == [
        GOOGLE_DRIVE_READ_FILE_TOOL,
        CLAUDE_ANALYZE_TOOL,
        GOOGLE_DOCS_CREATE_DOCUMENT_TOOL,
    ]
    assert calls[1]["input"] == {
        "instruction": "Summarize this document in 3 sentences",
        "text": "Source text",
    }
    assert calls[2]["input"] == {
        "title": "DF PIPELINE RESULT",
        "content": "Pipeline summary.",
    }
    assert artifact_path == tmp_path / "artifacts" / "pipeline-24.json"
    assert task["doc_id"] == "doc-pipeline-001"
    assert task["doc_url"] == "https://docs.google.com/document/d/doc-pipeline-001"
    assert [step["step_index"] for step in task["pipeline_trace"]] == [1, 2, 3]
    assert artifact["final_output"] == {
        "doc_id": "doc-pipeline-001",
        "url": "https://docs.google.com/document/d/doc-pipeline-001",
    }
    assert isinstance(artifact["execution_timeline"]["start_time"], str)
    assert isinstance(artifact["execution_timeline"]["end_time"], str)
    assert isinstance(artifact["execution_timeline"]["total_duration_ms"], int)
    assert artifact["pipeline_steps"] == [
        {
            "output_key": "drive_data",
            "step_index": 1,
            "tool_name": GOOGLE_DRIVE_READ_FILE_TOOL,
        },
        {
            "output_key": "analysis_data",
            "step_index": 2,
            "tool_name": CLAUDE_ANALYZE_TOOL,
        },
        {
            "step_index": 3,
            "tool_name": GOOGLE_DOCS_CREATE_DOCUMENT_TOOL,
        },
    ]
    assert len(artifact["step_metrics"]) == 3
    assert all(isinstance(step["step_duration_ms"], int) for step in artifact["step_metrics"])
    assert artifact["retry_info"]["total_retry_count"] == 0


def test_run_codex_task_pipeline_reports_missing_variable_reference(tmp_path, monkeypatch) -> None:
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        run_codex_task_module,
        "execute_tool_call",
        lambda tool_call: calls.append(dict(tool_call)) or {
            "ok": True,
            "tool_name": tool_call["tool_name"],
            "output": {},
            "error": None,
        },
    )

    _, artifact_path = run_codex_task(
        {
            "task_id": 24,
            "instruction": "Run explicit Tool Layer v1 pipeline",
            "pipeline": [
                {
                    "tool_name": CLAUDE_ANALYZE_TOOL,
                    "input": {
                        "instruction": "Summarize this document in 3 sentences",
                        "text": "{{drive_data.content_text}}",
                    },
                }
            ],
        },
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert calls == []
    assert artifact["reason"] == "Pipeline variable not found: drive_data.content_text"
    assert len(artifact["pipeline_trace"]) == 1
    assert artifact["pipeline_trace"][0]["success"] is False
    assert artifact["final_output"] == {}
    assert len(artifact["step_metrics"]) == 1


def test_run_codex_task_pipeline_propagates_tool_failure(tmp_path, monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def fake_execute_tool_call(tool_call):
        calls.append(dict(tool_call))
        if tool_call["tool_name"] == GOOGLE_DRIVE_READ_FILE_TOOL:
            return {
                "ok": True,
                "tool_name": GOOGLE_DRIVE_READ_FILE_TOOL,
                "output": {
                    "file_id": "file-live-001",
                    "name": "Project Notes",
                    "mime_type": "text/plain",
                    "content_text": "Source text",
                },
                "error": None,
            }
        if tool_call["tool_name"] == CLAUDE_ANALYZE_TOOL:
            return {
                "ok": False,
                "tool_name": CLAUDE_ANALYZE_TOOL,
                "output": None,
                "error": {
                    "code": "CLAUDE_API_FAILED",
                    "message": "anthropic unavailable",
                },
            }
        raise AssertionError("pipeline should stop before later steps")

    monkeypatch.setattr(run_codex_task_module, "execute_tool_call", fake_execute_tool_call)

    task, artifact_path = run_codex_task(
        {
            "task_id": 24,
            "instruction": "Run explicit Tool Layer v1 pipeline",
            "pipeline": [
                {
                    "tool_name": GOOGLE_DRIVE_READ_FILE_TOOL,
                    "input": {
                        "file_id": "file-live-001",
                    },
                    "output_key": "drive_data",
                },
                {
                    "tool_name": CLAUDE_ANALYZE_TOOL,
                    "input": {
                        "instruction": "Summarize this document in 3 sentences",
                        "text": "{{drive_data.content_text}}",
                    },
                },
                {
                    "tool_name": GOOGLE_DOCS_CREATE_DOCUMENT_TOOL,
                    "input": {
                        "title": "DF PIPELINE RESULT",
                        "content": "should-not-run",
                    },
                },
            ],
        },
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    artifact = json.loads(artifact_path.read_text(encoding="utf-8"))

    assert [call["tool_name"] for call in calls] == [
        GOOGLE_DRIVE_READ_FILE_TOOL,
        CLAUDE_ANALYZE_TOOL,
    ]
    assert task["pipeline_trace"][-1]["tool_error_code"] == "CLAUDE_API_FAILED"
    assert artifact["reason"] == "anthropic unavailable"
    assert artifact["final_output"] == {}
    assert artifact["step_metrics"][-1]["failure_reason"] == "anthropic unavailable"


def test_execute_tool_call_records_network_diagnostics_for_network_failure(monkeypatch) -> None:
    monkeypatch.setattr(
        "control.tool_registry._TOOL_REGISTRY",
        {
            GOOGLE_DRIVE_READ_FILE_TOOL: lambda payload: (_ for _ in ()).throw(
                TimeoutError("request timed out")
            ),
        },
    )

    result = execute_tool_call(
        {
            "tool_name": GOOGLE_DRIVE_READ_FILE_TOOL,
            "input": {
                "file_id": "file-timeout-001",
            },
        }
    )

    assert result["ok"] is False
    assert result["network_diagnostics"]["connection_attempted"] is True
    assert result["network_diagnostics"]["failure_type"] == "timeout"
    assert result["network_diagnostics"]["retry_attempts"] == 0


def test_run_codex_task_pipeline_creates_execution_log(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        run_codex_task_module,
        "execute_tool_call",
        lambda tool_call: {
            "ok": True,
            "tool_name": tool_call["tool_name"],
            "output": {"echo": tool_call["tool_name"]},
            "error": None,
        },
    )

    _, artifact_path = run_codex_task(
        {
            "task_id": 88,
            "instruction": "Write observability log",
            "pipeline": [
                {
                    "tool_name": GOOGLE_DRIVE_READ_FILE_TOOL,
                    "input": {"file_id": "file-001"},
                    "output_key": "drive_data",
                }
            ],
        },
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    log_path = tmp_path / "logs" / "execution.log"
    assert artifact_path.exists()
    assert log_path.exists()
    entries = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines()]
    assert len(entries) == 1
    assert entries[0]["command"] == "Write observability log"
    assert entries[0]["status"] == "success"
    assert isinstance(entries[0]["duration"], int)
    assert len(entries[0]["steps"]) == 1
    assert isinstance(entries[0]["steps"][0]["step_duration_ms"], int)


def test_run_codex_task_debug_mode_prints_step_timing(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        run_codex_task_module,
        "execute_tool_call",
        lambda tool_call: {
            "ok": True,
            "tool_name": tool_call["tool_name"],
            "output": {"echo": tool_call["tool_name"]},
            "error": None,
        },
    )

    run_codex_task(
        {
            "task_id": 89,
            "instruction": "Print debug trace",
            "pipeline": [
                {
                    "tool_name": GOOGLE_DRIVE_READ_FILE_TOOL,
                    "input": {"file_id": "file-002"},
                }
            ],
        },
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
        debug=True,
    )

    output = capsys.readouterr().out
    assert "DEBUG: pipeline step 1 start tool=google_drive.read_file retries=0" in output
    assert "DEBUG: pipeline step 1 finish tool=google_drive.read_file status=success" in output
    assert "duration_ms=" in output
