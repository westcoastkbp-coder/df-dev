from __future__ import annotations

from control.tool_executor import execute_tool_call
from control.tool_registry import GOOGLE_DOCS_CREATE_TOOL


def test_execute_tool_call_returns_google_docs_create_contract(monkeypatch) -> None:
    monkeypatch.setattr(
        "integrations.google_docs_tool.create_google_doc",
        lambda payload: {
            "doc_id": "doc-real-001",
            "name": payload["title"],
            "url": "https://docs.google.com/document/d/doc-real-001/edit",
        },
    )

    result = execute_tool_call(
        {
            "tool_name": GOOGLE_DOCS_CREATE_TOOL,
            "input": {
                "title": "DF TEST DOC",
                "content": "Execution OS test",
            },
        }
    )

    assert result["ok"] is True
    assert result["tool_name"] == GOOGLE_DOCS_CREATE_TOOL
    assert result["output"] == {
        "result": {
            "doc_url": "https://docs.google.com/document/d/doc-real-001/edit",
        },
        "execution_trace": {
            "tool_source": "external",
            "model": "google_api",
        },
    }
    assert result["error"] is None
    assert result["source"] == "external"
