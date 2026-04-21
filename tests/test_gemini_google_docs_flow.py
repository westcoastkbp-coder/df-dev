from __future__ import annotations

from integrations.gemini_tool import run_gemini_google_operator_external


def test_gemini_google_operator_creates_real_doc_via_tool(monkeypatch) -> None:
    monkeypatch.setattr(
        "integrations.gemini_tool.call_gemini_google_operator",
        lambda task, context: {
            "result": {
                "document": {
                    "title": "DF TEST DOC",
                    "content": "Execution OS test",
                }
            }
        },
    )
    monkeypatch.setattr(
        "control.tool_executor.execute_tool",
        lambda tool_name, payload: {
            "status": "success",
            "data": {
                "result": {
                    "doc_url": "https://docs.google.com/document/d/doc-flow-001/edit",
                },
                "execution_trace": {
                    "tool_source": "external",
                    "model": "google_api",
                },
            },
            "error": None,
            "source": "external",
        },
    )

    result = run_gemini_google_operator_external(
        {
            "task": "Create a Google Doc for the execution summary.",
            "context": {
                "owner": "Anton Vorontsov",
                "business": "Digital Foreman",
                "product": "Execution control system",
                "policies": ["Return JSON only"],
            },
        }
    )

    assert result == {
        "result": {
            "doc_url": "https://docs.google.com/document/d/doc-flow-001/edit",
        },
        "execution_trace": {
            "tool_source": "external",
            "model": "google_api",
        },
    }
