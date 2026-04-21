from __future__ import annotations

import pytest

from control import gemini_executor


class _FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code
        self.ok = 200 <= status_code < 300

    def json(self) -> dict:
        return self._payload


def test_post_gemini_prompt_uses_current_request_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_request: dict[str, object] = {}
    monkeypatch.setattr(
        "control.gemini_executor._load_gemini_api_key", lambda: "test-key"
    )
    monkeypatch.setattr(
        "control.gemini_executor._load_gemini_model", lambda: "gemini-2.5-flash"
    )

    def _fake_post(
        url: str, *, headers: dict, json: dict, timeout: int
    ) -> _FakeResponse:
        captured_request["url"] = url
        captured_request["headers"] = headers
        captured_request["json"] = json
        captured_request["timeout"] = timeout
        return _FakeResponse(
            {
                "candidates": [
                    {
                        "content": {
                            "parts": [
                                {
                                    "text": '{"result":{"status":"ok"}}',
                                }
                            ]
                        }
                    }
                ]
            }
        )

    monkeypatch.setattr("control.gemini_executor.requests.post", _fake_post)

    result = gemini_executor._post_gemini_prompt("Task: ping")

    assert result == {"result": {"status": "ok"}}
    assert captured_request == {
        "url": "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent",
        "headers": {
            "Content-Type": "application/json",
            "x-goog-api-key": "test-key",
        },
        "json": {
            "contents": [
                {
                    "parts": [
                        {
                            "text": "Task: ping",
                        }
                    ]
                }
            ]
        },
        "timeout": 30,
    }


def test_post_gemini_prompt_surfaces_api_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "control.gemini_executor._load_gemini_api_key", lambda: "test-key"
    )
    monkeypatch.setattr(
        "control.gemini_executor._load_gemini_model", lambda: "gemini-2.5-flash"
    )
    monkeypatch.setattr(
        "control.gemini_executor.requests.post",
        lambda *args, **kwargs: _FakeResponse(
            {
                "error": {
                    "code": 404,
                    "status": "NOT_FOUND",
                    "message": "model not found",
                }
            },
            status_code=404,
        ),
    )

    with pytest.raises(
        ValueError, match="Gemini API error: code=404 \\| NOT_FOUND \\| model not found"
    ):
        gemini_executor._post_gemini_prompt("Task: ping")


def test_extract_candidate_text_joins_text_parts() -> None:
    text = gemini_executor._extract_candidate_text(
        {
            "candidates": [
                {
                    "content": {
                        "parts": [
                            {"text": '{"result":'},
                            {"text": '{"status":"ok"}}'},
                        ]
                    }
                }
            ]
        }
    )

    assert text == '{"result":\n{"status":"ok"}}'
