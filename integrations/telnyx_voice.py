from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

TELNYX_API_BASE = "https://api.telnyx.com/v2"


def _env(name: str) -> str:
    return str(os.getenv(name, "")).strip()


def telnyx_api_key() -> str:
    return _env("TELNYX_API_KEY")


def telnyx_stream_url() -> str:
    return _env("TELNYX_STREAM_URL")


def telnyx_voice_name() -> str:
    return _env("TELNYX_SPEAK_VOICE") or "female"


def telnyx_live_commands_enabled() -> bool:
    return bool(telnyx_api_key())


def _command_result(
    *,
    ok: bool,
    command: str,
    mode: str,
    payload: dict[str, Any],
    status_code: int = 0,
    response: dict[str, Any] | None = None,
    error: str = "",
) -> dict[str, Any]:
    return {
        "ok": ok,
        "command": command,
        "mode": mode,
        "status_code": int(status_code),
        "payload": dict(payload),
        "response": dict(response or {}),
        "error": str(error).strip(),
    }


def _post_command(
    *,
    path: str,
    command: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    api_key = telnyx_api_key()
    if not api_key:
        return _command_result(
            ok=True,
            command=command,
            mode="mock",
            payload=payload,
            response={"mock": True},
        )

    request = urllib.request.Request(
        url=f"{TELNYX_API_BASE}{path}",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5.0) as response:
            raw_body = response.read().decode("utf-8") or "{}"
            body = json.loads(raw_body)
            return _command_result(
                ok=200 <= response.status < 300,
                command=command,
                mode="live",
                payload=payload,
                status_code=response.status,
                response=body if isinstance(body, dict) else {"raw": raw_body},
            )
    except urllib.error.HTTPError as exc:
        raw_body = exc.read().decode("utf-8") if exc.fp is not None else ""
        return _command_result(
            ok=False,
            command=command,
            mode="live",
            payload=payload,
            status_code=exc.code,
            response={"raw": raw_body} if raw_body else {},
            error=raw_body or str(exc),
        )
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        return _command_result(
            ok=False,
            command=command,
            mode="live",
            payload=payload,
            error=str(exc),
        )


def answer_call(
    *,
    call_control_id: str,
    client_state: str = "",
    command_id: str = "",
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if client_state:
        payload["client_state"] = client_state
    if command_id:
        payload["command_id"] = command_id
    return _post_command(
        path=f"/calls/{call_control_id}/actions/answer",
        command="answer",
        payload=payload,
    )


def start_streaming(
    *,
    call_control_id: str,
    stream_url: str,
    stream_track: str = "both_tracks",
    command_id: str = "",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "stream_url": stream_url,
        "stream_track": stream_track,
    }
    if command_id:
        payload["command_id"] = command_id
    return _post_command(
        path=f"/calls/{call_control_id}/actions/streaming_start",
        command="streaming_start",
        payload=payload,
    )


def speak_text(
    *,
    call_control_id: str,
    text: str,
    voice: str = "",
    language: str = "en-US",
    command_id: str = "",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "payload": str(text).strip(),
        "voice": voice or telnyx_voice_name(),
        "language": language,
    }
    if command_id:
        payload["command_id"] = command_id
    return _post_command(
        path=f"/calls/{call_control_id}/actions/speak",
        command="speak",
        payload=payload,
    )
