from __future__ import annotations

import json
import logging
import os
import time
import uuid
from urllib.parse import parse_qsl, urlencode
from xml.sax.saxutils import escape

import httpx
from fastapi import FastAPI, Request, Response

from app.voice.app import app as core_app
from app.execution.rbac import issue_principal_token


logger = logging.getLogger("digital_foreman.twilio")
TWILIO_CALL_CONTROL_WEBHOOK_PATH = "/api/telnyx/call-control/webhook"
TWILIO_CONNECTED_MESSAGE = (
    "Digital Foreman connected. Please say your request after the tone."
)
TWILIO_FALLBACK_MESSAGE = (
    "I could not complete that request. Please try again in a moment."
)
EXECUTION_CORE_HOST = str(os.getenv("VOICE_EDGE_BIND_HOST", "")).strip() or "0.0.0.0"
EXECUTION_CORE_PORT = int(str(os.getenv("VOICE_EDGE_BIND_PORT", "")).strip() or "8080")
VOICE_PUBLIC_WEBHOOK_BASE_URL = str(
    os.getenv("VOICE_EDGE_PUBLIC_WEBHOOK_BASE_URL", "")
).strip()
TWILIO_BACKEND_ROLE = "foreman"
TWILIO_USER_ROLE = "viewer"


def _env(name: str) -> str:
    return str(os.getenv(name, "")).strip()


def _utc_now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _twiml_response_xml(*parts: str) -> str:
    return (
        '<?xml version="1.0" encoding="UTF-8"?><Response>'
        + "".join(parts)
        + "</Response>"
    )


def _twiml_say(message: str) -> str:
    return f"<Say>{escape(str(message).strip())}</Say>"


def _twiml_gather(*, prompt: str, action_url: str) -> str:
    escaped_action = escape(action_url)
    return (
        f'<Gather input="speech" method="POST" action="{escaped_action}" '
        'speechTimeout="auto">'
        f"{_twiml_say(prompt)}"
        "</Gather>"
    )


def _twiml_hangup() -> str:
    return "<Hangup/>"


def _is_twilio_voice_webhook(*, content_type: str) -> bool:
    normalized_content_type = str(content_type or "").strip().lower()
    return "application/x-www-form-urlencoded" in normalized_content_type


def _request_base_url(request: Request) -> str:
    public_base = VOICE_PUBLIC_WEBHOOK_BASE_URL.rstrip("/")
    if public_base:
        return public_base
    return str(request.base_url).rstrip("/")


def _action_url(
    request: Request,
    *,
    interaction_id: str,
    trace_id: str,
    session_id: str = "",
) -> str:
    params = {
        "interaction_id": interaction_id,
        "trace_id": trace_id,
    }
    if session_id:
        params["session_id"] = session_id
    return f"{_request_base_url(request)}{TWILIO_CALL_CONTROL_WEBHOOK_PATH}?{urlencode(params)}"


def _backend_headers(*, interaction_id: str, trace_id: str) -> dict[str, str]:
    headers = {
        "x-df-interaction-id": interaction_id,
        "x-df-trace-id": trace_id,
    }
    secret = _env("DIGITAL_FOREMAN_RBAC_SECRET")
    if not secret:
        return headers
    token = issue_principal_token(
        actor_id=f"voice:{interaction_id}",
        role=TWILIO_BACKEND_ROLE,
        expires_at=int(time.time()) + 300,
        secret=secret,
    )
    headers["authorization"] = f"Bearer {token}"
    return headers


async def _dispatch_backend_input(
    *,
    payload: dict[str, object],
    interaction_id: str,
    trace_id: str,
) -> tuple[int, dict[str, object]]:
    transport = httpx.ASGITransport(app=core_app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://execution-core.internal",
        timeout=15.0,
    ) as client:
        response = await client.post(
            "/input",
            json=payload,
            headers=_backend_headers(
                interaction_id=interaction_id,
                trace_id=trace_id,
            ),
        )
    try:
        body = response.json()
    except json.JSONDecodeError:
        body = {"raw": response.text}
    return response.status_code, body if isinstance(body, dict) else {"body": body}


def _adapt_incoming_event_payload(payload: dict[str, object]) -> dict[str, object]:
    message = str(payload.get("message", "")).strip()
    user_id = (
        str(payload.get("user_id", "")).strip() or f"voice:{uuid.uuid4().hex[:12]}"
    )
    adapted_payload: dict[str, object] = {
        "message": message,
        "user_id": user_id,
        "channel": "phone",
        "contact_id": user_id,
    }
    session_id = str(payload.get("session_id", "")).strip()
    if session_id:
        adapted_payload["session_id"] = session_id
    user_role = str(payload.get("user_role", "")).strip()
    if user_role:
        adapted_payload["user_role"] = user_role
    return adapted_payload


async def handle_incoming_event(payload: dict) -> dict[str, object]:
    print("EXECUTION START:", payload)
    execution_payload = _adapt_incoming_event_payload(payload)
    interaction_id = (
        str(payload.get("interaction_id", "")).strip()
        or f"test-{uuid.uuid4().hex[:12]}"
    )
    trace_id = (
        str(payload.get("trace_id", "")).strip()
        or f"test-trace-{uuid.uuid4().hex[:12]}"
    )
    status_code, backend_body = await _dispatch_backend_input(
        payload=execution_payload,
        interaction_id=interaction_id,
        trace_id=trace_id,
    )
    return {
        "status_code": status_code,
        "payload": execution_payload,
        "body": backend_body,
    }


def _backend_response_text(body: dict[str, object]) -> str:
    confirmation_prompt = str(body.get("confirmation_prompt", "")).strip()
    if confirmation_prompt:
        return confirmation_prompt
    message = str(body.get("message", "")).strip()
    if message:
        return message
    user_summary = str(body.get("user_summary", "")).strip()
    if user_summary:
        return user_summary
    task_id = str(body.get("task_id", "")).strip()
    if bool(body.get("task_created")) and task_id:
        return f"Okay. Task {task_id} has been created."
    status = str(body.get("status", "")).strip()
    if status:
        return f"Status: {status}."
    detail = str(body.get("detail", "")).strip()
    if detail:
        return detail
    return TWILIO_FALLBACK_MESSAGE


async def _twilio_voice_loop(request: Request) -> Response:
    raw_body = (await request.body()).decode("utf-8", errors="replace")
    raw_form = {
        str(key): str(value)
        for key, value in parse_qsl(raw_body, keep_blank_values=True)
    }
    call_sid = (
        str(raw_form.get("CallSid", "")).strip() or f"call-{uuid.uuid4().hex[:12]}"
    )
    from_number = str(raw_form.get("From", "")).strip() or call_sid
    speech_result = str(raw_form.get("SpeechResult", "")).strip()
    session_id = str(request.query_params.get("session_id", "")).strip()
    interaction_id = (
        str(request.query_params.get("interaction_id", "")).strip()
        or f"twilio-{call_sid}"
    )
    trace_id = (
        str(request.query_params.get("trace_id", "")).strip()
        or f"twilio-trace-{call_sid}"
    )

    logger.info(
        "TWILIO_INBOUND %s",
        json.dumps(
            {
                "interaction_id": interaction_id,
                "trace_id": trace_id,
                "path": request.url.path,
                "query": dict(request.query_params),
                "form": raw_form,
            },
            ensure_ascii=False,
        ),
    )

    if not speech_result:
        return Response(
            content=_twiml_response_xml(
                _twiml_gather(
                    prompt=TWILIO_CONNECTED_MESSAGE,
                    action_url=_action_url(
                        request,
                        interaction_id=interaction_id,
                        trace_id=trace_id,
                    ),
                ),
                _twiml_say("I did not catch that."),
                _twiml_hangup(),
            ),
            media_type="text/xml",
        )

    payload = {
        "message": speech_result,
        "user_id": f"voice:{from_number}",
        "user_role": TWILIO_USER_ROLE,
        "channel": "phone",
        "contact_id": from_number,
        "session_id": session_id,
        "interaction_id": interaction_id,
        "trace_id": trace_id,
    }
    print("VOICE WEBHOOK HIT:", payload)
    execution_result = await handle_incoming_event(payload)
    status_code = int(execution_result.get("status_code", 500))
    backend_body = execution_result.get("body", {})
    if not isinstance(backend_body, dict):
        backend_body = {"body": backend_body}
    logger.info(
        "VOICE_BACKEND_RESPONSE %s",
        json.dumps(
            {
                "interaction_id": interaction_id,
                "trace_id": trace_id,
                "status_code": status_code,
                "body": backend_body,
            },
            ensure_ascii=False,
        ),
    )

    if status_code != 200:
        return Response(
            content=_twiml_response_xml(
                _twiml_say(_backend_response_text(backend_body)),
                _twiml_hangup(),
            ),
            media_type="text/xml",
        )

    response_text = _backend_response_text(backend_body)
    next_session_id = str(backend_body.get("session_id", "")).strip() or session_id
    if bool(backend_body.get("confirmation_required")):
        return Response(
            content=_twiml_response_xml(
                _twiml_gather(
                    prompt=response_text,
                    action_url=_action_url(
                        request,
                        interaction_id=interaction_id,
                        trace_id=trace_id,
                        session_id=next_session_id,
                    ),
                ),
                _twiml_say("Please say yes or no."),
                _twiml_hangup(),
            ),
            media_type="text/xml",
        )

    return Response(
        content=_twiml_response_xml(
            _twiml_say(response_text),
            _twiml_hangup(),
        ),
        media_type="text/xml",
    )


def create_execution_core_app() -> FastAPI:
    adapter_app = FastAPI()

    @adapter_app.get("/")
    def execution_core_health() -> dict[str, str]:
        return {"status": "ok"}

    @adapter_app.get("/health")
    def execution_core_healthcheck() -> dict[str, object]:
        return {
            "status": "ok",
            "service": "execution-core",
            "host": EXECUTION_CORE_HOST,
            "port": EXECUTION_CORE_PORT,
            "public_webhook_base_url": VOICE_PUBLIC_WEBHOOK_BASE_URL,
        }

    @adapter_app.on_event("startup")
    async def execution_core_startup_log() -> None:
        logger.info(
            "execution-core server started on %s:%s",
            EXECUTION_CORE_HOST,
            EXECUTION_CORE_PORT,
        )
        logger.info("execution-core health endpoint ready at /health")
        if VOICE_PUBLIC_WEBHOOK_BASE_URL:
            logger.info(
                "twilio webhook base configured as %s",
                VOICE_PUBLIC_WEBHOOK_BASE_URL,
            )

    async def _twilio_voice_webhook_response(request: Request):
        if not _is_twilio_voice_webhook(
            content_type=str(request.headers.get("content-type", "")),
        ):
            return Response(
                content=_twiml_response_xml(
                    _twiml_say(TWILIO_FALLBACK_MESSAGE),
                    _twiml_hangup(),
                ),
                media_type="text/xml",
                status_code=415,
            )
        return await _twilio_voice_loop(request)

    @adapter_app.post("/")
    async def twilio_voice_webhook_root(request: Request):
        return await _twilio_voice_webhook_response(request)

    @adapter_app.post(TWILIO_CALL_CONTROL_WEBHOOK_PATH)
    async def twilio_voice_webhook(request: Request):
        return await _twilio_voice_webhook_response(request)

    adapter_app.mount("", core_app)
    return adapter_app
