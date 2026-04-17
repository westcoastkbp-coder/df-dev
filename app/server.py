import uuid
import os
from pathlib import Path
from urllib.parse import parse_qsl

from fastapi import FastAPI, Request
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles

from app.voice.twilio_compat import create_execution_core_app, handle_incoming_event
from app.web.site import contact_page, home_page

ROOT_DIR = Path(__file__).resolve().parents[1]
STATIC_DIR = ROOT_DIR / "app" / "web" / "static"
MODE = str(os.getenv("MODE", "product")).strip().lower() or "product"

execution_app = create_execution_core_app()
app = FastAPI()
CALL_STATE: dict[str, dict[str, str | int]] = {}

GREETING_TEXT = "Hello, this is the office. How can I help you today?"
INTAKE_REPEAT_TEXT = "Sorry, I didn't catch that. Could you repeat?"
GOODBYE_TEXT = "Alright, feel free to call back anytime. Goodbye."
CONFIRM_YES_TEXT = "Perfect. I've recorded your request."
CLOSING_TEXT = "Perfect. Your request is recorded. Our team will contact you shortly."
CONFIRM_PROMPT_TEXT = "Just to confirm - you're looking for an estimate for your project, right?"
CONFIRM_RETRY_TEXT = "Sorry, I didn't catch a clear yes. Please say yes to confirm."
CONFIRM_WORDS = {"yes", "yeah", "yep", "correct", "right", "sure", "okay"}


def _debug_log(*parts: object) -> None:
    if MODE != "product":
        print(*parts)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
def website_home() -> Response:
    return home_page()


@app.get("/contact")
def website_contact() -> Response:
    return contact_page()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def _get_call_state(session_id: str) -> dict[str, str | int]:
    return CALL_STATE.setdefault(
        session_id,
        {
            "stage": "greeting",
            "message": "",
            "missed": 0,
        },
    )


def _set_stage(state: dict[str, str | int], stage: str) -> None:
    state["stage"] = stage


def _reset_missed(state: dict[str, str | int]) -> None:
    state["missed"] = 0


def _increment_missed(state: dict[str, str | int]) -> int:
    next_value = int(state.get("missed", 0) or 0) + 1
    state["missed"] = next_value
    return next_value


def _twiml_gather(*, message: str, action: str) -> Response:
    twiml = f"""
<Response>
    <Gather input="speech" action="{action}" method="POST" speechTimeout="auto">
        <Say>{message}</Say>
    </Gather>
    <Say>{GOODBYE_TEXT}</Say>
</Response>
"""
    return Response(content=twiml, media_type="application/xml")


def _twiml_say_and_hangup(*, lines: list[str]) -> Response:
    joined_lines = "".join(f"<Say>{line}</Say>" for line in lines if line.strip())
    twiml = f"""
<Response>
    {joined_lines}
    <Hangup/>
</Response>
"""
    return Response(content=twiml, media_type="application/xml")


def _normalized_payload(data: dict[str, object], session_id: str) -> dict[str, str]:
    return {
        "message": str(data.get("SpeechResult") or data.get("Body") or "").strip(),
        "user_id": str(data.get("From") or "twilio_user").strip() or "twilio_user",
        "channel": "phone",
        "user_role": "foreman",
        "session_id": session_id,
    }


@app.post("/test-webhook")
async def test_webhook(request: Request):
    content_type = str(request.headers.get("content-type", "")).lower()
    if "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type:
        raw_body = (await request.body()).decode("utf-8", errors="replace")
        data = {str(key): str(value) for key, value in parse_qsl(raw_body, keep_blank_values=True)}
    else:
        try:
            data = await request.json()
        except Exception:
            data = {}

    session_id = str(request.query_params.get("session_id", "")).strip() or f"voice-{uuid.uuid4().hex[:12]}"
    state = _get_call_state(session_id)
    stage = str(state.get("stage", "greeting")).strip() or "greeting"
    action = f"/test-webhook?session_id={session_id}"
    normalized = _normalized_payload(data, session_id)
    speech = str(data.get("SpeechResult", "")).strip().lower()
    message = str(normalized.get("message", "")).strip()
    is_confirmation_reply = any(word in speech for word in CONFIRM_WORDS)

    _debug_log("CALL STAGE:", stage)
    _debug_log("TWILIO SPEECH:", speech)
    _debug_log("TWILIO RAW:", data)
    _debug_log("NORMALIZED:", normalized)

    # Minimal interrupt-safe behavior: if speech arrives during the greeting stage,
    # treat it as intake instead of forcing the caller through another prompt.
    if stage == "greeting" and message:
        stage = "intake"
        _set_stage(state, "intake")

    if stage == "greeting":
        _set_stage(state, "intake")
        _reset_missed(state)
        return _twiml_gather(message=GREETING_TEXT, action=action)

    if stage == "intake":
        if not message:
            if _increment_missed(state) >= 2:
                _set_stage(state, "done")
                return _twiml_say_and_hangup(lines=[GOODBYE_TEXT])
            return _twiml_gather(message=INTAKE_REPEAT_TEXT, action=action)

        state["message"] = message
        _set_stage(state, "confirmation")
        _reset_missed(state)
        return _twiml_gather(
            message=CONFIRM_PROMPT_TEXT,
            action=action,
        )

    if stage == "confirmation":
        if is_confirmation_reply:
            normalized["message"] = "yes"
            normalized["intent"] = "confirmation"
            _debug_log("FORCED CONFIRM:", normalized)
            original_message = str(state.get("message", "")).strip()
            initial_result = await handle_incoming_event(
                {
                    "message": original_message,
                    "user_id": normalized["user_id"],
                    "channel": "phone",
                    "user_role": normalized["user_role"],
                    "session_id": session_id,
                }
            )
            _debug_log("EXECUTION RESULT:", initial_result)
            confirm_result = await handle_incoming_event(
                {
                    "message": "yes",
                    "user_id": normalized["user_id"],
                    "channel": "phone",
                    "user_role": normalized["user_role"],
                    "session_id": session_id,
                }
            )
            _debug_log("EXECUTION RESULT:", confirm_result)
            _set_stage(state, "done")
            _reset_missed(state)
            return _twiml_say_and_hangup(
                lines=[
                    CLOSING_TEXT,
                ]
            )

        if not message:
            if _increment_missed(state) >= 2:
                _set_stage(state, "done")
                return _twiml_say_and_hangup(lines=[GOODBYE_TEXT])
            return _twiml_gather(message=INTAKE_REPEAT_TEXT, action=action)

        if _increment_missed(state) >= 2:
            _set_stage(state, "done")
            return _twiml_say_and_hangup(lines=[GOODBYE_TEXT])
        return _twiml_gather(message=CONFIRM_RETRY_TEXT, action=action)

    return _twiml_say_and_hangup(lines=[GOODBYE_TEXT])


app.mount("", execution_app)
