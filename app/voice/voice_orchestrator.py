from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from app.execution.action_contract import build_action_contract
from app.execution.action_dispatcher import dispatch_action
from app.execution.input_normalizer import normalize_input
from app.ownerbox.domain import normalize_ownerbox_domain_binding
from app.voice.voice_session import (
    ResponsePlan,
    VoiceSession,
    VoiceTurn,
    create_response_plan,
    create_voice_session,
    create_voice_trace_metadata,
    create_voice_turn,
    update_voice_session,
)


DEFAULT_VOICE_MODEL = "gpt-5-mini"
DEFAULT_MAX_TOKENS = 240
DEFAULT_TEMPERATURE = 0.2
DEFAULT_TARGET_REF = "openai"
DEFAULT_TARGET_TYPE = "adapter"
DEFAULT_REQUESTED_BY = "voice_layer_v1"
DEFAULT_CHANNEL_TYPE = "phone"


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _collapse_whitespace(value: object) -> str:
    return " ".join(_normalize_text(value).split())


def _mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _result_payload_text(action_result: Mapping[str, object]) -> str:
    payload = _mapping(action_result.get("payload"))
    text = _collapse_whitespace(payload.get("text"))
    if text:
        return text
    note = _collapse_whitespace(payload.get("note"))
    if note:
        return note
    error_message = _collapse_whitespace(action_result.get("error_message"))
    if error_message:
        return f"Voice request could not be completed: {error_message}"
    return "Voice request completed without a spoken payload."


def _turn_status_from_result_status(result_status: str) -> str:
    normalized = _normalize_text(result_status).lower()
    if normalized == "success":
        return "completed"
    if normalized == "blocked":
        return "blocked"
    return "failed"


def _response_type_from_result_status(result_status: str) -> str:
    normalized = _normalize_text(result_status).lower()
    if normalized == "success":
        return "spoken_text"
    if normalized == "blocked":
        return "input_error"
    return "error"


def _response_metadata(
    *,
    session: VoiceSession,
    turn_id: str,
    detected_language: str,
    action_contract: Mapping[str, object] | None,
    action_result: Mapping[str, object] | None,
    started_at: str,
) -> dict[str, object]:
    action = _mapping(action_contract)
    result = _mapping(action_result)
    payload = _mapping(result.get("payload"))
    result_metadata = _mapping(payload.get("metadata"))
    return {
        "session_id": session.session_id,
        "turn_id": turn_id,
        "caller_id": session.caller_id,
        "detected_language": detected_language,
        "action_id": _normalize_text(action.get("action_id")) or None,
        "result_status": _normalize_text(result.get("status")) or "blocked",
        "result_type": _normalize_text(result.get("result_type")) or "voice_output",
        "started_at": started_at,
        "completed_at": _normalize_text(result.get("timestamp")) or started_at,
        "latency_ms": int(result_metadata.get("dispatcher_latency_ms") or 0),
    }


def _dispatcher_supports_kwarg(
    dispatcher: Callable[..., dict[str, object]], name: str
) -> bool:
    try:
        signature = inspect.signature(dispatcher)
    except (TypeError, ValueError):
        return False
    for parameter in signature.parameters.values():
        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            return True
        if parameter.name == name:
            return True
    return False


def _build_voice_prompt(
    *,
    session: VoiceSession,
    detected_language: str,
    normalized_intent_ref: str,
    input_text: str,
) -> str:
    return "\n".join(
        [
            "Digital Foreman Voice Layer v1",
            "Produce a bounded spoken reply for the current voice turn.",
            "Do not invent tool calls, state changes, or authority outside the request.",
            f"session_id={session.session_id}",
            f"caller_id={session.caller_id}",
            f"channel_type={session.channel_type}",
            f"active_language={session.active_language}",
            f"detected_language={detected_language}",
            f"normalized_intent_ref={normalized_intent_ref}",
            f"context_ref={session.context_ref or 'none'}",
            f"user_input={input_text}",
            "Return plain text only.",
        ]
    )


def _build_input_error_output(
    *,
    session: VoiceSession,
    detected_language: str,
    started_at: str,
    message: str,
) -> tuple[VoiceSession, VoiceTurn, ResponsePlan]:
    response_plan = create_response_plan(
        response_type="input_error",
        target_language=detected_language,
        text_payload=message,
        action_refs=[],
        requires_confirmation=False,
        execution_mode="live",
        metadata={
            "session_id": session.session_id,
            "caller_id": session.caller_id,
            "detected_language": detected_language,
            "result_status": "blocked",
            "started_at": started_at,
            "completed_at": started_at,
            "latency_ms": 0,
        },
    )
    turn = create_voice_turn(
        session_id=session.session_id,
        input_text="",
        detected_language=detected_language,
        normalized_intent_ref="generic_task",
        response_plan=response_plan,
        output_text=response_plan.text_payload,
        turn_status="blocked",
        created_at=started_at,
        trace_metadata=None,
    )
    turn_with_trace = create_voice_turn(
        session_id=session.session_id,
        input_text="",
        detected_language=detected_language,
        normalized_intent_ref="generic_task",
        response_plan=response_plan,
        output_text=response_plan.text_payload,
        turn_status="blocked",
        turn_id=turn.turn_id,
        created_at=started_at,
        trace_metadata=create_voice_trace_metadata(
            session_id=session.session_id,
            turn_id=turn.turn_id,
            caller_id=session.caller_id,
            detected_language=detected_language,
            action_id=None,
            result_status="blocked",
            started_at=started_at,
            completed_at=started_at,
            latency_ms=0,
            domain_binding=session.domain_binding,
        ),
    )
    updated_session = update_voice_session(
        session,
        last_turn_at=turn_with_trace.created_at,
        active_language=detected_language,
        session_status="active",
    )
    return updated_session, turn_with_trace, response_plan


@dataclass(frozen=True, slots=True)
class VoiceTurnOutput:
    session: VoiceSession
    turn: VoiceTurn
    action_contract: dict[str, object] | None
    action_result: dict[str, object] | None

    def to_dict(self) -> dict[str, object]:
        return {
            "session": self.session.to_dict(),
            "turn": self.turn.to_dict(),
            "action_contract": None
            if self.action_contract is None
            else dict(self.action_contract),
            "action_result": None
            if self.action_result is None
            else dict(self.action_result),
        }


class VoiceOrchestrator:
    def __init__(
        self,
        *,
        dispatcher: Callable[..., dict[str, object]] = dispatch_action,
        model: str = DEFAULT_VOICE_MODEL,
        max_tokens: int = DEFAULT_MAX_TOKENS,
        temperature: float = DEFAULT_TEMPERATURE,
    ) -> None:
        self._dispatcher = dispatcher
        self._model = model
        self._max_tokens = max_tokens
        self._temperature = temperature

    def process_turn(
        self,
        *,
        input_text: object,
        caller_id: object,
        channel_type: object = DEFAULT_CHANNEL_TYPE,
        session: VoiceSession | None = None,
        session_id: object | None = None,
        active_language: object = "und",
        detected_language: object | None = None,
        language_profile: dict[str, object] | None = None,
        context_ref: object = None,
        domain_binding: dict[str, object] | None = None,
        execution_mode: str = "live",
    ) -> VoiceTurnOutput:
        normalized_domain_binding = normalize_ownerbox_domain_binding(
            session.domain_binding
            if session is not None and domain_binding is None
            else domain_binding
        )
        voice_session = session or create_voice_session(
            caller_id=caller_id,
            channel_type=channel_type,
            active_language=active_language,
            language_profile=language_profile,
            context_ref=context_ref,
            domain_binding=normalized_domain_binding,
            session_id=session_id,
        )
        normalized_input_text = _collapse_whitespace(input_text)
        resolved_detected_language = (
            _normalize_text(detected_language) or voice_session.active_language
        )
        started_at = _utc_timestamp()

        if not normalized_input_text:
            updated_session, turn, _response_plan = _build_input_error_output(
                session=voice_session,
                detected_language=resolved_detected_language,
                started_at=started_at,
                message="Voice input was empty after normalization.",
            )
            return VoiceTurnOutput(
                session=updated_session,
                turn=turn,
                action_contract=None,
                action_result=None,
            )

        normalized_intent_ref, _normalized_payload = normalize_input(
            text=normalized_input_text
        )
        turn_seed = create_voice_turn(
            session_id=voice_session.session_id,
            input_text=normalized_input_text,
            detected_language=resolved_detected_language,
            normalized_intent_ref=normalized_intent_ref,
            response_plan=create_response_plan(
                response_type="spoken_text",
                target_language=resolved_detected_language,
                text_payload="pending voice response",
                action_refs=[],
                requires_confirmation=False,
                execution_mode=execution_mode,
                metadata={},
            ),
            output_text="pending voice response",
            turn_status="received",
        )
        started_at = turn_seed.created_at

        action_contract = build_action_contract(
            action_id=f"voice-action-{turn_seed.turn_id}",
            action_type="openai_request",
            target_type=DEFAULT_TARGET_TYPE,
            target_ref=DEFAULT_TARGET_REF,
            parameters={
                "model": self._model,
                "prompt": _build_voice_prompt(
                    session=voice_session,
                    detected_language=resolved_detected_language,
                    normalized_intent_ref=normalized_intent_ref,
                    input_text=normalized_input_text,
                ),
                "max_tokens": self._max_tokens,
                "temperature": self._temperature,
            },
            execution_mode=execution_mode,
            confirmation_policy="not_required",
            idempotency_key=f"voice:{voice_session.session_id}:{turn_seed.turn_id}",
            requested_by=DEFAULT_REQUESTED_BY,
        )
        dispatch_kwargs: dict[str, object] = {}
        if _dispatcher_supports_kwarg(self._dispatcher, "memory_domain"):
            dispatch_kwargs["memory_domain"] = (
                _normalize_text(normalized_domain_binding.get("domain_type")) or "dev"
            )
        if normalized_domain_binding and _dispatcher_supports_kwarg(
            self._dispatcher, "domain_binding"
        ):
            dispatch_kwargs["domain_binding"] = normalized_domain_binding
        action_result = self._dispatcher(action_contract, **dispatch_kwargs)
        response_plan = create_response_plan(
            response_type=_response_type_from_result_status(
                _normalize_text(action_result.get("status"))
            ),
            target_language=resolved_detected_language,
            text_payload=_result_payload_text(action_result),
            action_refs=[str(action_contract["action_id"])],
            requires_confirmation=bool(
                action_contract["confirmation_policy"] == "required"
            ),
            execution_mode=str(action_contract["execution_mode"]),
            metadata=_response_metadata(
                session=voice_session,
                turn_id=turn_seed.turn_id,
                detected_language=resolved_detected_language,
                action_contract=action_contract,
                action_result=action_result,
                started_at=started_at,
            ),
        )
        trace_metadata = create_voice_trace_metadata(
            session_id=voice_session.session_id,
            turn_id=turn_seed.turn_id,
            caller_id=voice_session.caller_id,
            detected_language=resolved_detected_language,
            action_id=action_contract["action_id"],
            result_status=action_result.get("status"),
            started_at=started_at,
            completed_at=response_plan.metadata.get("completed_at"),
            latency_ms=response_plan.metadata.get("latency_ms"),
            domain_binding=normalized_domain_binding,
        )
        turn = create_voice_turn(
            session_id=voice_session.session_id,
            input_text=normalized_input_text,
            detected_language=resolved_detected_language,
            normalized_intent_ref=normalized_intent_ref,
            response_plan=response_plan,
            output_text=response_plan.text_payload,
            turn_status=_turn_status_from_result_status(
                str(action_result.get("status"))
            ),
            turn_id=turn_seed.turn_id,
            created_at=started_at,
            trace_metadata=trace_metadata,
        )
        updated_session = update_voice_session(
            voice_session,
            last_turn_at=turn.created_at,
            active_language=resolved_detected_language,
            domain_binding=normalized_domain_binding,
            session_status="active",
        )
        return VoiceTurnOutput(
            session=updated_session,
            turn=turn,
            action_contract=action_contract,
            action_result=action_result,
        )
