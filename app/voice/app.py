import asyncio
import json
import logging
import re
import threading
import uuid
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from datetime import datetime, timezone
from types import SimpleNamespace

from fastapi import (
    FastAPI,
    Header,
    HTTPException,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.exception_handlers import (
    request_validation_exception_handler as fastapi_request_validation_exception_handler,
)
from fastapi.exceptions import RequestValidationError
from pydantic import BaseModel

from app.voice.box_behavior import (
    box_response_meta,
    resolve_box_state,
    resolve_run_objective,
    should_resume_scope_collection,
)
from app.voice.interaction_state import (
    advance_voice_session as interaction_advance_voice_session,
    display_short_status as interaction_display_short_status,
    session_context_block as interaction_session_context_block,
    session_mode_for_intent as interaction_session_mode_for_intent,
    suggested_replies_for_context as interaction_suggested_replies_for_context,
    update_voice_session_state as interaction_update_voice_session_state,
    voice_summary_with_followup as interaction_voice_summary_with_followup,
)
from app.voice.session_store import (
    find_session,
    get_or_create_session,
    update_session,
)
from app.voice.voice_runtime import VOICE_RUNTIME_BUS, publish_tts_text
from app.voice.voice_runtime_store import (
    append_trace_event as append_voice_trace_event,
    clear_pending_outbound,
    create_or_update_call_session,
    find_call_session,
    list_pending_outbound,
    load_trace_events,
    register_processed_event,
    remove_pending_outbound,
    update_call_session as update_voice_call_session,
)
from app.execution.input_normalizer import IncomingMessage, normalize_incoming_message
from memory.storage import find_recent_tasks_by_contact_id, upsert_contact
from app.execution.rbac import (
    authenticate_principal_token,
    authorize_controlled_action,
    authorize_product_action,
    require_controlled_principal,
)
from integrations.telnyx_voice import (
    answer_call,
    speak_text,
    start_streaming,
    telnyx_stream_url,
)
from app.execution.product_runtime import ProductRuntimeBoundaryError
from app.orchestrator.task_memory import store_task_result
from app.orchestrator.execution_router import materialize_routing_contract
from app.orchestrator.execution_store import (
    find_execution_record,
    load_execution_records,
    verify_execution_records,
)
from app.orchestrator.task_factory import (
    build_task_summary,
    get_task as find_task_by_id,
    get_open_tasks as get_operator_open_tasks,
    get_recent_tasks as get_operator_recent_tasks,
    get_tasks_by_contact as get_operator_tasks_by_contact,
)
from app.orchestrator.task_worker import TaskWorker
from app.execution.paths import TASKS_FILE as OPERATOR_TASKS_FILE
from app.product.intake import build_product_task_request
from app.product.runner import (
    build_execution_ready,
    build_typed_action_result,
    dispatch_development_action,
    dispatch_action_trigger,
    execute_product_task_request,
    validate_action_trigger,
)
from runtime.system_log import log_task_execution
from runtime.network.startup import initialize_network_runtime
from runtime.policy_engine import decide_execution_mode
from runtime.telemetry import collect_runtime_metrics
from runtime.network.monitor import get_network_snapshot
from runtime.validation import build_runtime_validation
from runtime.validation import build_runtime_verdict
from runtime.validation import (
    authority_chain_snapshot,
    voice_guardrail_boundary_payload,
    VOICE_RUNTIME_LATENCY_BUDGETS,
    latency_record,
    latency_warning,
)

app = FastAPI()
logger = logging.getLogger("digital_foreman.backend")
_OPERATOR_TASK_WORKER: TaskWorker | None = None
_OPERATOR_TASK_WORKER_CONTEXT: dict[str, object] | None = None
INTERNAL_ONLY_ERROR = {
    "detail": "public access disabled; use /api/secretary/entry",
}
INTERNAL_CALL_HEADER = "true"
TASK_LIFECYCLE_STATES = {
    "created",
    "running",
    "review_required",
    "completed",
    "failed",
}
VOICE_STATUS_KEYWORDS = (
    "status",
    "state",
    "progress",
    "what happened to",
    "where is",
    "Ð¿Ð¾ÐºÐ°Ð¶Ð¸ ÑÑ‚Ð°Ñ‚ÑƒÑ",
    "ÑÑ‚Ð°Ñ‚ÑƒÑ",
    "ÑÐ¾ÑÑ‚Ð¾ÑÐ½Ð¸Ðµ",
    "Ñ‡Ñ‚Ð¾ Ð´Ð°Ð»ÑŒÑˆÐµ",
)
VOICE_HISTORY_KEYWORDS = (
    "history",
    "recent",
    "last task",
    "last tasks",
    "what have you done",
    "what happened today",
    "Ð¿Ð¾ÑÐ»ÐµÐ´Ð½Ð¸Ðµ Ð·Ð°Ð´Ð°Ñ‡Ð¸",
    "Ð¿Ð¾ÐºÐ°Ð¶Ð¸ Ð¿Ð¾ÑÐ»ÐµÐ´Ð½Ð¸Ðµ Ð·Ð°Ð´Ð°Ñ‡Ð¸",
    "Ð¸ÑÑ‚Ð¾Ñ€Ð¸Ñ",
)
VOICE_HELP_KEYWORDS = (
    "help",
    "what can you do",
    "how do i use this",
    "Ð¿Ð¾Ð¼Ð¾Ñ‰ÑŒ",
    "Ñ‡Ñ‚Ð¾ Ñ‚Ñ‹ ÑƒÐ¼ÐµÐµÑˆÑŒ",
    "ÐºÐ°Ðº Ð¿Ð¾Ð»ÑŒÐ·Ð¾Ð²Ð°Ñ‚ÑŒÑÑ",
)
VOICE_REPEAT_KEYWORDS = (
    "repeat",
    "say that again",
    "Ð¿Ð¾Ð²Ñ‚Ð¾Ñ€Ð¸",
)
VOICE_AGENT_TIMEOUT_SECONDS = 20.0
VOICE_FIRST_RESPONSE_TEXT = "Digital Foreman connected. Go ahead."
VOICE_TURN_ACK_TEXT = "One moment."
VOICE_INTERRUPT_ACK_TEXT = "Okay, go ahead."
VOICE_FALLBACK_RESPONSE_TEXT = "I lost the execution path, but the call session is still active. Please repeat that."


def _get_operator_task_worker() -> TaskWorker:
    global _OPERATOR_TASK_WORKER
    if _OPERATOR_TASK_WORKER_CONTEXT is None:
        raise RuntimeError("SYSTEM_CONTEXT_REQUIRED")
    if _OPERATOR_TASK_WORKER is None:
        _OPERATOR_TASK_WORKER = TaskWorker(system_context=_OPERATOR_TASK_WORKER_CONTEXT)
    return _OPERATOR_TASK_WORKER


def configure_operator_task_worker(system_context: dict[str, object]) -> None:
    global _OPERATOR_TASK_WORKER_CONTEXT
    if system_context is None:
        raise RuntimeError("SYSTEM_CONTEXT_REQUIRED")
    _OPERATOR_TASK_WORKER_CONTEXT = dict(system_context)


@app.exception_handler(RequestValidationError)
async def input_validation_exception_handler(
    request: Request,
    exc: RequestValidationError,
):
    raw_body = (await request.body()).decode("utf-8", errors="replace")
    logger.info(
        "BACKEND_VALIDATION_ERRORS %s",
        json.dumps(
            {
                "path": request.url.path,
                "method": request.method,
                "body": raw_body,
                "errors": exc.errors(),
            },
            ensure_ascii=False,
        ),
    )
    return await fastapi_request_validation_exception_handler(request, exc)


@app.on_event("startup")
def _start_operator_task_worker() -> None:
    initialize_network_runtime(sample_count=3)
    _get_operator_task_worker().start()


@app.on_event("shutdown")
def _stop_operator_task_worker() -> None:
    worker = _OPERATOR_TASK_WORKER
    if worker is not None:
        worker.stop()


def _principal_from_headers(
    x_df_actor_id: str | None = Header(default=None),
    x_df_role: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    x_df_principal_token: str | None = Header(default=None),
) -> dict[str, str]:
    token = _extract_principal_token(
        authorization=authorization,
        x_df_principal_token=x_df_principal_token,
    )
    if token:
        return authenticate_principal_token(token)
    return require_controlled_principal(
        actor_id=x_df_actor_id,
        role=x_df_role,
    )


def _extract_principal_token(
    *,
    authorization: str | None,
    x_df_principal_token: str | None,
) -> str:
    bearer_value = str(authorization or "").strip()
    if bearer_value.lower().startswith("bearer "):
        return bearer_value[7:].strip()
    return str(x_df_principal_token or "").strip()


def _require_permission(principal: dict[str, str], permission: str) -> dict[str, str]:
    permission_map = {
        "execute_controlled": "controlled.execute",
        "read_history": "controlled.read_history",
        "read_report": "controlled.read_report",
        "verify_integrity": "controlled.verify_integrity",
    }
    raw_permission = str(permission).strip()
    normalized_permission = permission_map.get(raw_permission, raw_permission)
    try:
        return authorize_controlled_action(
            actor_id=principal.get("actor_id", ""),
            role=principal.get("role", ""),
            permission=normalized_permission,
        )
    except HTTPException as exc:
        detail = str(exc.detail)
        prefixed_permission = permission_map.get(raw_permission, "")
        if prefixed_permission and prefixed_permission in detail:
            raise HTTPException(
                status_code=exc.status_code,
                detail=detail.replace(prefixed_permission, raw_permission),
            ) from exc
        raise


def _history_summary_item(record: dict[str, object]) -> dict[str, object]:
    integrity = dict(record.get("integrity", {}))
    audit = dict(record.get("audit", {}))
    return {
        "task_id": str(record.get("task_id", "")).strip(),
        "selected_agent": str(record.get("selected_agent", "")).strip(),
        "recorded_at": str(record.get("recorded_at", "")).strip(),
        "acceptance_status": str(record.get("acceptance_status", "")).strip(),
        "validation_status": str(record.get("validation_status", "")).strip(),
        "apply_allowed": bool(record.get("apply_allowed", False)),
        "workspace_status": str(record.get("workspace_status", "")).strip(),
        "actor_id": str(audit.get("actor_id", "")).strip(),
        "role": str(audit.get("role", "")).strip(),
        "chain_hash": str(integrity.get("chain_hash", "")).strip(),
    }


class ExecuteRequest(BaseModel):
    goal: str = ""
    context: str = ""
    depends_on: str = ""
    linear_task_id: str
    linear_task_title: str
    mvp_priority: str
    expected_result: str
    done_condition: str
    done_condition_met: bool = False


class ControlledExecuteRequest(BaseModel):
    task_id: str
    title: str
    objective: str
    preferred_agent: str = "auto"
    allow_auto_edit: bool = False
    scope_files: list[str] = []
    constraints: list[str] = []
    validation_steps: list[str] = []


class ProductTaskRunRequest(BaseModel):
    task_id: str = ""
    objective: str = ""
    scope_files: list[str] = []
    user_id: str = ""
    user_role: str = ""


class VoiceEntryRequest(BaseModel):
    transcript: str = ""
    scope_files: list[str] = []
    user_id: str = ""
    user_role: str = ""
    task_id: str = ""
    session_id: str = ""


class SecretaryEntryRequest(BaseModel):
    channel: str = ""
    message: str = ""
    contact_id: str = ""
    raw_contact: str = ""
    timestamp: str = ""
    session_id: str = ""
    user_id: str = ""
    user_role: str = ""


class TelnyxCallControlWebhookRequest(BaseModel):
    event_type: str = ""
    occurred_at: str = ""
    id: str = ""
    payload: dict[str, object] = {}
    data: dict[str, object] = {}


class VoiceGatewayEventRequest(BaseModel):
    call_session_id: str = ""
    call_control_id: str = ""
    event_id: str = ""
    event_type: str = ""
    transcript: str = ""
    stream_id: str = ""
    sequence_number: int = 0
    timestamp: str = ""


class TelnyxMediaFrameRequest(BaseModel):
    event: str = ""
    sequence_number: int = 0
    stream_id: str = ""
    transcript: str = ""
    payload: dict[str, object] = {}


def _voice_call_user_id(from_number: object, call_control_id: object) -> str:
    normalized_from_number = str(from_number or "").strip()
    if normalized_from_number:
        return normalized_from_number
    return f"telnyx:{str(call_control_id or '').strip()}"


def _voice_call_identity(call_control_id: object) -> dict[str, str]:
    normalized_call_control_id = str(call_control_id or "").strip()
    suffix = normalized_call_control_id or uuid.uuid4().hex[:12]
    return {
        "call_session_id": f"call-{suffix}",
        "session_id": f"call-{suffix}",
        "interaction_id": f"voice-interaction-{suffix}",
        "job_id": f"voice-job-{suffix}",
        "trace_id": f"voice-trace-{suffix}",
    }


def _voice_call_trace(
    session: dict[str, object],
    event_type: str,
    **payload: object,
) -> dict[str, object]:
    return append_voice_trace_event(
        call_session_id=session.get("call_session_id", ""),
        interaction_id=session.get("interaction_id", ""),
        job_id=session.get("job_id", ""),
        trace_id=session.get("trace_id", ""),
        event_type=event_type,
        payload={key: value for key, value in payload.items()},
    )


def _queue_voice_call_text(
    session: dict[str, object],
    *,
    text: str,
    kind: str,
    interruptible: bool = True,
    use_telnyx_speak: bool = False,
) -> list[dict[str, object]]:
    normalized_text = " ".join(str(text or "").split()).strip()
    if not normalized_text:
        return []
    published = publish_tts_text(
        call_session_id=str(session.get("call_session_id", "")).strip(),
        text=normalized_text,
        kind=kind,
        interruptible=interruptible,
    )
    if use_telnyx_speak and str(session.get("call_control_id", "")).strip():
        for item in published:
            speak_text(
                call_control_id=str(session.get("call_control_id", "")).strip(),
                text=str(item.get("text", "")).strip(),
                command_id=str(item.get("event_id", "")).strip(),
            )
    return published


def _voice_call_response_text_from_secretary(result: dict[str, object]) -> str:
    confirmation_prompt = str(result.get("confirmation_prompt", "")).strip()
    if confirmation_prompt:
        return confirmation_prompt
    message = str(result.get("message", "")).strip()
    if message:
        return message
    task_payload = dict(result.get("task", {}))
    task_id = (
        str(result.get("task_id", "")).strip()
        or str(task_payload.get("task_id", "")).strip()
    )
    task_status = (
        str(result.get("status", "")).strip()
        or str(task_payload.get("status", "")).strip()
    )
    if task_id:
        if task_status:
            return f"Task {task_id} is {task_status}."
        return f"Task {task_id} has been created."
    return VOICE_FALLBACK_RESPONSE_TEXT


def _voice_call_state_payload(session: dict[str, object]) -> dict[str, object]:
    call_session_id = str(session.get("call_session_id", "")).strip()
    return {
        "call_session_id": call_session_id,
        "session_id": str(session.get("session_id", "")).strip(),
        "call_control_id": str(session.get("call_control_id", "")).strip(),
        "stream_id": str(session.get("stream_id", "")).strip(),
        "interaction_id": str(session.get("interaction_id", "")).strip(),
        "job_id": str(session.get("job_id", "")).strip(),
        "trace_id": str(session.get("trace_id", "")).strip(),
        "orchestrator_task_id": str(session.get("orchestrator_task_id", "")).strip(),
        "call_state": str(session.get("call_state", "")).strip(),
        "media_state": str(session.get("media_state", "")).strip(),
        "interaction_state": str(session.get("interaction_state", "")).strip(),
        "current_mode": str(session.get("current_mode", "")).strip(),
        "job_status": str(session.get("job_status", "")).strip(),
        "user_id": str(session.get("user_id", "")).strip(),
        "user_role": str(session.get("user_role", "")).strip(),
        "from_number": str(session.get("from_number", "")).strip(),
        "to_number": str(session.get("to_number", "")).strip(),
        "last_partial_transcript": str(
            session.get("last_partial_transcript", "")
        ).strip(),
        "last_transcript": str(session.get("last_transcript", "")).strip(),
        "last_response_text": str(session.get("last_response_text", "")).strip(),
        "last_ack_text": str(session.get("last_ack_text", "")).strip(),
        "last_error": str(session.get("last_error", "")).strip(),
        "validation_state": str(session.get("validation_state", "UNKNOWN")).strip(),
        "validation_guardrails": list(session.get("validation_guardrails", [])),
        "validation_summary": str(session.get("validation_summary", "")).strip(),
        "last_validation_at": str(session.get("last_validation_at", "")).strip(),
        "active_turn_event_id": str(session.get("active_turn_event_id", "")).strip(),
        "last_runtime_state": str(session.get("last_runtime_state", "")).strip(),
        "last_runtime_confidence": str(
            session.get("last_runtime_confidence", "")
        ).strip(),
        "voice_latency_metrics": dict(session.get("voice_latency_metrics", {})),
        "guardrail_activation_count": int(
            session.get("guardrail_activation_count", 0) or 0
        ),
        "runtime_transition_count": int(
            session.get("runtime_transition_count", 0) or 0
        ),
        "runtime_verdict": str(session.get("runtime_verdict", "")).strip(),
        "voice_runtime_verdict": str(session.get("voice_runtime_verdict", "")).strip(),
        "runtime_verdict_score": int(session.get("runtime_verdict_score", 0) or 0),
        "last_runtime_verdict_at": str(
            session.get("last_runtime_verdict_at", "")
        ).strip(),
        "reconnect_count": int(session.get("reconnect_count", 0) or 0),
        "interruption_count": int(session.get("interruption_count", 0) or 0),
        "pending_outbound": list_pending_outbound(call_session_id),
        "trace_count": len(load_trace_events(call_session_id=call_session_id)),
    }


def _voice_runtime_task_data(
    session: dict[str, object],
    *,
    transcript: str = "",
) -> dict[str, object]:
    return {
        "source": "voice",
        "source_channel": "voice",
        "interaction_id": str(session.get("interaction_id", "")).strip(),
        "job_id": str(session.get("job_id", "")).strip(),
        "trace_id": str(session.get("trace_id", "")).strip(),
        "payload": {
            "channel": "voice",
            "source": "voice",
            "transcript": transcript,
            "call_session_id": str(session.get("call_session_id", "")).strip(),
        },
    }


def _validation_summary(runtime_validation: dict[str, object]) -> str:
    state = str(runtime_validation.get("state", "PASSED")).strip().upper()
    guardrails = list(runtime_validation.get("guardrails", []))
    if not guardrails:
        return state
    return f"{state}: {', '.join(str(item) for item in guardrails)}"


def _append_voice_latency_metric(
    session: dict[str, object],
    *,
    metric: str,
    value_ms: float,
) -> dict[str, object]:
    current_metrics = dict(session.get("voice_latency_metrics", {}))
    current_metrics[metric] = round(max(0.0, float(value_ms)), 3)
    return update_voice_call_session(
        session["call_session_id"],
        voice_latency_metrics=current_metrics,
    )


def _increment_voice_counter(
    session: dict[str, object],
    *,
    field_name: str,
    amount: int = 1,
) -> dict[str, object]:
    current_value = int(session.get(field_name, 0) or 0)
    return update_voice_call_session(
        session["call_session_id"],
        **{field_name: current_value + max(0, int(amount))},
    )


def _trace_voice_validation_warning(
    session: dict[str, object],
    *,
    event_id: str,
    warning: dict[str, object],
    runtime_validation: dict[str, object] | None = None,
    routing_contract: dict[str, object] | None = None,
) -> None:
    _voice_call_trace(
        session,
        "VOICE_RUNTIME_VALIDATION_WARNING",
        event_id=event_id,
        warning=warning,
        runtime_validation=runtime_validation or {},
        routing_contract=routing_contract or {},
        latency_budgets=VOICE_RUNTIME_LATENCY_BUDGETS,
    )


def _trace_latency_event(
    session: dict[str, object],
    *,
    event_type: str,
    event_id: str,
    metric: str,
    value_ms: float,
    runtime_validation: dict[str, object] | None = None,
    routing_contract: dict[str, object] | None = None,
) -> dict[str, object]:
    payload = latency_record(
        metric=metric,
        value_ms=value_ms,
        budget_ms=VOICE_RUNTIME_LATENCY_BUDGETS.get(metric),
    )
    _voice_call_trace(
        session,
        event_type,
        event_id=event_id,
        latency=payload,
        runtime_validation=runtime_validation or {},
        routing_contract=routing_contract or {},
        latency_budgets=VOICE_RUNTIME_LATENCY_BUDGETS,
    )
    warning = latency_warning(metric, value_ms)
    if warning is not None:
        _trace_voice_validation_warning(
            session,
            event_id=event_id,
            warning=warning,
            runtime_validation=runtime_validation,
            routing_contract=routing_contract,
        )
    return payload


def _trace_voice_latency_stack(
    session: dict[str, object],
    *,
    event_id: str,
    runtime_validation: dict[str, object],
    runtime_decision: dict[str, object],
    routing_contract: dict[str, object] | None = None,
) -> tuple[dict[str, object], dict[str, object]]:
    latency_metrics = dict(session.get("voice_latency_metrics", {}))
    stack_value = round(
        float(latency_metrics.get("policy_evaluation_ms", 0.0) or 0.0)
        + float(latency_metrics.get("validation_processing_ms", 0.0) or 0.0)
        + float(latency_metrics.get("verdict_aggregation_ms", 0.0) or 0.0)
        + float(latency_metrics.get("router_decision_ms", 0.0) or 0.0),
        3,
    )
    session = _append_voice_latency_metric(
        session,
        metric="latency_stack_ms",
        value_ms=stack_value,
    )
    payload = _trace_latency_event(
        session,
        event_type="VOICE_LATENCY_STACK_MEASURED",
        event_id=event_id,
        metric="latency_stack_ms",
        value_ms=stack_value,
        runtime_validation=runtime_validation,
        routing_contract=routing_contract,
    )
    _voice_call_trace(
        session,
        "VOICE_STRESS_VALIDATION_COMPLETED",
        event_id=event_id,
        runtime_decision=runtime_decision,
        runtime_validation=runtime_validation,
        runtime_latency_stack=payload,
        routing_contract=routing_contract or {},
    )
    return session, payload


def _update_voice_runtime_verdict(
    session: dict[str, object],
    *,
    event_id: str,
    runtime_validation: dict[str, object],
    runtime_decision: dict[str, object],
    routing_contract: dict[str, object] | None = None,
) -> tuple[dict[str, object], dict[str, object]]:
    verdict_started = time.monotonic()
    verdict = build_runtime_verdict(
        runtime_validation=runtime_validation,
        runtime_decision=runtime_decision,
        latency_metrics=dict(session.get("voice_latency_metrics", {})),
        session=session,
    )
    verdict_latency_ms = (time.monotonic() - verdict_started) * 1000.0
    updated_session = _append_voice_latency_metric(
        session,
        metric="verdict_aggregation_ms",
        value_ms=verdict_latency_ms,
    )
    updated_session = update_voice_call_session(
        updated_session["call_session_id"],
        runtime_verdict=str(verdict.get("runtime_verdict", "")).strip(),
        voice_runtime_verdict=str(verdict.get("voice_verdict", "")).strip(),
        runtime_verdict_score=int(verdict.get("score", 0) or 0),
        last_runtime_verdict_at=str(verdict.get("timestamp", "")).strip(),
    )
    _trace_latency_event(
        updated_session,
        event_type="VOICE_VERDICT_LATENCY_RECORDED",
        event_id=event_id,
        metric="verdict_aggregation_ms",
        value_ms=verdict_latency_ms,
        runtime_validation=runtime_validation,
        routing_contract=routing_contract,
    )
    _voice_call_trace(
        updated_session,
        "RUNTIME_VERDICT_EVALUATED",
        event_id=event_id,
        runtime_verdict=verdict,
        runtime_validation=runtime_validation,
        runtime_decision=runtime_decision,
        routing_contract=routing_contract or {},
    )
    if str(verdict.get("voice_verdict", "")).strip() not in {"", "NOT_APPLICABLE"}:
        _voice_call_trace(
            updated_session,
            "VOICE_RUNTIME_VERDICT_UPDATED",
            event_id=event_id,
            runtime_verdict=verdict,
            runtime_validation=runtime_validation,
            runtime_decision=runtime_decision,
            routing_contract=routing_contract or {},
        )
    return updated_session, verdict


def _evaluate_voice_runtime_validation(
    session: dict[str, object],
    *,
    transcript: str,
    event_id: str,
) -> tuple[
    dict[str, object],
    dict[str, object],
    dict[str, object],
    dict[str, object],
    dict[str, object],
    dict[str, object],
    dict[str, object],
]:
    task_data = _voice_runtime_task_data(session, transcript=transcript)
    metrics = dict(collect_runtime_metrics())
    network_snapshot = dict(get_network_snapshot())
    _voice_call_trace(
        session,
        "VOICE_STRESS_VALIDATION_STARTED",
        event_id=event_id,
        authority_chain=authority_chain_snapshot(),
        telemetry_snapshot=metrics,
        network_snapshot=network_snapshot,
    )
    policy_started = time.monotonic()
    _, _, _, _, runtime_decision = decide_execution_mode(
        task_data,
        metrics,
        network_snapshot,
    )
    policy_latency_ms = (time.monotonic() - policy_started) * 1000.0
    router_started = time.monotonic()
    validation_started = time.monotonic()
    runtime_validation = build_runtime_validation(
        task_data=task_data,
        metrics=metrics,
        network_snapshot=network_snapshot,
        runtime_decision=runtime_decision,
        session=session,
    )
    validation_latency_ms = (time.monotonic() - validation_started) * 1000.0
    routing_contract = materialize_routing_contract(
        execution_mode="LOCAL",
        runtime_profile=str(task_data.get("runtime_profile", "VOICE")).strip()
        or "VOICE",
        routing_reason={
            "type": "runtime_decision",
            "value": runtime_decision.get("overall_runtime_state", "NORMAL"),
            "threshold": runtime_decision.get("execution_preference", "SAFE_LOCAL"),
            "confidence": runtime_decision.get("confidence", "HIGH_CONFIDENCE"),
        },
        telemetry_snapshot=metrics,
        safety_override={
            "triggered": False,
            "reason": "",
            "value": 0.0,
            "threshold": 0.0,
        },
        network_snapshot=network_snapshot,
        network_policy={"voice_protected": True, "prefer_remote": False},
        runtime_decision=runtime_decision,
        runtime_validation=runtime_validation,
    )
    router_latency_ms = (time.monotonic() - router_started) * 1000.0
    updated_session = update_voice_call_session(
        session["call_session_id"],
        validation_state=str(runtime_validation.get("state", "UNKNOWN")).strip(),
        validation_guardrails=list(runtime_validation.get("guardrails", [])),
        validation_summary=_validation_summary(runtime_validation),
        last_validation_at=str(runtime_validation.get("timestamp", "")).strip(),
        last_runtime_state=str(
            runtime_decision.get("overall_runtime_state", "")
        ).strip(),
        last_runtime_confidence=str(runtime_decision.get("confidence", "")).strip(),
    )
    updated_session = _append_voice_latency_metric(
        updated_session,
        metric="policy_evaluation_ms",
        value_ms=policy_latency_ms,
    )
    updated_session = _append_voice_latency_metric(
        updated_session,
        metric="router_decision_ms",
        value_ms=router_latency_ms,
    )
    updated_session = _append_voice_latency_metric(
        updated_session,
        metric="validation_processing_ms",
        value_ms=validation_latency_ms,
    )
    _trace_latency_event(
        updated_session,
        event_type="VOICE_POLICY_LATENCY_RECORDED",
        event_id=event_id,
        metric="policy_evaluation_ms",
        value_ms=policy_latency_ms,
        runtime_validation=runtime_validation,
        routing_contract=routing_contract,
    )
    _trace_latency_event(
        updated_session,
        event_type="VOICE_VALIDATION_LATENCY_RECORDED",
        event_id=event_id,
        metric="validation_processing_ms",
        value_ms=validation_latency_ms,
        runtime_validation=runtime_validation,
        routing_contract=routing_contract,
    )
    _trace_latency_event(
        updated_session,
        event_type="VOICE_ROUTER_LATENCY_RECORDED",
        event_id=event_id,
        metric="router_decision_ms",
        value_ms=router_latency_ms,
        runtime_validation=runtime_validation,
        routing_contract=routing_contract,
    )
    _voice_call_trace(
        updated_session,
        "voice.runtime.validation.evaluated",
        event_id=event_id,
        runtime_decision=runtime_decision,
        runtime_validation=runtime_validation,
        routing_contract=routing_contract,
        telemetry_snapshot=metrics,
        network_snapshot=network_snapshot,
    )
    previous_runtime_state = str(session.get("last_runtime_state", "")).strip()
    previous_runtime_confidence = str(
        session.get("last_runtime_confidence", "")
    ).strip()
    current_runtime_state = str(
        runtime_decision.get("overall_runtime_state", "")
    ).strip()
    current_runtime_confidence = str(runtime_decision.get("confidence", "")).strip()
    if previous_runtime_state and previous_runtime_state != current_runtime_state:
        updated_session = _increment_voice_counter(
            updated_session,
            field_name="runtime_transition_count",
        )
        _trace_voice_validation_warning(
            updated_session,
            event_id=event_id,
            warning={
                "code": "voice_runtime_state_transition",
                "previous_state": previous_runtime_state,
                "new_state": current_runtime_state,
            },
            runtime_validation=runtime_validation,
            routing_contract=routing_contract,
        )
    if (
        previous_runtime_confidence
        and previous_runtime_confidence != current_runtime_confidence
        and current_runtime_confidence == "LOW_CONFIDENCE"
    ):
        _trace_voice_validation_warning(
            updated_session,
            event_id=event_id,
            warning={
                "code": "voice_confidence_drop",
                "previous_confidence": previous_runtime_confidence,
                "new_confidence": current_runtime_confidence,
            },
            runtime_validation=runtime_validation,
            routing_contract=routing_contract,
        )
    if bool(runtime_validation.get("latency_path_protected")):
        _voice_call_trace(
            updated_session,
            "voice.latency.path.protected",
            event_id=event_id,
            runtime_validation=runtime_validation,
            routing_contract=routing_contract,
        )
    if str(runtime_validation.get("state", "")).strip().upper() == "GUARDED":
        _voice_call_trace(
            updated_session,
            "voice.runtime.validation.guarded",
            event_id=event_id,
            runtime_validation=runtime_validation,
            routing_contract=routing_contract,
        )
    if bool(runtime_decision.get("offload_recommended")):
        _trace_voice_validation_warning(
            updated_session,
            event_id=event_id,
            warning={
                "code": "voice_offload_attempt_blocked",
                "execution_preference": runtime_decision.get(
                    "execution_preference", "SAFE_LOCAL"
                ),
                "path_type": runtime_decision.get("path_type", "voice"),
            },
            runtime_validation=runtime_validation,
            routing_contract=routing_contract,
        )
    if list(runtime_validation.get("guardrails", [])):
        updated_session = _increment_voice_counter(
            updated_session,
            field_name="guardrail_activation_count",
            amount=len(list(runtime_validation.get("guardrails", []))),
        )
        _voice_call_trace(
            updated_session,
            "VOICE_DECISION_GUARD_APPLIED",
            event_id=event_id,
            runtime_validation=runtime_validation,
            routing_contract=routing_contract,
        )
        _voice_call_trace(
            updated_session,
            "VOICE_GUARDRAIL_BOUNDARY_APPLIED",
            event_id=event_id,
            boundary=voice_guardrail_boundary_payload(
                runtime_decision=runtime_decision,
                runtime_validation=runtime_validation,
            ),
            routing_contract=routing_contract,
        )
    _voice_call_trace(
        updated_session,
        "VOICE_AUTHORITY_CHAIN_ENFORCED",
        event_id=event_id,
        authority_chain=authority_chain_snapshot(),
        boundary=voice_guardrail_boundary_payload(
            runtime_decision=runtime_decision,
            runtime_validation=runtime_validation,
        ),
        routing_contract=routing_contract,
    )
    updated_session, runtime_verdict = _update_voice_runtime_verdict(
        updated_session,
        event_id=event_id,
        runtime_validation=runtime_validation,
        runtime_decision=runtime_decision,
        routing_contract=routing_contract,
    )
    updated_session, _ = _trace_voice_latency_stack(
        updated_session,
        event_id=event_id,
        runtime_validation=runtime_validation,
        runtime_decision=runtime_decision,
        routing_contract=routing_contract,
    )
    return (
        updated_session,
        metrics,
        network_snapshot,
        runtime_validation,
        routing_contract,
        runtime_decision,
        runtime_verdict,
    )


def _execute_voice_call_secretary_turn(
    payload: SecretaryEntryRequest,
) -> dict[str, object]:
    return secretary_entry(payload)


def _run_voice_call_turn_with_timeout(
    payload: SecretaryEntryRequest,
) -> dict[str, object]:
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(_execute_voice_call_secretary_turn, payload)
    try:
        return future.result(timeout=VOICE_AGENT_TIMEOUT_SECONDS)
    except FutureTimeoutError as exc:
        future.cancel()
        raise TimeoutError("voice turn timed out") from exc
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def _process_voice_call_turn(
    *,
    call_session_id: str,
    transcript: str,
    event_id: str,
) -> None:
    session = find_call_session(call_session_id=call_session_id)
    if session is None:
        return

    def _safe_update(**updates: object) -> dict[str, object] | None:
        try:
            return update_voice_call_session(call_session_id, **updates)
        except ValueError:
            return None

    def _safe_clear_pending() -> None:
        try:
            clear_pending_outbound(call_session_id)
        except ValueError:
            return

    try:
        request_payload = SecretaryEntryRequest(
            channel="phone",
            message=transcript,
            contact_id=str(session.get("from_number", "")).strip()
            or str(session.get("user_id", "")).strip(),
            timestamp=_utc_now_iso(),
            session_id=str(session.get("session_id", "")).strip(),
            user_id=str(session.get("user_id", "")).strip(),
            user_role=str(session.get("user_role", "")).strip() or "foreman",
        )
        result = _run_voice_call_turn_with_timeout(request_payload)
        spoken_response = _voice_call_response_text_from_secretary(result)
        updated_session = _safe_update(
            interaction_state="waiting_for_followup",
            current_mode="voice_call",
            media_state="listening",
            job_status="active",
            active_turn_event_id="",
            orchestrator_task_id=(
                str(result.get("task_id", "")).strip()
                or str(dict(result.get("task", {})).get("task_id", "")).strip()
                or str(session.get("orchestrator_task_id", "")).strip()
            ),
            last_response_text=spoken_response,
            last_error="",
        )
        if updated_session is None:
            return
        _voice_call_trace(
            updated_session,
            "voice.turn.completed",
            event_id=event_id,
            transcript=transcript,
            task_id=str(updated_session.get("orchestrator_task_id", "")).strip(),
            response_text=spoken_response,
        )
        _safe_clear_pending()
        _queue_voice_call_text(
            updated_session,
            text=spoken_response,
            kind="assistant_response",
            interruptible=True,
            use_telnyx_speak=True,
        )
    except TimeoutError:
        updated_session = _safe_update(
            interaction_state="waiting_for_followup",
            current_mode="voice_call",
            media_state="listening",
            job_status="timeout",
            active_turn_event_id="",
            last_error="voice turn timed out",
            last_response_text=VOICE_FALLBACK_RESPONSE_TEXT,
        )
        if updated_session is None:
            return
        _voice_call_trace(
            updated_session,
            "voice.turn.timeout",
            event_id=event_id,
            transcript=transcript,
        )
        _safe_clear_pending()
        _queue_voice_call_text(
            updated_session,
            text=VOICE_FALLBACK_RESPONSE_TEXT,
            kind="fallback_response",
            interruptible=True,
            use_telnyx_speak=True,
        )
    except Exception as exc:
        fallback_text = VOICE_FALLBACK_RESPONSE_TEXT
        updated_session = _safe_update(
            interaction_state="waiting_for_followup",
            current_mode="voice_call",
            media_state="listening",
            job_status="failed",
            active_turn_event_id="",
            last_error=str(exc).strip() or "voice turn failed",
            last_response_text=fallback_text,
        )
        if updated_session is None:
            return
        _voice_call_trace(
            updated_session,
            "voice.turn.failed",
            event_id=event_id,
            transcript=transcript,
            error=str(exc).strip() or "voice turn failed",
        )
        _safe_clear_pending()
        _queue_voice_call_text(
            updated_session,
            text=fallback_text,
            kind="fallback_response",
            interruptible=True,
            use_telnyx_speak=True,
        )


def _start_voice_call_turn(
    *,
    call_session_id: str,
    transcript: str,
    event_id: str,
) -> None:
    worker = threading.Thread(
        target=_process_voice_call_turn,
        kwargs={
            "call_session_id": call_session_id,
            "transcript": transcript,
            "event_id": event_id,
        },
        daemon=True,
    )
    worker.start()


def _ensure_voice_call_session_from_telnyx(
    payload: TelnyxCallControlWebhookRequest,
) -> tuple[dict[str, object], dict[str, object], list[dict[str, object]]]:
    raw_payload = dict(payload.payload or payload.data or {})
    call_control_id = str(raw_payload.get("call_control_id", "")).strip()
    existing_session = find_call_session(call_control_id=call_control_id)
    identity = _voice_call_identity(call_control_id)
    session = create_or_update_call_session(
        call_session_id=(
            str(existing_session.get("call_session_id", "")).strip()
            if existing_session is not None
            else identity["call_session_id"]
        ),
        session_id=(
            str(existing_session.get("session_id", "")).strip()
            if existing_session is not None
            else identity["session_id"]
        ),
        call_control_id=call_control_id,
        telnyx_call_leg_id=str(raw_payload.get("call_leg_id", "")).strip(),
        stream_id=str(raw_payload.get("stream_id", "")).strip(),
        from_number=str(raw_payload.get("from", "")).strip(),
        to_number=str(raw_payload.get("to", "")).strip(),
        user_id=_voice_call_user_id(raw_payload.get("from", ""), call_control_id),
        user_role="foreman",
        interaction_id=(
            str(existing_session.get("interaction_id", "")).strip()
            if existing_session is not None
            else identity["interaction_id"]
        ),
        job_id=(
            str(existing_session.get("job_id", "")).strip()
            if existing_session is not None
            else identity["job_id"]
        ),
        trace_id=(
            str(existing_session.get("trace_id", "")).strip()
            if existing_session is not None
            else identity["trace_id"]
        ),
        call_state="answered",
        media_state="listening",
        interaction_state="waiting_for_followup",
        current_mode="voice_call",
        reconnect_count=int(existing_session.get("reconnect_count", 0) or 0)
        if existing_session is not None
        else 0,
    )
    get_or_create_session(
        session_id=session["session_id"],
        user_id=session["user_id"],
        user_role=session["user_role"],
    )
    _voice_call_trace(
        session,
        "telnyx.call_control.received",
        telnyx_event_type=payload.event_type,
        telnyx_event_id=payload.id,
        occurred_at=payload.occurred_at,
    )
    commands: list[dict[str, object]] = []
    if call_control_id:
        commands.append(
            answer_call(
                call_control_id=call_control_id,
                command_id=f"{session['call_session_id']}-answer",
            )
        )
        stream_url = telnyx_stream_url()
        if stream_url:
            commands.append(
                start_streaming(
                    call_control_id=call_control_id,
                    stream_url=stream_url,
                    command_id=f"{session['call_session_id']}-stream",
                )
            )
    if existing_session is None:
        _queue_voice_call_text(
            session,
            text=VOICE_FIRST_RESPONSE_TEXT,
            kind="system_greeting",
            interruptible=True,
            use_telnyx_speak=True,
        )
    else:
        session = update_voice_call_session(
            session["call_session_id"],
            reconnect_count=int(session.get("reconnect_count", 0) or 0) + 1,
            media_state="listening",
            interaction_state="waiting_for_followup",
            call_state="answered",
        )
        _voice_call_trace(
            session,
            "voice.call.reconnected",
            call_control_id=call_control_id,
        )
    return session, raw_payload, commands


def _require_internal_only_call(x_df_internal_call: str | None) -> None:
    if str(x_df_internal_call or "").strip().lower() != INTERNAL_CALL_HEADER:
        raise HTTPException(status_code=403, detail=INTERNAL_ONLY_ERROR["detail"])


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _model_payload(model: BaseModel) -> dict[str, object]:
    if hasattr(model, "model_dump"):
        return dict(model.model_dump())
    return dict(model.dict())


def _incoming_message_from_request(payload: SecretaryEntryRequest) -> IncomingMessage:
    try:
        return normalize_incoming_message(
            channel=payload.channel,
            contact_id=payload.contact_id,
            raw_contact=payload.raw_contact,
            raw_text=payload.message,
            timestamp=payload.timestamp or _utc_now_iso(),
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _secretary_extract_name(text: str) -> str:
    match = re.search(
        r"\b(?:my name is|this is|i am)\s+([A-Za-z][A-Za-z\s'-]{1,60}?)(?:\s+and\b|[.!?,]|$)",
        str(text or ""),
        flags=re.IGNORECASE,
    )
    if not match:
        return ""
    return str(match.group(1)).strip().rstrip(".!,?")


def _secretary_extract_address(text: str) -> str:
    match = re.search(
        r"\b(?:at|address is)\s+([0-9][A-Za-z0-9\s#.,'-]{5,80}?)(?:\s+needs\b|\s+for\b|[.!?,]|$)",
        str(text or ""),
        flags=re.IGNORECASE,
    )
    if not match:
        return ""
    return str(match.group(1)).strip().rstrip(".!,?")


def _secretary_extract_city(text: str) -> str:
    match = re.search(
        r"\bin\s+([A-Za-z][A-Za-z\s'-]{1,40}?)(?:[.!?,]|$)",
        str(text or ""),
        flags=re.IGNORECASE,
    )
    if not match:
        return ""
    return str(match.group(1)).strip().rstrip(".!,?")


def _secretary_service_request_payload(
    normalized_text: str,
    contact_id: str,
    source_channel: str,
) -> dict[str, object]:
    lowered = normalized_text.lower()
    service_type = (
        "plumbing"
        if any(phrase in lowered for phrase in ("faucet", "plumb", "pipe", "leak"))
        else "general_service"
    )
    requested_time = "tomorrow" if "tomorrow" in lowered else ""
    location = _secretary_extract_city(normalized_text)
    callback_requested = "call me back" in lowered
    return {
        "summary": normalized_text,
        "text": normalized_text,
        "contact_id": contact_id,
        "source_channel": source_channel,
        "request": normalized_text,
        "service_type": service_type,
        "requested_time": requested_time,
        "location": location,
        "callback_requested": callback_requested,
    }


def _secretary_intent_and_payload(
    message: IncomingMessage,
    contact: dict[str, object],
) -> tuple[str, dict[str, object]]:
    normalized_text = str(message.raw_text or "").strip()
    lowered = normalized_text.lower()
    payload: dict[str, object] = {
        "summary": normalized_text,
        "text": normalized_text,
        "contact_id": message.contact_id,
        "source_channel": message.channel,
    }

    outbound_phrases = (
        "email client",
        "send email",
        "call client and say",
        "call customer and say",
    )
    if any(phrase in lowered for phrase in outbound_phrases):
        outbound_text = normalized_text
        outbound_channel = "email"
        for prefix in (
            "email client that ",
            "send email that ",
            "call client and say ",
            "call customer and say ",
        ):
            if lowered.startswith(prefix):
                outbound_text = normalized_text[len(prefix) :].strip()
                break
        if any(
            phrase in lowered
            for phrase in ("call client and say", "call customer and say")
        ):
            outbound_channel = "phone"
        payload["message_text"] = outbound_text or normalized_text
        payload["outbound_channel"] = outbound_channel
        if outbound_channel == "email":
            payload["subject"] = "Digital Foreman update"
            payload["summary"] = f"Send email: {payload['message_text']}"
        else:
            payload["summary"] = f"Call client: {payload['message_text']}"
        return "outbound_message", payload

    if any(
        phrase in lowered
        for phrase in (
            "Ð½Ð°Ð¿Ð¸ÑˆÐ¸ ÐºÐ»Ð¸ÐµÐ½Ñ‚Ñƒ",
            "message client",
            "text client",
            "sms client",
            "send sms",
        )
    ):
        outbound_text = normalized_text
        for prefix in (
            "Ð½Ð°Ð¿Ð¸ÑˆÐ¸ ÐºÐ»Ð¸ÐµÐ½Ñ‚Ñƒ, Ñ‡Ñ‚Ð¾ ",
            "Ð½Ð°Ð¿Ð¸ÑˆÐ¸ ÐºÐ»Ð¸ÐµÐ½Ñ‚Ñƒ Ñ‡Ñ‚Ð¾ ",
            "message client that ",
            "text client that ",
            "send sms that ",
        ):
            if lowered.startswith(prefix):
                outbound_text = normalized_text[len(prefix) :].strip()
                break
        payload["message_text"] = outbound_text or normalized_text
        payload["summary"] = f"Send SMS: {payload['message_text']}"
        return "outbound_message", payload

    if any(
        phrase in lowered
        for phrase in ("new lead", "lead", "new customer", "new client")
    ):
        phone_numbers = [
            str(item).strip()
            for item in contact.get("phone_numbers", [])
            if str(item).strip()
        ]
        payload.update(
            {
                "name": _secretary_extract_name(normalized_text),
                "phone": phone_numbers[0] if phone_numbers else "",
                "address": _secretary_extract_address(normalized_text),
                "request": normalized_text,
                "lead_summary": normalized_text,
            }
        )
        return "new_lead", payload

    if any(
        phrase in lowered
        for phrase in (
            "leaking faucet",
            "leak",
            "faucet",
            "plumber",
            "plumbing",
            "fix",
            "repair",
        )
    ):
        return (
            "service_request",
            _secretary_service_request_payload(
                normalized_text,
                message.contact_id,
                message.channel,
            ),
        )

    return "generic_task", payload


def _secretary_task_request(
    message: IncomingMessage,
    *,
    contact: dict[str, object],
) -> object:
    normalized_message = message.raw_text
    task_intent, task_payload = _secretary_intent_and_payload(message, contact)
    intake_context = (
        f"secretary_channel={message.channel}\n"
        f"contact_id={message.contact_id}\n"
        f"message_timestamp={message.timestamp}"
    )
    return SimpleNamespace(
        goal=normalized_message,
        intent=task_intent,
        payload=task_payload,
        context=intake_context,
        depends_on="",
        constraints="",
        expected_output=normalized_message,
        change_type="review",
        priority="normal",
        client_id="default",
        contact_id=message.contact_id,
        source_channel=message.channel,
        linear_task_id=f"SEC-{uuid.uuid4().hex[:10]}",
        linear_task_title=normalized_message[:80].strip() or "Secretary task",
        mvp_priority="P1",
        expected_result=normalized_message,
        done_condition="Secretary task is processed through the business task flow.",
        done_condition_met=False,
        entry_point="secretary",
        confirmed=True,
        timeout_seconds=30.0,
        max_retry=1,
    )


def _secretary_summary(message: IncomingMessage) -> str:
    return str(message.raw_text or "").strip()


def _secretary_confirmation_prompt(message: IncomingMessage) -> str:
    return (
        f"\u042f \u0437\u0430\u043f\u0438\u0441\u0430\u043b\u0430:\n"
        f"{_secretary_summary(message)}\n"
        "\u0412\u0441\u0451 \u0432\u0435\u0440\u043d\u043e?"
    )


def _secretary_yes(value: object) -> bool:
    normalized = str(value or "").strip().lower()
    return normalized in {"yes", "y", "Ð´Ð°", "Ð°Ð³Ð°", "Ð²ÐµÑ€Ð½Ð¾", "correct"}


def _secretary_no(value: object) -> bool:
    normalized = str(value or "").strip().lower()
    return normalized in {"no", "n", "Ð½ÐµÑ‚", "Ð½ÐµÐ²ÐµÑ€Ð½Ð¾", "wrong"}


def _serialize_incoming_message(message: IncomingMessage) -> str:
    return json.dumps(message.as_dict(), ensure_ascii=False)


def _deserialize_incoming_message(value: object) -> IncomingMessage | None:
    raw_value = str(value or "").strip()
    if not raw_value:
        return None
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    try:
        return normalize_incoming_message(
            channel=payload.get("channel", ""),
            contact_id=payload.get("contact_id", ""),
            raw_text=payload.get("raw_text", ""),
            timestamp=payload.get("timestamp", ""),
        )
    except ValueError:
        return None


def _secretary_confirmation_response(
    *,
    session: dict[str, str],
    incoming_message: IncomingMessage,
    contact: dict[str, object],
) -> dict[str, object]:
    task_intent, task_payload = _secretary_intent_and_payload(incoming_message, contact)
    summary_text = str(task_payload.get("summary") or incoming_message.raw_text).strip()
    prompt = f"ÃÂ¯ ÃÂ·ÃÂ°ÃÂ¿ÃÂ¸Ã‘ÂÃÂ°ÃÂ»ÃÂ°:\n{summary_text}\nÃâ€™Ã‘ÂÃ‘â€˜ ÃÂ²ÃÂµÃ‘â‚¬ÃÂ½ÃÂ¾?"
    history = find_recent_tasks_by_contact_id(contact.get("contact_id", ""), limit=3)
    updated_session = update_session(
        session["session_id"],
        user_id=session.get("user_id", ""),
        user_role=session.get("user_role", ""),
        current_mode="secretary_confirmation",
        interaction_state="pending_confirmation",
        last_intent="secretary_confirmation",
        last_response_summary=prompt,
        box_state="awaiting_confirmation",
        pending_confirmation_payload=_serialize_incoming_message(incoming_message),
    )
    return {
        "session_id": updated_session["session_id"],
        "interaction_state": "pending_confirmation",
        "status": "awaiting_confirmation",
        "confirmation_required": True,
        "confirmation_prompt": prompt,
        "incoming_message": incoming_message.as_dict(),
        "intent": task_intent,
        "payload": task_payload,
        "contact": dict(contact),
        "contact_history": [
            {
                "task_id": str(item.get("task_id", "")).strip(),
                "goal": str(item.get("goal", "")).strip(),
                "status": str(item.get("status", "")).strip(),
                "timestamp": str(item.get("timestamp", "")).strip(),
            }
            for item in history
        ],
        "task_created": False,
        "create_task_skip_reason": "confirmation_not_final",
    }


def _secretary_clarification_response(*, session: dict[str, str]) -> dict[str, object]:
    prompt = (
        "\u041e\u043f\u0438\u0448\u0438\u0442\u0435 \u0437\u0430\u0434\u0430\u0447\u0443 "
        "\u0435\u0449\u0451 \u0440\u0430\u0437, \u0438 \u044f "
        "\u043f\u043e\u0432\u0442\u043e\u0440\u044e \u0435\u0451 \u0434\u043b\u044f "
        "\u043f\u043e\u0434\u0442\u0432\u0435\u0440\u0436\u0434\u0435\u043d\u0438\u044f."
    )
    updated_session = update_session(
        session["session_id"],
        user_id=session.get("user_id", ""),
        user_role=session.get("user_role", ""),
        current_mode="secretary_confirmation",
        interaction_state="waiting_for_followup",
        last_intent="secretary_clarification",
        last_response_summary=prompt,
        box_state="awaiting_confirmation",
        pending_confirmation_payload="",
    )
    return {
        "session_id": updated_session["session_id"],
        "interaction_state": "waiting_for_followup",
        "status": "needs_clarification",
        "confirmation_required": False,
        "message": prompt,
        "task_created": False,
        "create_task_skip_reason": "confirmation_rejected",
    }


def _secretary_trace_state(session: dict[str, object] | None) -> dict[str, str]:
    state = session or {}
    return {
        "current_mode": str(state.get("current_mode", "")).strip(),
        "interaction_state": str(state.get("interaction_state", "")).strip(),
        "box_state": str(state.get("box_state", "")).strip(),
        "last_intent": str(state.get("last_intent", "")).strip(),
        "last_task_id": str(state.get("last_task_id", "")).strip(),
    }


def _secretary_trace_task_creation(
    *,
    session_id: object,
    state_before: dict[str, object] | None,
    state_after: dict[str, object] | None,
    intent_final: object,
    service_type: object,
    create_task_eligible: bool,
    create_task_skip_reason: object,
    create_task_handler_called: bool,
    task_created: bool,
) -> None:
    trace_payload = {
        "session_id": str(session_id or "").strip(),
        "state_before": _secretary_trace_state(state_before),
        "state_after": _secretary_trace_state(state_after),
        "intent_final": str(intent_final or "").strip(),
        "service_type": str(service_type or "").strip(),
        "create_task_eligible": bool(create_task_eligible),
        "create_task_skip_reason": str(create_task_skip_reason or "").strip(),
        "create_task_handler_called": bool(create_task_handler_called),
        "task_created": bool(task_created),
    }
    print("SECRETARY_TASK_CREATION_TRACE:")
    print(json.dumps(trace_payload, indent=2, ensure_ascii=False))


def check_permissions(
    *,
    user_id: object,
    user_role: object,
    action: str,
    authorization: str | None = None,
    x_df_principal_token: str | None = None,
) -> dict[str, str]:
    token = _extract_principal_token(
        authorization=authorization,
        x_df_principal_token=x_df_principal_token,
    )
    if token:
        principal = authenticate_principal_token(token)
        return authorize_product_action(
            user_id=principal["actor_id"],
            user_role=principal["role"],
            action=action,
        )
    return authorize_product_action(
        user_id=user_id,
        user_role=user_role,
        action=action,
    )


def _build_product_task(payload: ProductTaskRunRequest) -> object:
    raise ProductRuntimeBoundaryError(
        "product runtime boundary violation: product API task building cannot use dev tasks"
    )


def _queued_task_response(result: dict[str, object]) -> dict[str, object]:
    return {
        "task_id": str(result.get("task_id", "")).strip(),
        "job_id": str(result.get("job_id") or result.get("task_id", "")).strip(),
        "status": str(result.get("status", "")).strip(),
        "execution_mode": str(result.get("execution_mode", "")).strip(),
        "execution_location": str(result.get("execution_location", "")).strip(),
        "offload_latency": result.get("offload_latency"),
        "routing_reason": result.get("routing_reason"),
        "telemetry_snapshot": result.get("telemetry_snapshot"),
        "safety_override": result.get("safety_override"),
        "network_snapshot": result.get("network_snapshot"),
        "network_policy": result.get("network_policy"),
        "workflow_phase": str(result.get("workflow_phase", "")).strip(),
        "current_step": int(result.get("current_step", 0)),
        "linear_task": str(result.get("linear_task", "")).strip(),
        "linear_status": str(result.get("linear_status", "")).strip(),
        "done_condition_met": bool(result.get("done_condition_met", False)),
        "next_step": str(result.get("next_step", "")).strip(),
        "last_step_result": dict(result.get("last_result", {}))
        if isinstance(result.get("last_result"), dict)
        else {},
    }


def _augment_system_report(
    result: dict[str, object], *, request_source: str
) -> dict[str, object]:
    updated = dict(result)
    system_report = dict(updated.get("system_report", {}))
    system_report["api_called"] = True
    system_report["request_source"] = str(request_source).strip() or "api"
    system_report["lifecycle_state"] = str(updated.get("lifecycle_state", "")).strip()
    updated["system_report"] = system_report
    return updated


def _product_task_summary(record: dict[str, object]) -> dict[str, object]:
    system_report = dict(record.get("system_report", {}))
    return {
        "task_id": str(record.get("task_id", "")).strip(),
        "status": str(record.get("acceptance_status", "")).strip()
        or str(record.get("validation_status", "")).strip()
        or str(record.get("selected_agent", "")).strip(),
        "lifecycle_state": str(record.get("lifecycle_state", "")).strip(),
        "recorded_at": str(record.get("recorded_at", "")).strip(),
        "objective": str(record.get("objective", "")).strip(),
        "request_source": str(system_report.get("request_source", "")).strip(),
    }


def _product_status_from_result(result: dict[str, object]) -> str:
    return (
        str(result.get("acceptance_status", "")).strip()
        or str(result.get("validation_status", "")).strip()
        or str(result.get("selected_agent", "")).strip()
    )


def _operator_last_update(task: dict[str, object]) -> str:
    history = list(task.get("history", []))
    if not history:
        return ""
    last_item = dict(history[-1])
    return str(last_item.get("timestamp", "")).strip()


def _operator_task_item(task: dict[str, object]) -> dict[str, object]:
    return {
        "task_id": str(task.get("task_id", "")).strip(),
        "intent": str(task.get("intent", "")).strip(),
        "status": str(task.get("status", "")).strip(),
        "created_at": str(task.get("created_at", "")).strip(),
        "last_update": _operator_last_update(task),
        "summary": build_task_summary(task),
        "contact_id": str(task.get("contact_id", "")).strip()
        or str(dict(task.get("payload", {})).get("contact_id", "")).strip(),
    }


def _filter_operator_tasks(
    tasks: list[dict[str, object]],
    *,
    status: str,
) -> list[dict[str, object]]:
    normalized_status = str(status or "").strip().lower()
    if not normalized_status:
        return tasks
    if normalized_status == "open":
        allowed_statuses = {"created", "confirmed", "pending", "running"}
    elif normalized_status in {"done", "failed"}:
        allowed_statuses = {normalized_status}
    else:
        raise HTTPException(
            status_code=400, detail="status must be one of: open, done, failed"
        )
    return [
        dict(task)
        for task in tasks
        if str(task.get("status", "")).strip().lower() in allowed_statuses
    ]


def _resolve_lifecycle_state(result: dict[str, object]) -> str:
    acceptance_status = str(result.get("acceptance_status", "")).strip().lower()
    output_valid = bool(result.get("output_valid", True))
    atomic_apply = bool(result.get("atomic_apply", False))
    apply_attempted = bool(result.get("apply_attempted", False))
    apply_allowed = bool(result.get("apply_allowed", False))
    changes = tuple(
        dict(item) for item in result.get("agent_output", {}).get("changes", ())
    )
    has_changes = any(
        str(change.get("action", "")).strip() in {"modify", "propose"}
        for change in changes
    )

    if acceptance_status == "review_required":
        return "review_required"
    if acceptance_status == "rejected" or not output_valid:
        return "failed"
    if acceptance_status == "accepted":
        if apply_attempted:
            return "completed" if atomic_apply else "failed"
        if not has_changes or not apply_allowed:
            return "completed"
        return "failed"
    return "failed"


def _set_lifecycle(
    result: dict[str, object],
    *,
    lifecycle_state: str,
    lifecycle_history: list[str] | tuple[str, ...],
) -> dict[str, object]:
    updated = dict(result)
    normalized_state = str(lifecycle_state).strip()
    if normalized_state not in TASK_LIFECYCLE_STATES:
        normalized_state = "failed"
    updated["lifecycle_state"] = normalized_state
    updated["lifecycle_history"] = tuple(
        str(item).strip() for item in lifecycle_history if str(item).strip()
    )
    return updated


def _failed_product_result(
    *, task_id: str, objective: str, detail: str, request_source: str = "api"
) -> dict[str, object]:
    return {
        "task_id": task_id,
        "objective": objective,
        "acceptance_status": "rejected",
        "validation_status": "failed",
        "output_valid": False,
        "apply_allowed": False,
        "apply_attempted": False,
        "apply_success_count": 0,
        "atomic_apply": False,
        "semantic_risk_level": "high",
        "agent_output": {"changes": (), "notes": ()},
        "system_status_text": f"Task:\n- id: {task_id}\n\nFailure:\n- {detail}",
        "system_report": {
            "task": {"task_id": task_id},
            "api_called": True,
            "request_source": request_source,
            "lifecycle_state": "failed",
            "failure_detail": detail,
        },
    }


def _normalize_voice_text(value: object) -> str:
    return " ".join(str(value or "").split())


def _extract_task_id_from_text(value: object) -> str:
    text = str(value or "").strip()
    match = re.search(r"\b((?:api|dev)-[A-Za-z0-9_-]+)\b", text, flags=re.IGNORECASE)
    if not match:
        return ""
    return match.group(1).strip()


def _voice_intent(payload: VoiceEntryRequest) -> str:
    transcript = _normalize_voice_text(payload.transcript)
    lowered = transcript.lower()
    explicit_task_id = str(payload.task_id or "").strip() or _extract_task_id_from_text(
        transcript
    )
    if not transcript and not explicit_task_id:
        return "help"
    if any(keyword in lowered for keyword in VOICE_REPEAT_KEYWORDS):
        return "repeat"
    if explicit_task_id and any(
        keyword in lowered for keyword in VOICE_STATUS_KEYWORDS
    ):
        return "read_task"
    if any(keyword in lowered for keyword in VOICE_HISTORY_KEYWORDS):
        return "read_task_history"
    if explicit_task_id:
        return "read_task"
    if any(keyword in lowered for keyword in VOICE_STATUS_KEYWORDS):
        return "read_task"
    if any(keyword in lowered for keyword in VOICE_HELP_KEYWORDS):
        return "help"
    return "run_task"


def _session_mode_for_intent(intent: str) -> str:
    return interaction_session_mode_for_intent(intent)


def _resolve_follow_up_task_id(
    *,
    payload: VoiceEntryRequest,
    session: dict[str, str] | None,
    intent: str,
) -> str:
    explicit_task_id = str(payload.task_id or "").strip() or _extract_task_id_from_text(
        payload.transcript
    )
    if explicit_task_id:
        return explicit_task_id
    if intent == "read_task" and session is not None:
        return str(session.get("last_task_id", "")).strip()
    return ""


def _session_context_block(session: dict[str, str] | None) -> dict[str, str]:
    return interaction_session_context_block(session)


def _update_voice_session(
    *,
    session: dict[str, str],
    payload: VoiceEntryRequest,
    intent: str,
    current_mode: str,
    interaction_state: str = "",
    task_id: str = "",
    response_summary: str = "",
    box_state: str | None = None,
    pending_objective: str | None = None,
) -> dict[str, str]:
    return interaction_update_voice_session_state(
        session=session,
        payload=payload,
        intent=intent,
        current_mode=current_mode,
        interaction_state=interaction_state,
        task_id=task_id,
        response_summary=response_summary,
        box_state=box_state,
        pending_objective=pending_objective,
    )


def _transition_voice_session(
    *,
    session: dict[str, str],
    payload: VoiceEntryRequest,
    intent: str,
    current_mode: str,
    interaction_state: str,
    task_id: str = "",
    response_summary: str = "",
) -> dict[str, str]:
    return _update_voice_session(
        session=session,
        payload=payload,
        intent=intent,
        current_mode=current_mode,
        interaction_state=interaction_state,
        task_id=task_id,
        response_summary=response_summary,
    )


def _advance_voice_session(
    *,
    session: dict[str, str],
    payload: VoiceEntryRequest,
    intent: str,
    current_mode: str,
    states: tuple[str, ...] | list[str],
    task_id: str = "",
    response_summary: str = "",
    box_state: str | None = None,
    pending_objective: str | None = None,
) -> dict[str, str]:
    return interaction_advance_voice_session(
        session=session,
        payload=payload,
        intent=intent,
        current_mode=current_mode,
        states=states,
        task_id=task_id,
        response_summary=response_summary,
        box_state=box_state,
        pending_objective=pending_objective,
    )


def _display_short_status(
    *,
    interaction_state: str,
    current_mode: str,
    lifecycle_state: str = "",
    has_task: bool = False,
) -> str:
    return interaction_display_short_status(
        interaction_state=interaction_state,
        current_mode=current_mode,
        lifecycle_state=lifecycle_state,
        has_task=has_task,
    )


def _suggested_replies_for_context(
    *,
    current_mode: str,
    interaction_state: str,
    last_task_id: str,
) -> list[str]:
    return interaction_suggested_replies_for_context(
        current_mode=current_mode,
        interaction_state=interaction_state,
        last_task_id=last_task_id,
    )


def _find_product_record(task_id: str) -> dict[str, object] | None:
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id:
        return None

    direct_match = find_execution_record(normalized_task_id)
    if direct_match is not None:
        return direct_match

    lowered_task_id = normalized_task_id.lower()
    for record in reversed(load_execution_records()):
        if str(record.get("task_id", "")).strip().lower() == lowered_task_id:
            return record
    stored_task = find_task_by_id(normalized_task_id)
    if stored_task is not None:
        return stored_task
    return None


def _materialize_product_record(
    record: dict[str, object],
    *,
    request_source: str,
) -> dict[str, object]:
    normalized_record = dict(record)
    if not str(normalized_record.get("lifecycle_state", "")).strip():
        normalized_record = _set_lifecycle(
            normalized_record,
            lifecycle_state=_resolve_lifecycle_state(normalized_record),
            lifecycle_history=(
                normalized_record.get("lifecycle_state", "")
                or _resolve_lifecycle_state(normalized_record),
            ),
        )
    normalized_record = _augment_system_report(
        normalized_record,
        request_source=request_source,
    )
    if not str(normalized_record.get("user_summary", "")).strip():
        normalized_record["user_summary"] = (
            str(normalized_record.get("system_status_text", "")).strip()
            or str(normalized_record.get("result_summary", "")).strip()
            or str(normalized_record.get("task_id", "")).strip()
        )
    return normalized_record


def _run_product_task_internal(
    payload: ProductTaskRunRequest,
    *,
    principal: dict[str, str],
    request_source: str,
) -> dict[str, object]:
    descriptor_action = str(getattr(payload, "descriptor_action", "") or "").strip()
    if descriptor_action:
        request = {
            "task_id": str(getattr(payload, "task_id", "") or "").strip(),
            "objective": str(getattr(payload, "objective", "") or "").strip(),
            "scope_files": list(getattr(payload, "scope_files", []) or []),
            "descriptor_path": str(
                getattr(payload, "descriptor_path", "") or ""
            ).strip(),
            "descriptor_action": descriptor_action,
        }
    else:
        request = build_product_task_request(payload)
    if str(request.get("task_id", "")).strip():
        task_id = str(request.get("task_id", "")).strip()
        descriptor_path = str(request.get("descriptor_path", "")).strip()
        descriptor_action = str(request.get("descriptor_action", "")).strip()
        scope_files = list(request.get("scope_files", []) or [])
        if descriptor_action in {"WRITE_FILE", "READ_FILE"}:
            execution_ready = build_execution_ready(payload)
            validation_gate = validate_action_trigger(execution_ready)
            if bool(validation_gate.get("valid", False)):
                action_result = dispatch_action_trigger(
                    dict(validation_gate.get("trigger", {}) or {}),
                    task_state={
                        "task_id": task_id,
                        "status": "running",
                    },
                )
            else:
                action_result = build_typed_action_result(
                    status="error",
                    action_type=str(execution_ready.get("action_type", "")).strip(),
                    result_type="validation_rejected",
                    result_summary=str(validation_gate.get("reason", "")).strip(),
                    task_id=task_id,
                )
        elif descriptor_action in {
            "RUN_TESTS",
            "BUILD_WEBSITE",
            "SYSTEM_STATUS",
            "RESOURCES",
        }:
            validation_gate = {
                "valid": True,
                "reason": "",
                "trigger": {
                    "task_id": task_id,
                    "descriptor_action": descriptor_action,
                },
            }
            execution_ready = {
                "task_id": task_id,
                "descriptor_path": descriptor_path,
                "action_type": descriptor_action,
                "payload": {},
            }
            action_result = dispatch_development_action(
                action_type=descriptor_action,
                task_id=task_id,
            )
        else:
            validation_gate = {
                "valid": True,
                "reason": "",
                "trigger": {
                    "task_id": task_id,
                    "descriptor_action": descriptor_action,
                },
            }
            execution_ready = {
                "task_id": task_id,
                "descriptor_path": descriptor_path,
                "action_type": descriptor_action,
                "payload": {},
            }
            action_result = build_typed_action_result(
                status="completed",
                action_type=descriptor_action,
                result_type="orchestrated_action",
                result_summary=f"{descriptor_action or 'action'} completed",
                task_id=task_id,
            )
        lifecycle_state = (
            "completed"
            if str(action_result.get("status", "")).strip() == "completed"
            else "failed"
        )
        result = {
            "task_id": task_id,
            "selected_agent": "orchestrated",
            "acceptance_status": (
                "orchestrated" if lifecycle_state == "completed" else "rejected"
            ),
            "validation_status": (
                "passed" if bool(validation_gate.get("valid", False)) else "failed"
            ),
            "lifecycle_state": lifecycle_state,
            "lifecycle_history": (
                "created",
                "running",
                lifecycle_state,
            ),
            "objective": str(request.get("objective", "")).strip(),
            "system_status_text": str(action_result.get("result_summary", "")).strip()
            or "orchestrated",
            "user_summary": task_id,
            "system_report": {
                "descriptor_path": descriptor_path,
                "descriptor_action": descriptor_action,
                "scope_files": scope_files,
                "request_source": str(request_source).strip() or "api",
                "api_called": True,
                "lifecycle_state": lifecycle_state,
                "execution_ready": execution_ready,
                "validation_gate": validation_gate,
                "action_result": action_result,
            },
            "action_result": action_result,
            "status": str(action_result.get("status", "")).strip() or "error",
        }
        log_task_execution(
            task_id=task_id,
            status=str(action_result.get("status", "")).strip() or "error",
            result_type=str(action_result.get("result_type", "")).strip() or "unknown",
        )
        if lifecycle_state == "completed":
            store_task_result(
                {
                    "task_id": task_id,
                    "result_type": str(action_result.get("result_type", "")).strip(),
                    "result_summary": str(
                        action_result.get("result_summary", "")
                    ).strip(),
                }
            )
        return _materialize_product_record(
            result,
            request_source=request_source,
        )

    boundary_message = (
        "product runtime boundary violation: unmapped product requests "
        "cannot fall through to dev execution"
    )
    logger.warning(boundary_message)
    return _materialize_product_record(
        _set_lifecycle(
            _failed_product_result(
                task_id=str(getattr(payload, "task_id", "") or "").strip(),
                objective=str(getattr(payload, "objective", "") or "").strip(),
                detail=boundary_message,
                request_source=request_source,
            ),
            lifecycle_state="failed",
            lifecycle_history=["created", "failed"],
        ),
        request_source=request_source,
    )


def _product_api_result(result: dict[str, object]) -> dict[str, object]:
    if str(result.get("status", "")).strip() == "rejected":
        return {
            "status": "rejected",
            "reason": str(result.get("reason", "")).strip(),
        }
    return {
        "task_id": str(result.get("task_id", "")).strip(),
        "status": _product_status_from_result(result),
        "lifecycle_state": str(result.get("lifecycle_state", "")).strip(),
        "system_status_text": str(result.get("system_status_text", "")).strip(),
        "user_summary": str(result.get("user_summary", "")).strip(),
        "system_report": dict(result.get("system_report", {})),
    }


def _voice_display_card(
    *,
    title: str,
    body: str,
    status: str,
    state: str,
    short_status: str,
) -> dict[str, object]:
    return {
        "title": str(title).strip(),
        "body": str(body).strip(),
        "status": str(status).strip(),
        "state": str(state).strip(),
        "short_status": str(short_status).strip(),
    }


def _voice_response(
    *,
    intent: str,
    interaction_state: str,
    spoken_response: str,
    display: dict[str, object],
    suggested_replies: list[str],
    session: dict[str, str] | None = None,
    task: dict[str, object] | None = None,
    history: dict[str, object] | None = None,
) -> dict[str, object]:
    session_payload = dict(session or {})
    display_payload = dict(display)
    display_payload["session_context"] = _session_context_block(session_payload)
    response_interaction_state = (
        str(session_payload.get("interaction_state", "")).strip()
        or str(interaction_state).strip()
        or "idle"
    )
    response = {
        "mode": "voice_first",
        "intent": intent,
        "interaction_state": response_interaction_state,
        "spoken_response": spoken_response.strip(),
        "display": display_payload,
        "suggested_replies": [
            str(item).strip() for item in suggested_replies if str(item).strip()
        ],
        "session_id": str(session_payload.get("session_id", "")).strip(),
        "current_mode": str(session_payload.get("current_mode", "idle")).strip()
        or "idle",
        "box": box_response_meta(session_payload),
    }
    if task is not None:
        response["task"] = task
    if history is not None:
        response["history"] = history
    return response


def _voice_history_response(
    items: list[dict[str, object]],
    *,
    session: dict[str, str],
) -> dict[str, object]:
    if not items:
        return _voice_response(
            intent="read_task_history",
            interaction_state="completed",
            spoken_response="Ð£ Ð¼ÐµÐ½Ñ Ð¿Ð¾ÐºÐ° Ð½ÐµÑ‚ Ð¿Ð¾ÑÐ»ÐµÐ´Ð½Ð¸Ñ… Ð·Ð°Ð´Ð°Ñ‡ Ð´Ð»Ñ Ð¾Ñ‚Ñ‡Ñ‘Ñ‚Ð°.",
            display=_voice_display_card(
                title="No Recent Tasks",
                body="No product tasks have been recorded yet.",
                status="empty",
            ),
            suggested_replies=[
                "Run a scoped task",
                "Show help",
            ],
            session=session,
            history={"count": 0, "items": []},
        )

    latest = items[0]
    spoken_lines = [
        f"ÐŸÐ¾ÑÐ»ÐµÐ´Ð½ÑÑ Ð·Ð°Ð´Ð°Ñ‡Ð° {latest.get('task_id', '')} ÑÐµÐ¹Ñ‡Ð°Ñ Ð² ÑÐ¾ÑÑ‚Ð¾ÑÐ½Ð¸Ð¸ {latest.get('lifecycle_state', 'unknown')}."
    ]
    if len(items) > 1:
        spoken_lines.append(
            f"Ð’ÑÐµÐ³Ð¾ Ð² Ð¾Ñ‚Ñ‡Ñ‘Ñ‚Ðµ {len(items)} Ð¿Ð¾ÑÐ»ÐµÐ´Ð½Ð¸Ñ… Ð·Ð°Ð´Ð°Ñ‡."
        )
    body = "\n".join(
        f"{item.get('task_id', '')}: {item.get('lifecycle_state', '')} - {item.get('objective', '')}"
        for item in items[:3]
    )
    return _voice_response(
        intent="read_task_history",
        interaction_state="completed",
        spoken_response=" ".join(spoken_lines).strip(),
        display=_voice_display_card(
            title="Recent Tasks",
            body=body,
            status="completed",
        ),
        suggested_replies=[
            "Open the latest task",
            "Run a new scoped task",
        ],
        session=session,
        history={"count": len(items), "items": items},
    )


def _voice_first_entry_with_sessions(
    payload: VoiceEntryRequest,
    *,
    authorization: str | None,
    x_df_principal_token: str | None,
) -> dict[str, object]:
    intent = _voice_intent(payload)
    transcript = _normalize_voice_text(payload.transcript)
    session, _ = get_or_create_session(
        session_id=payload.session_id,
        user_id=payload.user_id,
        user_role=payload.user_role,
    )
    resume_scope_collection = should_resume_scope_collection(
        session=session,
        transcript=transcript,
        scope_files=payload.scope_files,
        intent=intent,
    )
    if resume_scope_collection:
        intent = "run_task"
    resolved_task_id = _resolve_follow_up_task_id(
        payload=payload,
        session=session,
        intent=intent,
    )

    if intent == "help":
        session = _update_voice_session(
            session=session,
            payload=payload,
            intent="help",
            current_mode="help",
            interaction_state="waiting_for_followup",
            response_summary=(
                "Ð’Ñ‹ Ð¼Ð¾Ð¶ÐµÑ‚Ðµ Ð¿Ð¾Ð¿Ñ€Ð¾ÑÐ¸Ñ‚ÑŒ Ð¼ÐµÐ½Ñ Ð·Ð°Ð¿ÑƒÑÑ‚Ð¸Ñ‚ÑŒ scoped task, Ð¿Ð¾ÐºÐ°Ð·Ð°Ñ‚ÑŒ ÑÑ‚Ð°Ñ‚ÑƒÑ Ð·Ð°Ð´Ð°Ñ‡Ð¸ "
                "Ð¸Ð»Ð¸ Ð¾Ð·Ð²ÑƒÑ‡Ð¸Ñ‚ÑŒ Ð¿Ð¾ÑÐ»ÐµÐ´Ð½Ð¸Ðµ Ð·Ð°Ð´Ð°Ñ‡Ð¸."
            ),
        )
        return _voice_response(
            intent="help",
            interaction_state="completed",
            spoken_response=(
                "Ð’Ñ‹ Ð¼Ð¾Ð¶ÐµÑ‚Ðµ Ð¿Ð¾Ð¿Ñ€Ð¾ÑÐ¸Ñ‚ÑŒ Ð¼ÐµÐ½Ñ Ð·Ð°Ð¿ÑƒÑÑ‚Ð¸Ñ‚ÑŒ Ð·Ð°Ð´Ð°Ñ‡Ñƒ Ð¿Ð¾ Ð²Ñ‹Ð±Ñ€Ð°Ð½Ð½Ñ‹Ð¼ Ñ„Ð°Ð¹Ð»Ð°Ð¼, "
                "Ð¿Ð¾ÐºÐ°Ð·Ð°Ñ‚ÑŒ ÑÑ‚Ð°Ñ‚ÑƒÑ Ð·Ð°Ð´Ð°Ñ‡Ð¸ Ð¸Ð»Ð¸ Ð¾Ð·Ð²ÑƒÑ‡Ð¸Ñ‚ÑŒ Ð¿Ð¾ÑÐ»ÐµÐ´Ð½Ð¸Ðµ Ð·Ð°Ð´Ð°Ñ‡Ð¸."
            ),
            display=_voice_display_card(
                title="Voice Entry",
                body=(
                    "Try: 'Run a scoped task for api/app.py', "
                    "'Status for api-123', or 'Show recent tasks'."
                ),
                status="ready",
            ),
            suggested_replies=[
                "Run a scoped task",
                "ÐŸÐ¾ÐºÐ°Ð¶Ð¸ ÑÑ‚Ð°Ñ‚ÑƒÑ",
                "ÐŸÐ¾ÐºÐ°Ð¶Ð¸ Ð¿Ð¾ÑÐ»ÐµÐ´Ð½Ð¸Ðµ Ð·Ð°Ð´Ð°Ñ‡Ð¸",
            ],
            session=session,
        )

    if intent == "repeat":
        repeated_summary = str(session.get("last_response_summary", "")).strip()
        if not repeated_summary:
            repeated_summary = (
                "ÐœÐ½Ðµ Ð¿Ð¾ÐºÐ° Ð½ÐµÑ‡ÐµÐ³Ð¾ Ð¿Ð¾Ð²Ñ‚Ð¾Ñ€Ð¸Ñ‚ÑŒ Ð² ÑÑ‚Ð¾Ð¹ ÑÐµÑÑÐ¸Ð¸."
            )
        session = _update_voice_session(
            session=session,
            payload=payload,
            intent="repeat",
            current_mode=str(session.get("current_mode", "help")).strip() or "help",
            interaction_state="waiting_for_followup",
            task_id=str(session.get("last_task_id", "")).strip(),
            response_summary=repeated_summary,
        )
        return _voice_response(
            intent="repeat",
            interaction_state="completed",
            spoken_response=repeated_summary,
            display=_voice_display_card(
                title="Repeat",
                body=repeated_summary,
                status="completed",
            ),
            suggested_replies=[
                "Ð§Ñ‚Ð¾ Ð´Ð°Ð»ÑŒÑˆÐµ",
                "ÐŸÐ¾ÐºÐ°Ð¶Ð¸ Ð¿Ð¾ÑÐ»ÐµÐ´Ð½Ð¸Ðµ Ð·Ð°Ð´Ð°Ñ‡Ð¸",
            ],
            session=session,
        )

    if intent == "read_task_history":
        check_permissions(
            user_id=payload.user_id,
            user_role=payload.user_role,
            action="read_task_history",
            authorization=authorization,
            x_df_principal_token=x_df_principal_token,
        )
        records = list(reversed(load_execution_records()))
        items = [
            _product_task_summary(
                _materialize_product_record(record, request_source="voice")
            )
            for record in records[:5]
        ]
        history_response = _voice_history_response(items, session=session)
        session = _update_voice_session(
            session=session,
            payload=payload,
            intent="read_task_history",
            current_mode="history_review",
            interaction_state="waiting_for_followup",
            task_id=(
                str(items[0].get("task_id", "")).strip()
                if items
                else str(session.get("last_task_id", "")).strip()
            ),
            response_summary=str(history_response.get("spoken_response", "")).strip(),
        )
        history_response["session_id"] = session["session_id"]
        history_response["current_mode"] = session["current_mode"]
        history_response["display"]["session_context"] = _session_context_block(session)
        return history_response

    if intent == "read_task":
        check_permissions(
            user_id=payload.user_id,
            user_role=payload.user_role,
            action="read_task",
            authorization=authorization,
            x_df_principal_token=x_df_principal_token,
        )
        if not resolved_task_id:
            session = _update_voice_session(
                session=session,
                payload=payload,
                intent="read_task",
                current_mode="task_status",
                response_summary="ÐœÐ½Ðµ Ð½ÑƒÐ¶ÐµÐ½ Ð¸Ð´ÐµÐ½Ñ‚Ð¸Ñ„Ð¸ÐºÐ°Ñ‚Ð¾Ñ€ Ð·Ð°Ð´Ð°Ñ‡Ð¸, Ñ‡Ñ‚Ð¾Ð±Ñ‹ Ð¾Ð·Ð²ÑƒÑ‡Ð¸Ñ‚ÑŒ ÐµÑ‘ ÑÐ¾ÑÑ‚Ð¾ÑÐ½Ð¸Ðµ.",
            )
            return _voice_response(
                intent="read_task",
                interaction_state="needs_task_id",
                spoken_response="ÐœÐ½Ðµ Ð½ÑƒÐ¶ÐµÐ½ Ð¸Ð´ÐµÐ½Ñ‚Ð¸Ñ„Ð¸ÐºÐ°Ñ‚Ð¾Ñ€ Ð·Ð°Ð´Ð°Ñ‡Ð¸, Ñ‡Ñ‚Ð¾Ð±Ñ‹ Ð¾Ð·Ð²ÑƒÑ‡Ð¸Ñ‚ÑŒ ÐµÑ‘ ÑÐ¾ÑÑ‚Ð¾ÑÐ½Ð¸Ðµ.",
                display=_voice_display_card(
                    title="Task ID Needed",
                    body="Provide a task id like api-123abc or DEV-001.",
                    status="needs_input",
                ),
                suggested_replies=[
                    "ÐŸÐ¾ÐºÐ°Ð¶Ð¸ Ð¿Ð¾ÑÐ»ÐµÐ´Ð½Ð¸Ðµ Ð·Ð°Ð´Ð°Ñ‡Ð¸",
                    "Status for latest task",
                ],
                session=session,
            )
        record = _find_product_record(resolved_task_id)
        if record is None:
            session = _update_voice_session(
                session=session,
                payload=payload,
                intent="read_task",
                current_mode="task_status",
                response_summary=f"Ð¯ Ð½Ðµ Ð½Ð°ÑˆÐ»Ð° Ð·Ð°Ð´Ð°Ñ‡Ñƒ {resolved_task_id}.",
            )
            return _voice_response(
                intent="read_task",
                interaction_state="completed",
                spoken_response=f"Ð¯ Ð½Ðµ Ð½Ð°ÑˆÐ»Ð° Ð·Ð°Ð´Ð°Ñ‡Ñƒ {resolved_task_id}.",
                display=_voice_display_card(
                    title="Task Not Found",
                    body=f"No saved product task was found for {resolved_task_id}.",
                    status="missing",
                ),
                suggested_replies=[
                    "ÐŸÐ¾ÐºÐ°Ð¶Ð¸ Ð¿Ð¾ÑÐ»ÐµÐ´Ð½Ð¸Ðµ Ð·Ð°Ð´Ð°Ñ‡Ð¸",
                    "Run a scoped task",
                ],
                session=session,
            )
        rendered_record = _materialize_product_record(record, request_source="voice")
        task_summary = {
            "task_id": str(rendered_record.get("task_id", "")).strip(),
            "status": _product_status_from_result(rendered_record),
            "lifecycle_state": str(rendered_record.get("lifecycle_state", "")).strip(),
            "system_status_text": str(
                rendered_record.get("system_status_text", "")
            ).strip(),
            "user_summary": str(rendered_record.get("user_summary", "")).strip(),
            "system_report": dict(rendered_record.get("system_report", {})),
        }
        spoken_response = (
            f"Ð—Ð°Ð´Ð°Ñ‡Ð° {task_summary['task_id']} ÑÐµÐ¹Ñ‡Ð°Ñ Ð² ÑÐ¾ÑÑ‚Ð¾ÑÐ½Ð¸Ð¸ "
            f"{task_summary['lifecycle_state']}."
        )
        session = _update_voice_session(
            session=session,
            payload=payload,
            intent="read_task",
            current_mode="task_status",
            task_id=task_summary["task_id"],
            response_summary=spoken_response,
        )
        return _voice_response(
            intent="read_task",
            interaction_state="completed",
            spoken_response=spoken_response,
            display=_voice_display_card(
                title=f"Task {task_summary['task_id']}",
                body=task_summary["user_summary"],
                status=task_summary["lifecycle_state"],
            ),
            suggested_replies=[
                "Ð§Ñ‚Ð¾ Ð´Ð°Ð»ÑŒÑˆÐµ",
                "ÐŸÐ¾ÐºÐ°Ð¶Ð¸ Ð¿Ð¾ÑÐ»ÐµÐ´Ð½Ð¸Ðµ Ð·Ð°Ð´Ð°Ñ‡Ð¸",
            ],
            session=session,
            task=task_summary,
        )

    principal = check_permissions(
        user_id=payload.user_id,
        user_role=payload.user_role,
        action="run_task",
        authorization=authorization,
        x_df_principal_token=x_df_principal_token,
    )
    if not payload.scope_files:
        session = _update_voice_session(
            session=session,
            payload=payload,
            intent="run_task",
            current_mode="task_run",
            response_summary=(
                "Ð¯ Ð³Ð¾Ñ‚Ð¾Ð²Ð° Ð·Ð°Ð¿ÑƒÑÑ‚Ð¸Ñ‚ÑŒ Ð·Ð°Ð´Ð°Ñ‡Ñƒ, Ð½Ð¾ ÑÐ½Ð°Ñ‡Ð°Ð»Ð° ÑÐºÐ°Ð¶Ð¸Ñ‚Ðµ, Ð² ÐºÐ°ÐºÐ¸Ñ… Ñ„Ð°Ð¹Ð»Ð°Ñ… Ð¸Ð»Ð¸ "
                "Ð² ÐºÐ°ÐºÐ¾Ð¹ Ð¾Ð±Ð»Ð°ÑÑ‚Ð¸ Ð¿Ñ€Ð¾ÐµÐºÑ‚Ð° Ð¼Ð½Ðµ Ñ€Ð°Ð±Ð¾Ñ‚Ð°Ñ‚ÑŒ."
            ),
        )
        return _voice_response(
            intent="run_task",
            interaction_state="needs_scope",
            spoken_response=(
                "Ð¯ Ð³Ð¾Ñ‚Ð¾Ð²Ð° Ð·Ð°Ð¿ÑƒÑÑ‚Ð¸Ñ‚ÑŒ Ð·Ð°Ð´Ð°Ñ‡Ñƒ, Ð½Ð¾ ÑÐ½Ð°Ñ‡Ð°Ð»Ð° ÑÐºÐ°Ð¶Ð¸Ñ‚Ðµ, Ð² ÐºÐ°ÐºÐ¸Ñ… Ñ„Ð°Ð¹Ð»Ð°Ñ… Ð¸Ð»Ð¸ "
                "Ð² ÐºÐ°ÐºÐ¾Ð¹ Ð¾Ð±Ð»Ð°ÑÑ‚Ð¸ Ð¿Ñ€Ð¾ÐµÐºÑ‚Ð° Ð¼Ð½Ðµ Ñ€Ð°Ð±Ð¾Ñ‚Ð°Ñ‚ÑŒ."
            ),
            display=_voice_display_card(
                title="Scope Needed",
                body=(
                    "Voice task requests need at least one scope file, for example "
                    "`api/app.py` or `orchestrator/dev_task.py`."
                ),
                status="needs_input",
            ),
            suggested_replies=[
                "Use scope api/app.py",
                "Use scope orchestrator/dev_task.py",
            ],
            session=session,
        )
    run_payload = ProductTaskRunRequest(
        objective=resolve_run_objective(
            transcript=transcript,
            session=session,
            resume_scope_collection=resume_scope_collection,
        ),
        scope_files=payload.scope_files,
        user_id=payload.user_id,
        user_role=payload.user_role,
    )
    result = _run_product_task_internal(
        run_payload,
        principal=principal,
        request_source="voice",
    )
    task_summary = {
        "task_id": str(result.get("task_id", "")).strip(),
        "status": _product_status_from_result(result),
        "lifecycle_state": str(result.get("lifecycle_state", "")).strip(),
        "system_status_text": str(result.get("system_status_text", "")).strip(),
        "user_summary": str(result.get("user_summary", "")).strip(),
        "system_report": dict(result.get("system_report", {})),
    }
    spoken_response = (
        f"Ð—Ð°Ð´Ð°Ñ‡Ð° {task_summary['task_id']} Ð¾Ð±Ñ€Ð°Ð±Ð¾Ñ‚Ð°Ð½Ð°. "
        f"Ð¢ÐµÐºÑƒÑ‰ÐµÐµ ÑÐ¾ÑÑ‚Ð¾ÑÐ½Ð¸Ðµ: {task_summary['lifecycle_state']}."
    )
    session = _update_voice_session(
        session=session,
        payload=payload,
        intent="run_task",
        current_mode="task_run",
        task_id=task_summary["task_id"],
        response_summary=spoken_response,
    )
    return _voice_response(
        intent="run_task",
        interaction_state="completed",
        spoken_response=spoken_response,
        display=_voice_display_card(
            title="Task Processed",
            body=task_summary["user_summary"],
            status=task_summary["lifecycle_state"],
        ),
        suggested_replies=[
            "ÐŸÐ¾ÐºÐ°Ð¶Ð¸ ÑÑ‚Ð°Ñ‚ÑƒÑ",
            "ÐŸÐ¾ÐºÐ°Ð¶Ð¸ Ð¿Ð¾ÑÐ»ÐµÐ´Ð½Ð¸Ðµ Ð·Ð°Ð´Ð°Ñ‡Ð¸",
        ],
        session=session,
        task=task_summary,
    )


def _voice_summary_with_followup(user_summary: str, *, waiting: bool) -> str:
    return interaction_voice_summary_with_followup(user_summary, waiting=waiting)


def _voice_history_response(
    items: list[dict[str, object]],
    *,
    session: dict[str, str],
) -> dict[str, object]:
    current_mode = "history_review"
    interaction_state = "waiting_for_followup"
    last_task_id = str(items[0].get("task_id", "")).strip() if items else ""
    if not items:
        return _voice_response(
            intent="read_task_history",
            interaction_state=interaction_state,
            spoken_response="Ð£ Ð¼ÐµÐ½Ñ Ð¿Ð¾ÐºÐ° Ð½ÐµÑ‚ Ð¿Ð¾ÑÐ»ÐµÐ´Ð½Ð¸Ñ… Ð·Ð°Ð´Ð°Ñ‡ Ð´Ð»Ñ Ð¾Ñ‚Ñ‡Ñ‘Ñ‚Ð°.",
            display=_voice_display_card(
                title="No Recent Tasks",
                body="No product tasks have been recorded yet.",
                status="empty",
                state=interaction_state,
                short_status=_display_short_status(
                    interaction_state=interaction_state,
                    current_mode=current_mode,
                ),
            ),
            suggested_replies=_suggested_replies_for_context(
                current_mode=current_mode,
                interaction_state=interaction_state,
                last_task_id="",
            ),
            session=session,
            history={"count": 0, "items": []},
        )

    latest = items[0]
    spoken_response = (
        f"ÐŸÐ¾ÑÐ»ÐµÐ´Ð½ÑÑ Ð·Ð°Ð´Ð°Ñ‡Ð° {latest.get('task_id', '')} ÑÐµÐ¹Ñ‡Ð°Ñ Ð² ÑÐ¾ÑÑ‚Ð¾ÑÐ½Ð¸Ð¸ "
        f"{latest.get('lifecycle_state', 'unknown')}."
    )
    if len(items) > 1:
        spoken_response += (
            f" Ð’ÑÐµÐ³Ð¾ Ð² Ð¾Ñ‚Ñ‡Ñ‘Ñ‚Ðµ {len(items)} Ð¿Ð¾ÑÐ»ÐµÐ´Ð½Ð¸Ñ… Ð·Ð°Ð´Ð°Ñ‡."
        )
    body = "\n".join(
        f"{item.get('task_id', '')}: {item.get('lifecycle_state', '')} - {item.get('objective', '')}"
        for item in items[:3]
    )
    return _voice_response(
        intent="read_task_history",
        interaction_state=interaction_state,
        spoken_response=spoken_response,
        display=_voice_display_card(
            title="Recent Tasks",
            body=body,
            status="completed",
            state=interaction_state,
            short_status=_display_short_status(
                interaction_state=interaction_state,
                current_mode=current_mode,
                lifecycle_state=str(latest.get("lifecycle_state", "")).strip(),
                has_task=True,
            ),
        ),
        suggested_replies=_suggested_replies_for_context(
            current_mode=current_mode,
            interaction_state=interaction_state,
            last_task_id=last_task_id,
        ),
        session=session,
        history={"count": len(items), "items": items},
    )


def _voice_first_entry_with_sessions(
    payload: VoiceEntryRequest,
    *,
    authorization: str | None,
    x_df_principal_token: str | None,
) -> dict[str, object]:
    session, _ = get_or_create_session(
        session_id=payload.session_id,
        user_id=payload.user_id,
        user_role=payload.user_role,
    )
    transcript = _normalize_voice_text(payload.transcript)
    intent = _voice_intent(payload)
    resume_scope_collection = should_resume_scope_collection(
        session=session,
        transcript=transcript,
        scope_files=payload.scope_files,
        intent=intent,
    )
    if resume_scope_collection:
        intent = "run_task"
    current_mode = _session_mode_for_intent(intent)
    box_state = resolve_box_state(
        current_mode=current_mode,
        interaction_state="processing",
        last_task_id=str(session.get("last_task_id", "")).strip(),
        pending_objective=str(session.get("pending_objective", "")).strip(),
    )
    session = _advance_voice_session(
        session=session,
        payload=payload,
        intent=intent,
        current_mode=current_mode,
        states=("listening", "processing"),
        task_id=str(session.get("last_task_id", "")).strip(),
        response_summary=str(session.get("last_response_summary", "")).strip(),
        box_state=box_state,
        pending_objective=str(session.get("pending_objective", "")).strip(),
    )
    resolved_task_id = _resolve_follow_up_task_id(
        payload=payload,
        session=session,
        intent=intent,
    )

    if intent == "help":
        spoken_response = (
            "Ð’Ñ‹ Ð¼Ð¾Ð¶ÐµÑ‚Ðµ Ð¿Ð¾Ð¿Ñ€Ð¾ÑÐ¸Ñ‚ÑŒ Ð¼ÐµÐ½Ñ Ð·Ð°Ð¿ÑƒÑÑ‚Ð¸Ñ‚ÑŒ Ð·Ð°Ð´Ð°Ñ‡Ñƒ Ð¿Ð¾ Ð²Ñ‹Ð±Ñ€Ð°Ð½Ð½Ñ‹Ð¼ Ñ„Ð°Ð¹Ð»Ð°Ð¼, "
            "Ð¿Ð¾ÐºÐ°Ð·Ð°Ñ‚ÑŒ ÑÑ‚Ð°Ñ‚ÑƒÑ Ð·Ð°Ð´Ð°Ñ‡Ð¸ Ð¸Ð»Ð¸ Ð¾Ð·Ð²ÑƒÑ‡Ð¸Ñ‚ÑŒ Ð¿Ð¾ÑÐ»ÐµÐ´Ð½Ð¸Ðµ Ð·Ð°Ð´Ð°Ñ‡Ð¸."
        )
        session = _advance_voice_session(
            session=session,
            payload=payload,
            intent="help",
            current_mode="help",
            states=("responding", "waiting_for_followup"),
            response_summary=spoken_response,
            box_state="guiding",
            pending_objective="",
        )
        return _voice_response(
            intent="help",
            interaction_state="waiting_for_followup",
            spoken_response=spoken_response,
            display=_voice_display_card(
                title="Voice Entry",
                body=(
                    "Try: 'Run a scoped task for api/app.py', "
                    "'Status for api-123', or 'Show recent tasks'."
                ),
                status="ready",
                state="waiting_for_followup",
                short_status=_display_short_status(
                    interaction_state="waiting_for_followup",
                    current_mode="help",
                ),
            ),
            suggested_replies=_suggested_replies_for_context(
                current_mode="help",
                interaction_state="waiting_for_followup",
                last_task_id=str(session.get("last_task_id", "")).strip(),
            ),
            session=session,
        )

    if intent == "repeat":
        spoken_response = (
            str(session.get("last_response_summary", "")).strip()
            or "ÐœÐ½Ðµ Ð¿Ð¾ÐºÐ° Ð½ÐµÑ‡ÐµÐ³Ð¾ Ð¿Ð¾Ð²Ñ‚Ð¾Ñ€Ð¸Ñ‚ÑŒ Ð² ÑÑ‚Ð¾Ð¹ ÑÐµÑÑÐ¸Ð¸."
        )
        mode = str(session.get("current_mode", "help")).strip() or "help"
        last_task_id = str(session.get("last_task_id", "")).strip()
        session = _advance_voice_session(
            session=session,
            payload=payload,
            intent="repeat",
            current_mode=mode,
            states=("responding", "waiting_for_followup"),
            task_id=last_task_id,
            response_summary=spoken_response,
            box_state=resolve_box_state(
                current_mode=mode,
                interaction_state="waiting_for_followup",
                last_task_id=last_task_id,
                pending_objective=str(session.get("pending_objective", "")).strip(),
            ),
            pending_objective=str(session.get("pending_objective", "")).strip(),
        )
        return _voice_response(
            intent="repeat",
            interaction_state="waiting_for_followup",
            spoken_response=spoken_response,
            display=_voice_display_card(
                title="Repeat",
                body=spoken_response,
                status="completed",
                state="waiting_for_followup",
                short_status=_display_short_status(
                    interaction_state="waiting_for_followup",
                    current_mode=mode,
                    has_task=bool(last_task_id),
                ),
            ),
            suggested_replies=_suggested_replies_for_context(
                current_mode=mode,
                interaction_state="waiting_for_followup",
                last_task_id=last_task_id,
            ),
            session=session,
        )

    if intent == "read_task_history":
        check_permissions(
            user_id=payload.user_id,
            user_role=payload.user_role,
            action="read_task_history",
            authorization=authorization,
            x_df_principal_token=x_df_principal_token,
        )
        records = list(reversed(load_execution_records()))
        items = [
            _product_task_summary(
                _materialize_product_record(record, request_source="voice")
            )
            for record in records[:5]
        ]
        preview = _voice_history_response(items, session=session)
        last_task_id = str(items[0].get("task_id", "")).strip() if items else ""
        spoken_response = str(preview.get("spoken_response", "")).strip()
        session = _advance_voice_session(
            session=session,
            payload=payload,
            intent="read_task_history",
            current_mode="history_review",
            states=("responding", "waiting_for_followup"),
            task_id=last_task_id,
            response_summary=spoken_response,
            box_state="reviewing_history",
            pending_objective="",
        )
        return _voice_history_response(items, session=session)

    if intent == "read_task":
        check_permissions(
            user_id=payload.user_id,
            user_role=payload.user_role,
            action="read_task",
            authorization=authorization,
            x_df_principal_token=x_df_principal_token,
        )
        if not resolved_task_id:
            spoken_response = "ÐœÐ½Ðµ Ð½ÑƒÐ¶ÐµÐ½ Ð¸Ð´ÐµÐ½Ñ‚Ð¸Ñ„Ð¸ÐºÐ°Ñ‚Ð¾Ñ€ Ð·Ð°Ð´Ð°Ñ‡Ð¸, Ñ‡Ñ‚Ð¾Ð±Ñ‹ Ð¾Ð·Ð²ÑƒÑ‡Ð¸Ñ‚ÑŒ ÐµÑ‘ ÑÐ¾ÑÑ‚Ð¾ÑÐ½Ð¸Ðµ."
            session = _advance_voice_session(
                session=session,
                payload=payload,
                intent="read_task",
                current_mode="task_status",
                states=("responding", "waiting_for_followup"),
                response_summary=spoken_response,
                box_state="reviewing_task",
                pending_objective="",
            )
            return _voice_response(
                intent="read_task",
                interaction_state="waiting_for_followup",
                spoken_response=spoken_response,
                display=_voice_display_card(
                    title="Task ID Needed",
                    body="Provide a task id like api-123abc or DEV-001.",
                    status="needs_input",
                    state="waiting_for_followup",
                    short_status=_display_short_status(
                        interaction_state="waiting_for_followup",
                        current_mode="task_status",
                    ),
                ),
                suggested_replies=_suggested_replies_for_context(
                    current_mode="task_status",
                    interaction_state="waiting_for_followup",
                    last_task_id=str(session.get("last_task_id", "")).strip(),
                ),
                session=session,
            )
        record = _find_product_record(resolved_task_id)
        if record is None:
            spoken_response = f"Ð¯ Ð½Ðµ Ð½Ð°ÑˆÐ»Ð° Ð·Ð°Ð´Ð°Ñ‡Ñƒ {resolved_task_id}."
            session = _advance_voice_session(
                session=session,
                payload=payload,
                intent="read_task",
                current_mode="task_status",
                states=("responding", "waiting_for_followup"),
                response_summary=spoken_response,
                box_state="reviewing_task",
                pending_objective="",
            )
            return _voice_response(
                intent="read_task",
                interaction_state="waiting_for_followup",
                spoken_response=spoken_response,
                display=_voice_display_card(
                    title="Task Not Found",
                    body=f"No saved product task was found for {resolved_task_id}.",
                    status="missing",
                    state="waiting_for_followup",
                    short_status=_display_short_status(
                        interaction_state="waiting_for_followup",
                        current_mode="task_status",
                    ),
                ),
                suggested_replies=_suggested_replies_for_context(
                    current_mode="task_status",
                    interaction_state="waiting_for_followup",
                    last_task_id=str(session.get("last_task_id", "")).strip(),
                ),
                session=session,
            )
        rendered_record = _materialize_product_record(record, request_source="voice")
        task_summary = {
            "task_id": str(rendered_record.get("task_id", "")).strip(),
            "status": _product_status_from_result(rendered_record),
            "lifecycle_state": str(rendered_record.get("lifecycle_state", "")).strip(),
            "system_status_text": str(
                rendered_record.get("system_status_text", "")
            ).strip(),
            "user_summary": _voice_summary_with_followup(
                str(rendered_record.get("user_summary", "")).strip(),
                waiting=True,
            ),
            "system_report": dict(rendered_record.get("system_report", {})),
        }
        spoken_response = (
            f"Ð—Ð°Ð´Ð°Ñ‡Ð° {task_summary['task_id']} ÑÐµÐ¹Ñ‡Ð°Ñ Ð² ÑÐ¾ÑÑ‚Ð¾ÑÐ½Ð¸Ð¸ "
            f"{task_summary['lifecycle_state']}."
        )
        session = _advance_voice_session(
            session=session,
            payload=payload,
            intent="read_task",
            current_mode="task_status",
            states=("responding", "waiting_for_followup"),
            task_id=task_summary["task_id"],
            response_summary=spoken_response,
            box_state="reviewing_task",
            pending_objective="",
        )
        return _voice_response(
            intent="read_task",
            interaction_state="waiting_for_followup",
            spoken_response=spoken_response,
            display=_voice_display_card(
                title=f"Task {task_summary['task_id']}",
                body=task_summary["user_summary"],
                status=task_summary["lifecycle_state"],
                state="waiting_for_followup",
                short_status=_display_short_status(
                    interaction_state="waiting_for_followup",
                    current_mode="task_status",
                    lifecycle_state=task_summary["lifecycle_state"],
                    has_task=True,
                ),
            ),
            suggested_replies=_suggested_replies_for_context(
                current_mode="task_status",
                interaction_state="waiting_for_followup",
                last_task_id=task_summary["task_id"],
            ),
            session=session,
            task=task_summary,
        )

    principal = check_permissions(
        user_id=payload.user_id,
        user_role=payload.user_role,
        action="run_task",
        authorization=authorization,
        x_df_principal_token=x_df_principal_token,
    )
    if not payload.scope_files:
        pending_objective = resolve_run_objective(
            transcript=transcript,
            session=session,
            resume_scope_collection=resume_scope_collection,
        )
        spoken_response = (
            "Ð¯ Ð³Ð¾Ñ‚Ð¾Ð²Ð° Ð·Ð°Ð¿ÑƒÑÑ‚Ð¸Ñ‚ÑŒ Ð·Ð°Ð´Ð°Ñ‡Ñƒ, Ð½Ð¾ ÑÐ½Ð°Ñ‡Ð°Ð»Ð° ÑÐºÐ°Ð¶Ð¸Ñ‚Ðµ, Ð² ÐºÐ°ÐºÐ¸Ñ… Ñ„Ð°Ð¹Ð»Ð°Ñ… Ð¸Ð»Ð¸ "
            "Ð² ÐºÐ°ÐºÐ¾Ð¹ Ð¾Ð±Ð»Ð°ÑÑ‚Ð¸ Ð¿Ñ€Ð¾ÐµÐºÑ‚Ð° Ð¼Ð½Ðµ Ñ€Ð°Ð±Ð¾Ñ‚Ð°Ñ‚ÑŒ."
        )
        session = _advance_voice_session(
            session=session,
            payload=payload,
            intent="run_task",
            current_mode="task_run",
            states=("responding", "waiting_for_followup"),
            response_summary=spoken_response,
            box_state="awaiting_scope",
            pending_objective=pending_objective,
        )
        return _voice_response(
            intent="run_task",
            interaction_state="waiting_for_followup",
            spoken_response=spoken_response,
            display=_voice_display_card(
                title="Scope Needed",
                body=(
                    "Voice task requests need at least one scope file, for example "
                    "`api/app.py` or `orchestrator/dev_task.py`."
                ),
                status="needs_input",
                state="waiting_for_followup",
                short_status=_display_short_status(
                    interaction_state="waiting_for_followup",
                    current_mode="task_run",
                ),
            ),
            suggested_replies=_suggested_replies_for_context(
                current_mode="task_run",
                interaction_state="waiting_for_followup",
                last_task_id=str(session.get("last_task_id", "")).strip(),
            ),
            session=session,
        )
    run_payload = ProductTaskRunRequest(
        objective=resolve_run_objective(
            transcript=transcript,
            session=session,
            resume_scope_collection=resume_scope_collection,
        ),
        scope_files=payload.scope_files,
        user_id=payload.user_id,
        user_role=payload.user_role,
    )
    result = _run_product_task_internal(
        run_payload,
        principal=principal,
        request_source="voice",
    )
    task_summary = {
        "task_id": str(result.get("task_id", "")).strip(),
        "status": _product_status_from_result(result),
        "lifecycle_state": str(result.get("lifecycle_state", "")).strip(),
        "system_status_text": str(result.get("system_status_text", "")).strip(),
        "user_summary": _voice_summary_with_followup(
            str(result.get("user_summary", "")).strip(),
            waiting=True,
        ),
        "system_report": dict(result.get("system_report", {})),
    }
    spoken_response = (
        f"Ð—Ð°Ð´Ð°Ñ‡Ð° {task_summary['task_id']} Ð¾Ð±Ñ€Ð°Ð±Ð¾Ñ‚Ð°Ð½Ð°. "
        f"Ð¢ÐµÐºÑƒÑ‰ÐµÐµ ÑÐ¾ÑÑ‚Ð¾ÑÐ½Ð¸Ðµ: {task_summary['lifecycle_state']}."
    )
    session = _advance_voice_session(
        session=session,
        payload=payload,
        intent="run_task",
        current_mode="task_run",
        states=("responding", "waiting_for_followup"),
        task_id=task_summary["task_id"],
        response_summary=spoken_response,
        box_state="presenting_result",
        pending_objective="",
    )
    return _voice_response(
        intent="run_task",
        interaction_state="waiting_for_followup",
        spoken_response=spoken_response,
        display=_voice_display_card(
            title="Task Processed",
            body=task_summary["user_summary"],
            status=task_summary["lifecycle_state"],
            state="waiting_for_followup",
            short_status=_display_short_status(
                interaction_state="waiting_for_followup",
                current_mode="task_run",
                lifecycle_state=task_summary["lifecycle_state"],
                has_task=True,
            ),
        ),
        suggested_replies=_suggested_replies_for_context(
            current_mode="task_run",
            interaction_state="waiting_for_followup",
            last_task_id=task_summary["task_id"],
        ),
        session=session,
        task=task_summary,
    )


@app.post("/execute")
@app.post("/tasks")
def execute(
    payload: ExecuteRequest,
    x_df_internal_call: str | None = Header(default=None),
):
    _require_internal_only_call(x_df_internal_call)
    raise HTTPException(
        status_code=403,
        detail="product runtime boundary blocks dev task execution",
    )


@app.post("/execute-controlled")
def execute_controlled(
    payload: ControlledExecuteRequest,
    x_df_actor_id: str | None = Header(default=None),
    x_df_role: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    x_df_principal_token: str | None = Header(default=None),
    x_df_internal_call: str | None = Header(default=None),
):
    _require_internal_only_call(x_df_internal_call)
    principal = _require_permission(
        _principal_from_headers(
            x_df_actor_id,
            x_df_role,
            authorization,
            x_df_principal_token,
        ),
        "execute_controlled",
    )
    raise HTTPException(
        status_code=403,
        detail="product runtime boundary blocks controlled dev execution",
    )


@app.post("/api/task/run")
def run_product_task(
    payload: ProductTaskRunRequest,
    authorization: str | None = Header(default=None),
    x_df_principal_token: str | None = Header(default=None),
    x_df_internal_call: str | None = Header(default=None),
):
    _require_internal_only_call(x_df_internal_call)
    principal = check_permissions(
        user_id=payload.user_id,
        user_role=payload.user_role,
        action="run_task",
        authorization=authorization,
        x_df_principal_token=x_df_principal_token,
    )
    result = execute_product_task_request(
        payload,
        principal=principal,
        request_source="api",
        execute_single=_run_product_task_internal,
    )
    if isinstance(result, list):
        return [_product_api_result(item) for item in result]
    return _product_api_result(result)


@app.post("/api/secretary/entry")
def secretary_entry(
    payload: SecretaryEntryRequest,
    authorization: str | None = Header(default=None),
    x_df_principal_token: str | None = Header(default=None),
):
    principal = check_permissions(
        user_id=payload.user_id,
        user_role=payload.user_role,
        action="run_task",
        authorization=authorization,
        x_df_principal_token=x_df_principal_token,
    )
    raise HTTPException(
        status_code=403,
        detail="product runtime boundary blocks secretary dev orchestration",
    )
    session, _ = get_or_create_session(
        session_id=payload.session_id,
        user_id=payload.user_id,
        user_role=payload.user_role,
    )
    pending_message = _deserialize_incoming_message(
        session.get("pending_confirmation_payload", "")
    )
    session_id = session["session_id"]
    is_confirmation_yes = pending_message is not None and _secretary_yes(
        payload.message
    )
    is_confirmation_no = pending_message is not None and _secretary_no(payload.message)
    trace_intent = ""
    trace_service_type = ""
    if pending_message is not None:
        pending_contact = upsert_contact(
            source_channel=pending_message.channel,
            contact_id=pending_message.contact_id,
            raw_text=pending_message.raw_text,
        )
        trace_intent, trace_payload = _secretary_intent_and_payload(
            pending_message,
            pending_contact,
        )
        trace_service_type = str(trace_payload.get("service_type", "")).strip()

    if is_confirmation_yes:
        _get_operator_task_worker().start()
        result = run_task(
            _secretary_task_request(
                pending_message,
                contact=pending_contact,
            )
        )
        updated_session = update_session(
            session_id,
            user_id=session.get("user_id", ""),
            user_role=session.get("user_role", ""),
            current_mode="task_run",
            interaction_state="waiting_for_followup",
            last_task_id=str(result.get("task_id", "")).strip(),
            last_intent="secretary_confirmation_yes",
            last_response_summary=str(result.get("status", "")).strip(),
            box_state="presenting_result",
            pending_confirmation_payload="",
        )
        _secretary_trace_task_creation(
            session_id=session_id,
            state_before=session,
            state_after=updated_session,
            intent_final=trace_intent,
            service_type=trace_service_type,
            create_task_eligible=True,
            create_task_skip_reason="",
            create_task_handler_called=True,
            task_created=True,
        )
        return {
            **_queued_task_response(result),
            "actor_id": principal.get("actor_id", ""),
            "session_id": session_id,
            "incoming_message": pending_message.as_dict(),
            "contact": pending_contact,
            "confirmation_required": False,
            "task_created": True,
            "create_task_skip_reason": "",
            "task": dict(result),
        }

    if is_confirmation_no:
        response = _secretary_clarification_response(session=session)
        _secretary_trace_task_creation(
            session_id=session_id,
            state_before=session,
            state_after=find_session(session_id) or session,
            intent_final=trace_intent,
            service_type=trace_service_type,
            create_task_eligible=False,
            create_task_skip_reason="confirmation_rejected",
            create_task_handler_called=False,
            task_created=False,
        )
        return response

    incoming_message = _incoming_message_from_request(payload)
    contact = upsert_contact(
        source_channel=incoming_message.channel,
        contact_id=incoming_message.contact_id,
        raw_text=incoming_message.raw_text,
    )
    if pending_message is not None:
        response = _secretary_confirmation_response(
            session=session,
            incoming_message=incoming_message,
            contact=contact,
        )
        _secretary_trace_task_creation(
            session_id=session_id,
            state_before=session,
            state_after=find_session(session_id) or session,
            intent_final=trace_intent,
            service_type=trace_service_type,
            create_task_eligible=False,
            create_task_skip_reason="confirmation_not_recognized_as_yes",
            create_task_handler_called=False,
            task_created=False,
        )
        response["create_task_skip_reason"] = "confirmation_not_recognized_as_yes"
        return response
    return _secretary_confirmation_response(
        session=session,
        incoming_message=incoming_message,
        contact=contact,
    )


@app.post("/input")
def input_entry(
    payload: SecretaryEntryRequest,
    authorization: str | None = Header(default=None),
    x_df_principal_token: str | None = Header(default=None),
    x_df_interaction_id: str | None = Header(default=None),
    x_df_trace_id: str | None = Header(default=None),
):
    logger.info(
        "BACKEND_IN_PAYLOAD %s",
        json.dumps(
            {
                "interaction_id": str(x_df_interaction_id or "").strip(),
                "trace_id": str(x_df_trace_id or "").strip(),
                "payload": _model_payload(payload),
            },
            ensure_ascii=False,
        ),
    )
    return secretary_entry(
        payload,
        authorization=authorization,
        x_df_principal_token=x_df_principal_token,
    )


@app.post("/api/telnyx/call-control/webhook")
def telnyx_call_control_webhook(payload: TelnyxCallControlWebhookRequest):
    session, telnyx_payload, commands = _ensure_voice_call_session_from_telnyx(payload)
    return {
        "ok": True,
        "event_type": str(payload.event_type or "").strip(),
        "commands": commands,
        "session": _voice_call_state_payload(session),
        "telnyx_payload": telnyx_payload,
    }


@app.post("/api/telnyx/voice-gateway/events")
def telnyx_voice_gateway_event(payload: VoiceGatewayEventRequest):
    session = find_call_session(
        call_session_id=payload.call_session_id,
        call_control_id=payload.call_control_id,
    )
    if session is None:
        raise HTTPException(status_code=404, detail="voice call session not found")
    event_id = (
        str(payload.event_id or "").strip() or f"voice-event-{uuid.uuid4().hex[:12]}"
    )
    if not register_processed_event(session["call_session_id"], event_id):
        latest = (
            find_call_session(call_session_id=session["call_session_id"]) or session
        )
        return {
            "ok": True,
            "duplicate": True,
            "session": _voice_call_state_payload(latest),
        }

    normalized_event_type = str(payload.event_type or "").strip().lower()
    transcript = _normalize_voice_text(payload.transcript)
    if normalized_event_type in {"vad_start", "speech_start"}:
        session = update_voice_call_session(
            session["call_session_id"],
            media_state="speaking_detected",
            interaction_state="listening",
        )
        _voice_call_trace(
            session,
            "voice.vad.started",
            event_id=event_id,
            sequence_number=payload.sequence_number,
            stream_id=payload.stream_id,
        )
    elif normalized_event_type in {"partial_asr", "speech_partial"}:
        interrupted = bool(list_pending_outbound(session["call_session_id"]))
        if interrupted:
            interrupt_started = time.monotonic()
            clear_pending_outbound(session["call_session_id"])
            session = update_voice_call_session(
                session["call_session_id"],
                interruption_count=int(session.get("interruption_count", 0) or 0) + 1,
                media_state="speaking_detected",
                interaction_state="listening",
                last_partial_transcript=transcript,
            )
            _voice_call_trace(
                session,
                "voice.interruption.detected",
                event_id=event_id,
                transcript=transcript,
            )
            _queue_voice_call_text(
                session,
                text=VOICE_INTERRUPT_ACK_TEXT,
                kind="interrupt_ack",
                interruptible=True,
                use_telnyx_speak=True,
            )
            interrupt_latency_ms = (time.monotonic() - interrupt_started) * 1000.0
            session = _append_voice_latency_metric(
                session,
                metric="interrupt_reaction_ms",
                value_ms=interrupt_latency_ms,
            )
            _trace_latency_event(
                session,
                event_type="VOICE_INTERRUPT_LATENCY_RECORDED",
                event_id=event_id,
                metric="interrupt_reaction_ms",
                value_ms=interrupt_latency_ms,
            )
        else:
            session = update_voice_call_session(
                session["call_session_id"],
                media_state="speaking_detected",
                interaction_state="listening",
                last_partial_transcript=transcript,
            )
            _voice_call_trace(
                session,
                "voice.partial_asr.received",
                event_id=event_id,
                transcript=transcript,
            )
    elif normalized_event_type in {"final_asr", "speech_final"}:
        turn_started = time.monotonic()
        (
            session,
            _,
            _,
            runtime_validation,
            routing_contract,
            runtime_decision,
            runtime_verdict,
        ) = _evaluate_voice_runtime_validation(
            session,
            transcript=transcript,
            event_id=event_id,
        )
        if not bool(runtime_validation.get("allow_turn_start", True)):
            guarded_session = update_voice_call_session(
                session["call_session_id"],
                media_state="listening",
                interaction_state="waiting_for_followup",
                current_mode="voice_call",
                job_status="guarded",
                last_error="voice runtime guardrail blocked turn start",
            )
            return {
                "ok": True,
                "duplicate": False,
                "guarded": True,
                "validation": runtime_validation,
                "routing_contract": routing_contract,
                "runtime_verdict": runtime_verdict,
                "session": _voice_call_state_payload(guarded_session),
            }

        session = update_voice_call_session(
            session["call_session_id"],
            media_state="processing",
            interaction_state="processing",
            current_mode="voice_call",
            job_status="processing",
            last_transcript=transcript,
            last_ack_text=VOICE_TURN_ACK_TEXT,
            active_turn_event_id=event_id,
        )
        _voice_call_trace(
            session,
            "voice.final_asr.received",
            event_id=event_id,
            transcript=transcript,
        )
        acknowledgements = _queue_voice_call_text(
            session,
            text=VOICE_TURN_ACK_TEXT,
            kind="turn_ack",
            interruptible=True,
            use_telnyx_speak=True,
        )
        pre_response_latency_ms = (time.monotonic() - turn_started) * 1000.0
        session = _append_voice_latency_metric(
            session,
            metric="pre_response_ms",
            value_ms=pre_response_latency_ms,
        )
        _trace_latency_event(
            session,
            event_type="VOICE_PRE_RESPONSE_LATENCY_RECORDED",
            event_id=event_id,
            metric="pre_response_ms",
            value_ms=pre_response_latency_ms,
            runtime_validation=runtime_validation,
            routing_contract=routing_contract,
        )
        if acknowledgements:
            first_chunk_latency_ms = (time.monotonic() - turn_started) * 1000.0
            session = _append_voice_latency_metric(
                session,
                metric="first_outbound_chunk_ms",
                value_ms=first_chunk_latency_ms,
            )
            _trace_latency_event(
                session,
                event_type="VOICE_FIRST_CHUNK_LATENCY_RECORDED",
                event_id=event_id,
                metric="first_outbound_chunk_ms",
                value_ms=first_chunk_latency_ms,
                runtime_validation=runtime_validation,
                routing_contract=routing_contract,
            )
        session, runtime_verdict = _update_voice_runtime_verdict(
            session,
            event_id=event_id,
            runtime_validation=runtime_validation,
            runtime_decision=runtime_decision,
            routing_contract=routing_contract,
        )
        _start_voice_call_turn(
            call_session_id=session["call_session_id"],
            transcript=transcript,
            event_id=event_id,
        )
        return {
            "ok": True,
            "duplicate": False,
            "guarded": False,
            "validation": runtime_validation,
            "routing_contract": routing_contract,
            "runtime_verdict": runtime_verdict,
            "acknowledgements": acknowledgements,
            "session": _voice_call_state_payload(
                find_call_session(call_session_id=session["call_session_id"]) or session
            ),
        }
    elif normalized_event_type == "network_drop":
        session = update_voice_call_session(
            session["call_session_id"],
            call_state="degraded",
            media_state="disconnected",
            interaction_state="waiting_for_followup",
            last_error="network drop detected",
        )
        _voice_call_trace(
            session,
            "voice.network.drop",
            event_id=event_id,
        )
    elif normalized_event_type == "reconnect":
        session = update_voice_call_session(
            session["call_session_id"],
            call_state="answered",
            media_state="listening",
            interaction_state="waiting_for_followup",
            reconnect_count=int(session.get("reconnect_count", 0) or 0) + 1,
            last_error="",
        )
        _voice_call_trace(
            session,
            "voice.network.reconnect",
            event_id=event_id,
        )
    else:
        session = update_voice_call_session(
            session["call_session_id"],
            media_state="listening",
        )
        _voice_call_trace(
            session,
            "voice.gateway.unknown_event",
            event_id=event_id,
            raw_event_type=normalized_event_type,
        )

    return {
        "ok": True,
        "duplicate": False,
        "session": _voice_call_state_payload(
            find_call_session(call_session_id=session["call_session_id"]) or session
        ),
    }


@app.get("/api/telnyx/calls/{call_session_id}")
def get_telnyx_call_state(call_session_id: str):
    session = find_call_session(call_session_id=call_session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="voice call session not found")
    return _voice_call_state_payload(session)


@app.websocket("/ws/telnyx/media/{call_session_id}")
async def telnyx_media_stream(call_session_id: str, websocket: WebSocket):
    session = find_call_session(call_session_id=call_session_id)
    if session is None:
        await websocket.close(code=4404)
        return
    await websocket.accept()
    VOICE_RUNTIME_BUS.register(call_session_id)
    session = update_voice_call_session(
        call_session_id,
        media_state="connected",
        call_state="answered",
        interaction_state="waiting_for_followup",
    )
    _voice_call_trace(session, "voice.websocket.connected")
    for pending in list_pending_outbound(call_session_id):
        await websocket.send_json(
            {
                "event": "tts_chunk",
                "event_id": str(pending.get("event_id", "")).strip(),
                "sequence": int(pending.get("sequence", 0)),
                "call_session_id": call_session_id,
                "kind": str(pending.get("kind", "")).strip(),
                "text": str(pending.get("text", "")).strip(),
            }
        )
        remove_pending_outbound(call_session_id, pending.get("event_id", ""))
    try:
        while True:
            receive_task = asyncio.create_task(websocket.receive_json())
            outbound_task = asyncio.create_task(
                VOICE_RUNTIME_BUS.next_event(call_session_id, timeout_seconds=30.0)
            )
            done, pending = await asyncio.wait(
                {receive_task, outbound_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            if outbound_task in done:
                outbound = outbound_task.result()
                await websocket.send_json(outbound)
                remove_pending_outbound(call_session_id, outbound.get("event_id", ""))
                continue
            inbound = receive_task.result()
            frame = TelnyxMediaFrameRequest.model_validate(inbound)
            stream_id = (
                str(frame.stream_id or "").strip()
                or str(dict(frame.payload).get("stream_id", "")).strip()
            )
            if stream_id:
                update_voice_call_session(call_session_id, stream_id=stream_id)
            transcript = _normalize_voice_text(
                frame.transcript or dict(frame.payload).get("transcript", "")
            )
            event_name = str(frame.event or "").strip().lower()
            if transcript:
                telnyx_voice_gateway_event(
                    VoiceGatewayEventRequest(
                        call_session_id=call_session_id,
                        event_id=f"ws-{uuid.uuid4().hex[:12]}",
                        event_type="speech_final"
                        if event_name in {"media", "final"}
                        else "speech_partial",
                        transcript=transcript,
                        stream_id=stream_id,
                        sequence_number=frame.sequence_number,
                    )
                )
            elif event_name == "start":
                telnyx_voice_gateway_event(
                    VoiceGatewayEventRequest(
                        call_session_id=call_session_id,
                        event_id=f"ws-{uuid.uuid4().hex[:12]}",
                        event_type="vad_start",
                        stream_id=stream_id,
                        sequence_number=frame.sequence_number,
                    )
                )
            elif event_name == "stop":
                telnyx_voice_gateway_event(
                    VoiceGatewayEventRequest(
                        call_session_id=call_session_id,
                        event_id=f"ws-{uuid.uuid4().hex[:12]}",
                        event_type="network_drop",
                        stream_id=stream_id,
                        sequence_number=frame.sequence_number,
                    )
                )
                await websocket.send_json(
                    {"event": "stream_stopped", "call_session_id": call_session_id}
                )
                break
    except WebSocketDisconnect:
        disconnected_session = update_voice_call_session(
            call_session_id,
            media_state="disconnected",
            call_state="degraded",
        )
        _voice_call_trace(disconnected_session, "voice.websocket.disconnected")
    finally:
        VOICE_RUNTIME_BUS.close(call_session_id)


@app.get("/api/task/history")
def list_product_task_history(
    user_id: str = "",
    user_role: str = "",
    authorization: str | None = Header(default=None),
    x_df_principal_token: str | None = Header(default=None),
):
    check_permissions(
        user_id=user_id,
        user_role=user_role,
        action="read_task_history",
        authorization=authorization,
        x_df_principal_token=x_df_principal_token,
    )
    records = list(reversed(load_execution_records()))
    normalized_records: list[dict[str, object]] = []
    for record in records[:10]:
        normalized_records.append(
            _materialize_product_record(record, request_source="api")
        )
    return {
        "count": min(len(records), 10),
        "items": [_product_task_summary(record) for record in normalized_records],
    }


@app.get("/api/operator/tasks")
def list_operator_tasks(
    user_id: str = "",
    user_role: str = "",
    status: str = "",
    contact_id: str = "",
    limit: int = 10,
    authorization: str | None = Header(default=None),
    x_df_principal_token: str | None = Header(default=None),
):
    check_permissions(
        user_id=user_id,
        user_role=user_role,
        action="read_task_history",
        authorization=authorization,
        x_df_principal_token=x_df_principal_token,
    )
    if contact_id:
        tasks = get_operator_tasks_by_contact(
            contact_id,
            store_path=OPERATOR_TASKS_FILE,
        )
    elif str(status or "").strip().lower() == "open":
        tasks = get_operator_open_tasks(store_path=OPERATOR_TASKS_FILE)
    else:
        tasks = get_operator_recent_tasks(limit=limit, store_path=OPERATOR_TASKS_FILE)

    filtered_tasks = _filter_operator_tasks(tasks, status=status)
    sorted_tasks = sorted(
        filtered_tasks,
        key=lambda item: (
            str(item.get("created_at", "")).strip(),
            str(item.get("task_id", "")).strip(),
        ),
        reverse=True,
    )
    return {
        "count": len(sorted_tasks),
        "items": [
            _operator_task_item(task) for task in sorted_tasks[: max(0, int(limit))]
        ],
    }


@app.get("/api/task/{task_id}")
def get_product_task(
    task_id: str,
    user_id: str = "",
    user_role: str = "",
    authorization: str | None = Header(default=None),
    x_df_principal_token: str | None = Header(default=None),
):
    check_permissions(
        user_id=user_id,
        user_role=user_role,
        action="read_task",
        authorization=authorization,
        x_df_principal_token=x_df_principal_token,
    )
    record = _find_product_record(task_id)
    if record is None:
        return {"detail": "task not found", "task_id": task_id}
    return _materialize_product_record(record, request_source="api")


@app.get("/tasks/{task_id}")
def get_task_alias(
    task_id: str,
    user_id: str = "",
    user_role: str = "",
    authorization: str | None = Header(default=None),
    x_df_principal_token: str | None = Header(default=None),
):
    return get_product_task(
        task_id=task_id,
        user_id=user_id,
        user_role=user_role,
        authorization=authorization,
        x_df_principal_token=x_df_principal_token,
    )


@app.post("/api/voice/entry")
def voice_first_entry(
    payload: VoiceEntryRequest,
    authorization: str | None = Header(default=None),
    x_df_principal_token: str | None = Header(default=None),
    x_df_internal_call: str | None = Header(default=None),
):
    _require_internal_only_call(x_df_internal_call)
    return _voice_first_entry_with_sessions(
        payload,
        authorization=authorization,
        x_df_principal_token=x_df_principal_token,
    )

    intent = _voice_intent(payload)
    transcript = _normalize_voice_text(payload.transcript)
    resolved_task_id = str(payload.task_id or "").strip() or _extract_task_id_from_text(
        transcript
    )

    if intent == "help":
        return _voice_response(
            intent="help",
            interaction_state="completed",
            spoken_response=(
                "Ð’Ñ‹ Ð¼Ð¾Ð¶ÐµÑ‚Ðµ Ð¿Ð¾Ð¿Ñ€Ð¾ÑÐ¸Ñ‚ÑŒ Ð¼ÐµÐ½Ñ Ð·Ð°Ð¿ÑƒÑÑ‚Ð¸Ñ‚ÑŒ Ð·Ð°Ð´Ð°Ñ‡Ñƒ Ð¿Ð¾ Ð²Ñ‹Ð±Ñ€Ð°Ð½Ð½Ñ‹Ð¼ Ñ„Ð°Ð¹Ð»Ð°Ð¼, "
                "Ð¿Ð¾ÐºÐ°Ð·Ð°Ñ‚ÑŒ ÑÑ‚Ð°Ñ‚ÑƒÑ Ð·Ð°Ð´Ð°Ñ‡Ð¸ Ð¸Ð»Ð¸ Ð¾Ð·Ð²ÑƒÑ‡Ð¸Ñ‚ÑŒ Ð¿Ð¾ÑÐ»ÐµÐ´Ð½Ð¸Ðµ Ð·Ð°Ð´Ð°Ñ‡Ð¸."
            ),
            display=_voice_display_card(
                title="Voice Entry",
                body=(
                    "Try: 'Run a scoped task for api/app.py', "
                    "'Status for api-123', or 'Show recent tasks'."
                ),
                status="ready",
            ),
            suggested_replies=[
                "Run a scoped task",
                "Status for latest task",
                "Show recent tasks",
            ],
        )

    if intent == "read_task_history":
        check_permissions(
            user_id=payload.user_id,
            user_role=payload.user_role,
            action="read_task_history",
            authorization=authorization,
            x_df_principal_token=x_df_principal_token,
        )
        records = list(reversed(load_execution_records()))
        items = [
            _product_task_summary(
                _materialize_product_record(record, request_source="voice")
            )
            for record in records[:5]
        ]
        return _voice_history_response(items)

    if intent == "read_task":
        check_permissions(
            user_id=payload.user_id,
            user_role=payload.user_role,
            action="read_task",
            authorization=authorization,
            x_df_principal_token=x_df_principal_token,
        )
        if not resolved_task_id:
            return _voice_response(
                intent="read_task",
                interaction_state="needs_task_id",
                spoken_response="ÐœÐ½Ðµ Ð½ÑƒÐ¶ÐµÐ½ Ð¸Ð´ÐµÐ½Ñ‚Ð¸Ñ„Ð¸ÐºÐ°Ñ‚Ð¾Ñ€ Ð·Ð°Ð´Ð°Ñ‡Ð¸, Ñ‡Ñ‚Ð¾Ð±Ñ‹ Ð¾Ð·Ð²ÑƒÑ‡Ð¸Ñ‚ÑŒ ÐµÑ‘ ÑÐ¾ÑÑ‚Ð¾ÑÐ½Ð¸Ðµ.",
                display=_voice_display_card(
                    title="Task ID Needed",
                    body="Provide a task id like api-123abc or DEV-001.",
                    status="needs_input",
                ),
                suggested_replies=[
                    "Status for api-123abc",
                    "Show recent tasks",
                ],
            )
        record = _find_product_record(resolved_task_id)
        if record is None:
            return _voice_response(
                intent="read_task",
                interaction_state="completed",
                spoken_response=f"Ð¯ Ð½Ðµ Ð½Ð°ÑˆÐ»Ð° Ð·Ð°Ð´Ð°Ñ‡Ñƒ {resolved_task_id}.",
                display=_voice_display_card(
                    title="Task Not Found",
                    body=f"No saved product task was found for {resolved_task_id}.",
                    status="missing",
                ),
                suggested_replies=[
                    "Show recent tasks",
                    "Run a scoped task",
                ],
            )
        rendered_record = _materialize_product_record(record, request_source="voice")
        task_summary = {
            "task_id": str(rendered_record.get("task_id", "")).strip(),
            "status": _product_status_from_result(rendered_record),
            "lifecycle_state": str(rendered_record.get("lifecycle_state", "")).strip(),
            "system_status_text": str(
                rendered_record.get("system_status_text", "")
            ).strip(),
            "user_summary": str(rendered_record.get("user_summary", "")).strip(),
            "system_report": dict(rendered_record.get("system_report", {})),
        }
        return _voice_response(
            intent="read_task",
            interaction_state="completed",
            spoken_response=(
                f"Ð—Ð°Ð´Ð°Ñ‡Ð° {task_summary['task_id']} ÑÐµÐ¹Ñ‡Ð°Ñ Ð² ÑÐ¾ÑÑ‚Ð¾ÑÐ½Ð¸Ð¸ "
                f"{task_summary['lifecycle_state']}."
            ),
            display=_voice_display_card(
                title=f"Task {task_summary['task_id']}",
                body=task_summary["user_summary"],
                status=task_summary["lifecycle_state"],
            ),
            suggested_replies=[
                "Show recent tasks",
                "Run another scoped task",
            ],
            task=task_summary,
        )

    principal = check_permissions(
        user_id=payload.user_id,
        user_role=payload.user_role,
        action="run_task",
        authorization=authorization,
        x_df_principal_token=x_df_principal_token,
    )
    if not payload.scope_files:
        return _voice_response(
            intent="run_task",
            interaction_state="needs_scope",
            spoken_response=(
                "Ð¯ Ð³Ð¾Ñ‚Ð¾Ð²Ð° Ð·Ð°Ð¿ÑƒÑÑ‚Ð¸Ñ‚ÑŒ Ð·Ð°Ð´Ð°Ñ‡Ñƒ, Ð½Ð¾ ÑÐ½Ð°Ñ‡Ð°Ð»Ð° ÑÐºÐ°Ð¶Ð¸Ñ‚Ðµ, Ð² ÐºÐ°ÐºÐ¸Ñ… Ñ„Ð°Ð¹Ð»Ð°Ñ… Ð¸Ð»Ð¸ "
                "Ð² ÐºÐ°ÐºÐ¾Ð¹ Ð¾Ð±Ð»Ð°ÑÑ‚Ð¸ Ð¿Ñ€Ð¾ÐµÐºÑ‚Ð° Ð¼Ð½Ðµ Ñ€Ð°Ð±Ð¾Ñ‚Ð°Ñ‚ÑŒ."
            ),
            display=_voice_display_card(
                title="Scope Needed",
                body=(
                    "Voice task requests need at least one scope file, for example "
                    "`api/app.py` or `orchestrator/dev_task.py`."
                ),
                status="needs_input",
            ),
            suggested_replies=[
                "Use scope api/app.py",
                "Use scope orchestrator/dev_task.py",
            ],
        )
    run_payload = ProductTaskRunRequest(
        objective=transcript,
        scope_files=payload.scope_files,
        user_id=payload.user_id,
        user_role=payload.user_role,
    )
    result = _run_product_task_internal(
        run_payload,
        principal=principal,
        request_source="voice",
    )
    task_summary = {
        "task_id": str(result.get("task_id", "")).strip(),
        "status": _product_status_from_result(result),
        "lifecycle_state": str(result.get("lifecycle_state", "")).strip(),
        "system_status_text": str(result.get("system_status_text", "")).strip(),
        "user_summary": str(result.get("user_summary", "")).strip(),
        "system_report": dict(result.get("system_report", {})),
    }
    return _voice_response(
        intent="run_task",
        interaction_state="completed",
        spoken_response=(
            f"Ð—Ð°Ð´Ð°Ñ‡Ð° {task_summary['task_id']} Ð¾Ð±Ñ€Ð°Ð±Ð¾Ñ‚Ð°Ð½Ð°. "
            f"Ð¢ÐµÐºÑƒÑ‰ÐµÐµ ÑÐ¾ÑÑ‚Ð¾ÑÐ½Ð¸Ðµ: {task_summary['lifecycle_state']}."
        ),
        display=_voice_display_card(
            title="Task Processed",
            body=task_summary["user_summary"],
            status=task_summary["lifecycle_state"],
        ),
        suggested_replies=[
            f"Status for {task_summary['task_id']}",
            "Show recent tasks",
        ],
        task=task_summary,
    )


@app.get("/execute-controlled/history")
def list_controlled_execution_history(
    x_df_actor_id: str | None = Header(default=None),
    x_df_role: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    x_df_principal_token: str | None = Header(default=None),
):
    _require_permission(
        _principal_from_headers(
            x_df_actor_id,
            x_df_role,
            authorization,
            x_df_principal_token,
        ),
        "read_history",
    )
    records = list(reversed(load_execution_records()))
    return {
        "count": len(records),
        "items": [_history_summary_item(record) for record in records],
    }


@app.get("/execute-controlled/history/integrity")
def get_controlled_execution_integrity(
    x_df_actor_id: str | None = Header(default=None),
    x_df_role: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    x_df_principal_token: str | None = Header(default=None),
):
    _require_permission(
        _principal_from_headers(
            x_df_actor_id,
            x_df_role,
            authorization,
            x_df_principal_token,
        ),
        "verify_integrity",
    )
    return verify_execution_records()


@app.get("/execute-controlled/history/{task_id}")
def get_controlled_execution_report(
    task_id: str,
    x_df_actor_id: str | None = Header(default=None),
    x_df_role: str | None = Header(default=None),
    authorization: str | None = Header(default=None),
    x_df_principal_token: str | None = Header(default=None),
):
    _require_permission(
        _principal_from_headers(
            x_df_actor_id,
            x_df_role,
            authorization,
            x_df_principal_token,
        ),
        "read_report",
    )
    record = find_execution_record(task_id)
    if record is None:
        return {"detail": "controlled execution report not found", "task_id": task_id}
    return record
