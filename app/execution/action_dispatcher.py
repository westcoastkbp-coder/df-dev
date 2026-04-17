from __future__ import annotations

import inspect
import json
import signal
import re
import threading
import time
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from app.adapters.browser_adapter import execute_browser_action
from app.adapters.email_adapter import execute_email_action
from app.adapters.openai_adapter import execute_openai_action
from app.adapters.printer_adapter import execute_printer_action
from app.execution.action_contract import (
    ActionContractViolation,
    build_action_result_contract,
    validate_action_contract,
    validate_action_result_contract,
)
from app.execution.idempotency_store import (
    IdempotencyStore,
    IdempotencyStoreError,
)
from app.execution.paths import OUTPUT_DIR, ROOT_DIR
from app.memory.memory_object import make_trace_object
from app.memory.memory_registry import compute_artifact_key, register_artifact
from app.ownerbox.domain import build_ownerbox_trace_metadata, normalize_ownerbox_domain_binding
from app.orchestrator.task_memory import store_task_result
from runtime.system_log import log_event


OPENAI_ACTION_TYPE = "OPENAI_REQUEST"
EMAIL_ACTION_TYPE = "EMAIL_ACTION"
BROWSER_ACTION_TYPE = "BROWSER_ACTION"
PRINT_DOCUMENT_ACTION_TYPE = "PRINT_DOCUMENT"
OPENAI_ADAPTER_NAME = "openai_adapter"
EMAIL_ADAPTER_NAME = "email_adapter"
BROWSER_ADAPTER_NAME = "browser_adapter"
PRINTER_ADAPTER_NAME = "printer_adapter"
BLOCKED_ADAPTER_NAME = "none"
DEFAULT_MEMORY_DOMAIN = "dev"
TRACE_ARTIFACT_TYPE = "execution_trace"
TRACE_ARTIFACT_DIR = OUTPUT_DIR / "traces" / "actions"
_SAFE_FILENAME_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")
NORMALIZED_ERROR_CODES = frozenset(
    {
        "validation_error",
        "timeout",
        "transport_error",
        "provider_error",
        "provider_not_configured",
        "device_not_available",
        "unsupported_operation",
        "persistence_error",
        "state_corrupted",
        "unknown_error",
    }
)
RETRYABLE_ERROR_CODES = frozenset({"timeout", "transport_error"})
DEFAULT_DISPATCH_TIMEOUT_SECONDS = 30.0


class DispatchTimeoutError(TimeoutError):
    pass


@dataclass(frozen=True, slots=True)
class _CachedActionOutcome:
    action_id: str
    idempotency_key: str
    result: dict[str, object]


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _safe_filename(value: object) -> str:
    normalized = _normalize_text(value) or "unknown_action"
    safe = _SAFE_FILENAME_PATTERN.sub("_", normalized).strip("._")
    return safe or "unknown_action"


def _fallback_action_id(action_contract: object) -> str:
    if isinstance(action_contract, Mapping):
        candidate = _normalize_text(action_contract.get("action_id"))
        if candidate:
            return candidate
    return "unknown_action"


def _dispatcher_latency_ms(started_at_monotonic: float) -> int:
    return max(0, int(round((time.monotonic() - started_at_monotonic) * 1000)))


def _executor_supports_kwarg(executor: Callable[..., dict[str, object]], name: str) -> bool:
    try:
        signature = inspect.signature(executor)
    except (TypeError, ValueError):
        return False
    for parameter in signature.parameters.values():
        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            return True
        if parameter.name == name:
            return True
    return False


def _normalize_error_code(value: object) -> str | None:
    normalized = _normalize_text(value).lower()
    if not normalized:
        return None
    if normalized == "unsupported_action_type":
        return "unsupported_operation"
    if normalized not in NORMALIZED_ERROR_CODES:
        return "unknown_error"
    return normalized


def _result_attempt_count(result: Mapping[str, object]) -> int:
    metadata = _normalize_mapping(_normalize_mapping(result.get("payload")).get("metadata"))
    candidate = metadata.get("attempt_count")
    try:
        normalized = int(candidate)
    except (TypeError, ValueError):
        return 0 if _normalize_text(result.get("status")).lower() == "blocked" else 1
    return max(0, normalized)


def _with_metadata(
    result: Mapping[str, object],
    *,
    idempotency_cache_hit: bool | None = None,
    dispatch_timeout_seconds: float | None = None,
) -> dict[str, object]:
    payload = dict(_normalize_mapping(result.get("payload")))
    metadata = dict(_normalize_mapping(payload.get("metadata")))
    metadata["attempt_count"] = _result_attempt_count(result)
    if dispatch_timeout_seconds is not None:
        metadata["dispatch_timeout_seconds"] = dispatch_timeout_seconds
    if idempotency_cache_hit is not None:
        metadata["idempotency_cache_hit"] = idempotency_cache_hit
    payload["metadata"] = metadata
    return build_action_result_contract(
        action_id=_normalize_text(result.get("action_id")),
        status=_normalize_text(result.get("status")),
        result_type=_normalize_text(result.get("result_type")),
        payload=payload,
        error_code=_normalize_error_code(result.get("error_code")),
        error_message=result.get("error_message"),
        timestamp=result.get("timestamp"),
    )


def _normalize_result_contract(result: Mapping[str, object]) -> dict[str, object]:
    return _with_metadata(result)


def _cache_key(action_id: str, idempotency_key: str) -> tuple[str, str]:
    return (action_id, idempotency_key)


_ACTION_OUTCOME_CACHE: dict[tuple[str, str], _CachedActionOutcome] = {}


def _should_cache_result(result: Mapping[str, object]) -> bool:
    status = _normalize_text(result.get("status")).lower()
    error_code = _normalize_error_code(result.get("error_code"))
    if status == "success":
        return True
    if status == "blocked":
        return True
    if status == "failed" and error_code not in RETRYABLE_ERROR_CODES:
        return True
    return False


def _should_hot_cache_result(result: Mapping[str, object]) -> bool:
    error_code = _normalize_error_code(result.get("error_code"))
    if error_code in {"persistence_error", "state_corrupted"}:
        return False
    return _should_cache_result(result)


def _cached_dispatch_result(
    *,
    cached_result: Mapping[str, object],
    validated_action: Mapping[str, object],
    operation: str,
    dispatcher_start_time: str,
    started_at_monotonic: float,
    trace_path: Path,
    normalized_domain_binding: Mapping[str, object],
    normalized_dispatch_context: Mapping[str, object],
    resolved_memory_domain: str,
) -> dict[str, object]:
    adapter_used = _normalize_text(
        _normalize_mapping(
            _normalize_mapping(cached_result.get("payload")).get("metadata")
        ).get("adapter_used")
    ) or BLOCKED_ADAPTER_NAME
    result = _attach_execution_metadata(
        _with_metadata(
            cached_result,
            idempotency_cache_hit=True,
        ),
        adapter_used=adapter_used,
        operation=operation,
        dispatcher_start_time=dispatcher_start_time,
        dispatcher_end_time=_utc_timestamp(),
        dispatcher_latency_ms=_dispatcher_latency_ms(started_at_monotonic),
        trace_artifact_path=str(trace_path),
        memory_evidence_registered=False,
        domain_binding=normalized_domain_binding,
        dispatch_context=normalized_dispatch_context,
        idempotency_cache_hit=True,
    )
    trace_payload = _trace_payload(
        action_contract=validated_action,
        action_result=result,
        adapter_used=adapter_used,
        trace_artifact_path=trace_path,
        domain_binding=normalized_domain_binding,
        memory_domain=resolved_memory_domain,
    )
    log_event(
        "trace",
        trace_payload,
        task_id=_normalize_text(validated_action.get("action_id")),
        status=result["status"],
    )

    memory_evidence_registered = False
    try:
        _persist_trace_artifact(
            action_id=_normalize_text(validated_action.get("action_id")),
            action_type=_normalize_text(validated_action.get("action_type")),
            domain=resolved_memory_domain,
            trace_payload=trace_payload,
            trace_artifact_path=trace_path,
            domain_binding=normalized_domain_binding,
        )
    except Exception as exc:
        log_event(
            "memory",
            {
                "action_id": _normalize_text(validated_action.get("action_id")),
                "status": "memory_evidence_failed",
                "reason": _normalize_text(exc) or "memory evidence persistence failed",
            },
            task_id=_normalize_text(validated_action.get("action_id")),
            status="observed",
        )

    return _attach_execution_metadata(
        result,
        adapter_used=adapter_used,
        operation=operation,
        dispatcher_start_time=_normalize_text(
            _normalize_mapping(_normalize_mapping(result.get("payload")).get("metadata")).get(
                "dispatcher_start_time"
            )
        )
        or dispatcher_start_time,
        dispatcher_end_time=_utc_timestamp(),
        dispatcher_latency_ms=_dispatcher_latency_ms(started_at_monotonic),
        trace_artifact_path=str(trace_path),
        memory_evidence_registered=memory_evidence_registered,
        domain_binding=normalized_domain_binding,
        dispatch_context=normalized_dispatch_context,
        idempotency_cache_hit=True,
    )


def _normalize_timeout_value(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        normalized = float(value)
    except (TypeError, ValueError):
        return None
    if normalized <= 0:
        return None
    return normalized


def _dispatch_timeout_seconds(
    *,
    action_type: str,
    parameters: Mapping[str, object],
    dispatch_context: Mapping[str, object],
) -> float:
    dispatch_timeout = _normalize_timeout_value(dispatch_context.get("step_timeout_seconds"))
    if dispatch_timeout is not None:
        return dispatch_timeout
    parameter_timeout = _normalize_timeout_value(parameters.get("timeout_seconds"))
    if parameter_timeout is not None:
        return parameter_timeout
    if action_type == BROWSER_ACTION_TYPE:
        return 10.0
    return DEFAULT_DISPATCH_TIMEOUT_SECONDS


@contextmanager
def _bounded_dispatch_timeout(timeout_seconds: float | None):
    if (
        timeout_seconds is None
        or timeout_seconds <= 0
        or threading.current_thread() is not threading.main_thread()
        or not hasattr(signal, "SIGALRM")
        or not hasattr(signal, "setitimer")
    ):
        yield
        return
    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.setitimer(signal.ITIMER_REAL, timeout_seconds)

    def _handle_timeout(_signum, _frame):
        raise DispatchTimeoutError("action dispatch timed out")

    signal.signal(signal.SIGALRM, _handle_timeout)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0.0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer != (0.0, 0.0):
            signal.setitimer(signal.ITIMER_REAL, previous_timer[0], previous_timer[1])


def _trace_artifact_path(action_id: str) -> Path:
    return ROOT_DIR / TRACE_ARTIFACT_DIR / f"{_safe_filename(action_id)}.json"


def _build_dispatch_metadata(
    *,
    existing_metadata: Mapping[str, object] | None,
    adapter_used: str,
    operation: str = "",
    dispatcher_start_time: str,
    dispatcher_end_time: str,
    dispatcher_latency_ms: int,
    trace_artifact_path: str = "",
    memory_evidence_registered: bool | None = None,
    domain_binding: Mapping[str, object] | None = None,
    dispatch_context: Mapping[str, object] | None = None,
    idempotency_cache_hit: bool | None = None,
) -> dict[str, object]:
    metadata = dict(_normalize_mapping(existing_metadata))
    metadata["adapter_used"] = adapter_used
    if operation:
        metadata["operation"] = operation
    metadata["dispatcher_start_time"] = dispatcher_start_time
    metadata["dispatcher_end_time"] = dispatcher_end_time
    metadata["dispatcher_latency_ms"] = dispatcher_latency_ms
    trace_metadata = build_ownerbox_trace_metadata(domain_binding)
    if trace_metadata:
        metadata["domain_metadata"] = trace_metadata
    if trace_artifact_path:
        metadata["trace_artifact_path"] = trace_artifact_path
    if memory_evidence_registered is not None:
        metadata["memory_evidence_registered"] = memory_evidence_registered
    try:
        attempt_count = int(
            metadata.get("attempt_count", 0 if adapter_used == BLOCKED_ADAPTER_NAME else 1)
        )
    except (TypeError, ValueError):
        attempt_count = 0 if adapter_used == BLOCKED_ADAPTER_NAME else 1
    metadata["attempt_count"] = max(0, attempt_count)
    if idempotency_cache_hit is not None:
        metadata["idempotency_cache_hit"] = idempotency_cache_hit
    normalized_dispatch_context = _normalize_mapping(dispatch_context)
    for key in (
        "owner_id",
        "trust_class",
        "approval_id",
        "workflow_id",
        "step_id",
        "step_timeout_seconds",
        "scenario_type",
    ):
        value = _normalize_text(normalized_dispatch_context.get(key))
        if value:
            metadata[key] = value
    return metadata


def _attach_execution_metadata(
    result: Mapping[str, object],
    *,
    adapter_used: str,
    operation: str = "",
    dispatcher_start_time: str,
    dispatcher_end_time: str,
    dispatcher_latency_ms: int,
    trace_artifact_path: str = "",
    memory_evidence_registered: bool | None = None,
    domain_binding: Mapping[str, object] | None = None,
    dispatch_context: Mapping[str, object] | None = None,
    idempotency_cache_hit: bool | None = None,
) -> dict[str, object]:
    payload = dict(_normalize_mapping(result.get("payload")))
    payload["metadata"] = _build_dispatch_metadata(
        existing_metadata=_normalize_mapping(payload.get("metadata")),
        adapter_used=adapter_used,
        operation=operation,
        dispatcher_start_time=dispatcher_start_time,
        dispatcher_end_time=dispatcher_end_time,
        dispatcher_latency_ms=dispatcher_latency_ms,
        trace_artifact_path=trace_artifact_path,
        memory_evidence_registered=memory_evidence_registered,
        domain_binding=domain_binding,
        dispatch_context=dispatch_context,
        idempotency_cache_hit=idempotency_cache_hit,
    )
    return build_action_result_contract(
        action_id=_normalize_text(result.get("action_id")),
        status=_normalize_text(result.get("status")),
        result_type=_normalize_text(result.get("result_type")),
        payload=payload,
        error_code=_normalize_error_code(result.get("error_code")),
        error_message=result.get("error_message"),
        timestamp=result.get("timestamp"),
    )


def _build_blocked_result(
    *,
    action_id: str,
    result_type: str,
    error_code: str,
    error_message: str,
    adapter_used: str,
    operation: str = "",
    dispatcher_start_time: str,
    dispatcher_end_time: str,
    dispatcher_latency_ms: int,
    payload: Mapping[str, object] | None = None,
    domain_binding: Mapping[str, object] | None = None,
    dispatch_context: Mapping[str, object] | None = None,
) -> dict[str, object]:
    base_payload = dict(_normalize_mapping(payload))
    base_payload["metadata"] = _build_dispatch_metadata(
        existing_metadata=_normalize_mapping(base_payload.get("metadata")),
        adapter_used=adapter_used,
        operation=operation,
        dispatcher_start_time=dispatcher_start_time,
        dispatcher_end_time=dispatcher_end_time,
        dispatcher_latency_ms=dispatcher_latency_ms,
        domain_binding=domain_binding,
        dispatch_context=dispatch_context,
    )
    return build_action_result_contract(
        action_id=action_id,
        status="blocked",
        result_type=result_type,
        payload=base_payload,
        error_code=_normalize_error_code(error_code),
        error_message=error_message,
    )


def _build_failed_result(
    *,
    action_id: str,
    result_type: str,
    error_code: str,
    error_message: str,
    adapter_used: str,
    operation: str = "",
    dispatcher_start_time: str,
    dispatcher_end_time: str,
    dispatcher_latency_ms: int,
    payload: Mapping[str, object] | None = None,
    domain_binding: Mapping[str, object] | None = None,
    dispatch_context: Mapping[str, object] | None = None,
) -> dict[str, object]:
    base_payload = dict(_normalize_mapping(payload))
    base_payload["metadata"] = _build_dispatch_metadata(
        existing_metadata=_normalize_mapping(base_payload.get("metadata")),
        adapter_used=adapter_used,
        operation=operation,
        dispatcher_start_time=dispatcher_start_time,
        dispatcher_end_time=dispatcher_end_time,
        dispatcher_latency_ms=dispatcher_latency_ms,
        domain_binding=domain_binding,
        dispatch_context=dispatch_context,
    )
    return build_action_result_contract(
        action_id=action_id,
        status="failed",
        result_type=result_type,
        payload=base_payload,
        error_code=_normalize_error_code(error_code),
        error_message=error_message,
    )


def _build_dry_run_result(
    *,
    action_id: str,
    adapter_used: str,
    dispatcher_start_time: str,
    dispatcher_end_time: str,
    dispatcher_latency_ms: int,
    domain_binding: Mapping[str, object] | None = None,
) -> dict[str, object]:
    trace_metadata = build_ownerbox_trace_metadata(domain_binding)
    return build_action_result_contract(
        action_id=action_id,
        status="success",
        result_type="simulation",
        payload={
            "note": "dry run",
            "metadata": {
                "dry_run": True,
                "simulation_mode": "dry_run",
                "provider": "openai",
                "attempt_count": 0,
                "input_tokens": None,
                "output_tokens": None,
                "total_tokens": None,
                "adapter_used": adapter_used,
                "dispatcher_start_time": dispatcher_start_time,
                "dispatcher_end_time": dispatcher_end_time,
                "dispatcher_latency_ms": dispatcher_latency_ms,
                **({"domain_metadata": trace_metadata} if trace_metadata else {}),
            },
        },
    )


def _usage_snapshot(metadata: Mapping[str, object]) -> dict[str, object]:
    return {
        "input_tokens": metadata.get("input_tokens"),
        "output_tokens": metadata.get("output_tokens"),
        "total_tokens": metadata.get("total_tokens"),
    }


def _trace_payload(
    *,
    action_contract: Mapping[str, object],
    action_result: Mapping[str, object],
    adapter_used: str,
    trace_artifact_path: Path,
    domain_binding: Mapping[str, object] | None = None,
    memory_domain: str = DEFAULT_MEMORY_DOMAIN,
) -> dict[str, object]:
    payload = _normalize_mapping(action_result.get("payload"))
    metadata = _normalize_mapping(payload.get("metadata"))
    parameters = _normalize_mapping(action_contract.get("parameters"))
    normalized_domain_binding = _normalize_mapping(domain_binding)
    trace_payload = {
        "type": TRACE_ARTIFACT_TYPE,
        "action_id": _normalize_text(action_contract.get("action_id")),
        "idempotency_key": _normalize_text(action_contract.get("idempotency_key")) or None,
        "action_type": _normalize_text(action_contract.get("action_type")),
        "adapter_used": adapter_used,
        "backend_used": _normalize_text(
            metadata.get("backend_used") or metadata.get("provider")
        )
        or None,
        "memory_domain": memory_domain,
        "execution_mode": _normalize_text(action_contract.get("execution_mode")),
        "operation": _normalize_text(metadata.get("operation") or parameters.get("operation")) or None,
        "owner_id": _normalize_text(metadata.get("owner_id") or normalized_domain_binding.get("owner_id"))
        or None,
        "trust_class": _normalize_text(
            metadata.get("trust_class") or normalized_domain_binding.get("trust_class")
        )
        or None,
        "approval_id": _normalize_text(metadata.get("approval_id")) or None,
        "scenario_type": _normalize_text(metadata.get("scenario_type")) or None,
        "workflow_id": _normalize_text(metadata.get("workflow_id")) or None,
        "step_id": _normalize_text(metadata.get("step_id")) or None,
        "printer_name": _normalize_text(metadata.get("printer_name") or parameters.get("printer_name"))
        or None,
        "started_at": _normalize_text(metadata.get("dispatcher_start_time")) or None,
        "completed_at": _normalize_text(metadata.get("dispatcher_end_time")) or None,
        "timestamp": _utc_timestamp(),
        "result_status": _normalize_text(action_result.get("status")),
        "result_type": _normalize_text(action_result.get("result_type")),
        "error_code": _normalize_error_code(action_result.get("error_code")),
        "attempt_count": _result_attempt_count(action_result),
        "retry_status": _normalize_text(metadata.get("retry_status")) or None,
        "idempotency_cache_hit": bool(metadata.get("idempotency_cache_hit")),
        "dispatch_timeout_seconds": metadata.get("dispatch_timeout_seconds"),
        "latency_ms": metadata.get("dispatcher_latency_ms", metadata.get("latency_ms")),
        "usage": _usage_snapshot(metadata),
        "result_evidence": {
            "payload_keys": sorted(payload),
            "trace_artifact_path": str(trace_artifact_path),
        },
    }
    trace_metadata = build_ownerbox_trace_metadata(domain_binding)
    if trace_metadata:
        trace_payload["domain_metadata"] = trace_metadata
    return trace_payload


def _domain_binding_refs(domain_binding: Mapping[str, object] | None) -> list[str]:
    binding = normalize_ownerbox_domain_binding(domain_binding)
    if not binding:
        return []
    refs = [
        f"domain:{binding['domain_id']}",
        f"owner:{binding['owner_id']}",
    ]
    for key in ("memory_scope_ref", "action_scope_ref", "policy_scope_ref"):
        value = _normalize_text(binding.get(key))
        if value:
            refs.append(f"scope:{value}")
    trust_profile_id = _normalize_text(binding.get("trust_profile_id"))
    if trust_profile_id:
        refs.append(f"trust_profile:{trust_profile_id}")
    return refs


def _domain_binding_tags(domain_binding: Mapping[str, object] | None) -> list[str]:
    binding = normalize_ownerbox_domain_binding(domain_binding)
    if not binding:
        return []
    tags = [_normalize_text(binding.get("domain_type")).lower()]
    trust_class = _normalize_text(binding.get("trust_class")).lower()
    if trust_class:
        tags.append(trust_class)
    return [tag for tag in tags if tag]


def _persist_trace_artifact(
    *,
    action_id: str,
    action_type: str,
    domain: str,
    trace_payload: Mapping[str, object],
    trace_artifact_path: Path,
    domain_binding: Mapping[str, object] | None = None,
) -> None:
    trace_artifact_path.parent.mkdir(parents=True, exist_ok=True)
    trace_refs = [f"action:{action_id}", *_domain_binding_refs(domain_binding)]
    trace_tags = [
        "action_dispatch",
        _normalize_text(action_type).lower(),
        *_domain_binding_tags(domain_binding),
    ]
    trace_object = make_trace_object(
        id=action_id,
        domain=domain,
        payload=dict(trace_payload),
        local_path=trace_artifact_path,
        artifact_type=TRACE_ARTIFACT_TYPE,
        logical_key=compute_artifact_key(domain, TRACE_ARTIFACT_TYPE, action_id),
        refs=trace_refs,
        tags=trace_tags,
    ).to_dict()
    trace_artifact_path.write_text(
        json.dumps(trace_object, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    register_artifact(
        action_id,
        domain,
        TRACE_ARTIFACT_TYPE,
        trace_artifact_path,
        logical_key=compute_artifact_key(domain, TRACE_ARTIFACT_TYPE, action_id),
        refs=trace_refs,
        tags=trace_tags,
        payload=dict(trace_payload),
        memory_class="trace",
        execution_role="evidence",
    )


def _store_result_memory_evidence(
    *,
    action_contract: Mapping[str, object],
    action_id: str,
    action_result: Mapping[str, object],
    trace_artifact_path: Path,
) -> None:
    parameters = _normalize_mapping(action_contract.get("parameters"))
    summary_parts = [
        f"action_type={_normalize_text(action_contract.get('action_type'))}",
        f"operation={_normalize_text(parameters.get('operation')) or 'none'}",
        f"status={_normalize_text(action_result.get('status'))}",
        f"result_type={_normalize_text(action_result.get('result_type'))}",
    ]
    error_code = _normalize_text(action_result.get("error_code"))
    if error_code:
        summary_parts.append(f"error={error_code}")
    summary_parts.append(f"trace={trace_artifact_path}")
    store_task_result(
        {
            "task_id": action_id,
            "result_type": _normalize_text(action_result.get("result_type")),
            "result_summary": " ".join(summary_parts),
        }
    )


def dispatch_action(
    action_contract: object,
    *,
    openai_executor: Callable[[object], dict[str, object]] = execute_openai_action,
    browser_executor: Callable[[object], dict[str, object]] = execute_browser_action,
    email_executor: Callable[[object], dict[str, object]] = execute_email_action,
    printer_executor: Callable[[object], dict[str, object]] = execute_printer_action,
    idempotency_store: IdempotencyStore | None = None,
    memory_domain: str = DEFAULT_MEMORY_DOMAIN,
    domain_binding: Mapping[str, object] | None = None,
    dispatch_context: Mapping[str, object] | None = None,
) -> dict[str, object]:
    dispatcher_start_time = _utc_timestamp()
    started_at_monotonic = time.monotonic()
    fallback_action_id = _fallback_action_id(action_contract)
    adapter_used = BLOCKED_ADAPTER_NAME
    operation = _normalize_text(
        _normalize_mapping(_normalize_mapping(action_contract).get("parameters")).get("operation")
    )
    normalized_domain_binding = normalize_ownerbox_domain_binding(domain_binding)
    normalized_dispatch_context = _normalize_mapping(dispatch_context)
    resolved_memory_domain = _normalize_text(memory_domain) or DEFAULT_MEMORY_DOMAIN
    resolved_idempotency_store = idempotency_store or IdempotencyStore()
    if normalized_domain_binding and resolved_memory_domain == DEFAULT_MEMORY_DOMAIN:
        resolved_memory_domain = (
            _normalize_text(normalized_domain_binding.get("domain_type")) or DEFAULT_MEMORY_DOMAIN
        )

    try:
        validated_action = validate_action_contract(action_contract)
    except ActionContractViolation as exc:
        dispatcher_end_time = _utc_timestamp()
        result = _build_blocked_result(
            action_id=fallback_action_id,
            result_type="dispatch",
            error_code="validation_error",
            error_message=str(exc),
            adapter_used=adapter_used,
            operation=operation,
            dispatcher_start_time=dispatcher_start_time,
            dispatcher_end_time=dispatcher_end_time,
            dispatcher_latency_ms=_dispatcher_latency_ms(started_at_monotonic),
            domain_binding=normalized_domain_binding,
            dispatch_context=normalized_dispatch_context,
        )
        trace_path = _trace_artifact_path(fallback_action_id)
        trace_payload = _trace_payload(
            action_contract={"action_id": fallback_action_id, "action_type": "UNKNOWN_ACTION", "execution_mode": ""},
            action_result=result,
            adapter_used=adapter_used,
            trace_artifact_path=trace_path,
            domain_binding=normalized_domain_binding,
            memory_domain=resolved_memory_domain,
        )
        log_event("trace", trace_payload, task_id=fallback_action_id, status=result["status"])
        return result

    action_id = _normalize_text(validated_action.get("action_id"))
    action_type = _normalize_text(validated_action.get("action_type"))
    idempotency_key = _normalize_text(validated_action.get("idempotency_key"))
    operation = _normalize_text(_normalize_mapping(validated_action.get("parameters")).get("operation"))
    trace_path = _trace_artifact_path(action_id)
    cached_outcome = _ACTION_OUTCOME_CACHE.get(_cache_key(action_id, idempotency_key))
    if cached_outcome is not None:
        return _cached_dispatch_result(
            cached_result=cached_outcome.result,
            validated_action=validated_action,
            operation=operation,
            dispatcher_start_time=dispatcher_start_time,
            started_at_monotonic=started_at_monotonic,
            trace_path=trace_path,
            normalized_domain_binding=normalized_domain_binding,
            normalized_dispatch_context=normalized_dispatch_context,
            resolved_memory_domain=resolved_memory_domain,
        )
    try:
        persisted_record = resolved_idempotency_store.get(
            action_id=action_id,
            idempotency_key=idempotency_key,
        )
    except IdempotencyStoreError as exc:
        dispatcher_end_time = _utc_timestamp()
        store_failure = _build_failed_result(
            action_id=action_id,
            result_type="dispatch",
            error_code=exc.code,
            error_message=exc.reason,
            adapter_used=adapter_used,
            operation=operation,
            dispatcher_start_time=dispatcher_start_time,
            dispatcher_end_time=dispatcher_end_time,
            dispatcher_latency_ms=_dispatcher_latency_ms(started_at_monotonic),
            payload={"diagnostic": {"persistence_operation": exc.operation}},
            domain_binding=normalized_domain_binding,
            dispatch_context=normalized_dispatch_context,
        )
        trace_payload = _trace_payload(
            action_contract=validated_action,
            action_result=store_failure,
            adapter_used=adapter_used,
            trace_artifact_path=trace_path,
            domain_binding=normalized_domain_binding,
            memory_domain=resolved_memory_domain,
        )
        log_event("trace", trace_payload, task_id=action_id, status=store_failure["status"])
        return store_failure
    if persisted_record is not None:
        _ACTION_OUTCOME_CACHE[_cache_key(action_id, idempotency_key)] = _CachedActionOutcome(
            action_id=persisted_record.action_id,
            idempotency_key=persisted_record.idempotency_key,
            result=dict(persisted_record.result),
        )
        return _cached_dispatch_result(
            cached_result=persisted_record.result,
            validated_action=validated_action,
            operation=operation,
            dispatcher_start_time=dispatcher_start_time,
            started_at_monotonic=started_at_monotonic,
            trace_path=trace_path,
            normalized_domain_binding=normalized_domain_binding,
            normalized_dispatch_context=normalized_dispatch_context,
            resolved_memory_domain=resolved_memory_domain,
        )

    dispatch_timeout_seconds = _dispatch_timeout_seconds(
        action_type=action_type,
        parameters=_normalize_mapping(validated_action.get("parameters")),
        dispatch_context=normalized_dispatch_context,
    )
    dispatch_context_with_timeout = dict(normalized_dispatch_context)
    dispatch_context_with_timeout.setdefault(
        "step_timeout_seconds",
        str(dispatch_timeout_seconds),
    )

    if action_type not in {
        OPENAI_ACTION_TYPE,
        BROWSER_ACTION_TYPE,
        EMAIL_ACTION_TYPE,
        PRINT_DOCUMENT_ACTION_TYPE,
    }:
        dispatcher_end_time = _utc_timestamp()
        result = _build_blocked_result(
            action_id=action_id,
            result_type="dispatch",
            error_code="unsupported_operation",
            error_message=f"unsupported action_type for dispatcher: {action_type}",
            adapter_used=adapter_used,
            operation=operation,
            dispatcher_start_time=dispatcher_start_time,
            dispatcher_end_time=dispatcher_end_time,
            dispatcher_latency_ms=_dispatcher_latency_ms(started_at_monotonic),
            domain_binding=normalized_domain_binding,
            dispatch_context=dispatch_context_with_timeout,
        )
    else:
        if action_type == OPENAI_ACTION_TYPE:
            adapter_used = OPENAI_ADAPTER_NAME
            executor: Callable[[object], dict[str, object]] = openai_executor
        elif action_type == BROWSER_ACTION_TYPE:
            adapter_used = BROWSER_ADAPTER_NAME
            executor = browser_executor
        elif action_type == EMAIL_ACTION_TYPE:
            adapter_used = EMAIL_ADAPTER_NAME
            executor = email_executor
        else:
            adapter_used = PRINTER_ADAPTER_NAME
            executor = printer_executor

        if (
            action_type == OPENAI_ACTION_TYPE
            and _normalize_text(validated_action.get("execution_mode")) == "dry_run"
        ):
            dispatcher_end_time = _utc_timestamp()
            result = _build_dry_run_result(
                action_id=action_id,
                adapter_used=adapter_used,
                dispatcher_start_time=dispatcher_start_time,
                dispatcher_end_time=dispatcher_end_time,
                dispatcher_latency_ms=_dispatcher_latency_ms(started_at_monotonic),
                domain_binding=normalized_domain_binding,
            )
        else:
            executor_kwargs: dict[str, object] = {}
            if action_type == OPENAI_ACTION_TYPE and _executor_supports_kwarg(executor, "config"):
                executor_kwargs["config"] = {
                    "timeout_seconds": max(1, int(round(dispatch_timeout_seconds)))
                }
            try:
                with _bounded_dispatch_timeout(dispatch_timeout_seconds):
                    adapter_result = validate_action_result_contract(
                        executor(validated_action, **executor_kwargs)
                    )
                adapter_result = _normalize_result_contract(adapter_result)
            except DispatchTimeoutError:
                dispatcher_end_time = _utc_timestamp()
                result = _build_failed_result(
                    action_id=action_id,
                    result_type="dispatch",
                    error_code="timeout",
                    error_message="action dispatch timed out",
                    adapter_used=adapter_used,
                    operation=operation,
                    dispatcher_start_time=dispatcher_start_time,
                    dispatcher_end_time=dispatcher_end_time,
                    dispatcher_latency_ms=_dispatcher_latency_ms(started_at_monotonic),
                    payload={"diagnostic": {"timeout_seconds": dispatch_timeout_seconds}},
                    domain_binding=normalized_domain_binding,
                    dispatch_context=dispatch_context_with_timeout,
                )
            except ActionContractViolation:
                dispatcher_end_time = _utc_timestamp()
                result = _build_failed_result(
                    action_id=action_id,
                    result_type="dispatch",
                    error_code="unknown_error",
                    error_message="adapter returned invalid action result",
                    adapter_used=adapter_used,
                    operation=operation,
                    dispatcher_start_time=dispatcher_start_time,
                    dispatcher_end_time=dispatcher_end_time,
                    dispatcher_latency_ms=_dispatcher_latency_ms(started_at_monotonic),
                    domain_binding=normalized_domain_binding,
                    dispatch_context=dispatch_context_with_timeout,
                )
            except Exception as exc:
                dispatcher_end_time = _utc_timestamp()
                result = _build_failed_result(
                    action_id=action_id,
                    result_type="dispatch",
                    error_code="unknown_error",
                    error_message="action dispatch failed",
                    adapter_used=adapter_used,
                    operation=operation,
                    dispatcher_start_time=dispatcher_start_time,
                    dispatcher_end_time=dispatcher_end_time,
                    dispatcher_latency_ms=_dispatcher_latency_ms(started_at_monotonic),
                    payload={"diagnostic": {"exception_type": type(exc).__name__}},
                    domain_binding=normalized_domain_binding,
                    dispatch_context=dispatch_context_with_timeout,
                )
            else:
                dispatcher_end_time = _utc_timestamp()
                result = _attach_execution_metadata(
                    adapter_result,
                    adapter_used=adapter_used,
                    operation=operation,
                    dispatcher_start_time=dispatcher_start_time,
                    dispatcher_end_time=dispatcher_end_time,
                    dispatcher_latency_ms=_dispatcher_latency_ms(started_at_monotonic),
                    domain_binding=normalized_domain_binding,
                    dispatch_context=dispatch_context_with_timeout,
                )

    if _should_cache_result(result):
        try:
            resolved_idempotency_store.record(
                action_id=action_id,
                idempotency_key=idempotency_key,
                action_type=action_type,
                execution_status=result.get("status"),
                result=result,
            )
        except IdempotencyStoreError as exc:
            result = _build_failed_result(
                action_id=action_id,
                result_type="dispatch",
                error_code=exc.code,
                error_message=exc.reason,
                adapter_used=adapter_used,
                operation=operation,
                dispatcher_start_time=dispatcher_start_time,
                dispatcher_end_time=_utc_timestamp(),
                dispatcher_latency_ms=_dispatcher_latency_ms(started_at_monotonic),
                payload={
                    "diagnostic": {
                        "persistence_operation": exc.operation,
                        "original_result_status": _normalize_text(result.get("status")),
                    }
                },
                domain_binding=normalized_domain_binding,
                dispatch_context=dispatch_context_with_timeout,
            )

    trace_payload = _trace_payload(
        action_contract=validated_action,
        action_result=result,
        adapter_used=adapter_used,
        trace_artifact_path=trace_path,
        domain_binding=normalized_domain_binding,
        memory_domain=resolved_memory_domain,
    )
    log_event("trace", trace_payload, task_id=action_id, status=result["status"])

    memory_evidence_registered = False
    try:
        _persist_trace_artifact(
            action_id=action_id,
            action_type=action_type,
            domain=resolved_memory_domain,
            trace_payload=trace_payload,
            trace_artifact_path=trace_path,
            domain_binding=normalized_domain_binding,
        )
        _store_result_memory_evidence(
            action_contract=validated_action,
            action_id=action_id,
            action_result=result,
            trace_artifact_path=trace_path,
        )
        memory_evidence_registered = True
    except Exception as exc:
        log_event(
            "memory",
            {
                "action_id": action_id,
                "status": "memory_evidence_failed",
                "reason": _normalize_text(exc) or "memory evidence persistence failed",
            },
            task_id=action_id,
            status="observed",
        )

    final_result = _attach_execution_metadata(
        result,
        adapter_used=adapter_used,
        operation=operation,
        dispatcher_start_time=_normalize_text(
            _normalize_mapping(_normalize_mapping(result.get("payload")).get("metadata")).get(
                "dispatcher_start_time"
            )
        )
        or dispatcher_start_time,
        dispatcher_end_time=_normalize_text(
            _normalize_mapping(_normalize_mapping(result.get("payload")).get("metadata")).get(
                "dispatcher_end_time"
            )
        )
        or _utc_timestamp(),
        dispatcher_latency_ms=int(
            _normalize_mapping(_normalize_mapping(result.get("payload")).get("metadata")).get(
                "dispatcher_latency_ms",
                _dispatcher_latency_ms(started_at_monotonic),
            )
            or 0
        ),
        trace_artifact_path=str(trace_path),
        memory_evidence_registered=memory_evidence_registered,
        domain_binding=normalized_domain_binding,
        dispatch_context=dispatch_context_with_timeout,
    )
    if _should_hot_cache_result(final_result):
        _ACTION_OUTCOME_CACHE[_cache_key(action_id, idempotency_key)] = _CachedActionOutcome(
            action_id=action_id,
            idempotency_key=idempotency_key,
            result=dict(final_result),
        )
    return final_result
