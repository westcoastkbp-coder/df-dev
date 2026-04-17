from __future__ import annotations

from collections.abc import Callable

from hardware.runtime_profile import get_runtime_profile
from app.orchestrator.task_lifecycle import record_task_event
from runtime.network.monitor import get_network_snapshot
from runtime.policy_engine import decide_execution_mode as decide_policy_execution_mode
from runtime.telemetry import collect_runtime_metrics
from runtime.validation import (
    authority_chain_snapshot,
    build_runtime_validation,
    build_runtime_verdict,
    voice_guardrail_boundary_payload,
)


EXECUTION_MODES = {"LOCAL", "REMOTE"}
COMPUTE_MODES = {"cpu_mode", "gpu_mode"}


def _network_event_details(network_snapshot: dict[str, object]) -> dict[str, object]:
    return {
        "event_type": "NETWORK",
        "interface_type": network_snapshot.get("interface_type", "unknown"),
        "ping_ms": network_snapshot.get("ping_ms"),
        "jitter_ms": network_snapshot.get("jitter_ms"),
        "loss": network_snapshot.get("packet_loss_pct"),
        "quality": network_snapshot.get("quality", "UNKNOWN"),
        "policy_mode": network_snapshot.get("policy_mode", "NORMAL"),
        "confidence": network_snapshot.get("telemetry_confidence", "LOW"),
        "sample_window_size": network_snapshot.get("sample_window_size", 0),
        "previous_state": network_snapshot.get("previous_state", "UNKNOWN"),
        "new_state": network_snapshot.get("new_state", "UNKNOWN"),
        "network_snapshot": dict(network_snapshot),
    }


def _runtime_event_details(runtime_decision: dict[str, object]) -> dict[str, object]:
    return {
        "event_type": "RUNTIME",
        "overall_runtime_state": runtime_decision.get("overall_runtime_state", "NORMAL"),
        "execution_preference": runtime_decision.get("execution_preference", "SAFE_LOCAL"),
        "execution_compute_mode": runtime_decision.get("execution_compute_mode", "cpu_mode"),
        "offload_recommended": bool(runtime_decision.get("offload_recommended", False)),
        "load_reduction_required": bool(runtime_decision.get("load_reduction_required", False)),
        "voice_safety_mode": bool(runtime_decision.get("voice_safety_mode", False)),
        "confidence": runtime_decision.get("confidence", "HIGH_CONFIDENCE"),
        "path_type": runtime_decision.get("path_type", "non_voice"),
        "constraints": list(runtime_decision.get("constraints", [])),
        "reasons": list(runtime_decision.get("reasons", [])),
        "signals": dict(runtime_decision.get("signals", {})),
        "adaptive_thresholds": dict(runtime_decision.get("adaptive_thresholds", {})),
        "trend_signals": dict(runtime_decision.get("trend_signals", {})),
        "signal_weights": dict(runtime_decision.get("signal_weights", {})),
        "confidence_factors": dict(runtime_decision.get("confidence_factors", {})),
        "prediction": dict(runtime_decision.get("prediction", {})),
        "preemptive_actions": dict(runtime_decision.get("preemptive_actions", {})),
        "runtime_decision": dict(runtime_decision),
    }


def _validation_event_details(runtime_validation: dict[str, object]) -> dict[str, object]:
    return {
        "event_type": "RUNTIME_VALIDATION",
        "state": runtime_validation.get("state", "PASSED"),
        "path_type": runtime_validation.get("path_type", "non_voice"),
        "allow_turn_start": bool(runtime_validation.get("allow_turn_start", True)),
        "allow_remote_offload": bool(
            runtime_validation.get("allow_remote_offload", True)
        ),
        "latency_path_protected": bool(
            runtime_validation.get("latency_path_protected", False)
        ),
        "guardrails": list(runtime_validation.get("guardrails", [])),
        "reasons": list(runtime_validation.get("reasons", [])),
        "signals": dict(runtime_validation.get("signals", {})),
        "runtime_validation": dict(runtime_validation),
    }


def _verdict_event_details(runtime_verdict: dict[str, object]) -> dict[str, object]:
    return {
        "event_type": "RUNTIME_VERDICT",
        "path_type": runtime_verdict.get("path_type", "non_voice"),
        "runtime_verdict": runtime_verdict.get("runtime_verdict", "STABLE"),
        "voice_verdict": runtime_verdict.get("voice_verdict", "NOT_APPLICABLE"),
        "score": int(runtime_verdict.get("score", 0) or 0),
        "reasons": list(runtime_verdict.get("reasons", [])),
        "signals": dict(runtime_verdict.get("signals", {})),
        "runtime_verdict_payload": dict(runtime_verdict),
    }


def materialize_routing_contract(
    *,
    execution_mode: str,
    execution_compute_mode: str,
    runtime_profile: str,
    routing_reason: dict[str, object],
    telemetry_snapshot: dict[str, object],
    safety_override: dict[str, object],
    network_snapshot: dict[str, object],
    network_policy: dict[str, object],
    runtime_decision: dict[str, object],
    runtime_validation: dict[str, object],
) -> dict[str, object]:
    return {
        "execution_mode": execution_mode,
        "execution_compute_mode": execution_compute_mode,
        "execution_location": "remote" if execution_mode == "REMOTE" else "local",
        "runtime_profile": runtime_profile,
        "routing_reason": dict(routing_reason),
        "telemetry_snapshot": dict(telemetry_snapshot),
        "safety_override": dict(safety_override),
        "network_snapshot": dict(network_snapshot),
        "network_policy": dict(network_policy),
        "runtime_decision": dict(runtime_decision),
        "runtime_validation": dict(runtime_validation),
    }


def route_execution(
    task_data: dict[str, object],
    *,
    now: Callable[[], str],
    persist: Callable[[dict[str, object]], None],
    telemetry_collector: Callable[[], dict[str, object]] = collect_runtime_metrics,
    network_snapshot_collector: Callable[[], dict[str, object]] = get_network_snapshot,
) -> dict[str, object]:
    runtime_profile = get_runtime_profile()
    metrics = dict(telemetry_collector())
    network_snapshot = dict(network_snapshot_collector())
    execution_mode, execution_compute_mode, routing_reason, safety_override, network_policy, runtime_decision = (
        decide_policy_execution_mode(
            task_data,
            metrics,
            network_snapshot,
        )
    )
    runtime_decision["execution_compute_mode"] = execution_compute_mode
    runtime_validation = build_runtime_validation(
        task_data=task_data,
        metrics=metrics,
        network_snapshot=network_snapshot,
        runtime_decision=runtime_decision,
    )
    runtime_verdict = build_runtime_verdict(
        runtime_validation=runtime_validation,
        runtime_decision=runtime_decision,
        latency_metrics={},
        session=task_data,
    )

    routing_contract = materialize_routing_contract(
        execution_mode=execution_mode,
        execution_compute_mode=execution_compute_mode,
        runtime_profile=runtime_profile,
        routing_reason=routing_reason,
        telemetry_snapshot=metrics,
        safety_override=safety_override,
        network_snapshot=network_snapshot,
        network_policy=network_policy,
        runtime_decision=runtime_decision,
        runtime_validation=runtime_validation,
    )
    task_data.update(routing_contract)
    task_data["runtime_verdict"] = dict(runtime_verdict)
    task_data["runtime_authority_chain"] = authority_chain_snapshot()

    record_task_event(
        task_data,
        timestamp=now(),
        event="NETWORK_STATE_EVALUATED",
        from_status=task_data.get("status", ""),
        to_status=task_data.get("status", ""),
        details=_network_event_details(network_snapshot),
    )
    if network_snapshot.get("wifi_detected"):
        record_task_event(
            task_data,
            timestamp=now(),
            event="NETWORK_INTERFACE_WARNING",
            from_status=task_data.get("status", ""),
            to_status=task_data.get("status", ""),
            details=_network_event_details(network_snapshot),
        )
    elif network_snapshot.get("interface_type") == "unknown":
        record_task_event(
            task_data,
            timestamp=now(),
            event="NETWORK_INTERFACE_INVALID",
            from_status=task_data.get("status", ""),
            to_status=task_data.get("status", ""),
            details=_network_event_details(network_snapshot),
        )
    if bool(network_snapshot.get("telemetry_uncertain")):
        record_task_event(
            task_data,
            timestamp=now(),
            event="NETWORK_UNCERTAIN",
            from_status=task_data.get("status", ""),
            to_status=task_data.get("status", ""),
            details=_network_event_details(network_snapshot),
        )
    if bool(network_snapshot.get("state_transition")):
        record_task_event(
            task_data,
            timestamp=now(),
            event="NETWORK_STATE_TRANSITION",
            from_status=task_data.get("status", ""),
            to_status=task_data.get("status", ""),
            details=_network_event_details(network_snapshot),
        )
    if bool(network_snapshot.get("stabilized")):
        record_task_event(
            task_data,
            timestamp=now(),
            event="NETWORK_STABILIZED",
            from_status=task_data.get("status", ""),
            to_status=task_data.get("status", ""),
            details=_network_event_details(network_snapshot),
        )
    if network_snapshot.get("quality") == "DEGRADED" or network_snapshot.get("policy_mode") == "DEGRADED":
        record_task_event(
            task_data,
            timestamp=now(),
            event="NETWORK_DEGRADED",
            from_status=task_data.get("status", ""),
            to_status=task_data.get("status", ""),
            details=_network_event_details(network_snapshot),
        )
    if network_snapshot.get("quality") == "BAD":
        record_task_event(
            task_data,
            timestamp=now(),
            event="NETWORK_BAD",
            from_status=task_data.get("status", ""),
            to_status=task_data.get("status", ""),
            details=_network_event_details(network_snapshot),
        )

    record_task_event(
        task_data,
        timestamp=now(),
        event="RUNTIME_DECISION_EVALUATED",
        from_status=task_data.get("status", ""),
        to_status=task_data.get("status", ""),
        details=_runtime_event_details(runtime_decision),
    )
    if bool(runtime_decision.get("changed")):
        record_task_event(
            task_data,
            timestamp=now(),
            event="RUNTIME_DECISION_CHANGED",
            from_status=task_data.get("status", ""),
            to_status=task_data.get("status", ""),
            details=_runtime_event_details(runtime_decision),
        )
    if bool(runtime_decision.get("stabilized")):
        record_task_event(
            task_data,
            timestamp=now(),
            event="RUNTIME_DECISION_STABILIZED",
            from_status=task_data.get("status", ""),
            to_status=task_data.get("status", ""),
            details=_runtime_event_details(runtime_decision),
        )
    if str(runtime_decision.get("confidence", "")).strip().upper() == "LOW_CONFIDENCE":
        record_task_event(
            task_data,
            timestamp=now(),
            event="RUNTIME_CONFIDENCE_LOW",
            from_status=task_data.get("status", ""),
            to_status=task_data.get("status", ""),
            details=_runtime_event_details(runtime_decision),
        )
    if bool(runtime_decision.get("voice_safety_mode")):
        record_task_event(
            task_data,
            timestamp=now(),
            event="VOICE_RUNTIME_PROTECTED",
            from_status=task_data.get("status", ""),
            to_status=task_data.get("status", ""),
            details=_runtime_event_details(runtime_decision),
        )
    for trace_signal in list(runtime_decision.get("trace_signals", [])):
        signal_payload = dict(trace_signal)
        event_name = str(signal_payload.get("event", "")).strip()
        if not event_name:
            continue
        details = _runtime_event_details(runtime_decision)
        details["trace_signal"] = dict(signal_payload.get("details", {}))
        record_task_event(
            task_data,
            timestamp=now(),
            event=event_name,
            from_status=task_data.get("status", ""),
            to_status=task_data.get("status", ""),
            details=details,
        )

    record_task_event(
        task_data,
        timestamp=now(),
        event="RUNTIME_VALIDATION_EVALUATED",
        from_status=task_data.get("status", ""),
        to_status=task_data.get("status", ""),
        details=_validation_event_details(runtime_validation),
    )
    if str(runtime_validation.get("state", "")).strip().upper() == "GUARDED":
        record_task_event(
            task_data,
            timestamp=now(),
            event="RUNTIME_VALIDATION_GUARDED",
            from_status=task_data.get("status", ""),
            to_status=task_data.get("status", ""),
            details=_validation_event_details(runtime_validation),
        )
    if bool(runtime_validation.get("latency_path_protected")):
        record_task_event(
            task_data,
            timestamp=now(),
            event="VOICE_LATENCY_PATH_PROTECTED",
            from_status=task_data.get("status", ""),
            to_status=task_data.get("status", ""),
            details=_validation_event_details(runtime_validation),
        )
        record_task_event(
            task_data,
            timestamp=now(),
            event="VOICE_AUTHORITY_CHAIN_ENFORCED",
            from_status=task_data.get("status", ""),
            to_status=task_data.get("status", ""),
            details={
                "event_type": "AUTHORITY_CHAIN",
                "authority_chain": authority_chain_snapshot(),
                "boundary": voice_guardrail_boundary_payload(
                    runtime_decision=runtime_decision,
                    runtime_validation=runtime_validation,
                ),
            },
        )
    record_task_event(
        task_data,
        timestamp=now(),
        event="RUNTIME_VERDICT_EVALUATED",
        from_status=task_data.get("status", ""),
        to_status=task_data.get("status", ""),
        details=_verdict_event_details(runtime_verdict),
    )
    if str(runtime_verdict.get("voice_verdict", "")).strip() not in {"", "NOT_APPLICABLE"}:
        record_task_event(
            task_data,
            timestamp=now(),
            event="VOICE_RUNTIME_VERDICT_UPDATED",
            from_status=task_data.get("status", ""),
            to_status=task_data.get("status", ""),
            details=_verdict_event_details(runtime_verdict),
        )

    if bool(safety_override.get("triggered")):
        record_task_event(
            task_data,
            timestamp=now(),
            event="safety_override",
            from_status=task_data.get("status", ""),
            to_status=task_data.get("status", ""),
        details=dict(routing_contract),
        )

    record_task_event(
        task_data,
        timestamp=now(),
        event="policy_routed_remote" if execution_mode == "REMOTE" else "policy_routed_local",
        from_status=task_data.get("status", ""),
        to_status=task_data.get("status", ""),
        details=dict(routing_contract),
    )

    record_task_event(
        task_data,
        timestamp=now(),
        event="execution_routed_remote" if execution_mode == "REMOTE" else "execution_routed_local",
        from_status=task_data.get("status", ""),
        to_status=task_data.get("status", ""),
        details=dict(routing_contract),
    )
    persist(task_data)

    return {
        "execution_mode": execution_mode,
        "execution_compute_mode": execution_compute_mode,
        "execution_location": task_data["execution_location"],
        "routing_reason": dict(routing_reason),
        "telemetry_snapshot": dict(metrics),
        "safety_override": dict(safety_override),
        "network_snapshot": dict(network_snapshot),
        "network_policy": dict(network_policy),
        "runtime_decision": dict(runtime_decision),
        "runtime_validation": dict(runtime_validation),
        "routing_contract": dict(routing_contract),
    }

__all__ = ["COMPUTE_MODES", "EXECUTION_MODES", "materialize_routing_contract", "route_execution"]

