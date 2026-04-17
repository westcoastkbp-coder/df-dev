from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TypedDict

from app.execution.lead_estimate_contract import WORKFLOW_TYPE, build_action_payload, validate_input_payload
from app.execution.lead_estimate_decision import resolve_estimate_decision
from app.execution.paths import ROOT_DIR, TASKS_FILE
from app.orchestrator.task_factory import build_idempotency_key, find_task_by_idempotency_key, get_task


class ReplayReport(TypedDict):
    run_id: str
    replay_status: str
    mismatched_step: str
    notes: str


TRACE_STEP_NAMES = {
    "input_validated",
    "decision_recorded",
    "decision_evaluated",
    "action_bound",
    "task_created",
    "archived",
    "reporting_generated",
}


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _normalize_sequence(value: object) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    normalized: list[dict[str, object]] = []
    for item in value:
        if isinstance(item, Mapping):
            normalized.append(dict(item))
    return normalized


def _stored_result(task_data: Mapping[str, object]) -> tuple[dict[str, object], dict[str, object]]:
    result = _normalize_mapping(task_data.get("result"))
    result_payload = _normalize_mapping(result.get("result_payload"))
    return (
        _normalize_mapping(result.get("decision") or result_payload.get("decision")),
        _normalize_mapping(result.get("binding") or result_payload.get("binding")),
    )


def _normalize_binding(binding: Mapping[str, object]) -> dict[str, object]:
    return {
        "binding_action": _normalize_text(binding.get("binding_action")),
        "binding_status": _normalize_text(binding.get("binding_status")),
        "child_task_created": bool(binding.get("child_task_created", False)),
        "child_task_intent": _normalize_text(binding.get("child_task_intent")),
        "archive_status": _normalize_text(binding.get("archive_status")),
        "parent_task_id": _normalize_text(binding.get("parent_task_id")),
        "source_lead_id": _normalize_text(binding.get("source_lead_id")),
        "action_source": _normalize_text(binding.get("action_source")),
        "reason_code": _normalize_text(binding.get("reason_code")),
    }


def _normalize_trace_sequence(trace_sequence: object) -> list[dict[str, object]]:
    normalized_steps: list[dict[str, object]] = []
    for step in _normalize_sequence(trace_sequence):
        normalized_steps.append(
            {
                "step_name": _normalize_text(step.get("step_name")),
                "input_payload_type": _normalize_text(step.get("input_payload_type")),
                "output_payload_type": _normalize_text(step.get("output_payload_type")),
                "result_status": _normalize_text(step.get("result_status")),
            }
        )
    return normalized_steps


def _load_trace_for_run(run_id: str, *, log_path: Path | None = None) -> list[dict[str, object]]:
    target = Path(log_path) if log_path is not None else ROOT_DIR / "runtime" / "logs" / "system.log"
    if not target.exists():
        return []
    for line in target.read_text(encoding="utf-8").splitlines():
        if not line.startswith("[TRACE] "):
            continue
        payload = json.loads(line[len("[TRACE] ") :])
        if _normalize_text(payload.get("run_id")) == _normalize_text(run_id):
            return _normalize_trace_sequence(payload.get("step_sequence"))
    return []


def _trace_step(
    *,
    step_name: str,
    input_payload_type: str,
    output_payload_type: str,
    result_status: str,
) -> dict[str, object]:
    if step_name not in TRACE_STEP_NAMES:
        raise ValueError("invalid replay trace step")
    return {
        "step_name": step_name,
        "input_payload_type": input_payload_type,
        "output_payload_type": output_payload_type,
        "result_status": result_status,
    }


def _predict_binding(
    *,
    task_data: Mapping[str, object],
    decision: Mapping[str, object],
    store_path: Path,
) -> tuple[dict[str, object], str]:
    from app.execution.lead_estimate_contract import decision_reason_code

    next_action = _normalize_text(decision.get("next_step"))
    parent_task_id = _normalize_text(task_data.get("task_id"))
    lead_id = _normalize_text(_normalize_mapping(task_data.get("payload")).get("lead_id"))
    reason_code = decision_reason_code(next_action)
    stored_decision, stored_binding = _stored_result(task_data)

    if next_action == "archive_lead":
        return (
            build_action_payload(
                {
                    "binding_action": next_action,
                    "binding_status": "archived",
                    "child_task_created": False,
                    "child_task_id": "",
                    "child_task_intent": "",
                    "archive_status": "archived",
                    "parent_task_id": parent_task_id,
                    "source_lead_id": lead_id,
                    "action_source": WORKFLOW_TYPE,
                    "reason_code": reason_code,
                }
            ),
            "",
        )

    child_intent = {
        "create_estimate_task": "estimate_task",
        "request_missing_scope": "missing_scope_followup",
        "manual_review": "manual_review_task",
    }.get(next_action, "")
    idempotency_key = build_idempotency_key(
        lead_id=lead_id,
        workflow_type=_normalize_text(decision.get("workflow_type")) or WORKFLOW_TYPE,
        step_name=next_action,
        payload={
            "decision": _normalize_text(decision.get("decision")),
            "next_step": next_action,
        },
    )
    referenced_child = get_task(_normalize_text(stored_binding.get("child_task_id")), store_path=store_path)
    matched_child = referenced_child or find_task_by_idempotency_key(idempotency_key, store_path=store_path)
    child_task_id = _normalize_text((matched_child or {}).get("task_id")) or _normalize_text(
        stored_binding.get("child_task_id")
    )
    child_task_intent = _normalize_text((matched_child or {}).get("intent")) or child_intent
    if child_task_intent != child_intent and _normalize_text(stored_decision.get("next_step")) == next_action:
        child_task_intent = child_intent
    return (
        build_action_payload(
            {
                "binding_action": next_action,
                "binding_status": "child_task_created",
                "child_task_created": True,
                "child_task_id": child_task_id,
                "child_task_intent": child_task_intent,
                "archive_status": "",
                "parent_task_id": parent_task_id,
                "source_lead_id": lead_id,
                "action_source": WORKFLOW_TYPE,
                "reason_code": reason_code,
            }
        ),
        idempotency_key,
    )


def _replayed_trace(
    *,
    task_data: Mapping[str, object],
    decision: Mapping[str, object] | None,
    binding: Mapping[str, object] | None,
    input_valid: bool,
) -> list[dict[str, object]]:
    if not input_valid:
        return [
            _trace_step(
                step_name="input_validated",
                input_payload_type="workflow_payload",
                output_payload_type="workflow_policy",
                result_status="fail",
            )
        ]
    if decision is None or binding is None:
        return []
    return [
        _trace_step(
            step_name="input_validated",
            input_payload_type="workflow_payload",
            output_payload_type="workflow_policy",
            result_status="success",
        ),
        _trace_step(
            step_name="decision_recorded",
            input_payload_type="task_context",
            output_payload_type="action_plan",
            result_status="success",
        ),
        _trace_step(
            step_name="decision_evaluated",
            input_payload_type="decision_payload",
            output_payload_type="workflow_contract",
            result_status="success",
        ),
        _trace_step(
            step_name="action_bound",
            input_payload_type="decision_payload",
            output_payload_type="action_payload",
            result_status="success",
        ),
        _trace_step(
            step_name=(
                "archived"
                if _normalize_text(binding.get("archive_status")) == "archived"
                else "task_created"
            ),
            input_payload_type="action_payload",
            output_payload_type="action_payload",
            result_status="success",
        ),
        _trace_step(
            step_name="reporting_generated",
            input_payload_type="reporting_payload",
            output_payload_type="reporting_payload",
            result_status="success",
        ),
    ]


def _first_mismatch(
    stored_trace_sequence: Sequence[Mapping[str, object]],
    replay_trace_sequence: Sequence[Mapping[str, object]],
) -> str:
    for index, (stored_step, replay_step) in enumerate(zip(stored_trace_sequence, replay_trace_sequence), start=1):
        if dict(stored_step) != dict(replay_step):
            return f"trace_step_{index}"
    if len(stored_trace_sequence) != len(replay_trace_sequence):
        return "trace_length"
    return ""


def replay_execution(
    run_id: object,
    *,
    stored_trace_sequence: Sequence[Mapping[str, object]] | None = None,
    store_path: Path | None = None,
    log_path: Path | None = None,
) -> ReplayReport:
    normalized_run_id = _normalize_text(run_id)
    if not normalized_run_id:
        return {
            "run_id": "",
            "replay_status": "mismatch",
            "mismatched_step": "run_id",
            "notes": "missing run_id",
        }

    target_store = Path(store_path) if store_path is not None else TASKS_FILE
    try:
        task_data = get_task(normalized_run_id, store_path=target_store)
    except Exception:
        return {
            "run_id": normalized_run_id,
            "replay_status": "mismatch",
            "mismatched_step": "task_lookup",
            "notes": "run lookup unavailable",
        }
    if task_data is None:
        return {
            "run_id": normalized_run_id,
            "replay_status": "mismatch",
            "mismatched_step": "task_lookup",
            "notes": "run not found",
        }

    payload = _normalize_mapping(task_data.get("payload"))
    input_valid, _, _ = validate_input_payload(payload)
    replay_decision: dict[str, object] | None = None
    replay_binding: dict[str, object] | None = None
    replay_idempotency_key = ""
    if input_valid:
        replay_decision = resolve_estimate_decision(task_id=normalized_run_id, payload=payload)
        replay_binding, replay_idempotency_key = _predict_binding(
            task_data=task_data,
            decision=replay_decision,
            store_path=target_store,
        )

    stored_decision, stored_binding = _stored_result(task_data)
    normalized_stored_trace = _normalize_trace_sequence(
        stored_trace_sequence if stored_trace_sequence is not None else _load_trace_for_run(normalized_run_id, log_path=log_path)
    )
    normalized_replay_trace = _replayed_trace(
        task_data=task_data,
        decision=replay_decision,
        binding=replay_binding,
        input_valid=input_valid,
    )

    if input_valid and replay_decision != stored_decision:
        return {
            "run_id": normalized_run_id,
            "replay_status": "mismatch",
            "mismatched_step": "decision_evaluated",
            "notes": "decision output diverged",
        }

    if input_valid and _normalize_binding(replay_binding or {}) != _normalize_binding(stored_binding):
        return {
            "run_id": normalized_run_id,
            "replay_status": "mismatch",
            "mismatched_step": "action_bound",
            "notes": "action output diverged",
        }

    if replay_idempotency_key:
        try:
            stored_child = get_task(_normalize_text(stored_binding.get("child_task_id")), store_path=target_store)
        except Exception:
            stored_child = None
        stored_child_key = _normalize_text((stored_child or {}).get("idempotency_key"))
        if stored_child_key and stored_child_key != replay_idempotency_key:
            return {
                "run_id": normalized_run_id,
                "replay_status": "mismatch",
                "mismatched_step": "idempotency",
                "notes": "idempotency behavior diverged",
            }

    mismatched_step = _first_mismatch(normalized_stored_trace, normalized_replay_trace)
    if mismatched_step:
        return {
            "run_id": normalized_run_id,
            "replay_status": "mismatch",
            "mismatched_step": mismatched_step,
            "notes": "trace sequence diverged",
        }

    return {
        "run_id": normalized_run_id,
        "replay_status": "match",
        "mismatched_step": "",
        "notes": "dry-run replay matched stored execution",
    }
