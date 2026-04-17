from __future__ import annotations

from pathlib import Path

from app.execution.lead_estimate_contract import WORKFLOW_TYPE
from app.execution.paths import TASKS_FILE
from app.execution.real_lead_contract import (
    MISSING_INPUT_FOLLOWUP_WORKFLOW_TYPE,
    build_followup_context_payload,
    build_reentry_task_metadata,
    extract_followup_lead_input,
    followup_payload_from_task,
    normalize_mapping,
    validate_real_lead_input,
)
from app.execution.real_lead_tasks import execute_real_lead_pipeline
from app.orchestrator.task_factory import (
    build_idempotency_key,
    find_task_by_idempotency_key,
    load_tasks,
    next_task_id,
    save_task,
)
from runtime.system_log import (
    append_execution_trace_step,
    build_execution_trace,
    log_event,
    log_execution_trace,
    log_task_reentry,
)


def _record_trace(
    trace: dict[str, object] | None,
    *,
    step_name: str,
    input_payload_type: str,
    output_payload_type: str,
    result_status: str,
) -> None:
    if trace is None:
        return
    updated_trace = append_execution_trace_step(
        trace,
        step_name=step_name,
        input_payload_type=input_payload_type,
        output_payload_type=output_payload_type,
        result_status=result_status,
    )
    trace.clear()
    trace.update(updated_trace)


def _log_idempotent_skip(*, idempotency_key: str, task_id: str) -> None:
    log_event(
        "system",
        f"idempotent_skip idempotency_key={idempotency_key} task_id={task_id}",
    )


def _existing_reentry_task(
    followup_task_id: str,
    *,
    idempotency_key: str,
    store_path: Path,
) -> dict[str, object] | None:
    existing_by_key = find_task_by_idempotency_key(idempotency_key, store_path=store_path)
    if existing_by_key is not None:
        return existing_by_key
    normalized_followup_task_id = str(followup_task_id or "").strip()
    if not normalized_followup_task_id:
        return None
    for task in load_tasks(store_path):
        payload = normalize_mapping(task.get("payload"))
        task_idempotency_key = str(task.get("idempotency_key", "")).strip()
        if not task_idempotency_key and str(task.get("followup_task_id", "")).strip() == normalized_followup_task_id:
            return dict(task)
        if (
            not task_idempotency_key
            and
            str(payload.get("workflow_type", "")).strip() == WORKFLOW_TYPE
            and str(payload.get("reentry_source_followup_task_id", "")).strip()
            == normalized_followup_task_id
        ):
            return dict(task)
    return None


def reenter_completed_followup(
    task_data: dict[str, object],
    *,
    store_path: Path | None = None,
    trace: dict[str, object] | None = None,
) -> dict[str, object]:
    owns_trace = trace is None
    followup_context = build_followup_context_payload(task_data)
    if trace is None:
        trace = build_execution_trace(
            run_id=str(followup_context.get("task_id", "")).strip(),
            lead_id=str(followup_context.get("payload", {}).get("parent_lead_id", "")).strip(),
        )
    payload = followup_payload_from_task(followup_context)
    normalized_store = Path(store_path) if store_path is not None else TASKS_FILE
    followup_task_id = str(followup_context.get("task_id", "")).strip()
    followup_status = str(followup_context.get("status", "")).strip().lower()
    task_intent = str(followup_context.get("intent") or "").strip()
    payload_workflow_type = str(payload.get("workflow_type") or "").strip()
    if (
        followup_status != "completed"
        or (
            task_intent != MISSING_INPUT_FOLLOWUP_WORKFLOW_TYPE
            and payload_workflow_type != MISSING_INPUT_FOLLOWUP_WORKFLOW_TYPE
        )
    ):
        return {
            "status": "ignored",
            "reason": "not_completed_missing_input_followup",
            "reentry_task_id": "",
            "lead_id": str(payload.get("parent_lead_id") or "").strip(),
        }

    normalized_input = extract_followup_lead_input(followup_context)
    idempotency_key = build_idempotency_key(
        lead_id=normalized_input.get("lead_id"),
        workflow_type=WORKFLOW_TYPE,
        step_name="reentry",
        payload={
            "followup_task_id": followup_task_id,
            "updated_lead_input": normalized_input,
        },
    )
    existing_task = _existing_reentry_task(
        followup_task_id,
        idempotency_key=idempotency_key,
        store_path=normalized_store,
    )
    if existing_task is not None:
        _log_idempotent_skip(
            idempotency_key=idempotency_key,
            task_id=str(existing_task.get("task_id", "")).strip(),
        )
        _record_trace(
            trace,
            step_name="reentry_triggered",
            input_payload_type="followup_payload",
            output_payload_type="reentry_result",
            result_status="success",
        )
        existing_lead_id = str(
            existing_task.get("source_lead_id")
            or normalize_mapping(existing_task.get("payload")).get("lead_id")
            or payload.get("parent_lead_id")
            or ""
        ).strip()
        log_task_reentry(
            followup_task_id=followup_task_id,
            reentry_task_id=str(existing_task.get("task_id", "")).strip(),
            lead_id=existing_lead_id,
            status="already_exists",
        )
        log_event(
            "system",
            (
                f"follow-up re-entry skipped: followup_task={followup_task_id} "
                f"reentry_task={str(existing_task.get('task_id', '')).strip()} "
                f"reason=already_exists"
            ),
        )
        result = {
            "status": "already_exists",
            "reason": "followup_already_reentered",
            "reentry_task_id": str(existing_task.get("task_id", "")).strip(),
            "lead_id": existing_lead_id,
        }
        if owns_trace:
            log_execution_trace(trace)
        return result

    input_valid, input_reason = validate_real_lead_input(normalized_input)
    normalized_lead_id = str(normalized_input.get("lead_id") or "").strip()
    if not input_valid:
        _record_trace(
            trace,
            step_name="reentry_triggered",
            input_payload_type="followup_payload",
            output_payload_type="reentry_result",
            result_status="fail",
        )
        log_task_reentry(
            followup_task_id=followup_task_id,
            reentry_task_id="",
            lead_id=normalized_lead_id,
            status="missing_required_data",
        )
        log_event(
            "system",
            (
                f"follow-up re-entry skipped: followup_task={followup_task_id} "
                f"lead_id={normalized_lead_id} reason={input_reason}"
            ),
        )
        result = {
            "status": "missing_required_data",
            "reason": input_reason,
            "reentry_task_id": "",
            "lead_id": normalized_lead_id,
        }
        if owns_trace:
            log_execution_trace(trace)
        return result

    reentry_result = execute_real_lead_pipeline(
        normalized_input,
        store_path=normalized_store,
        task_id=next_task_id(),
        task_metadata=build_reentry_task_metadata(
            source_lead_id=normalized_lead_id,
            followup_task_id=followup_task_id,
            original_task_id=str(payload.get("parent_task_id") or payload.get("original_task_id") or "").strip(),
            reentry_source=MISSING_INPUT_FOLLOWUP_WORKFLOW_TYPE,
        ),
        trace=trace,
    )
    reentry_task = dict(reentry_result.get("task", {}) or {})
    reentry_task["idempotency_key"] = idempotency_key
    reentry_task = save_task(reentry_task, store_path=normalized_store)
    reentry_result["task"] = dict(reentry_task)
    reentry_task_id = str(reentry_task.get("task_id", "")).strip()
    decision = dict(reentry_result.get("decision", {}) or {})
    binding = dict(reentry_result.get("binding", {}) or {})
    _record_trace(
        trace,
        step_name="reentry_triggered",
        input_payload_type="followup_payload",
        output_payload_type="reentry_result",
        result_status="success",
    )
    log_task_reentry(
        followup_task_id=followup_task_id,
        reentry_task_id=reentry_task_id,
        lead_id=normalized_lead_id,
        status="created",
    )
    log_event(
        "system",
        (
            f"follow-up re-entry created: followup_task={followup_task_id} "
            f"reentry_task={reentry_task_id} lead_id={normalized_lead_id} "
            f"next_action={str(decision.get('next_action', '')).strip()} "
            f"child_task_id={str(binding.get('child_task_id', '')).strip()}"
        ),
    )
    result = {
        "status": "created",
        "reason": "reentry_created",
        "reentry_task_id": reentry_task_id,
        "lead_id": normalized_lead_id,
        "decision": decision,
        "binding": binding,
    }
    if owns_trace:
        log_execution_trace(trace)
    return result


def run_completion_followup(
    task_data: dict[str, object],
    *,
    store_path: Path | None = None,
    trace: dict[str, object] | None = None,
) -> dict[str, object]:
    followup_context = build_followup_context_payload(task_data)
    payload = followup_payload_from_task(followup_context)
    if (
        str(followup_context.get("intent", "")).strip() == MISSING_INPUT_FOLLOWUP_WORKFLOW_TYPE
        or str(payload.get("workflow_type", "")).strip() == MISSING_INPUT_FOLLOWUP_WORKFLOW_TYPE
    ):
        _record_trace(
            trace,
            step_name="followup_triggered",
            input_payload_type="followup_context_payload",
            output_payload_type="followup_payload",
            result_status="success",
        )
    return reenter_completed_followup(followup_context, store_path=store_path, trace=trace)
