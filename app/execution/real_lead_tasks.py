from __future__ import annotations

from pathlib import Path

from app.execution.lead_estimate_contract import WORKFLOW_TYPE
from app.execution.lead_estimate_write import bind_decision_action
from app.execution.real_lead_read import (
    prepare_missing_input_followup,
    prepare_real_lead_execution,
    prepare_real_lead_parent_task_input,
)
from app.execution.real_lead_contract import (
    MISSING_INPUT_FOLLOWUP_WORKFLOW_TYPE,
    build_reentry_task_metadata,
)
from app.orchestrator.task_factory import (
    build_idempotency_key,
    create_task,
    find_task_by_idempotency_key,
    get_task,
    save_task,
)
from app.orchestrator.task_lifecycle import set_task_state, transition_task_status
from app.policy.policy_gate import evaluate_workflow_contract, evaluate_workflow_policy
from runtime.system_log import (
    append_execution_trace_step,
    log_event,
    log_task_execution,
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


def _child_binding_result(
    *,
    next_action: str,
    reason_code: str,
    parent_task_id: str,
    source_lead_id: str,
    child_task: dict[str, object],
) -> dict[str, object]:
    return {
        "binding_action": next_action,
        "binding_status": "child_task_created",
        "child_task_created": True,
        "child_task_id": str(child_task.get("task_id", "")).strip(),
        "child_task_intent": str(child_task.get("intent", "")).strip(),
        "archive_status": "",
        "parent_task_id": parent_task_id,
        "source_lead_id": source_lead_id,
        "action_source": WORKFLOW_TYPE,
        "reason_code": reason_code,
    }


def create_real_lead_parent_task(
    lead_input: dict[str, object],
    *,
    store_path: Path,
    task_id: str | None = None,
) -> dict[str, object]:
    prepared = prepare_real_lead_parent_task_input(lead_input, task_id=task_id)
    lead_input_payload = dict(prepared.get("lead_input_payload", {}) or {})
    requested_task_id = str(
        dict(prepared.get("task_input", {}) or {}).get("task_id", "")
    ).strip()
    existing_parent = (
        get_task(requested_task_id, store_path=store_path)
        if requested_task_id
        else None
    )
    if existing_parent is not None:
        if str(existing_parent.get("status", "")).strip() == "CREATED":
            set_task_state(
                existing_parent,
                "VALIDATED",
                timestamp="2026-04-04T00:00:00Z",
                details="real lead parent validated",
            )
            return save_task(existing_parent, store_path=store_path)
        return existing_parent
    parent_task = create_task(
        dict(prepared.get("task_input", {}) or {}),
        store_path=store_path,
    )
    parent_task["intent"] = WORKFLOW_TYPE
    set_task_state(
        parent_task,
        "VALIDATED",
        timestamp="2026-04-04T00:00:00Z",
        details="real lead parent validated",
    )
    return save_task(parent_task, store_path=store_path)


def create_missing_input_followup_task(
    lead_input: dict[str, object],
    *,
    store_path: Path,
) -> dict[str, object]:
    prepared = prepare_missing_input_followup(lead_input)
    lead_input_payload = dict(prepared.get("lead_input_payload", {}) or {})
    missing_fields = list(prepared.get("missing_fields", []) or [])
    idempotency_key = str(prepared.get("idempotency_key", "")).strip()
    existing_followup = find_task_by_idempotency_key(
        idempotency_key, store_path=store_path
    )
    if existing_followup is not None:
        _log_idempotent_skip(
            idempotency_key=idempotency_key,
            task_id=str(existing_followup.get("task_id", "")).strip(),
        )
        return existing_followup
    followup_payload = dict(prepared.get("followup_payload", {}) or {})
    followup_task = create_task(
        {
            "status": "created",
            "intent": MISSING_INPUT_FOLLOWUP_WORKFLOW_TYPE,
            "payload": followup_payload,
        },
        store_path=store_path,
    )
    followup_task["intent"] = MISSING_INPUT_FOLLOWUP_WORKFLOW_TYPE
    followup_task["payload"] = dict(followup_payload)
    followup_task["idempotency_key"] = idempotency_key
    set_task_state(
        followup_task,
        "VALIDATED",
        timestamp="2026-04-04T00:00:00Z",
        details="missing input follow-up validated",
    )
    persisted_followup = save_task(followup_task, store_path=store_path)
    log_task_execution(
        task_id=str(persisted_followup.get("task_id", "")).strip(),
        status="VALIDATED",
        result_type=MISSING_INPUT_FOLLOWUP_WORKFLOW_TYPE,
    )
    log_event(
        "system",
        (
            f"missing input follow-up created: "
            f"task={str(persisted_followup.get('task_id', '')).strip()} "
            f"lead_id={str(lead_input_payload.get('lead_id') or '').strip()} "
            f"missing_fields={','.join(missing_fields)}"
        ),
    )
    return persisted_followup


def execute_real_lead_pipeline(
    lead_input: dict[str, object],
    *,
    store_path: Path,
    task_metadata: dict[str, object] | None = None,
    task_id: str | None = None,
    trace: dict[str, object] | None = None,
) -> dict[str, object]:
    parent_task = create_real_lead_parent_task(
        lead_input,
        store_path=store_path,
        task_id=task_id,
    )
    existing_result = dict(parent_task.get("result", {}) or {})
    if str(parent_task.get("status", "")).strip() == "COMPLETED":
        decision = dict(existing_result.get("decision", {}) or {})
        binding = dict(existing_result.get("binding", {}) or {})
        if decision or binding:
            return {
                "task": parent_task,
                "decision": decision,
                "binding": binding,
            }
    prepared = prepare_real_lead_execution(
        lead_input,
        task_id=str(parent_task.get("task_id", "")).strip(),
    )
    lead_input_payload = dict(prepared.get("lead_input_payload", {}) or {})
    workflow_payload = dict(prepared.get("workflow_payload", {}) or {})
    if task_metadata:
        parent_task.update(
            build_reentry_task_metadata(
                source_lead_id=task_metadata.get("source_lead_id"),
                followup_task_id=task_metadata.get("followup_task_id"),
                original_task_id=task_metadata.get("original_task_id"),
                reentry_source=task_metadata.get("reentry_source"),
            )
        )
        save_task(parent_task, store_path=store_path)

    workflow_policy = evaluate_workflow_policy(
        workflow_payload,
        {
            "task_id": str(parent_task.get("task_id", "")).strip(),
            "status": "pending",
        },
    )
    if not workflow_policy.execution_allowed:
        raise ValueError(f"policy_blocked:{workflow_policy.reason}")

    transition_task_status(
        parent_task,
        "EXECUTING",
        timestamp="2026-04-04T00:00:00Z",
        details="real lead runner started",
    )
    save_task(parent_task, store_path=store_path)
    decision = dict(prepared.get("decision", {}) or {})
    _record_trace(
        trace,
        step_name="decision_evaluated",
        input_payload_type="lead_input_payload",
        output_payload_type="decision_payload",
        result_status="success",
    )
    workflow_contract = evaluate_workflow_contract(
        decision,
        {
            "task_id": str(parent_task.get("task_id", "")).strip(),
            "status": "running",
        },
    )
    if not workflow_contract.execution_allowed:
        raise ValueError(f"contract_blocked:{workflow_contract.reason}")

    from app.execution.lead_estimate_contract import decision_reason_code

    next_action = str(decision.get("next_step", "")).strip()
    if next_action in {
        "create_estimate_task",
        "request_missing_scope",
        "manual_review",
    }:
        idempotency_key = build_idempotency_key(
            lead_id=lead_input_payload.get("lead_id"),
            workflow_type=WORKFLOW_TYPE,
            step_name=next_action,
            payload={
                "decision": str(decision.get("decision", "")).strip(),
                "next_step": next_action,
            },
        )
        existing_child = find_task_by_idempotency_key(
            idempotency_key, store_path=store_path
        )
        if existing_child is not None:
            _log_idempotent_skip(
                idempotency_key=idempotency_key,
                task_id=str(existing_child.get("task_id", "")).strip(),
            )
            binding = _child_binding_result(
                next_action=next_action,
                reason_code=decision_reason_code(next_action),
                parent_task_id=str(parent_task.get("task_id", "")).strip(),
                source_lead_id=str(lead_input_payload.get("lead_id", "")).strip(),
                child_task=existing_child,
            )
        else:
            binding = bind_decision_action(
                task_data=parent_task,
                decision=decision,
                store_path=store_path,
            )
            child_task_id = str(binding.get("child_task_id", "")).strip()
            if child_task_id:
                created_child = dict(
                    find_task_by_idempotency_key(idempotency_key, store_path=store_path)
                    or {}
                )
                if not created_child:
                    resolved_child = get_task(child_task_id, store_path=store_path)
                    if resolved_child is not None:
                        resolved_child["idempotency_key"] = idempotency_key
                        save_task(resolved_child, store_path=store_path)
    else:
        binding = bind_decision_action(
            task_data=parent_task,
            decision=decision,
            store_path=store_path,
        )
    _record_trace(
        trace,
        step_name="action_bound",
        input_payload_type="decision_payload",
        output_payload_type="action_payload",
        result_status="success",
    )
    _record_trace(
        trace,
        step_name=(
            "archived"
            if str(binding.get("archive_status", "")).strip() == "archived"
            else "task_created"
        ),
        input_payload_type="action_payload",
        output_payload_type="action_payload",
        result_status="success",
    )
    parent_task["result"] = {
        "decision": dict(decision),
        "binding": dict(binding),
    }
    transition_task_status(
        parent_task,
        "COMPLETED",
        timestamp="2026-04-04T00:00:00Z",
        details="real lead runner completed",
    )
    persisted_parent = save_task(parent_task, store_path=store_path)
    return {
        "task": persisted_parent,
        "decision": dict(decision),
        "binding": dict(binding),
    }
