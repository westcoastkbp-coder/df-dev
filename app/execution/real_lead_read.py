from __future__ import annotations

from collections.abc import Mapping

from app.execution.lead_estimate_contract import WORKFLOW_TYPE
from app.execution.lead_estimate_read import resolve_estimate_decision
from app.execution.real_lead_contract import (
    MISSING_INPUT_FOLLOWUP_WORKFLOW_TYPE,
    build_followup_payload,
    build_lead_input_payload,
    build_real_lead_run_id,
    build_real_lead_workflow_payload,
    tracked_missing_followup_fields,
)
from app.orchestrator.task_factory import build_idempotency_key


def prepare_real_lead_parent_task_input(
    lead_input: Mapping[str, object],
    *,
    task_id: str | None = None,
) -> dict[str, object]:
    lead_input_payload = build_lead_input_payload(lead_input)
    return {
        "lead_input_payload": lead_input_payload,
        "task_input": {
            "task_id": task_id
            or build_real_lead_run_id(
                str(lead_input_payload.get("lead_id", "")).strip()
            ),
            "status": "created",
            "intent": WORKFLOW_TYPE,
            "payload": build_real_lead_workflow_payload(lead_input_payload),
        },
    }


def prepare_missing_input_followup(
    lead_input: Mapping[str, object],
) -> dict[str, object]:
    lead_input_payload = build_lead_input_payload(lead_input)
    missing_fields = tracked_missing_followup_fields(lead_input_payload)
    return {
        "lead_input_payload": lead_input_payload,
        "missing_fields": missing_fields,
        "followup_payload": build_followup_payload(
            parent_lead_id=lead_input_payload.get("lead_id"),
            missing_fields=missing_fields,
            status="pending",
        ),
        "idempotency_key": build_idempotency_key(
            lead_id=lead_input_payload.get("lead_id"),
            workflow_type=MISSING_INPUT_FOLLOWUP_WORKFLOW_TYPE,
            step_name="followup",
            payload={
                "missing_fields": sorted(missing_fields),
                "required_action": "request_input_completion",
            },
        ),
    }


def prepare_real_lead_execution(
    lead_input: Mapping[str, object],
    *,
    task_id: str,
) -> dict[str, object]:
    lead_input_payload = build_lead_input_payload(lead_input)
    workflow_payload = build_real_lead_workflow_payload(lead_input_payload)
    decision = resolve_estimate_decision(
        task_id=task_id,
        payload=workflow_payload,
    )
    return {
        "lead_input_payload": lead_input_payload,
        "workflow_payload": workflow_payload,
        "decision": decision,
    }
