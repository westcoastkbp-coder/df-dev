from __future__ import annotations

import hashlib
import time
from collections.abc import Mapping
from pathlib import Path

from app.execution.action_result import build_action_result
from app.execution.decision_trace import build_decision_trace
from app.execution.execution_boundary import execution_boundary
from app.execution.vendor_router import route as route_vendor
from app.orchestrator import task_store
from integrations.gmail_gateway import send_email
from integrations.google_sheets_gateway import append_row

DEFAULT_LEAD_NOTIFICATION_EMAIL = "sales@digitalforeman.test"
CRM_SPREADSHEET_NAME = "Digital Foreman CRM"
CRM_WORKSHEET_NAME = "Incoming Leads"
EMAIL_DECISION_KEYWORDS = (
    "asap",
    "estimate",
    "follow up",
    "follow-up",
    "pricing",
    "quote",
    "urgent",
)


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _timestamp() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _lead_id(name: str, phone: str, request: str) -> str:
    identity = f"{name}|{phone}|{request}".encode("utf-8")
    return "lead-" + hashlib.sha256(identity).hexdigest()[:12]


def _required_text(payload: Mapping[str, object], field_name: str) -> str:
    value = _normalize_text(payload.get(field_name))
    if not value:
        raise ValueError(f"{field_name} is required")
    return value


def simulate_lead_intake(raw_input: Mapping[str, object]) -> dict[str, str]:
    if not isinstance(raw_input, Mapping):
        raise ValueError("lead input must be a mapping")

    name = _required_text(raw_input, "name")
    phone = _required_text(raw_input, "phone")
    request = _required_text(raw_input, "request")

    return {
        "lead_id": _lead_id(name, phone, request),
        "name": name,
        "phone": phone,
        "request": request,
        "received_at": _timestamp(),
        "source": "intake_simulation",
    }


def decide_next_step(lead_data: Mapping[str, object]) -> dict[str, object]:
    request = _normalize_text(lead_data.get("request")).lower()
    name = _normalize_text(lead_data.get("name")) or "lead"
    lead_id = _normalize_text(lead_data.get("lead_id"))
    phone = _normalize_text(lead_data.get("phone"))

    if any(keyword in request for keyword in EMAIL_DECISION_KEYWORDS):
        return {
            "next_step": "send_email",
            "adapter": "gmail",
            "action_type": "SEND_EMAIL",
            "reason": "high-intent lead should trigger immediate office email follow-up",
            "payload": {
                "to": DEFAULT_LEAD_NOTIFICATION_EMAIL,
                "subject": f"New lead requires follow-up: {name}",
                "body": (
                    f"Lead ID: {lead_id}\n"
                    f"Name: {name}\n"
                    f"Phone: {phone}\n"
                    f"Request: {_normalize_text(lead_data.get('request'))}"
                ),
            },
        }

    return {
        "next_step": "create_crm_entry",
        "adapter": "google.sheets",
        "action_type": "CREATE_CRM_ENTRY",
        "reason": "lead should be captured in CRM before manual follow-up",
        "payload": {
            "spreadsheet": CRM_SPREADSHEET_NAME,
            "worksheet": CRM_WORKSHEET_NAME,
            "row": {
                "lead_id": lead_id,
                "name": name,
                "phone": phone,
                "request": _normalize_text(lead_data.get("request")),
                "received_at": _normalize_text(lead_data.get("received_at")),
            },
        },
    }


def _decision_trace(
    task_id: str, lead_data: Mapping[str, object], decision: Mapping[str, object]
) -> dict[str, str]:
    vendor = route_vendor(
        {"task_id": task_id, "payload": dict(lead_data)},
        {},
        {
            "action_type": _normalize_text(decision.get("action_type")),
            "target": _normalize_text(decision.get("adapter")),
            "parameters": dict(decision.get("payload", {}) or {}),
        },
    )
    return build_decision_trace(
        reason=_normalize_text(decision.get("reason")),
        context_used=(
            f"lead={_normalize_text(lead_data.get('lead_id'))}; "
            f"phone={_normalize_text(lead_data.get('phone'))}; "
            f"adapter={_normalize_text(decision.get('adapter'))}"
        ),
        action_type=_normalize_text(decision.get("action_type")),
        policy_result=f"allowed: routed to {_normalize_text(decision.get('adapter'))}",
        confidence="high",
        vendor=vendor,
    )


def _execute_action(
    *,
    task_id: str,
    lead_data: Mapping[str, object],
    decision: Mapping[str, object],
    decision_trace: Mapping[str, object],
) -> dict[str, object]:
    with execution_boundary(
        {"task_id": task_id, "intent": "lead_processing"},
        policy_validated=True,
    ):
        if _normalize_text(decision.get("adapter")) == "gmail":
            provider_result = send_email(**dict(decision.get("payload", {}) or {}))
        else:
            payload = dict(decision.get("payload", {}) or {})
            provider_result = append_row(
                spreadsheet=payload.get("spreadsheet"),
                worksheet=payload.get("worksheet"),
                row=payload.get("row"),
            )

        success = bool(provider_result.get("ok"))
        return build_action_result(
            status="completed" if success else "failed",
            task_id=task_id,
            action_type=_normalize_text(decision.get("action_type")),
            result_payload={
                "adapter": _normalize_text(decision.get("adapter")),
                "lead_id": _normalize_text(lead_data.get("lead_id")),
                "next_step": _normalize_text(decision.get("next_step")),
                "provider_result": dict(provider_result),
            },
            error_code="" if success else "lead_processing_action_failed",
            error_message=""
            if success
            else _normalize_text(provider_result.get("error")),
            source="app.execution.lead_processing_flow",
            decision_trace=dict(decision_trace),
        )


def _task_payload(lead_data: Mapping[str, object]) -> dict[str, object]:
    return {
        "lead_data": dict(lead_data),
        "summary": _normalize_text(lead_data.get("request")),
        "workflow": "lead_processing",
    }


def _merge_result(
    *,
    lead_data: Mapping[str, object],
    decision: Mapping[str, object],
    action_result: Mapping[str, object] | None = None,
    decision_trace: Mapping[str, object] | None = None,
) -> dict[str, object]:
    result = {
        "lead_data": dict(lead_data),
        "decision": dict(decision),
    }
    if isinstance(decision_trace, Mapping):
        result["decision_trace"] = dict(decision_trace)
    if isinstance(action_result, Mapping):
        result["action"] = dict(action_result)
    return result


def run_lead_processing_flow(
    raw_input: Mapping[str, object],
    *,
    store_path: Path | None = None,
) -> dict[str, object]:
    lead_data = simulate_lead_intake(raw_input)
    created_task = task_store.create_task(
        task_type="lead_processing",
        task_input=_task_payload(lead_data),
        store_path=store_path,
    )
    task_id = _normalize_text(created_task.get("task_id"))

    task_store.update_task(
        task_id,
        status="VALIDATED",
        task_input=_task_payload(lead_data),
        result={"lead_data": dict(lead_data)},
        history_event="lead_intake_recorded",
        store_path=store_path,
    )

    decision = decide_next_step(lead_data)
    trace = _decision_trace(task_id, lead_data, decision)
    task_store.update_task(
        task_id,
        status="EXECUTING",
        result=_merge_result(
            lead_data=lead_data,
            decision=decision,
            decision_trace=trace,
        ),
        history_event="lead_decision_recorded",
        decision_trace=trace,
        store_path=store_path,
    )

    action_result = _execute_action(
        task_id=task_id,
        lead_data=lead_data,
        decision=decision,
        decision_trace=trace,
    )
    final_result = _merge_result(
        lead_data=lead_data,
        decision=decision,
        action_result=action_result,
        decision_trace=trace,
    )

    task_store.update_task(
        task_id,
        result=final_result,
        history_event="lead_action_executed",
        decision_trace=action_result.get("decision_trace"),
        store_path=store_path,
    )

    if _normalize_text(action_result.get("status")) in {"completed", "success"}:
        task = task_store.complete_task(
            task_id,
            result=final_result,
            decision_trace=action_result.get("decision_trace"),
            store_path=store_path,
        )
    else:
        task = task_store.fail_task(
            task_id,
            error=_normalize_text(action_result.get("error_message"))
            or "lead processing failed",
            result=final_result,
            decision_trace=action_result.get("decision_trace"),
            store_path=store_path,
        )

    return {
        "lead_data": lead_data,
        "task": task,
        "decision": dict(decision),
        "action_result": dict(action_result),
    }
