from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

from app.execution.action_result import build_action_result
from app.execution.execution_boundary import require_execution_boundary
from app.execution.lead_estimate_contract import (
    WORKFLOW_TYPE as LEAD_ESTIMATE_WORKFLOW_TYPE,
    build_decision_summary,
)
from app.execution.lead_estimate_read import resolve_estimate_decision
from app.execution.paths import ROOT_DIR
from app.product.runner import dispatch_action_trigger


def _extract_project_type(request: str) -> str:
    normalized = str(request or "").strip()
    if re.search(r"\badu\b", normalized, flags=re.IGNORECASE):
        return "ADU"
    return "unknown"


def _extract_lot_size(request: str) -> str:
    match = re.search(
        r"\blot\s+(\d[\d,]*)\s*(sq\s*ft|sqft)\b",
        str(request or ""),
        flags=re.IGNORECASE,
    )
    if not match:
        return "unknown"
    return f"{match.group(1).replace(',', '')} sqft"


def _lead_output_content(*, request: str, project_type: str, lot_size: str) -> str:
    timestamp = (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )
    return "\n".join(
        [
            f"request: {request}",
            f"type: {project_type}",
            f"lot_size: {lot_size}",
            f"timestamp: {timestamp}",
        ]
    ) + "\n"


def _followup_output_content(
    *,
    summary: str,
    next_step: str,
    short_message: str,
) -> str:
    return "\n".join(
        [
            f"summary: {summary}",
            f"recommended_next_action: {next_step}",
            f"short_message: {short_message}",
        ]
    ) + "\n"


def _load_existing_lead() -> dict[str, str]:
    lead_path = ROOT_DIR / "runtime" / "out" / "leads" / "lead_001.txt"
    if not lead_path.exists():
        raise ValueError("existing lead file not found: runtime/out/leads/lead_001.txt")

    parsed: dict[str, str] = {}
    for line in lead_path.read_text(encoding="utf-8").splitlines():
        key, separator, value = line.partition(":")
        if not separator:
            continue
        parsed[str(key).strip().lower()] = str(value).strip()
    return parsed


def execute_mock_task(task_data: dict[str, object]) -> dict[str, object]:
    require_execution_boundary(
        component="mock_executor.execute_mock_task",
        task_id=task_data.get("task_id", ""),
        reason="direct_action_call_blocked",
    )
    intent = str(task_data.get("intent", "")).strip().lower() or "generic_task"
    payload = dict(task_data.get("payload", {}))
    task_id = str(task_data.get("task_id", "")).strip()
    action_type = intent.upper() or "GENERIC_TASK"

    if intent == "service_request":
        service_type = str(payload.get("service_type", "")).strip() or "general_service"
        requested_time = str(payload.get("requested_time", "")).strip() or "unspecified"
        location = str(payload.get("location", "")).strip() or "unspecified"
        return build_action_result(
            status="completed",
            task_id=task_id,
            action_type=action_type,
            result_payload={
                "service_type": service_type,
                "requested_time": requested_time,
                "location": location,
            },
            error_code="",
            error_message="",
            source="mock_executor",
            diagnostic_message=(
                f"Mock execution completed for {service_type} request in "
                f"{location} ({requested_time})."
            ),
        )

    if intent == "new_lead":
        request = str(
            payload.get("request")
            or payload.get("summary")
            or task_data.get("goal")
            or ""
        ).strip()
        project_type = _extract_project_type(request)
        lot_size = _extract_lot_size(request)
        output_path = r"runtime\out\leads\lead_001.txt"
        write_result = dispatch_action_trigger(
            {
                "action_type": "WRITE_FILE",
                "payload": {
                    "task_id": str(task_data.get("task_id", "")).strip(),
                    "path": output_path,
                    "content": _lead_output_content(
                        request=request,
                        project_type=project_type,
                        lot_size=lot_size,
                    ),
                },
            },
            task_state=task_data,
        )
        if str(write_result.get("status", "")).strip().lower() != "completed":
            raise ValueError(
                str(
                    write_result.get("diagnostic_message", "")
                    or write_result.get("error_message", "")
                ).strip()
                or "lead intake write failed"
            )
        return build_action_result(
            status="completed",
            task_id=task_id,
            action_type=action_type,
            result_payload={
                "output_path": output_path,
                "extracted_data": {
                    "type": project_type,
                    "lot_size": lot_size,
                },
            },
            error_code="",
            error_message="",
            source="mock_executor",
            diagnostic_message=(
                f"Lead intake captured: type={project_type}; lot_size={lot_size}; "
                f"file={output_path}"
            ),
        )

    if intent == "outbound_message":
        outbound_channel = str(payload.get("outbound_channel", "sms")).strip() or "sms"
        return build_action_result(
            status="completed",
            task_id=task_id,
            action_type=action_type,
            result_payload={"outbound_channel": outbound_channel},
            error_code="",
            error_message="",
            source="mock_executor",
            diagnostic_message=f"Mock outbound execution completed via {outbound_channel}.",
        )

    if intent == "lead_followup":
        existing_lead = _load_existing_lead()
        request = str(existing_lead.get("request", "")).strip()
        project_type = str(existing_lead.get("type", "")).strip() or "unknown"
        lot_size = str(existing_lead.get("lot_size", "")).strip() or "unknown"
        summary = f"Lead for {project_type} project on {lot_size} lot requesting pricing."
        next_step = "Prepare preliminary pricing range and schedule qualification call."
        short_message = (
            "Thanks for reaching out about your ADU project. "
            "We can review pricing range and next steps with you."
        )
        output_path = r"runtime\out\leads\lead_001_followup.txt"
        write_result = dispatch_action_trigger(
            {
                "action_type": "WRITE_FILE",
                "payload": {
                    "task_id": str(task_data.get("task_id", "")).strip(),
                    "path": output_path,
                    "content": _followup_output_content(
                        summary=summary,
                        next_step=next_step,
                        short_message=short_message,
                    ),
                },
            },
            task_state=task_data,
        )
        if str(write_result.get("status", "")).strip().lower() != "completed":
            raise ValueError(
                str(
                    write_result.get("diagnostic_message", "")
                    or write_result.get("error_message", "")
                ).strip()
                or "lead follow-up write failed"
            )
        return build_action_result(
            status="completed",
            task_id=task_id,
            action_type=action_type,
            result_payload={
                "output_path": output_path,
                "request": request,
                "recommended_next_action": next_step,
                "short_message": short_message,
            },
            error_code="",
            error_message="",
            source="mock_executor",
            diagnostic_message=(
                f"Lead follow-up created: next_step={next_step}; file={output_path}"
            ),
        )

    if intent == LEAD_ESTIMATE_WORKFLOW_TYPE:
        decision = resolve_estimate_decision(
            task_id=task_data.get("task_id", ""),
            payload=payload,
        )
        return build_action_result(
            status="completed",
            task_id=task_id,
            action_type=action_type,
            result_payload={"decision": decision},
            error_code="",
            error_message="",
            source="mock_executor",
            diagnostic_message=build_decision_summary(decision),
        )

    return build_action_result(
        status="completed",
        task_id=task_id,
        action_type=action_type,
        result_payload={},
        error_code="",
        error_message="",
        source="mock_executor",
        diagnostic_message=str(payload.get("summary") or task_data.get("goal") or "Mock execution completed.").strip(),
    )
