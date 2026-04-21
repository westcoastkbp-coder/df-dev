from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import TypedDict

from app.execution.lead_estimate_contract import build_action_payload, build_decision_payload
from app.execution.paths import OUTPUT_DIR, ROOT_DIR
from runtime.system_log import log_event, log_task_binding_result, log_task_workflow_decision


REAL_LEAD_RUN_REPORT_FILE = OUTPUT_DIR / "reports" / "real_lead_run_report.json"


class ReportingPayload(TypedDict):
    task_id: str
    decision_payload: dict[str, object]
    action_payload: dict[str, object]


def validate_reporting_payload(payload: object) -> ReportingPayload:
    if not isinstance(payload, dict):
        raise ValueError("reporting_payload must be a dict")
    payload_fields = set(payload.keys())
    required_fields = {"task_id", "decision_payload", "action_payload"}
    missing_fields = sorted(required_fields - payload_fields)
    if missing_fields:
        raise ValueError(
            f"reporting_payload missing required fields: {', '.join(missing_fields)}"
        )
    unexpected_fields = sorted(payload_fields - required_fields)
    if unexpected_fields:
        raise ValueError(
            f"reporting_payload contains unsupported fields: {', '.join(unexpected_fields)}"
        )
    if not isinstance(payload.get("decision_payload"), dict):
        raise ValueError("reporting_payload.decision_payload must be a dict")
    if not isinstance(payload.get("action_payload"), dict):
        raise ValueError("reporting_payload.action_payload must be a dict")
    return {
        "task_id": str(payload.get("task_id") or "").strip(),
        "decision_payload": build_decision_payload(dict(payload.get("decision_payload") or {})),
        "action_payload": build_action_payload(dict(payload.get("action_payload") or {})),
    }


def write_real_lead_report(
    report: dict[str, object],
    *,
    output_path: Path | None = None,
) -> Path:
    target = Path(output_path) if output_path is not None else ROOT_DIR / REAL_LEAD_RUN_REPORT_FILE
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return target


def build_reporting_payload(
    *,
    task_id: object,
    decision_payload: dict[str, object],
    action_payload: dict[str, object],
) -> ReportingPayload:
    return validate_reporting_payload(
        {
            "task_id": str(task_id or "").strip(),
            "decision_payload": dict(decision_payload),
            "action_payload": dict(action_payload),
        }
    )


def report_workflow_execution(reporting_payload: ReportingPayload) -> None:
    validated_payload = validate_reporting_payload(reporting_payload)
    normalized_task_id = str(validated_payload.get("task_id", "")).strip()
    decision_payload = dict(validated_payload.get("decision_payload", {}) or {})
    binding_result = dict(validated_payload.get("action_payload", {}) or {})
    log_task_workflow_decision(
        task_id=normalized_task_id,
        workflow_type=str(decision_payload.get("workflow_type", "")).strip(),
        decision_payload=decision_payload,
    )
    log_task_binding_result(
        task_id=normalized_task_id,
        binding_result=dict(binding_result),
    )
    log_event(
        "binding",
        (
            f"task={normalized_task_id} "
            f"binding_action={binding_result.get('binding_action')} "
            f"binding_status={binding_result.get('binding_status')} "
            f"child_task_id={binding_result.get('child_task_id', '')}"
        ),
    )
    log_event(
        "workflow",
        (
            f"task={normalized_task_id} "
            f"decision={decision_payload.get('decision')} "
            f"confidence={decision_payload.get('confidence')} "
            f"next_step={decision_payload.get('next_step')}"
        ),
    )


def _execution_result_marker(execution_result: dict[str, object]) -> str:
    normalized_result_type = str(execution_result.get("result_type", "")).strip()
    if normalized_result_type:
        return normalized_result_type

    normalized_action_type = str(execution_result.get("action_type", "")).strip().lower()
    if normalized_action_type == "new_lead":
        return "lead_intake"
    return normalized_action_type


def store_execution_result_summary(
    *,
    task_id: object,
    execution_result: dict[str, object],
    result_writer: Callable[[dict[str, object]], object] | None = None,
) -> None:
    result_type = _execution_result_marker(execution_result)
    if not result_type:
        return

    if result_writer is None:
        from app.orchestrator.task_memory import store_task_result as result_writer

    result_writer(
        {
            "task_id": str(task_id or "").strip(),
            "result_type": result_type,
            "result_summary": str(execution_result.get("diagnostic_message", "")).strip(),
        }
    )
