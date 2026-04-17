from __future__ import annotations

import json
from pathlib import Path

from app.execution.lead_estimate_contract import WORKFLOW_TYPE
from app.execution.paths import TASKS_FILE
from app.execution.followup_reentry import reenter_completed_followup
from app.execution.real_lead_contract import (
    DEFAULT_REAL_LEAD_INPUT,
    build_real_lead_run_id,
    normalize_real_lead_input,
    validate_real_lead_input,
)
from app.execution.real_lead_reporting import write_real_lead_report
from app.execution.real_lead_tasks import (
    create_missing_input_followup_task,
    execute_real_lead_pipeline,
)
from app.orchestrator.task_factory import clear_task_runtime_store, get_task
from app.policy.policy_gate import record_policy_decision
from runtime.system_log import log_event


ALLOWED_FAILURE_CLASSES = {
    "classification_error",
    "missing_required_input",
    "wrong_archive_path",
    "wrong_child_task_type",
    "traceability_gap",
    "state_inconsistency",
    "operator_usability_issue",
    "none",
}

_normalize_input = normalize_real_lead_input


def run_real_lead(
    lead_input: object,
    *,
    store_path: Path | None = None,
) -> dict[str, object]:
    target_store = Path(store_path) if store_path is not None else TASKS_FILE
    clear_task_runtime_store()
    try:
        normalized_input = normalize_real_lead_input(lead_input)
        normalized_lead_id = str(normalized_input.get("lead_id") or "").strip()
        run_id = build_real_lead_run_id(normalized_lead_id or "UNKNOWN")
        input_valid, input_reason = validate_real_lead_input(normalized_input)
        if not input_valid:
            record_policy_decision(run_id, allowed=False, reason=input_reason)
            log_event("validation", f"real lead input blocked: task={run_id} reason={input_reason}")
            created_child_task_ids: list[str] = []
            persisted_state_ok = False
            trace_link_ok = False
            if input_reason.startswith("missing required fields:"):
                followup_task = create_missing_input_followup_task(
                    normalized_input,
                    store_path=target_store,
                )
                created_child_task_ids = [str(followup_task.get("task_id", "")).strip()]
                persisted_state_ok = True
                trace_link_ok = True
            return {
                "run_id": run_id,
                "lead_id": normalized_lead_id,
                "workflow_type": WORKFLOW_TYPE,
                "decision": {},
                "next_action": "",
                "created_child_task_ids": created_child_task_ids,
                "archived_state_flag": False,
                "parent_task_id": "",
                "persisted_state_ok": persisted_state_ok,
                "trace_link_ok": trace_link_ok,
                "pass_fail": "fail",
                "failure_class": "missing_required_input",
                "notes": input_reason.replace("missing required fields: ", "missing:"),
            }

        record_policy_decision(run_id, allowed=True, reason=input_reason)
        log_event("validation", f"real lead input allowed: task={run_id} reason={input_reason}")
        try:
            pipeline_result = execute_real_lead_pipeline(
                normalized_input,
                store_path=target_store,
            )
        except ValueError as exc:
            error_message = str(exc).strip()
            if error_message.startswith("policy_blocked:"):
                return {
                    "run_id": run_id,
                    "lead_id": str(normalized_input.get("lead_id", "")).strip(),
                    "workflow_type": WORKFLOW_TYPE,
                    "decision": {},
                    "next_action": "",
                    "created_child_task_ids": [],
                    "archived_state_flag": False,
                    "parent_task_id": "",
                    "persisted_state_ok": False,
                    "trace_link_ok": False,
                    "pass_fail": "fail",
                    "failure_class": "operator_usability_issue",
                    "notes": "policy_blocked",
                }
            if error_message.startswith("contract_blocked:"):
                blocked_parent = get_task(
                    build_real_lead_run_id(str(normalized_input.get("lead_id", "")).strip()),
                    store_path=target_store,
                )
                return {
                    "run_id": run_id,
                    "lead_id": str(normalized_input.get("lead_id", "")).strip(),
                    "workflow_type": WORKFLOW_TYPE,
                    "decision": {},
                    "next_action": "",
                    "created_child_task_ids": [],
                    "archived_state_flag": False,
                    "parent_task_id": str(
                        blocked_parent.get("task_id", "") if blocked_parent else ""
                    ).strip(),
                    "persisted_state_ok": False,
                    "trace_link_ok": False,
                    "pass_fail": "fail",
                    "failure_class": "classification_error",
                    "notes": "contract_blocked",
                }
            raise

        parent_task = dict(pipeline_result.get("task", {}) or {})
        decision = dict(pipeline_result.get("decision", {}) or {})
        binding = dict(pipeline_result.get("binding", {}) or {})
        if not parent_task:
            return {
                "run_id": run_id,
                "lead_id": str(normalized_input.get("lead_id", "")).strip(),
                "workflow_type": WORKFLOW_TYPE,
                "decision": {},
                "next_action": "",
                "created_child_task_ids": [],
                "archived_state_flag": False,
                "parent_task_id": "",
                "persisted_state_ok": False,
                "trace_link_ok": False,
                "pass_fail": "fail",
                "failure_class": "operator_usability_issue",
                "notes": "policy_blocked",
            }

        created_child_task_ids: list[str] = []
        child_task = None
        if bool(binding.get("child_task_created")) and str(binding.get("child_task_id", "")).strip():
            child_task_id = str(binding.get("child_task_id", "")).strip()
            created_child_task_ids.append(child_task_id)
            child_task = get_task(child_task_id, store_path=target_store)

        persisted_parent = get_task(str(parent_task.get("task_id", "")).strip(), store_path=target_store)
        persisted_state_ok = bool(
            persisted_parent
            and isinstance(persisted_parent.get("result"), dict)
            and dict(persisted_parent.get("result", {}).get("decision", {}) or {}) == dict(decision)
        )
        trace_link_ok = True
        failure_class = "none"
        notes = "validated"
        if str(decision.get("next_step", "")).strip() == "archive_lead":
            trace_link_ok = (
                str(binding.get("parent_task_id", "")).strip()
                == str(parent_task.get("task_id", "")).strip()
            )
            if not bool(binding.get("archive_status") == "archived"):
                failure_class = "wrong_archive_path"
                notes = "archive_mismatch"
        else:
            expected_child_intent = {
                "create_estimate_task": "estimate_task",
                "request_missing_scope": "missing_scope_followup",
                "manual_review": "manual_review_task",
            }.get(str(decision.get("next_step", "")).strip(), "")
            trace_link_ok = bool(
                child_task
                and str(child_task.get("intent", "")).strip() == expected_child_intent
                and str(dict(child_task.get("payload", {}) or {}).get("parent_task_id", "")).strip()
                == str(parent_task.get("task_id", "")).strip()
            )
            if child_task is None or str(child_task.get("intent", "")).strip() != expected_child_intent:
                failure_class = "wrong_child_task_type"
                notes = "child_type_mismatch"
        if not trace_link_ok and failure_class == "none":
            failure_class = "traceability_gap"
            notes = "trace_mismatch"
        if not persisted_state_ok and failure_class == "none":
            failure_class = "state_inconsistency"
            notes = "state_missing"

        return {
            "run_id": run_id,
            "lead_id": str(normalized_input.get("lead_id", "")).strip(),
            "workflow_type": WORKFLOW_TYPE,
            "decision": dict(decision),
            "next_action": str(decision.get("next_step", "")).strip(),
            "created_child_task_ids": created_child_task_ids,
            "archived_state_flag": str(binding.get("archive_status", "")).strip() == "archived",
            "parent_task_id": str(parent_task.get("task_id", "")).strip(),
            "persisted_state_ok": persisted_state_ok,
            "trace_link_ok": trace_link_ok,
            "pass_fail": "pass" if failure_class == "none" else "fail",
            "failure_class": failure_class,
            "notes": notes,
        }
    finally:
        clear_task_runtime_store()


def load_real_lead_input(input_path: Path | None = None) -> dict[str, object]:
    if input_path is None:
        return dict(DEFAULT_REAL_LEAD_INPUT)
    payload = json.loads(Path(input_path).read_text(encoding="utf-8"))
    return normalize_real_lead_input(payload)


def write_real_lead_run_report(
    lead_input: object,
    *,
    store_path: Path | None = None,
    output_path: Path | None = None,
) -> Path:
    report = run_real_lead(lead_input, store_path=store_path)
    return write_real_lead_report(report, output_path=output_path)
