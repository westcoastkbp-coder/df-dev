from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory

from app.execution.lead_estimate_contract import WORKFLOW_TYPE, validate_input_payload
from app.execution.lead_estimate_read import resolve_estimate_decision
from app.execution.lead_estimate_write import bind_decision_action
from app.execution.paths import OUTPUT_DIR, ROOT_DIR, TASKS_FILE
from app.orchestrator.task_factory import clear_task_runtime_store, create_task, get_task, save_task
from app.orchestrator.task_state_store import run_in_transaction


VALIDATION_REPORT_FILE = OUTPUT_DIR / "validation" / "lead_decision_binding_validation.json"
FAILURE_CLASSES = {
    "classification_error",
    "missing_required_input",
    "overuse_manual_review",
    "wrong_archive_path",
    "wrong_child_task_type",
    "traceability_gap",
    "state_inconsistency",
    "operator_usability_issue",
}

VALIDATION_SCENARIOS = (
    {
        "scenario_id": "qualified_lead",
        "input_type": "structured_lead",
        "lead_id": "lead-qualified-001",
        "lead_data": {
            "project_type": "ADU",
            "scope_summary": "Detached ADU with pricing request",
            "contact_info": {"phone": "555-0100"},
            "lead_exists": True,
        },
        "expected_decision": {
            "decision": "create_estimate",
            "confidence": "high",
            "next_step": "create_estimate_task",
        },
        "expected_action": "create_estimate_task",
        "expected_child_intent": "estimate_task",
    },
    {
        "scenario_id": "incomplete_scope_lead",
        "input_type": "structured_lead",
        "lead_id": "lead-incomplete-001",
        "lead_data": {
            "project_type": "ADU",
            "scope_summary": "",
            "contact_info": {"phone": "555-0101"},
            "lead_exists": True,
        },
        "expected_decision": {
            "decision": "request_followup",
            "confidence": "high",
            "next_step": "request_missing_scope",
        },
        "expected_action": "request_missing_scope",
        "expected_child_intent": "missing_scope_followup",
    },
    {
        "scenario_id": "non_qualified_lead",
        "input_type": "structured_lead",
        "lead_id": "lead-nonqualified-001",
        "lead_data": {
            "lead_exists": True,
            "unsupported_request": True,
            "contact_info": {"phone": "555-0102"},
        },
        "expected_decision": {
            "decision": "reject_lead",
            "confidence": "high",
            "next_step": "archive_lead",
        },
        "expected_action": "archive_lead",
        "expected_child_intent": "",
    },
    {
        "scenario_id": "ambiguous_lead",
        "input_type": "structured_lead",
        "lead_id": "lead-ambiguous-001",
        "lead_data": {
            "lead_exists": False,
            "contact_info": {},
        },
        "expected_decision": {
            "decision": "request_followup",
            "confidence": "medium",
            "next_step": "manual_review",
        },
        "expected_action": "manual_review",
        "expected_child_intent": "manual_review_task",
    },
    {
        "scenario_id": "repeated_same_lead",
        "input_type": "structured_lead",
        "lead_id": "lead-repeat-001",
        "lead_data": {
            "project_type": "ADU",
            "scope_summary": "Garage conversion with pricing request",
            "contact_info": {"phone": "555-0103"},
            "lead_exists": True,
        },
        "expected_decision": {
            "decision": "create_estimate",
            "confidence": "high",
            "next_step": "create_estimate_task",
        },
        "expected_action": "create_estimate_task",
        "expected_child_intent": "estimate_task",
    },
)


def _decision_snapshot(decision: dict[str, object]) -> dict[str, object]:
    return {
        "decision": str(decision.get("decision", "")).strip(),
        "confidence": str(decision.get("confidence", "")).strip(),
        "next_step": str(decision.get("next_step", "")).strip(),
    }


def _binding_shape(binding: dict[str, object]) -> dict[str, object]:
    return {
        "binding_action": str(binding.get("binding_action", "")).strip(),
        "binding_status": str(binding.get("binding_status", "")).strip(),
        "child_task_created": bool(binding.get("child_task_created", False)),
        "child_task_intent": str(binding.get("child_task_intent", "")).strip(),
        "archive_status": str(binding.get("archive_status", "")).strip(),
        "source_lead_id": str(binding.get("source_lead_id", "")).strip(),
        "action_source": str(binding.get("action_source", "")).strip(),
        "reason_code": str(binding.get("reason_code", "")).strip(),
    }


def _scenario_payload(scenario: dict[str, object]) -> dict[str, object]:
    return {
        "workflow_type": WORKFLOW_TYPE,
        "lead_id": str(scenario.get("lead_id", "")).strip(),
        "lead_data": dict(scenario.get("lead_data", {}) or {}),
    }


def _create_source_task(
    scenario: dict[str, object],
    *,
    run_index: int,
    store_path: Path,
) -> dict[str, object]:
    scenario_id = str(scenario.get("scenario_id", "")).strip()
    payload = _scenario_payload(scenario)
    return create_task(
        {
            "task_id": f"DF-VALIDATION-{scenario_id.upper()}-RUN-{run_index}",
            "status": "created",
            "intent": WORKFLOW_TYPE,
            "payload": payload,
        },
        store_path=store_path,
    )


def _persist_source_result(
    source_task: dict[str, object],
    *,
    decision: dict[str, object],
    binding: dict[str, object],
    store_path: Path,
) -> dict[str, object]:
    persisted = dict(source_task)
    persisted["result"] = {
        "decision": dict(decision),
        "binding": dict(binding),
    }
    return save_task(persisted, store_path=store_path)


def _run_once(
    scenario: dict[str, object],
    *,
    run_index: int,
    store_path: Path,
) -> dict[str, object]:
    source_task = _create_source_task(scenario, run_index=run_index, store_path=store_path)
    payload = _scenario_payload(scenario)
    decision = resolve_estimate_decision(
        task_id=source_task.get("task_id", ""),
        payload=payload,
    )
    binding = bind_decision_action(
        task_data=source_task,
        decision=decision,
        store_path=store_path,
    )
    persisted_source = _persist_source_result(
        source_task,
        decision=decision,
        binding=binding,
        store_path=store_path,
    )
    child_task = None
    if bool(binding.get("child_task_created")) and str(binding.get("child_task_id", "")).strip():
        child_task = get_task(str(binding.get("child_task_id", "")).strip(), store_path=store_path)
    return {
        "source_task": persisted_source,
        "decision": decision,
        "binding": binding,
        "child_task": child_task,
    }


def _reset_validation_store(store_path: Path) -> None:
    def _reset(active_connection) -> None:
        active_connection.execute("DELETE FROM Task")
        active_connection.execute("DELETE FROM ExecutionLedger")
        active_connection.execute("DELETE FROM task_versions")
        active_connection.execute("DELETE FROM execution_branches")

    run_in_transaction(
        _reset,
        store_path=store_path,
        operation_name="reset_validation_store",
    )


def _classify_failure(
    *,
    input_valid: bool,
    expected_decision: dict[str, object],
    actual_decision: dict[str, object],
    expected_action: str,
    actual_action: str,
    child_task: dict[str, object] | None,
    expected_child_intent: str,
    binding: dict[str, object],
    source_task: dict[str, object],
    repeatable: bool,
    store_path: Path,
) -> tuple[str, str]:
    if not input_valid:
        return "missing_required_input", "input_blocked"
    if actual_decision != expected_decision:
        if actual_action == "manual_review" and expected_action != "manual_review":
            return "overuse_manual_review", "manual_review_overused"
        return "classification_error", "decision_mismatch"
    if actual_action != expected_action:
        if expected_action == "archive_lead":
            return "wrong_archive_path", "archive_mismatch"
        return "wrong_child_task_type", "action_mismatch"
    if expected_action == "archive_lead":
        if bool(binding.get("child_task_created")) or str(binding.get("archive_status", "")).strip() != "archived":
            return "wrong_archive_path", "archive_mismatch"
    else:
        if child_task is None or str(child_task.get("intent", "")).strip() != expected_child_intent:
            return "wrong_child_task_type", "child_type_mismatch"
        child_payload = dict(child_task.get("payload", {}) or {})
        if (
            str(child_payload.get("parent_task_id", "")).strip()
            != str(source_task.get("task_id", "")).strip()
            or str(child_payload.get("source_lead_id", "")).strip()
            != str(binding.get("source_lead_id", "")).strip()
        ):
            return "traceability_gap", "trace_mismatch"
    persisted_source = get_task(str(source_task.get("task_id", "")).strip(), store_path=store_path)
    if persisted_source is None or not isinstance(persisted_source.get("result"), dict):
        return "state_inconsistency", "state_missing"
    if not repeatable:
        return "state_inconsistency", "repeatability_mismatch"
    return "", "validated"


def run_validation_pack(*, store_path: Path | None = None) -> dict[str, object]:
    target_store = Path(store_path) if store_path is not None else TASKS_FILE
    clear_task_runtime_store()
    _reset_validation_store(target_store)
    try:
        scenarios_report: list[dict[str, object]] = []
        for scenario in VALIDATION_SCENARIOS:
            payload = _scenario_payload(scenario)
            input_valid, _, _ = validate_input_payload(payload)
            first = _run_once(scenario, run_index=1, store_path=target_store)
            with TemporaryDirectory(prefix="df-validation-repeat-") as repeat_dir:
                second = _run_once(
                    scenario,
                    run_index=2,
                    store_path=Path(repeat_dir) / target_store.name,
                )
            actual_decision = _decision_snapshot(dict(first["decision"]))
            expected_decision = dict(scenario.get("expected_decision", {}) or {})
            actual_action = str(first["binding"].get("binding_action", "")).strip()
            expected_action = str(scenario.get("expected_action", "")).strip()
            repeatable = (
                _decision_snapshot(dict(first["decision"])) == _decision_snapshot(dict(second["decision"]))
                and _binding_shape(dict(first["binding"])) == _binding_shape(dict(second["binding"]))
            )
            failure_class, notes = _classify_failure(
                input_valid=input_valid,
                expected_decision=expected_decision,
                actual_decision=actual_decision,
                expected_action=expected_action,
                actual_action=actual_action,
                child_task=(
                    dict(first["child_task"])
                    if isinstance(first.get("child_task"), dict)
                    else None
                ),
                expected_child_intent=str(scenario.get("expected_child_intent", "")).strip(),
                binding=dict(first["binding"]),
                source_task=dict(first["source_task"]),
                repeatable=repeatable,
                store_path=target_store,
            )
            scenarios_report.append(
                {
                    "scenario_id": str(scenario.get("scenario_id", "")).strip(),
                    "input_type": str(scenario.get("input_type", "")).strip(),
                    "expected_decision": expected_decision,
                    "actual_decision": actual_decision,
                    "expected_action": expected_action,
                    "actual_action": actual_action,
                    "pass_fail": "pass" if failure_class == "" else "fail",
                    "failure_class": failure_class,
                    "notes": notes,
                }
            )
        return {
            "workflow_type": WORKFLOW_TYPE,
            "scenario_count": len(VALIDATION_SCENARIOS),
            "scenarios": scenarios_report,
        }
    finally:
        clear_task_runtime_store()


def write_validation_report(
    *,
    store_path: Path | None = None,
    output_path: Path | None = None,
) -> Path:
    report = run_validation_pack(store_path=store_path)
    target = Path(output_path) if output_path is not None else ROOT_DIR / VALIDATION_REPORT_FILE
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return target
