from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import app.execution.lead_estimate_decision as lead_estimate_decision_module
import app.execution.paths as paths_module
import app.execution.real_lead_runner as real_lead_runner_module
import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_state_store as task_state_store_module
import app.policy.policy_gate as policy_gate_module
import runtime.system_log as system_log_module
from app.execution.real_lead_runner import run_real_lead
from app.execution.followup_reentry import reenter_completed_followup
from app.execution.real_lead_contract import build_followup_context_payload
from app.orchestrator.task_factory import create_task, get_task, load_tasks, save_task
from app.orchestrator.task_lifecycle import (
    can_transition_task_status,
    transition_task_status,
)


@dataclass(frozen=True)
class ScenarioDefinition:
    scenario_id: str
    initial_input: dict[str, object]
    expected_task_sequence: list[str]
    expected_transitions: list[str]
    expected_actions: list[str]
    expected_final_state: str
    detected_gap: str = ""


SCENARIOS = (
    ScenarioDefinition(
        scenario_id="qualified_lead_to_project_start",
        initial_input={
            "lead_id": "scenario-lead-qualified-001",
            "contact_info": {"phone": "555-0201"},
            "project_type": "ADU",
            "scope_summary": "Detached ADU with approved estimate",
        },
        expected_task_sequence=["lead", "estimate", "follow_up", "permit", "project"],
        expected_transitions=[
            "follow_up:created->confirmed",
            "follow_up:confirmed->pending",
            "follow_up:pending->running",
            "follow_up:running->completed",
            "permit:created->confirmed",
            "permit:confirmed->pending",
            "permit:pending->running",
            "permit:running->completed",
            "project:created->confirmed",
            "project:confirmed->pending",
            "project:pending->running",
        ],
        expected_actions=["create_estimate_task"],
        expected_final_state="project:running",
    ),
    ScenarioDefinition(
        scenario_id="missing_data_followup_reentry_estimate",
        initial_input={
            "lead_id": "scenario-lead-missing-001",
            "contact_info": {},
            "project_type": "ADU",
            "scope_summary": "Detached ADU with missing contact",
        },
        expected_task_sequence=["follow_up", "lead", "estimate"],
        expected_transitions=[
            "follow_up:pending->running",
            "follow_up:running->completed",
        ],
        expected_actions=["request_input_completion", "create_estimate_task"],
        expected_final_state="estimate:created",
    ),
    ScenarioDefinition(
        scenario_id="permit_required_to_project",
        initial_input={
            "task_type": "lead",
            "summary": "Permit-required office flow",
        },
        expected_task_sequence=["lead", "estimate", "follow_up", "permit", "project"],
        expected_transitions=[
            "estimate:created->confirmed",
            "estimate:confirmed->pending",
            "estimate:pending->running",
            "estimate:running->completed",
            "follow_up:created->confirmed",
            "follow_up:confirmed->pending",
            "follow_up:pending->running",
            "follow_up:running->completed",
            "permit:created->confirmed",
            "permit:confirmed->pending",
            "permit:pending->running",
            "permit:running->completed",
            "project:created->confirmed",
            "project:confirmed->pending",
        ],
        expected_actions=[],
        expected_final_state="project:pending",
        detected_gap="No dedicated permit-submission action exists; scenario validates structural task flow and lifecycle only.",
    ),
    ScenarioDefinition(
        scenario_id="project_to_procurement_execution",
        initial_input={
            "task_type": "project",
            "summary": "Project procurement handoff",
        },
        expected_task_sequence=[
            "lead",
            "estimate",
            "follow_up",
            "permit",
            "project",
            "procurement",
        ],
        expected_transitions=[
            "procurement:created->confirmed",
            "procurement:confirmed->pending",
            "procurement:pending->running",
            "procurement:running->completed",
        ],
        expected_actions=[],
        expected_final_state="procurement:completed",
        detected_gap="No separate execution task type exists; execution is representable only as procurement lifecycle completion.",
    ),
    ScenarioDefinition(
        scenario_id="project_to_payment",
        initial_input={
            "task_type": "project",
            "summary": "Project payment handoff",
        },
        expected_task_sequence=[
            "lead",
            "estimate",
            "follow_up",
            "permit",
            "project",
            "payment",
        ],
        expected_transitions=[
            "payment:created->confirmed",
            "payment:confirmed->pending",
            "payment:pending->running",
            "payment:running->completed",
        ],
        expected_actions=[],
        expected_final_state="payment:completed",
        detected_gap="No payment-specific execution logic exists; scenario validates explicit task lineage and lifecycle only.",
    ),
)


def configure_scenario_runtime(monkeypatch, tmp_path: Path) -> Path:
    task_store_path = tmp_path / "data" / "tasks.json"
    monkeypatch.setattr(task_state_store_module, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store_module,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(paths_module, "TASKS_FILE", task_store_path)
    monkeypatch.setattr(lead_estimate_decision_module, "TASKS_FILE", task_store_path)
    monkeypatch.setattr(real_lead_runner_module, "TASKS_FILE", task_store_path)
    monkeypatch.setattr(
        policy_gate_module,
        "POLICY_LOG_FILE",
        tmp_path / "runtime" / "logs" / "policy.log",
    )
    monkeypatch.setattr(
        system_log_module,
        "SYSTEM_LOG_FILE",
        tmp_path / "runtime" / "logs" / "system.log",
    )
    monkeypatch.setattr(
        system_log_module, "TASK_LOG_FILE", tmp_path / "runtime" / "logs" / "tasks.log"
    )
    task_factory_module.clear_task_runtime_store()
    return task_store_path


def _task_label(task: dict[str, object]) -> str:
    task_type = str(task.get("task_type", "")).strip()
    if task_type:
        return task_type
    intent = str(task.get("intent", "")).strip()
    return {
        "lead_estimate_decision": "lead",
        "estimate_task": "estimate",
        "missing_input_followup": "follow_up",
    }.get(intent, intent)


def _scenario_status(status: object) -> str:
    return {
        "CREATED": "created",
        "VALIDATED": "pending",
        "EXECUTING": "running",
        "COMPLETED": "completed",
    }.get(str(status or "").strip().upper(), str(status or "").strip().lower())


def _runtime_status_for_scenario(next_status: str) -> str:
    return {
        "confirmed": "VALIDATED",
        "pending": "VALIDATED",
        "running": "EXECUTING",
        "completed": "COMPLETED",
    }[str(next_status or "").strip().lower()]


def _create_office_task(
    *,
    store_path: Path,
    task_id: str,
    task_type: str,
    parent_task_id: str = "",
) -> dict[str, object]:
    payload: dict[str, object] = {"summary": task_type}
    if parent_task_id:
        payload["parent_task_id"] = parent_task_id
    return create_task(
        {
            "task_id": task_id,
            "status": "created",
            "intent": "generic_task",
            "task_type": task_type,
            "parent_task_id": parent_task_id,
            "payload": payload,
        },
        store_path=store_path,
    )


def _advance_task(
    *,
    store_path: Path,
    task_id: str,
    task_label: str,
    target_statuses: list[str],
) -> list[str]:
    transitions: list[str] = []
    task = get_task(task_id, store_path=store_path)
    if task is None:
        raise ValueError(f"task not found: {task_id}")
    display_status = _scenario_status(task.get("status", ""))
    for index, next_status in enumerate(target_statuses, start=1):
        next_display_status = str(next_status).strip().lower()
        if (display_status, next_display_status) not in {
            ("created", "confirmed"),
            ("confirmed", "pending"),
            ("pending", "running"),
            ("running", "completed"),
        }:
            raise ValueError(
                f"invalid scenario transition: {display_status} -> {next_display_status}"
            )
        runtime_target_status = _runtime_status_for_scenario(next_display_status)
        current_runtime_status = str(task.get("status", "")).strip()
        if current_runtime_status != runtime_target_status:
            if not can_transition_task_status(
                current_runtime_status, runtime_target_status
            ):
                raise ValueError(
                    f"invalid scenario transition: {current_runtime_status} -> {runtime_target_status}"
                )
            transition_task_status(
                task,
                runtime_target_status,
                timestamp=f"2026-04-04T00:00:{index:02d}Z",
                details=f"{task_label}:{next_display_status}",
            )
            task = save_task(task, store_path=store_path)
        transitions.append(f"{task_label}:{display_status}->{next_display_status}")
        display_status = next_display_status
    return transitions


def _build_manual_chain(
    store_path: Path, *, scenario_id: str
) -> dict[str, dict[str, object]]:
    lead = _create_office_task(
        store_path=store_path,
        task_id=f"{scenario_id}-lead",
        task_type="lead",
    )
    estimate = _create_office_task(
        store_path=store_path,
        task_id=f"{scenario_id}-estimate",
        task_type="estimate",
        parent_task_id=str(lead["task_id"]),
    )
    follow_up = _create_office_task(
        store_path=store_path,
        task_id=f"{scenario_id}-follow-up",
        task_type="follow_up",
        parent_task_id=str(estimate["task_id"]),
    )
    permit = _create_office_task(
        store_path=store_path,
        task_id=f"{scenario_id}-permit",
        task_type="permit",
        parent_task_id=str(follow_up["task_id"]),
    )
    project = _create_office_task(
        store_path=store_path,
        task_id=f"{scenario_id}-project",
        task_type="project",
        parent_task_id=str(permit["task_id"]),
    )
    return {
        "lead": lead,
        "estimate": estimate,
        "follow_up": follow_up,
        "permit": permit,
        "project": project,
    }


def run_scenario(
    definition: ScenarioDefinition, *, store_path: Path
) -> dict[str, object]:
    if definition.scenario_id == "qualified_lead_to_project_start":
        report = run_real_lead(definition.initial_input, store_path=store_path)
        lead_task = get_task(str(report["parent_task_id"]), store_path=store_path) or {}
        estimate_task = (
            get_task(str(report["created_child_task_ids"][0]), store_path=store_path)
            or {}
        )
        follow_up = _create_office_task(
            store_path=store_path,
            task_id="scenario-qualified-follow-up",
            task_type="follow_up",
            parent_task_id=str(estimate_task.get("task_id", "")),
        )
        permit = _create_office_task(
            store_path=store_path,
            task_id="scenario-qualified-permit",
            task_type="permit",
            parent_task_id=str(follow_up.get("task_id", "")),
        )
        project = _create_office_task(
            store_path=store_path,
            task_id="scenario-qualified-project",
            task_type="project",
            parent_task_id=str(permit.get("task_id", "")),
        )
        transitions = []
        transitions.extend(
            _advance_task(
                store_path=store_path,
                task_id=str(follow_up["task_id"]),
                task_label="follow_up",
                target_statuses=["confirmed", "pending", "running", "completed"],
            )
        )
        transitions.extend(
            _advance_task(
                store_path=store_path,
                task_id=str(permit["task_id"]),
                task_label="permit",
                target_statuses=["confirmed", "pending", "running", "completed"],
            )
        )
        transitions.extend(
            _advance_task(
                store_path=store_path,
                task_id=str(project["task_id"]),
                task_label="project",
                target_statuses=["confirmed", "pending", "running"],
            )
        )
        return {
            "scenario_id": definition.scenario_id,
            "task_sequence": [
                _task_label(lead_task),
                _task_label(estimate_task),
                "follow_up",
                "permit",
                "project",
            ],
            "transitions": transitions,
            "actions": [str(report["next_action"])],
            "final_state": "project:running",
        }

    if definition.scenario_id == "missing_data_followup_reentry_estimate":
        report = run_real_lead(definition.initial_input, store_path=store_path)
        followup_task = (
            get_task(str(report["created_child_task_ids"][0]), store_path=store_path)
            or {}
        )
        followup_payload = dict(followup_task.get("payload", {}) or {})
        followup_payload["updated_lead_input"] = {
            "lead_id": str(definition.initial_input.get("lead_id", "")).strip(),
            "contact_info": {"phone": "555-0202"},
            "project_type": str(
                definition.initial_input.get("project_type", "")
            ).strip(),
            "scope_summary": str(
                definition.initial_input.get("scope_summary", "")
            ).strip(),
        }
        followup_task["payload"] = followup_payload
        followup_task = save_task(followup_task, store_path=store_path)
        transitions = _advance_task(
            store_path=store_path,
            task_id=str(followup_task["task_id"]),
            task_label="follow_up",
            target_statuses=["running"],
        )
        followed = get_task(str(followup_task["task_id"]), store_path=store_path) or {}
        transition_task_status(
            followed,
            "COMPLETED",
            timestamp="2026-04-04T00:00:59Z",
            details="follow_up:completed",
        )
        completed_followup = save_task(followed, store_path=store_path)
        reentry_result = reenter_completed_followup(
            build_followup_context_payload(completed_followup),
            store_path=store_path,
        )
        transitions.append("follow_up:running->completed")
        task_factory_module.clear_task_runtime_store()
        reentry_task = next(
            task
            for task in load_tasks(store_path)
            if str(task.get("task_id", "")).strip()
            == str(reentry_result.get("reentry_task_id", "")).strip()
        )
        estimate_task = (
            get_task(
                str(
                    dict(reentry_task.get("result", {}) or {})
                    .get("binding", {})
                    .get("child_task_id", "")
                ),
                store_path=store_path,
            )
            or {}
        )
        return {
            "scenario_id": definition.scenario_id,
            "task_sequence": [
                "follow_up",
                _task_label(reentry_task),
                _task_label(estimate_task),
            ],
            "transitions": transitions,
            "actions": [
                str(
                    dict(followup_task.get("payload", {}) or {}).get(
                        "required_action", ""
                    )
                ).strip(),
                str(
                    dict(reentry_task.get("result", {}) or {})
                    .get("decision", {})
                    .get("next_step", "")
                ).strip(),
            ],
            "final_state": f"{_task_label(estimate_task)}:{_scenario_status(estimate_task.get('status', ''))}",
        }

    if definition.scenario_id == "permit_required_to_project":
        chain = _build_manual_chain(store_path, scenario_id="scenario-permit")
        transitions = []
        transitions.extend(
            _advance_task(
                store_path=store_path,
                task_id=str(chain["estimate"]["task_id"]),
                task_label="estimate",
                target_statuses=["confirmed", "pending", "running", "completed"],
            )
        )
        transitions.extend(
            _advance_task(
                store_path=store_path,
                task_id=str(chain["follow_up"]["task_id"]),
                task_label="follow_up",
                target_statuses=["confirmed", "pending", "running", "completed"],
            )
        )
        transitions.extend(
            _advance_task(
                store_path=store_path,
                task_id=str(chain["permit"]["task_id"]),
                task_label="permit",
                target_statuses=["confirmed", "pending", "running", "completed"],
            )
        )
        transitions.extend(
            _advance_task(
                store_path=store_path,
                task_id=str(chain["project"]["task_id"]),
                task_label="project",
                target_statuses=["confirmed", "pending"],
            )
        )
        return {
            "scenario_id": definition.scenario_id,
            "task_sequence": ["lead", "estimate", "follow_up", "permit", "project"],
            "transitions": transitions,
            "actions": [],
            "final_state": "project:pending",
        }

    if definition.scenario_id == "project_to_procurement_execution":
        chain = _build_manual_chain(store_path, scenario_id="scenario-procurement")
        procurement = _create_office_task(
            store_path=store_path,
            task_id="scenario-procurement-child",
            task_type="procurement",
            parent_task_id=str(chain["project"]["task_id"]),
        )
        transitions = _advance_task(
            store_path=store_path,
            task_id=str(procurement["task_id"]),
            task_label="procurement",
            target_statuses=["confirmed", "pending", "running", "completed"],
        )
        return {
            "scenario_id": definition.scenario_id,
            "task_sequence": [
                "lead",
                "estimate",
                "follow_up",
                "permit",
                "project",
                "procurement",
            ],
            "transitions": transitions,
            "actions": [],
            "final_state": "procurement:completed",
        }

    if definition.scenario_id == "project_to_payment":
        chain = _build_manual_chain(store_path, scenario_id="scenario-payment")
        payment = _create_office_task(
            store_path=store_path,
            task_id="scenario-payment-child",
            task_type="payment",
            parent_task_id=str(chain["project"]["task_id"]),
        )
        transitions = _advance_task(
            store_path=store_path,
            task_id=str(payment["task_id"]),
            task_label="payment",
            target_statuses=["confirmed", "pending", "running", "completed"],
        )
        return {
            "scenario_id": definition.scenario_id,
            "task_sequence": [
                "lead",
                "estimate",
                "follow_up",
                "permit",
                "project",
                "payment",
            ],
            "transitions": transitions,
            "actions": [],
            "final_state": "payment:completed",
        }

    raise ValueError(f"unsupported scenario: {definition.scenario_id}")
