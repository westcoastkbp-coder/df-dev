from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from app.execution.paths import OUTPUT_DIR, ROOT_DIR
from app.memory.memory_object import make_artifact_object, make_trace_object
from app.memory.memory_registry import compute_artifact_key, register_artifact
from app.ownerbox.domain import (
    OWNERBOX_DOMAIN_TYPE,
    OwnerActionScope,
    OwnerDomain,
    OwnerMemoryScope,
    OwnerTrustProfile,
)
from app.ownerbox.owner_approval import OwnerApproval
from app.ownerbox.owner_orchestrator import OwnerOrchestrator
from app.ownerbox.owner_response_plan import OwnerResponsePlan, create_owner_response_plan
from app.ownerbox.owner_session import OwnerSession, create_owner_session
from app.ownerbox.trust_model import classify_action_risk
from app.ownerbox.workflow import (
    OwnerWorkflow,
    OwnerWorkflowStep,
    create_owner_workflow,
    instantiate_workflow_steps,
    summarize_owner_workflow_step,
    update_owner_workflow,
    update_owner_workflow_step,
)
from app.ownerbox.workflow_state_store import (
    PersistedWorkflowState,
    WorkflowStateStore,
    WorkflowStateStoreError,
)
from runtime.system_log import log_event


OWNER_WORKFLOW_TRACE_ARTIFACT_TYPE = "owner_workflow_trace"
OWNER_WORKFLOW_EVIDENCE_ARTIFACT_TYPE = "owner_workflow_evidence"
OWNER_WORKFLOW_TRACE_DIR = OUTPUT_DIR / "traces" / "owner_workflows"
OWNER_WORKFLOW_EVIDENCE_DIR = OUTPUT_DIR / "evidence" / "owner_workflows"
_SAFE_FILENAME_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")
_STEP_REFERENCE_PATTERN = re.compile(r"^\{\{step(?P<index>[1-9][0-9]*)\.(?P<path>[A-Za-z0-9_.]+)\}\}$")


def _utc_timestamp() -> str:
    from datetime import datetime, timezone

    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _safe_filename(value: object) -> str:
    normalized = _normalize_text(value) or "unknown"
    safe = _SAFE_FILENAME_PATTERN.sub("_", normalized).strip("._")
    return safe or "unknown"


def _result_metadata(action_result: Mapping[str, object] | None) -> dict[str, object]:
    if not isinstance(action_result, Mapping):
        return {}
    payload = action_result.get("payload")
    if not isinstance(payload, Mapping):
        return {}
    metadata = payload.get("metadata")
    if not isinstance(metadata, Mapping):
        return {}
    return dict(metadata)


def _result_attempt_count(action_result: Mapping[str, object] | None) -> int:
    metadata = _result_metadata(action_result)
    try:
        normalized = int(metadata.get("attempt_count", 0))
    except (TypeError, ValueError):
        return 0 if action_result is None else 1
    return max(0, normalized)


def _result_retry_status(action_result: Mapping[str, object] | None) -> str:
    return _normalize_text(_result_metadata(action_result).get("retry_status")).lower() or "not_needed"


def _result_last_error(action_result: Mapping[str, object] | None) -> dict[str, str] | None:
    if not isinstance(action_result, Mapping):
        return None
    error_code = _normalize_text(action_result.get("error_code"))
    error_message = _normalize_text(action_result.get("error_message"))
    if not error_code and not error_message:
        return None
    return {
        "error_code": error_code,
        "error_message": error_message,
    }


@dataclass(frozen=True, slots=True)
class OwnerWorkflowRunResult:
    owner_session: OwnerSession
    workflow: OwnerWorkflow
    steps: tuple[OwnerWorkflowStep, ...]
    response_plan: OwnerResponsePlan
    trace_metadata: dict[str, object]
    approval: OwnerApproval | None = None
    current_step: OwnerWorkflowStep | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "owner_session": self.owner_session.to_dict(),
            "workflow": self.workflow.to_dict(),
            "steps": [step.to_dict() for step in self.steps],
            "response_plan": self.response_plan.to_dict(),
            "trace_metadata": dict(self.trace_metadata),
            "approval": None if self.approval is None else self.approval.to_dict(),
            "current_step": None if self.current_step is None else self.current_step.to_dict(),
        }


@dataclass(slots=True)
class _WorkflowRuntime:
    owner_session: OwnerSession
    workflow: OwnerWorkflow
    steps: list[OwnerWorkflowStep]
    owner_domain: OwnerDomain
    memory_scope: OwnerMemoryScope
    action_scope: OwnerActionScope
    trust_profile: OwnerTrustProfile
    detected_language: str
    priority_class: str
    context_ref: str | None
    memory_records: list[Mapping[str, object]]
    execution_mode: str
    workflow_metadata: dict[str, object]


class OwnerWorkflowOrchestrator:
    def __init__(
        self,
        *,
        owner_orchestrator: OwnerOrchestrator | None = None,
        workflow_state_store: WorkflowStateStore | None = None,
    ) -> None:
        self._owner_orchestrator = owner_orchestrator or OwnerOrchestrator()
        self._workflow_state_store = workflow_state_store or WorkflowStateStore()
        self._runtime_by_workflow_id: dict[str, _WorkflowRuntime] = {}
        self._workflow_id_by_approval_id: dict[str, str] = {}
        self._startup_load_error: WorkflowStateStoreError | None = None
        self._restore_persisted_runtimes()

    def _restore_persisted_runtimes(self) -> None:
        try:
            states = self._workflow_state_store.list_states()
        except WorkflowStateStoreError as exc:
            self._startup_load_error = exc
            return
        for state in states:
            try:
                runtime = self._runtime_from_persisted_state(state)
            except Exception as exc:
                self._startup_load_error = WorkflowStateStoreError(
                    code="resume_not_possible",
                    workflow_id=state.workflow_id,
                    operation="restore_persisted_runtimes",
                    reason=str(exc) or "persisted workflow runtime could not be restored",
                )
                continue
            self._runtime_by_workflow_id[runtime.workflow.workflow_id] = runtime

    def _runtime_from_persisted_state(self, state: PersistedWorkflowState) -> _WorkflowRuntime:
        snapshot = dict(state.snapshot)
        workflow = OwnerWorkflow(**dict(_mapping(snapshot.get("workflow"))))
        steps = [
            OwnerWorkflowStep(**dict(_mapping(step_payload)))
            for step_payload in list(snapshot.get("steps", []))
        ]
        runtime_payload = _mapping(snapshot.get("runtime"))
        runtime = _WorkflowRuntime(
            owner_session=OwnerSession(**dict(_mapping(runtime_payload.get("owner_session")))),
            workflow=workflow,
            steps=list(steps),
            owner_domain=OwnerDomain(**dict(_mapping(runtime_payload.get("owner_domain")))),
            memory_scope=OwnerMemoryScope(**dict(_mapping(runtime_payload.get("memory_scope")))),
            action_scope=OwnerActionScope(**dict(_mapping(runtime_payload.get("action_scope")))),
            trust_profile=OwnerTrustProfile(**dict(_mapping(runtime_payload.get("trust_profile")))),
            detected_language=_normalize_text(runtime_payload.get("detected_language")) or "und",
            priority_class=_normalize_text(runtime_payload.get("priority_class")) or "medium",
            context_ref=_normalize_text(runtime_payload.get("context_ref")) or None,
            memory_records=[
                dict(item) if isinstance(item, Mapping) else {}
                for item in list(runtime_payload.get("memory_records", []))
            ],
            execution_mode=_normalize_text(runtime_payload.get("execution_mode")) or "live",
            workflow_metadata=_mapping(runtime_payload.get("workflow_metadata")),
        )
        pending_approval = _mapping(runtime_payload.get("pending_approval"))
        if pending_approval:
            approval = self._owner_orchestrator.hydrate_pending_approval_state(pending_approval)
            self._workflow_id_by_approval_id[approval.approval_id] = workflow.workflow_id
        return runtime

    def _runtime_snapshot(self, runtime: _WorkflowRuntime) -> dict[str, object]:
        current_step = self._current_step(runtime)
        pending_approval = None
        if current_step is not None and current_step.approval_id:
            pending_approval = self._owner_orchestrator.export_pending_approval_state(
                current_step.approval_id
            )
        return {
            "workflow": runtime.workflow.to_dict(),
            "steps": [step.to_dict() for step in runtime.steps],
            "runtime": {
                "owner_session": runtime.owner_session.to_dict(),
                "owner_domain": runtime.owner_domain.to_dict(),
                "memory_scope": runtime.memory_scope.to_dict(),
                "action_scope": runtime.action_scope.to_dict(),
                "trust_profile": runtime.trust_profile.to_dict(),
                "detected_language": runtime.detected_language,
                "priority_class": runtime.priority_class,
                "context_ref": runtime.context_ref,
                "memory_records": [
                    dict(record) if isinstance(record, Mapping) else {}
                    for record in runtime.memory_records
                ],
                "execution_mode": runtime.execution_mode,
                "workflow_metadata": dict(runtime.workflow_metadata),
                "pending_approval": pending_approval,
            },
            "current_step_index": -1 if current_step is None else current_step.sequence_index,
        }

    def _resume_reset_runtime(self, runtime: _WorkflowRuntime) -> _WorkflowRuntime:
        current_step = self._current_step(runtime)
        if current_step is not None and current_step.status == "running":
            runtime.steps[current_step.sequence_index] = update_owner_workflow_step(
                current_step,
                status="pending",
                updated_at=_utc_timestamp(),
            )
        if runtime.workflow.status == "running":
            runtime.workflow = update_owner_workflow(
                runtime.workflow,
                status="pending",
                updated_at=_utc_timestamp(),
            )
        return runtime

    def _persistence_failure_result(
        self,
        runtime: _WorkflowRuntime,
        *,
        exc: WorkflowStateStoreError,
        approval: OwnerApproval | None,
        current_step: OwnerWorkflowStep | None,
    ) -> OwnerWorkflowRunResult:
        timestamp = _utc_timestamp()
        failure_step = current_step
        if current_step is not None and current_step.status not in {"completed", "failed", "rejected"}:
            runtime.steps[current_step.sequence_index] = update_owner_workflow_step(
                current_step,
                status="failed",
                result_status="failed",
                outcome="non_retryable_failure",
                last_error={
                    "error_code": exc.code,
                    "error_message": exc.reason,
                },
                updated_at=timestamp,
            )
            failure_step = runtime.steps[current_step.sequence_index]
        runtime.workflow = update_owner_workflow(
            runtime.workflow,
            status="failed",
            current_step_id=None if failure_step is None else failure_step.step_id,
            final_result_summary=f"Workflow persistence failed: {exc.reason}",
            updated_at=timestamp,
        )
        return self._build_result(
            runtime,
            approval=approval,
            current_step=failure_step,
            durable=False,
        )

    def create_workflow(
        self,
        *,
        owner_id: object,
        workflow_type: object,
        workflow_payload: Mapping[str, object],
        owner_domain: OwnerDomain,
        memory_scope: OwnerMemoryScope,
        action_scope: OwnerActionScope,
        trust_profile: OwnerTrustProfile,
        title: object | None = None,
        session: OwnerSession | None = None,
        owner_session_id: object | None = None,
        active_language: object = "und",
        detected_language: object | None = None,
        priority_class: object = "medium",
        context_ref: object = None,
        memory_records: list[Mapping[str, object]] | None = None,
        execution_mode: str = "live",
        workflow_metadata: Mapping[str, object] | None = None,
    ) -> OwnerWorkflowRunResult:
        normalized_owner_id = _normalize_text(owner_id)
        owner_session = session or create_owner_session(
            owner_id=normalized_owner_id,
            active_language=active_language,
            context_ref=context_ref,
            owner_session_id=owner_session_id,
        )
        timestamp = _utc_timestamp()
        workflow = create_owner_workflow(
            owner_id=normalized_owner_id,
            workflow_type=workflow_type,
            title=title,
            created_at=timestamp,
            updated_at=timestamp,
        )
        steps = list(
            instantiate_workflow_steps(
                workflow_id=workflow.workflow_id,
                owner_id=normalized_owner_id,
                workflow_type=workflow.workflow_type,
                workflow_payload=workflow_payload,
                created_at=timestamp,
            )
        )
        workflow = update_owner_workflow(
            workflow,
            current_step_id=steps[0].step_id if steps else None,
            updated_at=timestamp,
        )
        runtime = _WorkflowRuntime(
            owner_session=owner_session,
            workflow=workflow,
            steps=steps,
            owner_domain=owner_domain,
            memory_scope=memory_scope,
            action_scope=action_scope,
            trust_profile=trust_profile,
            detected_language=_normalize_text(detected_language) or owner_session.active_language,
            priority_class=_normalize_text(priority_class) or "medium",
            context_ref=_normalize_text(context_ref) or owner_session.context_ref,
            memory_records=list(memory_records or []),
            execution_mode=_normalize_text(execution_mode) or "live",
            workflow_metadata=_mapping(workflow_metadata),
        )
        self._runtime_by_workflow_id[workflow.workflow_id] = runtime
        try:
            self._persist_workflow_state(runtime)
            return self._run_until_pause_or_terminal(runtime)
        except WorkflowStateStoreError as exc:
            return self._persistence_failure_result(
                runtime,
                exc=exc,
                approval=None,
                current_step=self._current_step(runtime),
            )

    def resume_workflow(self, workflow_id: object) -> OwnerWorkflowRunResult:
        normalized_workflow_id = _normalize_text(workflow_id)
        runtime = self._runtime_by_workflow_id.get(normalized_workflow_id)
        if runtime is None:
            try:
                state = self._workflow_state_store.load_state(normalized_workflow_id)
            except WorkflowStateStoreError as exc:
                raise ValueError(f"workflow could not be resumed: {exc.reason}") from exc
            if state is None:
                raise ValueError("workflow was not found")
            runtime = self._runtime_from_persisted_state(state)
            self._runtime_by_workflow_id[normalized_workflow_id] = runtime
        if runtime.workflow.status in {"blocked", "completed", "failed", "rejected", "partial_failure"}:
            return self._build_result(
                runtime,
                approval=self._approval_for_step(self._current_step(runtime)),
                current_step=self._current_step(runtime),
            )
        try:
            self._resume_reset_runtime(runtime)
            self._persist_workflow_state(runtime)
            return self._run_until_pause_or_terminal(runtime)
        except WorkflowStateStoreError as exc:
            return self._persistence_failure_result(
                runtime,
                exc=exc,
                approval=self._approval_for_step(self._current_step(runtime)),
                current_step=self._current_step(runtime),
            )

    def approve_step(self, approval_id: object) -> OwnerWorkflowRunResult:
        runtime = self._runtime_for_approval(approval_id)
        if runtime is None:
            raise ValueError("workflow approval was not found")
        try:
            step = self._current_step(runtime)
            if step is None:
                return self._build_result(runtime, approval=None, current_step=None)
            if step.status != "awaiting_approval" or step.approval_id != _normalize_text(approval_id):
                return self._build_result(runtime, approval=None, current_step=step)

            resolution = self._owner_orchestrator.approve_action(approval_id)
            self._workflow_id_by_approval_id.pop(_normalize_text(approval_id), None)
            runtime.steps[step.sequence_index] = self._step_from_resolution(
                runtime.steps[step.sequence_index],
                action_id=_normalize_text(
                    None if resolution.action_contract is None else resolution.action_contract.get("action_id")
                ),
                approval_id=_normalize_text(approval_id) or step.approval_id,
                result_status=(
                    None if resolution.action_result is None else _normalize_text(resolution.action_result.get("status"))
                ),
                result_summary=resolution.response_plan.summary_text,
                action_result=resolution.action_result,
            )
            runtime.workflow = update_owner_workflow(
                runtime.workflow,
                last_action_id=runtime.steps[step.sequence_index].action_id,
                last_approval_id=runtime.steps[step.sequence_index].approval_id,
                updated_at=_utc_timestamp(),
            )
            if resolution.action_result is None:
                runtime.workflow = update_owner_workflow(
                    runtime.workflow,
                    status="blocked",
                    final_result_summary=resolution.response_plan.summary_text,
                    updated_at=_utc_timestamp(),
                )
                self._persist_workflow_state(runtime)
                return self._build_result(
                    runtime,
                    approval=resolution.approval,
                    current_step=runtime.steps[step.sequence_index],
                )
            step_result_status = _normalize_text(resolution.action_result.get("status")).lower()
            if step_result_status == "success":
                runtime.steps[step.sequence_index] = update_owner_workflow_step(
                    runtime.steps[step.sequence_index],
                    status="completed",
                    outcome="success",
                    updated_at=_utc_timestamp(),
                )
                runtime.workflow = update_owner_workflow(
                    runtime.workflow,
                    status="running",
                    completed_step_count=self._completed_step_count(runtime.steps),
                    current_step_id=self._next_pending_step_id(runtime.steps),
                    final_result_summary=resolution.response_plan.summary_text,
                    updated_at=_utc_timestamp(),
                )
                self._persist_workflow_state(runtime)
                return self._run_until_pause_or_terminal(runtime)

            runtime.steps[step.sequence_index] = update_owner_workflow_step(
                runtime.steps[step.sequence_index],
                status="failed",
                outcome="non_retryable_failure",
                updated_at=_utc_timestamp(),
            )
            runtime.workflow = update_owner_workflow(
                runtime.workflow,
                status="failed",
                current_step_id=runtime.steps[step.sequence_index].step_id,
                final_result_summary=resolution.response_plan.summary_text,
                updated_at=_utc_timestamp(),
            )
            self._persist_workflow_state(runtime)
            return self._build_result(
                runtime,
                approval=resolution.approval,
                current_step=runtime.steps[step.sequence_index],
            )
        except WorkflowStateStoreError as exc:
            return self._persistence_failure_result(
                runtime,
                exc=exc,
                approval=self._approval_for_step(self._current_step(runtime)),
                current_step=self._current_step(runtime),
            )

    def reject_step(self, approval_id: object) -> OwnerWorkflowRunResult:
        runtime = self._runtime_for_approval(approval_id)
        if runtime is None:
            raise ValueError("workflow approval was not found")
        try:
            step = self._current_step(runtime)
            if step is None:
                return self._build_result(runtime, approval=None, current_step=None)
            if step.status != "awaiting_approval" or step.approval_id != _normalize_text(approval_id):
                return self._build_result(runtime, approval=None, current_step=step)

            resolution = self._owner_orchestrator.reject_action(approval_id)
            self._workflow_id_by_approval_id.pop(_normalize_text(approval_id), None)
            runtime.steps[step.sequence_index] = update_owner_workflow_step(
                step,
                status="rejected",
                approval_id=_normalize_text(approval_id) or step.approval_id,
                result_status="blocked",
                outcome="approval_rejected",
                result_summary=resolution.response_plan.summary_text,
                last_error={
                    "error_code": "approval_rejected",
                    "error_message": resolution.response_plan.summary_text,
                },
                updated_at=_utc_timestamp(),
            )
            runtime.workflow = update_owner_workflow(
                runtime.workflow,
                status="rejected",
                current_step_id=runtime.steps[step.sequence_index].step_id,
                last_approval_id=runtime.steps[step.sequence_index].approval_id,
                final_result_summary=resolution.response_plan.summary_text,
                updated_at=_utc_timestamp(),
            )
            self._persist_workflow_state(runtime)
            return self._build_result(
                runtime,
                approval=resolution.approval,
                current_step=runtime.steps[step.sequence_index],
            )
        except WorkflowStateStoreError as exc:
            return self._persistence_failure_result(
                runtime,
                exc=exc,
                approval=self._approval_for_step(self._current_step(runtime)),
                current_step=self._current_step(runtime),
            )

    def _runtime_for_approval(self, approval_id: object) -> _WorkflowRuntime | None:
        normalized_approval_id = _normalize_text(approval_id)
        workflow_id = self._workflow_id_by_approval_id.get(normalized_approval_id)
        if not workflow_id:
            for candidate in self._runtime_by_workflow_id.values():
                for step in candidate.steps:
                    if step.approval_id == normalized_approval_id:
                        return candidate
            return None
        return self._runtime_by_workflow_id.get(workflow_id)

    def _current_step(self, runtime: _WorkflowRuntime) -> OwnerWorkflowStep | None:
        current_step_id = runtime.workflow.current_step_id
        for step in runtime.steps:
            if step.step_id == current_step_id:
                return step
        for step in runtime.steps:
            if step.status == "pending":
                return step
        return None

    def _run_until_pause_or_terminal(self, runtime: _WorkflowRuntime) -> OwnerWorkflowRunResult:
        while True:
            step = self._current_step(runtime)
            if step is None:
                runtime.workflow = update_owner_workflow(
                    runtime.workflow,
                    status="completed",
                    current_step_id=None,
                    completed_step_count=self._completed_step_count(runtime.steps),
                    final_result_summary=runtime.workflow.final_result_summary or self._workflow_summary(runtime),
                    updated_at=_utc_timestamp(),
                )
                self._persist_workflow_state(runtime)
                return self._build_result(runtime, approval=None, current_step=None)

            if step.status != "pending":
                if runtime.workflow.status in {"blocked", "failed", "completed", "rejected"}:
                    self._persist_workflow_state(runtime)
                    return self._build_result(
                        runtime,
                        approval=self._approval_for_step(step),
                        current_step=step,
                    )
                runtime.workflow = update_owner_workflow(
                    runtime.workflow,
                    current_step_id=self._next_pending_step_id(runtime.steps),
                    updated_at=_utc_timestamp(),
                )
                self._persist_workflow_state(runtime)
                continue

            runtime.workflow = update_owner_workflow(
                runtime.workflow,
                status="running",
                current_step_id=step.step_id,
                updated_at=_utc_timestamp(),
            )
            try:
                resolved_action_parameters = self._resolved_step_parameters(step, runtime.steps)
            except ValueError as exc:
                runtime.steps[step.sequence_index] = update_owner_workflow_step(
                    runtime.steps[step.sequence_index],
                    status="running",
                    updated_at=_utc_timestamp(),
                )
                self._persist_workflow_state(runtime)
                runtime.steps[step.sequence_index] = update_owner_workflow_step(
                    runtime.steps[step.sequence_index],
                    status="failed",
                    result_status="failed",
                    outcome="non_retryable_failure",
                    last_error={
                        "error_code": "validation_error",
                        "error_message": str(exc),
                    },
                    updated_at=_utc_timestamp(),
                )
                runtime.workflow = update_owner_workflow(
                    runtime.workflow,
                    status="failed",
                    current_step_id=step.step_id,
                    final_result_summary=str(exc),
                    updated_at=_utc_timestamp(),
                )
                self._persist_workflow_state(runtime)
                return self._build_result(
                    runtime,
                    approval=None,
                    current_step=runtime.steps[step.sequence_index],
                )
            runtime.steps[step.sequence_index] = update_owner_workflow_step(
                runtime.steps[step.sequence_index],
                action_parameters=resolved_action_parameters,
                status="running",
                updated_at=_utc_timestamp(),
            )
            self._persist_workflow_state(runtime)
            interaction = self._owner_orchestrator.process_request(
                request_text=step.request_text,
                owner_id=runtime.workflow.owner_id,
                owner_domain=runtime.owner_domain,
                memory_scope=runtime.memory_scope,
                action_scope=runtime.action_scope,
                trust_profile=runtime.trust_profile,
                session=runtime.owner_session,
                detected_language=runtime.detected_language,
                priority_class=runtime.priority_class,
                context_ref=runtime.context_ref,
                scenario_type=runtime.workflow_metadata.get("scenario_type"),
                execution_mode=runtime.execution_mode,
                memory_records=runtime.memory_records,
                action_type=step.action_type,
                target_type=step.target_type,
                target_ref=step.target_ref,
                action_parameters=resolved_action_parameters,
                workflow_id=runtime.workflow.workflow_id,
                workflow_step_id=step.step_id,
                max_retries=step.max_retries,
                step_timeout_seconds=step.timeout_seconds,
            )
            runtime.owner_session = interaction.session
            action_id = _normalize_text(
                None if interaction.action_contract is None else interaction.action_contract.get("action_id")
            ) or None
            approval_id = None if interaction.approval is None else interaction.approval.approval_id
            result_status = (
                None if interaction.action_result is None else _normalize_text(interaction.action_result.get("status"))
            )
            runtime.steps[step.sequence_index] = self._step_from_resolution(
                runtime.steps[step.sequence_index],
                action_id=action_id,
                approval_id=approval_id,
                result_status=result_status,
                result_summary=interaction.response_plan.summary_text,
                action_result=interaction.action_result,
            )
            runtime.workflow = update_owner_workflow(
                runtime.workflow,
                last_action_id=action_id,
                last_approval_id=approval_id,
                updated_at=_utc_timestamp(),
            )

            if interaction.approval is not None and interaction.action_result is None:
                runtime.steps[step.sequence_index] = update_owner_workflow_step(
                    runtime.steps[step.sequence_index],
                    status="awaiting_approval",
                    updated_at=_utc_timestamp(),
                )
                runtime.workflow = update_owner_workflow(
                    runtime.workflow,
                    status="blocked",
                    current_step_id=step.step_id,
                    final_result_summary=interaction.response_plan.summary_text,
                    updated_at=_utc_timestamp(),
                )
                self._workflow_id_by_approval_id[interaction.approval.approval_id] = runtime.workflow.workflow_id
                self._persist_workflow_state(runtime)
                return self._build_result(
                    runtime,
                    approval=interaction.approval,
                    current_step=runtime.steps[step.sequence_index],
                )

            if interaction.action_result is None:
                runtime.steps[step.sequence_index] = update_owner_workflow_step(
                    runtime.steps[step.sequence_index],
                    status="failed",
                    outcome="non_retryable_failure",
                    updated_at=_utc_timestamp(),
                )
                runtime.workflow = update_owner_workflow(
                    runtime.workflow,
                    status="failed",
                    current_step_id=step.step_id,
                    final_result_summary=interaction.response_plan.summary_text,
                    updated_at=_utc_timestamp(),
                )
                self._persist_workflow_state(runtime)
                return self._build_result(runtime, approval=None, current_step=runtime.steps[step.sequence_index])

            normalized_result_status = _normalize_text(interaction.action_result.get("status")).lower()
            if normalized_result_status == "success":
                runtime.steps[step.sequence_index] = update_owner_workflow_step(
                    runtime.steps[step.sequence_index],
                    status="completed",
                    outcome="success",
                    updated_at=_utc_timestamp(),
                )
                runtime.workflow = update_owner_workflow(
                    runtime.workflow,
                    status="running",
                    current_step_id=self._next_pending_step_id(runtime.steps),
                    completed_step_count=self._completed_step_count(runtime.steps),
                    final_result_summary=interaction.response_plan.summary_text,
                    updated_at=_utc_timestamp(),
                )
                self._persist_workflow_state(runtime)
                continue

            runtime.steps[step.sequence_index] = update_owner_workflow_step(
                runtime.steps[step.sequence_index],
                status="failed",
                outcome="non_retryable_failure",
                updated_at=_utc_timestamp(),
            )
            runtime.workflow = update_owner_workflow(
                runtime.workflow,
                status="failed",
                current_step_id=step.step_id,
                final_result_summary=interaction.response_plan.summary_text,
                updated_at=_utc_timestamp(),
            )
            self._persist_workflow_state(runtime)
            return self._build_result(runtime, approval=None, current_step=runtime.steps[step.sequence_index])

    def _approval_for_step(self, step: OwnerWorkflowStep | None) -> OwnerApproval | None:
        if step is None or not step.approval_id:
            return None
        return self._owner_orchestrator.approval_store.get(step.approval_id)

    def _step_from_resolution(
        self,
        step: OwnerWorkflowStep,
        *,
        action_id: str | None,
        approval_id: str | None,
        result_status: str | None,
        result_summary: str,
        action_result: Mapping[str, object] | None,
    ) -> OwnerWorkflowStep:
        return update_owner_workflow_step(
            step,
            action_id=action_id,
            approval_id=approval_id,
            result_status=result_status,
            result_summary=result_summary,
            result_payload=None if action_result is None else _mapping(action_result.get("payload")),
            attempt_count=_result_attempt_count(action_result),
            last_error=_result_last_error(action_result),
            retry_status=_result_retry_status(action_result),
            updated_at=_utc_timestamp(),
        )

    def _next_pending_step_id(self, steps: list[OwnerWorkflowStep]) -> str | None:
        for step in steps:
            if step.status == "pending":
                return step.step_id
        return None

    def _completed_step_count(self, steps: list[OwnerWorkflowStep]) -> int:
        return len([step for step in steps if step.status == "completed"])

    def _resolved_step_parameters(
        self,
        step: OwnerWorkflowStep,
        steps: list[OwnerWorkflowStep],
    ) -> dict[str, object]:
        def resolve_result_payload(
            source_step: OwnerWorkflowStep,
            path: str,
        ) -> object:
            if source_step.result_payload is None:
                raise ValueError(f"validation_error: missing result payload for {source_step.step_id}")
            resolved: object = dict(source_step.result_payload)
            for key in path.split("."):
                if not isinstance(resolved, Mapping) or key not in resolved:
                    raise ValueError(
                        f"validation_error: unresolved step reference step{source_step.sequence_index + 1}.{path}"
                    )
                resolved = resolved[key]
            return resolved

        def resolve(value: object) -> object:
            if isinstance(value, str):
                match = _STEP_REFERENCE_PATTERN.fullmatch(value.strip())
                if match is None:
                    return value
                sequence_index = int(match.group("index")) - 1
                if sequence_index < 0 or sequence_index >= len(steps):
                    raise ValueError(f"validation_error: unresolved step reference {value}")
                source_step = steps[sequence_index]
                path = match.group("path")
                if path == "result_summary":
                    if not source_step.result_summary:
                        raise ValueError(
                            f"validation_error: missing result summary for {source_step.step_id}"
                        )
                    return source_step.result_summary
                if path.startswith("result_payload."):
                    return resolve_result_payload(source_step, path.removeprefix("result_payload."))
                raise ValueError(f"validation_error: unsupported step reference {value}")
            if isinstance(value, Mapping):
                return {str(key): resolve(item) for key, item in dict(value).items()}
            if isinstance(value, list):
                return [resolve(item) for item in value]
            return value

        resolved = resolve(step.action_parameters)
        return dict(resolved) if isinstance(resolved, dict) else dict(step.action_parameters)

    def _workflow_summary(self, runtime: _WorkflowRuntime) -> str:
        current_step = self._current_step(runtime)
        if runtime.workflow.status == "completed":
            return (
                f"Workflow completed. {runtime.workflow.completed_step_count}/{len(runtime.steps)} "
                "steps finished."
            )
        if (
            runtime.workflow.status == "blocked"
            and current_step is not None
            and current_step.status == "awaiting_approval"
        ):
            return f"Workflow is waiting for approval on step {current_step.title}."
        if runtime.workflow.status == "blocked" and current_step is not None:
            return f"Workflow blocked at step {current_step.title}."
        if runtime.workflow.status == "failed" and current_step is not None:
            return f"Workflow failed at step {current_step.title}."
        if runtime.workflow.status == "rejected" and current_step is not None:
            return f"Workflow rejected at step {current_step.title}."
        return f"Workflow is {runtime.workflow.status}."

    def _workflow_visibility_metadata(
        self,
        runtime: _WorkflowRuntime,
        *,
        trace_artifact_path: str,
        evidence_artifact_path: str,
    ) -> dict[str, object]:
        current_step = self._current_step(runtime)
        return {
            "workflow_id": runtime.workflow.workflow_id,
            "workflow_type": runtime.workflow.workflow_type,
            "scenario_type": _normalize_text(runtime.workflow_metadata.get("scenario_type")) or None,
            "workflow_status": runtime.workflow.status,
            "current_step_id": None if current_step is None else current_step.step_id,
            "owner_id": runtime.workflow.owner_id,
            "total_steps": len(runtime.steps),
            "completed_steps": runtime.workflow.completed_step_count,
            "trace_artifact_path": trace_artifact_path,
            "evidence_artifact_path": evidence_artifact_path,
            "steps": [summarize_owner_workflow_step(step) for step in runtime.steps],
        }

    def _preview_text_for_step(self, step: OwnerWorkflowStep | None) -> str | None:
        if step is None:
            return None
        if step.action_type != "PRINT_DOCUMENT":
            return f"Current step: {step.title}"
        parameters = _mapping(step.action_parameters)
        title = _normalize_text(parameters.get("document_title")) or "Untitled Document"
        document_text = _normalize_text(parameters.get("document_text"))
        if len(document_text) > 180:
            document_text = document_text[:177].rstrip() + "..."
        if document_text:
            return f"Preview ready: {title}\n\n{document_text}"
        return f"Preview ready: {title}"

    def _build_response_plan(
        self,
        runtime: _WorkflowRuntime,
        *,
        trace_artifact_path: str,
        evidence_artifact_path: str,
        trace_metadata: Mapping[str, object],
    ) -> OwnerResponsePlan:
        current_step = self._current_step(runtime)
        approval = None if current_step is None else self._approval_for_step(current_step)
        risk_profile = None
        if current_step is not None:
            risk_profile = classify_action_risk(
                current_step.action_type,
                action_scope=runtime.action_scope,
                action_parameters=current_step.action_parameters,
            )
        awaiting_approval = bool(current_step is not None and current_step.status == "awaiting_approval")
        return create_owner_response_plan(
            owner_session_id=runtime.owner_session.owner_session_id,
            owner_id=runtime.workflow.owner_id,
            response_type=(
                "confirmation_request"
                if awaiting_approval
                else ("error" if runtime.workflow.status in {"blocked", "failed", "rejected"} else "summary_text")
            ),
            target_language=runtime.detected_language,
            summary_text=runtime.workflow.final_result_summary or self._workflow_summary(runtime),
            action_refs=[step.action_id for step in runtime.steps if step.action_id],
            requires_confirmation=awaiting_approval,
            requires_high_trust=bool(risk_profile is not None and risk_profile.requires_high_trust),
            approval_id=None if approval is None else approval.approval_id,
            trust_class=None if risk_profile is None else risk_profile.trust_class,
            preview_text=self._preview_text_for_step(current_step),
            metadata={
                "workflow": self._workflow_visibility_metadata(
                    runtime,
                    trace_artifact_path=trace_artifact_path,
                    evidence_artifact_path=evidence_artifact_path,
                ),
                "trace_metadata": dict(trace_metadata),
            },
            status=(
                "planned"
                if runtime.workflow.status in {"running", "completed"} or awaiting_approval
                else "blocked"
            ),
        )

    def _trace_payload(self, runtime: _WorkflowRuntime) -> dict[str, object]:
        current_step = self._current_step(runtime)
        return {
            "type": OWNER_WORKFLOW_TRACE_ARTIFACT_TYPE,
            "scenario_type": _normalize_text(runtime.workflow_metadata.get("scenario_type")) or None,
            "workflow_id": runtime.workflow.workflow_id,
            "step_id": None if current_step is None else current_step.step_id,
            "owner_id": runtime.workflow.owner_id,
            "workflow_type": runtime.workflow.workflow_type,
            "action_id": None if current_step is None else current_step.action_id,
            "approval_id": None if current_step is None else current_step.approval_id,
            "step_status": None if current_step is None else current_step.status,
            "attempt_count": None if current_step is None else current_step.attempt_count,
            "retry_status": None if current_step is None else current_step.retry_status,
            "error_code": (
                None
                if current_step is None or current_step.last_error is None
                else current_step.last_error.get("error_code") or None
            ),
            "workflow_status": runtime.workflow.status,
            "created_at": runtime.workflow.created_at,
            "updated_at": runtime.workflow.updated_at,
            "step_summaries": [summarize_owner_workflow_step(step) for step in runtime.steps],
        }

    def _evidence_payload(self, runtime: _WorkflowRuntime) -> dict[str, object]:
        return {
            "scenario_type": _normalize_text(runtime.workflow_metadata.get("scenario_type")) or None,
            "workflow_id": runtime.workflow.workflow_id,
            "owner_id": runtime.workflow.owner_id,
            "workflow_type": runtime.workflow.workflow_type,
            "workflow_status": runtime.workflow.status,
            "workflow_summary": runtime.workflow.final_result_summary or self._workflow_summary(runtime),
            "step_summaries": [summarize_owner_workflow_step(step) for step in runtime.steps],
            "approvals": [
                {
                    "step_id": step.step_id,
                    "approval_id": step.approval_id,
                    "status": step.status,
                }
                for step in runtime.steps
                if step.approval_id
            ],
            "final_result_summary": runtime.workflow.final_result_summary,
        }

    def _should_persist_terminal_evidence(self, runtime: _WorkflowRuntime) -> bool:
        if runtime.workflow.status in {"completed", "failed", "rejected", "partial_failure"}:
            return True
        current_step = self._current_step(runtime)
        if runtime.workflow.status != "blocked":
            return False
        return current_step is None or current_step.status != "awaiting_approval"

    def _persist_workflow_state(
        self,
        runtime: _WorkflowRuntime,
        *,
        durable: bool = True,
    ) -> tuple[str, str, dict[str, object]]:
        if durable:
            self._workflow_state_store.save_state(self._runtime_snapshot(runtime))
        trace_payload = self._trace_payload(runtime)
        trace_path = ROOT_DIR / OWNER_WORKFLOW_TRACE_DIR / f"{_safe_filename(runtime.workflow.workflow_id)}.json"
        trace_path.parent.mkdir(parents=True, exist_ok=True)
        trace_object = make_trace_object(
            id=runtime.workflow.workflow_id,
            domain=OWNERBOX_DOMAIN_TYPE,
            payload=trace_payload,
            local_path=trace_path,
            artifact_type=OWNER_WORKFLOW_TRACE_ARTIFACT_TYPE,
            logical_key=compute_artifact_key(
                OWNERBOX_DOMAIN_TYPE,
                OWNER_WORKFLOW_TRACE_ARTIFACT_TYPE,
                runtime.workflow.workflow_id,
            ),
            refs=[
                f"workflow:{runtime.workflow.workflow_id}",
                f"owner:{runtime.workflow.owner_id}",
            ],
            tags=["ownerbox", "workflow", runtime.workflow.workflow_type, runtime.workflow.status],
        )
        trace_path.write_text(
            json.dumps(trace_object.to_dict(), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        register_artifact(
            trace_object.id,
            OWNERBOX_DOMAIN_TYPE,
            OWNER_WORKFLOW_TRACE_ARTIFACT_TYPE,
            trace_path,
            logical_key=trace_object.logical_key,
            refs=list(trace_object.refs),
            tags=list(trace_object.tags),
            payload=trace_payload,
            memory_class="trace",
            truth_level="working",
            execution_role="evidence",
            created_at=trace_object.created_at,
            updated_at=trace_object.updated_at,
        )
        log_event("trace", trace_payload, task_id=runtime.workflow.workflow_id, status=runtime.workflow.status)

        evidence_path = ROOT_DIR / OWNER_WORKFLOW_EVIDENCE_DIR / f"{_safe_filename(runtime.workflow.workflow_id)}.json"
        if self._should_persist_terminal_evidence(runtime):
            evidence_payload = self._evidence_payload(runtime)
            evidence_path.parent.mkdir(parents=True, exist_ok=True)
            evidence_object = make_artifact_object(
                id=runtime.workflow.workflow_id,
                domain=OWNERBOX_DOMAIN_TYPE,
                payload=evidence_payload,
                local_path=evidence_path,
                artifact_type=OWNER_WORKFLOW_EVIDENCE_ARTIFACT_TYPE,
                logical_key=compute_artifact_key(
                    OWNERBOX_DOMAIN_TYPE,
                    OWNER_WORKFLOW_EVIDENCE_ARTIFACT_TYPE,
                    runtime.workflow.workflow_id,
                ),
                refs=[
                    f"workflow:{runtime.workflow.workflow_id}",
                    f"owner:{runtime.workflow.owner_id}",
                ],
                tags=["ownerbox", "workflow", "evidence", runtime.workflow.status],
                execution_role="evidence",
                truth_level="working",
                created_at=runtime.workflow.created_at,
                updated_at=runtime.workflow.updated_at,
            )
            evidence_path.write_text(
                json.dumps(evidence_object.to_dict(), indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            register_artifact(
                evidence_object.id,
                OWNERBOX_DOMAIN_TYPE,
                OWNER_WORKFLOW_EVIDENCE_ARTIFACT_TYPE,
                evidence_path,
                logical_key=evidence_object.logical_key,
                refs=list(evidence_object.refs),
                tags=list(evidence_object.tags),
                payload=evidence_payload,
                memory_class="evidence",
                truth_level="working",
                execution_role="evidence",
                created_at=evidence_object.created_at,
                updated_at=evidence_object.updated_at,
            )
            return str(trace_path), str(evidence_path), trace_payload
        return str(trace_path), "", trace_payload

    def _build_result(
        self,
        runtime: _WorkflowRuntime,
        *,
        approval: OwnerApproval | None,
        current_step: OwnerWorkflowStep | None,
        durable: bool = False,
    ) -> OwnerWorkflowRunResult:
        trace_artifact_path, evidence_artifact_path, trace_metadata = self._persist_workflow_state(
            runtime,
            durable=durable,
        )
        response_plan = self._build_response_plan(
            runtime,
            trace_artifact_path=trace_artifact_path,
            evidence_artifact_path=evidence_artifact_path,
            trace_metadata=trace_metadata,
        )
        return OwnerWorkflowRunResult(
            owner_session=runtime.owner_session,
            workflow=runtime.workflow,
            steps=tuple(runtime.steps),
            response_plan=response_plan,
            trace_metadata=dict(trace_metadata),
            approval=approval,
            current_step=current_step,
        )
