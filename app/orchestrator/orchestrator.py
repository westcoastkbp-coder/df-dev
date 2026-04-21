import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

from pydantic import BaseModel, field_validator, model_validator

from agents.engine import run_agent
from app.execution.linear_workflow import (
    format_linear_task_reference,
    linear_status_for_phase,
    next_step_for_phase,
    normalize_done_condition_met,
    normalize_mvp_priority,
    normalize_task_intake_payload,
    priority_for_mvp_priority,
    validate_task_intake_contract,
    workflow_phase_for_logical_role,
)
from app.execution.business_signal import (
    BusinessSignal,
    Decision,
    build_business_signal,
    build_decision_from_business_signal,
    decision_to_task_input,
)
from app.execution.external_actions import MAKE_CALL, SEND_EMAIL, SEND_SMS, execute_external
from app.execution.policy_guard import PolicyViolationError, load_validated_system_context
from app.execution.paths import OUTPUT_DIR, TASKS_FILE
from app.execution.role_routing import build_routed_steps
from memory.memory_store import build_agent_context, build_assistant_context
from memory.storage import (
    find_contact,
    find_task_record_by_linear_task_id,
    load_memory_records,
    load_recent_memory_records,
    save_task_record,
)
from app.orchestrator.task_factory import (
    apply_task_approval,
    append_history_event,
    create_task as create_managed_task,
    get_task as get_stored_task,
    next_task_id,
    now,
    policy_input_from_task_input,
    save_task as save_managed_task,
    transition_task_status,
)
from app.orchestrator.task_queue import task_queue

DEFAULT_TIMEOUT_SECONDS = 30.0
DEFAULT_MAX_RETRY = 1


class ExecutionTimeoutError(RuntimeError):
    pass


def _standard_error_response(
    *,
    error_type: str,
    error_message: str,
    recoverable: bool,
    task_id: str = "",
    extra: dict[str, object] | None = None,
) -> dict[str, object]:
    response = {
        "status": "error",
        "error_type": str(error_type).strip() or "UNKNOWN_ERROR",
        "error_message": str(error_message).strip() or "unknown error",
        "recoverable": bool(recoverable),
    }
    normalized_task_id = str(task_id or "").strip()
    if normalized_task_id:
        response["task_id"] = normalized_task_id
    if extra:
        response.update(dict(extra))
    return response


class TaskRequest(BaseModel):
    intent: str = "run_task"
    payload: dict[str, object] = {}
    goal: str = ""
    context: str = ""
    depends_on: str = ""
    constraints: str = ""
    expected_output: str = ""
    change_type: str = "review"
    priority: str = "medium"
    client_id: str = "default"
    contact_id: str = ""
    source_channel: str = ""
    linear_task_id: str
    linear_task_title: str
    mvp_priority: str
    expected_result: str
    done_condition: str
    done_condition_met: bool = False
    entry_point: str = ""
    confirmed: bool = False
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS
    max_retry: int = DEFAULT_MAX_RETRY

    @field_validator(
        "linear_task_id",
        "linear_task_title",
        "expected_result",
        "done_condition",
    )
    @classmethod
    def ensure_non_empty_contract_field(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("Task Intake Contract fields must not be empty")
        return normalized

    @field_validator("mvp_priority")
    @classmethod
    def ensure_valid_mvp_priority(cls, value: str) -> str:
        return normalize_mvp_priority(value)

    @model_validator(mode="after")
    def apply_linear_defaults(self) -> "TaskRequest":
        if not self.goal.strip():
            self.goal = self.linear_task_title
        if not self.expected_output.strip():
            self.expected_output = self.expected_result
        if not self.priority.strip():
            self.priority = priority_for_mvp_priority(self.mvp_priority)
        self.done_condition_met = normalize_done_condition_met(self.done_condition_met)
        return self


def create_task_from_business_signal(
    signal_payload: dict[str, object] | BusinessSignal,
) -> dict[str, object]:
    signal = (
        signal_payload
        if isinstance(signal_payload, BusinessSignal)
        else build_business_signal(signal_payload)
    )
    decision = build_decision_from_business_signal(signal)
    return create_task_from_decision(signal, decision)


def create_task_from_decision(
    signal_payload: dict[str, object] | BusinessSignal,
    decision_payload: dict[str, object] | Decision,
) -> dict[str, object]:
    signal = (
        signal_payload
        if isinstance(signal_payload, BusinessSignal)
        else build_business_signal(signal_payload)
    )
    decision = (
        decision_payload
        if isinstance(decision_payload, Decision)
        else build_decision_from_business_signal(signal)
    )
    if decision.execution_mode == "strict":
        return {
            "task_created": False,
            "approval_flow": "decision -> approval -> task",
            "task_status_changes": [],
            "integration_point": "app.orchestrator.orchestrator.create_task_from_decision",
            "approval_status": "pending",
            "execution_mode": decision.execution_mode,
            "decision": decision.to_dict(),
        }
    task_input = decision_to_task_input(signal, decision, source="internal")
    task_data = create_managed_task(task_input, store_path=TASKS_FILE)
    task_data["intent"] = str(task_input.get("intent", "")).strip()
    task_data["payload"] = dict(task_input.get("payload", {}))
    task_data["task_type"] = str(task_input.get("task_type", "")).strip()
    task_data["execution_mode"] = str(task_input.get("execution_mode", "")).strip()
    task_data["approval_status"] = str(task_input.get("approval_status", "")).strip()
    task_data["notes"] = list(task_input.get("notes", []))
    return save_managed_task(task_data, store_path=TASKS_FILE)


def approve_decision_task_creation(
    signal_payload: dict[str, object] | BusinessSignal,
    decision_payload: dict[str, object] | Decision,
    *,
    approved: bool,
    approved_by: str,
) -> dict[str, object]:
    signal = (
        signal_payload
        if isinstance(signal_payload, BusinessSignal)
        else build_business_signal(signal_payload)
    )
    decision = (
        decision_payload
        if isinstance(decision_payload, Decision)
        else build_decision_from_business_signal(signal)
    )
    if decision.execution_mode != "strict":
        raise ValueError("approve_decision_task_creation is only required for strict decisions")
    if not approved:
        return {
            "task_created": False,
            "approval_flow": "decision -> approval -> task",
            "task_status_changes": [],
            "integration_point": "app.orchestrator.orchestrator.approve_decision_task_creation",
            "approval_status": "rejected",
            "approved_by": str(approved_by or "").strip(),
            "execution_mode": decision.execution_mode,
            "decision": decision.to_dict(),
        }

    task_input = decision_to_task_input(signal, decision, source="internal")
    task_input["approval_status"] = "approved"
    task_input["approved_by"] = str(approved_by or "").strip()
    task_input["approved_at"] = now()
    task_data = create_managed_task(task_input, store_path=TASKS_FILE)
    task_data["intent"] = str(task_input.get("intent", "")).strip()
    task_data["payload"] = dict(task_input.get("payload", {}))
    task_data["task_type"] = str(task_input.get("task_type", "")).strip()
    task_data["execution_mode"] = str(task_input.get("execution_mode", "")).strip()
    task_data["approval_status"] = "approved"
    task_data["approved_by"] = str(task_input.get("approved_by", "")).strip()
    task_data["approved_at"] = str(task_input.get("approved_at", "")).strip()
    task_data["notes"] = list(task_input.get("notes", []))
    return save_managed_task(task_data, store_path=TASKS_FILE)


def approve_task_for_execution(
    task_id: str,
    *,
    approved: bool,
    approved_by: str,
) -> dict[str, object]:
    return apply_task_approval(
        task_id,
        approved=approved,
        approved_by=approved_by,
        store_path=TASKS_FILE,
    )


def build_steps(change_type: str = "review"):
    return build_routed_steps(change_type)


def find_step_by_role(steps: list[dict], role: str) -> dict | None:
    for step in steps:
        if step.get("role") == role:
            return step

    return None


def ensure_storage() -> None:
    os.makedirs(TASKS_FILE.parent, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not TASKS_FILE.exists():
        with open(TASKS_FILE, "w", encoding="utf-8") as f:
            json.dump([], f)


def load_tasks():
    ensure_storage()
    try:
        with open(TASKS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return []


def save_tasks(tasks) -> None:
    with open(TASKS_FILE, "w", encoding="utf-8") as f:
        json.dump(tasks, f, indent=2)


def load_memory():
    return load_memory_records()


def create_task(task: TaskRequest) -> dict:
    if str(task.entry_point or "").strip().lower() != "secretary":
        raise ValueError("secretary entry required for task creation")
    if not bool(task.confirmed):
        raise ValueError("task confirmation required before task creation")

    intake_status = linear_status_for_phase(
        "intake",
        done_condition_met=task.done_condition_met,
    )
    task_payload = {
        "client_id": task.client_id,
        "contact_id": task.contact_id,
        "source_channel": task.source_channel,
        "intent": task.intent,
        "goal": task.goal,
        "context": task.context,
        "base_context": task.context,
        "resolved_context": task.context,
        "depends_on": task.depends_on,
        "constraints": task.constraints,
        "expected_output": task.expected_output,
        "change_type": task.change_type,
        "priority": task.priority,
        "linear_task_id": task.linear_task_id,
        "linear_task_title": task.linear_task_title,
        "mvp_priority": task.mvp_priority,
        "expected_result": task.expected_result,
        "done_condition": task.done_condition,
        "done_condition_met": task.done_condition_met,
        "entry_point": task.entry_point,
        "confirmed": bool(task.confirmed),
        "timeout_seconds": max(0.01, float(task.timeout_seconds or DEFAULT_TIMEOUT_SECONDS)),
        "max_retry": max(0, int(task.max_retry or DEFAULT_MAX_RETRY)),
    }
    task_payload.update(dict(task.payload or {}))
    task_data = create_managed_task(
        {
            "source": "api",
            "intent": task.intent,
            "payload": task_payload,
            "priority": task.priority,
            "constraints": task.constraints,
        },
        store_path=TASKS_FILE,
    )
    task_data.update(task_payload)
    task_data.update(
        {
            "task_id": task_data["task_id"],
            "payload": dict(task_payload),
            "contact_id": task.contact_id,
            "source_channel": task.source_channel,
            "linear_status": intake_status,
            "workflow_phase": "intake",
            "next_step": next_step_for_phase(
                "intake",
                done_condition_met=task.done_condition_met,
            ),
            "task_intake_source": "linear_contract_fields",
            "steps": build_steps(task.change_type),
            "current_step": 0,
            "status": "created",
            "created_at": task_data["created_at"],
            "results": [],
        }
    )
    return save_managed_task(task_data, store_path=TASKS_FILE)


def upsert_task(task_data: dict) -> None:
    save_managed_task(task_data, store_path=TASKS_FILE)


def ensure_task_intake_contract(task_data: dict) -> dict:
    errors = validate_task_intake_contract(task_data)
    if errors:
        raise ValueError("; ".join(errors))

    normalized = normalize_task_intake_payload(task_data)
    task_data.update(normalized)
    task_data["done_condition_met"] = normalize_done_condition_met(
        task_data.get("done_condition_met", False)
    )
    task_data.setdefault("task_intake_source", "linear_contract_fields")
    return task_data


def update_linear_phase(task_data: dict, phase: str) -> dict:
    done_condition_met = normalize_done_condition_met(
        task_data.get("done_condition_met", False)
    )
    task_data["workflow_phase"] = phase
    task_data["linear_status"] = linear_status_for_phase(
        phase,
        done_condition_met=done_condition_met,
    )
    task_data["next_step"] = next_step_for_phase(
        phase,
        done_condition_met=done_condition_met,
    )
    return task_data


def _policy_input_from_task_data(task_data: dict) -> dict[str, object]:
    return policy_input_from_task_input({"payload": task_data, "constraints": task_data.get("constraints", "")})


def enforce_task_system_policy(task_data: dict) -> dict:
    _, system_rules = load_validated_system_context(
        _policy_input_from_task_data(task_data)
    )
    task_data["system_rules"] = system_rules
    return task_data


def _record_task_history(
    task_data: dict,
    *,
    event: str,
    detail: str,
    status: str | None = None,
) -> dict:
    return append_history_event(
        task_data,
        action=event,
        data={
            "detail": detail,
            "status": str(status or task_data.get("status", "")).strip(),
        },
    )


def _execution_timeout_seconds(task_data: dict) -> float:
    value = task_data.get("timeout_seconds", DEFAULT_TIMEOUT_SECONDS)
    try:
        return max(0.01, float(value))
    except (TypeError, ValueError):
        return DEFAULT_TIMEOUT_SECONDS


def _execution_deadline(task_data: dict) -> float:
    existing = task_data.get("execution_deadline")
    if isinstance(existing, (int, float)) and float(existing) > 0:
        return float(existing)
    deadline = time.monotonic() + _execution_timeout_seconds(task_data)
    task_data["execution_deadline"] = deadline
    return deadline


def _remaining_execution_seconds(task_data: dict) -> float:
    remaining = _execution_deadline(task_data) - time.monotonic()
    return max(0.0, remaining)


def _max_retry_count(task_data: dict) -> int:
    value = task_data.get("max_retry", DEFAULT_MAX_RETRY)
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return DEFAULT_MAX_RETRY


def _fail_task_execution(task_data: dict, *, step: dict, error: str) -> dict:
    task_data["current_step"] = int(step.get("step", 0))
    transition_task_status(
        task_data,
        "failed",
        reason=f"{step.get('name', 'execution')} failed",
    )
    add_step_result(
        task_data=task_data,
        step=step,
        result=f"[ERROR] {error}",
        status="failed",
        counts_toward_completion=False,
    )
    _record_task_history(
        task_data,
        event="execution_failed",
        detail=error,
        status="failed",
    )
    upsert_task(task_data)
    return task_data


def _run_agent_once_with_timeout(role: str, task_data: dict) -> str:
    remaining = _remaining_execution_seconds(task_data)
    if remaining <= 0:
        raise ExecutionTimeoutError("execution timeout exceeded before step start")

    scoped_task = build_agent_task_payload(task_data, role)
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(run_agent, role, scoped_task)
    try:
        return future.result(timeout=remaining)
    except FutureTimeoutError as exc:
        future.cancel()
        raise ExecutionTimeoutError("execution timeout exceeded") from exc
    finally:
        executor.shutdown(wait=False, cancel_futures=True)


def _run_agent_with_retry(task_data: dict, step: dict) -> str:
    attempts_allowed = _max_retry_count(task_data) + 1
    last_error = "execution failed"
    for attempt in range(1, attempts_allowed + 1):
        try:
            task_data["retry_count"] = attempt - 1
            return _run_agent_once_with_timeout(step["role"], task_data)
        except Exception as exc:
            last_error = str(exc).strip() or "execution failed"
            _record_task_history(
                task_data,
                event="execution_retry" if attempt < attempts_allowed else "execution_error",
                detail=(
                    f"{step['name']} attempt {attempt}/{attempts_allowed}: {last_error}"
                ),
                status="running" if attempt < attempts_allowed else "failed",
            )
            upsert_task(task_data)
            if attempt >= attempts_allowed:
                raise
    raise RuntimeError(last_error)


def _business_task_result(task_data: dict, *, detail: str, action: str) -> dict:
    add_workflow_event(
        task_data,
        name="business_action",
        phase="report",
        result=detail,
        step_number=1,
    )
    transition_task_status(task_data, "completed", reason=action)
    _record_task_history(
        task_data,
        event=action,
        detail=detail,
        status="completed",
    )
    _record_task_history(
        task_data,
        event="execution_completed",
        detail="Task completed successfully.",
        status="completed",
    )
    upsert_task(task_data)
    save_task_record(task_data)
    return task_data


def _execute_new_lead(task_data: dict) -> dict:
    payload = dict(task_data.get("payload", {}))
    lead_request = str(
        payload.get("request") or payload.get("summary") or task_data.get("goal", "")
    ).strip()
    lead_name = str(payload.get("name", "")).strip()
    lead_phone = str(payload.get("phone", "")).strip()
    lead_address = str(payload.get("address", "")).strip()
    detail = (
        f"new_lead recorded: name={lead_name or 'unknown'}; "
        f"phone={lead_phone or 'unknown'}; "
        f"address={lead_address or 'unknown'}; "
        f"request={lead_request or 'unknown'}."
    )
    return _business_task_result(task_data, detail=detail, action="lead_recorded")


def _execute_service_request(task_data: dict) -> dict:
    payload = dict(task_data.get("payload", {}))
    service_type = str(payload.get("service_type", "")).strip() or "general_service"
    requested_time = str(payload.get("requested_time", "")).strip() or "unspecified"
    location = str(payload.get("location", "")).strip() or "unspecified"
    request = str(
        payload.get("request") or payload.get("summary") or task_data.get("goal", "")
    ).strip()
    detail = (
        f"service_request recorded: type={service_type}; "
        f"requested_time={requested_time}; "
        f"location={location}; "
        f"request={request or 'unknown'}."
    )
    return _business_task_result(
        task_data,
        detail=detail,
        action="service_request_recorded",
    )


def _execute_outbound_message(task_data: dict) -> dict:
    payload = dict(task_data.get("payload", {}))
    contact_id = str(
        task_data.get("contact_id") or payload.get("contact_id", "")
    ).strip()
    if not contact_id:
        raise ValueError("outbound_message requires contact_id")

    contact = find_contact(contact_id=contact_id)
    if contact is None:
        raise ValueError(f"contact not found: {contact_id}")

    outbound_channel = str(payload.get("outbound_channel", "sms")).strip().lower() or "sms"
    phone_numbers = [
        str(item).strip()
        for item in contact.get("phone_numbers", [])
        if str(item).strip()
    ]
    emails = [
        str(item).strip()
        for item in contact.get("emails", [])
        if str(item).strip()
    ]
    if outbound_channel in {"sms", "phone"} and not phone_numbers:
        raise ValueError(f"contact has no phone number: {contact_id}")

    message_text = str(
        payload.get("message_text") or payload.get("text") or task_data.get("goal", "")
    ).strip()
    if not message_text:
        raise ValueError("outbound_message requires message_text")

    if outbound_channel == "email":
        if not emails:
            raise ValueError(f"contact has no email address: {contact_id}")
        subject = str(payload.get("subject", "")).strip() or "Digital Foreman update"
        email_result = execute_external(
            SEND_EMAIL,
            {
                "destination": "gmail_gateway",
                "to": emails[0],
                "subject": subject,
                "body": message_text,
            },
        )
        if str(email_result.get("status", "")).strip().lower() != "completed":
            raise ValueError(str(email_result.get("error_message") or "email send failed"))
        append_history_event(
            task_data,
            action="email_sent",
            data={
                "contact_id": contact_id,
                "email": emails[0],
                "subject": subject,
                "text": message_text[:160],
            },
        )
        detail = f"email sent to {emails[0]}: {subject}"
    elif outbound_channel == "phone":
        call_result = execute_external(
            MAKE_CALL,
            {
                "destination": "phone_gateway",
                "contact_id": contact_id,
                "phone_number": phone_numbers[0],
                "script": message_text,
            },
        )
        if str(call_result.get("status", "")).strip().lower() != "completed":
            raise ValueError(str(call_result.get("error_message") or "phone call failed"))
        append_history_event(
            task_data,
            action="phone_call_scheduled",
            data={
                "contact_id": contact_id,
                "phone": phone_numbers[0],
                "script": message_text[:160],
            },
        )
        detail = f"phone call scheduled for {phone_numbers[0]}: {message_text[:160]}"
    else:
        sms_result = execute_external(
            SEND_SMS,
            {
                "destination": "sms_gateway",
                "contact_id": contact_id,
                "text": message_text,
            },
        )
        if str(sms_result.get("status", "")).strip().lower() != "completed":
            raise ValueError(str(sms_result.get("error_message") or "sms send failed"))
        append_history_event(
            task_data,
            action="sms_sent",
            data={
                "contact_id": contact_id,
                "phone": phone_numbers[0],
                "text": message_text[:160],
            },
        )
        detail = f"sms sent to {phone_numbers[0]}: {message_text[:160]}"
    return _business_task_result(
        task_data,
        detail=detail,
        action="outbound_completed",
    )


def workflow_event_step(*, step_number: int, name: str, phase: str) -> dict:
    return {
        "step": step_number,
        "name": name,
        "role": "orchestrator",
        "logical_role": phase.capitalize(),
    }


def build_step_result(
    task_data: dict,
    step: dict,
    result: str,
    status: str,
    *,
    counts_toward_completion: bool = True,
) -> dict:
    return {
        "task_id": task_data["task_id"],
        "step": step["step"],
        "name": step["name"],
        "role": step["role"],
        "logical_role": step.get("logical_role"),
        "result": result,
        "status": status,
        "workflow_phase": task_data.get("workflow_phase"),
        "linear_status": task_data.get("linear_status"),
        "done_condition_met": normalize_done_condition_met(
            task_data.get("done_condition_met", False)
        ),
        "next_step": task_data.get("next_step", ""),
        "counts_toward_completion": counts_toward_completion,
        "ts": now(),
    }


def add_step_result(
    task_data: dict,
    step: dict,
    result: str,
    status: str,
    *,
    counts_toward_completion: bool = True,
) -> dict:
    step_result = build_step_result(
        task_data=task_data,
        step=step,
        result=result,
        status=status,
        counts_toward_completion=counts_toward_completion,
    )
    task_data["results"].append(step_result)
    task_data["last_step_result"] = step_result
    return step_result


def add_workflow_event(
    task_data: dict,
    *,
    name: str,
    phase: str,
    result: str,
    status: str = "done",
    step_number: int = 0,
) -> dict:
    update_linear_phase(task_data, phase)
    record = add_step_result(
        task_data=task_data,
        step=workflow_event_step(
            step_number=step_number,
            name=name,
            phase=phase,
        ),
        result=result,
        status=status,
        counts_toward_completion=False,
    )
    _record_task_history(
        task_data,
        event=f"workflow_{name}",
        detail=result,
        status=status,
    )
    return record


def find_task_by_id(task_id: str) -> dict | None:
    task = get_stored_task(task_id, store_path=TASKS_FILE)
    if task is None:
        return None
    return dict(task)


def extract_task_result(task_data: dict) -> str:
    for step_result in reversed(task_data.get("results", [])):
        if step_result.get("role") == "memory_agent":
            continue

        detail = str(step_result.get("result", "")).strip()
        if detail:
            return detail

    last_step_result = task_data.get("last_step_result", {})
    detail = str(last_step_result.get("result", "")).strip()
    if detail:
        return detail

    return str(task_data.get("status", "unknown")).strip()


def linear_task_reference(task_data: dict) -> str:
    return format_linear_task_reference(
        task_data.get("linear_task_id", ""),
        task_data.get("linear_task_title", ""),
    )


def intake_result_message(task_data: dict) -> str:
    return (
        "Task Intake Contract verified from Linear fields for "
        f"{linear_task_reference(task_data)}. "
        f"MVP priority: {task_data.get('mvp_priority', '')}. "
        f"Expected result: {task_data.get('expected_result', '')}. "
        f"Done condition: {task_data.get('done_condition', '')}."
    ).strip()


def sync_before_message(task_data: dict) -> str:
    return (
        "linear_sync_before: staged "
        f"{linear_task_reference(task_data)} as {task_data.get('linear_status', '')} "
        "using the enforced task contract."
    ).strip()


def sync_after_message(task_data: dict) -> str:
    state = "met" if task_data.get("done_condition_met") else "not met"
    return (
        "linear_sync_after: prepared final Linear update for "
        f"{linear_task_reference(task_data)}. "
        f"Status: {task_data.get('linear_status', '')}. "
        f"Done condition: {state}."
    ).strip()


def done_condition_satisfied_by_step(step: dict, result: str) -> bool:
    if step.get("logical_role") != "QA":
        return False

    normalized_result = str(result or "").strip().lower()
    return "[qa] file_exists: true" in normalized_result


def build_dependency_context(task_data: dict, dependency_task: dict) -> str:
    base_context = str(
        task_data.get("base_context", task_data.get("context", ""))
    ).strip()
    dependency_result = extract_task_result(dependency_task)
    dependency_goal = str(dependency_task.get("goal", "")).strip()
    dependency_task_id = str(dependency_task.get("task_id", "")).strip()

    dependency_context = "\n".join(
        [
            f"Dependency task_id: {dependency_task_id}",
            f"Dependency goal: {dependency_goal}",
            f"Dependency result: {dependency_result}",
        ]
    ).strip()

    if base_context:
        return f"{base_context}\n\n{dependency_context}"

    return dependency_context


def build_memory_context(limit: int = 3) -> str:
    records = load_recent_memory_records(limit)
    if not records:
        return ""

    lines = ["Previous tasks from memory:"]

    for index, record in enumerate(records, start=1):
        goal = str(record.get("goal", "")).strip() or "None"
        result = str(record.get("result", "")).strip() or "None"
        lines.append(f"{index}. Goal: {goal}")
        lines.append(f"Result: {result}")

    return "\n".join(lines).strip()


def _memory_override_roles(task_data: dict) -> set[str]:
    raw_roles = task_data.get("full_memory_roles", ())
    if isinstance(raw_roles, str):
        values = [raw_roles]
    elif isinstance(raw_roles, (list, tuple, set)):
        values = list(raw_roles)
    else:
        values = []
    return {
        str(value or "").strip().lower()
        for value in values
        if str(value or "").strip()
    }


def _allow_full_memory_context(task_data: dict, role: str) -> bool:
    if bool(task_data.get("allow_full_memory_context")):
        return True
    return str(role or "").strip().lower() in _memory_override_roles(task_data)


def _format_agent_memory_context(agent_context: dict[str, object]) -> str:
    shared_system_context = agent_context.get("shared_system_context", {})
    if not isinstance(shared_system_context, dict):
        shared_system_context = {}
    shared_memory_summary = agent_context.get("shared_memory_summary", {})
    if not isinstance(shared_memory_summary, dict):
        shared_memory_summary = {}
    scoped_memory = agent_context.get("scoped_memory", {})
    if not isinstance(scoped_memory, dict):
        scoped_memory = {}

    lines = ["Scoped agent memory:"]
    system_name = str(shared_system_context.get("system_name", "")).strip()
    system_type = str(shared_system_context.get("system_type", "")).strip()
    business_name = str(shared_system_context.get("business_name", "")).strip()
    product_name = str(shared_system_context.get("product_name", "")).strip()
    if system_name or system_type:
        lines.append(
            f"- system: {system_name or 'unknown'}"
            + (f" ({system_type})" if system_type else "")
        )
    if business_name:
        lines.append(f"- business: {business_name}")
    if product_name:
        lines.append(f"- product: {product_name}")

    for field in (
        "current_stage",
        "core_status",
        "operating_phase",
        "system_mode",
        "focus",
        "next_step",
        "active_block",
    ):
        value = str(shared_memory_summary.get(field, "")).strip()
        if value:
            lines.append(f"- {field}: {value}")

    rules = shared_memory_summary.get("architecture_rules", ())
    if isinstance(rules, list):
        for rule in rules[:3]:
            normalized_rule = str(rule or "").strip()
            if normalized_rule:
                lines.append(f"- architecture_rule: {normalized_rule}")

    for scope_name, scope_payload in scoped_memory.items():
        if not isinstance(scope_payload, dict):
            continue
        lines.append(f"- scope: {scope_name}")
        focus_items = scope_payload.get("focus", ())
        if isinstance(focus_items, list):
            for item in focus_items[:3]:
                normalized_item = str(item or "").strip()
                if normalized_item:
                    lines.append(f"  focus: {normalized_item}")
        guardrails = scope_payload.get("guardrails", ())
        if isinstance(guardrails, list):
            for item in guardrails[:2]:
                normalized_item = str(item or "").strip()
                if normalized_item:
                    lines.append(f"  guardrail: {normalized_item}")

    return "\n".join(lines).strip()


def build_agent_task_payload(task_data: dict, role: str) -> dict:
    scoped_task = dict(task_data)
    include_full_memory = _allow_full_memory_context(task_data, role)
    scoped_task["agent_context"] = build_agent_context(
        role,
        include_full_memory=include_full_memory,
    )
    scoped_task["memory_access_level"] = (
        "assistant_context" if include_full_memory else "agent_context"
    )
    if include_full_memory:
        scoped_task["assistant_context"] = build_assistant_context()
    else:
        scoped_task.pop("assistant_context", None)
    return scoped_task


def apply_memory_context(task_data: dict, role: str = "coder_agent") -> dict:
    resolved_context = str(
        task_data.get("resolved_context", task_data.get("context", ""))
    ).strip()
    memory_context = build_memory_context()
    include_full_memory = _allow_full_memory_context(task_data, role)
    agent_context = build_agent_context(
        role,
        include_full_memory=include_full_memory,
    )
    scoped_memory_context = _format_agent_memory_context(agent_context)
    context_sections = [
        section
        for section in (resolved_context, scoped_memory_context, memory_context)
        if str(section or "").strip()
    ]
    task_data["context"] = "\n\n".join(context_sections).strip()
    task_data["agent_context"] = agent_context
    task_data["memory_access_level"] = (
        "assistant_context" if include_full_memory else "agent_context"
    )
    if include_full_memory:
        task_data["assistant_context"] = build_assistant_context()
    else:
        task_data.pop("assistant_context", None)

    return task_data


def apply_task_dependency(task_data: dict) -> dict:
    task_data.setdefault("depends_on", "")
    task_data.setdefault("base_context", task_data.get("context", ""))
    task_data.setdefault("resolved_context", task_data.get("context", ""))

    depends_on = str(task_data.get("depends_on", "")).strip()
    if not depends_on:
        task_data["resolved_context"] = task_data.get("base_context", "")
        task_data["context"] = task_data["resolved_context"]
        return task_data

    dependency_task = find_task_by_id(depends_on)
    if not dependency_task:
        raise ValueError(f"Dependency task not found: {depends_on}")

    task_data["resolved_context"] = build_dependency_context(task_data, dependency_task)
    task_data["context"] = task_data["resolved_context"]
    return task_data


def execute_task(task_data: dict) -> dict:
    if str(task_data.get("entry_point", "")).strip().lower() != "secretary":
        return build_dependency_error_response(
            task_data,
            "Secretary entry required before execution",
        )

    try:
        task_data = enforce_task_system_policy(task_data)
    except PolicyViolationError as exc:
        return build_dependency_error_response(
            task_data,
            f"System policy rejected execution: {exc}",
        )

    transition_task_status(task_data, "running", reason="execution_started")
    task_data["execution_started_at"] = now()
    _execution_deadline(task_data)
    _record_task_history(
        task_data,
        event="execution_started",
        detail=(
            "Task entered execution flow. "
            f"timeout_seconds={_execution_timeout_seconds(task_data)}; "
            f"max_retry={_max_retry_count(task_data)}."
        ),
        status="running",
    )
    upsert_task(task_data)

    steps = task_data["steps"]

    add_workflow_event(
        task_data,
        name="intake",
        phase="intake",
        result=intake_result_message(task_data),
    )
    upsert_task(task_data)

    add_workflow_event(
        task_data,
        name="linear_sync_before",
        phase="intake",
        result=sync_before_message(task_data),
    )
    upsert_task(task_data)

    task_intent = str(task_data.get("intent", "")).strip().lower()
    if task_intent == "service_request":
        try:
            return _execute_service_request(task_data)
        except Exception as exc:
            return _fail_task_execution(
                task_data,
                step={"step": 1, "name": "service_request", "role": "orchestrator"},
                error=str(exc).strip() or "service request execution failed",
            )
    if task_intent == "new_lead":
        try:
            return _execute_new_lead(task_data)
        except Exception as exc:
            return _fail_task_execution(
                task_data,
                step={"step": 1, "name": "new_lead", "role": "orchestrator"},
                error=str(exc).strip() or "new lead execution failed",
            )
    if task_intent == "outbound_message":
        try:
            return _execute_outbound_message(task_data)
        except Exception as exc:
            return _fail_task_execution(
                task_data,
                step={"step": 1, "name": "outbound_message", "role": "orchestrator"},
                error=str(exc).strip() or "outbound message execution failed",
            )

    for step in steps:
        try:
            phase = workflow_phase_for_logical_role(step.get("logical_role"))
            update_linear_phase(task_data, phase)

            if step.get("logical_role") == "Reporter":
                add_workflow_event(
                    task_data,
                    name="linear_sync_after",
                    phase="report",
                    result=sync_after_message(task_data),
                    step_number=step["step"],
                )
                upsert_task(task_data)

            if step["role"] == "coder_agent":
                task_data = apply_memory_context(task_data, role=step["role"])

            result = _run_agent_with_retry(task_data, step)
            if step["role"] == "reviewer_agent" and result != "ok":
                task_data["current_step"] = step["step"]
                task_data["review_feedback"] = result
                task_data["review_retry_count"] = (
                    int(task_data.get("review_retry_count", 0)) + 1
                )

                add_step_result(
                    task_data=task_data,
                    step=step,
                    result=result,
                    status="running",
                )
                upsert_task(task_data)

                coder_step = find_step_by_role(steps, "coder_agent") or {
                    "step": step["step"],
                    "role": "coder_agent",
                    "name": "retry_build",
                }
                retry_step = {
                    "step": coder_step["step"],
                    "role": coder_step["role"],
                    "logical_role": "Implementer",
                    "name": "retry_build",
                }
                update_linear_phase(task_data, "implement")
                task_data = apply_memory_context(task_data, role=retry_step["role"])
                retry_result = _run_agent_with_retry(task_data, retry_step)
                add_step_result(
                    task_data=task_data,
                    step=retry_step,
                    result=retry_result,
                    status="running",
                )
                upsert_task(task_data)

                update_linear_phase(task_data, "implement")
                review_retry_step = dict(step)
                review_retry_result = _run_agent_with_retry(task_data, review_retry_step)
                if review_retry_result != "ok":
                    return _fail_task_execution(
                        task_data,
                        step=step,
                        error=review_retry_result,
                    )

                if done_condition_satisfied_by_step(step, review_retry_result):
                    task_data["done_condition_met"] = True
                    update_linear_phase(task_data, phase)

                task_data["current_step"] = step["step"]
                if step["step"] == len(steps):
                    transition_task_status(
                        task_data,
                        "completed",
                        reason=f"{step['name']} completed final step",
                    )
                task_data["review_feedback"] = ""
                add_step_result(
                    task_data=task_data,
                    step=step,
                    result=review_retry_result,
                    status=str(task_data["status"]),
                )
                upsert_task(task_data)
                continue

            if done_condition_satisfied_by_step(step, result):
                task_data["done_condition_met"] = True
                update_linear_phase(task_data, phase)

            task_data["current_step"] = step["step"]
            if step["step"] == len(steps):
                transition_task_status(
                    task_data,
                    "completed",
                    reason=f"{step['name']} completed final step",
                )

            add_step_result(
                task_data=task_data,
                step=step,
                result=result,
                status=str(task_data["status"]),
            )
            _record_task_history(
                task_data,
                event="step_completed",
                detail=f"{step['name']}: {result}",
                status=str(task_data["status"]),
            )
            upsert_task(task_data)
        except Exception as exc:
            return _fail_task_execution(
                task_data,
                step=step,
                error=str(exc).strip() or "unexpected execution failure",
            )

    _record_task_history(
        task_data,
        event="execution_completed",
        detail="Task completed successfully.",
        status=str(task_data.get("status", "completed")),
    )
    upsert_task(task_data)
    return task_data


def build_response(task_data: dict) -> dict:
    response = dict(task_data)
    response.pop("system_rules", None)
    response.pop("source", None)
    response.pop("execution_deadline", None)
    response["job_id"] = str(task_data.get("task_id", "")).strip()
    response["linear_task"] = linear_task_reference(task_data)
    response["steps_total"] = len(task_data.get("steps", []))
    return response


def build_memory_response(record: dict) -> dict:
    linear_task_id = record.get("linear_task_id", "")
    linear_task_title = record.get("linear_task_title", "")
    return {
        "task_id": record["task_id"],
        "job_id": record["task_id"],
        "status": record.get("status", "done"),
        "workflow_phase": record.get("workflow_phase", "report"),
        "current_step": 0,
        "steps_total": len(build_steps(record.get("change_type", "review"))),
        "steps_completed": 0,
        "linear_task": format_linear_task_reference(
            linear_task_id,
            linear_task_title,
        ),
        "linear_task_id": linear_task_id,
        "linear_task_title": linear_task_title,
        "linear_status": record.get("linear_status", ""),
        "mvp_priority": record.get("mvp_priority", ""),
        "done_condition_met": normalize_done_condition_met(
            record.get("done_condition_met", False)
        ),
        "next_step": record.get("next_step", ""),
        "last_result": {
            "task_id": record["task_id"],
            "step": 0,
            "name": "memory_hit",
            "role": "memory_agent",
            "logical_role": "Reporter",
            "result": record["result"],
            "status": record.get("status", "done"),
            "workflow_phase": record.get("workflow_phase", "report"),
            "linear_status": record.get("linear_status", ""),
            "done_condition_met": normalize_done_condition_met(
                record.get("done_condition_met", False)
            ),
            "next_step": record.get("next_step", ""),
            "ts": record["timestamp"],
            "file_path": record["file_path"],
        },
        "from_memory": True,
    }


def build_dependency_error_response(task_data: dict, message: str) -> dict:
    task_data.setdefault("task_id", next_task_id())
    task_data.setdefault("steps", build_steps(task_data.get("change_type", "review")))
    task_data.setdefault("current_step", 0)
    task_data.setdefault("results", [])
    task_data.setdefault("created_at", now())
    task_data.setdefault("workflow_phase", "intake")
    task_data.setdefault("execution_mode", "")
    task_data.setdefault("execution_location", "")
    task_data.setdefault("offload_latency", None)
    task_data.setdefault("routing_reason", None)
    task_data.setdefault("telemetry_snapshot", None)
    task_data.setdefault("safety_override", None)
    task_data.setdefault("network_snapshot", None)
    task_data.setdefault("network_policy", None)
    task_data.setdefault("job_id", task_data.get("task_id", ""))
    task_data.setdefault("interaction_id", task_data.get("task_id", ""))
    task_data.setdefault("trace_id", task_data.get("task_id", ""))
    task_data.setdefault(
        "linear_status",
        linear_status_for_phase(
            "intake",
            done_condition_met=normalize_done_condition_met(
                task_data.get("done_condition_met", False)
            ),
        ),
    )
    task_data.setdefault(
        "next_step",
        next_step_for_phase(
            "intake",
            done_condition_met=normalize_done_condition_met(
                task_data.get("done_condition_met", False)
            ),
        ),
    )
    current_status = str(task_data.get("status", "")).strip().lower() or "pending"
    if current_status == "created":
        transition_task_status(
            task_data,
            "confirmed",
            reason="validation_failed_before_queue",
        )
        current_status = "confirmed"
    if current_status == "confirmed":
        transition_task_status(
            task_data,
            "pending",
            reason="validation_failed_before_queue",
        )
        current_status = "pending"
    if current_status == "pending":
        transition_task_status(
            task_data,
            "running",
            reason="execution started before blocked termination",
        )
    transition_task_status(task_data, "failed", reason="execution_blocked")

    step_result = {
        "task_id": task_data["task_id"],
        "step": 0,
        "name": "dependency_check",
        "role": "orchestrator",
        "result": message,
        "status": "failed",
        "workflow_phase": task_data.get("workflow_phase"),
        "linear_status": task_data.get("linear_status"),
        "done_condition_met": normalize_done_condition_met(
            task_data.get("done_condition_met", False)
        ),
        "next_step": task_data.get("next_step", ""),
        "counts_toward_completion": False,
        "ts": now(),
    }
    task_data["last_step_result"] = step_result
    task_data["results"].append(step_result)
    _record_task_history(
        task_data,
        event="execution_blocked",
        detail=message,
        status="failed",
    )

    upsert_task(task_data)
    return build_response(task_data)


def run_task(task: TaskRequest) -> dict:
    try:
        task_data = create_task(task)
    except ValueError as exc:
        return _standard_error_response(
            error_type="BLOCKED",
            error_message=str(exc),
            recoverable=True,
            extra={
                "task_id": "",
                "job_id": "",
                "workflow_phase": "intake",
                "current_step": 0,
                "steps_total": len(build_steps(task.change_type)),
                "steps_completed": 0,
                "linear_task": linear_task_reference(
                    {
                        "linear_task_id": task.linear_task_id,
                        "linear_task_title": task.linear_task_title,
                    }
                ),
                "linear_task_id": task.linear_task_id,
                "linear_task_title": task.linear_task_title,
                "linear_status": "To Do",
                "mvp_priority": task.mvp_priority,
                "done_condition_met": normalize_done_condition_met(task.done_condition_met),
                "next_step": "Use /api/secretary/entry.",
                "last_result": {
                    "task_id": "",
                    "step": 0,
                    "name": "secretary_gate",
                    "role": "orchestrator",
                    "logical_role": "Intake",
                    "result": str(exc),
                    "status": "failed",
                    "workflow_phase": "intake",
                    "linear_status": "To Do",
                    "done_condition_met": normalize_done_condition_met(
                        task.done_condition_met
                    ),
                    "next_step": "Use /api/secretary/entry.",
                    "counts_toward_completion": False,
                    "ts": now(),
                },
            },
        )
    return run_prepared_task(task_data)


def run_multiple_tasks(goals: list[str]) -> list[dict]:
    results = []

    for index, goal in enumerate(goals, start=1):
        task = TaskRequest(
            goal=goal,
            linear_task_id=f"SIM-{next_task_id()}-{index}",
            linear_task_title=goal,
            mvp_priority="P1",
            expected_result=goal,
            done_condition="Generated output exists and QA passed for the requested task.",
        )
        results.append(run_task(task))

    return results


def run_prepared_task(task_data: dict) -> dict:
    try:
        task_data = ensure_task_intake_contract(task_data)
    except ValueError as exc:
        return build_dependency_error_response(
            task_data,
            f"Task Intake Contract rejected: {exc}",
        )

    memory_record = find_task_record_by_linear_task_id(
        task_data.get("linear_task_id", "")
    )
    if memory_record:
        return build_memory_response(memory_record)

    try:
        task_data = apply_task_dependency(task_data)
    except ValueError as exc:
        return build_dependency_error_response(task_data, str(exc))

    task_data.setdefault("steps", build_steps(task_data.get("change_type", "review")))
    task_data.setdefault("current_step", 0)
    task_data.setdefault("status", "pending")
    task_data.setdefault("results", [])
    task_data.setdefault("created_at", now())
    task_data.setdefault("workflow_phase", "intake")
    task_data.setdefault(
        "linear_status",
        linear_status_for_phase(
            "intake",
            done_condition_met=normalize_done_condition_met(
                task_data.get("done_condition_met", False)
            ),
        ),
    )
    task_data.setdefault(
        "next_step",
        next_step_for_phase(
            "intake",
            done_condition_met=normalize_done_condition_met(
                task_data.get("done_condition_met", False)
            ),
        ),
    )
    try:
        task_data = enforce_task_system_policy(task_data)
    except PolicyViolationError as exc:
        task_data["error"] = str(exc).strip() or "system policy rejected execution"
        return build_dependency_error_response(
            task_data,
            f"System policy rejected execution: {task_data['error']}",
        )
    transition_task_status(task_data, "confirmed", reason="secretary_confirmation_yes")
    upsert_task(task_data)
    transition_task_status(task_data, "pending", reason="queued_for_execution")
    upsert_task(task_data)

    _record_task_history(
        task_data,
        event="prepared",
        detail="Task prepared for execution.",
        status="pending",
    )
    queued = task_queue.enqueue(task_data["task_id"])
    _record_task_history(
        task_data,
        event="enqueued",
        detail=(
            "Task submitted to execution queue."
            if queued
            else "Task was already present in the execution queue."
        ),
        status="pending",
    )
    upsert_task(task_data)
    response = build_response(task_data)
    response["queued"] = bool(queued)
    response["queue_message"] = (
        "Task submitted to execution queue."
        if queued
        else "Task was already present in the execution queue."
    )
    return response

