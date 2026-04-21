from __future__ import annotations

import copy
from datetime import datetime, timezone
from typing import Any

import scripts.run_command as run_command_module


def _normalize_owner_input(input_text: str) -> str:
    return " ".join(str(input_text or "").split()).strip()


def _build_owner_command_task(
    input_text: str,
    *,
    context_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_input = _normalize_owner_input(input_text)
    task_payload = run_command_module._build_owner_task(normalized_input)
    task_payload["task_type"] = "owner_command"
    task_payload["command_name"] = (
        f"owner task {' '.join(normalized_input.lower().split())}"
    )
    task_payload["pipeline_route"] = "owner task"
    task_payload["context_mode"] = "owner"
    task_payload["print_analysis"] = True
    task_payload["input_text"] = normalized_input
    if isinstance(context_summary, dict):
        task_payload["context_summary"] = copy.deepcopy(context_summary)
    return task_payload


def _error_result(
    *,
    task_id: str,
    message: str,
    action_type: str,
    context_used: str,
    policy_result: str,
) -> dict[str, Any]:
    return {
        "result": {
            "status": "error",
            "message": str(message).strip() or "OWNER_ENTRY_FAILED",
        },
        "task_id": task_id,
        "status": "failure",
        "decision_trace": run_command_module.build_decision_trace(
            reason=str(message).strip() or "owner entry failed",
            context_used=context_used,
            action_type=action_type,
            policy_result=policy_result,
            confidence="low",
        ),
    }


def handle_owner_input(input_text: str) -> dict[str, Any]:
    normalized_input = _normalize_owner_input(input_text)
    if not normalized_input:
        return _error_result(
            task_id="",
            message="EMPTY_INPUT",
            action_type="owner_command",
            context_used="owner entry without input",
            policy_result="blocked: empty owner input",
        )

    task_payload = _build_owner_command_task(normalized_input)
    task_id = str(task_payload.get("task_id") or "").strip()
    output_execution_id = str(run_command_module._timestamp_task_id())
    output_timestamp = datetime.now(timezone.utc).isoformat()

    try:
        execution_system_context = run_command_module._load_required_execution_context()
    except RuntimeError as error:
        return {
            "result": {
                "status": "error",
                "message": str(error).strip() or "CONTEXT_NOT_LOADED",
            },
            "task_id": task_id,
            "status": "failure",
            "decision_trace": run_command_module.build_decision_trace(
                reason=str(error).strip() or "context not loaded",
                context_used="owner entry execution context bootstrap",
                action_type="owner_command",
                policy_result="blocked: required execution context missing",
                confidence="low",
            ),
        }

    context_payload = run_command_module._load_context_payload()
    context_summary = run_command_module._load_system_context_summary()
    task_payload["context_summary"] = copy.deepcopy(context_summary)
    selected_context = run_command_module.get_relevant_context(
        "owner",
        context_payload=context_payload,
    )
    selected_context = run_command_module._context_with_memory(
        selected_context,
        context_summary=context_summary,
    )
    task_payload = run_command_module._inject_context_into_pipeline(
        task_payload,
        selected_context,
        mode="owner",
    )
    output_memory_summary = dict(selected_context.get("memory_summary") or {})
    command_name = str(task_payload.get("command_name") or "owner task").strip()
    decision_context = {
        "task_id": task_id,
        "task_state": {
            "task_id": task_id,
            "status": "ready",
        },
        "command_name": command_name,
        "mode": "owner",
        "input_text": normalized_input,
        "context_summary": context_summary,
        "system_context": execution_system_context,
        "selected_context": selected_context,
        "owner_command": {
            "input_text": normalized_input,
            "context_summary": copy.deepcopy(context_summary),
        },
    }

    try:
        action_plan = run_command_module.decision_engine_module.validate_action_plan(
            run_command_module.decision_engine_module.decide(
                task_payload,
                decision_context,
            ),
            expected_task_id=task_id,
        )
        action_plan["vendor"] = run_command_module.route_vendor(
            task_payload,
            decision_context,
            action_plan,
        )
        action_plan = run_command_module.decision_engine_module.validate_action_plan(
            action_plan,
            expected_task_id=task_id,
        )
    except Exception as error:
        reason = str(error).strip() or "missing decision before command execution"
        return {
            "result": {
                "status": "error",
                "message": reason,
            },
            "task_id": task_id,
            "status": "failure",
            "decision_trace": run_command_module.decision_engine_module.failure_trace_for_context(
                task=task_payload,
                context=decision_context,
                reason=reason,
                source="owner_entry",
            ),
        }

    if action_plan["requires_confirmation"]:
        return {
            "result": {
                "status": "error",
                "message": str(action_plan["reason"]).strip()
                or "decision requires confirmation",
            },
            "task_id": task_id,
            "status": "failure",
            "decision_trace": run_command_module.decision_engine_module.decision_trace_for_plan(
                action_plan,
                task=task_payload,
                context=decision_context,
                source="owner_entry",
            ),
        }

    execution_state = run_command_module._execute_task_request(
        task_payload,
        command_text=normalized_input,
        system_context=execution_system_context,
    )
    if not bool(execution_state.get("succeeded")):
        failure_reason = str(execution_state.get("failure_reason") or "").strip()
        failure_trace = run_command_module._command_decision_trace(
            command_name=command_name,
            mode="owner",
            context_summary=context_summary,
            task_payload=task_payload,
            success=False,
            failure_reason=failure_reason,
            vendor=str(
                action_plan.get("vendor") or run_command_module.DEFAULT_VENDOR
            ).strip()
            or run_command_module.DEFAULT_VENDOR,
        )
        run_command_module._record_system_context_execution_summary(
            run_command_module._failed_action_summary(
                command_name,
                run_command_module._normalize_command(command_name),
                failure_reason,
            )
            + f" | {failure_trace['reason']}"
        )
        return {
            "result": {
                "status": "error",
                "message": failure_reason,
            },
            "task_id": task_id,
            "status": "failure",
            "decision_trace": failure_trace,
        }

    run_command_module._record_command_state(
        command_name,
        succeeded=True,
        doc_url=str(execution_state.get("doc_url") or ""),
        artifact_path=execution_state["artifact_path"],
        failure_reason="",
        state_path=run_command_module.COMMAND_STATE_FILE,
    )

    result, tool_source = run_command_module._build_task_result(
        task_payload,
        execution_state,
    )
    success_trace = run_command_module._command_decision_trace(
        command_name=command_name,
        mode="owner",
        context_summary=context_summary,
        task_payload=task_payload,
        success=True,
        vendor=str(
            action_plan.get("vendor") or run_command_module.DEFAULT_VENDOR
        ).strip()
        or run_command_module.DEFAULT_VENDOR,
    )
    result_payload = run_command_module._build_task_output_payload(
        result,
        memory_summary=output_memory_summary,
        tool_source=tool_source,
        execution_id=output_execution_id,
        timestamp=output_timestamp,
        decision_trace=success_trace,
    )
    run_command_module._persist_success_output(
        command_name,
        run_command_module._normalize_command(command_name),
        tool_source=tool_source,
        result_payload=result_payload,
    )
    return {
        "result": result_payload.get("result"),
        "task_id": task_id,
        "status": "success",
        "decision_trace": dict(result_payload.get("decision_trace") or {}),
    }
