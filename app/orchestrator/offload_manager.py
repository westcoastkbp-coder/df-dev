from __future__ import annotations

from collections.abc import Callable

from app.execution.product_executor import execute_product_task
from app.orchestrator.execution_runner import run_execution
from app.orchestrator.task_lifecycle import record_task_event


Executor = Callable[[dict[str, object]], dict[str, object]]
NowProvider = Callable[[], str]
TaskPersister = Callable[[dict[str, object]], None]


async def offload_task(
    task_data: dict[str, object],
    *,
    now: NowProvider,
    persist: TaskPersister,
    executor: Executor = execute_product_task,
    simulated_latency_seconds: float = 0.01,
) -> dict[str, object]:
    runtime_decision = dict(task_data.get("runtime_decision", {}))
    runtime_validation = dict(task_data.get("runtime_validation", {}))
    if str(runtime_decision.get("path_type", "")).strip().lower() == "voice" or bool(
        runtime_validation.get("latency_path_protected", False)
    ):
        record_task_event(
            task_data,
            timestamp=now(),
            event="VOICE_DECISION_GUARD_APPLIED",
            from_status=task_data.get("status", ""),
            to_status=task_data.get("status", ""),
            details={
                "reason": "voice_offload_blocked",
                "runtime_decision": runtime_decision,
                "runtime_validation": runtime_validation,
            },
        )
        task_data["execution_mode"] = "LOCAL"
        task_data["execution_location"] = "local"
        task_data["offload_latency"] = None
        return run_execution(
            task_data,
            now=now,
            persist=persist,
            executor=executor,
        )
    task_data["execution_mode"] = "LOCAL"
    task_data["execution_location"] = "local"
    task_data["offload_latency"] = None
    record_task_event(
        task_data,
        timestamp=now(),
        event="offload_unavailable_fallback_local",
        from_status=task_data.get("status", ""),
        to_status=task_data.get("status", ""),
        details={
            "reason": "remote_execution_not_implemented",
            "requested_execution_mode": "REMOTE",
            "execution_mode": "LOCAL",
            "execution_location": "local",
            "runtime_decision": runtime_decision,
            "runtime_validation": runtime_validation,
        },
    )
    persist(task_data)
    return run_execution(
        task_data,
        now=now,
        persist=persist,
        executor=executor,
    )


__all__ = ["offload_task"]

