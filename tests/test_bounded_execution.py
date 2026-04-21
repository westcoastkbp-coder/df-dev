from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from statistics import mean
from time import perf_counter

import app.context.shared_context_store as shared_context_store_module
import app.orchestrator.task_factory as task_factory
import app.orchestrator.task_state_store as task_state_store
import app.policy.policy_gate as policy_gate_module
import runtime.system_log as system_log_module
import runtime.token_efficiency as token_efficiency_module
import runtime.token_telemetry as token_telemetry_module
from app.execution.action_result import build_action_result
from app.execution.lead_estimate_contract import payload_size_bytes
from app.orchestrator.execution_runner import run_execution


CYCLES = 50
MAX_STEPS = 100
MAX_CONTEXT_SIZE = 4_096
MAX_RUNTIME_TIME = 5.0


def _configure_runtime(monkeypatch, tmp_path: Path) -> Path:
    store_path = tmp_path / "data" / "task_system.json"
    logs_dir = tmp_path / "runtime" / "logs"

    monkeypatch.setattr(task_state_store, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(task_factory, "TASK_SYSTEM_FILE", store_path)
    monkeypatch.setattr(shared_context_store_module, "DEFAULT_ROOT_DIR", tmp_path)
    monkeypatch.setattr(system_log_module, "SYSTEM_LOG_FILE", logs_dir / "system.log")
    monkeypatch.setattr(system_log_module, "TASK_LOG_FILE", logs_dir / "tasks.log")
    monkeypatch.setattr(policy_gate_module, "POLICY_LOG_FILE", logs_dir / "policy.log")
    monkeypatch.setattr(
        token_telemetry_module,
        "TOKEN_USAGE_LOG_FILE",
        logs_dir / "token_usage.jsonl",
    )
    monkeypatch.setattr(
        token_efficiency_module,
        "TOKEN_EFFICIENCY_LOG_FILE",
        logs_dir / "token_efficiency.jsonl",
    )

    task_factory.clear_task_runtime_store()
    shared_context_store_module._PREPARED_CONTEXT_ROOTS.clear()
    shared_context_store_module._JSON_FILE_CACHE.clear()
    return store_path


def _persist(updated_task: dict[str, object], *, store_path: Path) -> None:
    task_factory.save_task(updated_task, store_path=store_path)


def _create_validated_task(store_path: Path, *, task_id: str) -> dict[str, object]:
    task = task_factory.create_task(
        {
            "task_id": task_id,
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": "bounded execution replay"},
        },
        store_path=store_path,
    )
    task["status"] = "VALIDATED"
    return task_factory.save_task(task, store_path=store_path)


def _active_task_context_size(task_id: str) -> int:
    context = shared_context_store_module.get_context(f"active_task:{task_id}")
    return payload_size_bytes(context)


def test_bounded_execution_replay_keeps_context_payload_and_runtime_flat(
    monkeypatch,
    tmp_path: Path,
) -> None:
    store_path = _configure_runtime(monkeypatch, tmp_path)
    task_id = "DF-BOUND-CHECK-MINIMAL-V1"
    base_task = _create_validated_task(store_path, task_id=task_id)
    base_input = json.loads(json.dumps(base_task))

    seed_calls = 0

    def seed_executor(task_data: dict[str, object]) -> dict[str, object]:
        nonlocal seed_calls
        seed_calls += 1
        return build_action_result(
            status="completed",
            task_id=task_data.get("task_id"),
            action_type="NEW_LEAD",
            result_payload={"summary": "bounded replay seed"},
            error_code="",
            error_message="",
            source="test_bounded_execution_seed",
        )

    seeded = run_execution(
        deepcopy(base_input),
        now=lambda: "2026-04-13T00:00:00Z",
        persist=lambda updated_task: _persist(updated_task, store_path=store_path),
        executor=seed_executor,
    )
    assert str(seeded.get("status", "")).strip() == "COMPLETED"
    assert seed_calls == 1

    replay_calls = 0

    def replay_executor(_: dict[str, object]) -> dict[str, object]:
        nonlocal replay_calls
        replay_calls += 1
        raise AssertionError("executor must not run during bounded replay verification")

    metrics: list[dict[str, float | int]] = []
    started_at = perf_counter()

    for cycle in range(1, CYCLES + 1):
        cycle_started_at = perf_counter()
        executed = run_execution(
            deepcopy(base_input),
            now=lambda: "2026-04-13T00:00:00Z",
            persist=lambda updated_task: _persist(updated_task, store_path=store_path),
            executor=replay_executor,
        )
        duration_ms = (perf_counter() - cycle_started_at) * 1000.0
        context_size = _active_task_context_size(task_id)
        payload_size = payload_size_bytes(
            dict(executed.get("result", {}).get("result_payload", {}) or {})
        )
        step_count = len(list(executed.get("history", []) or []))

        metrics.append(
            {
                "cycle": cycle,
                "context_size": context_size,
                "payload_size": payload_size,
                "step_duration_ms": duration_ms,
                "step_count": step_count,
            }
        )
        print(
            "cycle="
            f"{cycle:02d} "
            f"context_len={context_size} "
            f"payload_size={payload_size} "
            f"step_duration_ms={duration_ms:.3f}"
        )

        assert str(executed.get("status", "")).strip() == "COMPLETED"
        assert step_count <= MAX_STEPS
        assert context_size <= MAX_CONTEXT_SIZE

    total_runtime = perf_counter() - started_at
    context_sizes = [int(metric["context_size"]) for metric in metrics]
    payload_sizes = [int(metric["payload_size"]) for metric in metrics]
    step_durations = [float(metric["step_duration_ms"]) for metric in metrics]
    head_avg = mean(step_durations[:10])
    tail_avg = mean(step_durations[-10:])

    assert replay_calls == 0
    assert total_runtime < MAX_RUNTIME_TIME
    assert all(size == context_sizes[0] for size in context_sizes), metrics
    assert all(size == payload_sizes[0] for size in payload_sizes), metrics
    assert tail_avg <= head_avg * 1.5 + 5.0, metrics
