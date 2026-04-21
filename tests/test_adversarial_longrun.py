from __future__ import annotations

import gc
import json
import queue
import sqlite3
import threading
import time
import tracemalloc
from collections import Counter
from pathlib import Path

import pytest

from app.execution.action_result import build_action_result
from app.orchestrator import task_factory
from app.orchestrator import task_state_store
from app.orchestrator.execution_runner import run_execution


MAX_RUNS = 150
MAX_DURATION_SECONDS = 90.0
STALL_TIMEOUT_SECONDS = 8.0
PER_EXECUTION_TIMEOUT_SECONDS = 2.0
RESTART_EVERY = 25
MEMORY_SAMPLE_EVERY = 50


def _configure_state_backend(monkeypatch, tmp_path: Path) -> Path:
    monkeypatch.setattr(task_state_store, "ROOT_DIR", tmp_path)
    monkeypatch.setattr(
        task_state_store,
        "TASK_STATE_DB_FILE",
        Path("runtime/state/task_state.sqlite3"),
    )
    monkeypatch.setattr(
        task_factory,
        "TASK_SYSTEM_FILE",
        tmp_path / "data" / "task_system.json",
    )
    task_factory.clear_task_runtime_store()
    return tmp_path / "data" / "task_system.json"


def _db_path(store_path: Path) -> Path:
    return store_path.with_suffix(".sqlite3")


def _ledger_rows(store_path: Path) -> list[tuple[str, str, str]]:
    connection = sqlite3.connect(str(_db_path(store_path)))
    try:
        return [
            (str(row[0]), str(row[1]), str(row[2]))
            for row in connection.execute(
                """
                SELECT execution_key, task_id, status
                FROM ExecutionLedger
                ORDER BY execution_key
                """
            ).fetchall()
        ]
    finally:
        connection.close()


def _create_validated_task(
    store_path: Path,
    *,
    task_id: str,
    summary: str,
) -> dict[str, object]:
    existing = task_factory.get_task(task_id, store_path)
    if existing is not None:
        return existing
    task = task_factory.create_task(
        {
            "task_id": task_id,
            "status": "created",
            "intent": "new_lead",
            "payload": {"summary": summary},
        },
        store_path=store_path,
    )
    task["status"] = "VALIDATED"
    return task_factory.save_task(task, store_path)


def _persist(updated_task: dict[str, object], *, store_path: Path) -> None:
    task_factory.save_task(updated_task, store_path)


def _run_with_timeout(
    task_data: dict[str, object],
    *,
    store_path: Path,
    executor,
    timeout_seconds: float,
) -> dict[str, object]:
    result_queue: queue.Queue[tuple[str, object]] = queue.Queue(maxsize=1)

    def _target() -> None:
        try:
            result = run_execution(
                json.loads(json.dumps(task_data)),
                now=lambda: "2026-04-06T02:00:00Z",
                persist=lambda updated_task: _persist(updated_task, store_path=store_path),
                executor=executor,
            )
        except Exception as exc:  # pragma: no cover - surfaced in parent thread
            result_queue.put(("error", exc))
            return
        result_queue.put(("result", result))

    worker = threading.Thread(target=_target, daemon=True)
    worker.start()
    worker.join(timeout_seconds)
    if worker.is_alive():
        pytest.fail(
            f"long-run execution timed out after {timeout_seconds:.1f}s for task "
            f"{task_data.get('task_id')}"
        )
    outcome, payload = result_queue.get_nowait()
    if outcome == "error":
        raise payload
    return payload


@pytest.mark.longrun
def test_longrun_execution_is_bounded_and_observable(
    monkeypatch,
    tmp_path: Path,
) -> None:
    store_path = _configure_state_backend(monkeypatch, tmp_path)
    tracemalloc.start()
    started_at = time.monotonic()
    last_progress_at = started_at
    memory_samples: list[dict[str, object]] = []
    side_effect_counts: Counter[str] = Counter()
    expected_results: dict[str, dict[str, object]] = {}
    expected_statuses: dict[str, str] = {}
    drift_events: list[dict[str, object]] = []

    def success_executor(task_data: dict[str, object]) -> dict[str, object]:
        task_id = str(task_data.get("task_id", "")).strip()
        side_effect_counts[task_id] += 1
        return build_action_result(
            status="completed",
            task_id=task_id,
            action_type="NEW_LEAD",
            result_payload={"summary": "longrun success", "effect_id": task_id},
            error_code="",
            error_message="",
            source="test_adversarial_longrun_executor",
        )

    success_executor.__module__ = "test_adversarial_longrun_executor"

    def failure_executor(task_data: dict[str, object]) -> dict[str, object]:
        task_id = str(task_data.get("task_id", "")).strip()
        return build_action_result(
            status="failed",
            task_id=task_id,
            action_type="NEW_LEAD",
            result_payload={"summary": "longrun failure", "effect_id": task_id},
            error_code="simulated_failure",
            error_message="simulated failure",
            source="test_adversarial_longrun_executor",
        )

    failure_executor.__module__ = "test_adversarial_longrun_executor"

    replay_task_id = "DF-LONGRUN-REPLAY-V1"
    replay_task = _create_validated_task(
        store_path,
        task_id=replay_task_id,
        summary="replay task",
    )
    first_replay = _run_with_timeout(
        replay_task,
        store_path=store_path,
        executor=success_executor,
        timeout_seconds=PER_EXECUTION_TIMEOUT_SECONDS,
    )
    expected_results[replay_task_id] = json.loads(json.dumps(first_replay.get("result", {})))
    expected_statuses[replay_task_id] = "COMPLETED"
    last_progress_at = time.monotonic()

    for run_index in range(1, MAX_RUNS + 1):
        now_monotonic = time.monotonic()
        if now_monotonic - started_at > MAX_DURATION_SECONDS:
            pytest.fail(
                f"long-run test exceeded max duration of {MAX_DURATION_SECONDS:.1f}s "
                f"after {run_index - 1} iterations"
            )
        if now_monotonic - last_progress_at > STALL_TIMEOUT_SECONDS:
            pytest.fail(
                f"long-run test stalled for more than {STALL_TIMEOUT_SECONDS:.1f}s "
                f"after {run_index - 1} iterations"
            )
        if run_index % RESTART_EVERY == 0:
            task_factory.clear_task_runtime_store()
            gc.collect()
        if run_index % MEMORY_SAMPLE_EVERY == 0:
            current_bytes, peak_bytes = tracemalloc.get_traced_memory()
            memory_samples.append(
                {
                    "iteration": run_index,
                    "current_bytes": current_bytes,
                    "peak_bytes": peak_bytes,
                }
            )

        scenario = run_index % 10
        if scenario in {1, 2, 3, 4}:
            task_id = f"DF-LONGRUN-SUCCESS-{run_index:04d}"
            executor = success_executor
            expected_status = "COMPLETED"
            _create_validated_task(store_path, task_id=task_id, summary="success task")
        elif scenario in {5, 6}:
            task_id = f"DF-LONGRUN-FAIL-{run_index:04d}"
            executor = failure_executor
            expected_status = "FAILED"
            _create_validated_task(store_path, task_id=task_id, summary="failure task")
        elif scenario in {7, 8}:
            task_id = f"DF-LONGRUN-RETRY-{run_index:04d}"
            executor = success_executor
            expected_status = "COMPLETED"
            _create_validated_task(store_path, task_id=task_id, summary="retry task")
        else:
            task_id = replay_task_id
            executor = success_executor
            expected_status = "COMPLETED"

        current_task = task_factory.get_task(task_id, store_path)
        assert current_task is not None
        executed = _run_with_timeout(
            current_task,
            store_path=store_path,
            executor=executor,
            timeout_seconds=PER_EXECUTION_TIMEOUT_SECONDS,
        )
        restored = task_factory.get_task(task_id, store_path)
        assert restored is not None
        last_progress_at = time.monotonic()

        observed_result = json.loads(json.dumps(executed.get("result", {})))
        restored_result = json.loads(json.dumps(restored.get("result", {})))
        observed_status = str(executed.get("status", "")).strip()
        restored_status = str(restored.get("status", "")).strip()
        if task_id not in expected_results:
            expected_results[task_id] = observed_result
            expected_statuses[task_id] = expected_status
        else:
            if observed_result != expected_results[task_id]:
                drift_events.append(
                    {
                        "iteration": run_index,
                        "task_id": task_id,
                        "type": "result_drift",
                    }
                )
            if restored_result != expected_results[task_id]:
                drift_events.append(
                    {
                        "iteration": run_index,
                        "task_id": task_id,
                        "type": "persisted_result_drift",
                    }
                )
        if observed_status != expected_statuses[task_id]:
            drift_events.append(
                {
                    "iteration": run_index,
                    "task_id": task_id,
                    "type": "status_drift",
                }
            )
        if restored_status != expected_statuses[task_id]:
            drift_events.append(
                {
                    "iteration": run_index,
                    "task_id": task_id,
                    "type": "persisted_status_drift",
                }
            )

    current_bytes, peak_bytes = tracemalloc.get_traced_memory()
    memory_samples.append(
        {
            "iteration": MAX_RUNS,
            "current_bytes": current_bytes,
            "peak_bytes": peak_bytes,
        }
    )
    ledger = _ledger_rows(store_path)
    duplicate_effects = {
        task_id: count
        for task_id, count in side_effect_counts.items()
        if task_id == replay_task_id and count > 1
    }
    memory_growth_bytes = (
        int(memory_samples[-1]["current_bytes"]) - int(memory_samples[0]["current_bytes"])
        if len(memory_samples) > 1
        else 0
    )

    assert time.monotonic() - started_at <= MAX_DURATION_SECONDS
    assert not drift_events, drift_events[:5]
    assert not duplicate_effects, duplicate_effects
    assert len(ledger) == len({row[0] for row in ledger})
    assert side_effect_counts[replay_task_id] == 1
    assert memory_growth_bytes < 5_000_000, {
        "memory_growth_bytes": memory_growth_bytes,
        "samples": memory_samples,
    }
