from __future__ import annotations

import json
from pathlib import Path

import app.orchestrator.task_factory as task_factory_module
import app.orchestrator.task_queue as task_queue_module
import app.orchestrator.task_state_store as task_state_store_module
from app.execution.action_result import build_action_result
from app.orchestrator.task_factory import get_task, load_tasks, save_task
from app.orchestrator.task_queue import InMemoryTaskQueue
from functools import partial
from app.orchestrator.task_worker import process_next_queued_task as _process_next_queued_task
from tests.system_context import WORKING_SYSTEM_CONTEXT

process_next_queued_task = partial(_process_next_queued_task, system_context=WORKING_SYSTEM_CONTEXT)
from app.system.analyzer import ensure_gap_priority_metadata
from app.system.gap_tasks import ingest_system_gaps


def test_system_stability_rerun_v2(tmp_path: Path) -> None:
    store_path = tmp_path / "data" / "tasks.json"
    task_state_store_module.ROOT_DIR = tmp_path
    task_state_store_module.TASK_STATE_DB_FILE = Path("runtime/state/task_state.sqlite3")
    task_factory_module.TASK_SYSTEM_FILE = store_path
    task_factory_module.clear_task_runtime_store()
    task_queue_module.TASK_QUEUE_FILE = tmp_path / "runtime" / "state" / "task_queue.json"
    task_queue_module.TASK_LOG_FILE = tmp_path / "runtime" / "logs" / "tasks.log"
    queue = InMemoryTaskQueue()

    base_gap = {
        "type": "system_gap",
        "impact": "Burst load risks reliability and can strand work if routing or execution stalls.",
        "proposed_fix": "Preserve idempotent routing, keep non-core issues auto-flowing, and park risky core changes for approval.",
    }
    gaps: list[dict[str, object]] = []

    for i in range(20):
        pattern = i % 4
        gaps.append(
            {
                **base_gap,
                "severity": "low",
                "impact_score": "high",
                "frequency": 2 + (i % 4),
                "problem": f"Repeated failures detected for `auto-burst-{pattern}`.",
                "task_id": f"AUTO-{i}",
                "interaction_id": f"interaction-auto-{i}",
                "context_reference": f"active_task:AUTO-{i}",
                "dedupe_key": f"auto-burst:{pattern}",
            }
        )
    for i in range(10):
        pattern = i % 5
        gaps.append(
            {
                **base_gap,
                "severity": "low",
                "impact_score": "low",
                "frequency": 1,
                "problem": f"Repeated delays detected for `batch-burst-{pattern}`.",
                "task_id": f"BATCH-{i}",
                "interaction_id": f"interaction-batch-{i}",
                "context_reference": f"active_task:BATCH-{i}",
                "dedupe_key": f"batch-burst:{pattern}",
            }
        )
    for i in range(10):
        pattern = i % 5
        gaps.append(
            {
                **base_gap,
                "severity": "medium",
                "impact_score": "medium",
                "frequency": 2,
                "problem": f"Incomplete flow detected for `manual-burst-{pattern}`.",
                "task_id": f"MED-{i}",
                "interaction_id": f"interaction-med-{i}",
                "context_reference": f"active_task:MED-{i}",
                "dedupe_key": f"manual-burst:{pattern}",
            }
        )
    for i in range(10):
        pattern = i % 5
        gaps.append(
            {
                **base_gap,
                "severity": "high",
                "impact_score": "high",
                "frequency": 4 + (i % 2),
                "problem": f"Core-impact regression detected for `core-burst-{pattern}`.",
                "task_id": f"CORE-{i}",
                "interaction_id": f"interaction-core-{i}",
                "context_reference": f"active_task:CORE-{i}",
                "dedupe_key": f"core-burst:{pattern}",
            }
        )

    created = ingest_system_gaps(
        [ensure_gap_priority_metadata(gap) for gap in gaps],
        queue=queue,
        store_path=store_path,
    )

    for task_id in sorted(
        {
            task["task_id"]
            for task in created
            if "Core-impact regression" in str(task.get("payload", {}).get("problem", ""))
        }
    ):
        task = get_task(task_id, store_path=store_path)
        assert task is not None
        payload = dict(task.get("payload", {}) or {})
        payload["affected_files"] = ["app/orchestrator/execution_runner.py"]
        task["payload"] = payload
        save_task(task, store_path=store_path)

    def fake_executor(task_data: dict[str, object]) -> dict[str, object]:
        return build_action_result(
            status="completed",
            task_id=task_data.get("task_id"),
            action_type=str(task_data.get("intent", "")).strip().upper(),
            result_payload={
                "summary": str(dict(task_data.get("payload", {}) or {}).get("summary", "")).strip(),
            },
            error_code="",
            error_message="",
            source="test_system_stability_rerun_v2_tmp",
        )

    fake_executor.__module__ = "test_system_stability_rerun_v2_tmp"

    execution_failures: list[str] = []
    while True:
        result = process_next_queued_task(
            queue=queue,
            now=lambda: "2026-04-06T20:00:00Z",
            fetch_task=lambda task_id: get_task(task_id, store_path=store_path),
            persist=lambda task_data: save_task(task_data, store_path=store_path),
            timeout=0.0,
            executor=fake_executor,
            telemetry_collector=lambda: {},
            network_snapshot_collector=lambda: {},
            decision_resolver=lambda *args, **kwargs: {
                "execution_mode": "LOCAL",
                "execution_compute_mode": "cpu_mode",
            },
            token_cost_resolver=lambda *args, **kwargs: {},
            active_task_loader=lambda: load_tasks(store_path),
        )
        if result is None:
            break
        status = str(result.get("status", "")).strip().upper()
        if status not in {"COMPLETED", "VALIDATED", "AWAITING_APPROVAL", "CREATED"}:
            execution_failures.append(status)

    persisted_tasks = load_tasks(store_path)
    unique_created_task_ids = {str(task.get("task_id", "")).strip() for task in created}
    persisted_task_ids = [str(task.get("task_id", "")).strip() for task in persisted_tasks]
    queued_ids = queue.queued_task_ids()

    policy_violations = any(
        str((task.get("result") or {}).get("status", "")).strip().lower()
        in {"policy_blocked", "execution_boundary_violation", "invalid_action_result"}
        for task in persisted_tasks
    ) or bool(execution_failures)

    duplicates_detected = (
        len(persisted_task_ids) != len(set(persisted_task_ids))
        or len(queued_ids) != len(set(queued_ids))
    )

    system_stable = (
        len(gaps) == 50
        and len(unique_created_task_ids) > 0
        and not duplicates_detected
        and not policy_violations
        and queue.is_idle()
        and all(str(task.get("status", "")).strip().upper() != "FAILED" for task in persisted_tasks)
    )

    summary = {
        "total_gaps": len(gaps),
        "tasks_created": len(unique_created_task_ids),
        "duplicates_detected": duplicates_detected,
        "system_stable": system_stable,
        "policy_violations": policy_violations,
        "PASS / FAIL": (
            "PASS"
            if len(unique_created_task_ids) > 0
            and system_stable
            and not policy_violations
            and not duplicates_detected
            else "FAIL"
        ),
    }
    print(json.dumps(summary))

    assert summary["total_gaps"] == 50
    assert summary["tasks_created"] > 0
    assert summary["duplicates_detected"] is False
    assert summary["system_stable"] is True
    assert summary["policy_violations"] is False
    assert summary["PASS / FAIL"] == "PASS"

