from __future__ import annotations

from pathlib import Path

import app.orchestrator.task_queue as task_queue_module
from app.context.shared_context_store import get_context
from app.orchestrator.task_factory import get_task, save_task
from app.orchestrator.task_queue import InMemoryTaskQueue
from app.system.gap_tasks import gap_to_task_input, ingest_system_gap, ingest_system_gaps


def _configure_queue_files(tmp_path: Path, monkeypatch) -> InMemoryTaskQueue:
    queue_file = tmp_path / "runtime" / "state" / "task_queue.json"
    task_log_file = tmp_path / "runtime" / "logs" / "tasks.log"
    monkeypatch.setattr(task_queue_module, "TASK_QUEUE_FILE", queue_file)
    monkeypatch.setattr(task_queue_module, "TASK_LOG_FILE", task_log_file)
    return InMemoryTaskQueue()


def _gap(
    *,
    severity: str,
    impact_score: str = "low",
    frequency: int = 1,
    task_id: str = "",
    interaction_id: str = "",
) -> dict[str, object]:
    return {
        "type": "system_gap",
        "severity": severity,
        "problem": "Repeated failures detected for `Lead followup` (2 occurrences).",
        "impact": "The same execution path is failing often enough to reduce reliability and waste retries.",
        "proposed_fix": "Add a guarded recovery path or validation step for this flow before execution is retried again.",
        "impact_score": impact_score,
        "frequency": frequency,
        "task_id": task_id,
        "interaction_id": interaction_id,
        "context_reference": f"active_task:{task_id}" if task_id else "system_context",
        "dedupe_key": "repeated_failures:lead_followup",
    }


def test_repeated_identical_gap_creates_single_task(tmp_path: Path, monkeypatch) -> None:
    queue = _configure_queue_files(tmp_path, monkeypatch)
    monkeypatch.setattr("app.system.gap_tasks.now", lambda: "2026-04-06T12:00:00Z")
    store_path = tmp_path / "tasks.json"

    created = ingest_system_gaps(
        [_gap(severity="medium", task_id="TASK-ROOT", interaction_id="interaction-1")] * 2,
        queue=queue,
        store_path=store_path,
    )

    assert len(created) == 2
    assert created[0]["task_id"] == created[1]["task_id"]
    assert created[0]["status"] == "AWAITING_APPROVAL"
    assert queue.queued_task_ids() == []


def test_repeated_low_priority_gap_is_batched_without_spamming_queue(tmp_path: Path, monkeypatch) -> None:
    queue = _configure_queue_files(tmp_path, monkeypatch)
    monkeypatch.setattr("app.system.gap_tasks.now", lambda: "2026-04-06T12:00:00Z")
    store_path = tmp_path / "tasks.json"

    created = ingest_system_gaps(
        [
            _gap(
                severity="low",
                impact_score="low",
                frequency=2,
                task_id="TASK-LOW",
                interaction_id="interaction-low",
            )
        ]
        * 2,
        queue=queue,
        store_path=store_path,
    )
    persisted = get_task(created[0]["task_id"], store_path=store_path)
    assert persisted is not None

    assert created[0]["task_id"] == created[1]["task_id"]
    assert created[0]["status"] == "CREATED"
    assert created[0]["approval_status"] == "approved"
    assert created[0]["payload"]["priority_level"] == "low"
    assert created[0]["payload"]["route_target"] == "batch_queue"
    assert created[0]["payload"]["auto_task_mode"] == "batched"
    assert queue.queued_task_ids() == []


def test_medium_priority_low_severity_auto_validates_and_enqueues_for_execution(tmp_path: Path, monkeypatch) -> None:
    queue = _configure_queue_files(tmp_path, monkeypatch)
    monkeypatch.setattr("app.system.gap_tasks.now", lambda: "2026-04-06T12:00:00Z")
    store_path = tmp_path / "tasks.json"

    created = ingest_system_gap(
        _gap(
            severity="low",
            impact_score="high",
            frequency=2,
            task_id="TASK-MEDIUM",
            interaction_id="interaction-medium",
        ),
        queue=queue,
        store_path=store_path,
    )

    assert created["status"] == "VALIDATED"
    assert created["approval_status"] == "approved"
    assert created["payload"]["priority_level"] == "medium"
    assert created["payload"]["route_target"] == "execution"
    assert created["payload"]["auto_task_mode"] == "queued"
    assert queue.queued_task_ids() == [created["task_id"]]


def test_high_priority_issue_routes_to_approval_queue_and_updates_context(tmp_path: Path, monkeypatch) -> None:
    queue = _configure_queue_files(tmp_path, monkeypatch)
    monkeypatch.setattr("app.system.gap_tasks.now", lambda: "2026-04-06T12:00:00Z")
    monkeypatch.setenv("ENV_ROLE", "local_dev")
    monkeypatch.setenv("DF_STORAGE_ROOT", str(tmp_path / "runtime"))
    store_path = tmp_path / "high_tasks.json"

    high = ingest_system_gap(
        {
            **_gap(severity="high", impact_score="high", frequency=4, task_id="TASK-HIGH"),
            "dedupe_key": "repeated_failures:high_path",
        },
        queue=queue,
        store_path=store_path,
    )

    assert high["status"] == "AWAITING_APPROVAL"
    assert high["approval_status"] == "pending"
    assert high["payload"]["requires_approval"] is True
    assert high["payload"]["priority_level"] == "high"
    assert high["payload"]["priority_score"] == 9
    assert high["payload"]["route_target"] == "approval_queue"
    assert queue.queued_task_ids() == []

    system_context = get_context("system_context", environ={"ENV_ROLE": "local_dev", "DF_STORAGE_ROOT": str(tmp_path / "runtime")}, root_dir=tmp_path)
    priority_context = dict(system_context["system_improvement_priorities"])["repeated_failures:high_path"]
    assert priority_context["task_id"] == high["task_id"]
    assert priority_context["priority_level"] == "high"
    assert priority_context["route_target"] == "approval_queue"


def test_gap_to_task_input_links_context_and_lineage() -> None:
    task_input = gap_to_task_input(
        _gap(
            severity="low",
            impact_score="high",
            frequency=2,
            task_id="TASK-SOURCE",
            interaction_id="interaction-42",
        )
    )

    assert task_input["intent"] == "system_improvement_task"
    assert task_input["interaction_id"] == "interaction-42"
    assert task_input["parent_task_id"] == "TASK-SOURCE"
    assert task_input["payload"]["type"] == "system_improvement_task"
    assert task_input["payload"]["source"] == "analyzer"
    assert task_input["payload"]["context_reference"] == "active_task:TASK-SOURCE"
    assert task_input["payload"]["lineage"]["source_task_id"] == "TASK-SOURCE"
    assert task_input["payload"]["priority_level"] == "medium"
    assert task_input["payload"]["priority_score"] == 6
    assert task_input["payload"]["core_impact"] is False
    assert task_input["payload"]["affected_files"] == []


def test_core_targeted_improvement_task_is_created_but_parked_for_approval(
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "tasks.json"

    created = save_task(
        {
            "task_contract_version": 1,
            "task_id": "DF-CORE-IMPROVEMENT-V1",
            "created_at": "2026-04-06T12:00:00Z",
            "last_updated_at": "2026-04-06T12:00:00Z",
            "status": "created",
            "approval_status": "approved",
            "intent": "system_improvement_task",
            "payload": {
                "summary": "Guard execution runner writes",
                "affected_files": ["app/orchestrator/execution_runner.py"],
                "core_impact": True,
                "requires_approval": False,
                "route_target": "execution",
                "priority": "NORMAL",
            },
            "notes": [],
            "history": [],
        },
        store_path=store_path,
    )

    assert created["status"] == "AWAITING_APPROVAL"
    assert created["approval_status"] == "pending"
    assert created["payload"]["requires_approval"] is True
    assert created["payload"]["route_target"] == "approval_queue"
    assert created["payload"]["priority"] == "HIGH"


def test_stress_mix_keeps_non_core_improvements_flowing_without_policy_violations(
    tmp_path: Path,
    monkeypatch,
) -> None:
    queue = _configure_queue_files(tmp_path, monkeypatch)
    monkeypatch.setattr("app.system.gap_tasks.now", lambda: "2026-04-06T12:00:00Z")
    monkeypatch.setenv("ENV_ROLE", "local_dev")
    monkeypatch.setenv("DF_STORAGE_ROOT", str(tmp_path / "runtime"))
    store_path = tmp_path / "stress_tasks.json"

    gaps: list[dict[str, object]] = []
    for i in range(8):
        gaps.append(
            {
                **_gap(
                    severity="low",
                    impact_score="high",
                    frequency=2 + (i % 4),
                    task_id=f"TASK-AUTO-{i}",
                    interaction_id=f"interaction-auto-{i}",
                ),
                "problem": f"Repeated failures detected for `auto-gap-{i % 4}`.",
                "dedupe_key": f"auto-gap-{i % 4}",
        }
    )
    for i in range(10):
        gaps.append(
            {
                **_gap(
                    severity="low",
                    impact_score="low",
                    frequency=1,
                    task_id=f"TASK-BATCH-{i}",
                    interaction_id=f"interaction-batch-{i}",
                ),
                "problem": f"Repeated failures detected for `batch-gap-{i % 5}`.",
                "dedupe_key": f"batch-gap-{i % 5}",
            }
        )
    for i in range(6):
        gaps.append(
            {
                **_gap(
                    severity="medium",
                    impact_score="medium",
                    frequency=2,
                    task_id=f"TASK-MED-{i}",
                    interaction_id=f"interaction-med-{i}",
                ),
                "problem": f"Repeated failures detected for `medium-gap-{i % 3}`.",
                "dedupe_key": f"medium-gap-{i % 3}",
            }
        )
    for i in range(6):
        gaps.append(
            {
                **_gap(
                    severity="high",
                    impact_score="high",
                    frequency=4,
                    task_id=f"TASK-HIGH-{i}",
                    interaction_id=f"interaction-high-{i}",
                ),
                "problem": f"Repeated failures detected for `high-gap-{i % 3}`.",
                "dedupe_key": f"high-gap-{i % 3}",
            }
        )

    created = ingest_system_gaps(gaps, queue=queue, store_path=store_path)
    unique_task_ids = {task["task_id"] for task in created}
    system_context = get_context(
        "system_context",
        environ={"ENV_ROLE": "local_dev", "DF_STORAGE_ROOT": str(tmp_path / "runtime")},
        root_dir=tmp_path,
    )
    priority_context = dict(system_context["system_improvement_priorities"])

    assert len(gaps) == 30
    assert len(unique_task_ids) == 15
    assert len(queue.queued_task_ids()) == 2
    assert len(queue.queued_task_ids()) == len(set(queue.queued_task_ids()))
    assert all(
        task["status"] == "AWAITING_APPROVAL"
        and task["approval_status"] == "pending"
        and task["payload"]["route_target"] == "approval_queue"
        for task in created
        if task["payload"]["severity"] == "high"
    )
    assert len(priority_context) == 15
