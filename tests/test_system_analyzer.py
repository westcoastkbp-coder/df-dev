from __future__ import annotations

import json
from pathlib import Path

from app.system.analyzer import analyze_system, analyze_system_gap_inputs


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, *entries: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(entry, ensure_ascii=True) for entry in entries)
        + ("\n" if entries else ""),
        encoding="utf-8",
    )


def _task_context(
    *,
    task_id: str,
    status: str,
    summary: str,
    approval_status: str = "approved",
    intent: str = "followup",
    payload: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "task_id": task_id,
        "status": status,
        "approval_status": approval_status,
        "intent": intent,
        "summary": summary,
        "payload": payload or {"summary": summary, "intent": intent},
    }


def test_analyzer_detects_repeated_failures_and_delays() -> None:
    gaps = analyze_system_gap_inputs(
        task_contexts=[
            _task_context(task_id="TASK-1", status="FAILED", summary="Lead followup"),
            _task_context(
                task_id="TASK-2",
                status="DEFERRED",
                summary="Invoice sync",
                payload={"summary": "Invoice sync", "offload_latency": 420},
            ),
        ],
        audit_events=[
            {
                "timestamp": "2026-04-06T10:00:00Z",
                "event_type": "execution_failed",
                "task_id": "TASK-1",
            },
            {
                "timestamp": "2026-04-06T10:01:00Z",
                "event_type": "execution_failed",
                "task_id": "TASK-1",
            },
            {
                "timestamp": "2026-04-06T10:02:00Z",
                "event_type": "execution_deferred",
                "task_id": "TASK-2",
                "payload": {"delay_seconds": 420},
            },
        ],
    )

    problems = {gap["problem"] for gap in gaps}
    assert any("Repeated failures detected" in problem for problem in problems)
    assert any("Repeated delays detected" in problem for problem in problems)
    failure_gap = next(
        gap for gap in gaps if "Repeated failures detected" in gap["problem"]
    )
    delay_gap = next(
        gap for gap in gaps if "Repeated delays detected" in gap["problem"]
    )
    assert failure_gap["impact_score"] == "high"
    assert failure_gap["frequency"] == 3
    assert failure_gap["priority_level"] == "high"
    assert delay_gap["impact_score"] == "medium"
    assert delay_gap["frequency"] == 2
    assert delay_gap["priority_level"] == "medium"


def test_analyzer_detects_missing_execution_path() -> None:
    gaps = analyze_system_gap_inputs(
        task_contexts=[
            _task_context(
                task_id="TASK-MISSING",
                status="VALIDATED",
                summary="Missing route task",
                approval_status="approved",
            )
        ],
        audit_events=[],
    )

    assert gaps == [
        {
            "type": "system_gap",
            "severity": "high",
            "problem": "Missing execution path detected for task `Missing route task` (TASK-MISSING).",
            "impact": "The task exists in active context but no execution-start event was recorded, which can leave work stranded.",
            "proposed_fix": "Register or route this task type to an execution handler and emit an `execution_started` audit event when work begins.",
            "impact_score": "high",
            "frequency": 1,
            "urgency": "immediate",
            "priority_score": 7,
            "priority_level": "high",
            "task_id": "TASK-MISSING",
            "interaction_id": "",
            "context_reference": "active_task:TASK-MISSING",
            "dedupe_key": "missing_execution_path:TASK-MISSING",
        }
    ]


def test_analyzer_detects_incomplete_flow() -> None:
    gaps = analyze_system_gap_inputs(
        task_contexts=[
            _task_context(
                task_id="TASK-FLOW", status="EXECUTING", summary="Partial workflow"
            ),
        ],
        audit_events=[
            {
                "timestamp": "2026-04-06T10:00:00Z",
                "event_type": "execution_started",
                "task_id": "TASK-FLOW",
            },
            {
                "timestamp": "2026-04-06T10:20:00Z",
                "event_type": "context_set",
                "task_id": "OTHER-TASK",
            },
        ],
    )

    assert any(
        gap["problem"]
        == "Incomplete flow detected for task `Partial workflow` (TASK-FLOW)."
        for gap in gaps
    )


def test_analyzer_reads_shared_context_and_runtime_files(
    tmp_path: Path, monkeypatch
) -> None:
    env = {
        "ENV_ROLE": "local_dev",
        "DF_STORAGE_ROOT": str(tmp_path / "runtime" / "local_dev"),
    }
    storage_root = Path(env["DF_STORAGE_ROOT"])
    shared_context_dir = storage_root / "shared_context"
    active_threads_dir = shared_context_dir / "active_threads"

    _write_json(
        shared_context_dir / "system_context.json",
        {
            "schema_version": "v1",
            "scope": "system",
            "updated_at": "",
            "value": {"known_gap": "No remediation task generator"},
        },
    )
    _write_json(
        shared_context_dir / "global_context.json",
        {"schema_version": "v1", "scope": "global", "updated_at": "", "value": {}},
    )
    _write_json(
        active_threads_dir / "task-task-gap.json",
        {
            "task_id": "TASK-GAP",
            "status": "VALIDATED",
            "approval_status": "approved",
            "summary": "Runtime orphan",
            "payload": {"summary": "Runtime orphan"},
        },
    )
    _write_jsonl(shared_context_dir / "audit_trail.jsonl")
    _write_jsonl(shared_context_dir / "interaction_history.jsonl")

    gaps = analyze_system(root_dir=tmp_path, environ=env)

    assert any(
        gap["problem"]
        == "Shared context reports a known gap: No remediation task generator"
        for gap in gaps
    )
    assert any("Missing execution path detected" in gap["problem"] for gap in gaps)
