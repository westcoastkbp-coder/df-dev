from __future__ import annotations

import json
from pathlib import Path

import scripts.run_codex_task as run_codex_task_module
from app.memory import memory_registry
from app.storage import storage_adapter
from app.trace.execution_trace import create_execution_trace
from scripts.run_codex_task import run_codex_task


def _write_policy(tmp_path: Path) -> Path:
    policy_path = tmp_path / "config" / "contour_policy.json"
    policy_path.parent.mkdir(parents=True, exist_ok=True)
    policy_path.write_text(
        json.dumps(
            {
                "contours": {
                    "df-dev": {
                        "working_root": str(tmp_path / "df-dev"),
                    },
                    "ownerbox": {
                        "working_root": str(tmp_path / "ownerbox"),
                    },
                }
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return policy_path


def _configure_storage(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(storage_adapter, "POLICY_FILE", _write_policy(tmp_path))
    monkeypatch.setattr(
        memory_registry,
        "REGISTRY_FILE",
        tmp_path / "df-system" / "memory_registry.json",
    )


def _read_trace_payload(trace_path: Path) -> dict[str, object]:
    record = json.loads(trace_path.read_text(encoding="utf-8"))
    assert record["type"] == "execution_trace"
    assert record["memory_class"] == "trace"
    return record["payload"]


def test_create_execution_trace_contains_all_stages() -> None:
    trace = create_execution_trace(
        {
            "task_id": 44,
            "payload": {"domain": "dev"},
        },
        {
            "resolver": {"resolved_memory": [{"id": "artifact-1"}]},
            "memory_policy": {"allowed": True, "reason": "no_recent_duplicate"},
            "conflict_gate": {"allowed": True, "reason": "no_cross_domain_conflict"},
            "final_decision": {"action": "execute", "artifact_id": "task-44.txt"},
            "execution_status": "executed",
        },
    )

    assert trace["type"] == "execution_trace"
    assert trace["task_id"] == "44"
    assert trace["domain"] == "dev"
    assert isinstance(trace["timestamp"], str)
    assert trace["stages"] == {
        "resolver": {"resolved_memory": [{"id": "artifact-1"}]},
        "memory_policy": {"allowed": True, "reason": "no_recent_duplicate"},
        "conflict_gate": {"allowed": True, "reason": "no_cross_domain_conflict"},
        "replay_protection": {},
        "execution_invariants": {},
        "final_decision": {"action": "execute", "artifact_id": "task-44.txt"},
    }
    assert trace["result"] == {
        "status": "executed",
        "artifact_id": "task-44.txt",
    }


def test_run_codex_task_creates_execution_trace_for_executed_task_in_ownerbox(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure_storage(monkeypatch, tmp_path)

    task_path = tmp_path / "task-71.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 71,\n'
            '  "instruction": "Write the ownerbox artifact",\n'
            '  "constraints": "Modify only necessary parts.",\n'
            '  "success_criteria": "Trace is persisted for executed tasks.",\n'
            '  "domain": "ownerbox"\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    task, artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    output = capsys.readouterr().out
    trace_path = Path(str(task["execution_trace_artifact_path"]))
    trace = _read_trace_payload(trace_path)

    assert artifact_path == tmp_path / "artifacts" / "task-71.txt"
    assert trace_path == tmp_path / "ownerbox" / "artifacts" / "traces" / "71.json"
    assert "[TRACE] created task=71" in output
    assert trace["domain"] == "ownerbox"
    assert trace["result"] == {
        "status": "executed",
        "artifact_id": "task-71.txt",
    }
    assert trace["stages"]["resolver"] == {"resolved_memory": []}
    assert trace["stages"]["memory_policy"] == {
        "allowed": True,
        "reason": "no_recent_duplicate",
        "matched_artifact_id": None,
        "action": "continue",
    }
    assert trace["stages"]["conflict_gate"] == {
        "allowed": True,
        "reason": "no_cross_domain_conflict",
        "conflict_with": None,
        "action": "continue",
    }
    assert trace["stages"]["replay_protection"] == {
        "allowed": True,
        "reason": "not_previously_executed",
        "previous_trace_id": "",
        "action": "allow",
    }
    assert trace["stages"]["execution_invariants"] == {
        "allowed": True,
        "violations": [],
        "action": "allow",
    }
    assert trace["stages"]["final_decision"]["action"] == "execute"
    assert trace["stages"]["final_decision"]["artifact_path"] == str(artifact_path)


def test_run_codex_task_creates_execution_trace_for_blocked_task_in_dev(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure_storage(monkeypatch, tmp_path)

    task_path = tmp_path / "task-72.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 72,\n'
            '  "instruction": "Block the duplicate task",\n'
            '  "constraints": "Do not create duplicates.",\n'
            '  "success_criteria": "Trace is persisted for blocked tasks.",\n'
            '  "memory_context": {"domain": "dev", "type": "task"}\n'
            "}\n"
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        run_codex_task_module,
        "evaluate_memory_policy",
        lambda task_packet, resolved_memory: {
            "allowed": False,
            "reason": "recent_duplicate_detected",
            "matched_artifact_id": "artifact-123",
            "action": "block",
        },
    )

    task, artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    output = capsys.readouterr().out
    trace_path = Path(str(task["execution_trace_artifact_path"]))
    trace = _read_trace_payload(trace_path)

    assert artifact_path == tmp_path / "artifacts" / "task-72.txt"
    assert trace_path == tmp_path / "df-dev" / "artifacts" / "traces" / "72.json"
    assert "[TRACE] created task=72" in output
    assert trace["domain"] == "dev"
    assert trace["result"] == {
        "status": "blocked",
        "artifact_id": None,
    }
    assert trace["stages"]["resolver"] == {"resolved_memory": []}
    assert trace["stages"]["memory_policy"] == {
        "allowed": False,
        "reason": "recent_duplicate_detected",
        "matched_artifact_id": "artifact-123",
        "action": "block",
    }
    assert trace["stages"]["conflict_gate"] == {}
    assert trace["stages"]["replay_protection"] == {}
    assert trace["stages"]["execution_invariants"] == {}
    assert trace["stages"]["final_decision"] == {
        "action": "block",
        "gate": "memory_policy",
        "reason": "recent_duplicate_detected",
        "artifact_path": str(artifact_path),
    }
