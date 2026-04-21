from __future__ import annotations

import json
from pathlib import Path

from app.memory import memory_registry
from app.policy.replay_protection import check_replay
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


def _store_trace(
    tmp_path: Path, *, task_id: str, status: str, relative_name: str
) -> dict[str, object]:
    trace_payload = create_execution_trace(
        {
            "task_id": task_id,
            "payload": {"domain": "dev"},
        },
        {
            "resolver": {"resolved_memory": []},
            "memory_policy": {"allowed": True, "reason": "no_recent_duplicate"},
            "conflict_gate": {"allowed": True, "reason": "no_cross_domain_conflict"},
            "final_decision": {
                "action": "execute" if status == "executed" else "block"
            },
            "execution_status": status,
        },
    )
    trace_path = storage_adapter.save_artifact(
        "dev",
        "execution_trace",
        trace_payload,
        overwrite=True,
        relative_path=Path("traces") / relative_name,
    )
    return json.loads(trace_path.read_text(encoding="utf-8"))


def test_check_replay_allows_when_no_prior_execution(
    monkeypatch, tmp_path: Path
) -> None:
    _configure_storage(monkeypatch, tmp_path)

    decision = check_replay({"task_id": "DF-REPLAY-ALLOW-V1"})

    assert decision == {
        "allowed": True,
        "reason": "not_previously_executed",
        "previous_trace_id": "",
        "action": "allow",
    }


def test_check_replay_blocks_when_prior_executed_trace_exists(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _configure_storage(monkeypatch, tmp_path)
    trace_record = _store_trace(
        tmp_path,
        task_id="DF-REPLAY-BLOCK-V1",
        status="executed",
        relative_name="DF-REPLAY-BLOCK-V1.json",
    )

    decision = check_replay({"task_id": "DF-REPLAY-BLOCK-V1"})

    assert decision == {
        "allowed": False,
        "reason": "already_executed",
        "previous_trace_id": str(trace_record["id"]),
        "action": "block",
    }


def test_check_replay_allows_when_prior_trace_was_blocked(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _configure_storage(monkeypatch, tmp_path)
    _store_trace(
        tmp_path,
        task_id="DF-REPLAY-BLOCKED-TRACE-V1",
        status="blocked",
        relative_name="DF-REPLAY-BLOCKED-TRACE-V1.json",
    )

    decision = check_replay({"task_id": "DF-REPLAY-BLOCKED-TRACE-V1"})

    assert decision == {
        "allowed": True,
        "reason": "not_previously_executed",
        "previous_trace_id": "",
        "action": "allow",
    }


def test_check_replay_allows_when_trace_task_id_differs(
    monkeypatch,
    tmp_path: Path,
) -> None:
    _configure_storage(monkeypatch, tmp_path)
    _store_trace(
        tmp_path,
        task_id="DF-REPLAY-OTHER-V1",
        status="executed",
        relative_name="DF-REPLAY-OTHER-V1.json",
    )

    decision = check_replay({"task_id": "DF-REPLAY-CURRENT-V1"})

    assert decision == {
        "allowed": True,
        "reason": "not_previously_executed",
        "previous_trace_id": "",
        "action": "allow",
    }


def test_run_codex_task_blocks_replay_after_first_success(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    _configure_storage(monkeypatch, tmp_path)

    task_path = tmp_path / "task-501.json"
    task_path.write_text(
        (
            "{\n"
            '  "task_id": 501,\n'
            '  "instruction": "Execute the task once",\n'
            '  "constraints": "Modify only necessary parts.",\n'
            '  "success_criteria": "First run succeeds and reruns are blocked."\n'
            "}\n"
        ),
        encoding="utf-8",
    )

    first_task, first_artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )
    second_task, second_artifact_path = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )
    third_task, _ = run_codex_task(
        task_path,
        artifact_dir=tmp_path / "artifacts",
        context_output_dir=tmp_path / "tasks" / "context",
        repo_root=tmp_path,
    )

    output = capsys.readouterr().out
    second_artifact = json.loads(second_artifact_path.read_text(encoding="utf-8"))

    assert first_task["task_id"] == 501
    assert first_artifact_path == tmp_path / "artifacts" / "task-501.txt"
    assert second_artifact_path == first_artifact_path
    assert second_task["replay_protection_decision"]["allowed"] is False
    assert second_task["replay_protection_decision"]["reason"] == "already_executed"
    assert second_task["failure_reason"] == "already_executed"
    assert second_artifact == {
        "instruction": "Execute the task once",
        "result_type": "replay_blocked",
        "reason": "already_executed",
        "previous_trace_id": second_task["replay_protection_decision"][
            "previous_trace_id"
        ],
        "action": "block",
        "replay_protection_decision": second_task["replay_protection_decision"],
        "execution_timeline": second_artifact["execution_timeline"],
        "step_metrics": second_artifact["step_metrics"],
        "retry_info": second_artifact["retry_info"],
    }
    assert second_task["execution_trace_artifact_path"] == str(
        tmp_path / "df-dev" / "artifacts" / "traces" / "501-replay-blocked.json"
    )
    assert third_task["replay_protection_decision"]["allowed"] is False
    assert "[REPLAY] blocked task=501 trace=" in output
