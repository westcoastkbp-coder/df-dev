from __future__ import annotations

import copy
import json
from pathlib import Path

import memory.memory_store as memory_store_module
import scripts.run_command as run_command_module


def _prime_required_execution_context(memory_dir: Path) -> None:
    memory_store_module.write_execution_system_context(
        copy.deepcopy(memory_store_module.DEFAULT_EXECUTION_SYSTEM_CONTEXT),
        memory_dir=memory_dir,
    )


def test_main_success_output_and_execution_log_include_decision_trace(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    context_dir = tmp_path / "context"
    task_path = tmp_path / "tasks" / "codex" / "auto-task.json"
    artifact_dir = tmp_path / "artifacts"
    state_path = tmp_path / "state" / "state.json"
    execution_log_path = tmp_path / "logs" / "execution_log.jsonl"
    doc_url = "https://docs.google.com/document/d/doc-trace"

    monkeypatch.setattr(run_command_module, "CONTEXT_DIR", context_dir)
    monkeypatch.setattr(run_command_module, "TASK_OUTPUT_PATH", task_path)
    monkeypatch.setattr(run_command_module, "ARTIFACT_DIR", artifact_dir)
    monkeypatch.setattr(run_command_module, "COMMAND_STATE_FILE", state_path)
    monkeypatch.setattr(run_command_module, "EXECUTION_LOG_PATH", execution_log_path)
    monkeypatch.setattr(run_command_module, "_timestamp_task_id", lambda: 202604120101)
    memory_dir = tmp_path / "memory"
    monkeypatch.setattr(memory_store_module, "MEMORY_DIR", memory_dir)
    _prime_required_execution_context(memory_dir)

    def fake_run_codex_task(task_source, artifact_dir=None, repo_root=None):
        payload = json.loads(Path(task_source).read_text(encoding="utf-8"))
        artifact_path = Path(artifact_dir) / f"pipeline-{payload['task_id']}.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps({"final_output": {"url": doc_url}}, indent=2) + "\n",
            encoding="utf-8",
        )
        return {
            "task_id": payload["task_id"],
            "doc_url": doc_url,
        }, artifact_path

    monkeypatch.setattr(
        run_command_module.run_codex_task_module,
        "run_codex_task",
        fake_run_codex_task,
    )

    exit_code = run_command_module.main(["create doc from analysis"])

    assert exit_code == 0
    output_payload = json.loads(capsys.readouterr().out.strip())
    assert output_payload["decision_trace"] == {
        "reason": "create doc from analysis executed successfully",
        "context_used": output_payload["decision_trace"]["context_used"],
        "action_type": "create doc from analysis",
        "policy_result": "allowed: context loaded and command routed",
        "confidence": "high",
        "vendor": "google",
    }

    execution_entries = [
        json.loads(line)
        for line in execution_log_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert execution_entries[-1]["decision_trace"] == output_payload["decision_trace"]
