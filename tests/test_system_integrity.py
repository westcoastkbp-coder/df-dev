from __future__ import annotations

import json
from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import control.audit_log as audit_log
import control.codex_loop as codex_loop
import control.memory as memory


class _SuccessfulExecution:
    returncode = 0


class _FailedExecution:
    returncode = 1


class _SuccessfulValidation:
    returncode = 0


def _base_context() -> dict[str, object]:
    return {
        "system": "Digital Foreman",
        "status": "NOT_WORKING",
        "modules": {},
        "broken_modules": ["execution_replay"],
        "broken": {"execution_replay": "failing"},
        "known_issues": [],
        "history": [],
        "last_update": "",
        "modules_state": {},
        "last_codex_loop": {},
        "next_required": "",
        "strategy_history": [],
    }


def _configure_storage(monkeypatch, tmp_path: Path) -> tuple[Path, Path]:
    context_path = tmp_path / "system_context.json"
    audit_path = tmp_path / "audit_log.jsonl"
    monkeypatch.setattr(memory, "CTX_PATH", context_path)
    monkeypatch.setattr(audit_log, "LOG_PATH", audit_path)
    return context_path, audit_path


def _approved_review(task_id: str = "execution_replay") -> dict[str, object]:
    return {
        "status": "review_complete",
        "decision": "APPROVED",
        "packet": {"task_id": task_id},
        "claude": {"status": "ok"},
        "gemini": {"verdict": "VERIFIED"},
    }


def _blocked_review(task_id: str = "execution_replay") -> dict[str, object]:
    return {
        "status": "review_complete",
        "decision": "BLOCKED",
        "packet": {"task_id": task_id},
        "claude": {"status": "ok"},
        "gemini": {"verdict": "NOT_VERIFIED"},
    }


def _read_output(capsys) -> dict[str, object]:
    return json.loads(capsys.readouterr().out.strip())


def test_system_integrity_verified_flow_is_consistent(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    _configure_storage(monkeypatch, tmp_path)
    memory.save_context(_base_context())

    run_test_calls: list[str] = []

    def fail_initial_test(test_path: Path) -> tuple[str, str]:
        run_test_calls.append(str(test_path))
        return "fail", "broken"

    monkeypatch.setattr(codex_loop, "run_test", fail_initial_test)
    monkeypatch.setattr(
        codex_loop, "execute_fix_task", lambda prompt: _SuccessfulExecution()
    )
    monkeypatch.setattr(
        codex_loop, "run_in_dev_env", lambda *args, **kwargs: _SuccessfulValidation()
    )
    monkeypatch.setattr(
        codex_loop, "run_external_review", lambda *args, **kwargs: _approved_review()
    )
    monkeypatch.setattr(codex_loop, "get_git_commit", lambda: "abc123")
    monkeypatch.setattr(codex_loop, "get_git_branch", lambda: "codex/system-integrity")

    codex_loop.main()
    output = _read_output(capsys)

    context = memory.load_context()
    audit_rows = audit_log.read_audit_log()
    trace = context["decision_trace"]

    assert output == {"status": "pass", "fixed": "execution_replay"}
    assert len(run_test_calls) == 1
    assert context["status"] == "WORKING"
    assert context["broken"] == {}
    assert context["broken_modules"] == []
    assert context["modules_state"]["execution_replay"]["status"] == "WORKING"
    assert context["last_codex_loop"]["status"] == "pass"
    assert context["last_codex_loop"]["local_test"] == "pass"
    assert context["last_codex_loop"]["review"]["decision"] == "APPROVED"
    assert context["last_strategy"] == "code_fix"
    assert context["strategy_feedback"] == {"strategy": "code_fix", "result": "success"}
    assert context["strategy_history"][-1] == context["strategy_feedback"]
    assert context["git"]["commit"] == "abc123"
    assert context["git"]["branch"] == "codex/system-integrity"
    assert "time" in context["git"]
    assert trace["type"] == "review_gate"
    assert trace["module"] == "execution_replay"
    assert trace["review"] == "APPROVED"
    assert trace["gemini_verdict"] == "VERIFIED"
    assert [row["status"] for row in audit_rows] == ["FAIL", "WORKING"]
    assert audit_rows[-1]["module"] == "execution_replay"
    assert audit_rows[-1]["status"] == "WORKING"
    assert audit_rows[-1]["review"] == "APPROVED"
    assert audit_rows[-1]["decision_trace"] == trace
    assert audit_rows[-1]["strategy_feedback"] == context["strategy_feedback"]


def test_system_integrity_blocked_review_preserves_guardrails(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    _configure_storage(monkeypatch, tmp_path)
    memory.save_context(_base_context())

    monkeypatch.setattr(codex_loop, "run_test", lambda test_path: ("pass", "ok"))
    monkeypatch.setattr(
        codex_loop, "run_external_review", lambda *args, **kwargs: _blocked_review()
    )
    monkeypatch.setattr(codex_loop, "get_git_commit", lambda: "def456")
    monkeypatch.setattr(codex_loop, "get_git_branch", lambda: "codex/system-integrity")

    codex_loop.main()
    output = _read_output(capsys)

    context = memory.load_context()
    audit_rows = audit_log.read_audit_log()
    trace = context["decision_trace"]

    assert output["status"] == "blocked_by_review"
    assert output["module"] == "execution_replay"
    assert context["status"] == "NOT_WORKING"
    assert context["broken"]["execution_replay"] == "failing"
    assert context["modules_state"]["execution_replay"]["status"] == "BLOCKED"
    assert context["last_codex_loop"]["status"] == "blocked_by_review"
    assert context["last_codex_loop"]["local_test"] == "pass"
    assert context["last_codex_loop"]["review"]["decision"] == "BLOCKED"
    assert context["last_strategy"] == "logic_review"
    assert context["strategy_feedback"] == {
        "strategy": "logic_review",
        "result": "failure",
    }
    assert trace["type"] == "review_gate"
    assert trace["review"] == "BLOCKED"
    assert trace["gemini_verdict"] == "NOT_VERIFIED"
    assert [row["status"] for row in audit_rows] == ["BLOCKED"]
    assert audit_rows[0]["review"] == "BLOCKED"
    assert audit_rows[0]["decision_trace"] == trace
    assert all(row["status"] != "ESCALATED" for row in audit_rows)


def test_system_integrity_repeated_failures_escalate_and_stop(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    _configure_storage(monkeypatch, tmp_path)
    memory.save_context(_base_context())

    run_test_calls = 0
    execute_fix_calls = 0
    outputs: list[dict[str, object]] = []

    def always_fail_test(test_path: Path) -> tuple[str, str]:
        nonlocal run_test_calls
        run_test_calls += 1
        return "fail", "broken"

    def always_fail_fix(prompt: str) -> _FailedExecution:
        nonlocal execute_fix_calls
        execute_fix_calls += 1
        return _FailedExecution()

    monkeypatch.setattr(codex_loop, "run_test", always_fail_test)
    monkeypatch.setattr(codex_loop, "execute_fix_task", always_fail_fix)

    for _ in range(3):
        codex_loop.main()
        outputs.append(_read_output(capsys))

    context = memory.load_context()
    audit_rows = audit_log.read_audit_log()

    assert outputs[:2] == [
        {
            "strategy": "code_fix",
            "instruction": "Fix failing test in execution_replay",
            "error_type": "execution_error",
        },
        {
            "strategy": "code_fix",
            "instruction": "Fix failing test in execution_replay",
            "error_type": "execution_error",
        },
    ]
    assert outputs[-1]["status"] == "escalated"
    assert outputs[-1]["reason"] == "repeated_failures"
    assert run_test_calls == 2
    assert execute_fix_calls == 2
    assert context["status"] == "NOT_WORKING"
    assert (
        context["next_required"] == "manual intervention required for execution_replay"
    )
    assert context["decision_trace"] == {
        "source": "memory_analysis",
        "trigger": "repeated_failures",
        "confidence": "high",
    }
    assert [row["status"] for row in audit_rows] == [
        "FAIL",
        "FAIL",
        "FAIL",
        "FAIL",
        "ESCALATED",
    ]
    assert audit_rows[-1]["decision_trace"] == {
        "type": "escalation",
        "reason": "repeated_failures",
    }


def test_system_integrity_recovers_from_corrupted_memory_and_executes(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    context_path, _ = _configure_storage(monkeypatch, tmp_path)
    context_path.write_text("{broken json", encoding="utf-8")

    recovered = memory.load_context()
    assert recovered == memory.default_context()

    recovered.update(_base_context())
    memory.save_context(recovered)

    monkeypatch.setattr(codex_loop, "run_test", lambda test_path: ("pass", "ok"))
    monkeypatch.setattr(
        codex_loop, "run_external_review", lambda *args, **kwargs: _approved_review()
    )
    monkeypatch.setattr(codex_loop, "get_git_commit", lambda: "ghi789")
    monkeypatch.setattr(codex_loop, "get_git_branch", lambda: "codex/system-recovery")

    codex_loop.main()
    output = _read_output(capsys)

    context = memory.load_context()
    audit_rows = audit_log.read_audit_log()

    assert output == {"status": "pass", "fixed": "execution_replay"}
    assert context["status"] == "WORKING"
    assert context["broken"] == {}
    assert context["broken_modules"] == []
    assert context["last_codex_loop"]["status"] == "pass"
    assert context["last_codex_loop"]["review"]["decision"] == "APPROVED"
    assert context["decision_trace"]["type"] == "review_gate"
    assert context["decision_trace"]["review"] == "APPROVED"
    assert audit_rows[-1]["status"] == "WORKING"
    assert audit_rows[-1]["review"] == "APPROVED"
