from __future__ import annotations

import copy
import json
from pathlib import Path
import sys

import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import control.analysis as analysis
import control.audit_log as audit_log
import control.codex_loop as codex_loop
from control.decision_contract import build_escalation_decision
import control.git_trace as git_trace
import control.memory as memory


def _base_context(**overrides: object) -> dict[str, object]:
    context: dict[str, object] = {
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
    context.update(copy.deepcopy(overrides))
    return context


def _patch_loop_state(
    monkeypatch: pytest.MonkeyPatch,
    state: dict[str, object],
    *,
    blocked_history: list[dict[str, object]] | None = None,
) -> None:
    monkeypatch.setattr(codex_loop, "load_context", lambda: state["context"])
    monkeypatch.setattr(
        codex_loop,
        "save_context",
        lambda payload: state.__setitem__("context", copy.deepcopy(payload)),
    )
    monkeypatch.setattr(
        codex_loop,
        "log_execution",
        lambda entry: state["audit"].append(copy.deepcopy(entry)),
    )
    monkeypatch.setattr(
        codex_loop,
        "read_audit_log",
        lambda limit=50: copy.deepcopy((blocked_history or state["audit"])[-limit:]),
    )


def _approved_review() -> dict[str, object]:
    return {
        "status": "review_complete",
        "decision": "APPROVED",
        "packet": {"task_id": "execution_replay"},
        "claude": {"status": "ok"},
        "gemini": {"verdict": "VERIFIED"},
    }


def _blocked_review() -> dict[str, object]:
    return {
        "status": "review_complete",
        "decision": "BLOCKED",
        "packet": {"task_id": "execution_replay"},
        "claude": {"status": "ok"},
        "gemini": {"verdict": "NOT_VERIFIED"},
    }


def _assert_escalation_trace(payload: dict[str, object]) -> None:
    trace = payload["decision_trace"]
    assert isinstance(trace, dict)
    assert trace["source"] == "memory_analysis"
    assert isinstance(trace["trigger"], str)
    assert trace["trigger"]
    assert trace["confidence"] == "high"


def _assert_audit_escalation_trace(entry: dict[str, object]) -> None:
    trace = entry["decision_trace"]
    assert isinstance(trace, dict)
    assert trace["type"] == "escalation"
    assert isinstance(trace["reason"], str)
    assert trace["reason"]


class _FailedExecution:
    returncode = 1


class _SuccessfulExecution:
    returncode = 0


class _SuccessfulValidation:
    returncode = 0


def test_memory_destruction_missing_system_context_recovers(
    monkeypatch, tmp_path: Path
) -> None:
    target = tmp_path / "system_context.json"
    monkeypatch.setattr(memory, "CTX_PATH", target)

    assert memory.load_context() == memory.default_context()


def test_memory_destruction_corrupted_json_recovers(
    monkeypatch, tmp_path: Path
) -> None:
    target = tmp_path / "system_context.json"
    target.write_text("{broken json", encoding="utf-8")
    monkeypatch.setattr(memory, "CTX_PATH", target)

    assert memory.load_context() == memory.default_context()


def test_memory_destruction_partial_json_restores_missing_defaults(
    monkeypatch, tmp_path: Path
) -> None:
    target = tmp_path / "system_context.json"
    target.write_text(
        json.dumps({"system": "Digital Foreman", "status": "WORKING"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(memory, "CTX_PATH", target)

    result = memory.load_context()

    assert result["system"] == "Digital Foreman"
    assert result["status"] == "WORKING"
    assert result["modules"] == {}
    assert result["broken_modules"] == []
    assert result["broken"] == {}
    assert result["known_issues"] == []
    assert result["history"] == []
    assert result["modules_state"] == {}
    assert result["last_codex_loop"] == {}
    assert result["next_required"] == ""


def test_memory_destruction_wrong_field_types_restore_defaults(
    monkeypatch, tmp_path: Path
) -> None:
    target = tmp_path / "system_context.json"
    target.write_text(
        json.dumps(
            {
                "system": 42,
                "status": ["WORKING"],
                "modules": [],
                "broken_modules": "execution_replay",
                "broken": [],
                "known_issues": {},
                "history": {},
                "last_update": [],
                "modules_state": [],
                "last_codex_loop": [],
                "next_required": {},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(memory, "CTX_PATH", target)

    result = memory.load_context()
    default = memory.default_context()

    assert result["system"] == default["system"]
    assert result["status"] == default["status"]
    assert result["modules"] == default["modules"]
    assert result["broken_modules"] == default["broken_modules"]
    assert result["broken"] == default["broken"]
    assert result["known_issues"] == default["known_issues"]
    assert result["history"] == default["history"]
    assert result["last_update"] == default["last_update"]
    assert result["modules_state"] == default["modules_state"]
    assert result["last_codex_loop"] == default["last_codex_loop"]
    assert result["next_required"] == default["next_required"]


def test_memory_destruction_empty_file_recovers(monkeypatch, tmp_path: Path) -> None:
    target = tmp_path / "system_context.json"
    target.write_text("", encoding="utf-8")
    monkeypatch.setattr(memory, "CTX_PATH", target)

    assert memory.load_context() == memory.default_context()


def test_audit_destruction_missing_audit_file_is_safe(
    monkeypatch, tmp_path: Path
) -> None:
    target = tmp_path / "audit_log.jsonl"
    monkeypatch.setattr(audit_log, "LOG_PATH", target)

    assert audit_log.read_audit_log() == []


def test_audit_destruction_broken_jsonl_row_is_skipped(
    monkeypatch, tmp_path: Path
) -> None:
    target = tmp_path / "audit_log.jsonl"
    target.write_text("{bad row}\n", encoding="utf-8")
    monkeypatch.setattr(audit_log, "LOG_PATH", target)

    assert audit_log.read_audit_log() == []


def test_audit_destruction_mixed_rows_keep_valid_entries(
    monkeypatch, tmp_path: Path
) -> None:
    target = tmp_path / "audit_log.jsonl"
    target.write_text(
        "\n".join(
            [
                json.dumps({"module": "execution_replay", "status": "FAIL"}),
                "{broken row}",
                json.dumps({"module": "execution_replay", "status": "WORKING"}),
                json.dumps(["not", "an", "entry"]),
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(audit_log, "LOG_PATH", target)

    rows = audit_log.read_audit_log()

    assert rows == [
        {"module": "execution_replay", "status": "FAIL"},
        {"module": "execution_replay", "status": "WORKING"},
    ]


def test_audit_destruction_empty_file_is_safe(monkeypatch, tmp_path: Path) -> None:
    target = tmp_path / "audit_log.jsonl"
    target.write_text("", encoding="utf-8")
    monkeypatch.setattr(audit_log, "LOG_PATH", target)

    assert audit_log.read_audit_log() == []


def test_audit_destruction_corrupt_history_does_not_false_escalate(
    monkeypatch, tmp_path: Path
) -> None:
    target = tmp_path / "audit_log.jsonl"
    target.write_text(
        "\n".join(
            [
                "{bad row}",
                json.dumps({"module": "execution_replay", "status": "FAIL"}),
                '{"module":"execution_replay"',
            ]
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(audit_log, "LOG_PATH", target)

    assert analysis.detect_unstable_module("execution_replay") == {
        "unstable": False,
        "reason": "",
    }


def test_false_success_attack_missing_review_cannot_mark_working(
    monkeypatch, capsys
) -> None:
    state = {"context": _base_context(), "audit": []}
    _patch_loop_state(monkeypatch, state)
    monkeypatch.setattr(
        codex_loop,
        "detect_unstable_module",
        lambda module: {"unstable": False, "reason": ""},
    )
    monkeypatch.setattr(codex_loop, "run_test", lambda test_path: ("pass", "ok"))
    monkeypatch.setattr(codex_loop, "run_external_review", lambda *args, **kwargs: None)
    monkeypatch.setattr(codex_loop, "get_git_commit", lambda: "abc123")
    monkeypatch.setattr(codex_loop, "get_git_branch", lambda: "main")

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output["status"] == "blocked_by_review"
    assert state["context"]["status"] != "WORKING"
    assert state["context"]["modules_state"]["execution_replay"]["status"] == "BLOCKED"
    assert state["audit"][-1]["status"] == "BLOCKED"


def test_false_success_attack_blocked_review_stays_blocked(monkeypatch, capsys) -> None:
    state = {"context": _base_context(), "audit": []}
    _patch_loop_state(monkeypatch, state)
    monkeypatch.setattr(
        codex_loop,
        "detect_unstable_module",
        lambda module: {"unstable": False, "reason": ""},
    )
    monkeypatch.setattr(codex_loop, "run_test", lambda test_path: ("pass", "ok"))
    monkeypatch.setattr(
        codex_loop, "run_external_review", lambda *args, **kwargs: _blocked_review()
    )
    monkeypatch.setattr(codex_loop, "get_git_commit", lambda: "abc123")
    monkeypatch.setattr(codex_loop, "get_git_branch", lambda: "main")

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output["status"] == "blocked_by_review"
    assert state["context"]["status"] != "WORKING"
    assert state["context"]["modules_state"]["execution_replay"]["status"] == "BLOCKED"
    assert state["audit"][-1]["status"] == "BLOCKED"


def test_false_success_attack_malformed_review_payload_cannot_mark_working(
    monkeypatch, capsys
) -> None:
    state = {"context": _base_context(), "audit": []}
    _patch_loop_state(monkeypatch, state)
    monkeypatch.setattr(
        codex_loop,
        "detect_unstable_module",
        lambda module: {"unstable": False, "reason": ""},
    )
    monkeypatch.setattr(codex_loop, "run_test", lambda test_path: ("pass", "ok"))
    monkeypatch.setattr(
        codex_loop,
        "run_external_review",
        lambda *args, **kwargs: {
            "status": "review_complete",
            "decision": "APPROVED",
            "packet": "broken",
            "claude": ["bad"],
            "gemini": {},
        },
    )
    monkeypatch.setattr(codex_loop, "get_git_commit", lambda: "abc123")
    monkeypatch.setattr(codex_loop, "get_git_branch", lambda: "main")

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output["status"] == "blocked_by_review"
    assert state["context"]["status"] != "WORKING"
    assert state["context"]["modules_state"]["execution_replay"]["status"] == "BLOCKED"
    assert state["audit"][-1]["status"] == "BLOCKED"


def test_false_success_attack_missing_decision_trace_never_becomes_working(
    monkeypatch, capsys
) -> None:
    state = {"context": _base_context(), "audit": []}
    _patch_loop_state(monkeypatch, state)
    monkeypatch.setattr(
        codex_loop,
        "detect_unstable_module",
        lambda module: {"unstable": True, "reason": "repeated_failures"},
    )
    monkeypatch.setattr(
        codex_loop,
        "build_escalation_decision",
        lambda module, reason: {
            "status": "escalated",
            "module": module,
            "reason": reason,
            "action": "manual_intervention_required",
        },
    )
    monkeypatch.setattr(
        codex_loop,
        "run_test",
        lambda test_path: (_ for _ in ()).throw(
            AssertionError("run_test should not execute")
        ),
    )

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output["status"] == "escalated"
    assert state["context"]["status"] != "WORKING"
    assert state["audit"][-1]["status"] == "ESCALATED"
    assert "decision_trace" not in output


def test_escalation_loop_attack_repeated_fail_entries_trigger_escalation(
    monkeypatch, capsys
) -> None:
    state = {"context": _base_context(), "audit": []}
    _patch_loop_state(monkeypatch, state)
    monkeypatch.setattr(
        analysis,
        "read_audit_log",
        lambda limit=10: [
            {"module": "execution_replay", "status": "FAIL"},
            {"module": "execution_replay", "status": "FAIL"},
            {"module": "execution_replay", "status": "FAIL"},
        ],
    )
    monkeypatch.setattr(
        codex_loop,
        "run_test",
        lambda test_path: (_ for _ in ()).throw(
            AssertionError("run_test should not execute")
        ),
    )

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output["status"] == "escalated"
    assert output["reason"] == "repeated_failures"
    assert state["audit"][-1]["status"] == "ESCALATED"


def test_escalation_loop_attack_repeated_blocked_entries_trigger_escalation(
    monkeypatch, capsys
) -> None:
    state = {"context": _base_context(), "audit": []}
    _patch_loop_state(monkeypatch, state)
    monkeypatch.setattr(
        analysis,
        "read_audit_log",
        lambda limit=10: [
            {"module": "execution_replay", "status": "BLOCKED"},
            {"module": "execution_replay", "status": "BLOCKED"},
            {"module": "execution_replay", "status": "BLOCKED"},
        ],
    )
    monkeypatch.setattr(
        codex_loop,
        "run_test",
        lambda test_path: (_ for _ in ()).throw(
            AssertionError("run_test should not execute")
        ),
    )

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output["status"] == "escalated"
    assert output["reason"] == "repeated_blocked_reviews"
    assert state["audit"][-1]["status"] == "ESCALATED"


def test_escalation_loop_attack_mixed_fail_and_blocked_history_escalates(
    monkeypatch, capsys
) -> None:
    state = {"context": _base_context(), "audit": []}
    _patch_loop_state(monkeypatch, state)
    monkeypatch.setattr(
        analysis,
        "read_audit_log",
        lambda limit=10: [
            {"module": "execution_replay", "status": "FAIL"},
            {"module": "execution_replay", "status": "BLOCKED"},
            {"module": "execution_replay", "status": "FAIL"},
        ],
    )
    monkeypatch.setattr(
        codex_loop,
        "run_test",
        lambda test_path: (_ for _ in ()).throw(
            AssertionError("run_test should not execute")
        ),
    )

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output["status"] == "escalated"
    assert output["reason"] == "repeated_failures"
    assert state["audit"][-1]["status"] == "ESCALATED"


def test_escalation_loop_attack_already_escalated_module_does_not_auto_downgrade(
    monkeypatch, capsys
) -> None:
    state = {
        "context": _base_context(
            modules_state={
                "execution_replay": {
                    "status": "ESCALATED",
                    "review": "NOT_RUN",
                }
            }
        ),
        "audit": [],
    }
    _patch_loop_state(monkeypatch, state)
    monkeypatch.setattr(
        codex_loop,
        "run_test",
        lambda test_path: (_ for _ in ()).throw(
            AssertionError("run_test should not execute")
        ),
    )

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output["status"] == "escalated"
    assert (
        state["context"]["modules_state"]["execution_replay"]["status"] == "ESCALATED"
    )
    assert state["context"]["status"] != "WORKING"


def test_strategy_learning_attack_malformed_strategy_history_does_not_crash(
    monkeypatch, capsys
) -> None:
    state = {
        "context": _base_context(
            strategy_history="corrupted",
            strategy_feedback="broken",
        ),
        "audit": [],
    }
    _patch_loop_state(monkeypatch, state)
    monkeypatch.setattr(
        codex_loop,
        "detect_unstable_module",
        lambda module: {"unstable": False, "reason": ""},
    )
    monkeypatch.setattr(codex_loop, "run_test", lambda test_path: ("timeout", ""))

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output == {
        "goal": "fix infinite loop or blocking execution",
        "priority": "critical",
    }
    assert state["context"]["last_strategy"] == "performance_fix"
    assert state["context"]["strategy_feedback"] == {
        "strategy": "performance_fix",
        "result": "failure",
    }
    assert state["context"]["strategy_history"][-1] == {
        "strategy": "performance_fix",
        "result": "failure",
    }


def test_strategy_learning_attack_conflicting_history_falls_back_to_default(
    monkeypatch, capsys
) -> None:
    state = {
        "context": _base_context(
            strategy_history=[
                {"strategy": "logic_review", "result": "success"},
                {"strategy": "logic_review", "result": "failure"},
            ]
        ),
        "audit": [],
    }
    _patch_loop_state(monkeypatch, state)
    monkeypatch.setattr(
        codex_loop,
        "detect_unstable_module",
        lambda module: {"unstable": False, "reason": ""},
    )
    monkeypatch.setattr(codex_loop, "run_test", lambda test_path: ("fail", "broken"))
    monkeypatch.setattr(
        codex_loop, "execute_fix_task", lambda prompt: _FailedExecution()
    )

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output["strategy"] == "code_fix"
    assert state["context"]["last_strategy"] == "code_fix"


def test_strategy_learning_attack_weak_evidence_cannot_override_default_strategy(
    monkeypatch, capsys
) -> None:
    state = {
        "context": _base_context(
            strategy_history=[{"strategy": "performance_fix", "result": "success"}]
        ),
        "audit": [],
    }
    _patch_loop_state(monkeypatch, state)
    monkeypatch.setattr(
        codex_loop,
        "detect_unstable_module",
        lambda module: {"unstable": False, "reason": ""},
    )
    monkeypatch.setattr(codex_loop, "run_test", lambda test_path: ("fail", "broken"))
    monkeypatch.setattr(
        codex_loop, "execute_fix_task", lambda prompt: _FailedExecution()
    )

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output["strategy"] == "code_fix"
    assert state["context"]["last_strategy"] == "code_fix"


def test_strategy_learning_attack_invalid_entries_still_append_safe_feedback(
    monkeypatch, capsys
) -> None:
    state = {
        "context": _base_context(
            strategy_history=[{"strategy": None}, "bad", 7],
        ),
        "audit": [],
    }
    _patch_loop_state(monkeypatch, state)
    monkeypatch.setattr(
        codex_loop,
        "detect_unstable_module",
        lambda module: {"unstable": False, "reason": ""},
    )
    monkeypatch.setattr(codex_loop, "run_test", lambda test_path: ("timeout", ""))

    codex_loop.main()
    capsys.readouterr()

    assert state["context"]["strategy_history"][-1] == {
        "strategy": "performance_fix",
        "result": "failure",
    }


def test_git_trace_integrity_no_git_available_uses_fallbacks(monkeypatch) -> None:
    monkeypatch.setattr(
        git_trace,
        "run_in_dev_env",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("git unavailable")),
    )

    assert git_trace.get_git_commit() == "no_git"
    assert git_trace.get_git_branch() == "no_branch"


def test_git_trace_integrity_malformed_context_payload_is_replaced_with_valid_trace(
    monkeypatch, capsys
) -> None:
    state = {
        "context": _base_context(git="corrupted"),
        "audit": [],
    }
    _patch_loop_state(monkeypatch, state)
    monkeypatch.setattr(
        codex_loop,
        "detect_unstable_module",
        lambda module: {"unstable": False, "reason": ""},
    )
    monkeypatch.setattr(codex_loop, "run_test", lambda test_path: ("pass", "ok"))
    monkeypatch.setattr(
        codex_loop, "run_external_review", lambda *args, **kwargs: _approved_review()
    )
    monkeypatch.setattr(
        git_trace,
        "run_in_dev_env",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("git unavailable")),
    )

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output == {"status": "pass", "fixed": "execution_replay"}
    assert state["context"]["git"]["commit"] == "no_git"
    assert state["context"]["git"]["branch"] == "no_branch"
    assert "time" in state["context"]["git"]


def test_git_trace_integrity_pass_path_survives_git_failure(
    monkeypatch, capsys
) -> None:
    state = {"context": _base_context(), "audit": []}
    _patch_loop_state(monkeypatch, state)
    monkeypatch.setattr(
        codex_loop,
        "detect_unstable_module",
        lambda module: {"unstable": False, "reason": ""},
    )
    monkeypatch.setattr(codex_loop, "run_test", lambda test_path: ("pass", "ok"))
    monkeypatch.setattr(
        codex_loop, "run_external_review", lambda *args, **kwargs: _approved_review()
    )
    monkeypatch.setattr(
        git_trace,
        "run_in_dev_env",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("git unavailable")),
    )

    codex_loop.main()
    capsys.readouterr()

    assert state["audit"][-1]["git"]["commit"] == "no_git"
    assert state["audit"][-1]["git"]["branch"] == "no_branch"
    assert "time" in state["audit"][-1]["git"]


def test_timeout_stale_state_attack_clears_invalid_working_status(
    monkeypatch, capsys
) -> None:
    state = {
        "context": _base_context(
            status="WORKING",
            last_codex_loop={"module": "execution_replay", "status": "timeout"},
        ),
        "audit": [],
    }
    _patch_loop_state(monkeypatch, state)
    monkeypatch.setattr(
        codex_loop,
        "detect_unstable_module",
        lambda module: {"unstable": False, "reason": ""},
    )
    monkeypatch.setattr(codex_loop, "run_test", lambda test_path: ("timeout", ""))

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output == {
        "goal": "fix infinite loop or blocking execution",
        "priority": "critical",
    }
    assert state["context"]["status"] != "WORKING"
    assert state["context"]["last_codex_loop"]["status"] == "timeout"


def test_timeout_stale_state_attack_overwrites_corrupted_strategy_feedback(
    monkeypatch, capsys
) -> None:
    state = {
        "context": _base_context(
            strategy_history="broken",
            strategy_feedback="broken",
        ),
        "audit": [],
    }
    _patch_loop_state(monkeypatch, state)
    monkeypatch.setattr(
        codex_loop,
        "detect_unstable_module",
        lambda module: {"unstable": False, "reason": ""},
    )
    monkeypatch.setattr(codex_loop, "run_test", lambda test_path: ("timeout", ""))

    codex_loop.main()
    capsys.readouterr()

    assert state["context"]["status"] != "WORKING"
    assert state["context"]["strategy_feedback"] == {
        "strategy": "performance_fix",
        "result": "failure",
    }


def test_timeout_stale_state_attack_escalation_history_prevents_false_recovery(
    monkeypatch, capsys
) -> None:
    state = {
        "context": _base_context(
            status="WORKING",
            last_codex_loop={"module": "execution_replay", "status": "timeout"},
        ),
        "audit": [],
    }
    _patch_loop_state(monkeypatch, state)
    monkeypatch.setattr(
        analysis,
        "read_audit_log",
        lambda limit=10: [
            {"module": "execution_replay", "status": "FAIL"},
            {"module": "execution_replay", "status": "BLOCKED"},
            {"module": "execution_replay", "status": "FAIL"},
        ],
    )
    monkeypatch.setattr(
        codex_loop,
        "run_test",
        lambda test_path: (_ for _ in ()).throw(
            AssertionError("run_test should not execute")
        ),
    )

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    assert output["status"] == "escalated"
    assert state["context"]["status"] != "WORKING"


def test_decision_trace_integrity_rejects_missing_trace() -> None:
    decision = build_escalation_decision("execution_replay", "repeated_failures")
    decision.pop("decision_trace")

    with pytest.raises(KeyError):
        _assert_escalation_trace(decision)


def test_decision_trace_integrity_rejects_malformed_trace() -> None:
    decision = build_escalation_decision("execution_replay", "repeated_failures")
    decision["decision_trace"] = "broken"

    with pytest.raises((AssertionError, TypeError)):
        _assert_escalation_trace(decision)


def test_decision_trace_integrity_escalation_audit_row_must_include_trace(
    monkeypatch, capsys
) -> None:
    state = {"context": _base_context(), "audit": []}
    _patch_loop_state(monkeypatch, state)
    monkeypatch.setattr(
        codex_loop,
        "detect_unstable_module",
        lambda module: {"unstable": True, "reason": "repeated_failures"},
    )
    monkeypatch.setattr(
        codex_loop,
        "run_test",
        lambda test_path: (_ for _ in ()).throw(
            AssertionError("run_test should not execute")
        ),
    )

    codex_loop.main()
    output = json.loads(capsys.readouterr().out.strip())

    _assert_escalation_trace(output)
    _assert_audit_escalation_trace(state["audit"][-1])
