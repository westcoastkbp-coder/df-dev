import copy
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from control.analysis import detect_unstable_module
from control.audit_log import build_audit_entry, log_execution, read_audit_log
from control.decision_contract import build_escalation_decision
from control.dev_session import (
    DEV_SESSION_COMMIT_MESSAGE,
    build_codex_execution_prompt,
    ensure_dev_session,
    extract_last_failing_test,
    parse_issue_number_from_branch,
    update_dev_session,
)
from control.dev_runtime import run_in_dev_env
from control.error_classifier import classify_error
from control.external_review_gate import run_external_review
from control.git_trace import ensure_not_main_branch, get_git_branch, get_git_commit
from control.memory import load_context, save_context

ROOT = Path(__file__).resolve().parents[1]
TEST_MAP = {
    "execution_replay": ROOT / "tests" / "test_execution_replay.py",
    "scheduler": ROOT / "tests" / "test_task_dependencies.py",
    "retry": ROOT / "tests" / "test_recovery_policy.py",
}
RELATED_FILES_MAP = {
    "execution_replay": [
        "app/execution/execution_replay.py",
        "app/orchestrator/task_worker.py",
        "tests/test_execution_replay.py",
    ],
    "scheduler": [
        "app/orchestrator/task_worker.py",
        "runtime/pipeline/run_pipeline.py",
        "tests/test_task_dependencies.py",
    ],
    "retry": [
        "runtime/pipeline/run_pipeline.py",
        "app/orchestrator/task_worker.py",
        "runtime/tests/test_recovery_policy.py",
    ],
}


def build_fix_task(module_name: str, error_type: str) -> dict[str, str]:
    if error_type == "execution_error":
        fix_task = {
            "strategy": "code_fix",
            "instruction": f"Fix failing test in {module_name}",
        }
    elif error_type == "verification_error":
        fix_task = {
            "strategy": "logic_review",
            "instruction": f"Adjust logic to satisfy external review for {module_name}",
        }
    elif error_type == "timeout_error":
        fix_task = {
            "strategy": "performance_fix",
            "instruction": f"Optimize execution or timeout handling in {module_name}",
        }
    else:
        fix_task = {
            "strategy": "generic_fix",
            "instruction": f"Investigate failure in {module_name}",
        }

    fix_task["error_type"] = error_type
    return fix_task


def apply_strategy_feedback(payload: dict[str, object], result: str) -> None:
    feedback = {
        "strategy": payload.get("last_strategy"),
        "result": result,
    }
    payload["strategy_feedback"] = feedback
    history = payload.get("strategy_history")
    if not isinstance(history, list):
        history = []
        payload["strategy_history"] = history
    history.append(dict(feedback))


def _normalize_review_result(review_result: object) -> dict[str, object]:
    if not isinstance(review_result, dict):
        review_result = {}

    packet = review_result.get("packet")
    claude = review_result.get("claude")
    gemini = review_result.get("gemini")
    is_well_formed = all(isinstance(item, dict) for item in (packet, claude, gemini))

    normalized = {
        "status": str(review_result.get("status", "") or "review_invalid"),
        "decision": str(review_result.get("decision", "") or "").strip().upper(),
        "packet": dict(packet) if isinstance(packet, dict) else {},
        "claude": dict(claude) if isinstance(claude, dict) else {},
        "gemini": dict(gemini) if isinstance(gemini, dict) else {},
    }

    if normalized["decision"] == "BLOCKED" and is_well_formed:
        return normalized

    if (
        normalized["decision"] == "APPROVED"
        and is_well_formed
        and normalized["gemini"].get("verdict") == "VERIFIED"
    ):
        return normalized

    normalized["status"] = "review_invalid"
    normalized["decision"] = "BLOCKED"
    normalized["claude"].setdefault("status", "error")
    normalized["claude"].setdefault("error", "invalid_review_payload")
    normalized["gemini"].setdefault("verdict", "INVALID")
    return normalized


def _context_is_cleanly_working(payload: dict[str, object]) -> bool:
    if payload.get("status") != "WORKING":
        return False

    broken_modules = payload.get("broken_modules", [])
    if isinstance(broken_modules, list) and broken_modules:
        return False

    broken = payload.get("broken", {})
    if isinstance(broken, dict) and broken:
        return False

    last_codex_loop = payload.get("last_codex_loop", {})
    if isinstance(last_codex_loop, dict):
        if last_codex_loop.get("status") in {
            "timeout",
            "fail",
            "blocked_by_review",
            "escalated",
        }:
            return False

    modules_state = payload.get("modules_state", {})
    if isinstance(modules_state, dict):
        for item in modules_state.values():
            if isinstance(item, dict) and item.get("status") in {
                "BLOCKED",
                "ESCALATED",
            }:
                return False

    return True


def _mark_not_working(payload: dict[str, object]) -> None:
    payload["status"] = "NOT_WORKING"


def _build_review_decision_trace(
    module_name: str,
    review_result: dict[str, object],
    *,
    local_test: str,
) -> dict[str, str]:
    claude = review_result.get("claude", {})
    gemini = review_result.get("gemini", {})
    return {
        "type": "review_gate",
        "module": module_name,
        "local_test": local_test,
        "review": str(review_result.get("decision", "") or "BLOCKED").upper(),
        "claude_status": str(claude.get("status", "unknown") or "unknown"),
        "gemini_verdict": str(gemini.get("verdict", "UNKNOWN") or "UNKNOWN").upper(),
    }


def _build_execution_decision_trace(
    module_name: str,
    *,
    local_test: str,
    review: str,
    reason: str,
) -> dict[str, str]:
    return {
        "type": "execution_gate",
        "module": module_name,
        "local_test": local_test,
        "review": review,
        "reason": reason,
    }


def select_broken_module(payload: dict[str, object]) -> str:
    raw_broken_modules = payload.get("broken_modules", [])
    broken_modules = (
        list(raw_broken_modules) if isinstance(raw_broken_modules, list) else []
    )
    if broken_modules:
        return str(broken_modules[0]).strip()
    raw_broken = payload.get("broken", {})
    broken = dict(raw_broken) if isinstance(raw_broken, dict) else {}
    if broken:
        return str(next(iter(broken))).strip()
    return ""


def run_test(test_path: Path) -> tuple[str, str]:
    try:
        completed = run_in_dev_env(
            ["python", "-m", "pytest", str(test_path), "-q"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return "timeout", ""

    combined_output = "\n".join(
        part for part in (completed.stdout.strip(), completed.stderr.strip()) if part
    )
    if completed.returncode == 0:
        return "pass", combined_output
    return "fail", combined_output


def execute_fix_task(prompt: str):
    enriched_prompt = build_codex_execution_prompt(
        prompt,
        repo_root=ROOT,
        issue_number=_current_issue_number(),
    )
    try:
        result = run_in_dev_env(
            ["codex", "exec", enriched_prompt],
            cwd=ROOT,
            timeout=60,
            check=False,
            capture_output=True,
            text=True,
        )
    except subprocess.TimeoutExpired:
        return {"status": "timeout", "reason": "codex_execution_timeout"}
    return result


def commit_loop_result(module_name: str, status: str, review: str) -> None:
    if status == "WORKING" and review == "APPROVED":
        run_in_dev_env(
            ["git", "add", "."],
            cwd=ROOT,
            check=True,
        )
        run_in_dev_env(
            [
                "git",
                "commit",
                "-m",
                DEV_SESSION_COMMIT_MESSAGE,
            ],
            cwd=ROOT,
            check=True,
        )
        try:
            run_in_dev_env(
                ["git", "push"],
                cwd=ROOT,
                check=True,
            )
        except Exception as exc:
            raise RuntimeError("GITHUB_SYNC_FAILED") from exc
        return

    if status == "BLOCKED" and review == "BLOCKED":
        run_in_dev_env(
            ["git", "add", "."],
            cwd=ROOT,
            check=True,
        )
        run_in_dev_env(
            [
                "git",
                "commit",
                "--allow-empty",
                "-m",
                DEV_SESSION_COMMIT_MESSAGE,
            ],
            cwd=ROOT,
            check=True,
        )
        try:
            run_in_dev_env(
                ["git", "push"],
                cwd=ROOT,
                check=True,
            )
        except Exception as exc:
            raise RuntimeError("GITHUB_SYNC_FAILED") from exc


def mark_fixed(payload: dict[str, object], module_name: str) -> dict[str, object]:
    broken = dict(payload.get("broken", {}) or {})
    broken.pop(module_name, None)
    payload["broken"] = broken
    payload["broken_modules"] = [
        item
        for item in list(payload.get("broken_modules", []) or [])
        if item != module_name
    ]
    payload["status"] = "WORKING" if not broken else "NOT_WORKING"
    payload["next_required"] = (
        "Proceed to next broken module" if broken else "System ready for execution"
    )
    payload["last_codex_loop"] = {
        "module": module_name,
        "status": "pass",
    }
    return payload


def _current_issue_number() -> int | None:
    try:
        branch_name = get_git_branch()
    except Exception:
        return None
    return parse_issue_number_from_branch(branch_name)


def _session_remaining_items(module_name: str, test_path: Path) -> list[str]:
    return [
        f"Keep /docs/dev_session.md aligned with GitHub context for {module_name}.",
        f"Re-run {test_path.name} before closing the module.",
        f'Sync session state with git add ., git commit -m "{DEV_SESSION_COMMIT_MESSAGE}", git push.',
    ]


def _update_loop_session(
    *,
    module_name: str,
    test_path: Path,
    status: str,
    next_step: str,
    did: str,
    fixed_items: list[str] | None = None,
    remaining_items: list[str] | None = None,
    last_failing_test: str | None = None,
) -> dict[str, object]:
    normalized_test = last_failing_test or str(test_path).replace("\\", "/")
    return update_dev_session(
        repo_root=ROOT,
        issue_number=_current_issue_number(),
        current_task=f"Resolve {module_name} from GitHub-backed session context.",
        last_failing_test=normalized_test,
        fixed_items=fixed_items,
        remaining_items=remaining_items
        or _session_remaining_items(module_name, test_path),
        status=status,
        next_step=next_step,
        did=did,
    )


def enforce_external_review(
    payload: dict[str, object], module_name: str, test_path: Path
) -> dict[str, object]:
    review_result = _normalize_review_result(
        run_external_review(
            task_id=module_name,
            summary=f"fix {module_name}",
            files=RELATED_FILES_MAP.get(module_name, []),
        )
    )
    decision_trace = _build_review_decision_trace(
        module_name,
        review_result,
        local_test="PASS",
    )

    if review_result["decision"] == "BLOCKED":
        error_type = classify_error("PASS", "BLOCKED")
        updated = copy.deepcopy(payload)
        _mark_not_working(updated)
        updated["last_codex_loop"] = {
            "module": module_name,
            "status": "blocked_by_review",
            "test": str(test_path).replace("\\", "/"),
        }
        updated["next_required"] = f"Resolve {module_name} via {test_path.name}"
        updated["last_codex_loop"]["local_test"] = "pass"
        updated["last_codex_loop"]["review"] = review_result
        updated.setdefault("modules_state", {})
        updated["modules_state"][module_name] = {
            "status": "BLOCKED",
            "last_test": "PASS",
            "review": review_result["decision"],
        }
        updated["last_error"] = {
            "module": module_name,
            "type": error_type,
        }
        error = updated.get("last_error", {})
        fix_task = build_fix_task(module_name, error.get("type", "unknown_error"))
        updated["last_strategy"] = fix_task["strategy"]
        apply_strategy_feedback(updated, "failure")
        updated["decision_trace"] = decision_trace
        recent_entries = read_audit_log(limit=5)
        blocked_count = sum(
            1
            for entry in recent_entries
            if entry.get("module") == module_name and entry.get("status") == "BLOCKED"
        )
        if blocked_count >= 2:
            updated["next_required"] = "manual intervention required"
        updated["git"] = {
            "commit": get_git_commit(),
            "branch": get_git_branch(),
            "time": datetime.utcnow().isoformat(),
        }
        save_context(updated)
        log_execution(
            {
                **build_audit_entry(
                    module=module_name,
                    status="BLOCKED",
                    local_test="PASS",
                    review=review_result["decision"],
                    git=updated.get("git", {}),
                ),
                "error_type": error_type,
                "strategy": fix_task["strategy"],
                "strategy_feedback": updated.get("strategy_feedback"),
                "decision_trace": decision_trace,
            }
        )
        _update_loop_session(
            module_name=module_name,
            test_path=test_path,
            status="blocked",
            next_step=updated["next_required"],
            did=f"External review blocked {module_name}.",
            last_failing_test=str(test_path).replace("\\", "/"),
        )
        commit_loop_result(module_name, "BLOCKED", "BLOCKED")
        print(
            json.dumps(
                {
                    "status": "blocked_by_review",
                    "module": module_name,
                    "review": review_result,
                },
                ensure_ascii=False,
            )
        )
        return review_result

    payload["last_codex_loop"] = {
        "module": module_name,
        "status": "pass",
        "test": str(test_path).replace("\\", "/"),
        "local_test": "pass",
        "review": review_result,
    }
    payload["decision_trace"] = decision_trace
    return review_result


def main() -> dict[str, str] | None:
    ensure_not_main_branch()
    ensure_dev_session(
        repo_root=ROOT,
        issue_number=_current_issue_number(),
        current_task="Continue only from /docs/dev_session.md and the linked GitHub packet.",
        last_failing_test=extract_last_failing_test(repo_root=ROOT),
        status="in_progress",
        next_step="Load the current broken module and continue from the session file.",
    )
    context = load_context()
    module_name = select_broken_module(context)

    if _context_is_cleanly_working(context):
        print(
            json.dumps(
                {"status": "pass", "message": "system already working"},
                ensure_ascii=False,
            )
        )
        return

    if not module_name:
        print(
            json.dumps(
                {"status": "fail", "reason": "no_broken_module_identified"},
                ensure_ascii=False,
            )
        )
        return

    test_path = TEST_MAP.get(module_name)
    if test_path is None:
        print(
            json.dumps(
                {"status": "fail", "reason": "no_test_mapping", "module": module_name},
                ensure_ascii=False,
            )
        )
        return

    normalized_test_path = str(test_path).replace("\\", "/")
    _update_loop_session(
        module_name=module_name,
        test_path=test_path,
        status="in_progress",
        next_step=f"Run {test_path.name} and keep /docs/dev_session.md current.",
        did=f"Loaded GitHub-backed session context for {module_name}.",
        last_failing_test=extract_last_failing_test(repo_root=ROOT)
        or normalized_test_path,
    )

    modules_state = context.get("modules_state", {})
    if not isinstance(modules_state, dict):
        modules_state = {}
    module_state = modules_state.get(module_name, {})
    if not isinstance(module_state, dict):
        module_state = {}
    if module_state.get("status") == "WORKING":
        result = {"status": "already_working", "module": module_name}
        print(json.dumps(result, ensure_ascii=False))
        return result
    if module_state.get("status") == "ESCALATED":
        updated = copy.deepcopy(context)
        _mark_not_working(updated)
        updated["next_required"] = f"manual intervention required for {module_name}"
        escalation_decision = build_escalation_decision(
            module_name, "previous_escalation_pending"
        )
        updated["decision_trace"] = dict(escalation_decision.get("decision_trace", {}))
        save_context(updated)
        log_execution(
            {
                **build_audit_entry(
                    module=module_name,
                    status="ESCALATED",
                    local_test="NOT_RUN",
                    review="NOT_RUN",
                    git=updated.get("git", {}),
                ),
                "decision_trace": {
                    "type": "escalation",
                    "reason": "previous_escalation_pending",
                },
            }
        )
        print(json.dumps(escalation_decision, ensure_ascii=False))
        return escalation_decision

    unstable = detect_unstable_module(module_name)
    if unstable["unstable"]:
        updated = copy.deepcopy(context)
        _mark_not_working(updated)
        updated["next_required"] = f"manual intervention required for {module_name}"
        escalation_decision = build_escalation_decision(module_name, unstable["reason"])
        updated["decision_trace"] = dict(escalation_decision.get("decision_trace", {}))
        save_context(updated)
        log_execution(
            {
                **build_audit_entry(
                    module=module_name,
                    status="ESCALATED",
                    local_test="NOT_RUN",
                    review="NOT_RUN",
                    git=updated.get("git", {}),
                ),
                "decision_trace": {
                    "type": "escalation",
                    "reason": unstable["reason"],
                },
            }
        )

        print(json.dumps(escalation_decision, ensure_ascii=False))
        return

    status, _ = run_test(test_path)

    if status == "pass":
        review_result = enforce_external_review(context, module_name, test_path)
        if review_result["decision"] == "BLOCKED":
            return
        updated = mark_fixed(context, module_name)
        updated["last_codex_loop"]["test"] = str(test_path).replace("\\", "/")
        updated["last_codex_loop"]["local_test"] = "pass"
        updated["last_codex_loop"]["review"] = review_result
        updated["last_codex_loop"]["review"]["decision"] = "APPROVED"
        updated.setdefault("modules_state", {})
        updated["modules_state"][module_name] = {
            "status": "WORKING",
            "last_test": "PASS",
            "review": review_result["decision"],
        }
        updated["git"] = {
            "commit": get_git_commit(),
            "branch": get_git_branch(),
            "time": datetime.utcnow().isoformat(),
        }
        apply_strategy_feedback(updated, "success")
        save_context(updated)
        log_execution(
            {
                **build_audit_entry(
                    module=module_name,
                    status="WORKING",
                    local_test="PASS",
                    review=review_result["decision"],
                    git=updated.get("git", {}),
                ),
                "strategy_feedback": updated.get("strategy_feedback"),
                "decision_trace": updated.get("decision_trace"),
            }
        )
        _update_loop_session(
            module_name=module_name,
            test_path=test_path,
            status="working",
            next_step=updated["next_required"],
            did=f"Validated {module_name} and marked it working.",
            fixed_items=[f"Validated {module_name} via {normalized_test_path}."],
            remaining_items=[
                item
                for item in (
                    updated["next_required"],
                    f'Sync session state with git add ., git commit -m "{DEV_SESSION_COMMIT_MESSAGE}", git push.',
                )
                if item
            ],
            last_failing_test=normalized_test_path,
        )
        commit_loop_result(module_name, "WORKING", "APPROVED")
        print(json.dumps({"status": "pass", "fixed": module_name}, ensure_ascii=False))
        return

    updated = copy.deepcopy(context)
    _mark_not_working(updated)
    updated["last_codex_loop"] = {
        "module": module_name,
        "status": status,
        "test": str(test_path).replace("\\", "/"),
    }
    updated["next_required"] = f"Resolve {module_name} via {test_path.name}"
    error_type = classify_error("FAIL" if status == "fail" else "TIMEOUT", "NOT_RUN")
    updated["last_error"] = {
        "module": module_name,
        "type": error_type,
    }

    if status == "timeout":
        error = updated.get("last_error", {})
        fix_task = build_fix_task(module_name, error.get("type", "unknown_error"))
        updated["last_strategy"] = fix_task["strategy"]
        apply_strategy_feedback(updated, "failure")
        updated["decision_trace"] = _build_execution_decision_trace(
            module_name,
            local_test="TIMEOUT",
            review="NOT_RUN",
            reason=error_type,
        )
        save_context(updated)
        log_execution(
            {
                **build_audit_entry(
                    module=module_name,
                    status="TIMEOUT",
                    local_test="TIMEOUT",
                    review="NOT_RUN",
                    git=updated.get("git", {}),
                ),
                "error_type": error_type,
                "strategy": fix_task["strategy"],
                "strategy_feedback": updated.get("strategy_feedback"),
                "decision_trace": updated.get("decision_trace"),
            }
        )
        _update_loop_session(
            module_name=module_name,
            test_path=test_path,
            status="timeout",
            next_step=f"Resolve timeout in {module_name} before retrying {test_path.name}.",
            did=f"Local test timed out for {module_name}.",
            last_failing_test=normalized_test_path,
        )
        print(
            json.dumps(
                {
                    "goal": "fix infinite loop or blocking execution",
                    "priority": "critical",
                },
                ensure_ascii=False,
            )
        )
        return

    error = updated.get("last_error", {})
    fix_task = build_fix_task(module_name, error.get("type", "unknown_error"))
    updated["last_strategy"] = fix_task["strategy"]
    updated["decision_trace"] = _build_execution_decision_trace(
        module_name,
        local_test="FAIL",
        review="NOT_RUN",
        reason=error_type,
    )
    save_context(updated)
    log_execution(
        {
            **build_audit_entry(
                module=module_name,
                status="FAIL",
                local_test="FAIL",
                review="NOT_RUN",
                git=updated.get("git", {}),
            ),
            "error_type": error_type,
            "strategy": fix_task["strategy"],
            "strategy_feedback": updated.get("strategy_feedback"),
            "decision_trace": updated.get("decision_trace"),
        }
    )

    related_files = RELATED_FILES_MAP.get(module_name, [])
    fix_prompt = f"""
Instruction:
{fix_task["instruction"]}

Strategy:
{fix_task["strategy"]}

Error Type:
{fix_task["error_type"]}

Test:
{normalized_test_path}

Files:
{related_files}

STRICT RULES:
- minimal fix only
- do not add new features
- do not modify unrelated files
- do not change architecture
- must eliminate hanging behavior
"""

    _update_loop_session(
        module_name=module_name,
        test_path=test_path,
        status="in_progress",
        next_step=f"Run Codex fix attempt for {module_name} and update /docs/dev_session.md immediately after.",
        did=f"Prepared Codex fix prompt for {module_name}.",
        last_failing_test=normalized_test_path,
    )

    execution_result = execute_fix_task(fix_prompt)

    if isinstance(execution_result, dict):
        updated = copy.deepcopy(context)
        _mark_not_working(updated)
        updated["last_codex_loop"] = {
            "module": module_name,
            "status": "timeout",
            "test": str(test_path).replace("\\", "/"),
        }
        updated["next_required"] = f"Resolve {module_name} via {test_path.name}"
        error_type = classify_error("TIMEOUT", "NOT_RUN")
        updated["last_error"] = {
            "module": module_name,
            "type": error_type,
        }
        updated["last_strategy"] = fix_task["strategy"]
        apply_strategy_feedback(updated, "failure")
        updated["decision_trace"] = _build_execution_decision_trace(
            module_name,
            local_test="TIMEOUT",
            review="NOT_RUN",
            reason=error_type,
        )
        save_context(updated)
        log_execution(
            {
                **build_audit_entry(
                    module=module_name,
                    status="TIMEOUT",
                    local_test="TIMEOUT",
                    review="NOT_RUN",
                    git=updated.get("git", {}),
                ),
                "error_type": error_type,
                "strategy": fix_task["strategy"],
                "strategy_feedback": updated.get("strategy_feedback"),
                "decision_trace": updated.get("decision_trace"),
            }
        )
        _update_loop_session(
            module_name=module_name,
            test_path=test_path,
            status="timeout",
            next_step=f"Retry {module_name} after addressing the Codex timeout.",
            did=f"Codex execution timed out while working on {module_name}.",
            last_failing_test=normalized_test_path,
        )
        print(json.dumps(execution_result, ensure_ascii=False))
        return

    if execution_result.returncode != 0:
        updated = copy.deepcopy(context)
        _mark_not_working(updated)
        updated["last_codex_loop"] = {
            "module": module_name,
            "status": "fail",
            "test": str(test_path).replace("\\", "/"),
        }
        updated["next_required"] = f"Resolve {module_name} via {test_path.name}"
        error_type = classify_error("FAIL", "NOT_RUN")
        updated["last_error"] = {
            "module": module_name,
            "type": error_type,
        }
        updated["last_strategy"] = fix_task["strategy"]
        apply_strategy_feedback(updated, "failure")
        updated["decision_trace"] = _build_execution_decision_trace(
            module_name,
            local_test="FAIL",
            review="NOT_RUN",
            reason=error_type,
        )
        save_context(updated)
        log_execution(
            {
                **build_audit_entry(
                    module=module_name,
                    status="FAIL",
                    local_test="FAIL",
                    review="NOT_RUN",
                    git=updated.get("git", {}),
                ),
                "error_type": error_type,
                "strategy": fix_task["strategy"],
                "strategy_feedback": updated.get("strategy_feedback"),
                "decision_trace": updated.get("decision_trace"),
            }
        )
        _update_loop_session(
            module_name=module_name,
            test_path=test_path,
            status="fail",
            next_step=f"Inspect the failed Codex attempt for {module_name} before retrying.",
            did=f"Codex execution failed for {module_name}.",
            last_failing_test=normalized_test_path,
        )
        print(json.dumps(fix_task, ensure_ascii=False))
        return

    validation = run_in_dev_env(
        [
            "python",
            "-m",
            "pytest",
            normalized_test_path,
            "-q",
        ],
        cwd=ROOT,
        timeout=30,
        check=False,
        capture_output=True,
        text=True,
    )

    if validation.returncode == 0:
        review_result = enforce_external_review(context, module_name, test_path)
        if review_result["decision"] == "BLOCKED":
            return
        updated = mark_fixed(context, module_name)
        updated["last_codex_loop"]["test"] = str(test_path).replace("\\", "/")
        updated["last_codex_loop"]["local_test"] = "pass"
        updated["last_codex_loop"]["review"] = review_result
        updated["last_codex_loop"]["review"]["decision"] = "APPROVED"
        updated.setdefault("modules_state", {})
        updated["modules_state"][module_name] = {
            "status": "WORKING",
            "last_test": "PASS",
            "review": review_result["decision"],
        }
        updated["last_strategy"] = fix_task["strategy"]
        updated["git"] = {
            "commit": get_git_commit(),
            "branch": get_git_branch(),
            "time": datetime.utcnow().isoformat(),
        }
        apply_strategy_feedback(updated, "success")
        save_context(updated)
        log_execution(
            {
                **build_audit_entry(
                    module=module_name,
                    status="WORKING",
                    local_test="PASS",
                    review=review_result["decision"],
                    git=updated.get("git", {}),
                ),
                "strategy_feedback": updated.get("strategy_feedback"),
                "decision_trace": updated.get("decision_trace"),
            }
        )
        _update_loop_session(
            module_name=module_name,
            test_path=test_path,
            status="working",
            next_step=updated["next_required"],
            did=f"Codex fix for {module_name} passed validation and review.",
            fixed_items=[
                f"Validated Codex fix for {module_name} via {normalized_test_path}."
            ],
            remaining_items=[
                item
                for item in (
                    updated["next_required"],
                    f'Sync session state with git add ., git commit -m "{DEV_SESSION_COMMIT_MESSAGE}", git push.',
                )
                if item
            ],
            last_failing_test=normalized_test_path,
        )
        commit_loop_result(module_name, "WORKING", "APPROVED")
        print(json.dumps({"status": "pass", "fixed": module_name}, ensure_ascii=False))
        return

    updated = copy.deepcopy(context)
    _mark_not_working(updated)
    updated["last_codex_loop"] = {
        "module": module_name,
        "status": "fail",
        "test": str(test_path).replace("\\", "/"),
    }
    updated["next_required"] = f"Resolve {module_name} via {test_path.name}"
    error_type = classify_error("FAIL", "NOT_RUN")
    updated["last_error"] = {
        "module": module_name,
        "type": error_type,
    }
    updated["last_strategy"] = fix_task["strategy"]
    apply_strategy_feedback(updated, "failure")
    updated["decision_trace"] = _build_execution_decision_trace(
        module_name,
        local_test="FAIL",
        review="NOT_RUN",
        reason=error_type,
    )
    save_context(updated)
    log_execution(
        {
            **build_audit_entry(
                module=module_name,
                status="FAIL",
                local_test="FAIL",
                review="NOT_RUN",
                git=updated.get("git", {}),
            ),
            "error_type": error_type,
            "strategy": fix_task["strategy"],
            "strategy_feedback": updated.get("strategy_feedback"),
            "decision_trace": updated.get("decision_trace"),
        }
    )
    _update_loop_session(
        module_name=module_name,
        test_path=test_path,
        status="fail",
        next_step=f"Re-open {module_name}; validation still fails after the Codex attempt.",
        did=f"Validation still fails for {module_name} after the Codex attempt.",
        last_failing_test=normalized_test_path,
    )

    print(json.dumps(fix_task, ensure_ascii=False))


if __name__ == "__main__":
    main()
