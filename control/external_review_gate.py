from __future__ import annotations

from control.claude_executor import call_claude_local
from control.gemini_executor import call_gemini_verifier
from control.memory import record_event, record_issue
from control.review_packet import build_review_packet


def _normalize_claude_result(result: object) -> dict:
    if isinstance(result, dict):
        return dict(result)
    return {
        "status": "error",
        "error": "invalid_claude_result",
    }


def _claude_has_critical_issue(result: dict) -> bool:
    if str(result.get("status", "")).strip().lower() == "error":
        return False
    combined_text = " ".join(
        str(result.get(key, "") or "")
        for key in ("stdout", "stderr", "analysis", "notes", "result")
    ).lower()
    return "critical" in combined_text


def run_external_review(task_id: str, summary: str, files: list[str]) -> dict:
    review_packet = build_review_packet(
        task_id=task_id,
        summary=summary,
        files=files,
    )

    try:
        claude_result = _normalize_claude_result(call_claude_local(review_packet))
    except Exception as exc:
        claude_result = {
            "status": "error",
            "error": type(exc).__name__,
            "notes": "claude review failed",
        }

    gemini_result = call_gemini_verifier(review_packet)

    decision = "APPROVED"

    if gemini_result.get("verdict") != "VERIFIED":
        decision = "BLOCKED"

    if _claude_has_critical_issue(claude_result):
        decision = "BLOCKED"

    record_event(f"review executed for task {task_id}")

    if decision == "BLOCKED":
        record_issue(f"task {task_id} blocked by external review")

    if decision == "APPROVED":
        record_event(f"task {task_id} approved by external review")

    return {
        "status": "review_complete",
        "decision": decision,
        "packet": review_packet,
        "claude": claude_result,
        "gemini": gemini_result,
    }
