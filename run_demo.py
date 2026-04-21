from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from app.config.hybrid_runtime import load_runtime_config
from app.execution.execution_boundary import execution_boundary
from app.execution.paths import ROOT_DIR
from app.execution.system_context import DEFAULT_SYSTEM_CONTEXT_PATH, load_yaml_file
from app.product.runner import dispatch_action_trigger
from memory.storage import load_memory_records, save_task_record
from runtime.context.write_packet import (
    AUDITS_FILE,
    append_audit,
    append_decision,
    append_execution,
)
from runtime.decision.evaluator import (
    build_runtime_decision,
    reset_runtime_decision_history,
)


DEMO_TASK_ID = "demo_task"
DEMO_SUMMARY = "test execution"
DEMO_OUTPUT_PATH = Path("runtime") / "out" / "demo" / "demo_task.txt"


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _read_jsonl_tail(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}

    lines = [
        line.strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if not lines:
        return {}

    try:
        return dict(json.loads(lines[-1]))
    except json.JSONDecodeError:
        return {"raw": lines[-1]}


def _json_pretty(payload: object) -> str:
    return json.dumps(payload, indent=2, ensure_ascii=True, sort_keys=True)


def _build_demo_task() -> dict[str, object]:
    return {
        "task_id": DEMO_TASK_ID,
        "status": "VALIDATED",
        "intent": "demo_execution",
        "goal": DEMO_SUMMARY,
        "payload": {
            "summary": DEMO_SUMMARY,
            "request": "Write a visible demo artifact for demo_task and verify it.",
        },
    }


def _build_runtime_inputs(
    timestamp: str,
) -> tuple[dict[str, object], dict[str, object]]:
    metrics = {
        "cpu": 34.0,
        "memory": 48.0,
        "temperature": 61.0,
        "timestamp": timestamp,
    }
    network_snapshot = {
        "ping_ms": 18.0,
        "jitter_ms": 3.0,
        "packet_loss_pct": 0.0,
        "quality": "GOOD",
        "policy_mode": "NORMAL",
        "telemetry_confidence": "HIGH",
        "telemetry_uncertain": False,
        "sample_count": 3,
        "timestamp": timestamp,
    }
    return metrics, network_snapshot


def main() -> None:
    timestamp = _utc_timestamp()
    task = _build_demo_task()

    memory_before = load_memory_records()
    audit_count_before = 1 if _read_jsonl_tail(AUDITS_FILE) else 0
    if AUDITS_FILE.exists():
        audit_count_before = len(
            [
                line
                for line in AUDITS_FILE.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        )

    system_context = load_yaml_file(DEFAULT_SYSTEM_CONTEXT_PATH)
    runtime_config = load_runtime_config(root_dir=ROOT_DIR)
    company_name = (
        str(system_context.get("product_box", {}).get("company_name", "")).strip()
        or "unknown"
    )

    reset_runtime_decision_history()
    metrics, network_snapshot = _build_runtime_inputs(timestamp)
    decision = build_runtime_decision(task, metrics, network_snapshot)
    decision_packet = {
        "decision_id": f"decision-{timestamp.replace(':', '').replace('-', '')}",
        "task_id": DEMO_TASK_ID,
        "summary": DEMO_SUMMARY,
        "timestamp": timestamp,
        "decision": decision["execution_preference"],
        "runtime_state": decision["overall_runtime_state"],
        "confidence": decision["confidence"],
    }
    append_decision(decision_packet)

    execution_trigger = {
        "action_type": "WRITE_FILE",
        "payload": {
            "task_id": DEMO_TASK_ID,
            "path": str(DEMO_OUTPUT_PATH),
            "content": (
                "demo_task execution path reached the filesystem\n"
                f"summary={DEMO_SUMMARY}\n"
                f"timestamp={timestamp}\n"
            ),
        },
    }
    review_trigger = {
        "action_type": "READ_FILE",
        "payload": {
            "task_id": DEMO_TASK_ID,
            "path": str(DEMO_OUTPUT_PATH),
        },
    }

    with execution_boundary(task, policy_validated=True):
        execution_result = dispatch_action_trigger(execution_trigger, task_state=task)
        review_result = dispatch_action_trigger(review_trigger, task_state=task)

    append_execution(
        {
            "execution_id": f"execution-{timestamp.replace(':', '').replace('-', '')}",
            "task_id": DEMO_TASK_ID,
            "summary": DEMO_SUMMARY,
            "timestamp": timestamp,
            "status": execution_result.get("status", "unknown"),
            "action_type": execution_result.get("action_type", ""),
            "path": execution_result.get("result_payload", {}).get("path", ""),
        }
    )

    review_content = str(
        review_result.get("result_payload", {}).get("content", "")
    ).strip()
    review_passed = "demo_task execution path reached the filesystem" in review_content
    review_summary = "review_passed" if review_passed else "review_failed"

    memory_record = save_task_record(
        {
            "task_id": DEMO_TASK_ID,
            "goal": DEMO_SUMMARY,
            "status": "done" if review_passed else "failed",
            "change_type": "execution_demo",
            "workflow_phase": "demo_review",
            "done_condition_met": review_passed,
            "next_step": "Inspect the audit entry for this demo run.",
            "reporter_summary": (
                f"Execution {execution_result.get('status', 'unknown')}; "
                f"review {review_summary}; "
                f"path={execution_result.get('result_payload', {}).get('path', '')}"
            ),
        }
    )

    audit_entry = {
        "audit_id": f"audit-{timestamp.replace(':', '').replace('-', '')}",
        "task_id": DEMO_TASK_ID,
        "summary": DEMO_SUMMARY,
        "timestamp": timestamp,
        "status": "success" if review_passed else "failure",
        "decision": decision["execution_preference"],
        "runtime_state": decision["overall_runtime_state"],
        "execution_status": execution_result.get("status", "unknown"),
        "review_status": review_summary,
        "memory_recorded": bool(memory_record),
        "output_path": execution_result.get("result_payload", {}).get("path", ""),
    }
    append_audit(audit_entry)

    memory_after = load_memory_records()
    audit_after = _read_jsonl_tail(AUDITS_FILE)
    audit_count_after = audit_count_before + 1
    if AUDITS_FILE.exists():
        audit_count_after = len(
            [
                line
                for line in AUDITS_FILE.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        )

    final_status = "success"
    if execution_result.get("status") != "completed" or not review_passed:
        final_status = "failure"

    module_state = {
        "context_loaded": True,
        "company_name": company_name,
        "system_context_path": str(DEFAULT_SYSTEM_CONTEXT_PATH),
        "runtime_role": runtime_config.role,
        "execution_status": execution_result.get("status", "unknown"),
        "review_status": review_summary,
        "memory_records_before": len(memory_before),
        "memory_records_after": len(memory_after),
        "audit_entries_before": audit_count_before,
        "audit_entries_after": audit_count_after,
        "output_file_exists": (ROOT_DIR / DEMO_OUTPUT_PATH).exists(),
    }

    print("--- DEMO START ---")
    print(f"INPUT: {DEMO_TASK_ID}")
    print()
    print("PROCESSING:")
    print(f"- loaded system context from {DEFAULT_SYSTEM_CONTEXT_PATH}")
    print(f"- product box company: {company_name}")
    print(f"- runtime role: {runtime_config.role}")
    print(
        "- decision built: "
        f"{decision['execution_preference']} ({decision['overall_runtime_state']})"
    )
    print(
        "- execution path wrote: "
        f"{execution_result.get('result_payload', {}).get('path', '(missing)')}"
    )
    print(f"- review path result: {review_summary}")
    print(f"- memory records: {len(memory_before)} -> {len(memory_after)}")
    print(f"- audit entries: {audit_count_before} -> {audit_count_after}")
    print()
    print("OUTPUT:")
    print(f"- status: {final_status}")
    print(
        "- decision: "
        f"{decision['execution_preference']} | "
        f"runtime_state={decision['overall_runtime_state']} | "
        f"confidence={decision['confidence']}"
    )
    print(f"- module state: {_json_pretty(module_state)}")
    print(f"- memory snapshot: {_json_pretty(memory_record)}")
    print(f"- last audit entry: {_json_pretty(audit_after)}")
    print("--- DEMO END ---")


if __name__ == "__main__":
    main()
