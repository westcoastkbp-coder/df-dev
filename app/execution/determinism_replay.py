from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path

from app.execution.paths import LOGS_DIR, ROOT_DIR


DETERMINISM_REPLAY_LOG_FILE = ROOT_DIR / LOGS_DIR / "determinism_replay.jsonl"


def _normalize_mapping(value: object) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _normalize_sequence(value: object) -> list[dict[str, object]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    items: list[dict[str, object]] = []
    for item in value:
        if isinstance(item, Mapping):
            items.append(dict(item))
    return items


def build_determinism_snapshot(
    *,
    task_data: Mapping[str, object],
    trace_sequence: Sequence[Mapping[str, object]],
) -> dict[str, object]:
    result = _normalize_mapping(task_data.get("result"))
    action_result = result
    result_payload = _normalize_mapping(result.get("result_payload"))
    lifecycle_transitions = [
        {
            "event": str(entry.get("event", "")).strip(),
            "from_status": str(entry.get("from_status", "")).strip(),
            "to_status": str(entry.get("to_status", "")).strip(),
        }
        for entry in _normalize_sequence(task_data.get("history"))
    ]
    return {
        "decision": _normalize_mapping(result_payload.get("decision")),
        "final_task_state": {
            "status": str(task_data.get("status", "")).strip(),
            "error": str(task_data.get("error", "")).strip(),
            "started_at": str(task_data.get("started_at", "")).strip(),
            "completed_at": str(task_data.get("completed_at", "")).strip(),
            "failed_at": str(task_data.get("failed_at", "")).strip(),
            "result": action_result,
        },
        "action_result": action_result,
        "execution_order": [
            str(step.get("step_name", "")).strip()
            for step in trace_sequence
        ],
        "lifecycle_transitions": lifecycle_transitions,
    }


def compare_determinism_snapshots(
    baseline: Mapping[str, object],
    candidate: Mapping[str, object],
) -> tuple[bool, str]:
    baseline_snapshot = dict(baseline)
    candidate_snapshot = dict(candidate)
    if baseline_snapshot == candidate_snapshot:
        return True, ""
    for field in (
        "decision",
        "final_task_state",
        "action_result",
        "execution_order",
        "lifecycle_transitions",
    ):
        if baseline_snapshot.get(field) != candidate_snapshot.get(field):
            return False, field
    return False, "snapshot"


def append_determinism_replay_log(
    entry: Mapping[str, object],
    *,
    log_path: Path | None = None,
) -> Path:
    target = Path(log_path) if log_path is not None else DETERMINISM_REPLAY_LOG_FILE
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(entry), ensure_ascii=True, separators=(",", ":")) + "\n")
    return target
