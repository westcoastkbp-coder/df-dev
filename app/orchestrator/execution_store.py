from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Iterable

from app.execution.paths import CONTROLLED_EXECUTIONS_FILE

AUDIT_SCHEMA_VERSION = "v1"
READ_ONLY_RECORD_FIELDS = {
    "recorded_at",
    "audit",
    "integrity",
}


def now() -> str:
    return datetime.utcnow().isoformat() + "Z"


def ensure_execution_storage() -> None:
    CONTROLLED_EXECUTIONS_FILE.parent.mkdir(parents=True, exist_ok=True)

    if not CONTROLLED_EXECUTIONS_FILE.exists():
        CONTROLLED_EXECUTIONS_FILE.write_text("[]\n", encoding="utf-8")


def _canonical_json(payload: object) -> str:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )


def _sha256_text(payload: str) -> str:
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _record_body(record: dict[str, object]) -> dict[str, object]:
    return {
        str(key): value
        for key, value in dict(record).items()
        if str(key) not in READ_ONLY_RECORD_FIELDS
    }


def _normalize_actor(actor: dict[str, object] | None) -> dict[str, str]:
    payload = dict(actor or {})
    actor_id = str(payload.get("actor_id", "")).strip() or "system"
    role = str(payload.get("role", "")).strip().lower() or "system"
    return {
        "actor_id": actor_id,
        "role": role,
    }


def _audit_metadata(
    *,
    actor: dict[str, object] | None,
    action: str,
    task_id: str,
    timestamp: str,
) -> dict[str, object]:
    principal = _normalize_actor(actor)
    return {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "action": str(action).strip() or "controlled_execution.recorded",
        "actor_id": principal["actor_id"],
        "role": principal["role"],
        "task_id": str(task_id).strip(),
        "timestamp": timestamp,
    }


def _build_integrity_metadata(
    *,
    record_body: dict[str, object],
    previous_chain_hash: str,
    index: int,
) -> dict[str, object]:
    canonical_body = _canonical_json(record_body)
    payload_hash = _sha256_text(canonical_body)
    chain_hash = _sha256_text(f"{previous_chain_hash}:{payload_hash}")
    return {
        "schema_version": AUDIT_SCHEMA_VERSION,
        "index": int(index),
        "previous_chain_hash": str(previous_chain_hash).strip(),
        "payload_hash": payload_hash,
        "chain_hash": chain_hash,
    }


def load_execution_records() -> list[dict]:
    ensure_execution_storage()

    try:
        with open(CONTROLLED_EXECUTIONS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []

    if isinstance(data, list):
        return data

    return []


def save_execution_record(
    result: dict[str, object],
    *,
    actor: dict[str, object] | None = None,
    action: str = "controlled_execution.recorded",
) -> dict[str, object]:
    records = load_execution_records()
    record = _record_body(result)
    recorded_at = now()
    record["recorded_at"] = recorded_at
    record["audit"] = _audit_metadata(
        actor=actor,
        action=action,
        task_id=str(record.get("task_id", "")).strip(),
        timestamp=recorded_at,
    )

    previous_chain_hash = ""
    if records:
        previous_chain_hash = str(
            dict(records[-1].get("integrity", {})).get("chain_hash", "")
        ).strip()
    integrity_body = _record_body(record)
    record["integrity"] = _build_integrity_metadata(
        record_body=integrity_body,
        previous_chain_hash=previous_chain_hash,
        index=len(records),
    )

    records.append(record)

    with open(CONTROLLED_EXECUTIONS_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    return record


def _verify_record_chain(
    record: dict[str, object],
    *,
    previous_chain_hash: str,
    index: int,
) -> list[str]:
    issues: list[str] = []
    integrity = dict(record.get("integrity", {}))
    expected = _build_integrity_metadata(
        record_body=_record_body(record),
        previous_chain_hash=previous_chain_hash,
        index=index,
    )

    for field in (
        "schema_version",
        "index",
        "previous_chain_hash",
        "payload_hash",
        "chain_hash",
    ):
        if integrity.get(field) != expected[field]:
            issues.append(f"record {index} integrity mismatch for `{field}`")

    audit = dict(record.get("audit", {}))
    if not str(audit.get("actor_id", "")).strip():
        issues.append(f"record {index} missing audit.actor_id")
    if not str(audit.get("role", "")).strip():
        issues.append(f"record {index} missing audit.role")
    if not str(record.get("recorded_at", "")).strip():
        issues.append(f"record {index} missing recorded_at")

    return issues


def verify_execution_records(
    records: Iterable[dict[str, object]] | None = None,
) -> dict[str, object]:
    loaded_records = list(records if records is not None else load_execution_records())
    issues: list[str] = []
    previous_chain_hash = ""

    for index, record in enumerate(loaded_records):
        record_issues = _verify_record_chain(
            dict(record),
            previous_chain_hash=previous_chain_hash,
            index=index,
        )
        issues.extend(record_issues)
        previous_chain_hash = str(
            dict(record.get("integrity", {})).get("chain_hash", "")
        ).strip()

    latest_chain_hash = previous_chain_hash
    return {
        "valid": not issues,
        "count": len(loaded_records),
        "latest_chain_hash": latest_chain_hash,
        "issues": tuple(issues),
    }


def find_execution_record(task_id: str) -> dict[str, object] | None:
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id:
        return None

    for record in reversed(load_execution_records()):
        if str(record.get("task_id", "")).strip() == normalized_task_id:
            return record

    return None

