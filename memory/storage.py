from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from app.execution.paths import CONTACTS_FILE, MEMORY_FILE, OUTPUT_DIR


@dataclass(frozen=True)
class Contact:
    contact_id: str
    phone_numbers: list[str]
    emails: list[str]
    name: str | None
    source_channel: str
    created_at: str
    last_seen: str

    def as_dict(self) -> dict[str, object]:
        return {
            "contact_id": self.contact_id,
            "phone_numbers": list(self.phone_numbers),
            "emails": list(self.emails),
            "name": self.name,
            "source_channel": self.source_channel,
            "created_at": self.created_at,
            "last_seen": self.last_seen,
        }


def now() -> str:
    return datetime.utcnow().isoformat() + "Z"


def ensure_memory_storage() -> None:
    MEMORY_FILE.parent.mkdir(parents=True, exist_ok=True)

    if not MEMORY_FILE.exists():
        MEMORY_FILE.write_text("[]\n", encoding="utf-8")
    if not CONTACTS_FILE.exists():
        CONTACTS_FILE.write_text("[]\n", encoding="utf-8")


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _normalize_phone(value: object) -> str:
    raw = _normalize_text(value)
    if not raw:
        return ""
    stripped = re.sub(r"[^\d+]", "", raw)
    if stripped.startswith("00"):
        stripped = "+" + stripped[2:]
    return stripped


def _normalize_email(value: object) -> str:
    return _normalize_text(value).lower()


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value and value not in seen:
            seen.add(value)
            result.append(value)
    return result


def load_memory_records() -> list[dict]:
    ensure_memory_storage()

    try:
        with open(MEMORY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []

    if isinstance(data, list):
        return data

    return []


def load_contacts() -> list[dict]:
    ensure_memory_storage()

    try:
        with open(CONTACTS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []

    if isinstance(data, list):
        return data

    return []


def save_contacts(records: list[dict]) -> None:
    ensure_memory_storage()
    with open(CONTACTS_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)


def load_recent_memory_records(limit: int = 3) -> list[dict]:
    if limit <= 0:
        return []

    records = load_memory_records()
    return records[-limit:]


def find_task_record_by_linear_task_id(linear_task_id: str) -> dict | None:
    normalized_task_id = _normalize_text(linear_task_id)
    if not normalized_task_id:
        return None

    for record in reversed(load_memory_records()):
        saved_task_id = _normalize_text(record.get("linear_task_id"))
        if saved_task_id == normalized_task_id:
            return record

    return None


def find_task_record_by_goal(goal: str) -> dict | None:
    normalized_goal = _normalize_text(goal)
    if not normalized_goal:
        return None

    for record in reversed(load_memory_records()):
        saved_goal = _normalize_text(record.get("goal"))
        if saved_goal == normalized_goal:
            return record

    return None


def output_file_path(task_id: str) -> Path:
    return OUTPUT_DIR / f"generated_{task_id}.py"


def build_task_result(task: dict) -> str:
    reporter_summary = _normalize_text(task.get("reporter_summary"))
    if reporter_summary:
        return reporter_summary

    for step_result in reversed(task.get("results", [])):
        detail = _normalize_text(step_result.get("result"))
        status = _normalize_text(task.get("status", "unknown"))

        if detail and status:
            return f"{status}: {detail}"
        if detail:
            return detail
        if status:
            return status

    return _normalize_text(task.get("status", "unknown"))


def build_task_record(task: dict) -> dict:
    task_id = _normalize_text(task.get("task_id", "unknown"))

    return {
        "task_id": task_id,
        "goal": _normalize_text(task.get("goal")),
        "result": build_task_result(task),
        "file_path": str(output_file_path(task_id)),
        "timestamp": now(),
        "status": _normalize_text(task.get("status", "done")),
        "change_type": _normalize_text(task.get("change_type", "review")),
        "workflow_phase": _normalize_text(task.get("workflow_phase", "report")),
        "linear_task_id": _normalize_text(task.get("linear_task_id")),
        "linear_task_title": _normalize_text(task.get("linear_task_title")),
        "linear_status": _normalize_text(task.get("linear_status")),
        "mvp_priority": _normalize_text(task.get("mvp_priority")),
        "done_condition_met": bool(task.get("done_condition_met", False)),
        "next_step": _normalize_text(task.get("next_step")),
        "contact_id": _normalize_text(task.get("contact_id")),
    }


def save_task_record(task: dict) -> dict:
    records = load_memory_records()
    record = build_task_record(task)
    records.append(record)

    with open(MEMORY_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    return record


def _extract_emails(text: object) -> list[str]:
    matches = re.findall(
        r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}",
        _normalize_text(text),
        flags=re.IGNORECASE,
    )
    return _unique([_normalize_email(item) for item in matches])


def _extract_phone_numbers(text: object) -> list[str]:
    matches = re.findall(r"(?:\+?\d[\d\-\s()]{6,}\d)", _normalize_text(text))
    return _unique([_normalize_phone(item) for item in matches if _normalize_phone(item)])


def _extract_name(text: object) -> str:
    normalized_text = _normalize_text(text)
    patterns = (
        r"\bmy name is ([A-Za-z][A-Za-z\s'-]{1,60}?)(?:\s+and\b|[.!?,]|$)",
        r"\bthis is ([A-Za-z][A-Za-z\s'-]{1,60}?)(?:\s+and\b|[.!?,]|$)",
        r"\bi am ([A-Za-z][A-Za-z\s'-]{1,60}?)(?:\s+and\b|[.!?,]|$)",
    )
    for pattern in patterns:
        match = re.search(pattern, normalized_text, flags=re.IGNORECASE)
        if match:
            return _normalize_text(match.group(1).rstrip(".!,?"))
    return ""


def _build_contact(
    *,
    contact_id: str,
    phone_numbers: list[str],
    emails: list[str],
    name: str,
    source_channel: str,
    created_at: str,
    last_seen: str,
) -> dict[str, object]:
    return Contact(
        contact_id=contact_id,
        phone_numbers=_unique(phone_numbers),
        emails=_unique(emails),
        name=name or None,
        source_channel=source_channel,
        created_at=created_at,
        last_seen=last_seen,
    ).as_dict()


def _find_contact_index(
    *,
    records: list[dict],
    contact_id: str,
    phone: str,
    email: str,
) -> int:
    for index, record in enumerate(records):
        if contact_id and _normalize_text(record.get("contact_id")) == contact_id:
            return index
        if phone and phone in [_normalize_phone(item) for item in record.get("phone_numbers", [])]:
            return index
        if email and email in [_normalize_email(item) for item in record.get("emails", [])]:
            return index
    return -1


def find_contact(
    *,
    contact_id: object = "",
    phone: object = "",
    email: object = "",
) -> dict | None:
    records = load_contacts()
    match_index = _find_contact_index(
        records=records,
        contact_id=_normalize_text(contact_id),
        phone=_normalize_phone(phone),
        email=_normalize_email(email),
    )
    if match_index < 0:
        return None
    return dict(records[match_index])


def upsert_contact(
    *,
    source_channel: object,
    contact_id: object,
    raw_text: object,
) -> dict[str, object]:
    records = load_contacts()
    normalized_contact_id = _normalize_text(contact_id)
    normalized_channel = _normalize_text(source_channel).lower()
    timestamp = now()
    phones = _extract_phone_numbers(raw_text)
    emails = _extract_emails(raw_text)
    name = _extract_name(raw_text)
    direct_phone = _normalize_phone(contact_id)
    direct_email = _normalize_email(contact_id) if "@" in normalized_contact_id else ""
    if direct_phone:
        phones.insert(0, direct_phone)
    if direct_email:
        emails.insert(0, direct_email)

    match_index = _find_contact_index(
        records=records,
        contact_id=normalized_contact_id,
        phone=direct_phone,
        email=direct_email,
    )

    if match_index >= 0:
        existing = dict(records[match_index])
        updated = _build_contact(
            contact_id=_normalize_text(existing.get("contact_id")) or normalized_contact_id,
            phone_numbers=list(existing.get("phone_numbers", [])) + phones,
            emails=list(existing.get("emails", [])) + emails,
            name=name or _normalize_text(existing.get("name")),
            source_channel=normalized_channel or _normalize_text(existing.get("source_channel")),
            created_at=_normalize_text(existing.get("created_at")) or timestamp,
            last_seen=timestamp,
        )
        records[match_index] = updated
        save_contacts(records)
        return updated

    record = _build_contact(
        contact_id=normalized_contact_id or f"contact-{uuid.uuid4().hex[:12]}",
        phone_numbers=phones,
        emails=emails,
        name=name,
        source_channel=normalized_channel,
        created_at=timestamp,
        last_seen=timestamp,
    )
    records.append(record)
    save_contacts(records)
    return record


def find_recent_tasks_by_contact_id(contact_id: object, limit: int = 3) -> list[dict]:
    normalized_contact_id = _normalize_text(contact_id)
    if not normalized_contact_id or limit <= 0:
        return []
    records = [
        dict(record)
        for record in reversed(load_memory_records())
        if _normalize_text(record.get("contact_id")) == normalized_contact_id
    ]
    return records[:limit]

