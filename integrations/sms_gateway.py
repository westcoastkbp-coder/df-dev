from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

from app.execution.paths import OUTPUT_DIR

LOGGER = logging.getLogger(__name__)
MOCK_SMS_ROOT = OUTPUT_DIR / "external_business" / "sms_gateway"


def _log_event(log: list[str], message: str) -> None:
    LOGGER.info("sms_gateway: %s", message)
    log.append(message)


def _timestamp() -> str:
    return time.strftime("%Y%m%d%H%M%S")


def _ensure_outbox() -> Path:
    outbox = MOCK_SMS_ROOT / "outbox"
    outbox.mkdir(parents=True, exist_ok=True)
    return outbox


def send_sms(*, contact_id: str, text: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "ok": True,
        "provider": "sms_gateway",
        "operation": "send_sms",
        "status": "completed",
        "mode": "mock",
        "resource": {},
        "error": None,
        "log": [],
    }

    normalized_contact_id = str(contact_id or "").strip()
    normalized_text = str(text or "").strip()
    _log_event(
        result["log"],
        f"start send_sms contact_id={normalized_contact_id or '(empty)'}",
    )

    if not normalized_contact_id:
        result["ok"] = False
        result["status"] = "failed"
        result["error"] = "SMS contact_id is required."
        return result

    if not normalized_text:
        result["ok"] = False
        result["status"] = "failed"
        result["error"] = "SMS text is required."
        return result

    message_id = f"sms-msg-{_timestamp()}"
    payload = {
        "message_id": message_id,
        "contact_id": normalized_contact_id,
        "text": normalized_text,
        "mode": "mock",
    }
    target_path = _ensure_outbox() / f"{message_id}.json"
    target_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    result["resource"] = {
        "message_id": message_id,
        "contact_id": normalized_contact_id,
        "saved_path": str(target_path),
    }
    _log_event(result["log"], f"mock sms recorded path={target_path}")
    return result
