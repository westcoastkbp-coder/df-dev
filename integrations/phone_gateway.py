from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

from app.execution.paths import OUTPUT_DIR

LOGGER = logging.getLogger(__name__)
MOCK_PHONE_ROOT = OUTPUT_DIR / "external_business" / "phone_gateway"


def _log_event(log: list[str], message: str) -> None:
    LOGGER.info("phone_gateway: %s", message)
    log.append(message)


def _timestamp() -> str:
    return time.strftime("%Y%m%d%H%M%S")


def _ensure_outbox() -> Path:
    outbox = MOCK_PHONE_ROOT / "outbox"
    outbox.mkdir(parents=True, exist_ok=True)
    return outbox


def schedule_call(*, contact_id: str, phone_number: str, script: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "ok": True,
        "provider": "phone_gateway",
        "operation": "schedule_call",
        "status": "completed",
        "mode": "mock",
        "resource": {},
        "error": None,
        "log": [],
    }

    normalized_contact_id = str(contact_id or "").strip()
    normalized_phone_number = str(phone_number or "").strip()
    normalized_script = str(script or "").strip()
    _log_event(
        result["log"],
        f"start schedule_call contact_id={normalized_contact_id or '(empty)'}",
    )

    if not normalized_contact_id:
        result["ok"] = False
        result["status"] = "failed"
        result["error"] = "Phone contact_id is required."
        return result

    if not normalized_phone_number:
        result["ok"] = False
        result["status"] = "failed"
        result["error"] = "Phone number is required."
        return result

    if not normalized_script:
        result["ok"] = False
        result["status"] = "failed"
        result["error"] = "Call script is required."
        return result

    call_id = f"call-msg-{_timestamp()}"
    payload = {
        "call_id": call_id,
        "contact_id": normalized_contact_id,
        "phone_number": normalized_phone_number,
        "script": normalized_script,
        "mode": "mock",
    }
    target_path = _ensure_outbox() / f"{call_id}.json"
    target_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    result["resource"] = {
        "call_id": call_id,
        "contact_id": normalized_contact_id,
        "phone_number": normalized_phone_number,
        "saved_path": str(target_path),
    }
    _log_event(result["log"], f"mock call recorded path={target_path}")
    return result

