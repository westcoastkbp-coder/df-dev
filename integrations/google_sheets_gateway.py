from __future__ import annotations

import json
import os
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from app.execution.paths import OUTPUT_DIR

MOCK_SHEETS_ROOT = OUTPUT_DIR / "external_business" / "google_sheets_gateway"
TRUE_VALUES = {"1", "true", "yes", "on"}


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _timestamp() -> str:
    return time.strftime("%Y%m%d%H%M%S")


def _requested_mode() -> str:
    raw_mode = os.environ.get("DIGITAL_FOREMAN_GOOGLE_SHEETS_GATEWAY_MODE", "mock").strip()
    normalized = raw_mode.lower()
    if normalized == "real":
        return "real"
    return "mock"


def _api_connected() -> bool:
    raw_value = os.environ.get("DIGITAL_FOREMAN_GOOGLE_SHEETS_API_CONNECTED", "").strip()
    return raw_value.lower() in TRUE_VALUES


def _resolve_mode() -> tuple[str, str | None]:
    requested_mode = _requested_mode()
    if requested_mode == "real" and _api_connected():
        return (
            "mock",
            "Real Google Sheets API mode is intentionally deferred in this MVP; using mock mode.",
        )
    if requested_mode == "real":
        return "mock", "Google Sheets API is not connected; using mock mode."
    return "mock", None


def _slugify(value: object) -> str:
    cleaned = "".join(
        char.lower() if char.isalnum() else "-"
        for char in _normalize_text(value)
    )
    collapsed = "-".join(part for part in cleaned.split("-") if part)
    return collapsed or "sheet"


def _base_result(operation: str) -> dict[str, Any]:
    mode, fallback_reason = _resolve_mode()
    return {
        "ok": True,
        "provider": "google_sheets_gateway",
        "operation": operation,
        "status": "completed",
        "mode": mode,
        "requested_mode": _requested_mode(),
        "api_connected": _api_connected(),
        "fallback_reason": fallback_reason,
        "resource": {},
        "error": None,
        "log": [],
    }


def _fail(result: dict[str, Any], message: str) -> dict[str, Any]:
    result["ok"] = False
    result["status"] = "failed"
    result["error"] = message
    result["log"].append(message)
    return result


def _normalize_row(row: object) -> dict[str, object]:
    if not isinstance(row, Mapping):
        return {}
    return {
        _normalize_text(key): value
        for key, value in dict(row).items()
        if _normalize_text(key)
    }


def append_row(
    *,
    spreadsheet: object,
    worksheet: object,
    row: object,
) -> dict[str, Any]:
    result = _base_result("append_row")
    normalized_spreadsheet = _normalize_text(spreadsheet)
    normalized_worksheet = _normalize_text(worksheet)
    normalized_row = _normalize_row(row)

    if not normalized_spreadsheet:
        return _fail(result, "Google Sheets spreadsheet name is required.")
    if not normalized_worksheet:
        return _fail(result, "Google Sheets worksheet name is required.")
    if not normalized_row:
        return _fail(result, "Google Sheets row payload is required.")

    target_dir = MOCK_SHEETS_ROOT / "rows"
    target_dir.mkdir(parents=True, exist_ok=True)
    target_file = target_dir / f"{_slugify(normalized_spreadsheet)}.jsonl"
    row_id = f"gsheet-row-{_timestamp()}-{_slugify(normalized_worksheet)}"
    entry = {
        "row_id": row_id,
        "spreadsheet": normalized_spreadsheet,
        "worksheet": normalized_worksheet,
        "row": normalized_row,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    with target_file.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, ensure_ascii=True) + "\n")

    result["resource"] = {
        "row_id": row_id,
        "spreadsheet": normalized_spreadsheet,
        "worksheet": normalized_worksheet,
        "path": str(target_file),
        "row": normalized_row,
    }
    result["log"].append(
        f"mock sheet row appended spreadsheet={normalized_spreadsheet} worksheet={normalized_worksheet}"
    )
    return result
