from __future__ import annotations

import logging
import os
import shutil
import time
from pathlib import Path
from typing import Any

from app.execution.paths import OUTPUT_DIR

LOGGER = logging.getLogger(__name__)
MOCK_DRIVE_ROOT = OUTPUT_DIR / "external_business" / "drive_gateway"
TRUE_VALUES = {"1", "true", "yes", "on"}


def _log_event(log: list[str], message: str) -> None:
    LOGGER.info("drive_gateway: %s", message)
    log.append(message)


def _timestamp() -> str:
    return time.strftime("%Y%m%d%H%M%S")


def _slugify(value: str) -> str:
    cleaned = "".join(
        char.lower() if char.isalnum() else "-" for char in str(value or "").strip()
    )
    collapsed = "-".join(part for part in cleaned.split("-") if part)
    return collapsed or "item"


def _requested_mode() -> str:
    raw_mode = os.environ.get("DIGITAL_FOREMAN_DRIVE_GATEWAY_MODE", "mock").strip()
    normalized = raw_mode.lower()
    if normalized in {"mock", "stub", "local"}:
        return "mock"
    if normalized == "real":
        return "real"
    return "mock"


def _api_connected() -> bool:
    raw_value = os.environ.get("DIGITAL_FOREMAN_DRIVE_API_CONNECTED", "").strip()
    return raw_value.lower() in TRUE_VALUES


def _resolve_mode() -> tuple[str, str | None]:
    requested_mode = _requested_mode()
    api_connected = _api_connected()

    if requested_mode == "real" and api_connected:
        return (
            "mock",
            "Real Google Drive API mode is intentionally deferred in this MVP; using mock mode.",
        )

    if requested_mode == "real":
        return (
            "mock",
            "Google Drive API is not connected; using mock mode.",
        )

    return "mock", None


def _base_result(operation: str) -> dict[str, Any]:
    mode, fallback_reason = _resolve_mode()
    return {
        "ok": True,
        "provider": "drive_gateway",
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
    _log_event(result["log"], message)
    return result


def _ensure_root(child: str) -> Path:
    path = MOCK_DRIVE_ROOT / child
    path.mkdir(parents=True, exist_ok=True)
    return path


def create_folder(name: str) -> dict[str, Any]:
    result = _base_result("create_folder")
    normalized_name = str(name or "").strip()
    _log_event(
        result["log"], f"start create_folder name={normalized_name or '(empty)'}"
    )

    if not normalized_name:
        return _fail(result, "Drive folder name is required.")

    folder_id = f"drv-folder-{_timestamp()}-{_slugify(normalized_name)}"
    folder_path = _ensure_root("folders") / folder_id
    folder_path.mkdir(parents=True, exist_ok=True)

    result["resource"] = {
        "folder_id": folder_id,
        "name": normalized_name,
        "path": str(folder_path),
    }
    _log_event(result["log"], f"mock folder ready path={folder_path}")
    return result


def create_file(name: str, content: str) -> dict[str, Any]:
    result = _base_result("create_file")
    normalized_name = Path(str(name or "").strip() or "document.txt").name
    _log_event(result["log"], f"start create_file name={normalized_name}")

    if not normalized_name:
        return _fail(result, "Drive file name is required.")

    file_id = f"drv-file-{_timestamp()}-{_slugify(normalized_name)}"
    target_path = _ensure_root("files") / f"{file_id}-{normalized_name}"
    text_content = str(content or "")
    target_path.write_text(text_content, encoding="utf-8")

    result["resource"] = {
        "file_id": file_id,
        "name": normalized_name,
        "path": str(target_path),
        "bytes": len(text_content.encode("utf-8")),
    }
    _log_event(result["log"], f"mock file saved path={target_path}")
    return result


def upload_file(path: str) -> dict[str, Any]:
    result = _base_result("upload_file")
    source_path = Path(str(path or "").strip()).expanduser()
    if not source_path.is_absolute():
        source_path = source_path.resolve()

    _log_event(result["log"], f"start upload_file path={source_path}")

    if not source_path.exists() or not source_path.is_file():
        return _fail(result, f"Drive upload source is missing: {source_path}")

    file_id = f"drv-upload-{_timestamp()}-{_slugify(source_path.name)}"
    target_path = _ensure_root("uploads") / f"{file_id}-{source_path.name}"
    shutil.copy2(source_path, target_path)

    result["resource"] = {
        "file_id": file_id,
        "name": source_path.name,
        "source_path": str(source_path),
        "path": str(target_path),
        "bytes": target_path.stat().st_size,
    }
    _log_event(result["log"], f"mock upload copied path={target_path}")
    return result


def get_share_link(file_id: str) -> dict[str, Any]:
    result = _base_result("get_share_link")
    normalized_file_id = str(file_id or "").strip()
    _log_event(
        result["log"], f"start get_share_link file_id={normalized_file_id or '(empty)'}"
    )

    if not normalized_file_id:
        return _fail(result, "Drive file_id is required to build a share link.")

    share_link = f"https://drive.mock.local/share/{normalized_file_id}"
    result["resource"] = {
        "file_id": normalized_file_id,
        "share_link": share_link,
    }
    _log_event(result["log"], f"mock share link generated link={share_link}")
    return result

