from __future__ import annotations

import json
import sys
import time
import uuid
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from app.execution.paths import LOGS_DIR, ROOT_DIR
from app.execution.product_box_manifest import (
    ProductBoxManifestError,
    load_product_box_manifest,
)


def _matches_blocked_module(module_name: str, blocked_pattern: str) -> bool:
    normalized_pattern = (
        str(blocked_pattern or "")
        .strip()
        .replace("\\", "/")
        .removesuffix("/*")
        .replace("/", ".")
    )
    return module_name == normalized_pattern or module_name.startswith(
        normalized_pattern + "."
    )


def _required_runtime_paths(manifest: dict[str, object]) -> list[str]:
    return [
        str(path).strip().replace("\\", "/").strip("/")
        for path in list(manifest.get("required_runtime_paths", []) or [])
    ]


def _missing_path_from_manifest_error(error: Exception) -> str:
    message = str(error).strip()
    marker = "missing path `"
    if marker not in message:
        return ""
    return message.partition(marker)[2].partition("`")[0].strip().replace("\\", "/")


def _detect_dev_leaks(manifest: dict[str, object]) -> list[str]:
    leaks: list[str] = []
    tests_dir = ROOT_DIR / "tests"
    if tests_dir.exists():
        leaks.append("tests")

    scripts_dir = ROOT_DIR / "scripts"
    if scripts_dir.exists():
        allowed_script_files = {"start_df.py"}
        unexpected_scripts = [
            path
            for path in scripts_dir.rglob("*")
            if path.is_file()
            and path.relative_to(scripts_dir).as_posix() not in allowed_script_files
        ]
        if unexpected_scripts:
            leaks.append("scripts")
    blocked_patterns = [
        str(item).strip() for item in list(manifest.get("blocked_modules", []) or [])
    ]
    for module_name in sorted(sys.modules):
        if any(
            _matches_blocked_module(module_name, pattern)
            for pattern in blocked_patterns
        ):
            leaks.append(module_name)
    return sorted(set(leaks))


def _run_real_task(system_context: dict[str, object]) -> tuple[str, list[str]]:
    from app.orchestrator.task_factory import create_task, save_task
    from app.orchestrator.task_lifecycle import set_task_state
    from app.orchestrator.task_queue import task_queue
    from app.orchestrator.task_worker import process_next_queued_task

    missing_logs: list[str] = []
    task_id = f"DF-PRODUCT-BOOT-SIM-{uuid.uuid4().hex[:8].upper()}"
    task = create_task(
        {
            "task_id": task_id,
            "status": "created",
            "intent": "new_lead",
            "payload": {
                "summary": "Detached ADU pricing request",
                "request": "Need a detached ADU estimate at 123 Main St",
            },
        }
    )
    set_task_state(
        task,
        "VALIDATED",
        timestamp="2026-04-04T00:00:00Z",
        details="boot simulation validated task",
    )
    save_task(task)
    task_queue.clear()
    task_queue.enqueue_task(task["task_id"])
    result = process_next_queued_task(
        queue=task_queue,
        now=lambda: "2026-04-04T00:00:00Z",
        timeout=0.0,
        telemetry_collector=lambda: {},
        network_snapshot_collector=lambda: {},
        system_context=system_context,
    )
    required_logs = tuple(
        str((LOGS_DIR / log_name).as_posix())
        for log_name in ("tasks.log", "system.log")
    )
    if (
        not isinstance(result, dict)
        or str(result.get("status", "")).strip().upper() != "COMPLETED"
    ):
        return "FAIL", list(required_logs)

    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        pending_logs = [
            relative_path
            for relative_path in required_logs
            if not (ROOT_DIR / Path(relative_path)).exists()
            or not (ROOT_DIR / Path(relative_path)).read_text(encoding="utf-8").strip()
        ]
        if not pending_logs:
            return "OK", []
        time.sleep(0.05)
    missing_logs.extend(pending_logs)
    return ("OK" if not missing_logs else "FAIL"), missing_logs


def boot_product_box(system_context: dict[str, object]) -> dict[str, object]:
    if system_context is None:
        raise RuntimeError("SYSTEM_CONTEXT_REQUIRED")

    report: dict[str, object] = {
        "boot_status": "FAIL",
        "dev_leaks": [],
        "missing_files": [],
        "execution_status": "FAIL",
    }

    try:
        manifest = load_product_box_manifest()
    except ProductBoxManifestError as exc:
        missing_path = _missing_path_from_manifest_error(exc)
        report["missing_files"] = [missing_path] if missing_path else []
        return report
    missing_files = [
        relative_path
        for relative_path in _required_runtime_paths(manifest)
        if not (ROOT_DIR / relative_path).exists()
    ]
    from app.orchestrator.task_state_store import initialize_database
    import app.voice.app as voice_app_module

    initialize_database()
    voice_app_module.configure_operator_task_worker(system_context)
    voice_app_module._start_operator_task_worker()
    try:
        dev_leaks = _detect_dev_leaks(manifest)
        execution_status, missing_logs = _run_real_task(system_context)
        missing_files.extend(missing_logs)
        report["dev_leaks"] = dev_leaks
        report["missing_files"] = sorted(set(missing_files))
        report["execution_status"] = execution_status
        report["boot_status"] = (
            "PASS"
            if not report["dev_leaks"]
            and not report["missing_files"]
            and execution_status == "OK"
            else "FAIL"
        )
        return report
    finally:
        voice_app_module._stop_operator_task_worker()


if __name__ == "__main__":
    raise RuntimeError("SYSTEM_CONTEXT_REQUIRED")
