from __future__ import annotations

import hashlib
import json
import os
import platform
import sqlite3
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from app.config.hybrid_runtime import HybridRuntimeConfig, load_runtime_config
from app.config.hybrid_topology import HYBRID_SERVICE_BOUNDARY
from app.context.shared_context_store import prepare_shared_context_store
from app.execution.paths import DATA_DIR, LOGS_DIR, OUTPUT_DIR, RUNTIME_DIR, STATE_DIR
from app.execution.product_box_manifest import default_product_box_manifest_path, load_product_box_manifest
from runtime.system_log import log_event


MODE = str(os.getenv("MODE", "product")).strip().lower() or "product"


def _entry_url(root_dir: Path) -> str:
    return (root_dir / OUTPUT_DIR / "landing" / "index.html").relative_to(root_dir).as_posix()


def _required_dirs(root_dir: Path, config: HybridRuntimeConfig) -> tuple[Path, ...]:
    shared = (
        root_dir / RUNTIME_DIR,
        root_dir / LOGS_DIR,
        root_dir / STATE_DIR,
        root_dir / OUTPUT_DIR,
        root_dir / DATA_DIR,
        root_dir / "config",
        root_dir / "scripts",
        root_dir / "tools",
    )
    if config.role == "local_dev":
        return shared + (root_dir / "tests", root_dir / "app")
    return shared


def _write_targets(root_dir: Path, config: HybridRuntimeConfig) -> tuple[Path, ...]:
    return (
        root_dir / LOGS_DIR,
        root_dir / STATE_DIR,
        root_dir / DATA_DIR,
        config.storage_paths.logs_dir,
        config.storage_paths.state_dir,
        config.storage_paths.shared_context_dir,
    )


def _root_relative(root_dir: Path, path: Path) -> str:
    try:
        return path.relative_to(root_dir).as_posix()
    except ValueError:
        return path.resolve(strict=False).as_posix()


def _add_check(
    checks: list[dict[str, str]],
    errors: list[str],
    *,
    name: str,
    ok: bool,
    detail: str,
) -> None:
    checks.append(
        {
            "name": name,
            "status": "ok" if ok else "error",
            "detail": detail,
        }
    )
    if not ok:
        errors.append(f"{name}: {detail}")


def _ensure_runtime_layout(root_dir: Path, config: HybridRuntimeConfig) -> None:
    for directory in _required_dirs(root_dir, config):
        directory.mkdir(parents=True, exist_ok=True)
    for directory in (
        config.storage_paths.storage_root,
        config.storage_paths.runtime_root,
        config.storage_paths.logs_dir,
        config.storage_paths.state_dir,
        config.storage_paths.verification_dir,
        config.storage_paths.shared_context_dir,
        config.storage_paths.active_threads_dir,
    ):
        directory.mkdir(parents=True, exist_ok=True)


def _write_probe(path: Path) -> tuple[bool, str]:
    try:
        allowed = os.access(path, os.W_OK)
    except Exception as exc:
        return False, str(exc).strip() or f"write check failed for {path.as_posix()}"
    return allowed, path.as_posix()


def _validate_mode(checks: list[dict[str, str]], errors: list[str]) -> None:
    _add_check(checks, errors, name="mode", ok=MODE == "product", detail=MODE)


def _validate_role_config(
    checks: list[dict[str, str]],
    errors: list[str],
    *,
    config: HybridRuntimeConfig,
) -> None:
    _add_check(checks, errors, name="role", ok=True, detail=config.role)
    _add_check(checks, errors, name="bind:host", ok=bool(config.host), detail=config.host)
    _add_check(checks, errors, name="bind:port", ok=config.port > 0, detail=str(config.port))
    if config.role == "local_dev":
        _add_check(
            checks,
            errors,
            name="remote:endpoint",
            ok=True,
            detail=config.remote_endpoint.base_url or "optional",
        )
        return
    _add_check(
        checks,
        errors,
        name="remote:endpoint",
        ok=True,
        detail=config.remote_endpoint.base_url or "managed externally",
    )


def _validate_paths(
    checks: list[dict[str, str]],
    errors: list[str],
    *,
    root_dir: Path,
    config: HybridRuntimeConfig,
) -> None:
    _add_check(
        checks,
        errors,
        name="root",
        ok=root_dir.exists() and root_dir.is_dir(),
        detail=_root_relative(root_dir, root_dir),
    )
    for directory in _required_dirs(root_dir, config):
        _add_check(
            checks,
            errors,
            name=f"dir:{_root_relative(root_dir, directory)}",
            ok=directory.exists() and directory.is_dir(),
            detail=_root_relative(root_dir, directory),
        )
    for name, directory in (
        ("storage_root", config.storage_paths.storage_root),
        ("runtime_root", config.storage_paths.runtime_root),
        ("logs", config.storage_paths.logs_dir),
        ("state", config.storage_paths.state_dir),
        ("verification", config.storage_paths.verification_dir),
        ("shared_context", config.storage_paths.shared_context_dir),
        ("active_threads", config.storage_paths.active_threads_dir),
    ):
        _add_check(
            checks,
            errors,
            name=f"dir:{name}",
            ok=directory.exists() and directory.is_dir(),
            detail=directory.as_posix(),
        )


def _validate_config(
    checks: list[dict[str, str]],
    errors: list[str],
    *,
    root_dir: Path,
    config: HybridRuntimeConfig,
) -> None:
    manifest_path = default_product_box_manifest_path(root_dir)
    system_context_path = root_dir / "config" / "system_context.yaml"
    if config.role == "remote_runtime":
        _add_check(
            checks,
            errors,
            name="config:manifest_path",
            ok=True,
            detail=_root_relative(root_dir, manifest_path) if manifest_path.exists() else "not-required",
        )
        _add_check(
            checks,
            errors,
            name="config:system_context",
            ok=True,
            detail=_root_relative(root_dir, system_context_path) if system_context_path.exists() else "not-required",
        )
        return
    _add_check(
        checks,
        errors,
        name="config:manifest_path",
        ok=manifest_path.exists(),
        detail=_root_relative(root_dir, manifest_path),
    )
    _add_check(
        checks,
        errors,
        name="config:system_context",
        ok=system_context_path.exists(),
        detail=_root_relative(root_dir, system_context_path),
    )
    if not manifest_path.exists():
        return
    try:
        manifest = load_product_box_manifest()
    except Exception as exc:
        _add_check(
            checks,
            errors,
            name="config:manifest_valid",
            ok=False,
            detail=str(exc).strip() or "manifest validation failed",
        )
        return
    _add_check(
        checks,
        errors,
        name="config:manifest_valid",
        ok=True,
        detail=f"entrypoints={len(list(manifest.get('product_entrypoints', [])))}",
    )


def _validate_write_boundaries(
    checks: list[dict[str, str]],
    errors: list[str],
    *,
    root_dir: Path,
    config: HybridRuntimeConfig,
) -> None:
    for target in _write_targets(root_dir, config):
        ok, detail = _write_probe(target)
        _add_check(
            checks,
            errors,
            name=f"write:{_root_relative(root_dir, target)}",
            ok=ok,
            detail=detail,
        )


def _validate_shared_context_contract(
    checks: list[dict[str, str]],
    errors: list[str],
    *,
    config: HybridRuntimeConfig,
) -> None:
    contract = prepare_shared_context_store(config)
    _add_check(
        checks,
        errors,
        name="context:schema",
        ok=bool(contract.get("schema_version")),
        detail=str(contract.get("schema_version", "")),
    )
    for key in (
        "global_context",
        "active_thread_context",
        "decisions",
        "interaction_history",
        "audit_trail",
        "system_context",
    ):
        entry = dict(contract.get(key, {}))
        _add_check(
            checks,
            errors,
            name=f"context:{key}",
            ok=bool(entry.get("path")),
            detail=str(entry.get("path", "")),
        )


def _validate_runtime_state(
    checks: list[dict[str, str]],
    errors: list[str],
    *,
    root_dir: Path,
    config: HybridRuntimeConfig,
) -> None:
    from app.orchestrator import task_state_store

    task_state_store.ROOT_DIR = root_dir
    task_state_store.LEGACY_TASK_MEMORY_FILE = STATE_DIR / "task_memory.json"
    task_state_store.TASK_STATE_DB_FILE = config.storage_paths.state_dir / "task_state.sqlite3"
    db_path = task_state_store.db_path_for()
    if not db_path.exists() or db_path.stat().st_size == 0:
        _add_check(
            checks,
            errors,
            name="state:task_db",
            ok=True,
            detail=f"{_root_relative(root_dir, db_path)}: init-on-demand",
        )
        return
    connection = sqlite3.connect(str(db_path))
    try:
        row = connection.execute("PRAGMA integrity_check").fetchone()
    finally:
        connection.close()
    result = str(row[0] if row else "").strip().lower()
    _add_check(
        checks,
        errors,
        name="state:task_db",
        ok=result == "ok",
        detail=f"{_root_relative(root_dir, db_path)}: {result or 'integrity_check failed'}",
    )


def _topology_summary(config: HybridRuntimeConfig) -> dict[str, object]:
    return {
        "local_only": list(HYBRID_SERVICE_BOUNDARY.local_only),
        "remote_capable": list(HYBRID_SERVICE_BOUNDARY.remote_capable),
        "shared_state": list(HYBRID_SERVICE_BOUNDARY.shared_state),
        "remote_endpoint_enabled": config.remote_endpoint.enabled,
    }


def build_startup_report(
    *,
    root_dir: Path = ROOT_DIR,
    environ: dict[str, str] | None = None,
) -> dict[str, object]:
    config = load_runtime_config(root_dir=root_dir, environ=environ)
    _ensure_runtime_layout(root_dir, config)
    checks: list[dict[str, str]] = []
    errors: list[str] = []
    _validate_mode(checks, errors)
    _validate_role_config(checks, errors, config=config)
    _validate_paths(checks, errors, root_dir=root_dir, config=config)
    _validate_config(checks, errors, root_dir=root_dir, config=config)
    _validate_write_boundaries(checks, errors, root_dir=root_dir, config=config)
    _validate_shared_context_contract(checks, errors, config=config)
    _validate_runtime_state(checks, errors, root_dir=root_dir, config=config)
    return {
        "system_status": "ready" if not errors else "error",
        "architecture_mode": "hybrid_dev",
        "startup_mode": config.role,
        "role": config.role,
        "mode": MODE,
        "bind": {"host": config.host, "port": config.port},
        "remote_endpoint": {
            "enabled": config.remote_endpoint.enabled,
            "base_url": config.remote_endpoint.base_url,
            "verify_tls": config.remote_endpoint.verify_tls,
            "request_timeout_seconds": config.remote_endpoint.request_timeout_seconds,
        },
        "storage_paths": config.as_dict()["storage_paths"],
        "topology": _topology_summary(config),
        "entry_url": _entry_url(root_dir),
        "checks": checks,
        "errors": errors,
    }


def run_startup_validation() -> dict[str, object]:
    return build_startup_report()


def _generate_system_id() -> str:
    seed = "|".join(
        (
            "/digital_foreman",
            "digital-foreman",
            platform.system().strip() or "linux",
        )
    )
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]


def _initialize_system_state(config: HybridRuntimeConfig) -> None:
    system_state_file = config.storage_paths.state_dir / "system_state.json"
    payload = {
        "initialized": True,
        "first_boot_completed": True,
        "system_id": _generate_system_id(),
        "role": config.role,
    }
    if system_state_file.exists():
        try:
            existing = json.loads(system_state_file.read_text(encoding="utf-8"))
        except Exception:
            existing = {}
        payload["system_id"] = str(existing.get("system_id", "")).strip() or payload["system_id"]
    system_state_file.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def boot_runtime(*, config: HybridRuntimeConfig) -> None:
    from runtime.telemetry import collect_runtime_metrics

    _initialize_system_state(config)
    metrics = collect_runtime_metrics()
    log_event(
        "system",
        {
            "system_status": "ready",
            "mode": MODE,
            "role": config.role,
            "entry_url": _entry_url(ROOT_DIR),
            "execution_compute_mode": str(metrics.get("execution_compute_mode", "")).strip(),
        },
    )


def main() -> int:
    config = load_runtime_config(root_dir=ROOT_DIR)
    report = build_startup_report(root_dir=ROOT_DIR)
    if report["system_status"] != "ready":
        log_event("validation", {"message": "startup validation failed", "errors": report["errors"]}, status="error")
        print(f"system_status: {report['system_status']}")
        print(f"role: {report['role']}")
        print(f"mode: {report['mode']}")
        print(f"entry_url: {report['entry_url']}")
        return 1
    boot_runtime(config=config)
    print(f"system_status: {report['system_status']}")
    print(f"role: {report['role']}")
    print(f"mode: {report['mode']}")
    print(f"entry_url: {report['entry_url']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
