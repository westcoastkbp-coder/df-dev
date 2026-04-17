from __future__ import annotations

import json
from pathlib import Path

from app.execution.product_packaging import validate_product_packaging


def _write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _base_manifest() -> dict[str, object]:
    return {
        "manifest_version": 1,
        "product_only_rule": "deny_by_default",
        "product_entrypoints": [
            "app.server",
            "app.voice.app",
        ],
        "product_runtime_allowlist": [
            "app.server",
            "app.voice",
            "app.product.runner",
        ],
        "blocked_modules": [
            "agents/*",
            "scripts/*",
            "tests/*",
            "app.execution.execution_policy",
            "app.execution.execution_replay",
            "app.execution.linear_workflow",
            "app.execution.policy_guard",
            "app.orchestrator.dev_orchestrator",
            "app.orchestrator.dev_task",
            "app.orchestrator.mock_executor",
            "app.orchestrator.orchestrator",
        ],
        "allowed_actions": [
            "READ_FILE",
            "RESOURCES",
            "SYSTEM_STATUS",
            "WRITE_FILE",
        ],
        "blocked_actions": [
            "BUILD_WEBSITE",
            "CONTROL_HEALTH",
            "RUN_TESTS",
        ],
        "writable_paths": [
            "runtime/state",
            "runtime/logs",
            "data",
        ],
        "required_runtime_paths": [
            "runtime",
            "runtime/state",
            "runtime/logs",
            "data",
        ],
        "optional_paths": [
            "runtime/out",
        ],
    }


def _build_clean_package_root(tmp_path: Path) -> tuple[Path, Path]:
    _write_file(tmp_path / "app" / "__init__.py", "")
    _write_file(tmp_path / "app" / "server.py", "from app.voice.app import run_voice_app\n")
    _write_file(tmp_path / "app" / "voice" / "__init__.py", "")
    _write_file(tmp_path / "app" / "voice" / "app.py", "from app.product.runner import run_product\n")
    _write_file(tmp_path / "app" / "product" / "__init__.py", "")
    _write_file(tmp_path / "app" / "product" / "runner.py", "def run_product():\n    return 'ok'\n")
    _write_file(tmp_path / "runtime" / "state" / ".gitkeep", "")
    _write_file(tmp_path / "runtime" / "logs" / ".gitkeep", "")
    _write_file(tmp_path / "runtime" / "out" / ".gitkeep", "")
    _write_file(tmp_path / "data" / ".gitkeep", "")

    manifest_path = tmp_path / "app" / "config" / "product_box_manifest.json"
    _write_file(manifest_path, json.dumps(_base_manifest(), indent=2))
    return tmp_path, manifest_path


def test_valid_packaging_passes(tmp_path: Path) -> None:
    root_dir, manifest_path = _build_clean_package_root(tmp_path)

    report = validate_product_packaging(manifest_path=manifest_path, root_dir=root_dir)

    assert report["packaging_status"] == "PASS"
    assert report["violations"] == []
    assert "app.server" in report["modules_included"]
    assert "app.voice.app" in report["modules_included"]


def test_blocked_import_fails(tmp_path: Path) -> None:
    root_dir, manifest_path = _build_clean_package_root(tmp_path)
    _write_file(
        root_dir / "app" / "voice" / "app.py",
        "import app.orchestrator.orchestrator\n",
    )

    report = validate_product_packaging(manifest_path=manifest_path, root_dir=root_dir)

    assert report["packaging_status"] == "FAIL"
    assert any("blocked import" in item for item in report["violations"])
    assert "app.orchestrator.orchestrator" in report["blocked_references_detected"]


def test_forbidden_folder_fails(tmp_path: Path) -> None:
    root_dir, manifest_path = _build_clean_package_root(tmp_path)
    _write_file(root_dir / "scripts" / "dev_tool.py", "print('dev')\n")

    report = validate_product_packaging(manifest_path=manifest_path, root_dir=root_dir)

    assert report["packaging_status"] == "FAIL"
    assert any("forbidden folder present: scripts/" == item for item in report["violations"])
    assert "scripts" in report["blocked_references_detected"]


def test_invalid_writable_path_fails(tmp_path: Path) -> None:
    root_dir, _ = _build_clean_package_root(tmp_path)
    manifest = _base_manifest()
    manifest["writable_paths"] = list(manifest["writable_paths"]) + ["runtime/out"]

    report = validate_product_packaging(manifest=manifest, root_dir=root_dir)

    assert report["packaging_status"] == "FAIL"
    assert any("invalid writable path(s): runtime/out" == item for item in report["violations"])
