from __future__ import annotations

import hashlib
import json
from pathlib import Path

from app.execution.product_box_build import generate_product_box_build


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
            "data",
            "runtime/logs",
            "runtime/state",
        ],
        "required_runtime_paths": [
            "runtime",
            "data",
            "runtime/logs",
            "runtime/state",
        ],
        "optional_paths": [
            "runtime/out",
        ],
    }


def _build_source_root(tmp_path: Path) -> tuple[Path, Path]:
    _write_file(tmp_path / "app" / "__init__.py", "")
    _write_file(
        tmp_path / "app" / "server.py", "from app.voice.app import run_voice_app\n"
    )
    _write_file(tmp_path / "app" / "voice" / "__init__.py", "")
    _write_file(
        tmp_path / "app" / "voice" / "app.py",
        "from app.product.runner import run_product\n",
    )
    _write_file(tmp_path / "app" / "product" / "__init__.py", "")
    _write_file(
        tmp_path / "app" / "product" / "runner.py",
        "def run_product():\n    return 'ok'\n",
    )
    _write_file(tmp_path / "runtime" / "state" / "stale.json", '{"keep": false}\n')
    _write_file(tmp_path / "runtime" / "logs" / "app.log", "stale log\n")
    _write_file(tmp_path / "runtime" / "out" / "temp.txt", "ignore me\n")
    _write_file(tmp_path / "data" / "seed.json", '{"ok": true}\n')
    _write_file(tmp_path / "scripts" / "dev_only.py", "print('dev')\n")
    _write_file(tmp_path / "tests" / "test_only.py", "assert True\n")
    _write_file(tmp_path / "devlog" / "note.md", "dev note\n")
    _write_file(tmp_path / "app" / "__pycache__" / "cache.pyc", "cached\n")

    manifest_path = tmp_path / "app" / "config" / "product_box_manifest.json"
    _write_file(manifest_path, json.dumps(_base_manifest(), indent=2))
    return tmp_path, manifest_path


def _tree_digest(root_dir: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(item for item in root_dir.rglob("*") if item.is_file()):
        relative = path.relative_to(root_dir).as_posix()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def test_valid_build_generation_passes(tmp_path: Path) -> None:
    root_dir, manifest_path = _build_source_root(tmp_path / "src")
    output_dir = tmp_path / "build" / "product_box"

    report = generate_product_box_build(
        manifest_path=manifest_path,
        root_dir=root_dir,
        output_dir=output_dir,
    )

    assert report["build_status"] == "PASS"
    assert report["missing_required_items"] == []
    assert "app/server.py" in report["included_files"]
    assert "config/product_box_manifest.json" in report["included_files"]
    assert (output_dir / "runtime").is_dir()
    assert (output_dir / "runtime" / "logs").is_dir()
    assert (output_dir / "runtime" / "state").is_dir()


def test_blocked_module_inclusion_fails(tmp_path: Path) -> None:
    root_dir, manifest_path = _build_source_root(tmp_path / "src")
    _write_file(
        root_dir / "app" / "voice" / "app.py", "import app.orchestrator.orchestrator\n"
    )

    report = generate_product_box_build(
        manifest_path=manifest_path,
        root_dir=root_dir,
        output_dir=tmp_path / "build" / "product_box",
    )

    assert report["build_status"] == "FAIL"
    assert any("blocked import" in item for item in report["missing_required_items"])


def test_missing_required_module_fails(tmp_path: Path) -> None:
    root_dir, manifest_path = _build_source_root(tmp_path / "src")
    (root_dir / "app" / "product" / "runner.py").unlink()

    report = generate_product_box_build(
        manifest_path=manifest_path,
        root_dir=root_dir,
        output_dir=tmp_path / "build" / "product_box",
    )

    assert report["build_status"] == "FAIL"
    assert any(
        "app.product.runner" in item for item in report["missing_required_items"]
    )


def test_forbidden_folder_and_artifact_are_excluded(tmp_path: Path) -> None:
    root_dir, manifest_path = _build_source_root(tmp_path / "src")
    output_dir = tmp_path / "build" / "product_box"

    report = generate_product_box_build(
        manifest_path=manifest_path,
        root_dir=root_dir,
        output_dir=output_dir,
    )

    assert report["build_status"] == "PASS"
    assert not (output_dir / "scripts").exists()
    assert not (output_dir / "tests").exists()
    assert not (output_dir / "devlog").exists()
    assert not (output_dir / "app" / "__pycache__").exists()
    assert "scripts/dev_only.py" in report["excluded_files"]


def test_writable_runtime_path_set_matches_manifest(tmp_path: Path) -> None:
    root_dir, manifest_path = _build_source_root(tmp_path / "src")
    output_dir = tmp_path / "build" / "product_box"

    report = generate_product_box_build(
        manifest_path=manifest_path,
        root_dir=root_dir,
        output_dir=output_dir,
    )

    staged_dirs = {
        path.relative_to(output_dir).as_posix()
        for path in output_dir.rglob("*")
        if path.is_dir()
        and path.relative_to(output_dir).as_posix()
        in {"data", "runtime/logs", "runtime/state"}
    }
    assert report["build_status"] == "PASS"
    assert staged_dirs == {"data", "runtime/logs", "runtime/state"}


def test_build_output_is_deterministic(tmp_path: Path) -> None:
    root_dir, manifest_path = _build_source_root(tmp_path / "src")
    first_output = tmp_path / "build1" / "product_box"
    second_output = tmp_path / "build2" / "product_box"

    first_report = generate_product_box_build(
        manifest_path=manifest_path,
        root_dir=root_dir,
        output_dir=first_output,
    )
    second_report = generate_product_box_build(
        manifest_path=manifest_path,
        root_dir=root_dir,
        output_dir=second_output,
    )

    assert first_report["build_status"] == "PASS"
    assert second_report["build_status"] == "PASS"
    assert first_report["included_files"] == second_report["included_files"]
    assert _tree_digest(first_output) == _tree_digest(second_output)
