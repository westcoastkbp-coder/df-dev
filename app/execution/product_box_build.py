from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any

from app.execution.product_box_manifest import (
    APPROVED_WRITABLE_PATHS,
    DEFAULT_PRODUCT_BOX_MANIFEST_PATH,
    PROJECT_ROOT,
    _matches_prefix,
    _normalize_module_pattern,
    _normalize_relative_path,
    validate_product_box_manifest,
)
from app.execution.product_packaging import (
    _collect_modules_from_entrypoints,
    _module_to_path,
    validate_product_packaging,
)


DEFAULT_BUILD_DIR = PROJECT_ROOT / "build" / "product_box"
DEFAULT_SYSTEM_CONTEXT_PATH = PROJECT_ROOT / "config" / "system_context.yaml"
FORBIDDEN_BUILD_FOLDERS = ("tests", "scripts", "agents", "devlog", "venv", "build")
FORBIDDEN_BUILD_ARTIFACT_PARTS = ("__pycache__", ".pytest_cache")
FORBIDDEN_BUILD_ARTIFACT_PREFIXES = ("pytest-cache-files-", "tmp", "temp")


class ProductBoxBuildError(ValueError):
    pass


def _module_parent_init_files(root_dir: Path, module_name: str) -> list[Path]:
    module_path = Path(*module_name.split("."))
    parents: list[Path] = []
    for index in range(1, len(module_path.parts)):
        init_path = root_dir / Path(*module_path.parts[:index]) / "__init__.py"
        if init_path.exists():
            parents.append(init_path)
    return parents


def _should_exclude_path(relative_path: str, *, blocked_patterns: list[str]) -> bool:
    normalized = relative_path.replace("\\", "/").strip("/")
    parts = normalized.split("/") if normalized else []
    if any(part in FORBIDDEN_BUILD_ARTIFACT_PARTS for part in parts):
        return True
    if any(
        part.startswith(prefix)
        for part in parts
        for prefix in FORBIDDEN_BUILD_ARTIFACT_PREFIXES
    ):
        return True
    if parts and parts[0] in FORBIDDEN_BUILD_FOLDERS:
        return True
    module_name = normalized.removesuffix(".py").replace("/", ".")
    if module_name.endswith(".__init__"):
        module_name = module_name[: -len(".__init__")]
    if module_name and any(
        _matches_prefix(module_name, blocked) for blocked in blocked_patterns
    ):
        return True
    return False


def _copy_file(source_root: Path, destination_root: Path, relative_path: Path) -> None:
    source_path = source_root / relative_path
    destination_path = destination_root / relative_path
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, destination_path)


def _iter_project_files(root_dir: Path) -> list[Path]:
    collected: list[Path] = []

    def _ignore_walk_error(_: OSError) -> None:
        return

    for current_root, _, file_names in os.walk(
        root_dir, topdown=True, onerror=_ignore_walk_error, followlinks=False
    ):
        current_root_path = Path(current_root)
        for file_name in file_names:
            candidate = current_root_path / file_name
            try:
                collected.append(candidate.relative_to(root_dir))
            except (OSError, ValueError):
                continue

    return sorted(set(collected))


def _build_selected_files(
    *,
    root_dir: Path,
    manifest: dict[str, Any],
) -> tuple[list[Path], list[str], list[str], list[str]]:
    allowlist = [
        _normalize_module_pattern(entry)
        for entry in manifest["product_runtime_allowlist"]
    ]
    blocked = [
        _normalize_module_pattern(entry) for entry in manifest["blocked_modules"]
    ]
    entrypoints = [
        _normalize_module_pattern(entry) for entry in manifest["product_entrypoints"]
    ]

    included_modules, violations, blocked_references = (
        _collect_modules_from_entrypoints(
            root_dir=root_dir,
            entrypoints=entrypoints,
            allowlist=allowlist,
            blocked=blocked,
        )
    )
    if violations:
        return [], violations, sorted(included_modules), blocked_references

    selected_files: set[Path] = set()
    missing_required_items: list[str] = []
    for module_name in sorted(included_modules):
        module_path = _module_to_path(root_dir, module_name)
        if module_path is None:
            missing_required_items.append(module_name)
            continue
        selected_files.add(module_path.relative_to(root_dir))
        for init_path in _module_parent_init_files(root_dir, module_name):
            selected_files.add(init_path.relative_to(root_dir))

    if missing_required_items:
        return (
            [],
            [f"missing module file for {item}" for item in missing_required_items],
            sorted(included_modules),
            blocked_references,
        )

    return (
        sorted(selected_files),
        [],
        sorted(included_modules),
        sorted(blocked_references),
    )


def generate_product_box_build(
    manifest: dict[str, Any] | None = None,
    *,
    manifest_path: Path | None = None,
    root_dir: Path | None = None,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    resolved_root = (root_dir or PROJECT_ROOT).resolve(strict=False)
    resolved_manifest_path = (
        manifest_path or DEFAULT_PRODUCT_BOX_MANIFEST_PATH
    ).resolve(strict=False)
    resolved_output_dir = (output_dir or DEFAULT_BUILD_DIR).resolve(strict=False)

    try:
        validated_manifest = validate_product_box_manifest(
            manifest,
            manifest_path=resolved_manifest_path,
            root_dir=resolved_root,
        )
    except Exception as exc:
        return {
            "build_status": "FAIL",
            "total_files": 0,
            "included_files": [],
            "excluded_files": [],
            "missing_required_items": [str(exc)],
            "blocked_references_detected": [],
            "total_size_estimate": 0,
            "output_dir": str(resolved_output_dir),
        }

    selected_files, dependency_violations, included_modules, blocked_references = (
        _build_selected_files(
            root_dir=resolved_root,
            manifest=validated_manifest,
        )
    )
    if dependency_violations:
        return {
            "build_status": "FAIL",
            "total_files": 0,
            "included_files": [],
            "excluded_files": [],
            "missing_required_items": dependency_violations,
            "blocked_references_detected": blocked_references,
            "total_size_estimate": 0,
            "output_dir": str(resolved_output_dir),
            "modules_included": included_modules,
        }

    blocked_patterns = [
        _normalize_module_pattern(entry)
        for entry in validated_manifest["blocked_modules"]
    ]
    excluded_files: list[str] = []
    for relative in _iter_project_files(resolved_root):
        relative_text = relative.as_posix()
        if relative in selected_files:
            continue
        if _should_exclude_path(relative_text, blocked_patterns=blocked_patterns):
            excluded_files.append(relative_text)

    if resolved_output_dir.exists():
        shutil.rmtree(resolved_output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    included_files: list[str] = []
    total_size_estimate = 0
    for relative_path in selected_files:
        _copy_file(resolved_root, resolved_output_dir, relative_path)
        relative_text = relative_path.as_posix()
        included_files.append(relative_text)
        total_size_estimate += (resolved_root / relative_path).stat().st_size

    required_paths = sorted(
        {
            _normalize_relative_path(path)
            for path in validated_manifest["required_runtime_paths"]
        }
    )
    writable_paths = sorted(
        {
            _normalize_relative_path(path)
            for path in validated_manifest["writable_paths"]
        }
    )
    if set(writable_paths) != {
        _normalize_relative_path(path) for path in APPROVED_WRITABLE_PATHS
    }:
        return {
            "build_status": "FAIL",
            "total_files": 0,
            "included_files": included_files,
            "excluded_files": excluded_files,
            "missing_required_items": ["required runtime path policy is violated"],
            "blocked_references_detected": blocked_references,
            "total_size_estimate": total_size_estimate,
            "output_dir": str(resolved_output_dir),
            "modules_included": included_modules,
        }

    for relative_text in required_paths:
        (resolved_output_dir / relative_text).mkdir(parents=True, exist_ok=True)

    config_dir = resolved_output_dir / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    staged_manifest_path = config_dir / "product_box_manifest.json"
    staged_manifest_path.write_text(
        json.dumps(validated_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if "config/product_box_manifest.json" not in included_files:
        included_files.append("config/product_box_manifest.json")
        total_size_estimate += staged_manifest_path.stat().st_size
    if DEFAULT_SYSTEM_CONTEXT_PATH.exists():
        staged_system_context_path = config_dir / "system_context.yaml"
        shutil.copy2(DEFAULT_SYSTEM_CONTEXT_PATH, staged_system_context_path)
        if "config/system_context.yaml" not in included_files:
            included_files.append("config/system_context.yaml")
            total_size_estimate += staged_system_context_path.stat().st_size
    packaging_report = validate_product_packaging(
        manifest=validated_manifest,
        manifest_path=staged_manifest_path,
        root_dir=resolved_output_dir,
    )
    if packaging_report["packaging_status"] != "PASS":
        return {
            "build_status": "FAIL",
            "total_files": len(included_files),
            "included_files": sorted(included_files),
            "excluded_files": sorted(set(excluded_files)),
            "missing_required_items": list(packaging_report["violations"]),
            "blocked_references_detected": sorted(
                set(
                    blocked_references
                    + list(packaging_report["blocked_references_detected"])
                )
            ),
            "total_size_estimate": total_size_estimate,
            "output_dir": str(resolved_output_dir),
            "modules_included": included_modules,
        }

    return {
        "build_status": "PASS",
        "total_files": len(included_files),
        "included_files": sorted(included_files),
        "excluded_files": sorted(set(excluded_files)),
        "missing_required_items": [],
        "blocked_references_detected": sorted(blocked_references),
        "total_size_estimate": total_size_estimate,
        "output_dir": str(resolved_output_dir),
        "modules_included": included_modules,
    }
