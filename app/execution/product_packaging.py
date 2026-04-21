from __future__ import annotations

import ast
import json
from pathlib import Path
from typing import Any

from app.execution.product_box_manifest import (
    APPROVED_WRITABLE_PATHS,
    PROJECT_ROOT,
    ProductBoxManifestError,
    _matches_prefix,
    _normalize_module_pattern,
    _normalize_relative_path,
    validate_product_box_manifest,
)


FORBIDDEN_PACKAGE_FOLDERS = ("tests", "agents", "devlog", "scripts")
FORBIDDEN_ARTIFACT_NAMES = ("__pycache__", ".pytest_cache")
FORBIDDEN_ARTIFACT_PREFIXES = ("pytest-cache-files-", "tmp", "temp")


class ProductPackagingValidationError(ValueError):
    pass


def _module_to_path(root_dir: Path, module_name: str) -> Path | None:
    module_path = Path(*module_name.split("."))
    module_file = root_dir / f"{module_path.as_posix()}.py"
    package_init = root_dir / module_path / "__init__.py"
    if module_file.exists():
        return module_file
    if package_init.exists():
        return package_init
    return None


def _path_to_module(root_dir: Path, file_path: Path) -> str:
    relative = file_path.relative_to(root_dir)
    if relative.name == "__init__.py":
        parts = relative.parent.parts
    else:
        parts = relative.with_suffix("").parts
    return ".".join(parts)


def _resolve_relative_import(module_name: str, imported_module: str, level: int) -> str:
    if level == 0:
        return imported_module
    parts = module_name.split(".")
    if parts and parts[-1] == "__init__":
        parts = parts[:-1]
    if level > 0:
        package_parts = parts[:-1]
        if level > len(package_parts) + 1:
            return imported_module
        base_parts = package_parts[: len(package_parts) - (level - 1)]
    else:
        base_parts = parts[:-1]
    if imported_module:
        return ".".join([*base_parts, imported_module])
    return ".".join(base_parts)


def _iter_imported_modules(
    tree: ast.AST,
    module_name: str,
    *,
    root_dir: Path,
) -> tuple[set[str], list[str]]:
    imports: set[str] = set()
    dynamic_imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imported = str(alias.name or "").strip()
                if imported:
                    imports.add(imported)
        elif isinstance(node, ast.ImportFrom):
            imported_module = _resolve_relative_import(
                module_name,
                str(node.module or "").strip(),
                int(node.level or 0),
            )
            if imported_module:
                imports.add(imported_module)
                for alias in node.names:
                    imported_name = str(alias.name or "").strip()
                    if not imported_name or imported_name == "*":
                        continue
                    candidate_module = f"{imported_module}.{imported_name}"
                    if _module_to_path(root_dir, candidate_module) is not None:
                        imports.add(candidate_module)
        elif isinstance(node, ast.Call):
            if isinstance(node.func, ast.Name) and node.func.id == "__import__":
                target = _literal_string_arg(node)
                if target:
                    dynamic_imports.append(target)
            elif (
                isinstance(node.func, ast.Attribute)
                and node.func.attr == "import_module"
            ):
                target = _literal_string_arg(node)
                if target:
                    dynamic_imports.append(target)
    return imports, dynamic_imports


def _literal_string_arg(node: ast.Call) -> str:
    if not node.args:
        return ""
    candidate = node.args[0]
    if isinstance(candidate, ast.Constant) and isinstance(candidate.value, str):
        return str(candidate.value).strip()
    return ""


def _normalize_blocked_patterns(entries: list[str]) -> list[str]:
    return [_normalize_module_pattern(entry) for entry in entries]


def _collect_modules_from_entrypoints(
    *,
    root_dir: Path,
    entrypoints: list[str],
    allowlist: list[str],
    blocked: list[str],
) -> tuple[set[str], list[str], list[str]]:
    included_modules: set[str] = set()
    violations: list[str] = []
    blocked_references: list[str] = []
    to_visit = list(entrypoints)
    visited: set[str] = set()

    while to_visit:
        module_name = to_visit.pop()
        if module_name in visited:
            continue
        visited.add(module_name)
        included_modules.add(module_name)

        if any(_matches_prefix(module_name, entry) for entry in blocked):
            message = f"blocked module included: {module_name}"
            violations.append(message)
            blocked_references.append(module_name)
            continue
        if not any(_matches_prefix(module_name, entry) for entry in allowlist):
            violations.append(f"module outside allowlist: {module_name}")
            continue

        module_path = _module_to_path(root_dir, module_name)
        if module_path is None:
            violations.append(f"missing module file for {module_name}")
            continue

        tree = ast.parse(
            module_path.read_text(encoding="utf-8-sig"), filename=str(module_path)
        )
        imported_modules, dynamic_imports = _iter_imported_modules(
            tree,
            module_name,
            root_dir=root_dir,
        )

        for dynamic_import in dynamic_imports:
            normalized_dynamic = _normalize_module_pattern(dynamic_import)
            if any(_matches_prefix(normalized_dynamic, entry) for entry in blocked):
                detail = (
                    f"forbidden dynamic import from {module_name}: {dynamic_import}"
                )
                violations.append(detail)
                blocked_references.append(dynamic_import)

        for imported_module in sorted(imported_modules):
            normalized_import = _normalize_module_pattern(imported_module)
            if not normalized_import:
                continue
            if normalized_import.split(".")[0] in {
                "typing",
                "__future__",
                "dataclasses",
                "collections",
                "pathlib",
                "json",
                "ast",
            }:
                pass
            if any(_matches_prefix(normalized_import, entry) for entry in blocked):
                detail = f"blocked import from {module_name}: {normalized_import}"
                violations.append(detail)
                blocked_references.append(normalized_import)
                continue
            if normalized_import.split(".")[0] in {
                "app",
                "runtime",
                "memory",
                "integrations",
                "hardware",
                "agents",
                "scripts",
                "tests",
            }:
                if not any(
                    _matches_prefix(normalized_import, entry) for entry in allowlist
                ):
                    violations.append(f"module outside allowlist: {normalized_import}")
                    continue
                if _module_to_path(root_dir, normalized_import) is not None:
                    to_visit.append(normalized_import)

    return included_modules, sorted(set(violations)), sorted(set(blocked_references))


def _validate_filesystem(root_dir: Path) -> tuple[list[str], list[str]]:
    violations: list[str] = []
    blocked_references: list[str] = []

    for folder_name in FORBIDDEN_PACKAGE_FOLDERS:
        folder_path = root_dir / folder_name
        if folder_path.exists():
            detail = f"forbidden folder present: {folder_name}/"
            violations.append(detail)
            blocked_references.append(folder_name)

    for path in root_dir.rglob("*"):
        name = path.name
        if not name:
            continue
        if name in FORBIDDEN_ARTIFACT_NAMES:
            violations.append(
                f"forbidden artifact present: {path.relative_to(root_dir).as_posix()}"
            )
        elif any(name.startswith(prefix) for prefix in FORBIDDEN_ARTIFACT_PREFIXES):
            violations.append(
                f"temporary artifact present: {path.relative_to(root_dir).as_posix()}"
            )

    return sorted(set(violations)), sorted(set(blocked_references))


def _validate_writable_paths(manifest: dict[str, Any]) -> list[str]:
    violations: list[str] = []
    approved = {_normalize_relative_path(path) for path in APPROVED_WRITABLE_PATHS}
    declared = {
        _normalize_relative_path(path)
        for path in list(manifest.get("writable_paths", []) or [])
    }
    invalid = sorted(declared - approved)
    if invalid:
        violations.append("invalid writable path(s): " + ", ".join(invalid))
    return violations


def _load_raw_manifest(
    manifest: dict[str, Any] | None,
    manifest_path: Path | None,
) -> dict[str, Any]:
    if manifest is not None:
        return dict(manifest)
    if manifest_path is None:
        return {}
    try:
        return json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def validate_product_packaging(
    manifest: dict[str, Any] | None = None,
    *,
    manifest_path: Path | None = None,
    root_dir: Path | None = None,
) -> dict[str, Any]:
    resolved_root = (root_dir or PROJECT_ROOT).resolve(strict=False)
    raw_manifest = _load_raw_manifest(manifest, manifest_path)
    raw_writable_path_violations = (
        _validate_writable_paths(raw_manifest) if raw_manifest else []
    )
    try:
        validated_manifest = validate_product_box_manifest(
            manifest,
            manifest_path=manifest_path,
            root_dir=resolved_root,
        )
    except ProductBoxManifestError as exc:
        return {
            "packaging_status": "FAIL",
            "violations": sorted(set(raw_writable_path_violations + [str(exc)])),
            "modules_included": [],
            "blocked_references_detected": [],
        }

    allowlist = [
        _normalize_module_pattern(entry)
        for entry in validated_manifest["product_runtime_allowlist"]
    ]
    blocked = _normalize_blocked_patterns(validated_manifest["blocked_modules"])
    entrypoints = [
        _normalize_module_pattern(entry)
        for entry in validated_manifest["product_entrypoints"]
    ]

    included_modules, import_violations, import_blocked = (
        _collect_modules_from_entrypoints(
            root_dir=resolved_root,
            entrypoints=entrypoints,
            allowlist=allowlist,
            blocked=blocked,
        )
    )
    filesystem_violations, filesystem_blocked = _validate_filesystem(resolved_root)
    writable_path_violations = _validate_writable_paths(validated_manifest)

    violations = sorted(
        set(import_violations + filesystem_violations + writable_path_violations)
    )
    blocked_references = sorted(set(import_blocked + filesystem_blocked))
    return {
        "packaging_status": "PASS" if not violations else "FAIL",
        "violations": violations,
        "modules_included": sorted(included_modules),
        "blocked_references_detected": blocked_references,
    }
