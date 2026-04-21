from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.execution.product_box_manifest import (
    DEFAULT_PRODUCT_BOX_MANIFEST_PATH,
    ProductBoxManifestError,
    validate_product_box_manifest,
)


def _load_manifest() -> dict[str, object]:
    return json.loads(DEFAULT_PRODUCT_BOX_MANIFEST_PATH.read_text(encoding="utf-8"))


def _write_manifest(tmp_path: Path, manifest: dict[str, object]) -> Path:
    manifest_path = tmp_path / "product_box_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest_path


def test_valid_manifest_passes() -> None:
    manifest = _load_manifest()
    validated = validate_product_box_manifest(manifest)

    assert validated["product_only_rule"] == "deny_by_default"
    assert "app.server" in validated["product_entrypoints"]
    assert "runtime/state" in validated["writable_paths"]


def test_missing_required_entry_fails(tmp_path: Path) -> None:
    manifest = _load_manifest()
    manifest["required_runtime_paths"] = [
        path
        for path in list(manifest["required_runtime_paths"])
        if path != "runtime/logs"
    ]
    manifest_path = _write_manifest(tmp_path, manifest)

    with pytest.raises(
        ProductBoxManifestError,
        match="writable_paths must also appear in required_runtime_paths",
    ):
        validate_product_box_manifest(manifest_path=manifest_path)


def test_forbidden_module_in_allowlist_fails(tmp_path: Path) -> None:
    manifest = _load_manifest()
    manifest["product_runtime_allowlist"] = list(
        manifest["product_runtime_allowlist"]
    ) + ["app.execution.execution_replay"]
    manifest_path = _write_manifest(tmp_path, manifest)

    with pytest.raises(ProductBoxManifestError, match="conflicts with blocked_modules"):
        validate_product_box_manifest(manifest_path=manifest_path)


def test_forbidden_writable_path_fails(tmp_path: Path) -> None:
    manifest = _load_manifest()
    manifest["writable_paths"] = list(manifest["writable_paths"]) + ["runtime/out"]
    manifest_path = _write_manifest(tmp_path, manifest)

    with pytest.raises(ProductBoxManifestError, match="writable_paths must be limited"):
        validate_product_box_manifest(manifest_path=manifest_path)


def test_duplicated_or_conflicting_entries_fail(tmp_path: Path) -> None:
    manifest = _load_manifest()
    manifest["optional_paths"] = list(manifest["optional_paths"]) + ["runtime/out"]
    manifest_path = _write_manifest(tmp_path, manifest)

    with pytest.raises(
        ProductBoxManifestError, match="optional_paths` contains duplicate entries"
    ):
        validate_product_box_manifest(manifest_path=manifest_path)
