from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

from app.execution.paths import DATA_DIR, LOGS_DIR, STATE_DIR


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_PRODUCT_BOX_MANIFEST_PATH = (
    PROJECT_ROOT / "config" / "product_box_manifest.json"
)


class ProductBoxManifestError(ValueError):
    pass


def default_product_box_manifest_path(root_dir: Path | None = None) -> Path:
    resolved_root = (root_dir or PROJECT_ROOT).resolve(strict=False)
    return resolved_root / "config" / "product_box_manifest.json"


def _normalize_module_pattern(value: object) -> str:
    normalized = str(value or "").strip().replace("\\", "/")
    if normalized.endswith("/*"):
        return normalized[:-2].replace("/", ".").strip(".")
    return normalized.replace("/", ".").strip(".")


def _normalize_relative_path(value: object) -> str:
    return Path(str(value or "").strip()).as_posix().strip("/")


APPROVED_WRITABLE_PATHS = tuple(
    dict.fromkeys(
        _normalize_relative_path(path)
        for path in (
            DATA_DIR.relative_to(PROJECT_ROOT),
            LOGS_DIR,
            STATE_DIR,
        )
    )
)


def _read_manifest(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ProductBoxManifestError(
            f"product box manifest is missing: {path}"
        ) from exc
    except json.JSONDecodeError as exc:
        raise ProductBoxManifestError(
            f"product box manifest is invalid JSON: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise ProductBoxManifestError("product box manifest root must be an object")
    return payload


def _require_string_list(manifest: dict[str, Any], key: str) -> list[str]:
    value = manifest.get(key)
    if not isinstance(value, list):
        raise ProductBoxManifestError(f"manifest field `{key}` must be a list")
    normalized: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if not text:
            raise ProductBoxManifestError(
                f"manifest field `{key}` contains an empty entry"
            )
        normalized.append(text)
    if len(normalized) != len(set(normalized)):
        raise ProductBoxManifestError(
            f"manifest field `{key}` contains duplicate entries"
        )
    return normalized


def _path_exists(root_dir: Path, entry: str) -> bool:
    return (root_dir / _normalize_relative_path(entry)).exists()


def _module_exists(root_dir: Path, entry: str) -> bool:
    normalized = str(entry or "").strip()
    if normalized.endswith("/*"):
        return (root_dir / normalized[:-2]).exists()

    module_path = Path(*_normalize_module_pattern(normalized).split("."))
    module_file = root_dir / f"{module_path.as_posix()}.py"
    package_init = root_dir / module_path / "__init__.py"
    return module_file.exists() or package_init.exists()


def _matches_prefix(module_name: str, prefix: str) -> bool:
    return module_name == prefix or module_name.startswith(prefix + ".")


def _validate_module_entries(
    root_dir: Path, entries: list[str], *, field_name: str
) -> None:
    for entry in entries:
        if not _module_exists(root_dir, entry):
            raise ProductBoxManifestError(
                f"manifest field `{field_name}` references missing module or scope `{entry}`"
            )


def _validate_path_entries(
    root_dir: Path, entries: list[str], *, field_name: str
) -> list[str]:
    normalized_entries = [_normalize_relative_path(entry) for entry in entries]
    if len(normalized_entries) != len(set(normalized_entries)):
        raise ProductBoxManifestError(
            f"manifest field `{field_name}` contains duplicate entries"
        )
    for entry in normalized_entries:
        if not entry:
            raise ProductBoxManifestError(
                f"manifest field `{field_name}` contains an empty path"
            )
        if not _path_exists(root_dir, entry):
            raise ProductBoxManifestError(
                f"manifest field `{field_name}` references missing path `{entry}`"
            )
    return normalized_entries


def _normalize_path_entries(entries: list[str], *, field_name: str) -> list[str]:
    normalized_entries = [_normalize_relative_path(entry) for entry in entries]
    if len(normalized_entries) != len(set(normalized_entries)):
        raise ProductBoxManifestError(
            f"manifest field `{field_name}` contains duplicate entries"
        )
    for entry in normalized_entries:
        if not entry:
            raise ProductBoxManifestError(
                f"manifest field `{field_name}` contains an empty path"
            )
    return normalized_entries


def validate_product_box_manifest(
    manifest: dict[str, Any] | None = None,
    *,
    manifest_path: Path | None = None,
    root_dir: Path | None = None,
) -> dict[str, Any]:
    resolved_root = (root_dir or PROJECT_ROOT).resolve(strict=False)
    resolved_manifest_path = (
        manifest_path or default_product_box_manifest_path(resolved_root)
    ).resolve(strict=False)
    payload = dict(manifest or _read_manifest(resolved_manifest_path))

    if int(payload.get("manifest_version", 0) or 0) != 1:
        raise ProductBoxManifestError("manifest_version must be 1")
    if str(payload.get("product_only_rule", "")).strip() != "deny_by_default":
        raise ProductBoxManifestError("product_only_rule must be `deny_by_default`")

    product_entrypoints = _require_string_list(payload, "product_entrypoints")
    allowlist = _require_string_list(payload, "product_runtime_allowlist")
    blocked_modules = _require_string_list(payload, "blocked_modules")
    allowed_actions = _require_string_list(payload, "allowed_actions")
    blocked_actions = _require_string_list(payload, "blocked_actions")
    writable_paths = _validate_path_entries(
        resolved_root,
        _require_string_list(payload, "writable_paths"),
        field_name="writable_paths",
    )
    required_runtime_paths = _validate_path_entries(
        resolved_root,
        _require_string_list(payload, "required_runtime_paths"),
        field_name="required_runtime_paths",
    )
    optional_paths = _normalize_path_entries(
        _require_string_list(payload, "optional_paths"),
        field_name="optional_paths",
    )

    _validate_module_entries(
        resolved_root, product_entrypoints, field_name="product_entrypoints"
    )
    _validate_module_entries(
        resolved_root, allowlist, field_name="product_runtime_allowlist"
    )
    normalized_allowlist = [_normalize_module_pattern(entry) for entry in allowlist]
    normalized_blocked = [_normalize_module_pattern(entry) for entry in blocked_modules]
    normalized_entrypoints = [
        _normalize_module_pattern(entry) for entry in product_entrypoints
    ]

    for entrypoint in normalized_entrypoints:
        if not any(
            _matches_prefix(entrypoint, allowed) for allowed in normalized_allowlist
        ):
            raise ProductBoxManifestError(
                f"product entrypoint `{entrypoint}` must be covered by product_runtime_allowlist"
            )
        if any(_matches_prefix(entrypoint, blocked) for blocked in normalized_blocked):
            raise ProductBoxManifestError(
                f"product entrypoint `{entrypoint}` conflicts with blocked_modules"
            )

    for allowed in normalized_allowlist:
        if any(_matches_prefix(allowed, blocked) for blocked in normalized_blocked):
            raise ProductBoxManifestError(
                f"allowlisted module `{allowed}` conflicts with blocked_modules"
            )

    if set(action.upper() for action in allowed_actions) & set(
        action.upper() for action in blocked_actions
    ):
        raise ProductBoxManifestError(
            "allowed_actions and blocked_actions must not overlap"
        )

    approved_writable_paths = {
        _normalize_relative_path(entry) for entry in APPROVED_WRITABLE_PATHS
    }
    normalized_writable_paths = {
        _normalize_relative_path(entry) for entry in writable_paths
    }
    if normalized_writable_paths != approved_writable_paths:
        raise ProductBoxManifestError(
            "writable_paths must be limited to the approved runtime storage paths"
        )
    if not normalized_writable_paths.issubset(set(required_runtime_paths)):
        raise ProductBoxManifestError(
            "writable_paths must also appear in required_runtime_paths"
        )

    path_sets = {
        "required_runtime_paths": set(required_runtime_paths),
        "optional_paths": set(optional_paths),
    }
    overlap = path_sets["required_runtime_paths"] & path_sets["optional_paths"]
    if overlap:
        conflicted = ", ".join(sorted(overlap))
        raise ProductBoxManifestError(
            f"required_runtime_paths and optional_paths must not overlap: {conflicted}"
        )

    payload["product_entrypoints"] = product_entrypoints
    payload["product_runtime_allowlist"] = allowlist
    payload["blocked_modules"] = blocked_modules
    payload["allowed_actions"] = [action.upper() for action in allowed_actions]
    payload["blocked_actions"] = [action.upper() for action in blocked_actions]
    payload["writable_paths"] = writable_paths
    payload["required_runtime_paths"] = required_runtime_paths
    payload["optional_paths"] = optional_paths
    return payload


@lru_cache(maxsize=1)
def load_product_box_manifest() -> dict[str, Any]:
    return validate_product_box_manifest()
