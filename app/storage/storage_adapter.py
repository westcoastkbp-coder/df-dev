from __future__ import annotations

import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Any

import httpx
import requests

from app.memory.memory_object import (
    effective_status,
    infer_memory_class,
    make_artifact_object,
    make_conflict_object,
    make_trace_object,
    memory_object_from_mapping,
)
from app.memory.memory_registry import (
    MemoryRegistryError,
    compute_artifact_key,
    get_artifact_by_logical_key,
    register_artifact,
)
from control.env_loader import load_env

REPO_ROOT = Path(__file__).resolve().parents[2]
POLICY_FILE = REPO_ROOT / "config" / "contour_policy.json"
STORAGE_BACKEND_FILE = REPO_ROOT / "config" / "storage_backend.json"
ALLOWED_DOMAINS = frozenset({"ownerbox", "dev"})
ALLOWED_BACKENDS = frozenset({"local", "opencloud"})
ARCHIVE_DIRNAME = "archive"
_SAFE_NAME_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")
_DEFAULT_STORAGE_BACKEND_CONFIG = {
    "backend": "local",
    "default": "local",
    "webdav_enabled": False,
    "webdav_url": "",
    "opencloud_enabled": True,
    "opencloud": {
        "base_url": "",
        "username_env": "OPENCLOUD_USERNAME",
        "app_token_env": "OPENCLOUD_APP_TOKEN",
        "ownerbox_remote_root": "ownerbox",
        "dev_remote_root": "exports/dev",
        "timeout_seconds": 15.0,
    },
}
_FALLBACK_ROOTS = {
    "ownerbox": Path("/home/avoro/ownerbox/artifacts"),
    "dev": Path("/home/avoro/df-dev/artifacts"),
}


class StorageAdapterError(RuntimeError):
    """Base exception for local storage adapter failures."""


class InvalidDomainError(StorageAdapterError):
    """Raised when a caller requests an unsupported domain."""


class BoundaryViolationError(StorageAdapterError):
    """Raised when a path crosses or escapes a domain boundary."""


class ArtifactNotFoundError(StorageAdapterError):
    """Raised when a requested artifact file does not exist."""


class OpenCloudError(StorageAdapterError):
    """Raised when an OpenCloud WebDAV operation fails."""


class OpenCloudConfigError(OpenCloudError):
    """Raised when the OpenCloud backend is not configured correctly."""


class OpenCloudNetworkError(OpenCloudError):
    """Raised when an OpenCloud request fails due to network conditions."""


class OpenCloudUnauthorizedError(OpenCloudError):
    """Raised when OpenCloud rejects authentication or authorization."""


class WebDAVError(StorageAdapterError):
    """Raised when a WebDAV operation fails."""


class WebDAVConfigError(WebDAVError):
    """Raised when the WebDAV backend is not configured correctly."""


class WebDAVNetworkError(WebDAVError):
    """Raised when a WebDAV request fails due to network conditions."""


class WebDAVUnauthorizedError(WebDAVError):
    """Raised when WebDAV rejects authentication or authorization."""


def _utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _normalize_domain(domain: str) -> str:
    normalized = str(domain or "").strip().lower()
    if normalized not in ALLOWED_DOMAINS:
        allowed = ", ".join(sorted(ALLOWED_DOMAINS))
        raise InvalidDomainError(
            f"Invalid domain '{domain}'. Allowed domains: {allowed}."
        )
    return normalized


def _normalize_name(value: str | None, *, field_name: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise StorageAdapterError(f"{field_name} must not be empty.")
    normalized = _SAFE_NAME_PATTERN.sub("_", raw).strip("._")
    if not normalized:
        raise StorageAdapterError(f"{field_name} must resolve to a safe filename.")
    return normalized


def _load_policy() -> dict[str, Any]:
    if not POLICY_FILE.exists():
        return {}
    try:
        return json.loads(POLICY_FILE.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise StorageAdapterError(
            f"Storage contour policy is not valid JSON: {POLICY_FILE}"
        ) from exc


def _deep_merge_dict(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in overrides.items():
        existing = merged.get(key)
        if isinstance(existing, dict) and isinstance(value, dict):
            merged[key] = _deep_merge_dict(existing, value)
            continue
        merged[key] = value
    return merged


def _load_env_value(name: str) -> str:
    value = os.getenv(name)
    if value:
        return value.strip()
    return str(load_env().get(name, "")).strip()


def get_storage_backend_config() -> dict[str, Any]:
    config = dict(_DEFAULT_STORAGE_BACKEND_CONFIG)
    if STORAGE_BACKEND_FILE.exists():
        try:
            loaded = json.loads(STORAGE_BACKEND_FILE.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise OpenCloudConfigError(
                f"Storage backend config is not valid JSON: {STORAGE_BACKEND_FILE}"
            ) from exc
        if not isinstance(loaded, dict):
            raise OpenCloudConfigError(
                f"Storage backend config must contain a JSON object: {STORAGE_BACKEND_FILE}"
            )
        config = _deep_merge_dict(config, loaded)

    backend = (
        str(config.get("backend") or "").strip().lower()
        or str(config.get("default") or "").strip().lower()
        or "local"
    )
    if backend not in ALLOWED_BACKENDS:
        allowed = ", ".join(sorted(ALLOWED_BACKENDS))
        raise OpenCloudConfigError(
            f"Invalid storage backend '{backend}'. Allowed backends: {allowed}."
        )
    config["backend"] = backend
    config["default"] = backend
    return config


def get_default_backend() -> str:
    return str(get_storage_backend_config()["backend"])


def _domain_root_from_policy(domain: str, policy: dict[str, Any]) -> Path:
    if domain == "ownerbox":
        contour = policy.get("contours", {}).get("ownerbox", {})
    else:
        contour = policy.get("contours", {}).get("df-dev", {})

    working_root = str(contour.get("working_root") or "").strip()
    if not working_root:
        raise StorageAdapterError(
            f"Storage contour policy does not define a working root for '{domain}'."
        )
    return Path(working_root) / "artifacts"


def _domain_root(domain: str) -> Path:
    normalized_domain = _normalize_domain(domain)
    policy = _load_policy()
    if policy:
        return _domain_root_from_policy(normalized_domain, policy)
    return _FALLBACK_ROOTS[normalized_domain]


def _domain_remote_root(domain: str, config: dict[str, Any]) -> str:
    opencloud = dict(config.get("opencloud") or {})
    key = "ownerbox_remote_root" if domain == "ownerbox" else "dev_remote_root"
    remote_root = str(opencloud.get(key) or "").strip().strip("/")
    if not remote_root:
        raise OpenCloudConfigError(
            f"Storage backend config does not define an OpenCloud remote root for '{domain}'."
        )
    return remote_root


def _ensure_domain_path(domain: str, path: Path | str) -> Path:
    domain_root = _domain_root(domain).resolve(strict=False)
    return _ensure_path_with_root(domain_root, path, domain=domain)


def _ensure_path_with_root(
    domain_root: Path,
    path: Path | str,
    *,
    domain: str,
) -> Path:
    domain_root = domain_root.resolve(strict=False)
    candidate = Path(path)
    if candidate.is_absolute():
        resolved_candidate = candidate.resolve(strict=False)
    else:
        resolved_candidate = (domain_root / candidate).resolve(strict=False)

    try:
        resolved_candidate.relative_to(domain_root)
    except ValueError as exc:
        raise BoundaryViolationError(
            f"Path '{resolved_candidate}' is outside the allowed namespace for domain '{domain}'."
        ) from exc
    return resolved_candidate


def _normalize_webdav_remote_path(remote_path: str) -> str:
    raw = str(remote_path or "").strip().replace("\\", "/")
    if not raw:
        raise WebDAVError("remote_path must not be empty.")
    if raw.startswith("/") or "://" in raw:
        raise WebDAVError(f"Invalid WebDAV remote path: {remote_path}")
    parts = [part for part in raw.split("/") if part not in ("", ".")]
    if not parts or any(part == ".." for part in parts):
        raise WebDAVError(f"Invalid WebDAV remote path: {remote_path}")
    return "/".join(parts)


def _webdav_remote_path_for_domain(domain: str, local_path: Path) -> str:
    normalized_domain = _normalize_domain(domain)
    domain_root = _domain_root(normalized_domain)
    relative_path = _ensure_domain_path(normalized_domain, local_path).relative_to(
        domain_root
    )
    remote_root = "DF/owner" if normalized_domain == "ownerbox" else "DF/dev"
    return _normalize_webdav_remote_path(f"{remote_root}/{relative_path.as_posix()}")


def _normalize_remote_path(
    domain: str, remote_path: str, config: dict[str, Any]
) -> str:
    raw = str(remote_path or "").strip().replace("\\", "/")
    if not raw:
        raise OpenCloudError("remote_path must not be empty.")
    if raw.startswith("/") or "://" in raw:
        raise OpenCloudError(f"Invalid OpenCloud remote path: {remote_path}")

    parts = [part for part in raw.split("/") if part not in ("", ".")]
    if not parts or any(part == ".." for part in parts):
        raise OpenCloudError(f"Invalid OpenCloud remote path: {remote_path}")

    normalized = "/".join(parts)
    remote_root = _domain_remote_root(domain, config)
    if normalized == remote_root or normalized.startswith(f"{remote_root}/"):
        return normalized
    return f"{remote_root}/{normalized}"


def _relative_local_path_for_remote(
    domain: str, remote_path: str, config: dict[str, Any]
) -> Path:
    normalized_remote = _normalize_remote_path(domain, remote_path, config)
    remote_root = _domain_remote_root(domain, config)
    relative = normalized_remote[len(remote_root) :].lstrip("/")
    if not relative:
        raise OpenCloudError(f"Invalid OpenCloud remote path: {remote_path}")
    return Path(relative)


def _artifact_id_for_payload(
    domain: str,
    artifact_type: str,
    payload: Any,
) -> str:
    if isinstance(payload, dict):
        for candidate_field in ("id", "logical_id"):
            candidate = str(payload.get(candidate_field) or "").strip()
            if candidate:
                return _normalize_name(candidate, field_name="artifact_id")

    canonical_payload = json.dumps(payload, sort_keys=True, default=str)
    digest = hashlib.sha256(
        f"{domain}:{artifact_type}:{canonical_payload}".encode("utf-8")
    ).hexdigest()
    return digest[:12]


def _logical_id_for_payload(
    domain: str,
    artifact_type: str,
    payload: Any,
) -> str:
    if isinstance(payload, dict):
        for candidate_field in ("logical_id", "id"):
            candidate = str(payload.get(candidate_field) or "").strip()
            if candidate:
                return _normalize_name(candidate, field_name="logical_id")
    return _artifact_id_for_payload(domain, artifact_type, payload)


def _string_list_from_payload(payload: Any, field_name: str) -> list[str]:
    if not isinstance(payload, dict):
        return []

    raw_value = payload.get(field_name)
    if isinstance(raw_value, str):
        candidates = [raw_value]
    elif isinstance(raw_value, (list, tuple, set, frozenset)):
        candidates = list(raw_value)
    else:
        return []

    normalized_values: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = str(candidate or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        normalized_values.append(normalized)
    return normalized_values


def _memory_object_for_artifact(
    *,
    artifact_id: str,
    domain: str,
    artifact_type: str,
    payload: Any,
    artifact_path: Path,
    remote_path: str | None,
    logical_key: str,
    artifact_status: str | None,
    resolution: dict[str, Any] | None,
    existing_artifact: dict[str, Any] | None,
    memory_class_override: str | None = None,
    truth_level_override: str | None = None,
    execution_role_override: str | None = None,
    tags_override: list[str] | None = None,
    refs_override: list[str] | None = None,
) -> dict[str, Any]:
    normalized_type = str(artifact_type or "").strip()
    inferred_memory_class = str(
        memory_class_override or ""
    ).strip() or infer_memory_class(normalized_type)
    created_at = None
    if isinstance(existing_artifact, dict):
        created_at = (
            str(
                existing_artifact.get("created_at")
                or existing_artifact.get("timestamp")
                or ""
            ).strip()
            or None
        )
    updated_at = _utc_timestamp()
    tags = (
        list(tags_override)
        if isinstance(tags_override, list)
        else _string_list_from_payload(payload, "tags")
    )
    refs = (
        list(refs_override)
        if isinstance(refs_override, list)
        else _string_list_from_payload(payload, "refs")
    )

    if normalized_type == "execution_trace":
        memory_object = make_trace_object(
            id=artifact_id,
            domain=domain,
            payload=payload,
            local_path=artifact_path,
            remote_path=remote_path,
            status=str(artifact_status or "").strip() or "active",
            truth_level=str(truth_level_override or "").strip() or "working",
            execution_role=str(execution_role_override or "").strip() or "evidence",
            created_at=created_at,
            updated_at=updated_at,
            tags=tags,
            refs=refs,
            artifact_type=normalized_type,
            logical_key=logical_key,
            state=str(artifact_status or "").strip() or None,
            resolution=resolution,
        )
    elif inferred_memory_class == "conflict":
        memory_object = make_conflict_object(
            id=artifact_id,
            domain=domain,
            payload=payload,
            local_path=artifact_path,
            remote_path=remote_path,
            status=str(artifact_status or "").strip() or "active",
            truth_level=str(truth_level_override or "").strip() or "working",
            execution_role=str(execution_role_override or "").strip() or "blocker",
            created_at=created_at,
            updated_at=updated_at,
            tags=tags,
            refs=refs,
            artifact_type=normalized_type,
            logical_key=logical_key,
            state=str(artifact_status or "").strip() or None,
            resolution=resolution,
        )
    elif inferred_memory_class != "artifact":
        memory_object = memory_object_from_mapping(
            {
                "id": artifact_id,
                "domain": domain,
                "memory_class": inferred_memory_class,
                "status": str(artifact_status or "").strip() or "active",
                "truth_level": str(truth_level_override or "").strip() or "working",
                "execution_role": str(execution_role_override or "").strip()
                or "state_holder",
                "created_at": created_at or updated_at,
                "updated_at": updated_at,
                "tags": tags,
                "refs": refs,
                "local_path": str(artifact_path),
                "remote_path": remote_path,
                "payload": payload,
                "type": normalized_type,
                "logical_key": logical_key,
                "state": str(artifact_status or "").strip() or None,
                "resolution": resolution,
            }
        )
    else:
        memory_object = make_artifact_object(
            id=artifact_id,
            domain=domain,
            payload=payload,
            local_path=artifact_path,
            remote_path=remote_path,
            status=str(artifact_status or "").strip() or "active",
            truth_level=str(truth_level_override or "").strip() or "working",
            execution_role=str(execution_role_override or "").strip() or "output",
            created_at=created_at,
            updated_at=updated_at,
            tags=tags,
            refs=refs,
            artifact_type=normalized_type,
            logical_key=logical_key,
            state=str(artifact_status or "").strip() or None,
            resolution=resolution,
        )
    return memory_object.to_dict()


def _write_artifact_record(path: Path, record: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(record, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def resolve_path(
    domain: str, artifact_type: str, artifact_id: str | None = None
) -> Path:
    normalized_domain = _normalize_domain(domain)
    normalized_type = _normalize_name(artifact_type, field_name="artifact_type")
    if artifact_id is None:
        filename = f"{normalized_type}.json"
    else:
        normalized_id = _normalize_name(artifact_id, field_name="artifact_id")
        filename = f"{normalized_type}_{normalized_id}.json"
    return _ensure_domain_path(
        normalized_domain, _domain_root(normalized_domain) / filename
    )


def save_artifact(
    domain: str,
    artifact_type: str,
    payload: Any,
    *,
    overwrite: bool = False,
    artifact_status: str | None = None,
    resolution: dict[str, Any] | None = None,
    relative_path: Path | str | None = None,
    domain_root_override: Path | str | None = None,
    memory_class_override: str | None = None,
    truth_level_override: str | None = None,
    execution_role_override: str | None = None,
    tags_override: list[str] | None = None,
    refs_override: list[str] | None = None,
) -> Path:
    normalized_domain = _normalize_domain(domain)
    normalized_type = _normalize_name(artifact_type, field_name="artifact_type")
    logical_id = _logical_id_for_payload(normalized_domain, normalized_type, payload)
    logical_key = compute_artifact_key(normalized_domain, normalized_type, logical_id)
    try:
        existing_artifact = get_artifact_by_logical_key(logical_key)
    except MemoryRegistryError as exc:
        print(f"[STORAGE] registry warning {exc}")
        existing_artifact = None

    if isinstance(existing_artifact, dict) and not overwrite:
        existing_local_path = str(existing_artifact.get("local_path") or "").strip()
        if existing_local_path:
            existing_path = Path(existing_local_path)
            if existing_path.exists():
                print(
                    f"[STORAGE] idempotent_hit artifact={existing_artifact.get('id')}"
                )
                return existing_path

    artifact_id = _artifact_id_for_payload(normalized_domain, normalized_type, payload)
    domain_root = (
        Path(domain_root_override)
        if domain_root_override is not None
        else _domain_root(normalized_domain)
    )
    if relative_path is None:
        artifact_path = _ensure_path_with_root(
            domain_root,
            f"{normalized_type}_{artifact_id}.json",
            domain=normalized_domain,
        )
    else:
        raw_relative_path = str(relative_path).strip()
        if not raw_relative_path:
            raise StorageAdapterError("relative_path must not be empty.")
        artifact_path = _ensure_path_with_root(
            domain_root,
            Path(raw_relative_path),
            domain=normalized_domain,
        )
    artifact_path.parent.mkdir(parents=True, exist_ok=True)

    record = _memory_object_for_artifact(
        artifact_id=artifact_id,
        domain=normalized_domain,
        artifact_type=normalized_type,
        payload=payload,
        artifact_path=artifact_path,
        remote_path=None,
        logical_key=logical_key,
        artifact_status=artifact_status,
        resolution=resolution,
        existing_artifact=existing_artifact
        if isinstance(existing_artifact, dict)
        else None,
        memory_class_override=memory_class_override,
        truth_level_override=truth_level_override,
        execution_role_override=execution_role_override,
        tags_override=tags_override,
        refs_override=refs_override,
    )
    _write_artifact_record(artifact_path, record)
    print(f"[STORAGE] local saved {artifact_path}")

    uploaded_remote_path: str | None = None
    config = get_storage_backend_config()
    if domain_root_override is None and bool(config.get("webdav_enabled", False)):
        remote_path = _webdav_remote_path_for_domain(normalized_domain, artifact_path)
        try:
            uploaded_remote_path = upload_to_webdav(artifact_path, remote_path)
            print(f"[STORAGE] webdav uploaded {uploaded_remote_path}")
        except WebDAVError as exc:
            print(f"[STORAGE] webdav warning {exc}")
    if uploaded_remote_path is not None:
        record = _memory_object_for_artifact(
            artifact_id=artifact_id,
            domain=normalized_domain,
            artifact_type=normalized_type,
            payload=payload,
            artifact_path=artifact_path,
            remote_path=uploaded_remote_path,
            logical_key=logical_key,
            artifact_status=artifact_status,
            resolution=resolution,
            existing_artifact=existing_artifact
            if isinstance(existing_artifact, dict)
            else None,
            memory_class_override=memory_class_override,
            truth_level_override=truth_level_override,
            execution_role_override=execution_role_override,
            tags_override=tags_override,
            refs_override=refs_override,
        )
        _write_artifact_record(artifact_path, record)
    try:
        register_artifact(
            artifact_id,
            normalized_domain,
            normalized_type,
            artifact_path,
            logical_key=logical_key,
            remote_path=uploaded_remote_path,
            timestamp=str(record.get("timestamp") or "").strip() or None,
            status=effective_status(record) or None,
            resolution=record.get("resolution")
            if isinstance(record.get("resolution"), dict)
            else None,
            refs=list(record.get("refs") or []),
            memory_class=str(record.get("memory_class") or "").strip() or None,
            truth_level=str(record.get("truth_level") or "").strip() or None,
            execution_role=str(record.get("execution_role") or "").strip() or None,
            created_at=str(record.get("created_at") or "").strip() or None,
            updated_at=str(record.get("updated_at") or "").strip() or None,
            state=str(record.get("state") or "").strip() or None,
            payload=payload,
        )
    except MemoryRegistryError as exc:
        print(f"[STORAGE] registry warning {exc}")
    return artifact_path


def load_artifact(domain: str, path: Path | str) -> dict[str, Any]:
    normalized_domain = _normalize_domain(domain)
    artifact_path = _ensure_domain_path(normalized_domain, path)
    if not artifact_path.exists():
        raise ArtifactNotFoundError(f"Artifact not found: {artifact_path}")

    try:
        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise StorageAdapterError(
            f"Artifact is not valid JSON: {artifact_path}"
        ) from exc

    if not isinstance(payload, dict):
        raise StorageAdapterError(
            f"Artifact must contain a JSON object: {artifact_path}"
        )

    stored_domain = str(payload.get("domain") or "").strip().lower()
    if stored_domain != normalized_domain:
        raise BoundaryViolationError(
            f"Artifact domain '{stored_domain}' does not match requested domain '{normalized_domain}'."
        )
    return payload


def archive_artifact(domain: str, path: Path | str) -> Path:
    normalized_domain = _normalize_domain(domain)
    artifact_path = _ensure_domain_path(normalized_domain, path)
    if not artifact_path.exists():
        raise ArtifactNotFoundError(f"Artifact not found: {artifact_path}")

    archive_dir = _ensure_domain_path(
        normalized_domain,
        _domain_root(normalized_domain) / ARCHIVE_DIRNAME,
    )
    archive_dir.mkdir(parents=True, exist_ok=True)
    archived_path = _ensure_domain_path(
        normalized_domain, archive_dir / artifact_path.name
    )
    shutil.move(str(artifact_path), str(archived_path))
    return archived_path


def upload_to_webdav(local_path: Path | str, remote_path: str) -> str:
    config = get_storage_backend_config()
    if not bool(config.get("webdav_enabled", False)):
        raise WebDAVConfigError(
            "WebDAV backend is disabled in config/storage_backend.json."
        )

    webdav_url = str(config.get("webdav_url") or "").strip().rstrip("/")
    webdav_user = str(os.environ.get("WEBDEV_USER") or "").strip()
    webdav_password = str(os.environ.get("WEBDEV_PASSWORD") or "").strip()
    if not webdav_url or not webdav_user or not webdav_password:
        raise WebDAVConfigError(
            "WebDAV config requires webdav_url plus WEBDEV_USER and WEBDEV_PASSWORD in the environment."
        )

    normalized_remote_path = _normalize_webdav_remote_path(remote_path)
    candidate_local_path = Path(local_path).resolve(strict=False)
    if not candidate_local_path.exists():
        raise ArtifactNotFoundError(f"Artifact not found: {candidate_local_path}")

    target_url = f"{webdav_url}/{normalized_remote_path}"
    try:
        with candidate_local_path.open("rb") as handle:
            response = requests.put(
                target_url,
                data=handle,
                auth=(webdav_user, webdav_password),
                timeout=15,
            )
    except requests.RequestException as exc:
        raise WebDAVNetworkError(
            f"WebDAV request failed for '{normalized_remote_path}': {exc}"
        ) from exc

    if response.status_code in (401, 403):
        raise WebDAVUnauthorizedError(
            f"WebDAV rejected access to '{normalized_remote_path}'."
        )
    if response.status_code >= 400:
        raise WebDAVError(
            f"WebDAV upload failed for '{normalized_remote_path}' with status {response.status_code}."
        )
    return normalized_remote_path


def _opencloud_credentials(config: dict[str, Any]) -> tuple[str, str]:
    opencloud = dict(config.get("opencloud") or {})
    username_env = str(opencloud.get("username_env") or "OPENCLOUD_USERNAME").strip()
    token_env = str(opencloud.get("app_token_env") or "OPENCLOUD_APP_TOKEN").strip()
    username = _load_env_value(username_env)
    token = _load_env_value(token_env)
    if not username or not token:
        raise OpenCloudConfigError(
            "OpenCloud credentials are missing. Configure the username and app token in the environment."
        )
    return username, token


def _opencloud_base_url(config: dict[str, Any]) -> str:
    if not bool(config.get("opencloud_enabled", False)):
        raise OpenCloudConfigError(
            "OpenCloud backend is disabled in config/storage_backend.json."
        )
    opencloud = dict(config.get("opencloud") or {})
    base_url = str(opencloud.get("base_url") or "").strip().rstrip("/")
    if not base_url:
        raise OpenCloudConfigError(
            "OpenCloud base_url is required in config/storage_backend.json."
        )
    if not base_url.startswith("https://"):
        raise OpenCloudConfigError("OpenCloud base_url must use https://")
    return base_url


def _webdav_request(
    method: str,
    remote_path: str,
    config: dict[str, Any],
    *,
    content: bytes | None = None,
) -> httpx.Response:
    base_url = _opencloud_base_url(config)
    username, token = _opencloud_credentials(config)
    opencloud = dict(config.get("opencloud") or {})
    timeout_seconds = float(opencloud.get("timeout_seconds") or 15.0)
    url = f"{base_url}/{remote_path.lstrip('/')}"
    try:
        return httpx.request(
            method=method,
            url=url,
            content=content,
            auth=(username, token),
            timeout=timeout_seconds,
        )
    except httpx.RequestError as exc:
        raise OpenCloudNetworkError(
            f"OpenCloud request failed for '{remote_path}': {exc}"
        ) from exc


def _ensure_opencloud_success(
    response: httpx.Response,
    *,
    remote_path: str,
    operation: str,
    missing_as_not_found: bool = False,
) -> None:
    if response.status_code in (401, 403):
        raise OpenCloudUnauthorizedError(
            f"OpenCloud rejected access to '{remote_path}' during {operation}."
        )
    if missing_as_not_found and response.status_code == 404:
        raise ArtifactNotFoundError(f"Artifact not found in OpenCloud: {remote_path}")
    if response.status_code >= 400:
        raise OpenCloudError(
            f"OpenCloud {operation} failed for '{remote_path}' with status {response.status_code}."
        )


def _ensure_opencloud_collections(remote_path: str, config: dict[str, Any]) -> None:
    current_parts: list[str] = []
    path_parts = remote_path.split("/")[:-1]
    for part in path_parts:
        current_parts.append(part)
        collection_path = "/".join(current_parts)
        response = _webdav_request("MKCOL", collection_path, config)
        if response.status_code not in (201, 301, 405):
            _ensure_opencloud_success(
                response,
                remote_path=collection_path,
                operation="collection create",
            )


def sync_to_opencloud(domain: str, local_path: Path | str) -> str:
    normalized_domain = _normalize_domain(domain)
    config = get_storage_backend_config()
    local_artifact_path = _ensure_domain_path(normalized_domain, local_path)
    if not local_artifact_path.exists():
        raise ArtifactNotFoundError(f"Artifact not found: {local_artifact_path}")

    relative_local_path = local_artifact_path.relative_to(
        _domain_root(normalized_domain)
    )
    remote_path = _normalize_remote_path(
        normalized_domain,
        relative_local_path.as_posix(),
        config,
    )
    _ensure_opencloud_collections(remote_path, config)
    response = _webdav_request(
        "PUT",
        remote_path,
        config,
        content=local_artifact_path.read_bytes(),
    )
    _ensure_opencloud_success(response, remote_path=remote_path, operation="sync")
    return remote_path


def fetch_from_opencloud(domain: str, remote_path: str) -> Path:
    normalized_domain = _normalize_domain(domain)
    if normalized_domain == "dev":
        raise BoundaryViolationError(
            "The dev contour is export-only for OpenCloud and may not fetch artifacts back into local storage."
        )

    config = get_storage_backend_config()
    normalized_remote_path = _normalize_remote_path(
        normalized_domain, remote_path, config
    )
    response = _webdav_request("GET", normalized_remote_path, config)
    _ensure_opencloud_success(
        response,
        remote_path=normalized_remote_path,
        operation="fetch",
        missing_as_not_found=True,
    )

    relative_local_path = _relative_local_path_for_remote(
        normalized_domain,
        normalized_remote_path,
        config,
    )
    local_target_path = _ensure_domain_path(
        normalized_domain,
        _domain_root(normalized_domain) / relative_local_path,
    )
    local_target_path.parent.mkdir(parents=True, exist_ok=True)
    local_target_path.write_bytes(response.content)
    return local_target_path
