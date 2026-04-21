from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Mapping


VALID_ENV_ROLES = {"local_dev", "remote_runtime"}


@dataclass(frozen=True)
class RemoteEndpointSettings:
    enabled: bool
    base_url: str
    verify_tls: bool
    request_timeout_seconds: float


@dataclass(frozen=True)
class StoragePaths:
    storage_root: Path
    runtime_root: Path
    logs_dir: Path
    state_dir: Path
    verification_dir: Path
    shared_context_dir: Path
    interactions_file: Path
    decisions_file: Path
    audit_file: Path
    system_context_file: Path
    global_context_file: Path
    active_threads_dir: Path


@dataclass(frozen=True)
class HybridRuntimeConfig:
    role: str
    host: str
    port: int
    workspace_root: Path
    storage_paths: StoragePaths
    remote_endpoint: RemoteEndpointSettings

    def as_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["workspace_root"] = self.workspace_root.as_posix()
        payload["storage_paths"] = {
            key: value.as_posix() for key, value in asdict(self.storage_paths).items()
        }
        return payload


def _normalize_role(value: object) -> str:
    role = str(value or "").strip().lower() or "local_dev"
    if role not in VALID_ENV_ROLES:
        raise ValueError(f"unsupported ENV_ROLE `{role}`")
    return role


def _env_flag(environ: Mapping[str, str], key: str, *, default: bool) -> bool:
    value = str(environ.get(key, "")).strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def _env_float(environ: Mapping[str, str], key: str, *, default: float) -> float:
    value = str(environ.get(key, "")).strip()
    if not value:
        return default
    return float(value)


def _env_int(environ: Mapping[str, str], key: str, *, default: int) -> int:
    value = str(environ.get(key, "")).strip()
    if not value:
        return default
    return int(value)


def _storage_root_for_role(
    role: str,
    *,
    root_dir: Path,
    environ: Mapping[str, str],
) -> Path:
    configured_root = str(environ.get("DF_STORAGE_ROOT", "")).strip()
    if configured_root:
        return Path(configured_root)
    suffix = "local_dev" if role == "local_dev" else "remote_runtime"
    return root_dir / "runtime" / suffix


def _build_storage_paths(storage_root: Path) -> StoragePaths:
    runtime_root = storage_root / "runtime"
    shared_context_dir = storage_root / "shared_context"
    return StoragePaths(
        storage_root=storage_root,
        runtime_root=runtime_root,
        logs_dir=runtime_root / "logs",
        state_dir=runtime_root / "state",
        verification_dir=runtime_root / "verification",
        shared_context_dir=shared_context_dir,
        interactions_file=shared_context_dir / "interaction_history.jsonl",
        decisions_file=shared_context_dir / "decisions.jsonl",
        audit_file=shared_context_dir / "audit_trail.jsonl",
        system_context_file=shared_context_dir / "system_context.json",
        global_context_file=shared_context_dir / "global_context.json",
        active_threads_dir=shared_context_dir / "active_threads",
    )


def load_runtime_config(
    *,
    root_dir: Path,
    environ: Mapping[str, str] | None = None,
) -> HybridRuntimeConfig:
    env = dict(os.environ if environ is None else environ)
    role = _normalize_role(env.get("ENV_ROLE"))
    default_port = 9000 if role == "local_dev" else 9100
    storage_root = _storage_root_for_role(role, root_dir=root_dir, environ=env)
    storage_paths = _build_storage_paths(storage_root)
    return HybridRuntimeConfig(
        role=role,
        host=str(env.get("DF_HOST", "")).strip() or "127.0.0.1",
        port=_env_int(env, "DF_PORT", default=default_port),
        workspace_root=root_dir,
        storage_paths=storage_paths,
        remote_endpoint=RemoteEndpointSettings(
            enabled=role == "local_dev",
            base_url=str(env.get("DF_REMOTE_BASE_URL", "")).strip(),
            verify_tls=_env_flag(env, "DF_REMOTE_VERIFY_TLS", default=True),
            request_timeout_seconds=_env_float(
                env, "DF_REMOTE_TIMEOUT_SECONDS", default=30.0
            ),
        ),
    )
