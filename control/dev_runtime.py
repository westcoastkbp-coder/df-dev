from __future__ import annotations

import os
import re
import shlex
import subprocess
from pathlib import Path
from typing import Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
DEV_RUNTIME_ENV = "DF_DEV_RUNTIME"
WSL_DISTRO_ENV = "DF_WSL_DISTRO"
_WINDOWS_DRIVE_PATTERN = re.compile(r"^(?P<drive>[A-Za-z]):[\\/](?P<rest>.*)$")
_WSL_VENV_CANDIDATES = (".venv/bin/activate", "venv/bin/activate")


def preferred_dev_runtime(environ: Mapping[str, str] | None = None) -> str:
    env = os.environ if environ is None else environ
    configured = str(env.get(DEV_RUNTIME_ENV, "")).strip().lower()
    if configured in {"native", "wsl"}:
        return configured
    return "wsl" if os.name == "nt" else "native"


def is_wsl_primary(environ: Mapping[str, str] | None = None) -> bool:
    return preferred_dev_runtime(environ) == "wsl"


def normalize_path_for_dev_env(path: Path | str) -> str:
    raw = str(path)
    match = _WINDOWS_DRIVE_PATTERN.match(raw)
    if match:
        rest = match.group("rest").replace("\\", "/").lstrip("/")
        return f"/mnt/{match.group('drive').lower()}/{rest}"
    return raw.replace("\\", "/")


def repo_root_for_dev_env(repo_root: Path | str | None = None) -> str:
    root = Path(repo_root) if repo_root is not None else REPO_ROOT
    return normalize_path_for_dev_env(root.resolve(strict=False))


def venv_activation_snippet(repo_root: Path | str | None = None) -> str:
    root = Path(repo_root) if repo_root is not None else REPO_ROOT
    for candidate in _WSL_VENV_CANDIDATES:
        if (root / candidate).is_file():
            return f". {shlex.quote(candidate)} >/dev/null 2>&1 && "
    return ""


def dev_runtime_summary(
    *,
    repo_root: Path | str | None = None,
    environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    return {
        "runtime": preferred_dev_runtime(environ),
        "repo_root": repo_root_for_dev_env(repo_root),
        "venv_activation": venv_activation_snippet(repo_root=repo_root),
    }


def _normalize_argument(arg: object) -> str:
    if isinstance(arg, Path):
        return normalize_path_for_dev_env(arg.resolve(strict=False))
    return normalize_path_for_dev_env(str(arg))


def _wsl_command(
    args: Sequence[object],
    *,
    cwd: Path,
    repo_root: Path | str | None = None,
) -> list[str]:
    quoted_command = " ".join(shlex.quote(_normalize_argument(arg)) for arg in args)
    activation = venv_activation_snippet(repo_root=repo_root or cwd)
    shell_command = (
        f"cd {shlex.quote(repo_root_for_dev_env(cwd))} && {activation}{quoted_command}"
    )
    distro = str(os.environ.get(WSL_DISTRO_ENV, "")).strip()
    command = ["wsl.exe"]
    if distro:
        command.extend(["-d", distro])
    command.extend(["bash", "-lc", shell_command])
    return command


def run_in_dev_env(
    args: Sequence[object],
    *,
    cwd: Path | str | None = None,
    env: Mapping[str, str] | None = None,
    capture_output: bool = False,
    text: bool = False,
    check: bool = False,
    timeout: float | None = None,
    encoding: str | None = None,
    errors: str | None = None,
) -> subprocess.CompletedProcess[str] | subprocess.CompletedProcess[bytes]:
    working_dir = Path(cwd) if cwd is not None else REPO_ROOT
    if not is_wsl_primary():
        return subprocess.run(
            [
                str(arg)
                if not isinstance(arg, Path)
                else str(arg.resolve(strict=False))
                for arg in args
            ],
            cwd=working_dir,
            env=dict(env) if env is not None else None,
            capture_output=capture_output,
            text=text,
            check=check,
            timeout=timeout,
            encoding=encoding,
            errors=errors,
        )
    return subprocess.run(
        _wsl_command(args, cwd=working_dir, repo_root=working_dir),
        cwd=working_dir,
        env=dict(env) if env is not None else None,
        capture_output=capture_output,
        text=text,
        check=check,
        timeout=timeout,
        encoding=encoding,
        errors=errors,
    )
