from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import control.dev_runtime as dev_runtime


def test_preferred_dev_runtime_defaults_to_wsl_on_windows(monkeypatch) -> None:
    monkeypatch.delenv(dev_runtime.DEV_RUNTIME_ENV, raising=False)
    monkeypatch.setattr(dev_runtime.os, "name", "nt", raising=False)

    assert dev_runtime.preferred_dev_runtime() == "wsl"


def test_normalize_path_for_dev_env_converts_windows_paths() -> None:
    assert (
        dev_runtime.normalize_path_for_dev_env(r"D:\digital_foreman\scripts\run_codex_task.py")
        == "/mnt/d/digital_foreman/scripts/run_codex_task.py"
    )


def test_run_in_dev_env_wraps_commands_for_wsl(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    venv_activate = repo_root / ".venv" / "bin" / "activate"
    venv_activate.parent.mkdir(parents=True, exist_ok=True)
    venv_activate.write_text("", encoding="utf-8")

    captured: list[tuple[list[str], dict[str, object]]] = []

    def fake_run(command, **kwargs):
        captured.append((list(command), dict(kwargs)))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setenv(dev_runtime.DEV_RUNTIME_ENV, "wsl")
    monkeypatch.delenv(dev_runtime.WSL_DISTRO_ENV, raising=False)
    monkeypatch.setattr(dev_runtime.subprocess, "run", fake_run)

    dev_runtime.run_in_dev_env(
        ["python", "-m", "pytest", repo_root / "tests" / "test_sample.py"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )

    command, kwargs = captured[0]
    assert command[:3] == ["wsl.exe", "bash", "-lc"]
    assert "python -m pytest" in command[-1]
    assert ". .venv/bin/activate" in command[-1]
    assert "/tests/test_sample.py" in command[-1]
    assert kwargs["cwd"] == repo_root
