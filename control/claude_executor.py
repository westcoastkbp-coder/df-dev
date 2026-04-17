from __future__ import annotations

from pathlib import Path

from control.dev_runtime import run_in_dev_env
from control.env_loader import load_env


def load_key():
    return load_env()["CLAUDE_API_KEY"]


def call_claude_local(packet):
    result = run_in_dev_env(
        ["claude", str(packet)],
        cwd=Path(__file__).resolve().parents[1],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )

    return {
        "status": "ok",
        "stdout": result.stdout,
        "stderr": result.stderr
    }
