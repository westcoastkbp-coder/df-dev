from __future__ import annotations

from runtime.adapters.codex_adapter import CodexAdapter


def run_agent(role: str, task: dict) -> str:
    return CodexAdapter().run_agent(role=role, task=task)
