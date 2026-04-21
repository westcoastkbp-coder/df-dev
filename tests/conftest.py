from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-longrun",
        action="store_true",
        default=False,
        help="include longrun tests in the collected suite",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    if config.getoption("--run-longrun"):
        return
    selected: list[pytest.Item] = []
    deselected: list[pytest.Item] = []
    for item in items:
        if "longrun" in item.keywords:
            deselected.append(item)
            continue
        selected.append(item)
    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = selected


@pytest.fixture(autouse=True)
def _allow_feature_branch_codex_loop_tests(monkeypatch: pytest.MonkeyPatch) -> None:
    try:
        import control.codex_loop as codex_loop
    except Exception:
        return

    monkeypatch.setattr(codex_loop, "ensure_not_main_branch", lambda: None)
    monkeypatch.setattr(codex_loop, "commit_loop_result", lambda *args, **kwargs: None)
    monkeypatch.setattr(codex_loop, "ensure_dev_session", lambda *args, **kwargs: {})
    monkeypatch.setattr(codex_loop, "update_dev_session", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        codex_loop,
        "build_codex_execution_prompt",
        lambda prompt, **kwargs: prompt,
    )
    monkeypatch.setattr(
        codex_loop, "extract_last_failing_test", lambda *args, **kwargs: ""
    )
