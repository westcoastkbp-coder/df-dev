from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import control.memory as memory


def test_load_context_recovers_when_file_missing(monkeypatch, tmp_path):
    target = tmp_path / "system_context.json"
    monkeypatch.setattr(memory, "CTX_PATH", target)

    result = memory.load_context()

    assert result == memory.default_context()


def test_load_context_recovers_when_json_corrupted(monkeypatch, tmp_path):
    target = tmp_path / "system_context.json"
    target.write_text("{broken json", encoding="utf-8")
    monkeypatch.setattr(memory, "CTX_PATH", target)

    result = memory.load_context()

    assert result == memory.default_context()


def test_load_context_fills_missing_keys_for_partial_context(monkeypatch, tmp_path):
    target = tmp_path / "system_context.json"
    target.write_text(
        '{"system":"Digital Foreman","status":"WORKING"}', encoding="utf-8"
    )
    monkeypatch.setattr(memory, "CTX_PATH", target)

    result = memory.load_context()

    assert result["system"] == "Digital Foreman"
    assert result["status"] == "WORKING"
    assert result["modules"] == {}
    assert result["broken_modules"] == []
    assert result["broken"] == {}
    assert result["known_issues"] == []
    assert result["history"] == []
    assert result["modules_state"] == {}
    assert result["last_codex_loop"] == {}
    assert result["next_required"] == ""
