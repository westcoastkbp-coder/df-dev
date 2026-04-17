from __future__ import annotations

from pathlib import Path
import sys

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import control.external_review_gate as external_review_gate


def test_external_review_gate_approves_when_claude_ok_and_gemini_verified(monkeypatch):
    monkeypatch.setattr(
        external_review_gate,
        "call_claude_local",
        lambda packet: {"status": "ok", "stdout": "no issues", "stderr": ""},
    )
    monkeypatch.setattr(
        external_review_gate,
        "call_gemini_verifier",
        lambda packet: {
            "verdict": "VERIFIED",
            "bypass_risks": [],
            "adversarial_test": "",
            "notes": "",
        },
    )

    result = external_review_gate.run_external_review("task-1", "summary", ["a.py"])

    assert result["status"] == "review_complete"
    assert result["decision"] == "APPROVED"


def test_external_review_gate_blocks_on_claude_critical_issue(monkeypatch):
    monkeypatch.setattr(
        external_review_gate,
        "call_claude_local",
        lambda packet: {"status": "ok", "stdout": "critical issue found", "stderr": ""},
    )
    monkeypatch.setattr(
        external_review_gate,
        "call_gemini_verifier",
        lambda packet: {
            "verdict": "VERIFIED",
            "bypass_risks": [],
            "adversarial_test": "",
            "notes": "",
        },
    )

    result = external_review_gate.run_external_review("task-2", "summary", ["a.py"])

    assert result["decision"] == "BLOCKED"


def test_external_review_gate_blocks_on_gemini_not_verified(monkeypatch):
    monkeypatch.setattr(
        external_review_gate,
        "call_claude_local",
        lambda packet: {"status": "ok", "stdout": "no issues", "stderr": ""},
    )
    monkeypatch.setattr(
        external_review_gate,
        "call_gemini_verifier",
        lambda packet: {
            "verdict": "NOT_VERIFIED",
            "bypass_risks": ["risk"],
            "adversarial_test": "",
            "notes": "",
        },
    )

    result = external_review_gate.run_external_review("task-3", "summary", ["a.py"])

    assert result["decision"] == "BLOCKED"


def test_external_review_gate_handles_unexpected_claude_failure(monkeypatch):
    def raise_claude(packet):
        raise RuntimeError("boom")

    monkeypatch.setattr(external_review_gate, "call_claude_local", raise_claude)
    monkeypatch.setattr(
        external_review_gate,
        "call_gemini_verifier",
        lambda packet: {
            "verdict": "VERIFIED",
            "bypass_risks": [],
            "adversarial_test": "",
            "notes": "",
        },
    )

    result = external_review_gate.run_external_review("task-4", "summary", ["a.py"])

    assert result["status"] == "review_complete"
    assert result["claude"]["status"] == "error"
    assert result["decision"] in {"APPROVED", "BLOCKED"}
