from __future__ import annotations

import json
from pathlib import Path


def build_claude_packet(task):
    return {
        "executor": "CLAUDE",
        "type": "UI_ACTION",
        "goal": task.get("goal"),
        "steps": task.get("steps", []),
        "constraints": [
            "WORK ONLY IN BROWSER",
            "NO FILE SYSTEM ACCESS",
            "NO SYSTEM MODIFICATION",
        ],
    }


def save_claude_packet(packet):
    path = Path("D:/digital_foreman/control/claude_tasks.json")

    if path.exists():
        data = json.loads(path.read_text())
    else:
        data = []

    data.append(packet)
    path.write_text(json.dumps(data, indent=2))
