from __future__ import annotations

import re


class CommandParserError(ValueError):
    pass


def parse_command(command_text: str) -> dict[str, object]:
    normalized = str(command_text or "").strip()
    if not normalized:
        return {"action": "none", "tasks": []}

    upper = normalized.upper()
    if upper.startswith("EXECUTE_CHAIN:"):
        raw_tasks = normalized.split(":", 1)[1]
        tasks = [
            item.strip()
            for item in re.split(r"\s*(?:→|->)\s*", raw_tasks)
            if item.strip()
        ]
        if not tasks:
            raise CommandParserError("EXECUTE_CHAIN requires at least one task_id")
        return {"action": "execute_chain", "tasks": tasks}

    if upper.startswith("EXECUTE:"):
        task_id = normalized.split(":", 1)[1].strip()
        if not task_id:
            raise CommandParserError("EXECUTE requires a task_id")
        return {"action": "execute", "tasks": [task_id]}

    if normalized.lower() == "resources":
        return {"action": "resources", "tasks": ["DF-RESOURCES"]}

    return {"action": "none", "tasks": []}
