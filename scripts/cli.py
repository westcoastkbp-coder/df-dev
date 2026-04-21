from __future__ import annotations

import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.product.runner import execute_product_task_request
from app.voice.app import ProductTaskRunRequest, _run_product_task_internal
from runtime.token_report import get_last_run_report


def _report_tokens_enabled() -> bool:
    return str(os.getenv("REPORT_TOKENS", "")).strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def main() -> None:
    principal = {"actor_id": "codex", "role": "admin"}
    while True:
        try:
            command = input(">> ").strip()
        except EOFError:
            break
        except KeyboardInterrupt:
            print()
            break

        if not command:
            continue
        if command.lower() in {"exit", "quit"}:
            break

        result = execute_product_task_request(
            ProductTaskRunRequest(
                objective=command,
                user_id="codex",
                user_role="admin",
            ),
            principal=principal,
            request_source="cli",
            execute_single=_run_product_task_internal,
        )
        print(result)
        if _report_tokens_enabled():
            print(json.dumps(get_last_run_report(), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
