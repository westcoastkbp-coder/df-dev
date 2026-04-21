from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.product.runner import execute_product_task_request
from app.voice.app import ProductTaskRunRequest, _run_product_task_internal


def main() -> None:
    result = execute_product_task_request(
        ProductTaskRunRequest(
            objective="EXECUTE_CHAIN: DF-CREATE-FILE-V1 → DF-READ-FILE-V1",
            user_id="codex",
            user_role="admin",
        ),
        principal={"actor_id": "codex", "role": "admin"},
        request_source="api",
        execute_single=_run_product_task_internal,
    )
    print(result)


if __name__ == "__main__":
    main()
