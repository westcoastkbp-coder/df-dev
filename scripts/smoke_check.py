from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.execution.paths import LOGS_DIR, OUTPUT_DIR, ROOT_DIR, STATE_DIR


def _check_path(name: str, path: Path, *, expect_file: bool = False) -> dict[str, str]:
    exists = path.is_file() if expect_file else path.is_dir()
    return {
        "name": name,
        "status": "ok" if exists else "error",
    }


def main() -> int:
    checks = [
        _check_path("runtime_logs", ROOT_DIR / LOGS_DIR),
        _check_path("runtime_state", ROOT_DIR / STATE_DIR),
        _check_path("runtime_out", ROOT_DIR / OUTPUT_DIR),
        _check_path(
            "task_state", ROOT_DIR / STATE_DIR / "task_state.sqlite3", expect_file=True
        ),
    ]
    status = "ok" if all(item["status"] == "ok" for item in checks) else "error"
    print(json.dumps({"status": status, "checks": checks}, indent=2))
    return 0 if status == "ok" else 1


if __name__ == "__main__":
    raise SystemExit(main())
