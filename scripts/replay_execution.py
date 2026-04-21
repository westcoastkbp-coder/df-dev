from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.execution.execution_replay import replay_execution


def main() -> int:
    if len(sys.argv) < 2:
        raise SystemExit("usage: replay_execution.py <run_id> [system_log_path]")
    run_id = sys.argv[1]
    log_path = Path(sys.argv[2]) if len(sys.argv) > 2 else None
    report = replay_execution(run_id, log_path=log_path)
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
