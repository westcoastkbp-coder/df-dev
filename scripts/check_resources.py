from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from runtime.resource_probe import get_resource_snapshot

RESOURCES_LOG_FILE = ROOT_DIR / "runtime" / "logs" / "resources.log"


def write_snapshot(snapshot: dict[str, object]) -> None:
    RESOURCES_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    RESOURCES_LOG_FILE.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")


def main() -> int:
    snapshot = get_resource_snapshot()
    write_snapshot(snapshot)
    json.dump(snapshot, sys.stdout, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
