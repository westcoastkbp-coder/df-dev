from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from modules.personal.context_store import (
    DEFAULT_PERSONAL_CONTEXT_PATH,
    extract_personal_context_update,
    update_personal_context_file,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add or update deterministic personal context records.",
    )
    parser.add_argument(
        "update_path",
        help="Path to a JSON payload or task JSON containing personal_context_update.",
    )
    parser.add_argument(
        "--context-file",
        default=str(DEFAULT_PERSONAL_CONTEXT_PATH),
        help="Optional path to personal_context.json.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = json.loads(Path(args.update_path).read_text(encoding="utf-8"))
    update_payload = extract_personal_context_update(payload)
    updated_context, path = update_personal_context_file(
        update_payload,
        context_path=args.context_file,
    )
    print(f"PERSONAL_CONTEXT_UPDATED: {path}")
    print(json.dumps(updated_context, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
