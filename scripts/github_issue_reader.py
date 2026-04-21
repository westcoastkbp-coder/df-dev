from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from control.github_issue_intake import fetch_and_store_issue_task_packet


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read one GitHub issue and save a normalized local task packet.",
    )
    parser.add_argument(
        "issue_number",
        type=int,
        help="GitHub issue number to fetch.",
    )
    parser.add_argument(
        "--repo",
        default=None,
        help="GitHub repository in owner/name format.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional output directory for the generated task packet.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        packet, output_path = fetch_and_store_issue_task_packet(
            issue_number=args.issue_number,
            repo_name=args.repo,
            output_dir=args.output_dir,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"TASK_PACKET_WRITTEN: {output_path.resolve()}")
    print(json.dumps(packet, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
