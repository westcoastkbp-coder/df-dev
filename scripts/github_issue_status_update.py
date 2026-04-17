from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from control.github_issue_status_update import update_issue_execution_status


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Add an execution-complete comment and set the GitHub issue status to DONE.",
    )
    parser.add_argument(
        "issue_number",
        type=int,
        help="GitHub issue number to update.",
    )
    parser.add_argument(
        "commit_hash",
        help="Commit hash to include in the completion comment.",
    )
    parser.add_argument(
        "artifact_path",
        help="Artifact path to include in the completion comment.",
    )
    parser.add_argument(
        "--repo",
        default=None,
        help="GitHub repository in owner/name format.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        _, comment_id = update_issue_execution_status(
            issue_number=args.issue_number,
            commit_hash=args.commit_hash,
            artifact_path=args.artifact_path,
            repo_name=args.repo,
        )
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print("issue updated")
    if comment_id is not None:
        print(f"comment id: {comment_id}")
    else:
        print("comment id: unavailable")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
