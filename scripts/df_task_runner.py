import sys
from datetime import datetime
from pathlib import Path


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("Usage: python scripts/df_task_runner.py <task_id>")

    task_id = sys.argv[1]
    repo_root = Path(__file__).resolve().parent.parent
    task_dir = repo_root / "runtime" / "tasks" / task_id
    result_file = task_dir / "result.txt"

    task_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().astimezone().isoformat()
    result_file.write_text(
        f"timestamp: {timestamp}\ntask_id: {task_id}\nstatus: SUCCESS\n",
        encoding="utf-8",
    )

    print(result_file.resolve())


if __name__ == "__main__":
    main()
