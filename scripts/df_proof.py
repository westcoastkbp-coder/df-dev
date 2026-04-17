from datetime import datetime
from pathlib import Path


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    proof_dir = repo_root / "runtime" / "proof"
    proof_file = proof_dir / "proof.txt"

    proof_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().astimezone().isoformat()
    proof_file.write_text(
        f"{timestamp}\nDF REAL EXECUTION CONFIRMED\n",
        encoding="utf-8",
    )

    print(proof_file.resolve())


if __name__ == "__main__":
    main()
