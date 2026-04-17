from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.execution.real_workflow_validation import (
    run_validation_pack,
    write_validation_report,
)


def main() -> int:
    report_path = write_validation_report()
    report = run_validation_pack()
    print(json.dumps({"report_path": str(report_path), "report": report}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
