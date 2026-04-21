from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.execution.batch_real_lead_runner import load_batch_leads, write_batch_report


def main(argv: list[str] | None = None) -> int:
    args = list(argv or sys.argv[1:])
    if not args:
        raise ValueError("batch input path required")
    input_path = Path(args[0]).resolve()
    leads = load_batch_leads(input_path)
    report_path = write_batch_report(leads)
    report = json.loads(report_path.read_text(encoding="utf-8"))
    print(json.dumps({"report_path": str(report_path), "report": report}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
