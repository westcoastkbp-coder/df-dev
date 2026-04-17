from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.execution.real_lead_runner import (
    load_real_lead_input,
    run_real_lead,
    write_real_lead_run_report,
)


def main(argv: list[str] | None = None) -> int:
    args = list(argv or sys.argv[1:])
    input_path = Path(args[0]).resolve() if args else None
    lead_input = load_real_lead_input(input_path)
    report_path = write_real_lead_run_report(lead_input)
    report = run_real_lead(lead_input)
    print(json.dumps({"report_path": str(report_path), "report": report}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
