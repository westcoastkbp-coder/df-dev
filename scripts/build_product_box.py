from __future__ import annotations

import json
from pathlib import Path

from app.execution.product_box_build import generate_product_box_build
from app.execution.product_box_manifest import validate_product_box_manifest
from app.execution.product_packaging import validate_product_packaging


def main() -> int:
    validate_product_box_manifest()
    build_report = generate_product_box_build()
    if build_report["build_status"] != "PASS":
        print(json.dumps(build_report, indent=2, sort_keys=True))
        return 1

    packaging_report = validate_product_packaging(
        root_dir=Path(build_report["output_dir"])
    )
    final_report = dict(build_report)
    final_report["packaging_validation"] = packaging_report
    print(json.dumps(final_report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
