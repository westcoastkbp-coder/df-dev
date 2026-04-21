from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

from app.execution.product_box_build import generate_product_box_build


def _build_product_box(tmp_path: Path) -> Path:
    output_dir = tmp_path / "product_box"
    report = generate_product_box_build(output_dir=output_dir)
    assert report["build_status"] == "PASS"
    return output_dir


def _run_boot(build_dir: Path) -> dict[str, object]:
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            "import json; from app.start_product import boot_product_box; print(json.dumps(boot_product_box({'status': 'WORKING', 'broken': {}}), ensure_ascii=True))",
        ],
        cwd=build_dir,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    return json.loads(completed.stdout.strip().splitlines()[-1])


def test_boot_from_clean_build_works(tmp_path: Path) -> None:
    build_dir = _build_product_box(tmp_path)

    report = _run_boot(build_dir)

    assert report["boot_status"] == "PASS"
    assert report["dev_leaks"] == []
    assert report["missing_files"] == []
    assert report["execution_status"] == "OK"


def test_dev_leak_detected_fails_boot(tmp_path: Path) -> None:
    build_dir = _build_product_box(tmp_path)
    leak_dir = build_dir / "scripts"
    leak_dir.mkdir(parents=True, exist_ok=True)
    (leak_dir / "dev_only.py").write_text("print('dev leak')\n", encoding="utf-8")

    report = _run_boot(build_dir)

    assert report["boot_status"] == "FAIL"
    assert "scripts" in report["dev_leaks"]


def test_missing_runtime_folder_fails_boot(tmp_path: Path) -> None:
    build_dir = _build_product_box(tmp_path)
    shutil.rmtree(build_dir / "runtime" / "logs")

    report = _run_boot(build_dir)

    assert report["boot_status"] == "FAIL"
    assert "runtime/logs" in report["missing_files"]
