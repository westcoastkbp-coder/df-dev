from __future__ import annotations

import json
from pathlib import Path

from app.execution.paths import OUTPUT_DIR, ROOT_DIR, TASKS_FILE
from app.execution.real_lead_contract import normalize_real_lead_input
from app.execution.real_lead_runner import load_real_lead_input, run_real_lead


BATCH_REPORT_FILE = OUTPUT_DIR / "reports" / "real_lead_batch_report.json"
INDIVIDUAL_REPORTS_DIR = OUTPUT_DIR / "reports" / "real_lead_batch"
FAILURE_CLASSES = (
    "classification_error",
    "missing_required_input",
    "wrong_archive_path",
    "wrong_child_task_type",
    "traceability_gap",
    "state_inconsistency",
    "operator_usability_issue",
)


def load_batch_leads(input_path: Path) -> list[dict[str, object]]:
    payload = json.loads(Path(input_path).read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("batch input must be a JSON array")
    return [load_real_lead_input_from_payload(item) for item in payload]


def load_real_lead_input_from_payload(payload: object) -> dict[str, object]:
    return load_real_lead_input(None) if payload is None else _normalize_batch_item(payload)


def _normalize_batch_item(payload: object) -> dict[str, object]:
    return normalize_real_lead_input(payload)


def _individual_report_path(*, batch_id: str, lead_id: str, output_dir: Path) -> Path:
    safe_lead_id = str(lead_id or "unknown").strip().replace("-", "_")
    return output_dir / f"{batch_id.lower()}__{safe_lead_id.lower()}.json"


def _batch_id(leads: list[dict[str, object]]) -> str:
    if not leads:
        return "DF-BATCH-REAL-RUN-EMPTY"
    first = str(leads[0].get("lead_id") or "unknown").strip().upper().replace("-", "_")
    return f"DF-BATCH-REAL-RUN-{len(leads)}-{first}"


def run_real_lead_batch(
    leads: list[dict[str, object]],
    *,
    store_path: Path | None = None,
    output_dir: Path | None = None,
) -> dict[str, object]:
    target_store = Path(store_path) if store_path is not None else TASKS_FILE
    reports_dir = Path(output_dir) if output_dir is not None else ROOT_DIR / INDIVIDUAL_REPORTS_DIR
    reports_dir.mkdir(parents=True, exist_ok=True)
    batch_id = _batch_id(leads)

    individual_reports: list[dict[str, object]] = []
    for lead in leads:
        report = run_real_lead(lead, store_path=target_store)
        report_path = _individual_report_path(
            batch_id=batch_id,
            lead_id=str(report.get("lead_id", "")).strip(),
            output_dir=reports_dir,
        )
        report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        individual_reports.append(
            {
                "lead_id": str(report.get("lead_id", "")).strip(),
                "report_path": str(report_path),
                "report": dict(report),
            }
        )

    failure_distribution = {failure_class: 0 for failure_class in FAILURE_CLASSES}
    for item in individual_reports:
        failure_class = str(item["report"].get("failure_class", "")).strip()
        if failure_class in failure_distribution:
            failure_distribution[failure_class] += 1

    total_runs = len(individual_reports)
    passed_runs = sum(1 for item in individual_reports if item["report"].get("pass_fail") == "pass")
    failed_runs = total_runs - passed_runs
    manual_review_count = sum(
        1 for item in individual_reports if item["report"].get("next_action") == "manual_review"
    )
    most_common_failure = "none"
    max_count = 0
    for failure_class in FAILURE_CLASSES:
        count = failure_distribution[failure_class]
        if count > max_count:
            max_count = count
            most_common_failure = failure_class

    return {
        "batch_id": batch_id,
        "total_runs": total_runs,
        "passed_runs": passed_runs,
        "failed_runs": failed_runs,
        "failure_distribution": failure_distribution,
        "manual_review_count": manual_review_count,
        "most_common_failure": most_common_failure if max_count > 0 else "none",
        "notes": "batch_validated",
        "individual_reports": individual_reports,
    }


def write_batch_report(
    leads: list[dict[str, object]],
    *,
    store_path: Path | None = None,
    output_path: Path | None = None,
    reports_dir: Path | None = None,
) -> Path:
    report = run_real_lead_batch(
        leads,
        store_path=store_path,
        output_dir=reports_dir,
    )
    target = Path(output_path) if output_path is not None else ROOT_DIR / BATCH_REPORT_FILE
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return target
