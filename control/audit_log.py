from pathlib import Path
import json
from datetime import datetime

LOG_PATH = Path(__file__).resolve().with_name("audit_log.jsonl")


def build_audit_entry(
    module: str,
    status: str,
    local_test: str,
    review: str,
    git: dict
) -> dict:
    return {
        "module": module,
        "status": status,
        "local_test": local_test,
        "review": review,
        "git": git
    }


def log_execution(entry: dict):
    payload = dict(entry)
    payload["time"] = datetime.utcnow().isoformat()
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def read_audit_log(limit: int = 50):
    if not LOG_PATH.exists():
        return []
    lines = LOG_PATH.read_text(encoding="utf-8").splitlines()
    rows = []
    for line in lines:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except (TypeError, ValueError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows[-limit:]
