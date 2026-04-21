import subprocess
from __future__ import annotations

import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from control.env_loader import load_env
from control.dev_runtime import run_in_dev_env

ENV_PATH = REPO_ROOT / ".env"
TASK_PATH = REPO_ROOT / "tasks" / "codex" / "task-google-doc.json"
ARTIFACT_PATH = REPO_ROOT / "artifacts" / "doc-101.json"
RUN_CODEX_TASK_PATH = REPO_ROOT / "scripts" / "run_codex_task.py"
REQUIRED_ENV_VARS = (
    "GOOGLE_CLIENT_ID",
    "GOOGLE_CLIENT_SECRET",
    "GOOGLE_REFRESH_TOKEN",
)
TASK_PAYLOAD = {
    "task_id": 101,
    "instruction": "Create a test Google Doc from Digital Foreman",
    "constraints": "Create only one new document",
    "success_criteria": "Google Doc is created and URL returned",
    "task_type": "external_write_google_doc",
    "title": "DF FIRST REAL TEST",
    "content": "This document was created by Digital Foreman system.",
}


def _load_required_env() -> None:
    if not ENV_PATH.is_file():
        raise FileNotFoundError(f"Missing env file: {ENV_PATH}")

    for key, value in load_env().items():
        os.environ[str(key)] = str(value)

    missing = [
        name
        for name in REQUIRED_ENV_VARS
        if not str(os.environ.get(name) or "").strip()
    ]
    if missing:
        raise RuntimeError(f"Missing required env keys: {', '.join(missing)}")


def _write_task_file() -> Path:
    TASK_PATH.parent.mkdir(parents=True, exist_ok=True)
    TASK_PATH.write_text(
        json.dumps(TASK_PAYLOAD, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return TASK_PATH


def _read_artifact_payload() -> dict[str, object] | None:
    if not ARTIFACT_PATH.is_file():
        return None

    try:
        return json.loads(ARTIFACT_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def _result_reason(
    result: subprocess.CompletedProcess[str], artifact_payload: dict[str, object] | None
) -> str:
    if isinstance(artifact_payload, dict):
        artifact_reason = str(artifact_payload.get("reason") or "").strip()
        if artifact_reason:
            return artifact_reason

    stderr = str(result.stderr or "").strip()
    if stderr:
        stderr_lines = [line.strip() for line in stderr.splitlines() if line.strip()]
        if stderr_lines:
            return stderr_lines[-1]

    stdout = str(result.stdout or "").strip()
    if stdout:
        stdout_lines = [line.strip() for line in stdout.splitlines() if line.strip()]
        if stdout_lines:
            return stdout_lines[-1]

    return "executor failed without a reported reason"


def main() -> int:
    try:
        _load_required_env()
        _write_task_file()
    except Exception as error:
        print("RESULT: FAILURE")
        print(f"REASON: {error}")
        return 1

    result = run_in_dev_env(
        ["python", str(RUN_CODEX_TASK_PATH), str(TASK_PATH)],
        cwd=REPO_ROOT,
        env=os.environ.copy(),
        capture_output=True,
        text=True,
        check=False,
    )
    artifact_payload = _read_artifact_payload()
    doc_url = ""
    if isinstance(artifact_payload, dict):
        doc_url = str(artifact_payload.get("url") or "").strip()

    if result.returncode == 0 and doc_url:
        print("RESULT: SUCCESS")
        print(f"GOOGLE_DOC_URL: {doc_url}")
        print(f"ARTIFACT_PATH: {ARTIFACT_PATH}")
        return 0

    print("RESULT: FAILURE")
    print(f"REASON: {_result_reason(result, artifact_payload)}")
    if doc_url:
        print(f"GOOGLE_DOC_URL: {doc_url}")
    if ARTIFACT_PATH.is_file():
        print(f"ARTIFACT_PATH: {ARTIFACT_PATH}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
