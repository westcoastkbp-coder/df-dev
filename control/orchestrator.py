from pathlib import Path
import json
from datetime import datetime
import sys

ACTIVE_TASK_NAME = "BUILD_CONTROL_PLANE_V1"
STATE_PATH = Path("control/SYSTEM_STATE.json")
TASK_PATH = Path("control/ACTIVE_TASK.md")
REGISTRY_PATH = Path("tasks/TASK-0001.json")
AUDIT_PATH = Path("audit/audit_log.jsonl")

def log_event(status: str, message: str) -> None:
    entry = {
        "timestamp": datetime.now().isoformat(),
        "event": "orchestrator_run",
        "status": status,
        "message": message,
    }
    with open(AUDIT_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")

def load_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8")

def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))

def save_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

def verify_active_task(content: str) -> None:
    if not content:
        print("NO ACTIVE TASK")
        log_event("fail", "active task file missing")
        sys.exit(1)

    if ACTIVE_TASK_NAME not in content:
        print("TASK MISMATCH - STOP")
        log_event("blocked", "active task mismatch")
        sys.exit(1)

def run_step(state: dict) -> dict:
    current_step = state.get("current_step", "")

    print("=== CURRENT STEP ===")
    print(current_step)

    if current_step == "CREATE_BASE_CONTROL_FILES":
        print("=== STEP RESULT ===")
        print("STEP_CREATE_BASE_CONTROL_FILES_CONFIRMED")

        state["current_step"] = "CONTROL_PLANE_V1_READY"
        state["status"] = "ready_for_next_stage"
        state["last_updated_by"] = "orchestrator_v7"

        log_event("ok", "step CREATE_BASE_CONTROL_FILES executed")
        return state

    print("UNKNOWN STEP - STOP")
    log_event("blocked", f"unknown step: {current_step}")
    sys.exit(1)

def main() -> None:
    task_content = load_text(TASK_PATH)
    verify_active_task(task_content)

    state = load_json(STATE_PATH)
    registry = load_json(REGISTRY_PATH)

    print("=== ACTIVE TASK ===")
    print(task_content)

    print("=== SYSTEM STATE BEFORE ===")
    print(state)

    print("=== TASK REGISTRY ===")
    print(registry)

    updated_state = run_step(state)

    save_json(STATE_PATH, updated_state)

    print("=== SYSTEM STATE AFTER ===")
    print(updated_state)

if __name__ == "__main__":
    main()
