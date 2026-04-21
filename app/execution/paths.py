from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
APP_DIR = ROOT_DIR / "app"
DATA_DIR = ROOT_DIR / "data"
RUNTIME_DIR = Path("runtime")
TOOLS_DIR = ROOT_DIR / "tools"
LOGS_DIR = RUNTIME_DIR / "logs"
STATE_DIR = RUNTIME_DIR / "state"
ENV_DIR = ROOT_DIR / "env"
OUTPUT_DIR = RUNTIME_DIR / "out"

TASKS_FILE = DATA_DIR / "tasks.json"
TASK_SYSTEM_FILE = DATA_DIR / "task_system.json"
MEMORY_FILE = DATA_DIR / "memory.json"
CONTACTS_FILE = DATA_DIR / "contacts.json"
CONTROLLED_EXECUTIONS_FILE = DATA_DIR / "controlled_executions.json"
BOX_SESSIONS_FILE = DATA_DIR / "box_sessions.json"
VOICE_CALL_SESSIONS_FILE = DATA_DIR / "voice_call_sessions.json"
VOICE_TRACE_EVENTS_FILE = DATA_DIR / "voice_trace_events.json"
