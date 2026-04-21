from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import sqlite3
import threading
import time
from collections.abc import Callable
from pathlib import Path

from app.execution.paths import ROOT_DIR, STATE_DIR, TASK_SYSTEM_FILE
from app.execution.task_schema import validate_task_contract, validate_task_lineage
from runtime.system_log import log_event

try:
    import msvcrt
except ImportError:  # pragma: no cover - Windows-only path.
    msvcrt = None

try:
    import fcntl
except ImportError:  # pragma: no cover - POSIX-only fallback.
    fcntl = None


TASK_STATE_DB_FILE = STATE_DIR / "task_state.sqlite3"
LEGACY_TASK_MEMORY_FILE = STATE_DIR / "task_memory.json"
MAX_MEMORY_ENTRIES = 100
EXECUTION_LEDGER_STATUS_CLAIMED = "claimed"
EXECUTION_LEDGER_STATUS_EXECUTED = "executed"
TERMINAL_TASK_STATUSES = {"COMPLETED", "FAILED"}
PRE_EXECUTION_TASK_STATUSES = {"AWAITING_APPROVAL", "CREATED", "VALIDATED"}
SQLITE_CONNECTION_TIMEOUT_SECONDS = 5.0
SQLITE_BUSY_TIMEOUT_MS = 5_000
SQLITE_WRITE_RETRY_ATTEMPTS = 3
SQLITE_WRITE_RETRY_DELAY_SECONDS = 0.05
SQLITE_FILE_LOCK_TIMEOUT_SECONDS = 5.0

_WRITE_LOCKS: dict[str, threading.RLock] = {}
_WRITE_LOCKS_GUARD = threading.Lock()
_INITIALIZED_DATABASES: set[str] = set()
_INITIALIZED_DATABASES_GUARD = threading.Lock()


class StatePersistenceError(RuntimeError):
    def __init__(self, signal: dict[str, object]) -> None:
        self.signal = dict(signal)
        super().__init__(
            json.dumps(self.signal, ensure_ascii=True, separators=(",", ":"))
        )


class InvalidPersistedTaskStateError(RuntimeError):
    def __init__(self, *, task_id: object, reason: str) -> None:
        self.signal = {
            "status": "invalid_state",
            "task_id": str(task_id or "").strip(),
            "reason": str(reason or "").strip() or "persisted task state invalid",
        }
        super().__init__(self.signal["reason"])


def _root_relative(path: Path) -> Path:
    return path if path.is_absolute() else ROOT_DIR / path


def default_db_path() -> Path:
    return _root_relative(TASK_STATE_DB_FILE)


def db_path_for(store_path: Path | None = None) -> Path:
    if store_path is None:
        return default_db_path()
    target = _root_relative(store_path)
    if target.suffix.lower() == ".sqlite3":
        return target
    return target.with_suffix(".sqlite3")


def _database_identity(db_path: Path) -> str:
    return str(db_path.resolve())


def _lock_file_path(db_path: Path) -> Path:
    return db_path.with_suffix(f"{db_path.suffix}.lock")


def _write_lock_for(db_path: Path) -> threading.RLock:
    identity = _database_identity(db_path)
    with _WRITE_LOCKS_GUARD:
        lock = _WRITE_LOCKS.get(identity)
        if lock is None:
            lock = threading.RLock()
            _WRITE_LOCKS[identity] = lock
        return lock


def _acquire_file_lock(lock_path: Path, *, timeout_seconds: float) -> object:
    deadline = time.monotonic() + timeout_seconds
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    handle = lock_path.open("a+b")
    handle.seek(0, 2)
    if handle.tell() == 0:
        handle.write(b"0")
        handle.flush()
    while True:
        try:
            handle.seek(0)
            if msvcrt is not None:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            elif (
                fcntl is not None
            ):  # pragma: no cover - Windows is the primary runtime.
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            return handle
        except OSError:
            if time.monotonic() >= deadline:
                handle.close()
                raise TimeoutError(f"timed out acquiring sqlite lock: {lock_path}")
            time.sleep(SQLITE_WRITE_RETRY_DELAY_SECONDS)


def _release_file_lock(handle: object) -> None:
    if msvcrt is not None:
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
    elif fcntl is not None:  # pragma: no cover - Windows is the primary runtime.
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    handle.close()


@contextmanager
def _database_write_lock(db_path: Path) -> object:
    lock = _write_lock_for(db_path)
    acquired = lock.acquire(timeout=SQLITE_FILE_LOCK_TIMEOUT_SECONDS)
    if not acquired:
        signal = build_state_persist_failure(
            task_id="",
            operation="acquire_database_write_lock",
        )
        _log_persist_failure(signal)
        raise StatePersistenceError(signal)
    file_handle = None
    try:
        file_handle = _acquire_file_lock(
            _lock_file_path(db_path),
            timeout_seconds=SQLITE_FILE_LOCK_TIMEOUT_SECONDS,
        )
        yield
    finally:
        if file_handle is not None:
            _release_file_lock(file_handle)
        lock.release()


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    print("OPEN CONNECTION")
    connection = sqlite3.connect(
        str(db_path),
        timeout=SQLITE_CONNECTION_TIMEOUT_SECONDS,
    )
    connection.isolation_level = None
    connection.row_factory = sqlite3.Row
    connection.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
    try:
        connection.execute("PRAGMA journal_mode=WAL")
    except sqlite3.OperationalError:
        connection.execute("PRAGMA journal_mode=DELETE")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA foreign_keys=OFF")
    return connection


def _ensure_schema(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS Task (
            task_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            descriptor TEXT NOT NULL,
            result TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            control_fields TEXT NOT NULL,
            memory_ref TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS ExecutionLedger (
            execution_key TEXT PRIMARY KEY,
            task_id TEXT NOT NULL,
            action_type TEXT NOT NULL,
            status TEXT NOT NULL,
            action_result TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS task_versions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id TEXT NOT NULL,
            version_number INTEGER NOT NULL,
            lineage_root_task_id TEXT NOT NULL,
            lineage_branch_key TEXT NOT NULL,
            status TEXT NOT NULL,
            approval_status TEXT NOT NULL,
            event_type TEXT NOT NULL,
            execution_key TEXT NOT NULL,
            created_at TEXT NOT NULL,
            descriptor TEXT NOT NULL,
            UNIQUE(task_id, version_number)
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS execution_branches (
            branch_key TEXT PRIMARY KEY,
            task_id TEXT NOT NULL UNIQUE,
            lineage_root_task_id TEXT NOT NULL,
            latest_execution_key TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )


def build_state_persist_failure(
    *,
    task_id: object,
    operation: str,
    storage: str = "sqlite",
) -> dict[str, object]:
    return {
        "status": "state_persist_failed",
        "task_id": str(task_id or "").strip(),
        "operation": str(operation or "").strip(),
        "storage": str(storage or "").strip() or "sqlite",
    }


def _log_persist_failure(signal: dict[str, object]) -> None:
    log_event("storage", json.dumps(signal, ensure_ascii=True, separators=(",", ":")))


def _raise_persist_failure(
    *,
    operation_name: str,
    task_id: object = "",
    exc: sqlite3.Error,
) -> None:
    signal = build_state_persist_failure(task_id=task_id, operation=operation_name)
    _log_persist_failure(signal)
    raise StatePersistenceError(signal) from exc


def _is_retryable_sqlite_error(exc: BaseException | None) -> bool:
    if not isinstance(exc, sqlite3.Error):
        return False
    message = str(exc).strip().lower()
    return any(
        token in message
        for token in (
            "disk i/o error",
            "database is locked",
            "database table is locked",
            "unable to open database file",
            "locking protocol",
            "database busy",
        )
    )


def _log_persist_retry(
    *,
    operation_name: str,
    task_id: object = "",
    attempt_number: int,
    max_attempts: int,
    exc: BaseException | None,
) -> None:
    log_event(
        "storage",
        {
            "status": "state_persist_retry",
            "task_id": str(task_id or "").strip(),
            "operation": str(operation_name or "").strip(),
            "attempt": attempt_number,
            "max_attempts": max_attempts,
            "error": str(exc or "").strip(),
        },
        task_id=task_id,
        status="retry",
    )


def _execute_transaction_once(
    connection: sqlite3.Connection,
    operation: Callable[[sqlite3.Connection], object],
    *,
    operation_name: str,
    task_id: object = "",
) -> object:
    try:
        connection.execute("BEGIN IMMEDIATE")
        result = operation(connection)
        connection.execute("COMMIT")
        return result
    except sqlite3.Error as exc:
        try:
            connection.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        _raise_persist_failure(operation_name=operation_name, task_id=task_id, exc=exc)
    except Exception:
        try:
            connection.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        raise


def _run_write_operation(
    operation: Callable[[sqlite3.Connection], object],
    *,
    store_path: Path | None = None,
    operation_name: str,
    task_id: object = "",
) -> object:
    target = initialize_database(store_path)
    last_error: StatePersistenceError | None = None
    for attempt_index in range(SQLITE_WRITE_RETRY_ATTEMPTS):
        with _database_write_lock(target):
            connection = _connect(target)
            try:
                print("BEFORE WRITE")
                result = _execute_transaction_once(
                    connection,
                    operation,
                    operation_name=operation_name,
                    task_id=task_id,
                )
                print("AFTER WRITE")
                return result
            except StatePersistenceError as exc:
                print("AFTER WRITE")
                last_error = exc
                if (
                    attempt_index + 1 >= SQLITE_WRITE_RETRY_ATTEMPTS
                    or not _is_retryable_sqlite_error(exc.__cause__)
                ):
                    raise
                _log_persist_retry(
                    operation_name=operation_name,
                    task_id=task_id,
                    attempt_number=attempt_index + 1,
                    max_attempts=SQLITE_WRITE_RETRY_ATTEMPTS,
                    exc=exc.__cause__,
                )
            finally:
                print("BEFORE CLOSE")
                connection.close()
                print("AFTER CLOSE")
        time.sleep(SQLITE_WRITE_RETRY_DELAY_SECONDS * (attempt_index + 1))
    if last_error is not None:
        raise last_error
    raise RuntimeError(
        f"write operation failed without explicit error: {operation_name}"
    )


def _json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"))


def _json_value(value: object, default: object) -> object:
    if not isinstance(value, str) or not value.strip():
        return default
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return default
    return decoded


def _now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _normalize_task_id(value: object) -> str:
    return str(value or "").strip()


def _lineage_root_task_id(task_data: dict[str, object]) -> str:
    parent_task_id = _normalize_task_id(task_data.get("parent_task_id"))
    return parent_task_id or _normalize_task_id(task_data.get("task_id"))


def _lineage_branch_key(task_data: dict[str, object]) -> str:
    return f"task::{_normalize_task_id(task_data.get('task_id'))}"


def _task_version_event_type(
    task_data: dict[str, object], existing_task: dict[str, object] | None
) -> str:
    current_status = str(task_data.get("status", "")).strip()
    current_approval = str(task_data.get("approval_status", "")).strip()
    if existing_task is None:
        return "created"
    previous_status = str(existing_task.get("status", "")).strip()
    previous_approval = str(existing_task.get("approval_status", "")).strip()
    if previous_approval != current_approval:
        return "approval_updated"
    if previous_status != current_status:
        return "status_updated"
    return "task_saved"


def _append_task_version(
    connection: sqlite3.Connection,
    *,
    task_data: dict[str, object],
    existing_task: dict[str, object] | None,
    timestamp: str,
) -> None:
    task_id = _normalize_task_id(task_data.get("task_id"))
    if not task_id:
        return
    row = connection.execute(
        """
        SELECT COALESCE(MAX(version_number), 0) AS max_version
        FROM task_versions
        WHERE task_id = ?
        """,
        (task_id,),
    ).fetchone()
    next_version = int(row["max_version"]) + 1 if row is not None else 1
    connection.execute(
        """
        INSERT INTO task_versions (
            task_id,
            version_number,
            lineage_root_task_id,
            lineage_branch_key,
            status,
            approval_status,
            event_type,
            execution_key,
            created_at,
            descriptor
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            task_id,
            next_version,
            _lineage_root_task_id(task_data),
            _lineage_branch_key(task_data),
            str(task_data.get("status", "")).strip(),
            str(task_data.get("approval_status", "")).strip(),
            _task_version_event_type(task_data, existing_task),
            str(task_data.get("execution_key", "")).strip(),
            timestamp,
            _json_text(dict(task_data)),
        ),
    )


def _upsert_execution_branch(
    connection: sqlite3.Connection,
    *,
    task_data: dict[str, object],
    timestamp: str,
    status: str | None = None,
    execution_key: str | None = None,
) -> None:
    task_id = _normalize_task_id(task_data.get("task_id"))
    if not task_id:
        return
    branch_key = _lineage_branch_key(task_data)
    lineage_root_task_id = _lineage_root_task_id(task_data)
    effective_status = str(status or task_data.get("status") or "").strip() or "CREATED"
    effective_execution_key = str(
        execution_key or task_data.get("execution_key") or ""
    ).strip()
    connection.execute(
        """
        INSERT INTO execution_branches (
            branch_key,
            task_id,
            lineage_root_task_id,
            latest_execution_key,
            status,
            created_at,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(branch_key) DO UPDATE SET
            lineage_root_task_id = CASE
                WHEN execution_branches.lineage_root_task_id != '' THEN execution_branches.lineage_root_task_id
                ELSE excluded.lineage_root_task_id
            END,
            latest_execution_key = CASE
                WHEN excluded.latest_execution_key != '' THEN excluded.latest_execution_key
                ELSE execution_branches.latest_execution_key
            END,
            status = excluded.status,
            updated_at = excluded.updated_at
        """,
        (
            branch_key,
            task_id,
            lineage_root_task_id,
            effective_execution_key,
            effective_status,
            timestamp,
            timestamp,
        ),
    )


def _build_invalid_state_task(
    *,
    task_id: object,
    row_status: object,
    created_at: object,
    updated_at: object,
    descriptor: dict[str, object] | None,
    reason: str,
) -> dict[str, object]:
    base = dict(descriptor or {})
    normalized_task_id = str(task_id or "").strip()
    timestamp = str(updated_at or created_at or _now()).strip() or _now()
    history = (
        list(base.get("history", [])) if isinstance(base.get("history"), list) else []
    )
    history.append(
        {
            "timestamp": timestamp,
            "event": "invalid_state_blocked",
            "from_status": str(base.get("status") or row_status or "").strip(),
            "to_status": "FAILED",
            "details": {
                "error_code": "invalid_state",
                "reason": str(reason).strip(),
            },
        }
    )
    return {
        "task_contract_version": 1,
        "task_id": normalized_task_id,
        "created_at": str(created_at or timestamp).strip() or timestamp,
        "last_updated_at": timestamp,
        "intent": str(base.get("intent") or "invalid_state").strip() or "invalid_state",
        "payload": dict(base.get("payload", {}) or {}),
        "status": "FAILED",
        "notes": list(base.get("notes", []))
        if isinstance(base.get("notes"), list)
        else [],
        "history": history,
        "interaction_id": str(base.get("interaction_id", "")).strip(),
        "job_id": str(base.get("job_id") or normalized_task_id).strip(),
        "trace_id": str(base.get("trace_id") or normalized_task_id).strip(),
        "task_type": str(base.get("task_type", "")).strip(),
        "parent_task_id": str(base.get("parent_task_id", "")).strip(),
        "parent_task_type": str(base.get("parent_task_type", "")).strip(),
        "execution_mode": str(base.get("execution_mode", "")).strip(),
        "execution_location": str(base.get("execution_location", "")).strip(),
        "started_at": str(base.get("started_at", "")).strip(),
        "completed_at": "",
        "failed_at": timestamp,
        "error": _normalized_invalid_state_reason(reason),
        "execution_key": str(base.get("execution_key", "")).strip(),
        "result": {
            "status": "failed",
            "error_code": "invalid_state",
            "error_message": _normalized_invalid_state_reason(reason),
            "persisted_status": str(row_status or "").strip(),
        },
    }


def _normalized_invalid_state_reason(reason: object) -> str:
    normalized_reason = str(reason).strip()
    if "status must be one of:" not in normalized_reason:
        return normalized_reason
    return normalized_reason.replace("AWAITING_APPROVAL, ", "")


def _persist_invalid_task_state(
    connection: sqlite3.Connection,
    *,
    row: sqlite3.Row,
    reason: str,
) -> dict[str, object]:
    descriptor_value = _json_value(row["descriptor"], {})
    descriptor = dict(descriptor_value) if isinstance(descriptor_value, dict) else None
    failed_task = _build_invalid_state_task(
        task_id=row["task_id"],
        row_status=row["status"],
        created_at=row["created_at"],
        updated_at=_now(),
        descriptor=descriptor,
        reason=reason,
    )
    failed_row = _task_row(failed_task)
    connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            """
            UPDATE Task
            SET status = ?, descriptor = ?, result = ?, updated_at = ?
            WHERE task_id = ? AND memory_ref = ''
            """,
            (
                failed_row["status"],
                failed_row["descriptor"],
                failed_row["result"],
                failed_row["updated_at"],
                failed_row["task_id"],
            ),
        )
        connection.execute("COMMIT")
    except sqlite3.Error as exc:
        try:
            connection.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        signal = build_state_persist_failure(
            task_id=row["task_id"],
            operation="persist_invalid_task_state",
        )
        _log_persist_failure(signal)
        raise StatePersistenceError(signal) from exc
    log_event(
        "validation",
        {
            "task_id": failed_task["task_id"],
            "status": "invalid_state",
            "reason": str(reason).strip(),
        },
        task_id=failed_task["task_id"],
        status="invalid_state",
    )
    return failed_task


def _validated_task_from_row(
    connection: sqlite3.Connection,
    row: sqlite3.Row,
) -> dict[str, object] | None:
    descriptor_raw = row["descriptor"]
    descriptor_value = _json_value(descriptor_raw, {})
    if not isinstance(descriptor_value, dict):
        return _persist_invalid_task_state(
            connection,
            row=row,
            reason="persisted descriptor is not valid JSON object",
        )
    descriptor = dict(descriptor_value)
    row_status = str(row["status"] or "").strip()
    try:
        validated = validate_task_contract(descriptor)
    except ValueError as exc:
        return _persist_invalid_task_state(
            connection,
            row=row,
            reason=str(exc),
        )
    if row_status != str(validated.get("status", "")).strip():
        return _persist_invalid_task_state(
            connection,
            row=row,
            reason=(
                "persisted status mismatch: "
                f"row={row_status or '(empty)'} "
                f"descriptor={str(validated.get('status', '')).strip() or '(empty)'}"
            ),
        )
    return validated


def build_execution_key(*, task_id: object, action_type: object) -> str:
    normalized_task_id = str(task_id or "").strip()
    normalized_action_type = str(action_type or "").strip().upper()
    if not normalized_task_id or not normalized_action_type:
        return ""
    payload = f"{normalized_task_id}:{normalized_action_type}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _task_row(task_data: dict[str, object]) -> dict[str, str]:
    now_value = str(
        task_data.get("last_updated_at")
        or task_data.get("updated_at")
        or task_data.get("created_at")
        or _now()
    ).strip()
    created_at = str(task_data.get("created_at") or now_value).strip()
    stored = dict(task_data)
    stored.setdefault("last_updated_at", now_value)
    status = str(stored.get("status", "")).strip() or "created"
    result_value = stored.get("result")
    return {
        "task_id": str(stored.get("task_id", "")).strip(),
        "status": status,
        "descriptor": _json_text(stored),
        "result": _json_text(result_value if isinstance(result_value, dict) else {}),
        "created_at": created_at,
        "updated_at": now_value,
        "control_fields": _json_text({}),
        "memory_ref": "",
    }


def _task_status_value(task_data: dict[str, object]) -> str:
    return str(task_data.get("status", "")).strip().upper()


def _task_execution_key(task_data: dict[str, object]) -> str:
    return str(task_data.get("execution_key", "")).strip()


def _should_keep_current_task_state(
    current_task: dict[str, object],
    incoming_task: dict[str, object],
) -> bool:
    current_status = _task_status_value(current_task)
    incoming_status = _task_status_value(incoming_task)
    current_execution_key = _task_execution_key(current_task)
    incoming_execution_key = _task_execution_key(incoming_task)

    if current_status in TERMINAL_TASK_STATUSES:
        if current_execution_key and current_execution_key == incoming_execution_key:
            return True
        if incoming_status != current_status:
            return True

    if current_status == "EXECUTING" and incoming_status in PRE_EXECUTION_TASK_STATUSES:
        if not current_execution_key or current_execution_key == incoming_execution_key:
            return True

    return False


def task_row(task_data: dict[str, object]) -> dict[str, str]:
    return _task_row(task_data)


def _memory_row(
    entry: dict[str, object], *, ordinal: int, updated_at: str
) -> dict[str, str]:
    logical_task_id = str(entry.get("task_id", "")).strip()
    row_task_id = f"{logical_task_id}::memory::{ordinal:08d}"
    status = str(entry.get("status", "")).strip() or "completed"
    created_at = str(entry.get("created_at", "")).strip() or updated_at
    return {
        "task_id": row_task_id,
        "status": status,
        "descriptor": _json_text(dict(entry)),
        "result": _json_text({}),
        "created_at": created_at,
        "updated_at": updated_at,
        "control_fields": _json_text({}),
        "memory_ref": logical_task_id,
    }


def _task_select_sql() -> str:
    return """
        descriptor
      , status
      , task_id
      , created_at
      , updated_at
    """


def _read_task_rows(
    connection: sqlite3.Connection,
    *,
    memory_ref: str,
    task_id: str | None = None,
) -> list[sqlite3.Row]:
    if task_id is None:
        return connection.execute(
            f"""
            SELECT {_task_select_sql()}
            FROM Task
            WHERE memory_ref = ?
            ORDER BY created_at ASC, task_id ASC
            """,
            (memory_ref,),
        ).fetchall()
    row = connection.execute(
        f"""
        SELECT {_task_select_sql()}
        FROM Task
        WHERE task_id = ? AND memory_ref = ?
        """,
        (task_id, memory_ref),
    ).fetchone()
    return [row] if row is not None else []


def _validated_tasks_from_rows(
    connection: sqlite3.Connection,
    rows: list[sqlite3.Row],
) -> list[dict[str, object]]:
    tasks: list[dict[str, object]] = []
    for row in rows:
        validated = _validated_task_from_row(connection, row)
        if isinstance(validated, dict):
            tasks.append(dict(validated))
    return tasks


def _load_legacy_entries(json_path: Path) -> list[dict[str, object]]:
    try:
        data = json.loads(json_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(data, list):
        return []
    return [dict(item) for item in data if isinstance(item, dict)]


def _bootstrap_legacy_memory(connection: sqlite3.Connection) -> None:
    json_path = _root_relative(LEGACY_TASK_MEMORY_FILE)
    if not json_path.exists():
        return
    existing = connection.execute(
        "SELECT COUNT(1) AS count FROM Task WHERE memory_ref != ''"
    ).fetchone()
    if existing and int(existing["count"]) > 0:
        return
    entries = _load_legacy_entries(json_path)
    if not entries:
        return
    timestamp = _now()

    def _write_entries(active_connection: sqlite3.Connection) -> None:
        for index, entry in enumerate(entries, start=1):
            row = _memory_row(entry, ordinal=index, updated_at=timestamp)
            active_connection.execute(
                """
                INSERT OR REPLACE INTO Task (
                    task_id, status, descriptor, result, created_at,
                    updated_at, control_fields, memory_ref
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["task_id"],
                    row["status"],
                    row["descriptor"],
                    row["result"],
                    row["created_at"],
                    row["updated_at"],
                    row["control_fields"],
                    row["memory_ref"],
                ),
            )

    _execute_transaction_once(
        connection,
        _write_entries,
        operation_name="bootstrap_legacy_memory",
    )


def _bootstrap_legacy_tasks(
    connection: sqlite3.Connection, store_path: Path | None
) -> None:
    target = _root_relative(store_path or TASK_SYSTEM_FILE)
    if not target.exists() or target.suffix.lower() == ".sqlite3":
        return
    existing = connection.execute(
        "SELECT COUNT(1) AS count FROM Task WHERE memory_ref = ''"
    ).fetchone()
    if existing and int(existing["count"]) > 0:
        return
    entries = _load_legacy_entries(target)
    if not entries:
        return

    def _write_entries(active_connection: sqlite3.Connection) -> None:
        for entry in entries:
            row = _task_row(entry)
            if not row["task_id"]:
                continue
            active_connection.execute(
                """
                INSERT OR REPLACE INTO Task (
                    task_id, status, descriptor, result, created_at,
                    updated_at, control_fields, memory_ref
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["task_id"],
                    row["status"],
                    row["descriptor"],
                    row["result"],
                    row["created_at"],
                    row["updated_at"],
                    row["control_fields"],
                    row["memory_ref"],
                ),
            )

    _execute_transaction_once(
        connection,
        _write_entries,
        operation_name="bootstrap_legacy_tasks",
    )


def initialize_database(store_path: Path | None = None) -> Path:
    target = db_path_for(store_path)
    identity = _database_identity(target)
    with _INITIALIZED_DATABASES_GUARD:
        if identity in _INITIALIZED_DATABASES and target.exists():
            return target
    with _database_write_lock(target):
        with _INITIALIZED_DATABASES_GUARD:
            if identity in _INITIALIZED_DATABASES and target.exists():
                return target
        connection = _connect(target)
        try:
            _ensure_schema(connection)
            _bootstrap_legacy_memory(connection)
            _bootstrap_legacy_tasks(connection, store_path)
        finally:
            print("BEFORE CLOSE")
            connection.close()
            print("AFTER CLOSE")
        with _INITIALIZED_DATABASES_GUARD:
            _INITIALIZED_DATABASES.add(identity)
    return target


def read_all_tasks(store_path: Path | None = None) -> list[dict[str, object]]:
    target = initialize_database(store_path)
    connection = _connect(target)
    try:
        rows = _read_task_rows(connection, memory_ref="")
        tasks: list[dict[str, object]] = []
        for row in rows:
            descriptor_value = _json_value(row["descriptor"], {})
            if not isinstance(descriptor_value, dict):
                raise ValueError("persisted descriptor is not valid JSON object")
            validated = validate_task_contract(dict(descriptor_value))
            row_status = str(row["status"] or "").strip()
            validated_status = str(validated.get("status", "")).strip()
            if row_status != validated_status:
                raise ValueError(
                    "persisted status mismatch: "
                    f"row={row_status or '(empty)'} "
                    f"descriptor={validated_status or '(empty)'}"
                )
            tasks.append(dict(validated))
        return tasks
    except sqlite3.Error as exc:
        _raise_persist_failure(operation_name="read_all_tasks", exc=exc)
    finally:
        connection.close()


def write_task(
    task_data: dict[str, object], store_path: Path | None = None
) -> dict[str, object]:
    validated_task = validate_task_contract(task_data)

    def _write(active_connection: sqlite3.Connection) -> dict[str, object]:
        existing_tasks = _validated_tasks_from_rows(
            active_connection,
            _read_task_rows(active_connection, memory_ref=""),
        )
        existing_task = next(
            (
                dict(task)
                for task in existing_tasks
                if _normalize_task_id(task.get("task_id"))
                == _normalize_task_id(validated_task.get("task_id"))
            ),
            None,
        )
        parent_task_id = str(
            validated_task.get("parent_task_id")
            or dict(validated_task.get("payload", {}) or {}).get("parent_task_id")
            or ""
        ).strip()
        parent_task = None
        if parent_task_id:
            for task in existing_tasks:
                if str(task.get("task_id", "")).strip() == parent_task_id:
                    parent_task = dict(task)
                    break
        candidate_task = validate_task_lineage(
            dict(validated_task),
            parent_task=parent_task,
            existing_tasks=existing_tasks,
        )
        row = _task_row(candidate_task)
        timestamp = row["updated_at"]
        current_rows = _read_task_rows(
            active_connection,
            memory_ref="",
            task_id=row["task_id"],
        )
        current_task = (
            _validated_task_from_row(active_connection, current_rows[0])
            if current_rows
            else None
        )
        if isinstance(current_task, dict) and _should_keep_current_task_state(
            current_task,
            candidate_task,
        ):
            return dict(current_task)
        active_connection.execute(
            """
            INSERT OR REPLACE INTO Task (
                task_id, status, descriptor, result, created_at,
                updated_at, control_fields, memory_ref
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["task_id"],
                row["status"],
                row["descriptor"],
                row["result"],
                row["created_at"],
                row["updated_at"],
                row["control_fields"],
                row["memory_ref"],
            ),
        )
        _append_task_version(
            active_connection,
            task_data=candidate_task,
            existing_task=current_task
            if isinstance(current_task, dict)
            else existing_task,
            timestamp=timestamp,
        )
        _upsert_execution_branch(
            active_connection,
            task_data=candidate_task,
            timestamp=timestamp,
        )
        return dict(candidate_task)

    persisted_task = _run_write_operation(
        _write,
        store_path=store_path,
        operation_name="write_task",
        task_id=validated_task.get("task_id"),
    )
    return dict(persisted_task if isinstance(persisted_task, dict) else validated_task)


def read_task(task_id: str, store_path: Path | None = None) -> dict[str, object] | None:
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id:
        return None
    target = initialize_database(store_path)
    connection = _connect(target)
    try:
        rows = _read_task_rows(
            connection,
            memory_ref="",
            task_id=normalized_task_id,
        )
        if not rows:
            return None
        validated = _validated_task_from_row(connection, rows[0])
        return dict(validated) if isinstance(validated, dict) else None
    except sqlite3.Error as exc:
        _raise_persist_failure(
            operation_name="read_task",
            task_id=normalized_task_id,
            exc=exc,
        )
    finally:
        connection.close()


def read_memory_entries() -> list[dict[str, object]]:
    target = initialize_database()
    connection = _connect(target)
    try:
        rows = connection.execute(
            """
            SELECT descriptor
            FROM Task
            WHERE memory_ref != ''
            ORDER BY updated_at ASC, task_id ASC
            """
        ).fetchall()
    except sqlite3.Error as exc:
        _raise_persist_failure(operation_name="read_memory_entries", exc=exc)
    finally:
        connection.close()
    entries: list[dict[str, object]] = []
    for row in rows:
        value = _json_value(row["descriptor"], {})
        if isinstance(value, dict):
            entries.append(dict(value))
    return entries


def append_memory_entry(entry: dict[str, object]) -> dict[str, object]:
    def _append(active_connection: sqlite3.Connection) -> None:
        rows = active_connection.execute(
            """
            SELECT task_id
            FROM Task
            WHERE memory_ref != ''
            ORDER BY updated_at ASC, task_id ASC
            """
        ).fetchall()
        ordinal = len(rows) + 1
        updated_at = _now()
        row = _memory_row(entry, ordinal=ordinal, updated_at=updated_at)
        active_connection.execute(
            """
            INSERT INTO Task (
                task_id, status, descriptor, result, created_at,
                updated_at, control_fields, memory_ref
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["task_id"],
                row["status"],
                row["descriptor"],
                row["result"],
                row["created_at"],
                row["updated_at"],
                row["control_fields"],
                row["memory_ref"],
            ),
        )
        overflow = max(0, len(rows) + 1 - MAX_MEMORY_ENTRIES)
        if overflow > 0:
            delete_ids = [memory_row["task_id"] for memory_row in rows[:overflow]]
            active_connection.executemany(
                "DELETE FROM Task WHERE task_id = ?",
                [(task_id,) for task_id in delete_ids],
            )

    _run_write_operation(
        _append,
        operation_name="append_memory_entry",
        task_id=entry.get("task_id"),
    )
    return dict(entry)


def replace_memory_entries(entries: list[dict[str, object]]) -> list[dict[str, object]]:
    trimmed_entries = [dict(entry) for entry in entries[-MAX_MEMORY_ENTRIES:]]

    def _replace(active_connection: sqlite3.Connection) -> None:
        active_connection.execute("DELETE FROM Task WHERE memory_ref != ''")
        updated_at = _now()
        for index, entry in enumerate(trimmed_entries, start=1):
            row = _memory_row(entry, ordinal=index, updated_at=updated_at)
            active_connection.execute(
                """
                INSERT INTO Task (
                    task_id, status, descriptor, result, created_at,
                    updated_at, control_fields, memory_ref
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["task_id"],
                    row["status"],
                    row["descriptor"],
                    row["result"],
                    row["created_at"],
                    row["updated_at"],
                    row["control_fields"],
                    row["memory_ref"],
                ),
            )

    _run_write_operation(
        _replace,
        operation_name="replace_memory_entries",
    )
    return trimmed_entries


def get_task_history(task_id: str) -> list[dict[str, object]]:
    normalized_task_id = str(task_id or "").strip()
    if not normalized_task_id:
        return []
    target = initialize_database()
    connection = _connect(target)
    try:
        rows = connection.execute(
            """
            SELECT descriptor
            FROM Task
            WHERE memory_ref = ?
            ORDER BY updated_at ASC, task_id ASC
            """,
            (normalized_task_id,),
        ).fetchall()
    except sqlite3.Error as exc:
        _raise_persist_failure(
            operation_name="get_task_history",
            task_id=normalized_task_id,
            exc=exc,
        )
    finally:
        connection.close()
    history: list[dict[str, object]] = []
    for row in rows:
        value = _json_value(row["descriptor"], {})
        if isinstance(value, dict):
            history.append(dict(value))
    return history


def run_in_transaction(
    operation: Callable[[sqlite3.Connection], object],
    *,
    store_path: Path | None = None,
    operation_name: str = "run_in_transaction",
    task_id: object = "",
) -> object:
    return _run_write_operation(
        operation,
        store_path=store_path,
        operation_name=operation_name,
        task_id=task_id,
    )


def read_execution_record(
    execution_key: object,
    *,
    store_path: Path | None = None,
) -> dict[str, object] | None:
    normalized_execution_key = str(execution_key or "").strip()
    if not normalized_execution_key:
        return None
    target = initialize_database(store_path)
    connection = _connect(target)
    try:
        row = connection.execute(
            """
            SELECT execution_key, task_id, action_type, status, action_result, created_at, updated_at
            FROM ExecutionLedger
            WHERE execution_key = ?
            """,
            (normalized_execution_key,),
        ).fetchone()
    except sqlite3.Error as exc:
        _raise_persist_failure(
            operation_name="read_execution_record",
            exc=exc,
        )
    finally:
        connection.close()
    if row is None:
        return None
    action_result = _json_value(row["action_result"], {})
    return {
        "execution_key": str(row["execution_key"]),
        "task_id": str(row["task_id"]),
        "action_type": str(row["action_type"]),
        "status": str(row["status"]),
        "action_result": dict(action_result) if isinstance(action_result, dict) else {},
        "created_at": str(row["created_at"]),
        "updated_at": str(row["updated_at"]),
    }


def write_execution_record(
    *,
    execution_key: object,
    task_id: object,
    action_type: object,
    action_result: dict[str, object],
    store_path: Path | None = None,
) -> dict[str, object]:
    normalized_execution_key = str(execution_key or "").strip()
    normalized_task_id = str(task_id or "").strip()
    normalized_action_type = str(action_type or "").strip().upper()
    if not normalized_execution_key:
        raise ValueError("execution_key must not be empty")
    if not normalized_task_id:
        raise ValueError("task_id must not be empty")
    if not normalized_action_type:
        raise ValueError("action_type must not be empty")
    existing = read_execution_record(normalized_execution_key, store_path=store_path)
    if existing is not None:
        return existing
    timestamp = _now()

    def _write(active_connection: sqlite3.Connection) -> None:
        active_connection.execute(
            """
            INSERT OR IGNORE INTO ExecutionLedger (
                execution_key, task_id, action_type, status, action_result, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_execution_key,
                normalized_task_id,
                normalized_action_type,
                EXECUTION_LEDGER_STATUS_EXECUTED,
                _json_text(action_result),
                timestamp,
                timestamp,
            ),
        )

    _run_write_operation(
        _write,
        store_path=store_path,
        operation_name="write_execution_record",
        task_id=normalized_task_id,
    )
    return read_execution_record(normalized_execution_key, store_path=store_path) or {
        "execution_key": normalized_execution_key,
        "task_id": normalized_task_id,
        "action_type": normalized_action_type,
        "status": EXECUTION_LEDGER_STATUS_EXECUTED,
        "action_result": dict(action_result),
        "created_at": timestamp,
        "updated_at": timestamp,
    }


def claim_execution_record(
    *,
    execution_key: object,
    task_id: object,
    action_type: object,
    store_path: Path | None = None,
) -> dict[str, object]:
    normalized_execution_key = str(execution_key or "").strip()
    normalized_task_id = str(task_id or "").strip()
    normalized_action_type = str(action_type or "").strip().upper()
    if not normalized_execution_key:
        raise ValueError("execution_key must not be empty")
    if not normalized_task_id:
        raise ValueError("task_id must not be empty")
    if not normalized_action_type:
        raise ValueError("action_type must not be empty")
    timestamp = _now()
    claimed = False

    def _claim(active_connection: sqlite3.Connection) -> None:
        nonlocal claimed
        cursor = active_connection.execute(
            """
            INSERT OR IGNORE INTO ExecutionLedger (
                execution_key, task_id, action_type, status, action_result, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized_execution_key,
                normalized_task_id,
                normalized_action_type,
                EXECUTION_LEDGER_STATUS_CLAIMED,
                _json_text({}),
                timestamp,
                timestamp,
            ),
        )
        claimed = cursor.rowcount == 1
        _upsert_execution_branch(
            active_connection,
            task_data={
                "task_id": normalized_task_id,
                "parent_task_id": "",
                "status": "EXECUTING",
                "execution_key": normalized_execution_key,
            },
            timestamp=timestamp,
            status="EXECUTING",
            execution_key=normalized_execution_key,
        )

    _run_write_operation(
        _claim,
        store_path=store_path,
        operation_name="claim_execution_record",
        task_id=normalized_task_id,
    )
    record = read_execution_record(normalized_execution_key, store_path=store_path)
    return {
        "claimed": claimed,
        "record": (
            record
            or {
                "execution_key": normalized_execution_key,
                "task_id": normalized_task_id,
                "action_type": normalized_action_type,
                "status": EXECUTION_LEDGER_STATUS_CLAIMED,
                "action_result": {},
                "created_at": timestamp,
                "updated_at": timestamp,
            }
        ),
    }


def complete_execution_record(
    *,
    execution_key: object,
    action_result: dict[str, object],
    store_path: Path | None = None,
) -> dict[str, object]:
    normalized_execution_key = str(execution_key or "").strip()
    if not normalized_execution_key:
        raise ValueError("execution_key must not be empty")
    timestamp = _now()

    def _complete(active_connection: sqlite3.Connection) -> None:
        cursor = active_connection.execute(
            """
            UPDATE ExecutionLedger
            SET status = ?, action_result = ?, updated_at = ?
            WHERE execution_key = ? AND status = ?
            """,
            (
                EXECUTION_LEDGER_STATUS_EXECUTED,
                _json_text(action_result),
                timestamp,
                normalized_execution_key,
                EXECUTION_LEDGER_STATUS_CLAIMED,
            ),
        )
        if cursor.rowcount != 1:
            existing_row = active_connection.execute(
                """
                SELECT status, action_result
                FROM ExecutionLedger
                WHERE execution_key = ?
                """,
                (normalized_execution_key,),
            ).fetchone()
            if existing_row is None:
                raise ValueError("execution claim missing for completion")
            existing_result = _json_value(existing_row["action_result"], {})
            if (
                str(existing_row["status"] or "").strip()
                == EXECUTION_LEDGER_STATUS_EXECUTED
                and isinstance(existing_result, dict)
                and existing_result
            ):
                return
            raise ValueError("execution claim missing for completion")
        _upsert_execution_branch(
            active_connection,
            task_data={
                "task_id": str(action_result.get("task_id", "")).strip(),
                "parent_task_id": "",
                "status": str(action_result.get("status", "")).strip().upper(),
                "execution_key": normalized_execution_key,
            },
            timestamp=timestamp,
            status=(
                str(action_result.get("status", "")).strip().upper()
                or EXECUTION_LEDGER_STATUS_EXECUTED.upper()
            ),
            execution_key=normalized_execution_key,
        )

    _run_write_operation(
        _complete,
        store_path=store_path,
        operation_name="complete_execution_record",
        task_id=str(action_result.get("task_id", "")).strip(),
    )
    return read_execution_record(normalized_execution_key, store_path=store_path) or {
        "execution_key": normalized_execution_key,
        "task_id": str(action_result.get("task_id", "")).strip(),
        "action_type": str(action_result.get("action_type", "")).strip(),
        "status": EXECUTION_LEDGER_STATUS_EXECUTED,
        "action_result": dict(action_result),
        "created_at": timestamp,
        "updated_at": timestamp,
    }
