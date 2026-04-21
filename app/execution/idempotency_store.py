from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from app.execution.action_contract import validate_action_result_contract
from app.execution.paths import ROOT_DIR, STATE_DIR
from runtime.system_log import log_event


IDEMPOTENCY_DB_FILE = STATE_DIR / "idempotency.sqlite3"
SQLITE_CONNECTION_TIMEOUT_SECONDS = 5.0
SQLITE_BUSY_TIMEOUT_MS = 5_000
MAX_TERMINAL_RECORDS = 1_000


def _root_relative(path: Path) -> Path:
    return path if path.is_absolute() else ROOT_DIR / path


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _json_dumps(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _log_store_failure(*, code: str, action_id: str, operation: str, reason: str) -> None:
    log_event(
        "storage",
        {
            "store": "idempotency",
            "status": "failed",
            "code": code,
            "action_id": action_id,
            "operation": operation,
            "reason": reason,
        },
        task_id=action_id or None,
        status="failed",
    )


@dataclass(frozen=True, slots=True)
class PersistedIdempotencyRecord:
    action_id: str
    idempotency_key: str
    action_type: str
    execution_status: str
    result: dict[str, object]
    created_at: str
    updated_at: str


class IdempotencyStoreError(RuntimeError):
    def __init__(
        self,
        *,
        code: str,
        action_id: object = "",
        operation: str,
        reason: str,
    ) -> None:
        self.code = _normalize_text(code) or "persistence_error"
        self.action_id = _normalize_text(action_id)
        self.operation = _normalize_text(operation) or "unknown"
        self.reason = _normalize_text(reason) or "idempotency store failure"
        super().__init__(self.reason)


class IdempotencyStore:
    def __init__(
        self,
        *,
        db_path: Path | None = None,
        max_terminal_records: int = MAX_TERMINAL_RECORDS,
    ) -> None:
        self._db_path = _root_relative(db_path or IDEMPOTENCY_DB_FILE)
        self._max_terminal_records = max(1, int(max_terminal_records))

    def get(self, *, action_id: object, idempotency_key: object) -> PersistedIdempotencyRecord | None:
        normalized_action_id = _normalize_text(action_id)
        normalized_key = _normalize_text(idempotency_key)
        if not normalized_key:
            return None
        try:
            with self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT
                        action_id,
                        idempotency_key,
                        action_type,
                        execution_status,
                        result_json,
                        created_at,
                        updated_at
                    FROM IdempotencyRecord
                    WHERE idempotency_key = ?
                    """,
                    (normalized_key,),
                ).fetchone()
        except sqlite3.Error as exc:
            self._raise_error(
                code="persistence_error",
                action_id=normalized_action_id,
                operation="get",
                reason=str(exc) or "sqlite read failed",
            )
        if row is None:
            return None
        return self._record_from_row(row, action_id=normalized_action_id, operation="get")

    def record(
        self,
        *,
        action_id: object,
        idempotency_key: object,
        action_type: object,
        execution_status: object,
        result: Mapping[str, object],
    ) -> PersistedIdempotencyRecord:
        normalized_action_id = _normalize_text(action_id)
        normalized_key = _normalize_text(idempotency_key)
        normalized_action_type = _normalize_text(action_type).upper()
        normalized_status = _normalize_text(execution_status).lower()
        if not normalized_action_id or not normalized_key or not normalized_action_type or not normalized_status:
            self._raise_error(
                code="persistence_error",
                action_id=normalized_action_id,
                operation="record",
                reason="missing idempotency persistence fields",
            )
        normalized_result = validate_action_result_contract(dict(result))
        created_at = _normalize_text(normalized_result.get("timestamp"))
        updated_at = created_at
        result_json = _json_dumps(normalized_result)
        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO IdempotencyRecord (
                        action_id,
                        idempotency_key,
                        action_type,
                        execution_status,
                        result_json,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(idempotency_key) DO UPDATE SET
                        action_id = excluded.action_id,
                        action_type = excluded.action_type,
                        execution_status = excluded.execution_status,
                        result_json = excluded.result_json,
                        updated_at = excluded.updated_at
                    """,
                    (
                        normalized_action_id,
                        normalized_key,
                        normalized_action_type,
                        normalized_status,
                        result_json,
                        created_at,
                        updated_at,
                    ),
                )
                self._prune_terminal_records(connection)
                row = connection.execute(
                    """
                    SELECT
                        action_id,
                        idempotency_key,
                        action_type,
                        execution_status,
                        result_json,
                        created_at,
                        updated_at
                    FROM IdempotencyRecord
                    WHERE idempotency_key = ?
                    """,
                    (normalized_key,),
                ).fetchone()
        except sqlite3.Error as exc:
            self._raise_error(
                code="persistence_error",
                action_id=normalized_action_id,
                operation="record",
                reason=str(exc) or "sqlite write failed",
            )
        if row is None:
            self._raise_error(
                code="persistence_error",
                action_id=normalized_action_id,
                operation="record",
                reason="persisted idempotency record was not readable after write",
            )
        return self._record_from_row(row, action_id=normalized_action_id, operation="record")

    def _connect(self) -> sqlite3.Connection:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(
            str(self._db_path),
            timeout=SQLITE_CONNECTION_TIMEOUT_SECONDS,
        )
        connection.row_factory = sqlite3.Row
        connection.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
        try:
            connection.execute("PRAGMA journal_mode=WAL")
        except sqlite3.OperationalError:
            connection.execute("PRAGMA journal_mode=DELETE")
        connection.execute("PRAGMA synchronous=FULL")
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS IdempotencyRecord (
                idempotency_key TEXT PRIMARY KEY,
                action_id TEXT NOT NULL,
                action_type TEXT NOT NULL,
                execution_status TEXT NOT NULL,
                result_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_idempotency_record_updated_at
            ON IdempotencyRecord(updated_at)
            """
        )
        return connection

    def _prune_terminal_records(self, connection: sqlite3.Connection) -> None:
        overflow = connection.execute(
            """
            SELECT COUNT(1) AS count
            FROM IdempotencyRecord
            """
        ).fetchone()
        if overflow is None:
            return
        total_count = int(overflow["count"])
        if total_count <= self._max_terminal_records:
            return
        delete_count = total_count - self._max_terminal_records
        rows = connection.execute(
            """
            SELECT idempotency_key
            FROM IdempotencyRecord
            ORDER BY updated_at ASC, idempotency_key ASC
            LIMIT ?
            """,
            (delete_count,),
        ).fetchall()
        if not rows:
            return
        connection.executemany(
            "DELETE FROM IdempotencyRecord WHERE idempotency_key = ?",
            [(str(row["idempotency_key"]),) for row in rows],
        )

    def _record_from_row(
        self,
        row: sqlite3.Row,
        *,
        action_id: str,
        operation: str,
    ) -> PersistedIdempotencyRecord:
        try:
            result_payload = json.loads(str(row["result_json"]))
        except json.JSONDecodeError as exc:
            self._raise_error(
                code="state_corrupted",
                action_id=action_id or str(row["action_id"]),
                operation=operation,
                reason=str(exc) or "persisted idempotency result was malformed",
            )
        try:
            normalized_result = validate_action_result_contract(result_payload)
        except Exception as exc:  # pragma: no cover - defensive validation path.
            self._raise_error(
                code="state_corrupted",
                action_id=action_id or str(row["action_id"]),
                operation=operation,
                reason=str(exc) or "persisted idempotency result was invalid",
            )
        return PersistedIdempotencyRecord(
            action_id=str(row["action_id"]),
            idempotency_key=str(row["idempotency_key"]),
            action_type=str(row["action_type"]),
            execution_status=str(row["execution_status"]),
            result=normalized_result,
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    def _raise_error(
        self,
        *,
        code: str,
        action_id: str,
        operation: str,
        reason: str,
    ) -> None:
        _log_store_failure(
            code=code,
            action_id=action_id,
            operation=operation,
            reason=reason,
        )
        raise IdempotencyStoreError(
            code=code,
            action_id=action_id,
            operation=operation,
            reason=reason,
        )
