from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

from app.execution.paths import ROOT_DIR, STATE_DIR
from runtime.system_log import log_event


WORKFLOW_STATE_DB_FILE = STATE_DIR / "ownerbox_workflow_state.sqlite3"
SQLITE_CONNECTION_TIMEOUT_SECONDS = 5.0
SQLITE_BUSY_TIMEOUT_MS = 5_000
MAX_TERMINAL_WORKFLOW_RECORDS = 200
TERMINAL_WORKFLOW_STATUSES = frozenset(
    {"completed", "failed", "rejected", "partial_failure"}
)


def _root_relative(path: Path) -> Path:
    return path if path.is_absolute() else ROOT_DIR / path


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _json_dumps(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _log_store_failure(
    *, code: str, workflow_id: str, operation: str, reason: str
) -> None:
    log_event(
        "storage",
        {
            "store": "ownerbox_workflow_state",
            "status": "failed",
            "code": code,
            "workflow_id": workflow_id,
            "operation": operation,
            "reason": reason,
        },
        task_id=workflow_id or None,
        status="failed",
    )


@dataclass(frozen=True, slots=True)
class PersistedWorkflowState:
    workflow_id: str
    workflow_type: str
    owner_id: str
    workflow_status: str
    current_step_index: int
    snapshot: dict[str, object]
    created_at: str
    updated_at: str


class WorkflowStateStoreError(RuntimeError):
    def __init__(
        self,
        *,
        code: str,
        workflow_id: object = "",
        operation: str,
        reason: str,
    ) -> None:
        self.code = _normalize_text(code) or "persistence_error"
        self.workflow_id = _normalize_text(workflow_id)
        self.operation = _normalize_text(operation) or "unknown"
        self.reason = _normalize_text(reason) or "workflow persistence failed"
        super().__init__(self.reason)


class WorkflowStateStore:
    def __init__(
        self,
        *,
        db_path: Path | None = None,
        max_terminal_records: int = MAX_TERMINAL_WORKFLOW_RECORDS,
    ) -> None:
        self._db_path = _root_relative(db_path or WORKFLOW_STATE_DB_FILE)
        self._max_terminal_records = max(1, int(max_terminal_records))

    def save_state(self, snapshot: Mapping[str, object]) -> PersistedWorkflowState:
        normalized = self._normalize_snapshot(snapshot)
        try:
            with self._connect() as connection:
                connection.execute(
                    """
                    INSERT INTO WorkflowState (
                        workflow_id,
                        workflow_type,
                        owner_id,
                        workflow_status,
                        current_step_index,
                        snapshot_json,
                        created_at,
                        updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(workflow_id) DO UPDATE SET
                        workflow_type = excluded.workflow_type,
                        owner_id = excluded.owner_id,
                        workflow_status = excluded.workflow_status,
                        current_step_index = excluded.current_step_index,
                        snapshot_json = excluded.snapshot_json,
                        updated_at = excluded.updated_at
                    """,
                    (
                        normalized.workflow_id,
                        normalized.workflow_type,
                        normalized.owner_id,
                        normalized.workflow_status,
                        normalized.current_step_index,
                        _json_dumps(normalized.snapshot),
                        normalized.created_at,
                        normalized.updated_at,
                    ),
                )
                self._prune_terminal_records(connection)
        except sqlite3.Error as exc:
            self._raise_error(
                code="persistence_error",
                workflow_id=normalized.workflow_id,
                operation="save_state",
                reason=str(exc) or "sqlite write failed",
            )
        return normalized

    def load_state(self, workflow_id: object) -> PersistedWorkflowState | None:
        normalized_workflow_id = _normalize_text(workflow_id)
        if not normalized_workflow_id:
            return None
        try:
            with self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT
                        workflow_id,
                        workflow_type,
                        owner_id,
                        workflow_status,
                        current_step_index,
                        snapshot_json,
                        created_at,
                        updated_at
                    FROM WorkflowState
                    WHERE workflow_id = ?
                    """,
                    (normalized_workflow_id,),
                ).fetchone()
        except sqlite3.Error as exc:
            self._raise_error(
                code="persistence_error",
                workflow_id=normalized_workflow_id,
                operation="load_state",
                reason=str(exc) or "sqlite read failed",
            )
        if row is None:
            return None
        return self._state_from_row(
            row, workflow_id=normalized_workflow_id, operation="load_state"
        )

    def list_states(self) -> tuple[PersistedWorkflowState, ...]:
        try:
            with self._connect() as connection:
                rows = connection.execute(
                    """
                    SELECT
                        workflow_id,
                        workflow_type,
                        owner_id,
                        workflow_status,
                        current_step_index,
                        snapshot_json,
                        created_at,
                        updated_at
                    FROM WorkflowState
                    ORDER BY updated_at ASC, workflow_id ASC
                    """
                ).fetchall()
        except sqlite3.Error as exc:
            self._raise_error(
                code="persistence_error",
                workflow_id="",
                operation="list_states",
                reason=str(exc) or "sqlite list failed",
            )
        return tuple(
            self._state_from_row(
                row, workflow_id=str(row["workflow_id"]), operation="list_states"
            )
            for row in rows
        )

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
            CREATE TABLE IF NOT EXISTS WorkflowState (
                workflow_id TEXT PRIMARY KEY,
                workflow_type TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                workflow_status TEXT NOT NULL,
                current_step_index INTEGER NOT NULL,
                snapshot_json TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_workflow_state_updated_at
            ON WorkflowState(updated_at)
            """
        )
        return connection

    def _prune_terminal_records(self, connection: sqlite3.Connection) -> None:
        rows = connection.execute(
            """
            SELECT workflow_id
            FROM WorkflowState
            WHERE workflow_status IN (?, ?, ?, ?)
            ORDER BY updated_at DESC, workflow_id DESC
            """,
            tuple(sorted(TERMINAL_WORKFLOW_STATUSES)),
        ).fetchall()
        if len(rows) <= self._max_terminal_records:
            return
        for row in rows[self._max_terminal_records :]:
            connection.execute(
                "DELETE FROM WorkflowState WHERE workflow_id = ?",
                (str(row["workflow_id"]),),
            )

    def _state_from_row(
        self,
        row: sqlite3.Row,
        *,
        workflow_id: str,
        operation: str,
    ) -> PersistedWorkflowState:
        try:
            snapshot = json.loads(str(row["snapshot_json"]))
        except json.JSONDecodeError as exc:
            self._raise_error(
                code="state_corrupted",
                workflow_id=workflow_id,
                operation=operation,
                reason=str(exc) or "persisted workflow snapshot was malformed",
            )
        return self._normalize_snapshot(
            {
                "workflow": dict(snapshot.get("workflow", {})),
                "steps": list(snapshot.get("steps", [])),
                "runtime": dict(snapshot.get("runtime", {})),
                "current_step_index": snapshot.get(
                    "current_step_index", row["current_step_index"]
                ),
            },
            workflow_id=workflow_id,
            workflow_status=str(row["workflow_status"]),
            workflow_type=str(row["workflow_type"]),
            owner_id=str(row["owner_id"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    def _normalize_snapshot(
        self,
        snapshot: Mapping[str, object],
        *,
        workflow_id: str | None = None,
        workflow_type: str | None = None,
        owner_id: str | None = None,
        workflow_status: str | None = None,
        created_at: str | None = None,
        updated_at: str | None = None,
    ) -> PersistedWorkflowState:
        if not isinstance(snapshot, Mapping):
            self._raise_error(
                code="state_corrupted",
                workflow_id=workflow_id or "",
                operation="normalize_snapshot",
                reason="workflow snapshot must be a mapping",
            )
        workflow = snapshot.get("workflow")
        steps = snapshot.get("steps")
        runtime = snapshot.get("runtime")
        if not isinstance(workflow, Mapping):
            self._raise_error(
                code="state_corrupted",
                workflow_id=workflow_id or "",
                operation="normalize_snapshot",
                reason="workflow snapshot missing workflow payload",
            )
        if not isinstance(steps, list) or any(
            not isinstance(step, Mapping) for step in steps
        ):
            self._raise_error(
                code="state_corrupted",
                workflow_id=workflow_id or "",
                operation="normalize_snapshot",
                reason="workflow snapshot missing step payloads",
            )
        if not isinstance(runtime, Mapping):
            self._raise_error(
                code="state_corrupted",
                workflow_id=workflow_id or "",
                operation="normalize_snapshot",
                reason="workflow snapshot missing runtime payload",
            )
        resolved_workflow_id = workflow_id or _normalize_text(
            workflow.get("workflow_id")
        )
        resolved_workflow_type = workflow_type or _normalize_text(
            workflow.get("workflow_type")
        )
        resolved_owner_id = owner_id or _normalize_text(workflow.get("owner_id"))
        resolved_status = (
            workflow_status or _normalize_text(workflow.get("status")).lower()
        )
        resolved_created_at = created_at or _normalize_text(workflow.get("created_at"))
        resolved_updated_at = (
            updated_at
            or _normalize_text(workflow.get("updated_at"))
            or resolved_created_at
        )
        if (
            not resolved_workflow_id
            or not resolved_workflow_type
            or not resolved_owner_id
            or not resolved_status
        ):
            self._raise_error(
                code="state_corrupted",
                workflow_id=resolved_workflow_id,
                operation="normalize_snapshot",
                reason="workflow snapshot missing required identifiers",
            )
        current_step_index = snapshot.get("current_step_index")
        try:
            resolved_current_step_index = int(current_step_index)
        except (TypeError, ValueError):
            resolved_current_step_index = self._derive_current_step_index(
                current_step_id=_normalize_text(workflow.get("current_step_id")),
                steps=steps,
            )
        normalized_snapshot = {
            "workflow": dict(workflow),
            "steps": [dict(step) for step in steps],
            "runtime": dict(runtime),
            "current_step_index": resolved_current_step_index,
        }
        try:
            _json_dumps(normalized_snapshot)
        except (TypeError, ValueError) as exc:
            self._raise_error(
                code="state_corrupted",
                workflow_id=resolved_workflow_id,
                operation="normalize_snapshot",
                reason=str(exc) or "workflow snapshot was not JSON serializable",
            )
        return PersistedWorkflowState(
            workflow_id=resolved_workflow_id,
            workflow_type=resolved_workflow_type,
            owner_id=resolved_owner_id,
            workflow_status=resolved_status,
            current_step_index=max(-1, resolved_current_step_index),
            snapshot=normalized_snapshot,
            created_at=resolved_created_at,
            updated_at=resolved_updated_at,
        )

    def _derive_current_step_index(
        self,
        *,
        current_step_id: str,
        steps: list[Mapping[str, object]],
    ) -> int:
        if current_step_id:
            for step in steps:
                if _normalize_text(step.get("step_id")) != current_step_id:
                    continue
                try:
                    return int(step.get("sequence_index", -1))
                except (TypeError, ValueError):
                    return -1
        return -1

    def _raise_error(
        self,
        *,
        code: str,
        workflow_id: str,
        operation: str,
        reason: str,
    ) -> None:
        _log_store_failure(
            code=code,
            workflow_id=workflow_id,
            operation=operation,
            reason=reason,
        )
        raise WorkflowStateStoreError(
            code=code,
            workflow_id=workflow_id,
            operation=operation,
            reason=reason,
        )
