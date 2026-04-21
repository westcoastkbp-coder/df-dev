from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Iterable

from app.execution.paths import ROOT_DIR, STATE_DIR
from app.memory.canonical_memory import (
    CanonicalMemoryError,
    MemoryObject,
    build_audit_metadata,
    memory_object_from_mapping,
)
from app.memory.memory_lifecycle import ALLOWED_TRANSITIONS


CANONICAL_MEMORY_DB_FILE = STATE_DIR / "canonical_memory.sqlite3"
SQLITE_CONNECTION_TIMEOUT_SECONDS = 5.0
SQLITE_BUSY_TIMEOUT_MS = 5_000


class CanonicalMemoryStoreError(RuntimeError):
    """Raised when the canonical memory store cannot complete an operation."""


def _root_relative(path: Path) -> Path:
    return path if path.is_absolute() else ROOT_DIR / path


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _json_dumps(payload: object) -> str:
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _normalize_status_filters(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return (_normalize_text(value).lower(),) if _normalize_text(value) else ()
    if isinstance(value, Iterable):
        normalized: list[str] = []
        for item in value:
            text = _normalize_text(item).lower()
            if text and text not in normalized:
                normalized.append(text)
        return tuple(normalized)
    return ()


def _normalize_text_filters(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return (_normalize_text(value),) if _normalize_text(value) else ()
    if isinstance(value, Iterable):
        normalized: list[str] = []
        for item in value:
            text = _normalize_text(item)
            if text and text not in normalized:
                normalized.append(text)
        return tuple(normalized)
    return ()


class CanonicalMemoryStore:
    """Persistent canonical memory store kept separate from trace and runtime state stores."""

    def __init__(self, *, db_path: Path | None = None) -> None:
        self._db_path = _root_relative(db_path or CANONICAL_MEMORY_DB_FILE)

    @property
    def db_path(self) -> Path:
        return self._db_path

    def create_memory_object(
        self,
        memory_object: MemoryObject,
        *,
        supersede_prior: Iterable[MemoryObject] = (),
        promotion_gate_passed: bool = False,
    ) -> MemoryObject:
        if not promotion_gate_passed:
            raise CanonicalMemoryStoreError("direct_memory_write_rejected")
        record = memory_object.to_dict()
        prior_records = tuple(supersede_prior)
        try:
            with self._connect() as connection:
                for prior_record in prior_records:
                    prior_payload = prior_record.to_dict()
                    updated_prior = memory_object_from_mapping(
                        {
                            **prior_payload,
                            "status": "superseded",
                            "updated_at": record["updated_at"],
                            "superseded_by_memory_id": record["memory_id"],
                            "audit_metadata": build_audit_metadata(
                                operation_type="supersede",
                                source_type=record["source_type"],
                                source_ref=record["source_ref"],
                                timestamp=record["updated_at"],
                                prior_memory_id=prior_payload["memory_id"],
                                actor_scope=record["audit_metadata"].get("actor_scope"),
                                lifecycle_from=prior_payload["status"],
                                lifecycle_to="superseded",
                            ),
                        }
                    )
                    self._update_memory_object(connection, updated_prior)
                self._insert_memory_object(connection, memory_object)
        except sqlite3.IntegrityError as exc:
            raise CanonicalMemoryStoreError("memory object already exists") from exc
        except sqlite3.Error as exc:
            raise CanonicalMemoryStoreError(
                str(exc) or "canonical memory write failed"
            ) from exc
        return memory_object

    def seed_memory_object(self, memory_object: MemoryObject) -> MemoryObject:
        try:
            with self._connect() as connection:
                self._insert_memory_object(connection, memory_object)
        except sqlite3.IntegrityError as exc:
            raise CanonicalMemoryStoreError("memory object already exists") from exc
        except sqlite3.Error as exc:
            raise CanonicalMemoryStoreError(
                str(exc) or "canonical memory seed failed"
            ) from exc
        return memory_object

    def update_memory_object(self, memory_object: MemoryObject) -> MemoryObject:
        try:
            with self._connect() as connection:
                cursor = self._update_memory_object(connection, memory_object)
        except sqlite3.Error as exc:
            raise CanonicalMemoryStoreError(
                str(exc) or "canonical memory update failed"
            ) from exc
        if cursor.rowcount == 0:
            raise CanonicalMemoryStoreError("memory object does not exist")
        return memory_object

    def transition_memory_object(
        self,
        memory_id: object,
        *,
        status: object,
        operation_type: object,
        source_type: object,
        source_ref: object,
        timestamp: object,
        actor_scope: object | None = None,
        superseded_by_memory_id: object | None = None,
    ) -> MemoryObject:
        current = self.get_memory_object(memory_id)
        if current is None:
            raise CanonicalMemoryStoreError("memory object does not exist")
        normalized_status = _normalize_text(status).lower()
        allowed_next = ALLOWED_TRANSITIONS.get(current.status, frozenset())
        if normalized_status not in allowed_next:
            raise CanonicalMemoryStoreError(
                f"invalid_lifecycle_transition:{current.status}->{normalized_status}"
            )
        updated = memory_object_from_mapping(
            {
                **current.to_dict(),
                "status": normalized_status,
                "updated_at": _normalize_text(timestamp),
                "superseded_by_memory_id": superseded_by_memory_id,
                "audit_metadata": build_audit_metadata(
                    operation_type=operation_type,
                    source_type=source_type,
                    source_ref=source_ref,
                    timestamp=timestamp,
                    prior_memory_id=current.memory_id,
                    actor_scope=actor_scope,
                    lifecycle_from=current.status,
                    lifecycle_to=normalized_status,
                ),
            }
        )
        return self.update_memory_object(updated)

    def get_memory_object(self, memory_id: object) -> MemoryObject | None:
        normalized_memory_id = _normalize_text(memory_id)
        if not normalized_memory_id:
            return None
        try:
            with self._connect() as connection:
                row = connection.execute(
                    """
                    SELECT
                        memory_id,
                        memory_type,
                        domain_type,
                        owner_ref,
                        subject_ref,
                        content_summary,
                        structured_payload_json,
                        status,
                        created_at,
                        updated_at,
                        source_trace_id,
                        evidence_ref,
                        version,
                        confidence,
                        trust_level,
                        trust_class,
                        source_type,
                        source_ref,
                        audit_metadata_json,
                        superseded_by_memory_id,
                        previous_version_id,
                        conflict_key
                    FROM canonical_memory_objects
                    WHERE memory_id = ?
                    """,
                    (normalized_memory_id,),
                ).fetchone()
        except sqlite3.Error as exc:
            raise CanonicalMemoryStoreError(
                str(exc) or "canonical memory read failed"
            ) from exc
        if row is None:
            return None
        return self._memory_object_from_row(row)

    def search_memory_objects(
        self,
        *,
        query_text: object = "",
        domain_scope: object | None = None,
        domain_type: object | None = None,
        memory_type: object | None = None,
        owner_ref: object | None = None,
        subject_ref: object | None = None,
        status: object | None = None,
        trust_class: object | None = None,
        conflict_key: object | None = None,
        updated_since: object | None = None,
        limit: int = 20,
    ) -> list[MemoryObject]:
        sql = [
            """
            SELECT
                memory_id,
                memory_type,
                domain_type,
                owner_ref,
                subject_ref,
                content_summary,
                structured_payload_json,
                status,
                created_at,
                updated_at,
                source_trace_id,
                evidence_ref,
                version,
                confidence,
                trust_level,
                trust_class,
                source_type,
                source_ref,
                audit_metadata_json,
                superseded_by_memory_id,
                previous_version_id,
                conflict_key
            FROM canonical_memory_objects
            WHERE 1 = 1
            """
        ]
        parameters: list[object] = []
        normalized_domain = _normalize_text(domain_scope or domain_type).lower()
        normalized_memory_type = _normalize_text(memory_type).lower()
        normalized_owner_ref = _normalize_text(owner_ref)
        normalized_subject_ref = _normalize_text(subject_ref)
        normalized_status_filters = _normalize_status_filters(status)
        normalized_trust_classes = _normalize_text_filters(trust_class)
        normalized_conflict_key = _normalize_text(conflict_key)
        normalized_updated_since = _normalize_text(updated_since)
        normalized_query = _normalize_text(query_text).lower()

        if normalized_domain:
            sql.append("AND domain_type = ?")
            parameters.append(normalized_domain)
        if normalized_memory_type:
            sql.append("AND memory_type = ?")
            parameters.append(normalized_memory_type)
        if normalized_owner_ref:
            sql.append("AND owner_ref = ?")
            parameters.append(normalized_owner_ref)
        if normalized_subject_ref:
            sql.append("AND subject_ref = ?")
            parameters.append(normalized_subject_ref)
        if normalized_status_filters:
            placeholders = ", ".join("?" for _ in normalized_status_filters)
            sql.append(f"AND status IN ({placeholders})")
            parameters.extend(normalized_status_filters)
        if normalized_trust_classes:
            placeholders = ", ".join("?" for _ in normalized_trust_classes)
            sql.append(f"AND trust_class IN ({placeholders})")
            parameters.extend(normalized_trust_classes)
        if normalized_conflict_key:
            sql.append("AND conflict_key = ?")
            parameters.append(normalized_conflict_key)
        if normalized_updated_since:
            sql.append("AND updated_at >= ?")
            parameters.append(normalized_updated_since)
        if normalized_query:
            for token in normalized_query.split():
                sql.append(
                    """
                    AND LOWER(
                        COALESCE(content_summary, '') || ' ' ||
                        COALESCE(structured_payload_json, '') || ' ' ||
                        COALESCE(owner_ref, '') || ' ' ||
                        COALESCE(subject_ref, '') || ' ' ||
                        COALESCE(source_ref, '')
                    ) LIKE ?
                    """
                )
                parameters.append(f"%{token}%")
        sql.append("ORDER BY updated_at DESC, version DESC, memory_id ASC LIMIT ?")
        parameters.append(max(1, int(limit)))

        try:
            with self._connect() as connection:
                rows = connection.execute("\n".join(sql), tuple(parameters)).fetchall()
        except sqlite3.Error as exc:
            raise CanonicalMemoryStoreError(
                str(exc) or "canonical memory search failed"
            ) from exc
        return [self._memory_object_from_row(row) for row in rows]

    def list_memory_objects(
        self,
        *,
        domain_scope: object | None = None,
        domain_type: object | None = None,
        memory_type: object | None = None,
        status: object | None = None,
        owner_ref: object | None = None,
        subject_ref: object | None = None,
        limit: int = 100,
        trust_class: object | None = None,
        conflict_key: object | None = None,
        updated_since: object | None = None,
    ) -> list[MemoryObject]:
        return self.search_memory_objects(
            query_text="",
            domain_scope=domain_scope or domain_type,
            memory_type=memory_type,
            owner_ref=owner_ref,
            subject_ref=subject_ref,
            status=status,
            trust_class=trust_class,
            conflict_key=conflict_key,
            updated_since=updated_since,
            limit=limit,
        )

    def list_active_truths(
        self,
        *,
        domain_scope: object,
        owner_ref: object | None = None,
        subject_ref: object | None = None,
        memory_type: object | None = None,
        limit: int = 100,
    ) -> list[MemoryObject]:
        return self.list_memory_objects(
            domain_scope=domain_scope,
            owner_ref=owner_ref,
            subject_ref=subject_ref,
            memory_type=memory_type,
            status=("active",),
            limit=limit,
        )

    def get_supersession_chain(self, memory_id: object) -> list[MemoryObject]:
        current = self.get_memory_object(memory_id)
        if current is None:
            return []
        chain = [current]
        seen = {current.memory_id}
        while current.previous_version_id:
            previous = self.get_memory_object(current.previous_version_id)
            if previous is None or previous.memory_id in seen:
                break
            chain.append(previous)
            seen.add(previous.memory_id)
            current = previous
        return chain

    def trace_memory_lineage(self, memory_id: object) -> dict[str, object]:
        memory_object = self.get_memory_object(memory_id)
        if memory_object is None:
            raise CanonicalMemoryStoreError("memory object does not exist")
        chain = self.get_supersession_chain(memory_id)
        return {
            "memory_id": memory_object.memory_id,
            "memory_type": memory_object.memory_type,
            "domain_scope": memory_object.domain_scope,
            "source_trace_id": memory_object.source_trace_id,
            "evidence_ref": memory_object.evidence_ref,
            "version": memory_object.version,
            "status": memory_object.status,
            "lineage": [
                {
                    "memory_id": item.memory_id,
                    "version": item.version,
                    "status": item.status,
                    "source_trace_id": item.source_trace_id,
                    "evidence_ref": item.evidence_ref,
                }
                for item in chain
            ],
        }

    def detect_conflicts(
        self,
        *,
        domain_scope: object,
        owner_ref: object | None = None,
        subject_ref: object | None = None,
    ) -> list[dict[str, object]]:
        sql = [
            """
            SELECT
                conflict_key,
                SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active_count,
                COUNT(1) AS version_count,
                MIN(updated_at) AS first_seen_at,
                MAX(updated_at) AS last_seen_at
            FROM canonical_memory_objects
            WHERE domain_type = ?
              AND conflict_key IS NOT NULL
            """
        ]
        parameters: list[object] = [_normalize_text(domain_scope).lower()]
        if _normalize_text(owner_ref):
            sql.append("AND owner_ref = ?")
            parameters.append(_normalize_text(owner_ref))
        if _normalize_text(subject_ref):
            sql.append("AND subject_ref = ?")
            parameters.append(_normalize_text(subject_ref))
        sql.append(
            """
            GROUP BY conflict_key
            HAVING active_count > 1 OR version_count > 1
            ORDER BY conflict_key ASC
            """
        )
        try:
            with self._connect() as connection:
                rows = connection.execute("\n".join(sql), tuple(parameters)).fetchall()
        except sqlite3.Error as exc:
            raise CanonicalMemoryStoreError(
                str(exc) or "canonical memory conflict scan failed"
            ) from exc
        conflicts: list[dict[str, object]] = []
        for row in rows:
            conflict_key = str(row["conflict_key"])
            members = self.list_memory_objects(
                domain_scope=domain_scope,
                owner_ref=owner_ref,
                subject_ref=subject_ref,
                conflict_key=conflict_key,
                status=("active", "superseded", "deprecated", "archived"),
                limit=100,
            )
            conflicts.append(
                {
                    "conflict_key": conflict_key,
                    "active_count": int(row["active_count"]),
                    "version_count": int(row["version_count"]),
                    "first_seen_at": str(row["first_seen_at"]),
                    "last_seen_at": str(row["last_seen_at"]),
                    "memory_ids": [item.memory_id for item in members],
                }
            )
        return conflicts

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
            CREATE TABLE IF NOT EXISTS canonical_memory_objects (
                memory_id TEXT PRIMARY KEY,
                memory_type TEXT NOT NULL,
                domain_type TEXT NOT NULL,
                owner_ref TEXT,
                subject_ref TEXT,
                content_summary TEXT NOT NULL,
                structured_payload_json TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                source_trace_id TEXT NOT NULL DEFAULT '',
                evidence_ref TEXT NOT NULL DEFAULT '',
                version INTEGER NOT NULL DEFAULT 1,
                confidence REAL NOT NULL DEFAULT 1.0,
                trust_level TEXT NOT NULL,
                trust_class TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_ref TEXT NOT NULL,
                audit_metadata_json TEXT NOT NULL DEFAULT '{}',
                superseded_by_memory_id TEXT,
                previous_version_id TEXT,
                conflict_key TEXT
            )
            """
        )
        self._ensure_column(connection, "status", "TEXT NOT NULL DEFAULT 'active'")
        self._ensure_column(connection, "created_at", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(connection, "updated_at", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(connection, "source_trace_id", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(connection, "evidence_ref", "TEXT NOT NULL DEFAULT ''")
        self._ensure_column(connection, "version", "INTEGER NOT NULL DEFAULT 1")
        self._ensure_column(connection, "confidence", "REAL NOT NULL DEFAULT 1.0")
        self._ensure_column(
            connection, "audit_metadata_json", "TEXT NOT NULL DEFAULT '{}'"
        )
        self._ensure_column(connection, "superseded_by_memory_id", "TEXT")
        self._ensure_column(connection, "previous_version_id", "TEXT")
        self._ensure_column(connection, "conflict_key", "TEXT")
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_canonical_memory_domain_type
            ON canonical_memory_objects(domain_type, memory_type, status, updated_at)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_canonical_memory_owner_subject
            ON canonical_memory_objects(owner_ref, subject_ref, updated_at)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_canonical_memory_conflict_key
            ON canonical_memory_objects(conflict_key, status, version, updated_at)
            """
        )
        try:
            connection.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_canonical_memory_active_conflict_key
                ON canonical_memory_objects(conflict_key)
                WHERE conflict_key IS NOT NULL AND status = 'active'
                """
            )
        except sqlite3.Error:
            pass
        return connection

    def _ensure_column(
        self,
        connection: sqlite3.Connection,
        column_name: str,
        column_definition: str,
    ) -> None:
        existing_columns = {
            str(row["name"])
            for row in connection.execute(
                "PRAGMA table_info(canonical_memory_objects)"
            ).fetchall()
        }
        if column_name in existing_columns:
            return
        connection.execute(
            f"ALTER TABLE canonical_memory_objects ADD COLUMN {column_name} {column_definition}"
        )

    def _insert_memory_object(
        self,
        connection: sqlite3.Connection,
        memory_object: MemoryObject,
    ) -> None:
        record = memory_object.to_dict()
        connection.execute(
            """
            INSERT INTO canonical_memory_objects (
                memory_id,
                memory_type,
                domain_type,
                owner_ref,
                subject_ref,
                content_summary,
                structured_payload_json,
                status,
                created_at,
                updated_at,
                source_trace_id,
                evidence_ref,
                version,
                confidence,
                trust_level,
                trust_class,
                source_type,
                source_ref,
                audit_metadata_json,
                superseded_by_memory_id,
                previous_version_id,
                conflict_key
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record["memory_id"],
                record["memory_type"],
                record["domain_scope"],
                record["owner_ref"],
                record["subject_ref"],
                record["content_summary"],
                _json_dumps(record["structured_payload"]),
                record["status"],
                record["created_at"],
                record["updated_at"],
                record["source_trace_id"],
                record["evidence_ref"],
                record["version"],
                record["confidence"],
                record["trust_level"],
                record["trust_class"],
                record["source_type"],
                record["source_ref"],
                _json_dumps(record["audit_metadata"]),
                record["superseded_by_memory_id"],
                record["previous_version_id"],
                record["conflict_key"],
            ),
        )

    def _update_memory_object(
        self,
        connection: sqlite3.Connection,
        memory_object: MemoryObject,
    ) -> sqlite3.Cursor:
        record = memory_object.to_dict()
        return connection.execute(
            """
            UPDATE canonical_memory_objects
            SET
                memory_type = ?,
                domain_type = ?,
                owner_ref = ?,
                subject_ref = ?,
                content_summary = ?,
                structured_payload_json = ?,
                status = ?,
                created_at = ?,
                updated_at = ?,
                source_trace_id = ?,
                evidence_ref = ?,
                version = ?,
                confidence = ?,
                trust_level = ?,
                trust_class = ?,
                source_type = ?,
                source_ref = ?,
                audit_metadata_json = ?,
                superseded_by_memory_id = ?,
                previous_version_id = ?,
                conflict_key = ?
            WHERE memory_id = ?
            """,
            (
                record["memory_type"],
                record["domain_scope"],
                record["owner_ref"],
                record["subject_ref"],
                record["content_summary"],
                _json_dumps(record["structured_payload"]),
                record["status"],
                record["created_at"],
                record["updated_at"],
                record["source_trace_id"],
                record["evidence_ref"],
                record["version"],
                record["confidence"],
                record["trust_level"],
                record["trust_class"],
                record["source_type"],
                record["source_ref"],
                _json_dumps(record["audit_metadata"]),
                record["superseded_by_memory_id"],
                record["previous_version_id"],
                record["conflict_key"],
                record["memory_id"],
            ),
        )

    def _memory_object_from_row(self, row: sqlite3.Row) -> MemoryObject:
        try:
            return memory_object_from_mapping(
                {
                    "memory_id": str(row["memory_id"]),
                    "memory_type": str(row["memory_type"]),
                    "domain_scope": str(row["domain_type"]),
                    "owner_ref": row["owner_ref"],
                    "subject_ref": row["subject_ref"],
                    "content_summary": str(row["content_summary"]),
                    "structured_payload": json.loads(
                        str(row["structured_payload_json"])
                    ),
                    "status": str(row["status"]),
                    "created_at": str(row["created_at"]),
                    "updated_at": str(row["updated_at"]),
                    "source_trace_id": row["source_trace_id"],
                    "evidence_ref": row["evidence_ref"],
                    "version": row["version"],
                    "confidence": row["confidence"],
                    "trust_level": str(row["trust_level"]),
                    "trust_class": str(row["trust_class"]),
                    "source_type": str(row["source_type"]),
                    "source_ref": str(row["source_ref"]),
                    "audit_metadata": json.loads(str(row["audit_metadata_json"])),
                    "superseded_by_memory_id": row["superseded_by_memory_id"],
                    "previous_version_id": row["previous_version_id"],
                    "conflict_key": row["conflict_key"],
                }
            )
        except (
            CanonicalMemoryError,
            ValueError,
            TypeError,
            json.JSONDecodeError,
        ) as exc:
            raise CanonicalMemoryStoreError(
                "malformed canonical memory record"
            ) from exc
