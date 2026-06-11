from __future__ import annotations

import asyncio
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fala.schema_migrations import (
    RUNTIME_SCHEMA_VERSION,
    runtime_schema_migration_rows,
)
from fala.models import (
    CombinedProjection,
    OperatorAuditEvent,
    ProcessClaim,
    ProcessEvent,
    ProcessInput,
    ProcessOutput,
    ProcessStatus,
    RuntimeDocument,
    RuntimeDocumentStatus,
    RuntimeRun,
    RuntimeStreamCheckpoint,
    RuntimeStreamChunk,
    RuntimeWorkerHeartbeat,
)


class SQLiteStateStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=30.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_schema(self) -> None:
        with closing(self._connect()) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS runtime_schema_migrations (
                  version INTEGER PRIMARY KEY,
                  migration_id TEXT NOT NULL UNIQUE,
                  description TEXT NOT NULL,
                  checksum TEXT NOT NULL,
                  applied_at TEXT NOT NULL,
                  payload TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS process_runs (
                  id TEXT PRIMARY KEY,
                  status TEXT NOT NULL,
                  title TEXT,
                  outcome TEXT,
                  outcome_reason TEXT,
                  config TEXT NOT NULL,
                  metadata TEXT NOT NULL,
                  summary TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  started_at TEXT,
                  finished_at TEXT,
                  payload TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_process_runs_updated_at
                  ON process_runs(updated_at);

                CREATE TABLE IF NOT EXISTS operator_audit_events (
                  id TEXT PRIMARY KEY,
                  ts TEXT NOT NULL,
                  actor TEXT,
                  source TEXT,
                  action TEXT NOT NULL,
                  run_id TEXT,
                  document_id TEXT,
                  process_id TEXT,
                  target TEXT,
                  payload TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_operator_audit_events_run_ts
                  ON operator_audit_events(run_id, ts, id);

                CREATE INDEX IF NOT EXISTS idx_operator_audit_events_ts
                  ON operator_audit_events(ts, id);

                CREATE TABLE IF NOT EXISTS process_worker_heartbeats (
                  run_id TEXT NOT NULL,
                  worker_id TEXT NOT NULL,
                  pipeline_id TEXT,
                  process_id TEXT,
                  adapter_kind TEXT,
                  status TEXT NOT NULL,
                  last_seen_at TEXT NOT NULL,
                  payload TEXT NOT NULL,
                  PRIMARY KEY (run_id, worker_id)
                );

                CREATE INDEX IF NOT EXISTS idx_process_worker_heartbeats_run_seen
                  ON process_worker_heartbeats(run_id, last_seen_at);

                CREATE TABLE IF NOT EXISTS process_documents (
                  run_id TEXT NOT NULL,
                  document_id TEXT NOT NULL,
                  pipeline_id TEXT,
                  title TEXT,
                  document_type TEXT,
                  relation TEXT,
                  media_type TEXT,
                  source_uri TEXT,
                  parent_document_id TEXT,
                  parent_process_id TEXT,
                  status TEXT NOT NULL,
                  metadata TEXT NOT NULL,
                  summary TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  payload TEXT NOT NULL,
                  PRIMARY KEY (run_id, document_id)
                );

                CREATE INDEX IF NOT EXISTS idx_process_documents_run_status
                  ON process_documents(run_id, status);

                CREATE INDEX IF NOT EXISTS idx_process_documents_run_pipeline
                  ON process_documents(run_id, pipeline_id);

                CREATE INDEX IF NOT EXISTS idx_process_documents_run_type
                  ON process_documents(run_id, document_type);

                CREATE TABLE IF NOT EXISTS process_events (
                  id TEXT PRIMARY KEY,
                  run_id TEXT NOT NULL,
                  document_id TEXT NOT NULL,
                  process_id TEXT,
                  type TEXT NOT NULL,
                  ts TEXT NOT NULL,
                  payload TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_process_events_run_doc_ts
                  ON process_events(run_id, document_id, ts);

                CREATE INDEX IF NOT EXISTS idx_process_events_run_doc_process_ts_id
                  ON process_events(run_id, document_id, process_id, ts, id);

                CREATE TABLE IF NOT EXISTS process_statuses (
                  run_id TEXT NOT NULL,
                  document_id TEXT NOT NULL,
                  process_id TEXT NOT NULL,
                  status TEXT NOT NULL,
                  pipeline_id TEXT,
                  capability TEXT,
                  adapter_kind TEXT,
                  resource_pool TEXT,
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (run_id, document_id, process_id)
                );

                CREATE INDEX IF NOT EXISTS idx_process_statuses_run_status
                  ON process_statuses(run_id, status);

                CREATE INDEX IF NOT EXISTS idx_process_statuses_run_capability
                  ON process_statuses(run_id, capability);

                CREATE INDEX IF NOT EXISTS idx_process_statuses_run_adapter
                  ON process_statuses(run_id, adapter_kind);

                CREATE INDEX IF NOT EXISTS idx_process_statuses_run_resource_pool
                  ON process_statuses(run_id, resource_pool);

                CREATE TABLE IF NOT EXISTS process_document_inputs (
                  run_id TEXT NOT NULL,
                  document_id TEXT NOT NULL,
                  pipeline_id TEXT,
                  payload TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (run_id, document_id)
                );

                CREATE TABLE IF NOT EXISTS process_claims (
                  run_id TEXT NOT NULL,
                  document_id TEXT NOT NULL,
                  process_id TEXT NOT NULL,
                  worker_id TEXT,
                  attempt INTEGER NOT NULL,
                  claimed_at TEXT NOT NULL,
                  expires_at TEXT NOT NULL,
                  payload TEXT NOT NULL,
                  PRIMARY KEY (run_id, document_id, process_id)
                );

                CREATE INDEX IF NOT EXISTS idx_process_claims_run_doc_expires
                  ON process_claims(run_id, document_id, expires_at);

                CREATE TABLE IF NOT EXISTS process_outputs (
                  run_id TEXT NOT NULL,
                  document_id TEXT NOT NULL,
                  process_id TEXT NOT NULL,
                  payload TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (run_id, document_id, process_id)
                );

                CREATE TABLE IF NOT EXISTS process_stream_chunks (
                  run_id TEXT NOT NULL,
                  document_id TEXT NOT NULL,
                  process_id TEXT NOT NULL,
                  stream_id TEXT NOT NULL,
                  chunk_id TEXT NOT NULL,
                  sequence INTEGER NOT NULL,
                  kind TEXT,
                  created_at TEXT NOT NULL,
                  payload TEXT NOT NULL,
                  PRIMARY KEY (
                    run_id, document_id, process_id, stream_id, sequence
                  ),
                  UNIQUE (
                    run_id, document_id, process_id, stream_id, chunk_id
                  )
                );

                CREATE INDEX IF NOT EXISTS idx_process_stream_chunks_lookup
                  ON process_stream_chunks(
                    run_id, document_id, process_id, stream_id, sequence
                  );

                CREATE TABLE IF NOT EXISTS process_stream_checkpoints (
                  run_id TEXT NOT NULL,
                  document_id TEXT NOT NULL,
                  process_id TEXT NOT NULL,
                  stream_id TEXT NOT NULL,
                  consumer_id TEXT NOT NULL,
                  sequence INTEGER NOT NULL,
                  chunk_id TEXT,
                  updated_at TEXT NOT NULL,
                  payload TEXT NOT NULL,
                  PRIMARY KEY (
                    run_id, document_id, process_id, stream_id, consumer_id
                  )
                );

                CREATE TABLE IF NOT EXISTS process_projections (
                  run_id TEXT NOT NULL,
                  document_id TEXT NOT NULL,
                  projection_id TEXT NOT NULL,
                  complete INTEGER NOT NULL,
                  payload TEXT NOT NULL,
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (run_id, document_id, projection_id)
                );
                """
            )
            columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(process_document_inputs)").fetchall()
            }
            if "pipeline_id" not in columns:
                conn.execute(
                    "ALTER TABLE process_document_inputs ADD COLUMN pipeline_id TEXT"
                )
            document_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(process_documents)").fetchall()
            }
            if "parent_document_id" not in document_columns:
                conn.execute(
                    "ALTER TABLE process_documents ADD COLUMN parent_document_id TEXT"
                )
            if "relation" not in document_columns:
                conn.execute(
                    "ALTER TABLE process_documents ADD COLUMN relation TEXT"
                )
            if "parent_process_id" not in document_columns:
                conn.execute(
                    "ALTER TABLE process_documents ADD COLUMN parent_process_id TEXT"
                )
            status_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(process_statuses)").fetchall()
            }
            for column in ("pipeline_id", "capability", "adapter_kind", "resource_pool"):
                if column not in status_columns:
                    conn.execute(
                        f"ALTER TABLE process_statuses ADD COLUMN {column} TEXT"
                    )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_process_documents_run_parent
                  ON process_documents(run_id, parent_document_id)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_process_documents_run_relation
                  ON process_documents(run_id, relation)
                """
            )
            applied_at = datetime.now(timezone.utc).isoformat()
            for migration in runtime_schema_migration_rows():
                conn.execute(
                    """
                    INSERT INTO runtime_schema_migrations
                      (
                        version, migration_id, description, checksum, applied_at,
                        payload
                      )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(version)
                    DO UPDATE SET
                      migration_id = excluded.migration_id,
                      description = excluded.description,
                      checksum = excluded.checksum,
                      payload = excluded.payload
                    """,
                    (
                        migration["version"],
                        migration["migration_id"],
                        migration["description"],
                        migration["checksum"],
                        applied_at,
                        json.dumps(migration, sort_keys=True),
                    ),
                )
            conn.execute(f"PRAGMA user_version = {RUNTIME_SCHEMA_VERSION}")
            conn.commit()

    async def append_audit_event(self, event: OperatorAuditEvent) -> None:
        async with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO operator_audit_events
                      (
                        id, ts, actor, source, action, run_id, document_id,
                        process_id, target, payload
                      )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.id,
                        event.ts.isoformat(),
                        event.actor,
                        event.source,
                        event.action,
                        event.run_id,
                        event.document_id,
                        event.process_id,
                        event.target,
                        event.model_dump_json(),
                    ),
                )
                conn.commit()

    async def list_audit_events(
        self,
        *,
        run_id: str | None = None,
        limit: int | None = None,
        descending: bool = False,
    ) -> list[OperatorAuditEvent]:
        clauses: list[str] = []
        args: list[object] = []
        if run_id is not None:
            clauses.append("run_id = ?")
            args.append(run_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        order_sql = "ORDER BY ts DESC, id DESC" if descending else "ORDER BY ts, id"
        limit_sql = ""
        if limit is not None:
            limit_sql = " LIMIT ?"
            args.append(limit)
        async with self._lock:
            with closing(self._connect()) as conn:
                rows = conn.execute(
                    f"""
                    SELECT payload
                    FROM operator_audit_events
                    {where}
                    {order_sql}
                    {limit_sql}
                    """,
                    args,
                ).fetchall()
        return [OperatorAuditEvent.model_validate_json(row["payload"]) for row in rows]

    async def put_document_input(
        self,
        *,
        run_id: str,
        document_id: str,
        input: ProcessInput,
        pipeline_id: str | None = None,
    ) -> None:
        async with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT INTO process_document_inputs
                      (run_id, document_id, pipeline_id, payload, updated_at)
                    VALUES (?, ?, ?, ?, datetime('now'))
                    ON CONFLICT(run_id, document_id)
                    DO UPDATE SET
                      pipeline_id = COALESCE(excluded.pipeline_id, process_document_inputs.pipeline_id),
                      payload = excluded.payload,
                      updated_at = excluded.updated_at
                    """,
                    (run_id, document_id, pipeline_id, input.model_dump_json()),
                )
                conn.commit()

    async def get_document_input(
        self, *, run_id: str, document_id: str
    ) -> ProcessInput | None:
        async with self._lock:
            with closing(self._connect()) as conn:
                row = conn.execute(
                    """
                    SELECT payload
                    FROM process_document_inputs
                    WHERE run_id = ? AND document_id = ?
                    """,
                    (run_id, document_id),
                ).fetchone()
        if row is None:
            return None
        return ProcessInput.model_validate_json(row["payload"])

    async def get_document_pipeline_id(
        self, *, run_id: str, document_id: str
    ) -> str | None:
        async with self._lock:
            with closing(self._connect()) as conn:
                row = conn.execute(
                    """
                    SELECT pipeline_id FROM process_documents
                    WHERE run_id = ? AND document_id = ?
                    """,
                    (run_id, document_id),
                ).fetchone()
                if row is None or row["pipeline_id"] is None:
                    row = conn.execute(
                        """
                        SELECT pipeline_id
                        FROM process_document_inputs
                        WHERE run_id = ? AND document_id = ?
                        """,
                        (run_id, document_id),
                    ).fetchone()
        if row is None:
            return None
        return row["pipeline_id"]

    async def put_document(self, document: RuntimeDocument) -> None:
        async with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT INTO process_documents
                      (
                        run_id, document_id, pipeline_id, title, document_type,
                        relation, media_type, source_uri, parent_document_id,
                        parent_process_id, status, metadata, summary, created_at,
                        updated_at, payload
                      )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, document_id)
                    DO UPDATE SET
                      pipeline_id = excluded.pipeline_id,
                      title = excluded.title,
                      document_type = excluded.document_type,
                      relation = excluded.relation,
                      media_type = excluded.media_type,
                      source_uri = excluded.source_uri,
                      parent_document_id = excluded.parent_document_id,
                      parent_process_id = excluded.parent_process_id,
                      status = excluded.status,
                      metadata = excluded.metadata,
                      summary = excluded.summary,
                      updated_at = excluded.updated_at,
                      payload = excluded.payload
                    """,
                    (
                        document.run_id,
                        document.document_id,
                        document.pipeline_id,
                        document.title,
                        document.document_type,
                        document.relation,
                        document.media_type,
                        document.source_uri,
                        document.parent_document_id,
                        document.parent_process_id,
                        document.status.value,
                        json.dumps(document.metadata),
                        json.dumps(document.summary),
                        document.created_at.isoformat(),
                        document.updated_at.isoformat(),
                        document.model_dump_json(),
                    ),
                )
                conn.commit()

    async def get_document(
        self, *, run_id: str, document_id: str
    ) -> RuntimeDocument | None:
        async with self._lock:
            with closing(self._connect()) as conn:
                row = conn.execute(
                    """
                    SELECT payload
                    FROM process_documents
                    WHERE run_id = ? AND document_id = ?
                    """,
                    (run_id, document_id),
                ).fetchone()
        if row is None:
            return None
        return RuntimeDocument.model_validate_json(row["payload"])

    async def list_document_records(
        self,
        *,
        run_id: str,
        status: RuntimeDocumentStatus | None = None,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        relation: str | None = None,
        parent_document_id: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[RuntimeDocument]:
        clauses = ["run_id = ?"]
        args: list[object] = [run_id]
        if status is not None:
            clauses.append("status = ?")
            args.append(status.value)
        if pipeline_id is not None:
            clauses.append("pipeline_id = ?")
            args.append(pipeline_id)
        if document_type is not None:
            clauses.append("document_type = ?")
            args.append(document_type)
        if relation is not None:
            clauses.append("relation = ?")
            args.append(relation)
        if parent_document_id is not None:
            clauses.append("parent_document_id = ?")
            args.append(parent_document_id)
        limit_clause = ""
        if limit is not None:
            limit_clause = "LIMIT ? OFFSET ?"
            args.extend([limit, offset])
        elif offset:
            limit_clause = "LIMIT -1 OFFSET ?"
            args.append(offset)
        async with self._lock:
            with closing(self._connect()) as conn:
                rows = conn.execute(
                    f"""
                    SELECT payload
                    FROM process_documents
                    WHERE {' AND '.join(clauses)}
                    ORDER BY document_id ASC
                    {limit_clause}
                    """,
                    args,
                ).fetchall()
        return [RuntimeDocument.model_validate_json(row["payload"]) for row in rows]

    async def list_process_record_keys(
        self,
        *,
        run_id: str,
        status: ProcessStatus | None = None,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        parent_document_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
        capability: str | None = None,
        adapter_kind: str | None = None,
        resource_pool: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        clauses = ["run_id = ?"]
        args: list[object] = [run_id]
        if status is not None:
            clauses.append("status = ?")
            args.append(status.value)
        if pipeline_id is not None:
            clauses.append("pipeline_id = ?")
            args.append(pipeline_id)
        if document_type is not None:
            clauses.append("document_type = ?")
            args.append(document_type)
        if parent_document_id is not None:
            clauses.append("parent_document_id = ?")
            args.append(parent_document_id)
        if document_id is not None:
            clauses.append("document_id = ?")
            args.append(document_id)
        if process_id is not None:
            clauses.append("process_id = ?")
            args.append(process_id)
        if capability is not None:
            clauses.append("capability = ?")
            args.append(capability)
        if adapter_kind is not None:
            clauses.append("adapter_kind = ?")
            args.append(adapter_kind)
        if resource_pool is not None:
            clauses.append("resource_pool = ?")
            args.append(resource_pool)
        limit_clause = ""
        if limit is not None:
            limit_clause = "LIMIT ? OFFSET ?"
            args.extend([limit, offset])
        elif offset:
            limit_clause = "LIMIT -1 OFFSET ?"
            args.append(offset)
        async with self._lock:
            with closing(self._connect()) as conn:
                rows = conn.execute(
                    f"""
                    WITH process_keys AS (
                      SELECT run_id, document_id, process_id
                      FROM process_statuses
                      WHERE run_id = ?
                      UNION
                      SELECT run_id, document_id, process_id
                      FROM process_claims
                      WHERE run_id = ?
                      UNION
                      SELECT run_id, document_id, process_id
                      FROM process_outputs
                      WHERE run_id = ?
                      UNION
                      SELECT run_id, document_id, process_id
                      FROM process_stream_chunks
                      WHERE run_id = ?
                      UNION
                      SELECT run_id, document_id, process_id
                      FROM process_stream_checkpoints
                      WHERE run_id = ?
                    ),
                    process_rows AS (
                      SELECT
                        k.run_id,
                        k.document_id,
                        k.process_id,
                        COALESCE(
                          s.status,
                          CASE
                            WHEN o.process_id IS NOT NULL THEN 'completed'
                            WHEN c.process_id IS NOT NULL THEN 'running'
                            ELSE 'unknown'
                          END
                        ) AS status,
                        s.updated_at AS status_updated_at,
                        COALESCE(d.pipeline_id, i.pipeline_id) AS pipeline_id,
                        s.capability AS capability,
                        s.adapter_kind AS adapter_kind,
                        s.resource_pool AS resource_pool,
                        d.document_type AS document_type,
                        d.parent_document_id AS parent_document_id
                      FROM process_keys k
                      LEFT JOIN process_statuses s
                        ON s.run_id = k.run_id
                       AND s.document_id = k.document_id
                       AND s.process_id = k.process_id
                      LEFT JOIN process_outputs o
                        ON o.run_id = k.run_id
                       AND o.document_id = k.document_id
                       AND o.process_id = k.process_id
                      LEFT JOIN process_claims c
                        ON c.run_id = k.run_id
                       AND c.document_id = k.document_id
                       AND c.process_id = k.process_id
                      LEFT JOIN process_documents d
                        ON d.run_id = k.run_id
                       AND d.document_id = k.document_id
                      LEFT JOIN process_document_inputs i
                        ON i.run_id = k.run_id
                       AND i.document_id = k.document_id
                    )
                    SELECT
                      run_id,
                      document_id,
                      process_id,
                      status,
                      status_updated_at,
                      pipeline_id,
                      capability,
                      adapter_kind,
                      resource_pool
                    FROM process_rows
                    WHERE {' AND '.join(clauses)}
                    ORDER BY document_id ASC, process_id ASC
                    {limit_clause}
                    """,
                    [run_id, run_id, run_id, run_id, run_id, *args],
                ).fetchall()
        return [dict(row) for row in rows]

    async def put_claim(self, claim: ProcessClaim) -> None:
        async with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT INTO process_claims
                      (run_id, document_id, process_id, worker_id, attempt, claimed_at, expires_at, payload)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, document_id, process_id)
                    DO UPDATE SET
                      worker_id = excluded.worker_id,
                      attempt = excluded.attempt,
                      claimed_at = excluded.claimed_at,
                      expires_at = excluded.expires_at,
                      payload = excluded.payload
                    """,
                    (
                        claim.run_id,
                        claim.document_id,
                        claim.process_id,
                        claim.worker_id,
                        claim.attempt,
                        claim.claimed_at.isoformat(),
                        claim.expires_at.isoformat(),
                        claim.model_dump_json(),
                    ),
                )
                conn.commit()

    async def try_claim_process(self, claim: ProcessClaim) -> bool:
        async with self._lock:
            with closing(self._connect()) as conn:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    output = conn.execute(
                        """
                        SELECT 1
                        FROM process_outputs
                        WHERE run_id = ? AND document_id = ? AND process_id = ?
                        """,
                        (claim.run_id, claim.document_id, claim.process_id),
                    ).fetchone()
                    if output is not None:
                        conn.rollback()
                        return False

                    status = conn.execute(
                        """
                        SELECT status
                        FROM process_statuses
                        WHERE run_id = ? AND document_id = ? AND process_id = ?
                        """,
                        (claim.run_id, claim.document_id, claim.process_id),
                    ).fetchone()
                    if status is None or status["status"] != ProcessStatus.queued.value:
                        conn.rollback()
                        return False

                    conn.execute(
                        """
                        INSERT INTO process_claims
                          (run_id, document_id, process_id, worker_id, attempt, claimed_at, expires_at, payload)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(run_id, document_id, process_id)
                        DO UPDATE SET
                          worker_id = excluded.worker_id,
                          attempt = excluded.attempt,
                          claimed_at = excluded.claimed_at,
                          expires_at = excluded.expires_at,
                          payload = excluded.payload
                        """,
                        (
                            claim.run_id,
                            claim.document_id,
                            claim.process_id,
                            claim.worker_id,
                            claim.attempt,
                            claim.claimed_at.isoformat(),
                            claim.expires_at.isoformat(),
                            claim.model_dump_json(),
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO process_statuses
                          (run_id, document_id, process_id, status, updated_at)
                        VALUES (?, ?, ?, ?, datetime('now'))
                        ON CONFLICT(run_id, document_id, process_id)
                        DO UPDATE SET status = excluded.status, updated_at = excluded.updated_at
                        """,
                        (
                            claim.run_id,
                            claim.document_id,
                            claim.process_id,
                            ProcessStatus.running.value,
                        ),
                    )
                    conn.commit()
                    return True
                except BaseException:
                    conn.rollback()
                    raise

    async def get_claim(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> ProcessClaim | None:
        async with self._lock:
            with closing(self._connect()) as conn:
                row = conn.execute(
                    """
                    SELECT payload
                    FROM process_claims
                    WHERE run_id = ? AND document_id = ? AND process_id = ?
                    """,
                    (run_id, document_id, process_id),
                ).fetchone()
        if row is None:
            return None
        return ProcessClaim.model_validate_json(row["payload"])

    async def clear_claim(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> None:
        async with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    DELETE FROM process_claims
                    WHERE run_id = ? AND document_id = ? AND process_id = ?
                    """,
                    (run_id, document_id, process_id),
                )
                conn.commit()

    async def append_event(self, event: ProcessEvent) -> None:
        async with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO process_events
                      (id, run_id, document_id, process_id, type, ts, payload)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.id,
                        event.run_id,
                        event.document_id,
                        event.process_id,
                        event.type,
                        event.ts.isoformat(),
                        event.model_dump_json(),
                    ),
                )
                conn.commit()

    async def list_events(
        self,
        *,
        run_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
        after_event_id: str | None = None,
        limit: int | None = None,
        descending: bool = False,
    ) -> list[ProcessEvent]:
        clauses: list[str] = []
        args: list[object] = []
        if run_id is not None:
            clauses.append("run_id = ?")
            args.append(run_id)
        if document_id is not None:
            clauses.append("document_id = ?")
            args.append(document_id)
        if process_id is not None:
            clauses.append("process_id = ?")
            args.append(process_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        async with self._lock:
            with closing(self._connect()) as conn:
                if after_event_id is not None:
                    cursor_clauses = [*clauses, "id = ?"]
                    cursor_args = [*args, after_event_id]
                    cursor_where = f"WHERE {' AND '.join(cursor_clauses)}"
                    cursor = conn.execute(
                        f"SELECT ts, id FROM process_events {cursor_where}",
                        cursor_args,
                    ).fetchone()
                    if cursor is None:
                        raise ValueError("after_event_id not found")
                    clauses.append("(ts > ? OR (ts = ? AND id > ?))")
                    args.extend([cursor["ts"], cursor["ts"], cursor["id"]])
                    where = f"WHERE {' AND '.join(clauses)}"

                limit_sql = ""
                if limit is not None:
                    limit_sql = " LIMIT ?"
                    args.append(limit)
                order_sql = "ORDER BY ts DESC, id DESC" if descending else "ORDER BY ts, id"
                rows = conn.execute(
                    f"SELECT payload FROM process_events {where} {order_sql}{limit_sql}",
                    args,
                ).fetchall()
        return [ProcessEvent.model_validate_json(row["payload"]) for row in rows]

    async def count_events(
        self,
        *,
        run_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
    ) -> int:
        clauses: list[str] = []
        args: list[object] = []
        if run_id is not None:
            clauses.append("run_id = ?")
            args.append(run_id)
        if document_id is not None:
            clauses.append("document_id = ?")
            args.append(document_id)
        if process_id is not None:
            clauses.append("process_id = ?")
            args.append(process_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        async with self._lock:
            with closing(self._connect()) as conn:
                row = conn.execute(
                    f"SELECT COUNT(*) AS event_count FROM process_events {where}",
                    args,
                ).fetchone()
        return int(row["event_count"] if row is not None else 0)

    async def list_runs(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        args: list[object] = []
        limit_sql = ""
        if limit is not None:
            limit_sql = " LIMIT ?"
            args.append(limit)

        async with self._lock:
            with closing(self._connect()) as conn:
                rows = conn.execute(
                    f"""
                    SELECT
                      run_id,
                      MIN(touched_at) AS created_at,
                      MAX(touched_at) AS updated_at,
                      MAX(payload) AS payload
                    FROM (
                      SELECT id AS run_id, created_at AS touched_at, payload FROM process_runs
                      UNION ALL
                      SELECT id AS run_id, updated_at AS touched_at, payload FROM process_runs
                      UNION ALL
                      SELECT run_id, updated_at AS touched_at, NULL AS payload FROM process_documents
                      UNION ALL
                      SELECT run_id, last_seen_at AS touched_at, NULL AS payload FROM process_worker_heartbeats
                      UNION ALL
                      SELECT run_id, ts AS touched_at, NULL AS payload FROM process_events
                      UNION ALL
                      SELECT run_id, updated_at AS touched_at, NULL AS payload FROM process_statuses
                      UNION ALL
                      SELECT run_id, updated_at AS touched_at, NULL AS payload FROM process_document_inputs
                      UNION ALL
                      SELECT run_id, claimed_at AS touched_at, NULL AS payload FROM process_claims
                      UNION ALL
                      SELECT run_id, updated_at AS touched_at, NULL AS payload FROM process_outputs
                      UNION ALL
                      SELECT run_id, created_at AS touched_at, NULL AS payload FROM process_stream_chunks
                      UNION ALL
                      SELECT run_id, updated_at AS touched_at, NULL AS payload FROM process_stream_checkpoints
                      UNION ALL
                      SELECT run_id, updated_at AS touched_at, NULL AS payload FROM process_projections
                    )
                    GROUP BY run_id
                    ORDER BY updated_at DESC, run_id ASC
                    {limit_sql}
                    """,
                    args,
                ).fetchall()
        return [
            {
                "run_id": row["run_id"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "run": (
                    RuntimeRun.model_validate_json(row["payload"]).model_dump(mode="json")
                    if row["payload"]
                    else None
                ),
            }
            for row in rows
        ]

    async def put_run(self, run: RuntimeRun) -> None:
        async with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT INTO process_runs
                      (
                        id, status, title, outcome, outcome_reason, config, metadata,
                        summary, created_at, updated_at, started_at, finished_at, payload
                      )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id)
                    DO UPDATE SET
                      status = excluded.status,
                      title = excluded.title,
                      outcome = excluded.outcome,
                      outcome_reason = excluded.outcome_reason,
                      config = excluded.config,
                      metadata = excluded.metadata,
                      summary = excluded.summary,
                      updated_at = excluded.updated_at,
                      started_at = excluded.started_at,
                      finished_at = excluded.finished_at,
                      payload = excluded.payload
                    """,
                    (
                        run.id,
                        run.status.value,
                        run.title,
                        run.outcome.value if run.outcome else None,
                        run.outcome_reason,
                        json.dumps(run.config),
                        json.dumps(run.metadata),
                        json.dumps(run.summary),
                        run.created_at.isoformat(),
                        run.updated_at.isoformat(),
                        run.started_at.isoformat() if run.started_at else None,
                        run.finished_at.isoformat() if run.finished_at else None,
                        run.model_dump_json(),
                    ),
                )
                conn.commit()

    async def get_run(self, run_id: str) -> RuntimeRun | None:
        async with self._lock:
            with closing(self._connect()) as conn:
                row = conn.execute(
                    """
                    SELECT payload
                    FROM process_runs
                    WHERE id = ?
                    """,
                    (run_id,),
                ).fetchone()
        if row is None:
            return None
        return RuntimeRun.model_validate_json(row["payload"])

    async def delete_run(self, run_id: str) -> dict[str, int]:
        tables = [
            ("process_stream_checkpoints", "run_id"),
            ("process_stream_chunks", "run_id"),
            ("process_worker_heartbeats", "run_id"),
            ("process_projections", "run_id"),
            ("process_outputs", "run_id"),
            ("process_claims", "run_id"),
            ("process_document_inputs", "run_id"),
            ("process_statuses", "run_id"),
            ("process_events", "run_id"),
            ("process_documents", "run_id"),
            ("process_runs", "id"),
        ]
        counts: dict[str, int] = {}
        async with self._lock:
            with closing(self._connect()) as conn:
                for table, column in tables:
                    cursor = conn.execute(
                        f"DELETE FROM {table} WHERE {column} = ?",
                        (run_id,),
                    )
                    counts[table] = max(cursor.rowcount, 0)
                conn.commit()
        return counts

    async def put_worker_heartbeat(self, heartbeat: RuntimeWorkerHeartbeat) -> None:
        async with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT INTO process_worker_heartbeats
                      (
                        run_id, worker_id, pipeline_id, process_id, adapter_kind,
                        status, last_seen_at, payload
                      )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, worker_id)
                    DO UPDATE SET
                      pipeline_id = excluded.pipeline_id,
                      process_id = excluded.process_id,
                      adapter_kind = excluded.adapter_kind,
                      status = excluded.status,
                      last_seen_at = excluded.last_seen_at,
                      payload = excluded.payload
                    """,
                    (
                        heartbeat.run_id,
                        heartbeat.worker_id,
                        heartbeat.pipeline_id,
                        heartbeat.process_id,
                        heartbeat.adapter_kind,
                        heartbeat.status.value,
                        heartbeat.last_seen_at.isoformat(),
                        heartbeat.model_dump_json(),
                    ),
                )
                conn.commit()

    async def list_worker_heartbeats(
        self, *, run_id: str | None = None
    ) -> list[RuntimeWorkerHeartbeat]:
        clauses: list[str] = []
        args: list[object] = []
        if run_id is not None:
            clauses.append("run_id = ?")
            args.append(run_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        async with self._lock:
            with closing(self._connect()) as conn:
                rows = conn.execute(
                    f"""
                    SELECT payload
                    FROM process_worker_heartbeats
                    {where}
                    ORDER BY run_id ASC, worker_id ASC
                    """,
                    args,
                ).fetchall()
        return [RuntimeWorkerHeartbeat.model_validate_json(row["payload"]) for row in rows]

    async def put_stream_chunk(self, chunk: RuntimeStreamChunk) -> None:
        async with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT INTO process_stream_chunks
                      (
                        run_id, document_id, process_id, stream_id, chunk_id,
                        sequence, kind, created_at, payload
                      )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, document_id, process_id, stream_id, sequence)
                    DO UPDATE SET
                      chunk_id = excluded.chunk_id,
                      kind = excluded.kind,
                      created_at = excluded.created_at,
                      payload = excluded.payload
                    """,
                    (
                        chunk.run_id,
                        chunk.document_id,
                        chunk.process_id,
                        chunk.stream_id,
                        chunk.chunk_id,
                        chunk.sequence,
                        chunk.kind,
                        chunk.created_at.isoformat(),
                        chunk.model_dump_json(),
                    ),
                )
                conn.commit()

    async def list_stream_chunks(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str | None = None,
        stream_id: str | None = None,
        after_sequence: int | None = None,
        limit: int | None = None,
    ) -> list[RuntimeStreamChunk]:
        clauses = [
            "run_id = ?",
            "document_id = ?",
        ]
        args: list[object] = [run_id, document_id]
        if process_id is not None:
            clauses.append("process_id = ?")
            args.append(process_id)
        if stream_id is not None:
            clauses.append("stream_id = ?")
            args.append(stream_id)
        if after_sequence is not None:
            clauses.append("sequence > ?")
            args.append(after_sequence)
        limit_clause = ""
        if limit is not None:
            limit_clause = "LIMIT ?"
            args.append(limit)
        async with self._lock:
            with closing(self._connect()) as conn:
                rows = conn.execute(
                    f"""
                    SELECT payload
                    FROM process_stream_chunks
                    WHERE {' AND '.join(clauses)}
                    ORDER BY stream_id ASC, sequence ASC
                    {limit_clause}
                    """,
                    args,
                ).fetchall()
        return [RuntimeStreamChunk.model_validate_json(row["payload"]) for row in rows]

    async def put_stream_checkpoint(
        self, checkpoint: RuntimeStreamCheckpoint
    ) -> None:
        async with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT INTO process_stream_checkpoints
                      (
                        run_id, document_id, process_id, stream_id, consumer_id,
                        sequence, chunk_id, updated_at, payload
                      )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(
                      run_id, document_id, process_id, stream_id, consumer_id
                    )
                    DO UPDATE SET
                      sequence = excluded.sequence,
                      chunk_id = excluded.chunk_id,
                      updated_at = excluded.updated_at,
                      payload = excluded.payload
                    """,
                    (
                        checkpoint.run_id,
                        checkpoint.document_id,
                        checkpoint.process_id,
                        checkpoint.stream_id,
                        checkpoint.consumer_id,
                        checkpoint.sequence,
                        checkpoint.chunk_id,
                        checkpoint.updated_at.isoformat(),
                        checkpoint.model_dump_json(),
                    ),
                )
                conn.commit()

    async def get_stream_checkpoint(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        stream_id: str,
        consumer_id: str,
    ) -> RuntimeStreamCheckpoint | None:
        async with self._lock:
            with closing(self._connect()) as conn:
                row = conn.execute(
                    """
                    SELECT payload
                    FROM process_stream_checkpoints
                    WHERE run_id = ?
                      AND document_id = ?
                      AND process_id = ?
                      AND stream_id = ?
                      AND consumer_id = ?
                    """,
                    (run_id, document_id, process_id, stream_id, consumer_id),
                ).fetchone()
        if row is None:
            return None
        return RuntimeStreamCheckpoint.model_validate_json(row["payload"])

    async def list_stream_checkpoints(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str | None = None,
        stream_id: str | None = None,
        consumer_id: str | None = None,
    ) -> list[RuntimeStreamCheckpoint]:
        clauses = [
            "run_id = ?",
            "document_id = ?",
        ]
        args: list[object] = [run_id, document_id]
        if process_id is not None:
            clauses.append("process_id = ?")
            args.append(process_id)
        if stream_id is not None:
            clauses.append("stream_id = ?")
            args.append(stream_id)
        if consumer_id is not None:
            clauses.append("consumer_id = ?")
            args.append(consumer_id)
        async with self._lock:
            with closing(self._connect()) as conn:
                rows = conn.execute(
                    f"""
                    SELECT payload
                    FROM process_stream_checkpoints
                    WHERE {' AND '.join(clauses)}
                    ORDER BY process_id ASC, stream_id ASC, consumer_id ASC
                    """,
                    args,
                ).fetchall()
        return [
            RuntimeStreamCheckpoint.model_validate_json(row["payload"])
            for row in rows
        ]

    async def set_status(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        status: ProcessStatus,
        pipeline_id: str | None = None,
        capability: str | None = None,
        adapter_kind: str | None = None,
        resource_pool: str | None = None,
    ) -> None:
        async with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT INTO process_statuses
                      (
                        run_id, document_id, process_id, status, pipeline_id,
                        capability, adapter_kind, resource_pool, updated_at
                      )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                    ON CONFLICT(run_id, document_id, process_id)
                    DO UPDATE SET
                      status = excluded.status,
                      pipeline_id = COALESCE(excluded.pipeline_id, process_statuses.pipeline_id),
                      capability = COALESCE(excluded.capability, process_statuses.capability),
                      adapter_kind = COALESCE(excluded.adapter_kind, process_statuses.adapter_kind),
                      resource_pool = COALESCE(excluded.resource_pool, process_statuses.resource_pool),
                      updated_at = excluded.updated_at
                    """,
                    (
                        run_id,
                        document_id,
                        process_id,
                        status.value,
                        pipeline_id,
                        capability,
                        adapter_kind,
                        resource_pool,
                    ),
                )
                conn.commit()

    async def clear_status(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> None:
        async with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    DELETE FROM process_statuses
                    WHERE run_id = ? AND document_id = ? AND process_id = ?
                    """,
                    (run_id, document_id, process_id),
                )
                conn.commit()

    async def put_output(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        output: ProcessOutput,
    ) -> None:
        async with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT INTO process_outputs
                      (run_id, document_id, process_id, payload, updated_at)
                    VALUES (?, ?, ?, ?, datetime('now'))
                    ON CONFLICT(run_id, document_id, process_id)
                    DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at
                    """,
                    (run_id, document_id, process_id, output.model_dump_json()),
                )
                conn.commit()

    async def clear_output(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> None:
        async with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    DELETE FROM process_outputs
                    WHERE run_id = ? AND document_id = ? AND process_id = ?
                    """,
                    (run_id, document_id, process_id),
                )
                conn.commit()

    async def get_output(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> ProcessOutput | None:
        async with self._lock:
            with closing(self._connect()) as conn:
                row = conn.execute(
                    """
                    SELECT payload
                    FROM process_outputs
                    WHERE run_id = ? AND document_id = ? AND process_id = ?
                    """,
                    (run_id, document_id, process_id),
                ).fetchone()
        if row is None:
            return None
        return ProcessOutput.model_validate_json(row["payload"])

    async def get_outputs(
        self, *, run_id: str, document_id: str, process_ids: list[str]
    ) -> dict[str, ProcessOutput]:
        outputs: dict[str, ProcessOutput] = {}
        for process_id in process_ids:
            output = await self.get_output(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
            )
            if output is not None:
                outputs[process_id] = output
        return outputs

    async def put_projection(self, projection: CombinedProjection) -> None:
        async with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT INTO process_projections
                      (run_id, document_id, projection_id, complete, payload, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, document_id, projection_id)
                    DO UPDATE SET
                      complete = excluded.complete,
                      payload = excluded.payload,
                      updated_at = excluded.updated_at
                    """,
                    (
                        projection.run_id,
                        projection.document_id,
                        projection.id,
                        1 if projection.complete else 0,
                        projection.model_dump_json(),
                        projection.updated_at.isoformat(),
                    ),
                )
                conn.commit()

    async def clear_projections(self, *, run_id: str, document_id: str) -> None:
        async with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    DELETE FROM process_projections
                    WHERE run_id = ? AND document_id = ?
                    """,
                    (run_id, document_id),
                )
                conn.commit()

    async def get_projection(
        self, *, run_id: str, document_id: str, projection_id: str
    ) -> CombinedProjection | None:
        async with self._lock:
            with closing(self._connect()) as conn:
                row = conn.execute(
                    """
                    SELECT payload
                    FROM process_projections
                    WHERE run_id = ? AND document_id = ? AND projection_id = ?
                    """,
                    (run_id, document_id, projection_id),
                ).fetchone()
        if row is None:
            return None
        return CombinedProjection.model_validate_json(row["payload"])

    async def list_documents(self, *, run_id: str) -> list[str]:
        async with self._lock:
            with closing(self._connect()) as conn:
                rows = conn.execute(
                    """
                    SELECT document_id FROM process_events WHERE run_id = ?
                    UNION
                    SELECT document_id FROM process_documents WHERE run_id = ?
                    UNION
                    SELECT document_id FROM process_statuses WHERE run_id = ?
                    UNION
                    SELECT document_id FROM process_document_inputs WHERE run_id = ?
                    UNION
                    SELECT document_id FROM process_claims WHERE run_id = ?
                    UNION
                    SELECT document_id FROM process_outputs WHERE run_id = ?
                    UNION
                    SELECT document_id FROM process_projections WHERE run_id = ?
                    ORDER BY document_id ASC
                    """,
                    (run_id, run_id, run_id, run_id, run_id, run_id, run_id),
                ).fetchall()
        return [row["document_id"] for row in rows]

    async def list_statuses(
        self, *, run_id: str, document_id: str
    ) -> dict[str, ProcessStatus]:
        async with self._lock:
            with closing(self._connect()) as conn:
                rows = conn.execute(
                    """
                    SELECT process_id, status
                    FROM process_statuses
                    WHERE run_id = ? AND document_id = ?
                    ORDER BY process_id ASC
                    """,
                    (run_id, document_id),
                ).fetchall()
        return {
            row["process_id"]: ProcessStatus(row["status"])
            for row in rows
        }

    async def list_outputs(
        self, *, run_id: str, document_id: str
    ) -> dict[str, ProcessOutput]:
        async with self._lock:
            with closing(self._connect()) as conn:
                rows = conn.execute(
                    """
                    SELECT process_id, payload
                    FROM process_outputs
                    WHERE run_id = ? AND document_id = ?
                    ORDER BY process_id ASC
                    """,
                    (run_id, document_id),
                ).fetchall()
        return {
            row["process_id"]: ProcessOutput.model_validate_json(row["payload"])
            for row in rows
        }

    async def list_claims(
        self, *, run_id: str, document_id: str
    ) -> dict[str, ProcessClaim]:
        async with self._lock:
            with closing(self._connect()) as conn:
                rows = conn.execute(
                    """
                    SELECT process_id, payload
                    FROM process_claims
                    WHERE run_id = ? AND document_id = ?
                    ORDER BY process_id ASC
                    """,
                    (run_id, document_id),
                ).fetchall()
        return {
            row["process_id"]: ProcessClaim.model_validate_json(row["payload"])
            for row in rows
        }

    async def list_projections(
        self, *, run_id: str, document_id: str
    ) -> dict[str, CombinedProjection]:
        async with self._lock:
            with closing(self._connect()) as conn:
                rows = conn.execute(
                    """
                    SELECT projection_id, payload
                    FROM process_projections
                    WHERE run_id = ? AND document_id = ?
                    ORDER BY projection_id ASC
                    """,
                    (run_id, document_id),
                ).fetchall()
        return {
            row["projection_id"]: CombinedProjection.model_validate_json(row["payload"])
            for row in rows
        }
