from __future__ import annotations

import asyncio
import json
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Callable, Iterator

from fala.schema_migrations import (
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


POSTGRES_SCHEMA_SQL = """
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

ALTER TABLE process_documents
  ADD COLUMN IF NOT EXISTS relation TEXT;

CREATE INDEX IF NOT EXISTS idx_process_documents_run_status
  ON process_documents(run_id, status);

CREATE INDEX IF NOT EXISTS idx_process_documents_run_pipeline
  ON process_documents(run_id, pipeline_id);

CREATE INDEX IF NOT EXISTS idx_process_documents_run_type
  ON process_documents(run_id, document_type);

CREATE INDEX IF NOT EXISTS idx_process_documents_run_parent
  ON process_documents(run_id, parent_document_id);

CREATE INDEX IF NOT EXISTS idx_process_documents_run_relation
  ON process_documents(run_id, relation);

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
  PRIMARY KEY (run_id, document_id, process_id, stream_id, sequence),
  UNIQUE (run_id, document_id, process_id, stream_id, chunk_id)
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
  PRIMARY KEY (run_id, document_id, process_id, stream_id, consumer_id)
);

CREATE TABLE IF NOT EXISTS process_projections (
  run_id TEXT NOT NULL,
  document_id TEXT NOT NULL,
  projection_id TEXT NOT NULL,
  complete BOOLEAN NOT NULL,
  payload TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (run_id, document_id, projection_id)
);
"""


POSTGRES_TRY_CLAIM_STATUS_SQL = """
UPDATE process_statuses
SET status = %s,
    updated_at = %s
WHERE run_id = %s
  AND document_id = %s
  AND process_id = %s
  AND status = %s
  AND NOT EXISTS (
    SELECT 1
    FROM process_outputs
    WHERE run_id = %s
      AND document_id = %s
      AND process_id = %s
  )
RETURNING process_id
"""


ConnectionFactory = Callable[[], Any]


class PostgresStateStore:
    """PostgreSQL StateStore implementation for multi-worker runtimes.

    `psycopg` is imported lazily so core Fala stays dependency-light. Install the
    optional postgres extra before constructing this store in a real process.
    """

    def __init__(
        self,
        dsn: str,
        *,
        connect_factory: ConnectionFactory | None = None,
        ensure_schema: bool = True,
    ) -> None:
        self.dsn = dsn
        self._connect_factory = connect_factory
        self._psycopg_connect: Any | None = None
        self._dict_row: Any | None = None
        if ensure_schema:
            self._init_schema()

    def _load_psycopg(self) -> None:
        if self._connect_factory is not None or self._psycopg_connect is not None:
            return
        try:
            import psycopg
            from psycopg.rows import dict_row
        except ImportError as exc:
            raise RuntimeError(
                "PostgresStateStore requires psycopg. Install Fala with the "
                "`postgres` extra, for example `pip install 'fala[postgres]'`."
            ) from exc
        self._psycopg_connect = psycopg.connect
        self._dict_row = dict_row

    @contextmanager
    def _connect(self) -> Iterator[Any]:
        if self._connect_factory is not None:
            with self._connect_factory() as conn:
                yield conn
            return
        self._load_psycopg()
        assert self._psycopg_connect is not None
        with self._psycopg_connect(self.dsn, row_factory=self._dict_row) as conn:
            yield conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            for statement in _split_sql(POSTGRES_SCHEMA_SQL):
                conn.execute(statement)
            applied_at = datetime.now(timezone.utc).isoformat()
            for migration in runtime_schema_migration_rows():
                conn.execute(
                    """
                    INSERT INTO runtime_schema_migrations
                      (
                        version, migration_id, description, checksum, applied_at,
                        payload
                      )
                    VALUES (%s, %s, %s, %s, %s, %s)
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
            conn.commit()

    async def _run_sync(self, fn: Callable[[], Any]) -> Any:
        return await asyncio.to_thread(fn)

    async def append_audit_event(self, event: OperatorAuditEvent) -> None:
        def sync() -> None:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO operator_audit_events
                      (
                        id, ts, actor, source, action, run_id, document_id,
                        process_id, target, payload
                      )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(id)
                    DO UPDATE SET
                      ts = excluded.ts,
                      actor = excluded.actor,
                      source = excluded.source,
                      action = excluded.action,
                      run_id = excluded.run_id,
                      document_id = excluded.document_id,
                      process_id = excluded.process_id,
                      target = excluded.target,
                      payload = excluded.payload
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

        await self._run_sync(sync)

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
            clauses.append("run_id = %s")
            args.append(run_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        order_sql = "ORDER BY ts DESC, id DESC" if descending else "ORDER BY ts, id"
        limit_sql = _limit_sql(args, limit=limit)

        def sync() -> list[OperatorAuditEvent]:
            with self._connect() as conn:
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

        return await self._run_sync(sync)

    async def append_event(self, event: ProcessEvent) -> None:
        def sync() -> None:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO process_events
                      (id, run_id, document_id, process_id, type, ts, payload)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT(id)
                    DO UPDATE SET
                      run_id = excluded.run_id,
                      document_id = excluded.document_id,
                      process_id = excluded.process_id,
                      type = excluded.type,
                      ts = excluded.ts,
                      payload = excluded.payload
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

        await self._run_sync(sync)

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
            clauses.append("run_id = %s")
            args.append(run_id)
        if document_id is not None:
            clauses.append("document_id = %s")
            args.append(document_id)
        if process_id is not None:
            clauses.append("process_id = %s")
            args.append(process_id)

        def sync() -> list[ProcessEvent]:
            query_clauses = list(clauses)
            query_args = list(args)
            where = f"WHERE {' AND '.join(query_clauses)}" if query_clauses else ""
            with self._connect() as conn:
                if after_event_id is not None:
                    cursor_clauses = [*query_clauses, "id = %s"]
                    cursor_args = [*query_args, after_event_id]
                    cursor_where = f"WHERE {' AND '.join(cursor_clauses)}"
                    cursor = conn.execute(
                        f"SELECT ts, id FROM process_events {cursor_where}",
                        cursor_args,
                    ).fetchone()
                    if cursor is None:
                        raise ValueError("after_event_id not found")
                    query_clauses.append("(ts > %s OR (ts = %s AND id > %s))")
                    query_args.extend([cursor["ts"], cursor["ts"], cursor["id"]])
                    where = f"WHERE {' AND '.join(query_clauses)}"

                limit_sql = _limit_sql(query_args, limit=limit)
                order_sql = (
                    "ORDER BY ts DESC, id DESC" if descending else "ORDER BY ts, id"
                )
                rows = conn.execute(
                    f"SELECT payload FROM process_events {where} {order_sql}{limit_sql}",
                    query_args,
                ).fetchall()
            return [ProcessEvent.model_validate_json(row["payload"]) for row in rows]

        return await self._run_sync(sync)

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
            clauses.append("run_id = %s")
            args.append(run_id)
        if document_id is not None:
            clauses.append("document_id = %s")
            args.append(document_id)
        if process_id is not None:
            clauses.append("process_id = %s")
            args.append(process_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        def sync() -> int:
            with self._connect() as conn:
                row = conn.execute(
                    f"SELECT COUNT(*) AS event_count FROM process_events {where}",
                    args,
                ).fetchone()
            return int(row["event_count"] if row is not None else 0)

        return await self._run_sync(sync)

    async def list_runs(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        args: list[object] = []
        limit_sql = _limit_sql(args, limit=limit)

        def sync() -> list[dict[str, Any]]:
            with self._connect() as conn:
                rows = conn.execute(
                    f"""
                    SELECT
                      run_id,
                      MIN(touched_at) AS created_at,
                      MAX(touched_at) AS updated_at,
                      MAX(payload) FILTER (WHERE payload IS NOT NULL) AS payload
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
                    ) AS touched_runs
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

        return await self._run_sync(sync)

    async def put_run(self, run: RuntimeRun) -> None:
        def sync() -> None:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO process_runs
                      (
                        id, status, title, outcome, outcome_reason, config, metadata,
                        summary, created_at, updated_at, started_at, finished_at, payload
                      )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

        await self._run_sync(sync)

    async def get_run(self, run_id: str) -> RuntimeRun | None:
        def sync() -> RuntimeRun | None:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT payload
                    FROM process_runs
                    WHERE id = %s
                    """,
                    (run_id,),
                ).fetchone()
            if row is None:
                return None
            return RuntimeRun.model_validate_json(row["payload"])

        return await self._run_sync(sync)

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

        def sync() -> dict[str, int]:
            counts: dict[str, int] = {}
            with self._connect() as conn:
                for table, column in tables:
                    cursor = conn.execute(
                        f"DELETE FROM {table} WHERE {column} = %s",
                        (run_id,),
                    )
                    counts[table] = max(cursor.rowcount, 0)
                conn.commit()
            return counts

        return await self._run_sync(sync)

    async def put_worker_heartbeat(self, heartbeat: RuntimeWorkerHeartbeat) -> None:
        def sync() -> None:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO process_worker_heartbeats
                      (
                        run_id, worker_id, pipeline_id, process_id, adapter_kind,
                        status, last_seen_at, payload
                      )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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

        await self._run_sync(sync)

    async def list_worker_heartbeats(
        self, *, run_id: str | None = None
    ) -> list[RuntimeWorkerHeartbeat]:
        clauses: list[str] = []
        args: list[object] = []
        if run_id is not None:
            clauses.append("run_id = %s")
            args.append(run_id)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        def sync() -> list[RuntimeWorkerHeartbeat]:
            with self._connect() as conn:
                rows = conn.execute(
                    f"""
                    SELECT payload
                    FROM process_worker_heartbeats
                    {where}
                    ORDER BY run_id ASC, worker_id ASC
                    """,
                    args,
                ).fetchall()
            return [
                RuntimeWorkerHeartbeat.model_validate_json(row["payload"])
                for row in rows
            ]

        return await self._run_sync(sync)

    async def put_stream_chunk(self, chunk: RuntimeStreamChunk) -> None:
        def sync() -> None:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO process_stream_chunks
                      (
                        run_id, document_id, process_id, stream_id, chunk_id,
                        sequence, kind, created_at, payload
                      )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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

        await self._run_sync(sync)

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
            "run_id = %s",
            "document_id = %s",
        ]
        args: list[object] = [run_id, document_id]
        if process_id is not None:
            clauses.append("process_id = %s")
            args.append(process_id)
        if stream_id is not None:
            clauses.append("stream_id = %s")
            args.append(stream_id)
        if after_sequence is not None:
            clauses.append("sequence > %s")
            args.append(after_sequence)
        limit_sql = _limit_sql(args, limit=limit)

        def sync() -> list[RuntimeStreamChunk]:
            with self._connect() as conn:
                rows = conn.execute(
                    f"""
                    SELECT payload
                    FROM process_stream_chunks
                    WHERE {' AND '.join(clauses)}
                    ORDER BY stream_id ASC, sequence ASC
                    {limit_sql}
                    """,
                    args,
                ).fetchall()
            return [
                RuntimeStreamChunk.model_validate_json(row["payload"])
                for row in rows
            ]

        return await self._run_sync(sync)

    async def put_stream_checkpoint(
        self, checkpoint: RuntimeStreamCheckpoint
    ) -> None:
        def sync() -> None:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO process_stream_checkpoints
                      (
                        run_id, document_id, process_id, stream_id, consumer_id,
                        sequence, chunk_id, updated_at, payload
                      )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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

        await self._run_sync(sync)

    async def get_stream_checkpoint(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        stream_id: str,
        consumer_id: str,
    ) -> RuntimeStreamCheckpoint | None:
        def sync() -> RuntimeStreamCheckpoint | None:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT payload
                    FROM process_stream_checkpoints
                    WHERE run_id = %s
                      AND document_id = %s
                      AND process_id = %s
                      AND stream_id = %s
                      AND consumer_id = %s
                    """,
                    (run_id, document_id, process_id, stream_id, consumer_id),
                ).fetchone()
            if row is None:
                return None
            return RuntimeStreamCheckpoint.model_validate_json(row["payload"])

        return await self._run_sync(sync)

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
            "run_id = %s",
            "document_id = %s",
        ]
        args: list[object] = [run_id, document_id]
        if process_id is not None:
            clauses.append("process_id = %s")
            args.append(process_id)
        if stream_id is not None:
            clauses.append("stream_id = %s")
            args.append(stream_id)
        if consumer_id is not None:
            clauses.append("consumer_id = %s")
            args.append(consumer_id)

        def sync() -> list[RuntimeStreamCheckpoint]:
            with self._connect() as conn:
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

        return await self._run_sync(sync)

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
        def sync() -> None:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO process_statuses
                      (
                        run_id, document_id, process_id, status, pipeline_id,
                        capability, adapter_kind, resource_pool, updated_at
                      )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        _now_iso(),
                    ),
                )
                conn.commit()

        await self._run_sync(sync)

    async def clear_status(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> None:
        await self._delete_process_row(
            "process_statuses",
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )

    async def put_document_input(
        self,
        *,
        run_id: str,
        document_id: str,
        input: ProcessInput,
        pipeline_id: str | None = None,
    ) -> None:
        def sync() -> None:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO process_document_inputs
                      (run_id, document_id, pipeline_id, payload, updated_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT(run_id, document_id)
                    DO UPDATE SET
                      pipeline_id = COALESCE(
                        excluded.pipeline_id,
                        process_document_inputs.pipeline_id
                      ),
                      payload = excluded.payload,
                      updated_at = excluded.updated_at
                    """,
                    (
                        run_id,
                        document_id,
                        pipeline_id,
                        input.model_dump_json(),
                        _now_iso(),
                    ),
                )
                conn.commit()

        await self._run_sync(sync)

    async def get_document_input(
        self, *, run_id: str, document_id: str
    ) -> ProcessInput | None:
        def sync() -> ProcessInput | None:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT payload
                    FROM process_document_inputs
                    WHERE run_id = %s AND document_id = %s
                    """,
                    (run_id, document_id),
                ).fetchone()
            if row is None:
                return None
            return ProcessInput.model_validate_json(row["payload"])

        return await self._run_sync(sync)

    async def get_document_pipeline_id(
        self, *, run_id: str, document_id: str
    ) -> str | None:
        def sync() -> str | None:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT pipeline_id FROM process_documents
                    WHERE run_id = %s AND document_id = %s
                    """,
                    (run_id, document_id),
                ).fetchone()
                if row is None or row["pipeline_id"] is None:
                    row = conn.execute(
                        """
                        SELECT pipeline_id
                        FROM process_document_inputs
                        WHERE run_id = %s AND document_id = %s
                        """,
                        (run_id, document_id),
                    ).fetchone()
            if row is None:
                return None
            return row["pipeline_id"]

        return await self._run_sync(sync)

    async def put_document(self, document: RuntimeDocument) -> None:
        def sync() -> None:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO process_documents
                      (
                        run_id, document_id, pipeline_id, title, document_type,
                        relation, media_type, source_uri, parent_document_id,
                        parent_process_id, status, metadata, summary, created_at,
                        updated_at, payload
                      )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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

        await self._run_sync(sync)

    async def get_document(
        self, *, run_id: str, document_id: str
    ) -> RuntimeDocument | None:
        def sync() -> RuntimeDocument | None:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT payload
                    FROM process_documents
                    WHERE run_id = %s AND document_id = %s
                    """,
                    (run_id, document_id),
                ).fetchone()
            if row is None:
                return None
            return RuntimeDocument.model_validate_json(row["payload"])

        return await self._run_sync(sync)

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
        clauses = ["run_id = %s"]
        args: list[object] = [run_id]
        if status is not None:
            clauses.append("status = %s")
            args.append(status.value)
        if pipeline_id is not None:
            clauses.append("pipeline_id = %s")
            args.append(pipeline_id)
        if document_type is not None:
            clauses.append("document_type = %s")
            args.append(document_type)
        if relation is not None:
            clauses.append("relation = %s")
            args.append(relation)
        if parent_document_id is not None:
            clauses.append("parent_document_id = %s")
            args.append(parent_document_id)
        limit_clause = _limit_sql(args, limit=limit, offset=offset)

        def sync() -> list[RuntimeDocument]:
            with self._connect() as conn:
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

        return await self._run_sync(sync)

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
        clauses = ["run_id = %s"]
        args: list[object] = [run_id]
        if status is not None:
            clauses.append("status = %s")
            args.append(status.value)
        if pipeline_id is not None:
            clauses.append("pipeline_id = %s")
            args.append(pipeline_id)
        if document_type is not None:
            clauses.append("document_type = %s")
            args.append(document_type)
        if parent_document_id is not None:
            clauses.append("parent_document_id = %s")
            args.append(parent_document_id)
        if document_id is not None:
            clauses.append("document_id = %s")
            args.append(document_id)
        if process_id is not None:
            clauses.append("process_id = %s")
            args.append(process_id)
        if capability is not None:
            clauses.append("capability = %s")
            args.append(capability)
        if adapter_kind is not None:
            clauses.append("adapter_kind = %s")
            args.append(adapter_kind)
        if resource_pool is not None:
            clauses.append("resource_pool = %s")
            args.append(resource_pool)
        limit_clause = _limit_sql(args, limit=limit, offset=offset)

        def sync() -> list[dict[str, Any]]:
            with self._connect() as conn:
                rows = conn.execute(
                    f"""
                    WITH process_keys AS (
                      SELECT run_id, document_id, process_id
                      FROM process_statuses
                      WHERE run_id = %s
                      UNION
                      SELECT run_id, document_id, process_id
                      FROM process_claims
                      WHERE run_id = %s
                      UNION
                      SELECT run_id, document_id, process_id
                      FROM process_outputs
                      WHERE run_id = %s
                      UNION
                      SELECT run_id, document_id, process_id
                      FROM process_stream_chunks
                      WHERE run_id = %s
                      UNION
                      SELECT run_id, document_id, process_id
                      FROM process_stream_checkpoints
                      WHERE run_id = %s
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

        return await self._run_sync(sync)

    async def put_claim(self, claim: ProcessClaim) -> None:
        def sync() -> None:
            with self._connect() as conn:
                _upsert_claim(conn, claim)
                conn.commit()

        await self._run_sync(sync)

    async def try_claim_process(self, claim: ProcessClaim) -> bool:
        def sync() -> bool:
            with self._connect() as conn:
                claimed = conn.execute(
                    POSTGRES_TRY_CLAIM_STATUS_SQL,
                    (
                        ProcessStatus.running.value,
                        _now_iso(),
                        claim.run_id,
                        claim.document_id,
                        claim.process_id,
                        ProcessStatus.queued.value,
                        claim.run_id,
                        claim.document_id,
                        claim.process_id,
                    ),
                ).fetchone()
                if claimed is None:
                    conn.rollback()
                    return False
                _upsert_claim(conn, claim)
                conn.commit()
                return True

        return await self._run_sync(sync)

    async def get_claim(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> ProcessClaim | None:
        def sync() -> ProcessClaim | None:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT payload
                    FROM process_claims
                    WHERE run_id = %s AND document_id = %s AND process_id = %s
                    """,
                    (run_id, document_id, process_id),
                ).fetchone()
            if row is None:
                return None
            return ProcessClaim.model_validate_json(row["payload"])

        return await self._run_sync(sync)

    async def clear_claim(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> None:
        await self._delete_process_row(
            "process_claims",
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )

    async def put_output(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        output: ProcessOutput,
    ) -> None:
        def sync() -> None:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO process_outputs
                      (run_id, document_id, process_id, payload, updated_at)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT(run_id, document_id, process_id)
                    DO UPDATE SET
                      payload = excluded.payload,
                      updated_at = excluded.updated_at
                    """,
                    (run_id, document_id, process_id, output.model_dump_json(), _now_iso()),
                )
                conn.commit()

        await self._run_sync(sync)

    async def clear_output(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> None:
        await self._delete_process_row(
            "process_outputs",
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )

    async def get_output(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> ProcessOutput | None:
        def sync() -> ProcessOutput | None:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT payload
                    FROM process_outputs
                    WHERE run_id = %s AND document_id = %s AND process_id = %s
                    """,
                    (run_id, document_id, process_id),
                ).fetchone()
            if row is None:
                return None
            return ProcessOutput.model_validate_json(row["payload"])

        return await self._run_sync(sync)

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
        def sync() -> None:
            with self._connect() as conn:
                conn.execute(
                    """
                    INSERT INTO process_projections
                      (run_id, document_id, projection_id, complete, payload, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s)
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
                        projection.complete,
                        projection.model_dump_json(),
                        projection.updated_at.isoformat(),
                    ),
                )
                conn.commit()

        await self._run_sync(sync)

    async def clear_projections(self, *, run_id: str, document_id: str) -> None:
        def sync() -> None:
            with self._connect() as conn:
                conn.execute(
                    """
                    DELETE FROM process_projections
                    WHERE run_id = %s AND document_id = %s
                    """,
                    (run_id, document_id),
                )
                conn.commit()

        await self._run_sync(sync)

    async def get_projection(
        self, *, run_id: str, document_id: str, projection_id: str
    ) -> CombinedProjection | None:
        def sync() -> CombinedProjection | None:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT payload
                    FROM process_projections
                    WHERE run_id = %s AND document_id = %s AND projection_id = %s
                    """,
                    (run_id, document_id, projection_id),
                ).fetchone()
            if row is None:
                return None
            return CombinedProjection.model_validate_json(row["payload"])

        return await self._run_sync(sync)

    async def list_documents(self, *, run_id: str) -> list[str]:
        def sync() -> list[str]:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT document_id FROM process_events WHERE run_id = %s
                    UNION
                    SELECT document_id FROM process_documents WHERE run_id = %s
                    UNION
                    SELECT document_id FROM process_statuses WHERE run_id = %s
                    UNION
                    SELECT document_id FROM process_document_inputs WHERE run_id = %s
                    UNION
                    SELECT document_id FROM process_claims WHERE run_id = %s
                    UNION
                    SELECT document_id FROM process_outputs WHERE run_id = %s
                    UNION
                    SELECT document_id FROM process_projections WHERE run_id = %s
                    ORDER BY document_id ASC
                    """,
                    (run_id, run_id, run_id, run_id, run_id, run_id, run_id),
                ).fetchall()
            return [row["document_id"] for row in rows]

        return await self._run_sync(sync)

    async def list_statuses(
        self, *, run_id: str, document_id: str
    ) -> dict[str, ProcessStatus]:
        def sync() -> dict[str, ProcessStatus]:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT process_id, status
                    FROM process_statuses
                    WHERE run_id = %s AND document_id = %s
                    ORDER BY process_id ASC
                    """,
                    (run_id, document_id),
                ).fetchall()
            return {
                row["process_id"]: ProcessStatus(row["status"])
                for row in rows
            }

        return await self._run_sync(sync)

    async def list_outputs(
        self, *, run_id: str, document_id: str
    ) -> dict[str, ProcessOutput]:
        def sync() -> dict[str, ProcessOutput]:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT process_id, payload
                    FROM process_outputs
                    WHERE run_id = %s AND document_id = %s
                    ORDER BY process_id ASC
                    """,
                    (run_id, document_id),
                ).fetchall()
            return {
                row["process_id"]: ProcessOutput.model_validate_json(row["payload"])
                for row in rows
            }

        return await self._run_sync(sync)

    async def list_claims(
        self, *, run_id: str, document_id: str
    ) -> dict[str, ProcessClaim]:
        def sync() -> dict[str, ProcessClaim]:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT process_id, payload
                    FROM process_claims
                    WHERE run_id = %s AND document_id = %s
                    ORDER BY process_id ASC
                    """,
                    (run_id, document_id),
                ).fetchall()
            return {
                row["process_id"]: ProcessClaim.model_validate_json(row["payload"])
                for row in rows
            }

        return await self._run_sync(sync)

    async def list_projections(
        self, *, run_id: str, document_id: str
    ) -> dict[str, CombinedProjection]:
        def sync() -> dict[str, CombinedProjection]:
            with self._connect() as conn:
                rows = conn.execute(
                    """
                    SELECT projection_id, payload
                    FROM process_projections
                    WHERE run_id = %s AND document_id = %s
                    ORDER BY projection_id ASC
                    """,
                    (run_id, document_id),
                ).fetchall()
            return {
                row["projection_id"]: CombinedProjection.model_validate_json(row["payload"])
                for row in rows
            }

        return await self._run_sync(sync)

    async def _delete_process_row(
        self,
        table: str,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
    ) -> None:
        def sync() -> None:
            with self._connect() as conn:
                conn.execute(
                    f"""
                    DELETE FROM {table}
                    WHERE run_id = %s AND document_id = %s AND process_id = %s
                    """,
                    (run_id, document_id, process_id),
                )
                conn.commit()

        await self._run_sync(sync)


def _upsert_claim(conn: Any, claim: ProcessClaim) -> None:
    conn.execute(
        """
        INSERT INTO process_claims
          (
            run_id, document_id, process_id, worker_id, attempt,
            claimed_at, expires_at, payload
          )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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


def _limit_sql(
    args: list[object],
    *,
    limit: int | None,
    offset: int = 0,
) -> str:
    if limit is not None:
        args.extend([limit, offset])
        return "LIMIT %s OFFSET %s"
    if offset:
        args.append(offset)
        return "OFFSET %s"
    return ""


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _split_sql(sql: str) -> list[str]:
    return [statement.strip() for statement in sql.split(";") if statement.strip()]
