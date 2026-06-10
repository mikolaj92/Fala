from __future__ import annotations

import asyncio
import sqlite3
from contextlib import closing
from pathlib import Path

from fala.models import (
    CombinedProjection,
    ProcessClaim,
    ProcessEvent,
    ProcessInput,
    ProcessOutput,
    ProcessStatus,
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
                  updated_at TEXT NOT NULL,
                  PRIMARY KEY (run_id, document_id, process_id)
                );

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
            conn.commit()

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
                    SELECT pipeline_id
                    FROM process_document_inputs
                    WHERE run_id = ? AND document_id = ?
                    """,
                    (run_id, document_id),
                ).fetchone()
        if row is None:
            return None
        return row["pipeline_id"]

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

    async def set_status(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        status: ProcessStatus,
    ) -> None:
        async with self._lock:
            with closing(self._connect()) as conn:
                conn.execute(
                    """
                    INSERT INTO process_statuses
                      (run_id, document_id, process_id, status, updated_at)
                    VALUES (?, ?, ?, ?, datetime('now'))
                    ON CONFLICT(run_id, document_id, process_id)
                    DO UPDATE SET status = excluded.status, updated_at = excluded.updated_at
                    """,
                    (run_id, document_id, process_id, status.value),
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
                    (run_id, run_id, run_id, run_id, run_id, run_id),
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
