"""Public, domain-blind lifecycle operations for durable Fala journals.

These helpers deliberately expose lifecycle intent rather than SQLite.  Metadata and
JSON payloads are opaque to Fala; applications remain responsible for their meaning.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict

from fala.host import _ensure_durable_schema

TERMINAL_RUN_STATUSES = frozenset({"completed", "failed", "cancelled", "timed_out"})
TERMINAL_PROCESS_STATUSES = frozenset({"succeeded", "failed", "cancelled", "timed_out"})
_ENSURE_LOCK = threading.Lock()
_ENSURED_JOURNALS: set[Path] = set()


class LifecycleResult(TypedDict):
    run_id: str
    process_id: str
    changed: bool
    process_status: str
    run_status: str


def _db(db_path: str | Path) -> Path:
    return Path(db_path).expanduser().resolve()


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _required(value: str, label: str) -> str:
    result = str(value).strip()
    if not result:
        raise ValueError(f"fala journal: {label} must not be blank")
    return result


def _json(value: Mapping[str, Any] | None, label: str) -> str:
    try:
        return json.dumps(
            dict(value or {}), ensure_ascii=False, allow_nan=False, sort_keys=True
        )
    except (TypeError, ValueError) as exc:
        raise TypeError(f"fala journal: {label} is not JSON-recordable") from exc


def ensure_journal(db_path: str | Path) -> None:
    """Create/open a schema-v6 Fala journal and its parent directory."""
    db = _db(db_path)
    db.parent.mkdir(parents=True, exist_ok=True)
    with _ENSURE_LOCK:
        if db in _ENSURED_JOURNALS and db.is_file():
            return
        _ensure_durable_schema(db)
        _ENSURED_JOURNALS.add(db)


def upsert_run_metadata(
    db_path: str | Path,
    *,
    run_id: str,
    metadata: Mapping[str, Any],
    title: str | None = None,
    status: str = "active",
) -> None:
    """Create a run or replace its opaque metadata and current status."""
    db = _db(db_path)
    rid = _required(run_id, "run_id")
    state = _required(status, "status")
    encoded = _json(metadata, "metadata")
    ensure_journal(db)
    now = _now()
    with sqlite3.connect(db, timeout=30.0) as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO runs (id,status,title,schema_version,metadata,created_at,updated_at) "
            "VALUES (?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET "
            "status=excluded.status,title=COALESCE(?,runs.title),"
            "metadata=excluded.metadata,updated_at=excluded.updated_at",
            (rid, state, title or rid, 6, encoded, now, now, title),
        )


def transition_run(
    db_path: str | Path,
    *,
    run_id: str,
    status: str,
    metadata_updates: Mapping[str, Any] | None = None,
) -> None:
    """Transition an existing run, optionally merging opaque metadata fields."""
    db = _db(db_path)
    rid = _required(run_id, "run_id")
    state = _required(status, "status")
    updates = dict(metadata_updates or {})
    _json(updates, "metadata_updates")
    ensure_journal(db)
    now = _now()
    with sqlite3.connect(db, timeout=30.0) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT metadata FROM runs WHERE id=?", (rid,)).fetchone()
        if row is None:
            raise ValueError(f"fala journal: run {rid!r} not found")
        try:
            metadata = json.loads(row["metadata"] or "{}")
        except (TypeError, json.JSONDecodeError):
            metadata = {}
        if not isinstance(metadata, dict):
            metadata = {}
        metadata.update(updates)
        finished = now if state in TERMINAL_RUN_STATUSES else None
        conn.execute(
            "UPDATE runs SET status=?,metadata=?,updated_at=?,"
            "finished_at=COALESCE(?,finished_at) WHERE id=?",
            (state, _json(metadata, "metadata"), now, finished, rid),
        )


def finalize_run(
    db_path: str | Path,
    *,
    run_id: str,
    status: str,
    reason: str | None = None,
) -> None:
    """Compatibility convenience for a terminal run transition."""
    updates = {"finalize_reason": reason} if reason else None
    transition_run(db_path, run_id=run_id, status=status, metadata_updates=updates)


def upsert_process(
    db_path: str | Path,
    *,
    run_id: str,
    process_id: str,
    status: str,
    output: Mapping[str, Any] | None = None,
    error: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
    inputs: Mapping[str, Any] | None = None,
    attempt: int = 1,
    process_type: str = "external",
) -> None:
    """Upsert a journal process whose payloads and metadata are opaque JSON objects."""
    db = _db(db_path)
    rid = _required(run_id, "run_id")
    pid = _required(process_id, "process_id")
    state = _required(status, "status")
    ptype = _required(process_type, "process_type")
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
        raise ValueError("fala journal: attempt must be a positive integer")
    input_json = _json(inputs, "inputs")
    output_json = _json(output, "output")
    error_json = _json(error, "error")
    metadata_json = _json(metadata, "metadata")
    ensure_journal(db)
    now = _now()
    finished = now if state in TERMINAL_PROCESS_STATUSES else None
    with sqlite3.connect(db, timeout=30.0) as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO processes "
            "(run_id,id,process_type,status,priority,attempt,max_attempts,available_at,"
            "input_json,output_json,error_json,metadata,created_at,updated_at,started_at,"
            "finished_at,output_schema_json) VALUES (?,?,?, ?,0,?,1,?,?,?,?,?,?,?,?,?,'{}') "
            "ON CONFLICT(run_id,id) DO UPDATE SET status=excluded.status,"
            "input_json=excluded.input_json,output_json=excluded.output_json,"
            "error_json=excluded.error_json,metadata=excluded.metadata,attempt=excluded.attempt,"
            "updated_at=excluded.updated_at,finished_at=COALESCE(excluded.finished_at,processes.finished_at)",
            (
                rid,
                pid,
                ptype,
                state,
                attempt,
                now,
                input_json,
                output_json,
                error_json,
                metadata_json,
                now,
                now,
                now,
                finished,
            ),
        )


def park_process(db_path: str | Path, **kwargs: Any) -> None:
    """Upsert an externally blocked process in the generic ``waiting`` state."""
    upsert_process(db_path, status="waiting", **kwargs)


def complete_waiting_process(
    db_path: str | Path,
    *,
    run_id: str,
    process_id: str,
    blocker_id: str | None = None,
    output: Mapping[str, Any] | None = None,
    process_status: str = "succeeded",
    blocker_status: str = "completed",
    run_status: str = "completed",
) -> LifecycleResult:
    """Atomically complete a process, optional homeostat row, and its run.

    Repeating an already-applied completion is a no-op.  ``blocker_id`` merely
    identifies a durable homeostat row; Fala assigns no application semantics to it.
    """
    db = _db(db_path)
    rid = _required(run_id, "run_id")
    pid = _required(process_id, "process_id")
    pstate = _required(process_status, "process_status")
    bstate = _required(blocker_status, "blocker_status")
    rstate = _required(run_status, "run_status")
    bid = _required(blocker_id, "blocker_id") if blocker_id is not None else None
    encoded = _json(output or {"completed": True}, "output")
    ensure_journal(db)
    now = _now()
    with sqlite3.connect(db, timeout=30.0) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT status FROM processes WHERE run_id=? AND id=?", (rid, pid)
        ).fetchone()
        if row is None:
            raise ValueError(f"fala journal: process {pid!r} not found")
        if row["status"] == pstate:
            current = conn.execute(
                "SELECT status FROM runs WHERE id=?", (rid,)
            ).fetchone()
            return {
                "run_id": rid,
                "process_id": pid,
                "changed": False,
                "process_status": pstate,
                "run_status": current["status"] if current else rstate,
            }
        conn.execute(
            "UPDATE processes SET status=?,output_json=?,updated_at=?,finished_at=? "
            "WHERE run_id=? AND id=?",
            (pstate, encoded, now, now, rid, pid),
        )
        if bid is not None:
            conn.execute(
                "UPDATE homeostats SET status=?,values_json=?,updated_at=? "
                "WHERE run_id=? AND id=?",
                (bstate, encoded, now, rid, bid),
            )
        conn.execute(
            "UPDATE runs SET status=?,updated_at=?,finished_at=? WHERE id=?",
            (rstate, now, now, rid),
        )
    return {
        "run_id": rid,
        "process_id": pid,
        "changed": True,
        "process_status": pstate,
        "run_status": rstate,
    }


__all__ = [
    "TERMINAL_PROCESS_STATUSES",
    "TERMINAL_RUN_STATUSES",
    "LifecycleResult",
    "complete_waiting_process",
    "ensure_journal",
    "finalize_run",
    "park_process",
    "transition_run",
    "upsert_process",
    "upsert_run_metadata",
]
