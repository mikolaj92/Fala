"""Safe, domain-blind lifecycle operations for schema-v6 Fala journals."""

from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TypedDict

from fala.host import _ensure_durable_schema

RUN_STATUSES = frozenset({"created", "active", "waiting", "completed", "failed", "cancel_requested", "cancelled", "timed_out"})
PROCESS_STATUSES = frozenset({"pending", "ready", "running", "waiting", "retry_wait", "cancel_requested", "succeeded", "failed", "cancelled", "timed_out"})
BLOCKER_STATUSES = frozenset({"open", "completed", "cancelled", "expired"})
TERMINAL_RUN_STATUSES = frozenset({"completed", "failed", "cancelled", "timed_out"})
TERMINAL_PROCESS_STATUSES = frozenset({"succeeded", "failed", "cancelled", "timed_out"})
TERMINAL_BLOCKER_STATUSES = frozenset({"completed", "cancelled", "expired"})
_BLOCKER_PROCESS_TERMINALS = {"completed": "succeeded", "cancelled": "cancelled", "expired": "timed_out"}
_PROCESS_RUN_TERMINALS = {"succeeded": "completed", "cancelled": "cancelled", "timed_out": "timed_out"}
_ENSURE_LOCK = threading.Lock()

_RUN_TRANSITIONS = {
    "created": RUN_STATUSES - {"created"},
    "active": frozenset({"waiting", "completed", "failed", "cancel_requested", "cancelled", "timed_out"}),
    "waiting": frozenset({"active", "completed", "failed", "cancel_requested", "cancelled", "timed_out"}),
    "cancel_requested": frozenset({"cancelled", "failed", "timed_out"}),
}
_PROCESS_TRANSITIONS = {
    "pending": frozenset({"ready", "cancel_requested", "cancelled", "timed_out"}),
    "ready": frozenset({"running", "cancel_requested", "cancelled", "timed_out"}),
    "running": frozenset({"succeeded", "failed", "waiting", "retry_wait", "cancel_requested", "cancelled", "timed_out"}),
    "waiting": frozenset({"succeeded", "failed", "cancel_requested", "cancelled", "timed_out"}),
    "retry_wait": frozenset({"ready", "cancel_requested", "cancelled", "timed_out"}),
    "cancel_requested": frozenset({"cancelled", "failed", "timed_out"}),
}

class LifecycleResult(TypedDict):
    run_id: str
    process_id: str
    changed: bool
    process_status: str
    run_status: str


def _db(db_path: str | Path) -> Path:
    return Path(db_path).expanduser().resolve()


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _required(value: str, label: str) -> str:
    result = str(value).strip()
    if not result:
        raise ValueError(f"fala journal: {label} must not be blank")
    return result


def _status(value: str, label: str, allowed: frozenset[str]) -> str:
    result = _required(value, label)
    if result not in allowed:
        raise ValueError(f"fala journal: invalid {label} {result!r}")
    return result


def _json(value: Mapping[str, Any] | None, label: str) -> str:
    try:
        return json.dumps(dict(value or {}), ensure_ascii=False, allow_nan=False, sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise TypeError(f"fala journal: {label} is not JSON-recordable") from exc


def _connect(db: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(db, timeout=30.0)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn

# Canonical schema-v6 structural contract, mirrored from mojo/fala/schema.mojo.
_SCHEMA_SHAPES = {'runs': [('id', 'TEXT', 0, None, 1),
          ('status', 'TEXT', 1, None, 0),
          ('title', 'TEXT', 0, None, 0),
          ('package_id', 'TEXT', 0, None, 0),
          ('package_version', 'TEXT', 0, None, 0),
          ('package_digest', 'TEXT', 0, None, 0),
          ('correlation_path_id', 'TEXT', 0, None, 0),
          ('correlation_path_digest', 'TEXT', 0, None, 0),
          ('runtime_version', 'TEXT', 0, None, 0),
          ('backend_version', 'TEXT', 0, None, 0),
          ('schema_version', 'INTEGER', 1, None, 0),
          ('metadata', 'TEXT', 1, None, 0),
          ('created_at', 'TEXT', 1, None, 0),
          ('updated_at', 'TEXT', 1, None, 0),
          ('started_at', 'TEXT', 0, None, 0),
          ('finished_at', 'TEXT', 0, None, 0)],
 'schema_migrations': [('id', 'TEXT', 0, None, 1),
                       ('version', 'INTEGER', 1, None, 0),
                       ('name', 'TEXT', 1, None, 0),
                       ('applied_at', 'TEXT', 1, None, 0)],
 'impulses': [('run_id', 'TEXT', 1, None, 1),
              ('id', 'TEXT', 1, None, 2),
              ('impulse_type', 'TEXT', 1, None, 0),
              ('payload', 'TEXT', 1, None, 0),
              ('metadata', 'TEXT', 1, None, 0),
              ('created_at', 'TEXT', 1, None, 0),
              ('updated_at', 'TEXT', 1, None, 0)],
 'impulse_types': [('run_id', 'TEXT', 1, None, 1),
                   ('id', 'TEXT', 1, None, 2),
                   ('title', 'TEXT', 0, None, 0),
                   ('description', 'TEXT', 0, None, 0),
                   ('media_types', 'TEXT', 1, None, 0),
                   ('value_schema_json', 'TEXT', 1, None, 0),
                   ('metadata', 'TEXT', 1, None, 0),
                   ('created_at', 'TEXT', 1, None, 0),
                   ('updated_at', 'TEXT', 1, None, 0)],
 'impulse_relations': [('run_id', 'TEXT', 1, None, 1),
                       ('id', 'TEXT', 1, None, 2),
                       ('relation_type', 'TEXT', 1, None, 0),
                       ('source_impulse_id', 'TEXT', 1, None, 0),
                       ('target_impulse_id', 'TEXT', 1, None, 0),
                       ('metadata', 'TEXT', 1, None, 0),
                       ('created_at', 'TEXT', 1, None, 0)],
 'runtime_commands': [('run_id', 'TEXT', 1, None, 1),
                      ('id', 'TEXT', 1, None, 2),
                      ('command_type', 'TEXT', 1, None, 0),
                      ('idempotency_key', 'TEXT', 1, None, 0),
                      ('actor', 'TEXT', 0, None, 0),
                      ('correlation_id', 'TEXT', 0, None, 0),
                      ('causation_id', 'TEXT', 0, None, 0),
                      ('payload', 'TEXT', 1, None, 0),
                      ('created_at', 'TEXT', 1, None, 0)],
 'runtime_events': [('run_id', 'TEXT', 1, None, 1),
                    ('sequence', 'INTEGER', 1, None, 2),
                    ('id', 'TEXT', 1, None, 0),
                    ('event_type', 'TEXT', 1, None, 0),
                    ('schema_version', 'INTEGER', 1, '1', 0),
                    ('impulse_id', 'TEXT', 0, None, 0),
                    ('process_id', 'TEXT', 0, None, 0),
                    ('command_id', 'TEXT', 0, None, 0),
                    ('actor', 'TEXT', 0, None, 0),
                    ('correlation_id', 'TEXT', 0, None, 0),
                    ('causation_id', 'TEXT', 0, None, 0),
                    ('payload', 'TEXT', 1, None, 0),
                    ('created_at', 'TEXT', 1, None, 0)],
 'associations': [('run_id', 'TEXT', 1, None, 1),
                  ('id', 'TEXT', 1, None, 2),
                  ('kind', 'TEXT', 1, None, 0),
                  ('impulse_id', 'TEXT', 0, None, 0),
                  ('values_json', 'TEXT', 1, None, 0),
                  ('metadata', 'TEXT', 1, None, 0),
                  ('created_at', 'TEXT', 1, None, 0)],
 'reactions': [('run_id', 'TEXT', 1, None, 1),
               ('id', 'TEXT', 1, None, 2),
               ('kind', 'TEXT', 1, None, 0),
               ('uri', 'TEXT', 1, None, 0),
               ('impulse_id', 'TEXT', 0, None, 0),
               ('media_type', 'TEXT', 0, None, 0),
               ('size_bytes', 'INTEGER', 0, None, 0),
               ('content_hash', 'TEXT', 0, None, 0),
               ('metadata', 'TEXT', 1, None, 0),
               ('created_at', 'TEXT', 1, None, 0)],
 'processes': [('run_id', 'TEXT', 1, None, 1),
               ('id', 'TEXT', 1, None, 2),
               ('process_type', 'TEXT', 1, None, 0),
               ('impulse_id', 'TEXT', 0, None, 0),
               ('status', 'TEXT', 1, None, 0),
               ('priority', 'INTEGER', 1, None, 0),
               ('attempt', 'INTEGER', 1, None, 0),
               ('max_attempts', 'INTEGER', 1, None, 0),
               ('available_at', 'TEXT', 1, None, 0),
               ('lease_owner', 'TEXT', 0, None, 0),
               ('lease_expires_at', 'TEXT', 0, None, 0),
               ('input_json', 'TEXT', 1, None, 0),
               ('output_json', 'TEXT', 1, None, 0),
               ('error_json', 'TEXT', 1, None, 0),
               ('metadata', 'TEXT', 1, None, 0),
               ('created_at', 'TEXT', 1, None, 0),
               ('updated_at', 'TEXT', 1, None, 0),
               ('started_at', 'TEXT', 0, None, 0),
               ('finished_at', 'TEXT', 0, None, 0),
               ('output_schema_json', 'TEXT', 1, "'{}'", 0)],
 'homeostats': [('run_id', 'TEXT', 1, None, 1),
                ('id', 'TEXT', 1, None, 2),
                ('kind', 'TEXT', 1, None, 0),
                ('impulse_id', 'TEXT', 0, None, 0),
                ('status', 'TEXT', 1, None, 0),
                ('values_json', 'TEXT', 1, None, 0),
                ('metadata', 'TEXT', 1, None, 0),
                ('attempt', 'INTEGER', 1, '0', 0),
                ('max_attempts', 'INTEGER', 1, '1', 0),
                ('created_at', 'TEXT', 1, None, 0),
                ('updated_at', 'TEXT', 1, None, 0)],
 'projections': [('run_id', 'TEXT', 1, None, 1),
                 ('name', 'TEXT', 1, None, 2),
                 ('id', 'TEXT', 1, None, 0),
                 ('version', 'INTEGER', 1, None, 0),
                 ('data', 'TEXT', 1, None, 0),
                 ('source_event_sequence', 'INTEGER', 1, None, 0),
                 ('updated_at', 'TEXT', 1, None, 0)],
 'bridge_outbox': [('run_id', 'TEXT', 1, None, 1),
                   ('id', 'TEXT', 1, None, 2),
                   ('idempotency_key', 'TEXT', 1, None, 0),
                   ('source_ref', 'TEXT', 1, None, 0),
                   ('target_ref', 'TEXT', 1, None, 0),
                   ('impulse_json', 'TEXT', 1, None, 0),
                   ('event_ref', 'TEXT', 0, None, 0),
                   ('pool_id', 'TEXT', 0, None, 0),
                   ('budget', 'TEXT', 1, None, 0),
                   ('status', 'TEXT', 1, None, 0),
                   ('attempts', 'INTEGER', 1, None, 0),
                   ('metadata', 'TEXT', 1, None, 0),
                   ('created_at', 'TEXT', 1, None, 0),
                   ('updated_at', 'TEXT', 1, None, 0)],
 'bridge_inbox': [('run_id', 'TEXT', 1, None, 1),
                  ('id', 'TEXT', 1, None, 2),
                  ('idempotency_key', 'TEXT', 1, None, 0),
                  ('source_ref', 'TEXT', 1, None, 0),
                  ('target_ref', 'TEXT', 1, None, 0),
                  ('impulse_json', 'TEXT', 1, None, 0),
                  ('event_ref', 'TEXT', 0, None, 0),
                  ('pool_id', 'TEXT', 0, None, 0),
                  ('budget', 'TEXT', 1, None, 0),
                  ('status', 'TEXT', 1, None, 0),
                  ('attempts', 'INTEGER', 1, None, 0),
                  ('metadata', 'TEXT', 1, None, 0),
                  ('created_at', 'TEXT', 1, None, 0),
                  ('updated_at', 'TEXT', 1, None, 0)]}
_SCHEMA_INDEXES = {'idx_associations_impulse': ('run_id', 'impulse_id', 'created_at'),
 'idx_bridge_inbox_status': ('run_id', 'status', 'updated_at'),
 'idx_bridge_outbox_status': ('run_id', 'status', 'updated_at'),
 'idx_homeostats_status': ('run_id', 'status', 'updated_at'),
 'idx_impulse_relations_source': ('run_id', 'source_impulse_id', 'relation_type'),
 'idx_impulse_relations_target': ('run_id', 'target_impulse_id', 'relation_type'),
 'idx_processes_impulse': ('run_id', 'impulse_id', 'status'),
 'idx_processes_ready': ('status', 'available_at', 'priority', 'created_at'),
 'idx_processes_run_status': ('run_id', 'status', 'updated_at'),
 'idx_reactions_impulse': ('run_id', 'impulse_id', 'kind', 'created_at'),
 'idx_runs_status': ('status', 'updated_at'),
 'idx_runtime_events_impulse': ('run_id', 'impulse_id', 'sequence'),
 'idx_runtime_events_process': ('run_id', 'process_id', 'sequence')}
_SCHEMA_FOREIGN_KEYS = {'impulse_relations': [('impulses', 'run_id', 'run_id', 'NO ACTION', 'NO ACTION', 'NONE'),
                       ('impulses', 'target_impulse_id', 'id', 'NO ACTION', 'NO ACTION', 'NONE'),
                       ('impulses', 'run_id', 'run_id', 'NO ACTION', 'NO ACTION', 'NONE'),
                       ('impulses', 'source_impulse_id', 'id', 'NO ACTION', 'NO ACTION', 'NONE')],
 'processes': [('impulses', 'run_id', 'run_id', 'NO ACTION', 'NO ACTION', 'NONE'),
               ('impulses', 'impulse_id', 'id', 'NO ACTION', 'NO ACTION', 'NONE')],
 'reactions': [('impulses', 'run_id', 'run_id', 'NO ACTION', 'NO ACTION', 'NONE'),
               ('impulses', 'impulse_id', 'id', 'NO ACTION', 'NO ACTION', 'NONE')],
 'runtime_events': [('runtime_commands', 'run_id', 'run_id', 'NO ACTION', 'NO ACTION', 'NONE'),
                    ('runtime_commands', 'command_id', 'id', 'NO ACTION', 'NO ACTION', 'NONE')]}
_SCHEMA_TRIGGERS = {'runtime_commands_no_delete': 'create trigger runtime_commands_no_delete before delete on runtime_commands begin select raise(abort, '
                               "'runtime_commands is append-only'); end",
 'runtime_commands_no_update': 'create trigger runtime_commands_no_update before update on runtime_commands begin select raise(abort, '
                               "'runtime_commands is append-only'); end",
 'runtime_events_no_delete': 'create trigger runtime_events_no_delete before delete on runtime_events begin select raise(abort, '
                             "'runtime_events is append-only'); end",
 'runtime_events_no_update': 'create trigger runtime_events_no_update before update on runtime_events begin select raise(abort, '
                             "'runtime_events is append-only'); end"}

_SCHEMA_UNIQUES = {
    "runtime_commands": {("run_id", "idempotency_key")},
    "runtime_events": {("run_id", "id")},
    "bridge_outbox": {("run_id", "idempotency_key")},
    "bridge_inbox": {("run_id", "idempotency_key")},
}


def _shape(conn: sqlite3.Connection, table: str) -> list[tuple[Any, ...]]:
    return [(r[1], str(r[2]).upper(), r[3], r[4], r[5]) for r in conn.execute(f"PRAGMA table_info({table})")]


def _foreign_keys(conn: sqlite3.Connection, table: str) -> list[tuple[Any, ...]]:
    return [tuple(r[2:8]) for r in conn.execute(f"PRAGMA foreign_key_list({table})")]


def _index_columns(conn: sqlite3.Connection, name: str) -> tuple[str, ...]:
    return tuple(r[2] for r in conn.execute(f"PRAGMA index_info({name})"))


def _validate_schema(conn: sqlite3.Connection, *, before_migration: bool) -> None:
    objects = {(r[0], r[1]): r[2] for r in conn.execute(
        "SELECT type,name,sql FROM sqlite_master WHERE type IN ('table','index','trigger')"
    )}
    for table, expected in _SCHEMA_SHAPES.items():
        if ("table", table) not in objects:
            if before_migration:
                continue
            raise RuntimeError(f"fala journal: schema-v6 table {table!r} is missing")
        actual = _shape(conn, table)
        allowed = [expected]
        # These are the only legacy shapes the native v6 migration repairs.
        if before_migration and table == "processes":
            allowed.append(expected[:-1])
        if before_migration and table == "homeostats":
            allowed.extend([expected[:7] + expected[9:], expected[:8] + expected[9:]])
        if before_migration and table == "runtime_events":
            required = {"run_id", "sequence", "id", "event_type", "payload", "created_at"}
            expected_by_name = {column[0]: column for column in expected}
            actual_names = {column[0] for column in actual}
            legacy_ok = required <= actual_names <= set(expected_by_name) and all(
                column == expected_by_name[column[0]] for column in actual
            )
            if legacy_ok:
                allowed.append(actual)
        if not any(sorted(actual) == sorted(candidate) for candidate in allowed):
            raise RuntimeError(f"fala journal: incompatible {table} table; refusing schema-v6 write")
        expected_fks = _SCHEMA_FOREIGN_KEYS.get(table, [])
        if _foreign_keys(conn, table) != expected_fks:
            raise RuntimeError(f"fala journal: incompatible {table} foreign keys; refusing schema-v6 write")
        uniques = {
            _index_columns(conn, row[1])
            for row in conn.execute(f"PRAGMA index_list({table})")
            if row[3] == "u"
        }
        if uniques != _SCHEMA_UNIQUES.get(table, set()):
            raise RuntimeError(f"fala journal: incompatible {table} unique constraints; refusing schema-v6 write")
    for name, columns in _SCHEMA_INDEXES.items():
        if ("index", name) not in objects:
            if before_migration:
                continue
            raise RuntimeError(f"fala journal: schema-v6 index {name!r} is missing")
        if _index_columns(conn, name) != columns:
            raise RuntimeError(f"fala journal: incompatible index {name}; refusing schema-v6 write")
    for name, expected_sql in _SCHEMA_TRIGGERS.items():
        if ("trigger", name) not in objects:
            if before_migration:
                continue
            raise RuntimeError(f"fala journal: schema-v6 trigger {name!r} is missing")
        actual_sql = " ".join((objects[("trigger", name)] or "").lower().split())
        if actual_sql != expected_sql:
            raise RuntimeError(f"fala journal: incompatible trigger {name}; refusing schema-v6 write")


def _validate_v6(conn: sqlite3.Connection) -> None:
    _validate_schema(conn, before_migration=False)
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    row = conn.execute("SELECT version FROM schema_migrations WHERE id='runtime_backend'").fetchone()
    if version != 6 or row is None or row[0] != 6:
        raise RuntimeError("fala journal: schema-v6 initialization incomplete")


def ensure_journal(db_path: str | Path) -> None:
    """Ensure and validate schema v6 on every call (no pathname cache)."""
    db = _db(db_path)
    db.parent.mkdir(parents=True, exist_ok=True)
    with _ENSURE_LOCK:
        if db.exists():
            with _connect(db) as conn:
                _validate_schema(conn, before_migration=True)
        _ensure_durable_schema(db)
        with _connect(db) as conn:
            _validate_v6(conn)


def _require_transition(current: str, target: str, transitions: dict[str, frozenset[str]], kind: str) -> None:
    if current == target:
        return
    if target not in transitions.get(current, frozenset()):
        raise ValueError(f"fala journal: unsafe {kind} transition {current!r} -> {target!r}")


def upsert_run_metadata(db_path: str | Path, *, run_id: str, metadata: Mapping[str, Any], title: str | None = None, status: str = "active") -> None:
    db, rid = _db(db_path), _required(run_id, "run_id")
    state, encoded = _status(status, "status", RUN_STATUSES), _json(metadata, "metadata")
    ensure_journal(db); now = _now()
    with _connect(db) as conn:
        conn.row_factory = sqlite3.Row; conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT status,title,metadata FROM runs WHERE id=?", (rid,)).fetchone()
        if row is None:
            conn.execute("INSERT INTO runs (id,status,title,schema_version,metadata,created_at,updated_at,finished_at) VALUES (?,?,?,?,?,?,?,?)", (rid,state,title or rid,6,encoded,now,now,now if state in TERMINAL_RUN_STATUSES else None))
            return
        if row["status"] in TERMINAL_RUN_STATUSES:
            effective_title = title if title is not None else row["title"]
            if (state, effective_title, encoded) == (row["status"], row["title"], row["metadata"]): return
            raise ValueError(f"fala journal: terminal run {rid!r} cannot be overwritten or reopened")
        _require_transition(row["status"], state, _RUN_TRANSITIONS, "run")
        conn.execute("UPDATE runs SET status=?,title=COALESCE(?,title),metadata=?,updated_at=?,finished_at=? WHERE id=?", (state,title,encoded,now,now if state in TERMINAL_RUN_STATUSES else None,rid))


def transition_run(db_path: str | Path, *, run_id: str, status: str, metadata_updates: Mapping[str, Any] | None = None) -> None:
    db, rid = _db(db_path), _required(run_id, "run_id")
    state = _status(status, "status", RUN_STATUSES); updates = dict(metadata_updates or {}); _json(updates, "metadata_updates")
    ensure_journal(db); now = _now()
    with _connect(db) as conn:
        conn.row_factory=sqlite3.Row; conn.execute("BEGIN IMMEDIATE")
        row=conn.execute("SELECT status,metadata FROM runs WHERE id=?",(rid,)).fetchone()
        if row is None: raise ValueError(f"fala journal: run {rid!r} not found")
        _require_transition(row["status"],state,_RUN_TRANSITIONS,"run")
        try: metadata=json.loads(row["metadata"] or "{}")
        except (TypeError,json.JSONDecodeError): metadata={}
        if not isinstance(metadata,dict): metadata={}
        metadata.update(updates); encoded=_json(metadata,"metadata")
        if row["status"] == state and encoded == row["metadata"]: return
        if row["status"] in TERMINAL_RUN_STATUSES: raise ValueError(f"fala journal: terminal run {rid!r} cannot be overwritten")
        conn.execute("UPDATE runs SET status=?,metadata=?,updated_at=?,finished_at=? WHERE id=?",(state,encoded,now,now if state in TERMINAL_RUN_STATUSES else None,rid))


def finalize_run(db_path: str | Path, *, run_id: str, status: str, reason: str | None = None) -> None:
    state = _status(status,"status",TERMINAL_RUN_STATUSES)
    transition_run(db_path,run_id=run_id,status=state,metadata_updates={"finalize_reason":reason} if reason else None)


def upsert_process(db_path: str | Path, *, run_id: str, process_id: str, status: str, output: Mapping[str, Any] | None=None, error: Mapping[str, Any] | None=None, metadata: Mapping[str, Any] | None=None, inputs: Mapping[str, Any] | None=None, attempt: int=1, process_type: str="external") -> None:
    db,rid,pid=_db(db_path),_required(run_id,"run_id"),_required(process_id,"process_id")
    state=_status(status,"status",PROCESS_STATUSES); ptype=_required(process_type,"process_type")
    if isinstance(attempt,bool) or not isinstance(attempt,int) or attempt<1: raise ValueError("fala journal: attempt must be a positive integer")
    values=(_json(inputs,"inputs"),_json(output,"output"),_json(error,"error"),_json(metadata,"metadata"))
    ensure_journal(db); now=_now(); finished=now if state in TERMINAL_PROCESS_STATUSES else None
    with _connect(db) as conn:
        conn.row_factory=sqlite3.Row; conn.execute("BEGIN IMMEDIATE")
        if conn.execute("SELECT 1 FROM runs WHERE id=?",(rid,)).fetchone() is None: raise ValueError(f"fala journal: run {rid!r} not found")
        row=conn.execute("SELECT * FROM processes WHERE run_id=? AND id=?",(rid,pid)).fetchone()
        if row is not None:
            supplied=(state,ptype,attempt,*values); durable=(row["status"],row["process_type"],row["attempt"],row["input_json"],row["output_json"],row["error_json"],row["metadata"])
            if row["status"] in TERMINAL_PROCESS_STATUSES:
                if supplied == durable:
                    if row["lease_owner"] is not None or row["lease_expires_at"] is not None:
                        conn.execute("UPDATE processes SET lease_owner=NULL,lease_expires_at=NULL WHERE run_id=? AND id=?", (rid, pid))
                    return
                raise ValueError(f"fala journal: terminal process {pid!r} cannot be overwritten or reopened")
            _require_transition(row["status"],state,_PROCESS_TRANSITIONS,"process")
            if state == "waiting" and (row["lease_owner"] is not None or row["lease_expires_at"] is not None):
                raise ValueError(f"fala journal: leased process {pid!r} cannot be upserted as waiting")
            conn.execute("UPDATE processes SET process_type=?,status=?,attempt=?,input_json=?,output_json=?,error_json=?,metadata=?,lease_owner=NULL,lease_expires_at=NULL,updated_at=?,finished_at=? WHERE run_id=? AND id=?",(ptype,state,attempt,*values,now,finished,rid,pid)); return
        conn.execute("INSERT INTO processes (run_id,id,process_type,status,priority,attempt,max_attempts,available_at,input_json,output_json,error_json,metadata,created_at,updated_at,started_at,finished_at,output_schema_json) VALUES (?,?,?,?,0,?,1,?,?,?,?,?,?,?,?,?,'{}')",(rid,pid,ptype,state,attempt,now,*values,now,now,now,finished))


def park_process(db_path: str | Path, **kwargs: Any) -> None:
    upsert_process(db_path,status="waiting",**kwargs)


def complete_waiting_process(db_path: str | Path, *, run_id: str, process_id: str, blocker_id: str | None=None, output: Mapping[str, Any] | None=None, process_status: str="succeeded", blocker_status: str="completed", run_status: str="completed") -> LifecycleResult:
    db,rid,pid=_db(db_path),_required(run_id,"run_id"),_required(process_id,"process_id")
    pstate=_status(process_status,"process_status",TERMINAL_PROCESS_STATUSES); rstate=_status(run_status,"run_status",TERMINAL_RUN_STATUSES); bstate=_status(blocker_status,"blocker_status",TERMINAL_BLOCKER_STATUSES)
    if _BLOCKER_PROCESS_TERMINALS[bstate] != pstate:
        raise ValueError(f"fala journal: blocker/process terminal pairing {bstate!r}/{pstate!r} is invalid")
    if _PROCESS_RUN_TERMINALS[pstate] != rstate:
        raise ValueError(f"fala journal: process/run terminal pairing {pstate!r}/{rstate!r} is invalid")
    bid=_required(blocker_id,"blocker_id") if blocker_id is not None else None; encoded=_json({"completed": True} if output is None else output,"output")
    ensure_journal(db); now=_now()
    with _connect(db) as conn:
        conn.row_factory=sqlite3.Row; conn.execute("BEGIN IMMEDIATE")
        process=conn.execute("SELECT status,output_json,lease_owner,lease_expires_at FROM processes WHERE run_id=? AND id=?",(rid,pid)).fetchone()
        if process is None: raise ValueError(f"fala journal: process {pid!r} not found")
        run=conn.execute("SELECT status FROM runs WHERE id=?",(rid,)).fetchone()
        if run is None: raise ValueError(f"fala journal: run {rid!r} not found")
        blocker=conn.execute("SELECT status,values_json FROM homeostats WHERE run_id=? AND id=?",(rid,bid)).fetchone() if bid else None
        if bid and blocker is None: raise ValueError(f"fala journal: blocker {bid!r} not found")
        exact=(process["status"]==pstate and process["output_json"]==encoded and run["status"]==rstate and (not bid or (blocker["status"]==bstate and blocker["values_json"]==encoded)))
        if process["status"] != "waiting":
            if exact:
                changed = process["lease_owner"] is not None or process["lease_expires_at"] is not None
                if changed:
                    conn.execute("UPDATE processes SET lease_owner=NULL,lease_expires_at=NULL WHERE run_id=? AND id=?", (rid, pid))
                return {"run_id":rid,"process_id":pid,"changed":changed,"process_status":pstate,"run_status":rstate}
            raise ValueError("fala journal: completion conflicts with durable lifecycle state")
        if process["lease_owner"] is not None or process["lease_expires_at"] is not None:
            raise ValueError("fala journal: waiting process must be wholly unleased before completion")
        if run["status"] in TERMINAL_RUN_STATUSES and run["status"] != rstate: raise ValueError("fala journal: completion conflicts with terminal run")
        if bid and blocker["status"] != "open" and not (blocker["status"]==bstate and blocker["values_json"]==encoded): raise ValueError("fala journal: completion conflicts with terminal blocker")
        cur=conn.execute("UPDATE processes SET status=?,output_json=?,lease_owner=NULL,lease_expires_at=NULL,updated_at=?,finished_at=? WHERE run_id=? AND id=? AND status='waiting' AND lease_owner IS NULL AND lease_expires_at IS NULL",(pstate,encoded,now,now,rid,pid))
        if cur.rowcount != 1: raise ValueError("fala journal: process completion lost compare-and-set")
        if bid and blocker["status"] == "open":
            cur=conn.execute("UPDATE homeostats SET status=?,values_json=?,updated_at=? WHERE run_id=? AND id=? AND status='open'",(bstate,encoded,now,rid,bid))
            if cur.rowcount != 1: raise ValueError("fala journal: blocker completion lost compare-and-set")
        cur=conn.execute("UPDATE runs SET status=?,updated_at=?,finished_at=? WHERE id=? AND status NOT IN ('completed','failed','cancelled','timed_out')",(rstate,now,now,rid))
        if cur.rowcount != 1 and run["status"] != rstate: raise ValueError("fala journal: run completion lost compare-and-set")
    return {"run_id":rid,"process_id":pid,"changed":True,"process_status":pstate,"run_status":rstate}

__all__=["RUN_STATUSES","PROCESS_STATUSES","BLOCKER_STATUSES","TERMINAL_PROCESS_STATUSES","TERMINAL_RUN_STATUSES","LifecycleResult","complete_waiting_process","ensure_journal","finalize_run","park_process","transition_run","upsert_process","upsert_run_metadata"]
