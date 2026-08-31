from fala.host_journal import (
    complete_waiting_process,
    upsert_process,
    upsert_run_metadata,
)
from fala.schema_contract import ensure_host_journal
from fala.sqlite import Connection
from std.os import remove


def _cleanup(path: String):
    try:
        remove(path)
    except err:
        pass
    try:
        remove(path + "-wal")
    except err:
        pass
    try:
        remove(path + "-shm")
    except err:
        pass


def _check(ok: Bool, msg: String) raises:
    if not ok:
        raise Error("host journal smoke: " + msg)


def _has_column(mut db: Connection, table: String, column: String) raises -> Bool:
    var stmt = db.query("PRAGMA table_info(" + table + ")")
    var found = False
    while stmt.step():
        if stmt.column_text(1) == column:
            found = True
    stmt.close()
    return found


def _object_exists(mut db: Connection, kind: String, name: String) raises -> Bool:
    var stmt = db.query("SELECT 1 FROM sqlite_master WHERE type=? AND name=?")
    stmt.bind_text(1, kind)
    stmt.bind_text(2, name)
    var found = stmt.step()
    stmt.close()
    return found


def _check_legacy_runtime_events_rebuild(path: String) raises:
    _cleanup(path)
    var db = Connection(path)
    db.execute("""CREATE TABLE runtime_events (
        run_id TEXT NOT NULL, sequence INTEGER NOT NULL, id TEXT PRIMARY KEY,
        event_type TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL
    ); INSERT INTO runtime_events VALUES ('run', 7, 'event', 'legacy', '{}', 't');""")
    db.close()
    ensure_host_journal(path)
    db = Connection(path)
    var row = db.query(
        "SELECT sequence, schema_version FROM runtime_events WHERE id='event'"
    )
    _check(row.step() and row.column_int(0) == 7 and row.column_int(1) == 1,
           "legacy runtime_events row preserved")
    row.close()
    _check(_has_column(db, "runtime_events", "command_id"),
           "legacy runtime_events rebuilt with canonical columns")
    _check(_object_exists(db, "index", "idx_runtime_events_process"),
           "legacy runtime_events canonical indexes")
    _check(_object_exists(db, "trigger", "runtime_events_no_update"),
           "legacy runtime_events canonical triggers")
    db.close()
    _cleanup(path)


def _check_legacy_processes_rebuild(path: String) raises:
    _cleanup(path)
    var db = Connection(path)
    db.execute("""CREATE TABLE processes (
        run_id TEXT NOT NULL, id TEXT PRIMARY KEY, process_type TEXT NOT NULL,
        impulse_id TEXT, status TEXT NOT NULL, priority INTEGER NOT NULL,
        attempt INTEGER NOT NULL, max_attempts INTEGER NOT NULL,
        available_at TEXT NOT NULL, lease_owner TEXT, lease_expires_at TEXT,
        input_json TEXT NOT NULL, output_json TEXT NOT NULL, error_json TEXT NOT NULL,
        metadata TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        started_at TEXT, finished_at TEXT
    ); INSERT INTO processes VALUES (
        'run', 'process', 'manual', NULL, 'waiting', 1, 0, 1, 't', NULL, NULL,
        '{}', '{}', '{}', '{}', 't', 't', NULL, NULL
    );""")
    db.close()
    ensure_host_journal(path)
    db = Connection(path)
    var row = db.query("SELECT output_schema_json FROM processes WHERE id='process'")
    _check(row.step() and row.column_text(0) == "{}",
           "legacy process row preserved with canonical default")
    row.close()
    _check(_has_column(db, "processes", "output_schema_json"),
           "legacy processes rebuilt with canonical columns")
    _check(_object_exists(db, "index", "idx_processes_ready"),
           "legacy processes canonical indexes")
    db.close()
    _cleanup(path)


def _check_malformed_lookalike(path: String) raises:
    _cleanup(path)
    var db = Connection(path)
    db.execute("CREATE TABLE runtime_events (run_id TEXT NOT NULL, id TEXT PRIMARY KEY)")
    db.close()
    var refused = False
    try:
        ensure_host_journal(path)
    except err:
        refused = True
    _check(refused, "malformed legacy lookalike refused")
    db = Connection(path)
    _check(not _has_column(db, "runtime_events", "sequence"),
           "malformed legacy lookalike left unchanged")
    db.close()
    _cleanup(path)


def main() raises:
    _check_legacy_runtime_events_rebuild("/tmp/fala-host-journal-199-events.sqlite")
    _check_legacy_processes_rebuild("/tmp/fala-host-journal-199-processes.sqlite")
    _check_malformed_lookalike("/tmp/fala-host-journal-199-malformed.sqlite")
    var path = "/tmp/fala-host-journal-189.sqlite"
    _cleanup(path)
    ensure_host_journal(path)
    upsert_run_metadata(path, "run", "waiting", "{}", "t", "Run", True)
    upsert_process(path, "run", "process", "waiting", "manual", 1, "{}", "{}", "{}", "{}", "t")
    var result = complete_waiting_process(
        path, "run", "process", "succeeded", "completed", "", False, "completed",
        "{\"approved\":true}", "t",
    )
    _check(result.find("\"changed\":true") >= 0, "completion changes")
    var db = Connection(path)
    var run = db.query("SELECT status FROM runs WHERE id='run'")
    _check(run.step() and run.column_text(0) == "completed", "run completed")
    run.close()
    var process = db.query("SELECT status FROM processes WHERE id='process'")
    _check(process.step() and process.column_text(0) == "succeeded", "process succeeded")
    process.close()
    db.close()
    _cleanup(path)
    print("host journal native writes ok")
