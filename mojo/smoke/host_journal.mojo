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


def _column_count(mut db: Connection, table: String) raises -> Int:
    var count = 0
    var stmt = db.query("PRAGMA table_info(" + table + ")")
    while stmt.step():
        count += 1
    stmt.close()
    return count


def _pk_columns(mut db: Connection, table: String) raises -> String:
    var result = String()
    var stmt = db.query("PRAGMA table_info(" + table + ")")
    while stmt.step():
        if stmt.column_int(5) > 0:
            if result != "":
                result += ","
            result += stmt.column_text(1)
    stmt.close()
    return result


def _has_object(mut db: Connection, kind: String, name: String) raises -> Bool:
    var stmt = db.query("SELECT 1 FROM sqlite_master WHERE type=? AND name=?")
    stmt.bind_text(1, kind)
    stmt.bind_text(2, name)
    var found = stmt.step()
    stmt.close()
    return found


def _check_rebuilt_contract(path: String) raises:
    var db = Connection(path)
    _check(_column_count(db, "runtime_events") == 13, "runtime_events columns")
    _check(_pk_columns(db, "runtime_events") == "run_id,sequence", "runtime_events PK")
    _check(_column_count(db, "processes") == 20, "processes columns")
    _check(_pk_columns(db, "processes") == "run_id,id", "processes PK")
    _check(_has_object(db, "index", "idx_runtime_events_process"), "event index")
    _check(_has_object(db, "index", "idx_processes_ready"), "process index")
    _check(_has_object(db, "trigger", "runtime_events_no_update"), "event trigger")
    var event_fk = db.query("PRAGMA foreign_key_list(runtime_events)")
    _check(event_fk.step() and event_fk.column_text(2) == "runtime_commands", "event FK")
    event_fk.close()
    var process_fk = db.query("PRAGMA foreign_key_list(processes)")
    _check(process_fk.step() and process_fk.column_text(2) == "impulses", "process FK")
    process_fk.close()
    db.close()


def _legacy_runtime_events_rebuild() raises:
    var path = "/tmp/fala-host-journal-199-events.sqlite"
    _cleanup(path)
    var db = Connection(path)
    db.execute("""CREATE TABLE runtime_events (
        run_id TEXT NOT NULL, sequence INTEGER NOT NULL, id TEXT PRIMARY KEY,
        event_type TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL)""")
    db.close()
    ensure_host_journal(path)
    _check_rebuilt_contract(path)
    _cleanup(path)


def _legacy_processes_rebuild() raises:
    var path = "/tmp/fala-host-journal-199-processes.sqlite"
    _cleanup(path)
    var db = Connection(path)
    db.execute("""CREATE TABLE processes (
        run_id TEXT NOT NULL, id TEXT PRIMARY KEY, process_type TEXT NOT NULL,
        impulse_id TEXT, status TEXT NOT NULL, priority INTEGER NOT NULL,
        attempt INTEGER NOT NULL, max_attempts INTEGER NOT NULL,
        available_at TEXT NOT NULL, lease_owner TEXT, lease_expires_at TEXT,
        input_json TEXT NOT NULL, output_json TEXT NOT NULL, error_json TEXT NOT NULL,
        metadata TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        started_at TEXT, finished_at TEXT)""")
    db.close()
    ensure_host_journal(path)
    _check_rebuilt_contract(path)
    _cleanup(path)


def _malformed_lookalike_fails_closed() raises:
    var path = "/tmp/fala-host-journal-199-malformed.sqlite"
    _cleanup(path)
    var db = Connection(path)
    db.execute("CREATE TABLE runtime_events (run_id TEXT NOT NULL, id TEXT PRIMARY KEY)")
    db.close()
    var failed = False
    try:
        ensure_host_journal(path)
    except err:
        failed = True
    _check(failed, "malformed lookalike rejected")
    var unchanged = Connection(path)
    _check(_column_count(unchanged, "runtime_events") == 2, "malformed table unchanged")
    var version = unchanged.query("PRAGMA user_version")
    _check(version.step() and version.column_int(0) == 0, "malformed version unchanged")
    version.close()
    unchanged.close()
    _cleanup(path)


def main() raises:
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
    _legacy_runtime_events_rebuild()
    _legacy_processes_rebuild()
    _malformed_lookalike_fails_closed()
    print("host journal native writes and legacy schema rebuilds ok")
