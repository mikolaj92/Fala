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
    _malformed_lookalike_fails_closed()
    print("host journal native writes and current-schema ensure ok")
