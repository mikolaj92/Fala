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
    print("host journal native writes ok")
