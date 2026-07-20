"""#68 rearm_homeostat: waiting run + terminal homeostat/process, atomic reopen."""

from fala.journal import NativeJournal
from fala.native_driver import rearm_homeostat
from std.os import remove


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("homeostat rearm smoke: " + message)


def _path() -> String:
    return "/tmp/fala-homeostat-rearm.sqlite"


def _clean() raises:
    var path = _path()
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


def main() raises:
    _clean()
    var path = _path()
    var journal = NativeJournal.open(path)
    journal.initialize()
    var run_id = "run-rearm"
    _ = journal.create_run(run_id, "created", "{}", "2026-01-01T00:00:00Z")
    _ = journal.transition_run_status(run_id, "active", "2026-01-01T00:00:01Z", "seed:active")
    _ = journal.transition_run_status(run_id, "waiting", "2026-01-01T00:00:02Z", "seed:waiting")

    _ = journal.schedule_process(
        run_id, "proc-gate", "native", "2026-01-01T00:00:00Z",
        "{}", "{}", "", 0, 3,
    )
    _ = journal.claim_process(
        run_id, "proc-gate", "worker", "2026-01-01T00:00:01Z", "2026-01-01T00:01:00Z"
    )
    _ = journal.park_homeostat_process(
        run_id, "gate-1", "proc-gate", "worker", "2026-01-01T00:00:02Z",
        "{\"prompt\":\"review\"}", "{}", "homeostat-open-key",
    )
    _ = journal.transition_homeostat_process(
        run_id, "gate-1", "proc-gate", "completed", "succeeded",
        "worker", "2026-01-01T00:00:03Z", "{\"approved\":true}", "{}", "homeostat-complete-key",
    )

    var rearmed = rearm_homeostat(
        journal, run_id, "gate-1", "proc-gate", "operator", "2026-01-01T00:00:04Z", "rearm:1"
    )
    _check(rearmed.status == "waiting", "process reset to waiting")
    var homeostat_row = journal.db.query("SELECT status,attempt,max_attempts FROM homeostats WHERE run_id=? AND id=?")
    homeostat_row.bind_text(1, run_id)
    homeostat_row.bind_text(2, "gate-1")
    _check(homeostat_row.step(), "homeostat row exists")
    _check(homeostat_row.column_text(0) == "open", "homeostat reopened")
    _check(homeostat_row.column_int(1) >= 1, "homeostat attempt advanced")

    var replay = rearm_homeostat(
        journal, run_id, "gate-1", "proc-gate", "operator", "2026-01-01T00:00:04Z", "rearm:1"
    )
    _check(replay.status == "waiting", "replay returns waiting process")

    # Reject when run is not waiting
    _ = journal.transition_run_status(run_id, "active", "2026-01-01T00:00:05Z", "seed:active2")
    _ = journal.transition_homeostat_process(
        run_id, "gate-1", "proc-gate", "completed", "succeeded",
        "worker", "2026-01-01T00:00:06Z", "{\"approved\":false}", "{}", "homeostat-complete-2",
    )

    var rejected = False
    try:
        _ = rearm_homeostat(
            journal, run_id, "gate-1", "proc-gate", "operator", "2026-01-01T00:00:07Z", "rearm:bad"
        )
    except err:
        rejected = True
        var msg = String(err)
        _check(msg.find("waiting") >= 0, "error mentions waiting requirement")
    _check(rejected, "rearm rejected on non-waiting run")

    journal.close()
    _clean()
    print("homeostat rearm smoke ok: atomic rearm waiting-run idempotent reject")
