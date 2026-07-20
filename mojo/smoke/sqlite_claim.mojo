"""SQLite adapter: create_run + schedule + claim + complete via NativeJournal."""

from std.os import remove
from fala.journal import NativeJournal


def _check(ok: Bool, msg: String) raises:
    if not ok:
        raise Error("sqlite claim smoke: " + msg)


def main() raises:
    var path = "/tmp/fala-sqlite-claim.sqlite"
    try:
        remove(path)
    except e:
        pass

    var journal = NativeJournal(path)
    journal.initialize()
    var created = journal.create_run(
        "run-claim", "active", "{}", "2026-01-01T00:00:00Z"
    )
    _check(created.id == "run-claim", "create_run id")
    # Idempotent re-create with same contents
    var again = journal.create_run(
        "run-claim", "active", "{}", "2026-01-01T00:00:00Z"
    )
    _check(again.id == "run-claim", "create_run replay returns same id")

    _ = journal.schedule_process(
        "run-claim",
        "proc-a",
        "native",
        "2026-01-01T00:00:01Z",
        "{}",
        "{}",
        "",
        10,
        1,
        "",
        "{}",
        "schedule-a",
        "worker",
    )
    var claimed = journal.claim_process(
        "run-claim",
        "proc-a",
        "worker-1",
        "2026-01-01T00:00:02Z",
        "2026-01-01T00:10:00Z",
    )
    _check(claimed.status == "running", "claimed status running")
    _check(claimed.lease_owner == "worker-1", "lease owner")
    _check(claimed.attempt == 1, "attempt incremented")

    _ = journal.complete_process(
        "run-claim",
        "proc-a",
        "worker-1",
        "2026-01-01T00:00:03Z",
        "{\"value\":1}",
    )
    var done = journal.get_process("run-claim", "proc-a")
    _check(done.status == "succeeded", "process succeeded")

    journal.close()
    try:
        remove(path)
    except e:
        pass
    print("sqlite claim smoke ok")
