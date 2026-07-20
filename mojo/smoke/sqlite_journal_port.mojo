"""SqliteJournalPort: leading-unit append_batch + claim_next smoke."""

from std.collections import List
from std.os import remove
from fala.journal_port import (
    ClaimRequest,
    CommandRecord,
    CommandUnit,
    JournalBatch,
    StateFact,
)
from fala.sqlite_journal_port import SqliteJournalPort


def _check(ok: Bool, msg: String) raises:
    if not ok:
        raise Error("sqlite journal port smoke: " + msg)


def _unit(
    run_id: String,
    command_type: String,
    key: String,
    payload: String,
    at: String,
    actor: String = "worker",
) -> CommandUnit:
    var cmd = CommandRecord(
        id=key,
        run_id=run_id,
        command_type=command_type,
        idempotency_key=key,
        actor=actor,
        correlation_id="",
        causation_id="",
        payload_json=payload,
        created_at=at,
    )
    return CommandUnit(cmd^)


def main() raises:
    var path = "/tmp/fala-sqlite-journal-port.sqlite"
    try:
        remove(path)
    except e:
        pass
    try:
        remove(path + "-wal")
    except e:
        pass
    try:
        remove(path + "-shm")
    except e:
        pass

    var port = SqliteJournalPort.open(path)
    _check(port.runtime_uri().startswith("sqlite://"), "runtime_uri")

    # --- run.create via append_batch ---
    var create_units = List[CommandUnit]()
    var create_unit = _unit(
        "run-port",
        "run.create",
        "run.create",
        "{\"status\":\"active\"}",
        "2026-01-01T00:00:00Z",
    )
    create_unit.facts.append(
        StateFact(
            entity="run",
            op="upsert",
            key_id="run-port",
            body_json="{\"id\":\"run-port\",\"status\":\"active\",\"metadata\":\"{}\",\"created_at\":\"2026-01-01T00:00:00Z\"}",
        )
    )
    create_units.append(create_unit^)
    var first = port.append_batch(JournalBatch("run-port", create_units^))
    _check(not first.replayed, "first create not replay")
    _check(first.batch.run_id == "run-port", "create run id")

    # Idempotent replay
    var create_units2 = List[CommandUnit]()
    var create_unit2 = _unit(
        "run-port",
        "run.create",
        "run.create",
        "{\"status\":\"active\"}",
        "2026-01-01T00:00:00Z",
    )
    create_unit2.facts.append(
        StateFact(
            entity="run",
            op="upsert",
            key_id="run-port",
            body_json="{\"id\":\"run-port\",\"status\":\"active\",\"metadata\":\"{}\",\"created_at\":\"2026-01-01T00:00:00Z\"}",
        )
    )
    create_units2.append(create_unit2^)
    var again = port.append_batch(JournalBatch("run-port", create_units2^))
    _check(again.replayed, "create replay")

    # --- process.schedule ---
    var sched_units = List[CommandUnit]()
    var sched = _unit(
        "run-port",
        "process.schedule",
        "process.schedule:proc-a",
        "{\"process_id\":\"proc-a\"}",
        "2026-01-01T00:00:01Z",
    )
    sched.facts.append(
        StateFact(
            entity="process",
            op="upsert",
            key_id="proc-a",
            body_json="{\"id\":\"proc-a\",\"process_type\":\"native\",\"created_at\":\"2026-01-01T00:00:01Z\",\"input_json\":\"{}\",\"metadata\":\"{}\",\"max_attempts\":1}",
        )
    )
    # Non-leading unit is ignored as write input (parity with Python PR4).
    var ignored = _unit(
        "run-port",
        "process.side",
        "ignored-side",
        "{}",
        "2026-01-01T00:00:01Z",
    )
    sched_units.append(sched^)
    sched_units.append(ignored^)
    var scheduled = port.append_batch(JournalBatch("run-port", sched_units^))
    _check(not scheduled.replayed, "schedule first")
    _check(len(scheduled.units) == 1, "only leading unit returned")

    var sched_units2 = List[CommandUnit]()
    var sched2 = _unit(
        "run-port",
        "process.schedule",
        "process.schedule:proc-a",
        "{\"process_id\":\"proc-a\"}",
        "2026-01-01T00:00:01Z",
    )
    sched2.facts.append(
        StateFact(
            entity="process",
            op="upsert",
            key_id="proc-a",
            body_json="{\"id\":\"proc-a\",\"process_type\":\"native\",\"created_at\":\"2026-01-01T00:00:01Z\",\"input_json\":\"{}\",\"metadata\":\"{}\",\"max_attempts\":1}",
        )
    )
    sched_units2.append(sched2^)
    var sched_replay = port.append_batch(JournalBatch("run-port", sched_units2^))
    _check(sched_replay.replayed, "schedule replay")

    # --- claim_next ---
    var claim = port.claim_next(
        ClaimRequest("worker-1", "run-port", 300.0, False, 0.0),
        "2026-01-01T00:00:02Z",
        "2026-01-01T00:10:00Z",
    )
    _check(claim.process_id == "proc-a", "claimed process id")
    _check(claim.has_batch, "claim has batch")
    _check(claim.batch.units[0].command.command_type == "process.claim", "claim command")

    # Empty claim when nothing ready
    var empty = port.claim_next(
        ClaimRequest("worker-2", "run-port", 300.0, False, 0.0),
        "2026-01-01T00:00:03Z",
        "2026-01-01T00:10:00Z",
    )
    _check(empty.process_id == "" and not empty.has_batch, "no second claim")

    # --- process.complete ---
    var done_units = List[CommandUnit]()
    var done = _unit(
        "run-port",
        "process.complete",
        "process.complete:proc-a",
        "{\"process_id\":\"proc-a\",\"output_json\":\"{\\\"ok\\\":true}\"}",
        "2026-01-01T00:00:04Z",
        "worker-1",
    )
    done_units.append(done^)
    var completed = port.append_batch(JournalBatch("run-port", done_units^))
    _check(not completed.replayed, "complete applied")
    var row = port.engine.get_process("run-port", "proc-a")
    _check(row.status == "succeeded", "process succeeded")

    port.close()
    try:
        remove(path)
    except e:
        pass
    print("sqlite journal port smoke ok")
