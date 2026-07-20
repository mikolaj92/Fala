"""Core InMemoryJournal smoke — no SQLite."""

from std.collections import List
from fala.journal_port import (
    ClaimRequest,
    CommandRecord,
    CommandUnit,
    EventRecord,
    JournalBatch,
)
from fala.memory_journal import InMemoryJournal
from fala.processes import ProcessRecord
from fala.status import ProcessStatus


def main() raises:
    var journal = InMemoryJournal("memory://smoke")
    if journal.runtime_uri() != "memory://smoke":
        raise Error("unexpected runtime_uri")

    var cmd = CommandRecord(
        id="command_1",
        run_id="run_1",
        command_type="run.create",
        idempotency_key="create",
        actor="test",
        correlation_id="",
        causation_id="",
        payload_json="{}",
        created_at="",
    )
    var event = EventRecord(
        id="event_1",
        run_id="run_1",
        event_type="run.created",
        schema_version=1,
        impulse_id="",
        process_id="",
        sequence=0,
        command_id="",
        actor="",
        correlation_id="",
        causation_id="",
        payload_json="{}",
        created_at="",
    )
    var unit = CommandUnit(cmd^)
    unit.events.append(event^)
    var units = List[CommandUnit]()
    units.append(unit^)
    var batch = JournalBatch("run_1", units^)

    var first = journal.append_batch(batch.copy())
    if first.replayed:
        raise Error("first append must not replay")
    if first.batch.journal_seq != 1:
        raise Error("journal_seq must be 1")
    if len(first.units[0].events) != 1 or first.units[0].events[0].sequence != 1:
        raise Error("event sequence must be assigned")

    var second = journal.append_batch(batch^)
    if not second.replayed:
        raise Error("second append must replay")
    if len(second.units[0].events) != 0:
        raise Error("replay events must be empty")

    var events = journal.list_events("run_1")
    if len(events) != 1:
        raise Error("expected one durable event")

    journal.seed_process(
        ProcessRecord(
            id="p1",
            run_id="run_1",
            status=ProcessStatus.ready(),
            priority=10,
            attempt=0,
            max_attempts=2,
        )
    )
    journal.seed_process(
        ProcessRecord(
            id="p0",
            run_id="run_1",
            status=ProcessStatus.ready(),
            priority=1,
            attempt=0,
            max_attempts=2,
        )
    )
    var claim = journal.claim_next(
        ClaimRequest("worker_a", "run_1", 30.0, False, 100.0)
    )
    if claim.process_id != "p1":
        raise Error("must claim highest priority process, got " + claim.process_id)
    if not claim.has_batch:
        raise Error("claim must produce a batch")
    var claimed = journal.get_process("p1")
    if claimed.status != ProcessStatus.running():
        raise Error("claimed process must be running")
    if claimed.lease_owner != "worker_a":
        raise Error("lease owner mismatch")

    # Same simulated clock: p1 lease still held; p0 remains ready.
    var claim_low = journal.claim_next(
        ClaimRequest("worker_b", "run_1", 30.0, False, 100.0)
    )
    if claim_low.process_id != "p0":
        raise Error(
            "second claim should take remaining ready process, got "
            + claim_low.process_id
        )
    print("core journal memory smoke ok")
