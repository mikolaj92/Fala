"""TeeJournal mirrors appends to secondary sink."""

from std.collections import List
from fala.journal_port import CommandRecord, CommandUnit, EventRecord, JournalBatch
from fala.tee_journal import TeeJournal


def main() raises:
    var tee = TeeJournal("memory://t1", "memory://t2")
    var cmd = CommandRecord(
        id="c1",
        run_id="run_t",
        command_type="run.create",
        idempotency_key="k",
        actor="t",
        correlation_id="",
        causation_id="",
        payload_json="{}",
        created_at="",
    )
    var event = EventRecord(
        id="e1",
        run_id="run_t",
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
    _ = tee.append_batch(JournalBatch("run_t", units^))
    if tee.primary_event_count("run_t") != 1:
        raise Error("primary missing event")
    if tee.secondary_event_count("run_t") != 1:
        raise Error("secondary missing mirrored event")

    # Replay must not double-write secondary
    var units2 = List[CommandUnit]()
    var unit2 = CommandUnit(
        CommandRecord(
            id="c1",
            run_id="run_t",
            command_type="run.create",
            idempotency_key="k",
            actor="t",
            correlation_id="",
            causation_id="",
            payload_json="{}",
            created_at="",
        )
    )
    units2.append(unit2^)
    var replay = tee.append_batch(JournalBatch("run_t", units2^))
    if not replay.replayed:
        raise Error("expected replay")
    if tee.secondary_event_count("run_t") != 1:
        raise Error("replay must not double secondary")

    print("tee journal smoke ok")
