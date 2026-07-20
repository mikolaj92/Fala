"""JsonlJournal durability smoke."""

from std.os import remove
from std.pathlib import Path
from std.collections import List
from fala.journal_port import CommandRecord, CommandUnit, EventRecord, JournalBatch
from fala.jsonl_journal import JsonlJournal


def main() raises:
    var path = "/tmp/fala-jsonl-smoke.journal.jsonl"
    try:
        remove(path)
    except e:
        pass

    var journal = JsonlJournal(path)
    var cmd = CommandRecord(
        id="c1",
        run_id="run_j",
        command_type="run.create",
        idempotency_key="k1",
        actor="t",
        correlation_id="",
        causation_id="",
        payload_json="{}",
        created_at="",
    )
    var event = EventRecord(
        id="e1",
        run_id="run_j",
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
    var first = journal.append_batch(JournalBatch("run_j", units^))
    if first.replayed:
        raise Error("first append must not replay")
    if journal.line_count() != 1:
        raise Error("expected one durable line")

    # Torn partial line without trailing newline
    var existing = Path(path).read_text()
    Path(path).write_text(existing + "{\"v\":1,\"partial\"")

    var reopened = JsonlJournal(path)
    if reopened.line_count() != 1:
        raise Error("torn line must be truncated on open")

    try:
        remove(path)
    except e:
        pass
    print("jsonl journal smoke ok")
