"""JsonlJournal durability + full reopen rehydrate smoke."""

from std.os import remove
from std.pathlib import Path
from std.collections import List
from fala.journal_port import CommandRecord, CommandUnit, EventRecord, JournalBatch, StateFact
from fala.jsonl_journal import JsonlJournal


def _check(ok: Bool, msg: String) raises:
    if not ok:
        raise Error("jsonl journal smoke: " + msg)


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
        actor="tester",
        correlation_id="",
        causation_id="",
        payload_json="{\"status\":\"active\"}",
        created_at="2026-01-01T00:00:00Z",
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
        payload_json="{\"status\":\"active\"}",
        created_at="2026-01-01T00:00:00Z",
    )
    var unit = CommandUnit(cmd^)
    unit.events.append(event^)
    unit.facts.append(
        StateFact(entity="run", op="upsert", key_id="run_j", body_json="{\"id\":\"run_j\"}")
    )
    var units = List[CommandUnit]()
    units.append(unit^)
    var first = journal.append_batch(JournalBatch("run_j", units^))
    _check(not first.replayed, "first append must not replay")
    _check(journal.line_count() == 1, "expected one durable line")
    _check(len(journal.list_events("run_j")) == 1, "events present after append")
    _check(journal.has_command("run_j", "k1"), "command indexed after append")

    # Idempotent replay on same key
    var units2 = List[CommandUnit]()
    var unit2 = CommandUnit(
        CommandRecord(
            id="c1",
            run_id="run_j",
            command_type="run.create",
            idempotency_key="k1",
            actor="tester",
            correlation_id="",
            causation_id="",
            payload_json="{\"status\":\"active\"}",
            created_at="2026-01-01T00:00:00Z",
        )
    )
    units2.append(unit2^)
    var replay = journal.append_batch(JournalBatch("run_j", units2^))
    _check(replay.replayed, "same key replays")
    _check(journal.line_count() == 1, "replay does not write a second line")

    # Torn partial line without trailing newline
    var existing = Path(path).read_text()
    Path(path).write_text(existing + "{\"v\":1,\"partial\"")

    var reopened = JsonlJournal(path)
    _check(reopened.line_count() == 1, "torn line must be truncated on open")
    _check(reopened.has_command("run_j", "k1"), "command rehydrated after reopen")
    var events = reopened.list_events("run_j")
    _check(len(events) == 1, "events rehydrated after reopen")
    _check(events[0].event_type == "run.created", "event type restored")
    _check(events[0].sequence >= 1, "event sequence assigned")
    var loaded = reopened.load("run_j")
    _check(len(loaded) == 1, "load returns rehydrated batch")
    _check(loaded[0].journal_seq >= 1, "journal_seq restored")
    _check(len(loaded[0].units) == 1, "unit rehydrated")
    _check(loaded[0].units[0].command.idempotency_key == "k1", "command key rehydrated")
    _check(len(loaded[0].units[0].facts) == 1, "facts rehydrated")

    # Second accepted batch then reopen full history
    var cmd_b = CommandRecord(
        id="c2",
        run_id="run_j",
        command_type="note.append",
        idempotency_key="k2",
        actor="tester",
        correlation_id="",
        causation_id="",
        payload_json="{\"n\":2}",
        created_at="2026-01-01T00:00:01Z",
    )
    var event_b = EventRecord(
        id="e2",
        run_id="run_j",
        event_type="note.appended",
        schema_version=1,
        impulse_id="",
        process_id="",
        sequence=0,
        command_id="",
        actor="",
        correlation_id="",
        causation_id="",
        payload_json="{\"n\":2}",
        created_at="2026-01-01T00:00:01Z",
    )
    var unit_b = CommandUnit(cmd_b^)
    unit_b.events.append(event_b^)
    var units_b = List[CommandUnit]()
    units_b.append(unit_b^)
    var second = reopened.append_batch(JournalBatch("run_j", units_b^))
    _check(not second.replayed, "second batch accepted")
    _check(reopened.line_count() == 2, "two durable lines")

    var full = JsonlJournal(path)
    _check(full.line_count() == 2, "two lines after full reopen")
    _check(full.has_command("run_j", "k1") and full.has_command("run_j", "k2"), "both commands rehydrated")
    _check(len(full.list_events("run_j")) == 2, "both events rehydrated")
    _check(len(full.load("run_j")) == 2, "load returns two batches")

    try:
        remove(path)
    except e:
        pass
    print("jsonl journal smoke ok: rehydrate torn-line multi-batch")
