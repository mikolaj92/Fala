"""JsonlJournal — durable JSONL sink (adapter, same land as core).

Write barrier: prepare batch (sequences) → append full line → update index.
On open: truncate torn last line, rehydrate accepted batches into memory index.
"""

from std.collections import List
from std.pathlib import Path
from std.os import remove
from emberjson import Value, to_string
from fala.json import canonical_json_text
from fala.journal_port import (
    AppendResult,
    ClaimRequest,
    ClaimResult,
    CommandRecord,
    CommandUnit,
    EventRecord,
    JournalBatch,
    StateFact,
)
from fala.memory_journal import InMemoryJournal
from fala.processes import ProcessRecord


def _json_escape(value: String) -> String:
    var result = String()
    for i in range(value.byte_length()):
        var ch = value[byte=i]
        if ch == "\\":
            result += "\\\\"
        elif ch == "\"":
            result += "\\\""
        elif ch == "\n":
            result += "\\n"
        elif ch == "\r":
            result += "\\r"
        elif ch == "\t":
            result += "\\t"
        else:
            result += String(ch)
    return result^


def _json_quote(value: String) -> String:
    return "\"" + _json_escape(value) + "\""


def _json_string_field(obj: Value, key: String, default: String = "") raises -> String:
    if not obj.is_object() or key not in obj.object():
        return default
    var item = obj.object()[key].copy()
    if item.is_string():
        return item.string()
    if item.is_int():
        return String(item.int())
    if item.is_null():
        return default
    return default


def _json_int_field(obj: Value, key: String, default: Int = 0) raises -> Int:
    if not obj.is_object() or key not in obj.object():
        return default
    var item = obj.object()[key].copy()
    if item.is_int():
        return Int(item.int())
    if item.is_uint():
        return Int(item.uint())
    if item.is_string():
        try:
            return Int(item.string())
        except err:
            return default
    return default


def encode_command(cmd: CommandRecord) -> String:
    return (
        "{\"id\":"
        + _json_quote(cmd.id)
        + ",\"run_id\":"
        + _json_quote(cmd.run_id)
        + ",\"command_type\":"
        + _json_quote(cmd.command_type)
        + ",\"idempotency_key\":"
        + _json_quote(cmd.idempotency_key)
        + ",\"actor\":"
        + _json_quote(cmd.actor)
        + ",\"correlation_id\":"
        + _json_quote(cmd.correlation_id)
        + ",\"causation_id\":"
        + _json_quote(cmd.causation_id)
        + ",\"payload_json\":"
        + _json_quote(cmd.payload_json)
        + ",\"created_at\":"
        + _json_quote(cmd.created_at)
        + "}"
    )


def encode_event(event: EventRecord) -> String:
    return (
        "{\"id\":"
        + _json_quote(event.id)
        + ",\"run_id\":"
        + _json_quote(event.run_id)
        + ",\"event_type\":"
        + _json_quote(event.event_type)
        + ",\"schema_version\":"
        + String(event.schema_version)
        + ",\"impulse_id\":"
        + _json_quote(event.impulse_id)
        + ",\"process_id\":"
        + _json_quote(event.process_id)
        + ",\"sequence\":"
        + String(event.sequence)
        + ",\"command_id\":"
        + _json_quote(event.command_id)
        + ",\"actor\":"
        + _json_quote(event.actor)
        + ",\"correlation_id\":"
        + _json_quote(event.correlation_id)
        + ",\"causation_id\":"
        + _json_quote(event.causation_id)
        + ",\"payload_json\":"
        + _json_quote(event.payload_json)
        + ",\"created_at\":"
        + _json_quote(event.created_at)
        + "}"
    )


def encode_fact(fact: StateFact) -> String:
    return (
        "{\"entity\":"
        + _json_quote(fact.entity)
        + ",\"op\":"
        + _json_quote(fact.op)
        + ",\"key_id\":"
        + _json_quote(fact.key_id)
        + ",\"body_json\":"
        + _json_quote(fact.body_json)
        + "}"
    )


def encode_unit(unit: CommandUnit) -> String:
    var events = "["
    var first = True
    for event in unit.events:
        if not first:
            events += ","
        events += encode_event(event)
        first = False
    events += "]"
    var facts = "["
    first = True
    for fact in unit.facts:
        if not first:
            facts += ","
        facts += encode_fact(fact)
        first = False
    facts += "]"
    return (
        "{\"command\":"
        + encode_command(unit.command)
        + ",\"events\":"
        + events
        + ",\"facts\":"
        + facts
        + "}"
    )


def encode_batch_line(batch: JournalBatch) raises -> String:
    """Full wire line: one accepted JournalBatch per newline-terminated record."""
    var units = "["
    var first = True
    for unit in batch.units:
        if not first:
            units += ","
        units += encode_unit(unit)
        first = False
    units += "]"
    var body = (
        "{\"journal_seq\":"
        + String(batch.journal_seq)
        + ",\"run_id\":"
        + _json_quote(batch.run_id)
        + ",\"stream_id\":"
        + _json_quote(batch.stream_id)
        + ",\"parent_stream_id\":"
        + _json_quote(batch.parent_stream_id)
        + ",\"parent_process_id\":"
        + _json_quote(batch.parent_process_id)
        + ",\"units\":"
        + units
        + "}"
    )
    # Canonicalize nested payload for stable round-trip when possible.
    try:
        body = canonical_json_text(
            "{\"v\":1,\"kind\":\"journal_batch\",\"batch\":" + body + "}"
        )
        return body + "\n"
    except err:
        return "{\"v\":1,\"kind\":\"journal_batch\",\"batch\":" + body + "}\n"


def decode_command(value: Value) raises -> CommandRecord:
    return CommandRecord(
        id=_json_string_field(value, "id"),
        run_id=_json_string_field(value, "run_id"),
        command_type=_json_string_field(value, "command_type"),
        idempotency_key=_json_string_field(value, "idempotency_key"),
        actor=_json_string_field(value, "actor"),
        correlation_id=_json_string_field(value, "correlation_id"),
        causation_id=_json_string_field(value, "causation_id"),
        payload_json=_json_string_field(value, "payload_json", "{}"),
        created_at=_json_string_field(value, "created_at"),
    )


def decode_event(value: Value) raises -> EventRecord:
    return EventRecord(
        id=_json_string_field(value, "id"),
        run_id=_json_string_field(value, "run_id"),
        event_type=_json_string_field(value, "event_type"),
        schema_version=_json_int_field(value, "schema_version", 1),
        impulse_id=_json_string_field(value, "impulse_id"),
        process_id=_json_string_field(value, "process_id"),
        sequence=_json_int_field(value, "sequence", 0),
        command_id=_json_string_field(value, "command_id"),
        actor=_json_string_field(value, "actor"),
        correlation_id=_json_string_field(value, "correlation_id"),
        causation_id=_json_string_field(value, "causation_id"),
        payload_json=_json_string_field(value, "payload_json", "{}"),
        created_at=_json_string_field(value, "created_at"),
    )


def decode_fact(value: Value) raises -> StateFact:
    return StateFact(
        entity=_json_string_field(value, "entity"),
        op=_json_string_field(value, "op", "upsert"),
        key_id=_json_string_field(value, "key_id"),
        body_json=_json_string_field(value, "body_json"),
    )


def decode_unit(value: Value) raises -> CommandUnit:
    if not value.is_object() or "command" not in value.object():
        raise Error("command unit requires command object")
    var cmd = decode_command(value.object()["command"].copy())
    var unit = CommandUnit(cmd^)
    if "events" in value.object() and value.object()["events"].is_array():
        for item in value.object()["events"].array():
            unit.events.append(decode_event(item.copy()))
    if "facts" in value.object() and value.object()["facts"].is_array():
        for item in value.object()["facts"].array():
            unit.facts.append(decode_fact(item.copy()))
    return unit^


def decode_journal_line(line: String) raises -> JournalBatch:
    var payload = Value(parse_string=line)
    if not payload.is_object():
        raise Error("JSONL journal line must be an object")
    var root = payload.object().copy()
    if "kind" in root:
        var kind = root["kind"].copy()
        if kind.is_string() and kind.string() != "journal_batch":
            raise Error("Unsupported journal line kind: " + kind.string())
    var body = payload.copy()
    if "batch" in root:
        body = root["batch"].copy()
    if not body.is_object():
        raise Error("journal batch body must be an object")
    var b = body.object().copy()
    var run_id = _json_string_field(body, "run_id")
    var units = List[CommandUnit]()
    if "units" in b and b["units"].is_array():
        for item in b["units"].array():
            units.append(decode_unit(item.copy()))
    var batch = JournalBatch(run_id, units^)
    batch.journal_seq = _json_int_field(body, "journal_seq", 0)
    batch.stream_id = _json_string_field(body, "stream_id")
    batch.parent_stream_id = _json_string_field(body, "parent_stream_id")
    batch.parent_process_id = _json_string_field(body, "parent_process_id")
    return batch^


struct _LineSplit(Movable):
    var lines: List[String]
    var torn: String

    def __init__(out self, var lines: List[String], torn: String):
        self.lines = lines^
        self.torn = torn


def _split_complete_lines(text: String) raises -> _LineSplit:
    """Return complete newline-terminated lines and optional torn tail (no NL)."""
    var lines = List[String]()
    var current = String("")
    var i = 0
    while i < text.byte_length():
        var ch = text[byte=i]
        if ch == "\n":
            lines.append(current^)
            current = String("")
        else:
            current += String(ch)
        i += 1
    return _LineSplit(lines^, current^)


struct JsonlJournal(Movable):
    var path: String
    var index: InMemoryJournal

    def __init__(out self, path: String) raises:
        self.path = path
        self.index = InMemoryJournal("jsonl://" + path)
        self._load()

    def runtime_uri(self) -> String:
        return "jsonl://" + self.path

    def _load(mut self) raises:
        var p = Path(self.path)
        if not p.exists():
            return
        var text = p.read_text()
        var split = _split_complete_lines(text)
        # Torn last line (no trailing newline): drop partial write.
        if split.torn.byte_length() > 0:
            var kept = String("")
            for line in split.lines:
                kept += line + "\n"
            p.write_text(kept)
        for line in split.lines:
            if line.byte_length() == 0:
                continue
            try:
                var batch = decode_journal_line(line)
                self.index.import_stored_batch(batch^)
            except err:
                # Invalid complete line: fail closed (corrupt journal).
                raise Error(
                    "jsonl journal: invalid durable line: " + String(err)
                )

    def append_batch(mut self, batch: JournalBatch) raises -> AppendResult:
        var result = self.index.append_batch(batch)
        if not result.replayed:
            var line = encode_batch_line(result.batch)
            var p = Path(self.path)
            var existing = ""
            if p.exists():
                existing = p.read_text()
            p.write_text(existing + line)
        return result^

    def claim_next(mut self, request: ClaimRequest) raises -> ClaimResult:
        return self.index.claim_next(request)

    def seed_process(mut self, process: ProcessRecord):
        self.index.seed_process(process)

    def list_events(mut self, run_id: String) raises -> List[EventRecord]:
        return self.index.list_events(run_id)

    def has_command(self, run_id: String, idempotency_key: String) -> Bool:
        return self.index.has_command(run_id, idempotency_key)

    def load(
        mut self, run_id: String = "", after_journal_seq: Int = 0
    ) raises -> List[JournalBatch]:
        return self.index.load(run_id, after_journal_seq)

    def line_count(self) raises -> Int:
        var p = Path(self.path)
        if not p.exists():
            return 0
        var text = p.read_text()
        var count = 0
        var i = 0
        while i < text.byte_length():
            if text[byte=i] == "\n":
                count += 1
            i += 1
        return count
