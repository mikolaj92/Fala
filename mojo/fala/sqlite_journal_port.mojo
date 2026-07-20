"""SqliteJournalPort — SQLite adapter over NativeJournal (not core identity).

PR4-style wrap: ``append_batch`` dispatches the **leading unit only** into
existing NativeJournal TX methods; non-leading units are ignored as write
inputs (engine regenerates side effects). Callers that need full domain
APIs may still use ``.engine`` directly.
"""

from std.collections import List
from emberjson import Value
from fala.journal import EventInput, NativeJournal
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


def _json_string(obj: Value, key: String, default: String = "") raises -> String:
    if not obj.is_object() or key not in obj.object():
        return default
    var item = obj.object()[key].copy()
    if item.is_string():
        return item.string()
    if item.is_int():
        return String(item.int())
    if item.is_bool():
        return "true" if item.bool() else "false"
    return default


def _json_int(obj: Value, key: String, default: Int = 0) raises -> Int:
    if not obj.is_object() or key not in obj.object():
        return default
    var item = obj.object()[key].copy()
    if item.is_int():
        return Int(item.int())
    if item.is_string():
        try:
            return Int(item.string())
        except err:
            return default
    return default


def _fact_body(units: List[CommandUnit], entity: String) raises -> String:
    for unit in units:
        for fact in unit.facts:
            if fact.entity == entity and fact.op == "upsert" and fact.body_json != "":
                return fact.body_json
    return ""


def _process_id_from_unit(unit: CommandUnit) raises -> String:
    if unit.command.payload_json != "":
        try:
            var payload = Value(parse_string=unit.command.payload_json)
            var pid = _json_string(payload, "process_id", "")
            if pid != "":
                return pid
        except err:
            pass
    for fact in unit.facts:
        if fact.entity == "process" and fact.key_id != "":
            return fact.key_id
        if fact.entity == "process" and fact.body_json != "":
            try:
                var body = Value(parse_string=fact.body_json)
                var pid = _json_string(body, "id", "")
                if pid != "":
                    return pid
            except err:
                pass
    return ""


def _events_to_inputs(events: List[EventRecord]) -> List[EventInput]:
    var out = List[EventInput]()
    for event in events:
        out.append(
            EventInput(
                id=event.id,
                event_type=event.event_type,
                payload=event.payload_json,
                created_at=event.created_at,
                impulse_id=event.impulse_id,
                process_id=event.process_id,
                schema_version=event.schema_version,
                actor=event.actor,
                correlation_id=event.correlation_id,
                causation_id=event.causation_id,
            )
        )
    return out^


def _unit_result(
    command: CommandRecord,
    events: List[EventRecord],
    facts: List[StateFact],
    batch: JournalBatch,
    replayed: Bool,
) -> AppendResult:
    var unit_out = CommandUnit(command.copy())
    unit_out.events = events.copy()
    unit_out.facts = facts.copy()
    var units = List[CommandUnit]()
    units.append(unit_out^)
    var stored = batch.copy()
    stored.units = units.copy()
    return AppendResult(batch=stored^, replayed=replayed, units=units^)


struct SqliteJournalPort(Movable):
    """Reference production sink implementing the JournalPort surface."""

    var engine: NativeJournal
    var path: String

    def __init__(out self, path: String) raises:
        self.path = path
        self.engine = NativeJournal(path)

    def runtime_uri(self) -> String:
        return "sqlite://" + self.path

    def initialize(mut self) raises:
        self.engine.initialize()

    def close(mut self) raises:
        self.engine.close()

    @staticmethod
    def open(path: String) raises -> SqliteJournalPort:
        var port = SqliteJournalPort(path)
        port.initialize()
        return port^

    def append_batch(mut self, batch: JournalBatch) raises -> AppendResult:
        """Dispatch leading command_type → NativeJournal TX (non-leading ignored)."""
        if len(batch.units) < 1:
            raise Error("JournalBatch.units must be non-empty")
        var primary = batch.units[0].copy()
        var command = primary.command.copy()
        var command_type = command.command_type

        if command_type == "run.create":
            return self._append_run_create(batch, primary, command)
        if command_type == "process.schedule":
            return self._append_process_schedule(batch, primary, command)
        if command_type == "process.complete":
            return self._append_process_complete(batch, primary, command)
        return self._append_generic(batch, primary, command)

    def _append_run_create(
        mut self,
        batch: JournalBatch,
        primary: CommandUnit,
        command: CommandRecord,
    ) raises -> AppendResult:
        var body_json = _fact_body(batch.units, "run")
        var status = "active"
        var metadata = "{}"
        var title = ""
        var created_at = command.created_at
        var run_id = batch.run_id if batch.run_id != "" else command.run_id
        if body_json != "":
            var body = Value(parse_string=body_json)
            status = _json_string(body, "status", status)
            metadata = _json_string(body, "metadata", metadata)
            title = _json_string(body, "title", title)
            var body_created = _json_string(body, "created_at", "")
            if body_created != "":
                created_at = body_created
            if run_id == "":
                run_id = _json_string(body, "id", "")
        if run_id == "" or created_at == "":
            raise Error("run.create requires run id and created_at")
        var key = command.idempotency_key if command.idempotency_key != "" else "run.create"
        var replayed = False
        try:
            _ = self.engine.get_run_record(run_id)
            replayed = True
        except err:
            replayed = False
        _ = self.engine.create_run(
            run_id, status, metadata, created_at, title, created_at, key
        )
        var events = List[EventRecord]()
        events.append(
            EventRecord(
                id="run.create:" + run_id + ":event",
                run_id=run_id,
                event_type="run.created",
                schema_version=1,
                impulse_id="",
                process_id="",
                sequence=1,
                command_id="run.create:" + run_id,
                actor=command.actor,
                correlation_id=command.correlation_id,
                causation_id=command.causation_id,
                payload_json=command.payload_json,
                created_at=created_at,
            )
        )
        var stored_cmd = command.copy()
        stored_cmd.run_id = run_id
        var result_batch = batch.copy()
        result_batch.run_id = run_id
        return _unit_result(
            stored_cmd^, events^, primary.facts.copy(), result_batch^, replayed
        )

    def _append_process_schedule(
        mut self,
        batch: JournalBatch,
        primary: CommandUnit,
        command: CommandRecord,
    ) raises -> AppendResult:
        var body_json = _fact_body(batch.units, "process")
        if body_json == "":
            raise Error(
                "process.schedule append_batch requires a process StateFact body"
            )
        var body = Value(parse_string=body_json)
        var process_id = _json_string(body, "id", "")
        if process_id == "" and len(primary.facts) > 0:
            process_id = primary.facts[0].key_id
        var process_type = _json_string(body, "process_type", "native")
        var created_at = command.created_at
        if created_at == "":
            created_at = _json_string(body, "created_at", "")
        if process_id == "" or created_at == "":
            raise Error("process.schedule requires process id and created_at")
        var input_json = _json_string(body, "input_json", "{}")
        if input_json == "{}" and "input" in body.object():
            var input_val = body.object()["input"].copy()
            if input_val.is_string():
                input_json = input_val.string()
        var metadata = _json_string(body, "metadata", "{}")
        var impulse_id = _json_string(body, "impulse_id", "")
        var priority = _json_int(body, "priority", 0)
        var max_attempts = _json_int(body, "max_attempts", 1)
        if max_attempts < 1:
            max_attempts = 1
        var available_at = _json_string(body, "available_at", "")
        var output_schema = _json_string(body, "output_schema_json", "{}")
        var key = command.idempotency_key
        if key == "":
            key = "process.schedule:" + process_id
        var replayed = False
        try:
            _ = self.engine.get_command_by_idempotency(command.run_id, key)
            replayed = True
        except err:
            replayed = False
        _ = self.engine.schedule_process(
            command.run_id,
            process_id,
            process_type,
            created_at,
            input_json,
            metadata,
            impulse_id,
            priority,
            max_attempts,
            available_at,
            output_schema,
            key,
            command.actor,
        )
        var events = List[EventRecord]()
        events.append(
            EventRecord(
                id=key + ":event",
                run_id=command.run_id,
                event_type="process.scheduled",
                schema_version=1,
                impulse_id=impulse_id,
                process_id=process_id,
                sequence=0,
                command_id=key,
                actor=command.actor,
                correlation_id="",
                causation_id="",
                payload_json=command.payload_json,
                created_at=created_at,
            )
        )
        return _unit_result(
            command.copy(), events^, primary.facts.copy(), batch.copy(), replayed
        )

    def _append_process_complete(
        mut self,
        batch: JournalBatch,
        primary: CommandUnit,
        command: CommandRecord,
    ) raises -> AppendResult:
        var process_id = _process_id_from_unit(primary)
        if process_id == "":
            raise Error("process.complete requires process_id in payload or fact")
        var at = command.created_at
        if at == "":
            raise Error("process.complete requires created_at")
        var output_json = "{}"
        var error_json = "{}"
        try:
            var payload = Value(parse_string=command.payload_json)
            var out2 = _json_string(payload, "output_json", "")
            if out2 != "":
                output_json = out2
            var errj = _json_string(payload, "error_json", "")
            if errj != "":
                error_json = errj
        except err:
            pass
        _ = self.engine.complete_process(
            command.run_id,
            process_id,
            command.actor if command.actor != "" else "worker",
            at,
            output_json,
            error_json,
        )
        var events = List[EventRecord]()
        events.append(
            EventRecord(
                id=command.idempotency_key + ":event",
                run_id=command.run_id,
                event_type="process.completed",
                schema_version=1,
                impulse_id="",
                process_id=process_id,
                sequence=0,
                command_id=command.id,
                actor=command.actor,
                correlation_id="",
                causation_id="",
                payload_json=command.payload_json,
                created_at=at,
            )
        )
        return _unit_result(
            command.copy(), events^, primary.facts.copy(), batch.copy(), False
        )

    def _append_generic(
        mut self,
        batch: JournalBatch,
        primary: CommandUnit,
        command: CommandRecord,
    ) raises -> AppendResult:
        var event_inputs = _events_to_inputs(primary.events)
        var submission = self.engine.submit_command(
            command.run_id,
            command.id if command.id != "" else command.idempotency_key,
            command.command_type,
            command.idempotency_key,
            command.payload_json,
            command.created_at,
            event_inputs^,
            command.actor,
            command.correlation_id,
            command.causation_id,
        )
        var out_events = List[EventRecord]()
        for event in submission.events:
            out_events.append(
                EventRecord(
                    id=event.id,
                    run_id=event.run_id,
                    event_type=event.event_type,
                    schema_version=event.schema_version,
                    impulse_id=event.impulse_id,
                    process_id=event.process_id,
                    sequence=event.sequence,
                    command_id=event.command_id,
                    actor=event.actor,
                    correlation_id=event.correlation_id,
                    causation_id=event.causation_id,
                    payload_json=event.payload,
                    created_at=event.created_at,
                )
            )
        var stored_cmd = CommandRecord(
            id=submission.command.id,
            run_id=submission.command.run_id,
            command_type=submission.command.command_type,
            idempotency_key=submission.command.idempotency_key,
            actor=submission.command.actor,
            correlation_id=submission.command.correlation_id,
            causation_id=submission.command.causation_id,
            payload_json=submission.command.payload,
            created_at=submission.command.created_at,
        )
        return _unit_result(
            stored_cmd^,
            out_events^,
            primary.facts.copy(),
            batch.copy(),
            submission.replayed,
        )

    def claim_next(
        mut self,
        request: ClaimRequest,
        now: String,
        lease_expires_at: String,
    ) raises -> ClaimResult:
        """Claim one ready process. SQLite uses ISO timestamps (``now`` / lease).

        ``request.now`` (Float64 sim clock) is ignored; pass ISO strings for
        durable engine times. Claim reaps remain inside the engine TX.
        """
        _ = request.now
        _ = request.lease_seconds
        var claimed = self.engine.claim_next_ready(
            request.run_id,
            request.worker_id,
            now,
            lease_expires_at,
            "",
            request.all_runs,
        )
        if not claimed:
            var empty_units = List[CommandUnit]()
            return ClaimResult(
                process_id="",
                run_id=request.run_id,
                has_batch=False,
                batch=JournalBatch(request.run_id, empty_units^),
                replayed=False,
            )
        var row = claimed.value().copy()
        var command_id = "process.claim:" + row.id + ":" + String(row.attempt)
        var payload = (
            "{\"process_id\":\""
            + row.id
            + "\",\"worker_id\":\""
            + request.worker_id
            + "\",\"attempt\":"
            + String(row.attempt)
            + "}"
        )
        var cmd = CommandRecord(
            id=command_id,
            run_id=row.run_id,
            command_type="process.claim",
            idempotency_key=command_id,
            actor=request.worker_id,
            correlation_id="",
            causation_id="",
            payload_json=payload,
            created_at=now,
        )
        var event = EventRecord(
            id=command_id + ":event",
            run_id=row.run_id,
            event_type="process.claimed",
            schema_version=1,
            impulse_id=row.impulse_id,
            process_id=row.id,
            sequence=0,
            command_id=command_id,
            actor=request.worker_id,
            correlation_id="",
            causation_id="",
            payload_json=payload,
            created_at=now,
        )
        var unit = CommandUnit(cmd^)
        unit.events.append(event^)
        unit.facts.append(
            StateFact(
                entity="process",
                op="upsert",
                key_id=row.id,
                body_json="{\"id\":\"" + row.id + "\",\"status\":\"" + row.status + "\"}",
            )
        )
        var units = List[CommandUnit]()
        units.append(unit^)
        return ClaimResult(
            process_id=row.id,
            run_id=row.run_id,
            has_batch=True,
            batch=JournalBatch(row.run_id, units^),
            replayed=False,
        )
