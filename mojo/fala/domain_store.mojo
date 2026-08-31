"""Native SQLite domain records for Essential Fala core path.

Ops retention/maintain/GC live in ops_maintenance (bodies moved).
Projection rebuild in ops_projections; bridge in ops_bridge.
"""

from std.collections import List
from emberjson import Value, Object, to_string
from fala.sqlite import Connection, Statement, SQLiteError
from fala.schema import initialize_native_schema
from fala.json import canonical_json_text, quote_json_string as _quote
from fala.reactions import reaction_digest_or_empty

from fala.domain import (
    Impulse,
    ImpulseType,
    ImpulseRelation,
    Association,
    Reaction,
    Homeostat,
    Projection,
)

def _bind_nullable(mut stmt: Statement, index: Int, value: String) raises:
    if value == "":
        stmt.bind_null(index)
    else:
        stmt.bind_text(index, value)
def _nullable_json(mut stmt: Statement, index: Int) raises -> String:
    if stmt.column_null(index):
        return "null"
    return _quote(stmt.column_text(index))
def _canonical_reaction_metadata(value: String) raises -> String:
    try:
        var parsed = Value(parse_string=value)
        if not parsed.is_object():
            raise Error("reaction metadata must be an object")
        return canonical_json_text(to_string(parsed))
    except err:
        raise Error(String(SQLiteError(code=1, message="domain store: reaction metadata must be a JSON object")))
def _validate_reaction_cas_identity(row: Reaction) raises:
    """Reject malformed or conflicting canonical CAS identity fields."""
    var uri_digest = ""
    if row.uri.startswith("fala-reaction://sha256/"):
        uri_digest = reaction_digest_or_empty(row.uri)
        if uri_digest == "":
            raise Error(String(SQLiteError(code=1, message="domain store: invalid reaction digest URI")))
    var hash_digest = ""
    if row.content_hash.startswith("sha256:"):
        if row.content_hash.byte_length() != 71:
            raise Error(String(SQLiteError(code=1, message="domain store: invalid reaction content hash")))
        var hash_uri = "fala-reaction://sha256/"
        for index in range(7, row.content_hash.byte_length()):
            hash_uri += String(row.content_hash[byte=index])
        hash_digest = reaction_digest_or_empty(hash_uri)
        if hash_digest == "":
            raise Error(String(SQLiteError(code=1, message="domain store: invalid reaction content hash")))
    if uri_digest != "" and hash_digest != "" and uri_digest != hash_digest:
        raise Error(String(SQLiteError(code=1, message="domain store: reaction URI/content hash mismatch")))

def _reaction_journal_payload(row: Reaction) -> String:
    var content_hash = "null"
    if row.content_hash != "": content_hash = _quote(row.content_hash)
    return "{\"reaction_id\":" + _quote(row.id) + ",\"kind\":" + _quote(row.kind) + ",\"uri\":" + _quote(row.uri) + ",\"content_hash\":" + content_hash + "}"


from fala.journal import CommandRow, EventInput, EventRow, CommandSubmission

@fieldwise_init
struct ImpulseAcceptanceResult(Copyable, Movable):
    """Atomic impulse, command, and event persistence result."""
    var impulse: Impulse
    var command: CommandRow
    var events: List[EventRow]
    var replayed: Bool

@fieldwise_init
struct DomainCommandStart(Copyable, Movable):
    var command: CommandRow
    var replayed: Bool
@fieldwise_init
struct HomeostatTransitionResult(Copyable, Movable):
    var homeostat: Homeostat
    var submission: CommandSubmission


struct NativeDomainStore(Movable):
    """Connection-owning store for the schema-v6 domain tables."""

    var db: Connection

    def __init__(out self, path: String) raises:
        self.db = Connection(path)
    def __deinit__(deinit self):
        try:
            self.db.close()
        except e:
            pass


    def close(mut self) raises:
        self.db.close()
    @staticmethod
    def open(path: String) raises -> NativeDomainStore:
        return NativeDomainStore(path)

    def initialize(mut self) raises:
        initialize_native_schema(self.db)


    def _require_run(mut self, run_id: String) raises:
        if run_id == "":
            raise Error(String(SQLiteError(code=1, message="domain store: run_id must not be empty")))
        var stmt = self.db.query("SELECT 1 FROM runs WHERE id=?")
        stmt.bind_text(1, run_id)
        var found = stmt.step()
        stmt.close()
        if not found:
            raise Error(String(SQLiteError(code=1, message="domain store: unknown run")))


    @staticmethod
    def _text(mut stmt: Statement, index: Int) raises -> String:
        if stmt.column_null(index):
            return String("")
        return stmt.column_text(index)
    def _domain_command_start(
        mut self, run_id: String, command_id: String, command_type: String,
        idempotency_key: String, payload: String, created_at: String,
        actor: String = "", correlation_id: String = "", causation_id: String = "",
    ) raises -> DomainCommandStart:
        self._require_run(run_id)
        if command_id == "" or command_type == "" or idempotency_key == "" or created_at == "":
            raise Error(String(SQLiteError(code=1, message="domain store: command fields must not be empty")))
        self.db.begin_immediate()
        try:
            var existing = self.db.query("SELECT run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
            existing.bind_text(1, run_id); existing.bind_text(2, idempotency_key)
            if existing.step():
                var prior = CommandRow(run_id=self._text(existing, 0), id=self._text(existing, 1), command_type=self._text(existing, 2), idempotency_key=self._text(existing, 3), actor=self._text(existing, 4), correlation_id=self._text(existing, 5), causation_id=self._text(existing, 6), payload=self._text(existing, 7), created_at=self._text(existing, 8))
                existing.close()
                if prior.command_type != command_type or prior.actor != actor or prior.correlation_id != correlation_id or prior.causation_id != causation_id or prior.payload != payload or prior.created_at != created_at:
                    raise Error(String(SQLiteError(code=1, message="domain store: command idempotency conflict")))
                self.db.commit()
                return DomainCommandStart(command=prior^, replayed=True)
            existing.close()
            var by_id = self.db.query("SELECT 1 FROM runtime_commands WHERE run_id=? AND id=?")
            by_id.bind_text(1, run_id); by_id.bind_text(2, command_id)
            if by_id.step():
                by_id.close()
                raise Error(String(SQLiteError(code=1, message="domain store: command id already exists")))
            by_id.close()
            var insert = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at) VALUES (?,?,?,?,?,?,?,?,?)")
            insert.bind_text(1, run_id); insert.bind_text(2, command_id); insert.bind_text(3, command_type); insert.bind_text(4, idempotency_key)
            _bind_nullable(insert, 5, actor); _bind_nullable(insert, 6, correlation_id); _bind_nullable(insert, 7, causation_id); insert.bind_text(8, payload); insert.bind_text(9, created_at); _ = insert.step(); insert.close()
            var command = CommandRow(run_id=run_id, id=command_id, command_type=command_type, idempotency_key=idempotency_key, actor=actor, correlation_id=correlation_id, causation_id=causation_id, payload=payload, created_at=created_at)
            return DomainCommandStart(command=command^, replayed=False)
        except err:
            self.db.rollback()
            raise err^

    def _append_domain_event_in_tx(mut self, command: CommandRow, item: EventInput) raises -> EventRow:
        if item.id == "" or item.event_type == "" or item.created_at == "" or item.schema_version < 1:
            raise Error(String(SQLiteError(code=1, message="domain store: invalid event")))
        var existing = self.db.query("SELECT run_id,sequence,id,event_type,schema_version,impulse_id,process_id,command_id,actor,correlation_id,causation_id,payload,created_at FROM runtime_events WHERE run_id=? AND id=?")
        existing.bind_text(1, command.run_id); existing.bind_text(2, item.id)
        if existing.step():
            var prior = EventRow(run_id=self._text(existing, 0), sequence=existing.column_int(1), id=self._text(existing, 2), event_type=self._text(existing, 3), schema_version=existing.column_int(4), impulse_id=self._text(existing, 5), process_id=self._text(existing, 6), command_id=self._text(existing, 7), actor=self._text(existing, 8), correlation_id=self._text(existing, 9), causation_id=self._text(existing, 10), payload=self._text(existing, 11), created_at=self._text(existing, 12))
            existing.close()
            var actor = item.actor if item.actor != "" else command.actor
            var correlation = item.correlation_id if item.correlation_id != "" else command.correlation_id
            var causation = item.causation_id if item.causation_id != "" else command.causation_id
            if prior.event_type != item.event_type or prior.impulse_id != item.impulse_id or prior.process_id != item.process_id or prior.command_id != command.id or prior.actor != actor or prior.correlation_id != correlation or prior.causation_id != causation or prior.payload != item.payload or prior.created_at != item.created_at or prior.schema_version != item.schema_version:
                raise Error(String(SQLiteError(code=1, message="domain store: event idempotency conflict")))
            return prior^
        existing.close()
        var next = self.db.query("SELECT COALESCE(MAX(sequence),0)+1 FROM runtime_events WHERE run_id=?")
        next.bind_text(1, command.run_id)
        if not next.step():
            next.close(); raise Error(String(SQLiteError(code=1, message="domain store: event sequence unavailable")))
        var sequence = next.column_int(0); next.close()
        var actor = item.actor if item.actor != "" else command.actor
        var correlation = item.correlation_id if item.correlation_id != "" else command.correlation_id
        var causation = item.causation_id if item.causation_id != "" else command.causation_id
        var insert = self.db.query("INSERT INTO runtime_events (run_id,sequence,id,event_type,schema_version,impulse_id,process_id,command_id,actor,correlation_id,causation_id,payload,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")
        insert.bind_text(1, command.run_id); insert.bind_int(2, sequence); insert.bind_text(3, item.id); insert.bind_text(4, item.event_type); insert.bind_int(5, item.schema_version)
        _bind_nullable(insert, 6, item.impulse_id); _bind_nullable(insert, 7, item.process_id); insert.bind_text(8, command.id); _bind_nullable(insert, 9, actor); _bind_nullable(insert, 10, correlation); _bind_nullable(insert, 11, causation); insert.bind_text(12, item.payload); insert.bind_text(13, item.created_at); _ = insert.step(); insert.close()
        return EventRow(run_id=command.run_id, sequence=sequence, id=item.id, event_type=item.event_type, payload=item.payload, created_at=item.created_at, impulse_id=item.impulse_id, process_id=item.process_id, command_id=command.id, schema_version=item.schema_version, actor=actor, correlation_id=correlation, causation_id=causation)
    def accept_impulse(
        mut self,
        row: Impulse,
        idempotency_key: String,
        created_at: String,
        actor: String = "",
        correlation_id: String = "",
        causation_id: String = "",
    ) raises -> ImpulseAcceptanceResult:
        """Persist an impulse and its acceptance command/event as one unit.

        The idempotency key is run-scoped and becomes the durable command id;
        replays return the existing rows while any payload or identity mismatch
        fails before commit.  The impulse primary key is never overwritten.
        """
        if not row.is_valid() or idempotency_key == "" or created_at == "":
            raise Error(String(SQLiteError(code=1, message="domain store: invalid impulse acceptance")))
        self._require_run(row.run_id)
        var command_id = idempotency_key.copy()
        var event_id = command_id + ":event"
        var command_type = String("impulse.accept")
        var event_type = String("impulse.accepted")
        var payload = row.to_json()
        self.db.begin_immediate()
        try:
            var existing = self.db.query("SELECT run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
            existing.bind_text(1, row.run_id); existing.bind_text(2, idempotency_key)
            if existing.step():
                var stored_command = CommandRow(
                    run_id=self._text(existing, 0), id=self._text(existing, 1),
                    command_type=self._text(existing, 2), idempotency_key=self._text(existing, 3),
                    actor=self._text(existing, 4), correlation_id=self._text(existing, 5),
                    causation_id=self._text(existing, 6), payload=self._text(existing, 7),
                    created_at=self._text(existing, 8),
                )
                if stored_command.command_type != command_type or stored_command.actor != actor or stored_command.correlation_id != correlation_id or stored_command.causation_id != causation_id or stored_command.payload != payload or stored_command.created_at != created_at:
                    raise Error(String(SQLiteError(code=1, message="domain store: impulse acceptance idempotency conflict")))
                var prior_impulse = self.db.query("SELECT id,run_id,impulse_type,payload,metadata,created_at,updated_at FROM impulses WHERE run_id=? AND id=?")
                prior_impulse.bind_text(1, row.run_id); prior_impulse.bind_text(2, row.id)
                var stored_impulse = self._read_impulse(prior_impulse)
                if stored_impulse.id != row.id or stored_impulse.run_id != row.run_id or stored_impulse.impulse_type != row.impulse_type or stored_impulse.payload != row.payload or stored_impulse.metadata != row.metadata or stored_impulse.created_at != row.created_at or stored_impulse.updated_at != row.updated_at:
                    raise Error(String(SQLiteError(code=1, message="domain store: impulse id already exists with different contents")))
                var prior_event = self.db.query("SELECT run_id,sequence,id,event_type,schema_version,impulse_id,process_id,command_id,actor,correlation_id,causation_id,payload,created_at FROM runtime_events WHERE run_id=? AND id=?")
                prior_event.bind_text(1, row.run_id); prior_event.bind_text(2, event_id)
                if not prior_event.step():
                    raise Error(String(SQLiteError(code=1, message="domain store: impulse acceptance event is missing")))
                var stored_event = EventRow(run_id=self._text(prior_event, 0), sequence=prior_event.column_int(1), id=self._text(prior_event, 2), event_type=self._text(prior_event, 3), schema_version=prior_event.column_int(4), impulse_id=self._text(prior_event, 5), process_id=self._text(prior_event, 6), command_id=self._text(prior_event, 7), actor=self._text(prior_event, 8), correlation_id=self._text(prior_event, 9), causation_id=self._text(prior_event, 10), payload=self._text(prior_event, 11), created_at=self._text(prior_event, 12))
                if stored_event.event_type != event_type or stored_event.impulse_id != row.id or stored_event.command_id != stored_command.id or stored_event.payload != payload or stored_event.created_at != created_at:
                    raise Error(String(SQLiteError(code=1, message="domain store: impulse acceptance event conflict")))
                var replay_events = List[EventRow](); replay_events.append(stored_event^)
                self.db.commit()
                return ImpulseAcceptanceResult(impulse=stored_impulse^, command=stored_command^, events=replay_events^, replayed=True)
            var existing_id = self.db.query("SELECT id FROM runtime_commands WHERE run_id=? AND id=?")
            existing_id.bind_text(1, row.run_id); existing_id.bind_text(2, command_id)
            if existing_id.step():
                raise Error(String(SQLiteError(code=1, message="domain store: impulse acceptance command id already exists")))
            var existing_impulse = self.db.query("SELECT 1 FROM impulses WHERE run_id=? AND id=?")
            existing_impulse.bind_text(1, row.run_id); existing_impulse.bind_text(2, row.id)
            if existing_impulse.step():
                raise Error(String(SQLiteError(code=1, message="domain store: impulse id already exists")))
            var command = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at) VALUES (?,?,?,?,?,?,?,?,?)")
            command.bind_text(1, row.run_id); command.bind_text(2, command_id); command.bind_text(3, command_type); command.bind_text(4, idempotency_key)
            _bind_nullable(command, 5, actor); _bind_nullable(command, 6, correlation_id); _bind_nullable(command, 7, causation_id); command.bind_text(8, payload); command.bind_text(9, created_at); _ = command.step()
            var impulse = self.db.query("INSERT INTO impulses (run_id,id,impulse_type,payload,metadata,created_at,updated_at) VALUES (?,?,?,?,?,?,?)")
            impulse.bind_text(1, row.run_id); impulse.bind_text(2, row.id); impulse.bind_text(3, row.impulse_type); impulse.bind_text(4, row.payload); impulse.bind_text(5, row.metadata); impulse.bind_text(6, row.created_at); impulse.bind_text(7, row.updated_at); _ = impulse.step()
            var next = self.db.query("SELECT COALESCE(MAX(sequence),0)+1 FROM runtime_events WHERE run_id=?"); next.bind_text(1, row.run_id)
            if not next.step(): raise Error(String(SQLiteError(code=1, message="domain store: unable to allocate impulse event sequence")))
            var event = self.db.query("INSERT INTO runtime_events (run_id,sequence,id,event_type,schema_version,impulse_id,command_id,actor,correlation_id,causation_id,payload,created_at) VALUES (?,?,?, ?,1,?,?,?,?,?,?,?)")
            event.bind_text(1, row.run_id); event.bind_int(2, next.column_int(0)); event.bind_text(3, event_id); event.bind_text(4, event_type); event.bind_text(5, row.id); event.bind_text(6, command_id)
            _bind_nullable(event, 7, actor); _bind_nullable(event, 8, correlation_id); _bind_nullable(event, 9, causation_id); event.bind_text(10, payload); event.bind_text(11, created_at); _ = event.step()
            self.db.commit()
            var stored_events = List[EventRow]()
            var result_event = self.db.query("SELECT run_id,sequence,id,event_type,schema_version,impulse_id,process_id,command_id,actor,correlation_id,causation_id,payload,created_at FROM runtime_events WHERE run_id=? AND id=?")
            result_event.bind_text(1, row.run_id); result_event.bind_text(2, event_id)
            if not result_event.step(): raise Error(String(SQLiteError(code=1, message="domain store: stored impulse event is missing")))
            stored_events.append(EventRow(run_id=self._text(result_event, 0), sequence=result_event.column_int(1), id=self._text(result_event, 2), event_type=self._text(result_event, 3), schema_version=result_event.column_int(4), impulse_id=self._text(result_event, 5), process_id=self._text(result_event, 6), command_id=self._text(result_event, 7), actor=self._text(result_event, 8), correlation_id=self._text(result_event, 9), causation_id=self._text(result_event, 10), payload=self._text(result_event, 11), created_at=self._text(result_event, 12))^)
            var stored = Impulse(id=row.id, run_id=row.run_id, impulse_type=row.impulse_type, payload=row.payload, metadata=row.metadata, created_at=row.created_at, updated_at=row.updated_at)
            var command_row = CommandRow(run_id=row.run_id, id=command_id, command_type=command_type, idempotency_key=idempotency_key, actor=actor, correlation_id=correlation_id, causation_id=causation_id, payload=payload, created_at=created_at)
            return ImpulseAcceptanceResult(impulse=stored^, command=command_row^, events=stored_events^, replayed=False)
        except err:
            self.db.rollback()
            raise err^

    def put_impulse(mut self, row: Impulse) raises:
        if not row.is_valid():
            raise Error(String(SQLiteError(code=1, message="domain store: invalid impulse")))
        self._require_run(row.run_id)
        self.db.begin()
        try:
            var stmt = self.db.query("INSERT INTO impulses (run_id,id,impulse_type,payload,metadata,created_at,updated_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(run_id,id) DO UPDATE SET impulse_type=excluded.impulse_type,payload=excluded.payload,metadata=excluded.metadata,created_at=excluded.created_at,updated_at=excluded.updated_at")
            stmt.bind_text(1, row.run_id)
            stmt.bind_text(2, row.id)
            stmt.bind_text(3, row.impulse_type)
            stmt.bind_text(4, row.payload)
            stmt.bind_text(5, row.metadata)
            stmt.bind_text(6, row.created_at)
            stmt.bind_text(7, row.updated_at)
            _ = stmt.step()
            self.db.commit()
        except err:
            self.db.rollback()
            raise Error(String(SQLiteError(code=1, message="domain store: put_impulse failed")))

    def list_impulses(mut self, run_id: String) raises -> List[String]:
        self._require_run(run_id)
        var result = List[String]()
        var stmt = self.db.query("SELECT id,impulse_type,payload,metadata,created_at,updated_at FROM impulses WHERE run_id=? ORDER BY created_at ASC, id ASC")
        stmt.bind_text(1, run_id)
        while stmt.step():
            var item = "{\"id\":" + _quote(self._text(stmt, 0)) + ",\"run_id\":" + _quote(run_id) + ",\"impulse_type\":" + _quote(self._text(stmt, 1)) + ",\"payload\":" + self._text(stmt, 2) + ",\"metadata\":" + self._text(stmt, 3) + ",\"created_at\":" + _quote(self._text(stmt, 4)) + ",\"updated_at\":" + _quote(self._text(stmt, 5)) + "}"
            result.append(item^)
        return result^

    def put_impulse_type(mut self, row: ImpulseType) raises:
        if not row.is_valid():
            raise Error(String(SQLiteError(code=1, message="domain store: invalid impulse type")))
        self._require_run(row.run_id)
        self.db.begin()
        try:
            var stmt = self.db.query("INSERT INTO impulse_types (run_id,id,title,description,media_types,value_schema_json,metadata,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(run_id,id) DO UPDATE SET title=excluded.title,description=excluded.description,media_types=excluded.media_types,value_schema_json=excluded.value_schema_json,metadata=excluded.metadata,created_at=excluded.created_at,updated_at=excluded.updated_at")
            stmt.bind_text(1, row.run_id); stmt.bind_text(2, row.id); _bind_nullable(stmt, 3, row.title); _bind_nullable(stmt, 4, row.description)
            stmt.bind_text(5, row.media_types); stmt.bind_text(6, row.value_schema); stmt.bind_text(7, row.metadata); stmt.bind_text(8, row.created_at); stmt.bind_text(9, row.updated_at)
            _ = stmt.step(); self.db.commit()
        except err:
            self.db.rollback()
            raise Error(String(SQLiteError(code=1, message="domain store: put_impulse_type failed")))

    def list_impulse_types(mut self, run_id: String) raises -> List[String]:
        self._require_run(run_id)
        var result = List[String]()
        var stmt = self.db.query("SELECT id,title,description,media_types,value_schema_json,metadata,created_at,updated_at FROM impulse_types WHERE run_id=? ORDER BY id ASC")
        stmt.bind_text(1, run_id)
        while stmt.step():
            var item = "{\"id\":" + _quote(self._text(stmt, 0)) + ",\"run_id\":" + _quote(run_id) + ",\"title\":" + _nullable_json(stmt, 1) + ",\"description\":" + _nullable_json(stmt, 2) + ",\"media_types\":" + self._text(stmt, 3) + ",\"value_schema\":" + self._text(stmt, 4) + ",\"metadata\":" + self._text(stmt, 5) + ",\"created_at\":" + _quote(self._text(stmt, 6)) + ",\"updated_at\":" + _quote(self._text(stmt, 7)) + "}"
            result.append(item^)
        return result^
    def register_impulse_type(
        mut self, row: ImpulseType, command_id: String, command_type: String,
        idempotency_key: String, created_at: String, events: List[EventInput] = List[EventInput](),
        actor: String = "", correlation_id: String = "", causation_id: String = "",
    ) raises -> CommandSubmission:
        if command_type != "impulse_type.register":
            raise Error(String(SQLiteError(code=1, message="register_impulse_type requires command_type 'impulse_type.register'")))
        if not row.is_valid(): raise Error(String(SQLiteError(code=1, message="domain store: invalid impulse type")))
        var start = self._domain_command_start(row.run_id, command_id, command_type, idempotency_key, row.to_json(), created_at, actor, correlation_id, causation_id)
        var command = start.command.copy()
        var stored_events = List[EventRow]()
        if start.replayed:
            var prior = self.get_impulse_type(row.run_id, row.id)
            if prior.to_json() != row.to_json(): raise Error(String(SQLiteError(code=1, message="domain store: impulse type idempotency conflict")))
            return CommandSubmission(command=command^, events=stored_events^, replayed=True)
        try:
            var insert = self.db.query("INSERT INTO impulse_types (run_id,id,title,description,media_types,value_schema_json,metadata,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)")
            insert.bind_text(1, row.run_id); insert.bind_text(2, row.id); _bind_nullable(insert, 3, row.title); _bind_nullable(insert, 4, row.description); insert.bind_text(5, row.media_types); insert.bind_text(6, row.value_schema); insert.bind_text(7, row.metadata); insert.bind_text(8, row.created_at); insert.bind_text(9, row.updated_at); _ = insert.step(); insert.close()
            for item in events: stored_events.append(self._append_domain_event_in_tx(command, item)^)
            self.db.commit()
            return CommandSubmission(command=command^, events=stored_events^, replayed=False)
        except err:
            self.db.rollback(); raise err^

    def record_impulse_relation(
        mut self, row: ImpulseRelation, command_id: String, command_type: String,
        idempotency_key: String, created_at: String, events: List[EventInput] = List[EventInput](),
        actor: String = "", correlation_id: String = "", causation_id: String = "",
    ) raises -> CommandSubmission:
        if command_type != "impulse_relation.record": raise Error(String(SQLiteError(code=1, message="record_impulse_relation requires command_type 'impulse_relation.record'")))
        if not row.is_valid(): raise Error(String(SQLiteError(code=1, message="domain store: invalid impulse relation")))
        var start = self._domain_command_start(row.run_id, command_id, command_type, idempotency_key, row.to_json(), created_at, actor, correlation_id, causation_id)
        var command = start.command.copy(); var stored_events = List[EventRow]()
        if start.replayed:
            var prior = self.get_impulse_relation(row.run_id, row.id)
            if prior.to_json() != row.to_json(): raise Error(String(SQLiteError(code=1, message="domain store: impulse relation idempotency conflict")))
            return CommandSubmission(command=command^, events=stored_events^, replayed=True)
        try:
            var existing = self.db.query("SELECT 1 FROM impulse_relations WHERE run_id=? AND id=?"); existing.bind_text(1, row.run_id); existing.bind_text(2, row.id)
            if existing.step(): existing.close(); raise Error(String(SQLiteError(code=1, message="domain store: impulse relation already exists")))
            existing.close()
            var insert = self.db.query("INSERT INTO impulse_relations (run_id,id,relation_type,source_impulse_id,target_impulse_id,metadata,created_at) VALUES (?,?,?,?,?,?,?)")
            insert.bind_text(1, row.run_id); insert.bind_text(2, row.id); insert.bind_text(3, row.relation_type); insert.bind_text(4, row.source_impulse_id); insert.bind_text(5, row.target_impulse_id); insert.bind_text(6, row.metadata); insert.bind_text(7, row.created_at); _ = insert.step(); insert.close()
            for item in events: stored_events.append(self._append_domain_event_in_tx(command, item)^)
            self.db.commit(); return CommandSubmission(command=command^, events=stored_events^, replayed=False)
        except err:
            self.db.rollback(); raise err^

    def record_association(
        mut self, row: Association, command_id: String, command_type: String,
        idempotency_key: String, created_at: String, events: List[EventInput] = List[EventInput](),
        actor: String = "", correlation_id: String = "", causation_id: String = "",
    ) raises -> CommandSubmission:
        if command_type != "association.record": raise Error(String(SQLiteError(code=1, message="record_association requires command_type 'association.record'")))
        if not row.is_valid(): raise Error(String(SQLiteError(code=1, message="domain store: invalid association")))
        var start = self._domain_command_start(row.run_id, command_id, command_type, idempotency_key, row.to_json(), created_at, actor, correlation_id, causation_id)
        var command = start.command.copy(); var stored_events = List[EventRow]()
        if start.replayed:
            var prior = self.get_association(row.run_id, row.id)
            if prior.to_json() != row.to_json(): raise Error(String(SQLiteError(code=1, message="domain store: association idempotency conflict")))
            return CommandSubmission(command=command^, events=stored_events^, replayed=True)
        try:
            var existing = self.db.query("SELECT 1 FROM associations WHERE run_id=? AND id=?"); existing.bind_text(1, row.run_id); existing.bind_text(2, row.id)
            if existing.step(): existing.close(); raise Error(String(SQLiteError(code=1, message="domain store: association already exists")))
            existing.close()
            var insert = self.db.query("INSERT INTO associations (run_id,id,kind,impulse_id,values_json,metadata,created_at) VALUES (?,?,?,?,?,?,?)")
            insert.bind_text(1, row.run_id); insert.bind_text(2, row.id); insert.bind_text(3, row.kind); _bind_nullable(insert, 4, row.impulse_id); insert.bind_text(5, row.values); insert.bind_text(6, row.metadata); insert.bind_text(7, row.created_at); _ = insert.step(); insert.close()
            for item in events: stored_events.append(self._append_domain_event_in_tx(command, item)^)
            self.db.commit(); return CommandSubmission(command=command^, events=stored_events^, replayed=False)
        except err:
            self.db.rollback()
            raise err^

    def record_reaction(
        mut self, row: Reaction, command_id: String, command_type: String,
        idempotency_key: String, created_at: String, events: List[EventInput] = List[EventInput](),
        actor: String = "", correlation_id: String = "", causation_id: String = "",
    ) raises -> CommandSubmission:
        if command_type != "reaction.record": raise Error(String(SQLiteError(code=1, message="record_reaction requires command_type 'reaction.record'")))
        if not row.is_valid(): raise Error(String(SQLiteError(code=1, message="domain store: invalid reaction")))
        _validate_reaction_cas_identity(row)
        var normalized = row.copy()
        normalized.metadata = _canonical_reaction_metadata(row.metadata)
        var normalized_events = List[EventInput]()
        for event in events:
            var compact_event = event.copy()
            if compact_event.event_type == "reaction.recorded": compact_event.payload = _reaction_journal_payload(normalized)
            normalized_events.append(compact_event^)
        var start = self._domain_command_start(normalized.run_id, command_id, command_type, idempotency_key, _reaction_journal_payload(normalized), created_at, actor, correlation_id, causation_id)
        var command = start.command.copy(); var stored_events = List[EventRow]()
        if start.replayed:
            var prior = self.get_reaction(normalized.run_id, normalized.id)
            if prior.to_json() != normalized.to_json(): raise Error(String(SQLiteError(code=1, message="domain store: reaction idempotency conflict")))
            return CommandSubmission(command=command^, events=stored_events^, replayed=True)
        try:
            var existing = self.db.query("SELECT 1 FROM reactions WHERE run_id=? AND id=?"); existing.bind_text(1, normalized.run_id); existing.bind_text(2, normalized.id)
            if existing.step(): existing.close(); raise Error(String(SQLiteError(code=1, message="domain store: reaction already exists")))
            existing.close()
            var insert = self.db.query("INSERT INTO reactions (run_id,id,kind,uri,impulse_id,media_type,size_bytes,content_hash,metadata,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)")
            insert.bind_text(1, normalized.run_id); insert.bind_text(2, normalized.id); insert.bind_text(3, normalized.kind); insert.bind_text(4, normalized.uri); _bind_nullable(insert, 5, normalized.impulse_id); _bind_nullable(insert, 6, normalized.media_type)
            if normalized.size_bytes < 0: insert.bind_null(7)
            else: insert.bind_int(7, normalized.size_bytes)
            _bind_nullable(insert, 8, normalized.content_hash); insert.bind_text(9, normalized.metadata); insert.bind_text(10, normalized.created_at); _ = insert.step(); insert.close()
            for item in normalized_events: stored_events.append(self._append_domain_event_in_tx(command, item)^)
            self.db.commit(); return CommandSubmission(command=command^, events=stored_events^, replayed=False)
        except err:
            self.db.rollback(); raise err^
    def save_homeostat(
        mut self, row: Homeostat, command_id: String, command_type: String,
        idempotency_key: String, created_at: String, events: List[EventInput] = List[EventInput](),
        actor: String = "", correlation_id: String = "", causation_id: String = "",
    ) raises -> CommandSubmission:
        if command_type != "homeostat.save" and command_type != "homeostat.open":
            raise Error(String(SQLiteError(code=1, message="save_homeostat requires command_type 'homeostat.save' or 'homeostat.open'")))
        if not row.is_valid(): raise Error(String(SQLiteError(code=1, message="domain store: invalid homeostat")))
        var start = self._domain_command_start(row.run_id, command_id, command_type, idempotency_key, row.to_json(), created_at, actor, correlation_id, causation_id)
        var command = start.command.copy()
        var stored_events = List[EventRow]()
        if start.replayed:
            var prior = self.get_homeostat(row.run_id, row.id)
            if prior.to_json() != row.to_json(): raise Error(String(SQLiteError(code=1, message="domain store: homeostat idempotency conflict")))
            return CommandSubmission(command=command^, events=stored_events^, replayed=True)
        try:
            var existing = self.db.query("SELECT 1 FROM homeostats WHERE run_id=? AND id=?")
            existing.bind_text(1, row.run_id); existing.bind_text(2, row.id)
            if existing.step():
                existing.close()
                raise Error(String(SQLiteError(code=1, message="domain store: homeostat already exists")))
            existing.close()
            var insert = self.db.query("INSERT INTO homeostats (run_id,id,kind,impulse_id,status,values_json,metadata,attempt,max_attempts,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)")
            insert.bind_text(1, row.run_id); insert.bind_text(2, row.id); insert.bind_text(3, row.kind); _bind_nullable(insert, 4, row.impulse_id)
            insert.bind_text(5, row.status); insert.bind_text(6, row.values); insert.bind_text(7, row.metadata); insert.bind_int(8, row.attempt); insert.bind_int(9, row.max_attempts); insert.bind_text(10, row.created_at); insert.bind_text(11, row.updated_at); _ = insert.step(); insert.close()
            for item in events: stored_events.append(self._append_domain_event_in_tx(command, item)^)
            self.db.commit()
            return CommandSubmission(command=command^, events=stored_events^, replayed=False)
        except err:
            self.db.rollback(); raise err^

    def transition_homeostat(
        mut self, row: Homeostat, command_id: String, command_type: String,
        idempotency_key: String, created_at: String, events: List[EventInput] = List[EventInput](),
        actor: String = "", correlation_id: String = "", causation_id: String = "",
    ) raises -> HomeostatTransitionResult:
        if row.status != "completed" and row.status != "cancelled" and row.status != "expired":
            raise Error(String(SQLiteError(code=1, message="domain store: invalid homeostat terminal status")))
        var expected = "homeostat.complete"
        if row.status == "cancelled": expected = "homeostat.cancel"
        elif row.status == "expired": expected = "homeostat.expire"
        if command_type != expected:
            raise Error(String(SQLiteError(code=1, message="transition_homeostat requires command_type '" + expected + "'")))
        if not row.is_valid(): raise Error(String(SQLiteError(code=1, message="domain store: invalid homeostat")))
        var payload = row.to_json()
        var start = self._domain_command_start(row.run_id, command_id, command_type, idempotency_key, payload, created_at, actor, correlation_id, causation_id)
        var command = start.command.copy()
        var stored_events = List[EventRow]()
        if start.replayed:
            var replayed = self.get_homeostat(row.run_id, row.id)
            return HomeostatTransitionResult(homeostat=replayed^, submission=CommandSubmission(command=command^, events=stored_events^, replayed=True))
        try:
            var current = self.get_homeostat(row.run_id, row.id)
            if current.status != "open": raise Error(String(SQLiteError(code=1, message="domain store: homeostat is not open")))
            var at = row.updated_at
            if at == "": at = created_at
            var update = self.db.query("UPDATE homeostats SET status=?,values_json=?,updated_at=? WHERE run_id=? AND id=? AND status='open'")
            update.bind_text(1, row.status); update.bind_text(2, row.values); update.bind_text(3, at); update.bind_text(4, row.run_id); update.bind_text(5, row.id); _ = update.step(); update.close()
            if self.db.changes() != 1: raise Error(String(SQLiteError(code=1, message="domain store: homeostat transition lost ownership")))
            for item in events: stored_events.append(self._append_domain_event_in_tx(command, item)^)
            self.db.commit()
            var updated = self.get_homeostat(row.run_id, row.id)
            return HomeostatTransitionResult(homeostat=updated^, submission=CommandSubmission(command=command^, events=stored_events^, replayed=False))
        except err:
            self.db.rollback(); raise err^

    def save_projection(
        mut self, row: Projection, command_id: String, command_type: String,
        idempotency_key: String, created_at: String, events: List[EventInput] = List[EventInput](),
        actor: String = "", correlation_id: String = "", causation_id: String = "",
    ) raises -> CommandSubmission:
        if command_type != "projection.save":
            raise Error(String(SQLiteError(code=1, message="save_projection requires command_type 'projection.save'")))
        if not row.is_valid(): raise Error(String(SQLiteError(code=1, message="domain store: invalid projection")))
        var start = self._domain_command_start(row.run_id, command_id, command_type, idempotency_key, row.to_json(), created_at, actor, correlation_id, causation_id)
        var command = start.command.copy()
        var stored_events = List[EventRow]()
        if start.replayed:
            var prior = self.get_projection(row.run_id, row.name)
            if prior.id != row.id or prior.run_id != row.run_id or prior.name != row.name or prior.version != row.version or prior.data != row.data or prior.source_event_sequence != row.source_event_sequence or prior.updated_at != row.updated_at:
                raise Error(String(SQLiteError(code=1, message="domain store: projection idempotency conflict")))
            return CommandSubmission(command=command^, events=stored_events^, replayed=True)
        try:
            var insert = self.db.query("INSERT INTO projections (run_id,name,id,version,data,source_event_sequence,updated_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(run_id,name) DO UPDATE SET id=excluded.id,version=excluded.version,data=excluded.data,source_event_sequence=excluded.source_event_sequence,updated_at=excluded.updated_at")
            insert.bind_text(1, row.run_id); insert.bind_text(2, row.name); insert.bind_text(3, row.id); insert.bind_int(4, row.version); insert.bind_text(5, row.data); insert.bind_int(6, row.source_event_sequence); insert.bind_text(7, row.updated_at); _ = insert.step(); insert.close()
            for item in events: stored_events.append(self._append_domain_event_in_tx(command, item)^)
            self.db.commit()
            return CommandSubmission(command=command^, events=stored_events^, replayed=False)
        except err:
            self.db.rollback(); raise err^

    def put_impulse_relation(mut self, row: ImpulseRelation) raises:
        if not row.is_valid():
            raise Error(String(SQLiteError(code=1, message="domain store: invalid impulse relation")))
        self._require_run(row.run_id)
        self.db.begin()
        try:
            var stmt = self.db.query("INSERT INTO impulse_relations (run_id,id,relation_type,source_impulse_id,target_impulse_id,metadata,created_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(run_id,id) DO UPDATE SET relation_type=excluded.relation_type,source_impulse_id=excluded.source_impulse_id,target_impulse_id=excluded.target_impulse_id,metadata=excluded.metadata,created_at=excluded.created_at")
            stmt.bind_text(1, row.run_id); stmt.bind_text(2, row.id); stmt.bind_text(3, row.relation_type)
            stmt.bind_text(4, row.source_impulse_id); stmt.bind_text(5, row.target_impulse_id); stmt.bind_text(6, row.metadata); stmt.bind_text(7, row.created_at)
            _ = stmt.step(); self.db.commit()
        except err:
            self.db.rollback()
            raise Error(String(SQLiteError(code=1, message="domain store: put_impulse_relation failed")))

    def list_impulse_relations(mut self, run_id: String) raises -> List[String]:
        self._require_run(run_id)
        var result = List[String]()
        var stmt = self.db.query("SELECT id,relation_type,source_impulse_id,target_impulse_id,metadata,created_at FROM impulse_relations WHERE run_id=? ORDER BY created_at ASC, id ASC")
        stmt.bind_text(1, run_id)
        while stmt.step():
            var item = "{\"id\":" + _quote(self._text(stmt, 0)) + ",\"run_id\":" + _quote(run_id) + ",\"relation_type\":" + _quote(self._text(stmt, 1)) + ",\"source_impulse_id\":" + _quote(self._text(stmt, 2)) + ",\"target_impulse_id\":" + _quote(self._text(stmt, 3)) + ",\"metadata\":" + self._text(stmt, 4) + ",\"created_at\":" + _quote(self._text(stmt, 5)) + "}"
            result.append(item^)
        return result^

    def put_association(mut self, row: Association) raises:
        if not row.is_valid():
            raise Error(String(SQLiteError(code=1, message="domain store: invalid association")))
        self._require_run(row.run_id)
        self.db.begin()
        try:
            var stmt = self.db.query("INSERT INTO associations (run_id,id,kind,impulse_id,values_json,metadata,created_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(run_id,id) DO UPDATE SET kind=excluded.kind,impulse_id=excluded.impulse_id,values_json=excluded.values_json,metadata=excluded.metadata,created_at=excluded.created_at")
            stmt.bind_text(1, row.run_id); stmt.bind_text(2, row.id); stmt.bind_text(3, row.kind); _bind_nullable(stmt, 4, row.impulse_id)
            stmt.bind_text(5, row.values); stmt.bind_text(6, row.metadata); stmt.bind_text(7, row.created_at)
            _ = stmt.step(); self.db.commit()
        except err:
            self.db.rollback()
            raise Error(String(SQLiteError(code=1, message="domain store: put_association failed")))

    def list_associations(mut self, run_id: String) raises -> List[String]:
        self._require_run(run_id)
        var result = List[String]()
        var stmt = self.db.query("SELECT id,kind,impulse_id,values_json,metadata,created_at FROM associations WHERE run_id=? ORDER BY created_at ASC, id ASC")
        stmt.bind_text(1, run_id)
        while stmt.step():
            var item = "{\"id\":" + _quote(self._text(stmt, 0)) + ",\"run_id\":" + _quote(run_id) + ",\"kind\":" + _quote(self._text(stmt, 1)) + ",\"impulse_id\":" + _nullable_json(stmt, 2) + ",\"values\":" + self._text(stmt, 3) + ",\"metadata\":" + self._text(stmt, 4) + ",\"created_at\":" + _quote(self._text(stmt, 5)) + "}"
            result.append(item^)
        return result^

    def put_reaction(mut self, row: Reaction) raises:
        if not row.is_valid():
            raise Error(String(SQLiteError(code=1, message="domain store: invalid reaction")))
        _validate_reaction_cas_identity(row)
        self._require_run(row.run_id)
        var normalized = row.copy()
        normalized.metadata = _canonical_reaction_metadata(row.metadata)
        var existing = self.db.query("SELECT kind,uri,impulse_id,media_type,size_bytes,content_hash,metadata,created_at FROM reactions WHERE run_id=? AND id=?")
        existing.bind_text(1, normalized.run_id); existing.bind_text(2, normalized.id)
        if existing.step():
            var existing_size = -1
            if not existing.column_null(4): existing_size = existing.column_int(4)
            var conflict = self._text(existing, 0) != normalized.kind or self._text(existing, 1) != normalized.uri or self._text(existing, 2) != normalized.impulse_id or self._text(existing, 3) != normalized.media_type or existing_size != normalized.size_bytes or self._text(existing, 5) != normalized.content_hash or self._text(existing, 6) != normalized.metadata or self._text(existing, 7) != normalized.created_at
            existing.close()
            if conflict:
                raise Error(String(SQLiteError(code=1, message="domain store: reaction id already exists with different data")))
            return
        existing.close()
        self.db.begin()
        try:
            var stmt = self.db.query("INSERT INTO reactions (run_id,id,kind,uri,impulse_id,media_type,size_bytes,content_hash,metadata,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)")
            stmt.bind_text(1, normalized.run_id); stmt.bind_text(2, normalized.id); stmt.bind_text(3, normalized.kind); stmt.bind_text(4, normalized.uri); _bind_nullable(stmt, 5, normalized.impulse_id); _bind_nullable(stmt, 6, normalized.media_type)
            if normalized.size_bytes < 0: stmt.bind_null(7)
            else: stmt.bind_int(7, normalized.size_bytes)
            _bind_nullable(stmt, 8, normalized.content_hash); stmt.bind_text(9, normalized.metadata); stmt.bind_text(10, normalized.created_at)
            _ = stmt.step(); self.db.commit()
        except err:
            self.db.rollback()
            raise Error(String(SQLiteError(code=1, message="domain store: put_reaction failed")))

    def list_reactions(mut self, run_id: String) raises -> List[String]:
        self._require_run(run_id)
        var result = List[String]()
        var stmt = self.db.query("SELECT id,kind,uri,impulse_id,media_type,size_bytes,content_hash,metadata,created_at FROM reactions WHERE run_id=? ORDER BY created_at ASC, id ASC")
        stmt.bind_text(1, run_id)
        while stmt.step():
            var item = "{\"id\":" + _quote(self._text(stmt, 0)) + ",\"run_id\":" + _quote(run_id) + ",\"kind\":" + _quote(self._text(stmt, 1)) + ",\"uri\":" + _quote(self._text(stmt, 2)) + ",\"impulse_id\":" + _nullable_json(stmt, 3) + ",\"media_type\":" + _nullable_json(stmt, 4) + ",\"size_bytes\":" + ("null" if stmt.column_null(5) else String(stmt.column_int(5))) + ",\"content_hash\":" + _nullable_json(stmt, 6) + ",\"metadata\":" + self._text(stmt, 7) + ",\"created_at\":" + _quote(self._text(stmt, 8)) + "}"
            result.append(item^)
        return result^

    def put_homeostat(mut self, row: Homeostat) raises:
        if not row.is_valid():
            raise Error(String(SQLiteError(code=1, message="domain store: invalid homeostat")))
        self._require_run(row.run_id)
        var existing = self.db.query("SELECT kind,impulse_id,status,values_json,metadata,attempt,max_attempts,created_at,updated_at FROM homeostats WHERE run_id=? AND id=?")
        existing.bind_text(1, row.run_id); existing.bind_text(2, row.id)
        if existing.step():
            var conflict = self._text(existing, 0) != row.kind or self._text(existing, 1) != row.impulse_id or self._text(existing, 2) != row.status or self._text(existing, 3) != row.values or self._text(existing, 4) != row.metadata or existing.column_int(5) != row.attempt or existing.column_int(6) != row.max_attempts or self._text(existing, 7) != row.created_at or self._text(existing, 8) != row.updated_at
            existing.close()
            if conflict:
                raise Error(String(SQLiteError(code=1, message="domain store: homeostat id already exists with different data")))
            return
        existing.close()
        self.db.begin()
        try:
            var stmt = self.db.query("INSERT INTO homeostats (run_id,id,kind,impulse_id,status,values_json,metadata,attempt,max_attempts,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)")
            stmt.bind_text(1, row.run_id); stmt.bind_text(2, row.id); stmt.bind_text(3, row.kind); _bind_nullable(stmt, 4, row.impulse_id); stmt.bind_text(5, row.status); stmt.bind_text(6, row.values); stmt.bind_text(7, row.metadata); stmt.bind_int(8, row.attempt); stmt.bind_int(9, row.max_attempts); stmt.bind_text(10, row.created_at); stmt.bind_text(11, row.updated_at)
            _ = stmt.step(); self.db.commit()
        except err:
            self.db.rollback()
            raise Error(String(SQLiteError(code=1, message="domain store: put_homeostat failed")))

    def list_homeostats(mut self, run_id: String) raises -> List[String]:
        self._require_run(run_id)
        var result = List[String]()
        var stmt = self.db.query("SELECT id,kind,impulse_id,status,values_json,metadata,attempt,max_attempts,created_at,updated_at FROM homeostats WHERE run_id=? ORDER BY updated_at ASC, id ASC")
        stmt.bind_text(1, run_id)
        while stmt.step():
            var item = "{\"id\":" + _quote(self._text(stmt, 0)) + ",\"run_id\":" + _quote(run_id) + ",\"kind\":" + _quote(self._text(stmt, 1)) + ",\"impulse_id\":" + _nullable_json(stmt, 2) + ",\"status\":" + _quote(self._text(stmt, 3)) + ",\"values\":" + self._text(stmt, 4) + ",\"metadata\":" + self._text(stmt, 5) + ",\"attempt\":" + String(stmt.column_int(6)) + ",\"max_attempts\":" + String(stmt.column_int(7)) + ",\"created_at\":" + _quote(self._text(stmt, 8)) + ",\"updated_at\":" + _quote(self._text(stmt, 9)) + "}"
            result.append(item^)
        return result^


    def put_projection(mut self, row: Projection) raises:
        if not row.is_valid():
            raise Error(String(SQLiteError(code=1, message="domain store: invalid projection")))
        self._require_run(row.run_id)
        self.db.begin()
        try:
            var stmt = self.db.query("INSERT INTO projections (run_id,name,id,version,data,source_event_sequence,updated_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(run_id,name) DO UPDATE SET id=excluded.id,version=excluded.version,data=excluded.data,source_event_sequence=excluded.source_event_sequence,updated_at=excluded.updated_at")
            stmt.bind_text(1, row.run_id); stmt.bind_text(2, row.name); stmt.bind_text(3, row.id); stmt.bind_int(4, row.version); stmt.bind_text(5, row.data); stmt.bind_int(6, row.source_event_sequence); stmt.bind_text(7, row.updated_at)
            _ = stmt.step(); self.db.commit()
        except err:
            self.db.rollback()
            raise Error(String(SQLiteError(code=1, message="domain store: put_projection failed")))

    def list_projections(mut self, run_id: String) raises -> List[String]:
        self._require_run(run_id)
        var result = List[String]()
        var stmt = self.db.query("SELECT p.id,p.name,p.version,p.data,p.source_event_sequence,p.updated_at,CASE WHEN EXISTS (SELECT 1 FROM runtime_events e WHERE e.run_id=p.run_id AND e.sequence>p.source_event_sequence) THEN 1 ELSE 0 END FROM projections p WHERE p.run_id=? ORDER BY p.name ASC")
        stmt.bind_text(1, run_id)
        while stmt.step():
            var stale = "false"
            if stmt.column_int(6) != 0: stale = "true"
            var item = "{\"id\":" + _quote(self._text(stmt, 0)) + ",\"run_id\":" + _quote(run_id) + ",\"name\":" + _quote(self._text(stmt, 1)) + ",\"version\":" + String(stmt.column_int(2)) + ",\"data\":" + self._text(stmt, 3) + ",\"source_event_sequence\":" + String(stmt.column_int(4)) + ",\"updated_at\":" + _quote(self._text(stmt, 5)) + ",\"stale\":" + stale + "}"
            result.append(item^)
        return result^


    @staticmethod
    def _read_impulse(mut stmt: Statement) raises -> Impulse:
        if not stmt.step():
            stmt.close()
            raise Error(String(SQLiteError(code=1, message="domain store: impulse not found")))
        var row = Impulse(id=NativeDomainStore._text(stmt, 0), run_id=NativeDomainStore._text(stmt, 1), impulse_type=NativeDomainStore._text(stmt, 2), payload=NativeDomainStore._text(stmt, 3), metadata=NativeDomainStore._text(stmt, 4), created_at=NativeDomainStore._text(stmt, 5), updated_at=NativeDomainStore._text(stmt, 6))
        stmt.close()
        return row^

    @staticmethod
    def _read_impulse_type(mut stmt: Statement) raises -> ImpulseType:
        if not stmt.step():
            stmt.close()
            raise Error(String(SQLiteError(code=1, message="domain store: impulse type not found")))
        var row = ImpulseType(id=NativeDomainStore._text(stmt, 0), run_id=NativeDomainStore._text(stmt, 1), title=NativeDomainStore._text(stmt, 2), description=NativeDomainStore._text(stmt, 3), media_types=NativeDomainStore._text(stmt, 4), value_schema=NativeDomainStore._text(stmt, 5), metadata=NativeDomainStore._text(stmt, 6), created_at=NativeDomainStore._text(stmt, 7), updated_at=NativeDomainStore._text(stmt, 8))
        stmt.close()
        return row^

    @staticmethod
    def _read_impulse_relation(mut stmt: Statement) raises -> ImpulseRelation:
        if not stmt.step():
            stmt.close()
            raise Error(String(SQLiteError(code=1, message="domain store: impulse relation not found")))
        var row = ImpulseRelation(id=NativeDomainStore._text(stmt, 0), run_id=NativeDomainStore._text(stmt, 1), relation_type=NativeDomainStore._text(stmt, 2), source_impulse_id=NativeDomainStore._text(stmt, 3), target_impulse_id=NativeDomainStore._text(stmt, 4), metadata=NativeDomainStore._text(stmt, 5), created_at=NativeDomainStore._text(stmt, 6))
        stmt.close()
        return row^

    @staticmethod
    def _read_association(mut stmt: Statement) raises -> Association:
        if not stmt.step():
            stmt.close()
            raise Error(String(SQLiteError(code=1, message="domain store: association not found")))
        var row = Association(id=NativeDomainStore._text(stmt, 0), run_id=NativeDomainStore._text(stmt, 1), kind=NativeDomainStore._text(stmt, 2), impulse_id=NativeDomainStore._text(stmt, 3), values=NativeDomainStore._text(stmt, 4), metadata=NativeDomainStore._text(stmt, 5), created_at=NativeDomainStore._text(stmt, 6))
        stmt.close()
        return row^

    @staticmethod
    def _read_reaction(mut stmt: Statement) raises -> Reaction:
        if not stmt.step():
            stmt.close()
            raise Error(String(SQLiteError(code=1, message="domain store: reaction not found")))
        var row = Reaction(id=NativeDomainStore._text(stmt, 0), run_id=NativeDomainStore._text(stmt, 1), kind=NativeDomainStore._text(stmt, 2), uri=NativeDomainStore._text(stmt, 3), impulse_id=NativeDomainStore._text(stmt, 4), media_type=NativeDomainStore._text(stmt, 5), size_bytes=(-1 if stmt.column_null(6) else stmt.column_int(6)), content_hash=NativeDomainStore._text(stmt, 7), metadata=NativeDomainStore._text(stmt, 8), created_at=NativeDomainStore._text(stmt, 9))
        stmt.close()
        return row^

    @staticmethod
    def _read_homeostat(mut stmt: Statement) raises -> Homeostat:
        if not stmt.step():
            stmt.close()
            raise Error(String(SQLiteError(code=1, message="domain store: homeostat not found")))
        var row = Homeostat(id=NativeDomainStore._text(stmt, 0), run_id=NativeDomainStore._text(stmt, 1), kind=NativeDomainStore._text(stmt, 2), impulse_id=NativeDomainStore._text(stmt, 3), status=NativeDomainStore._text(stmt, 4), values=NativeDomainStore._text(stmt, 5), metadata=NativeDomainStore._text(stmt, 6), attempt=stmt.column_int(7), max_attempts=stmt.column_int(8), created_at=NativeDomainStore._text(stmt, 9), updated_at=NativeDomainStore._text(stmt, 10))
        stmt.close()
        return row^

    @staticmethod
    def _read_projection(mut stmt: Statement) raises -> Projection:
        if not stmt.step():
            stmt.close()
            raise Error(String(SQLiteError(code=1, message="domain store: projection not found")))
        var stale = False
        if stmt.column_int(7) != 0: stale = True
        var row = Projection(id=NativeDomainStore._text(stmt, 0), run_id=NativeDomainStore._text(stmt, 1), name=NativeDomainStore._text(stmt, 2), version=stmt.column_int(3), data=NativeDomainStore._text(stmt, 4), source_event_sequence=stmt.column_int(5), updated_at=NativeDomainStore._text(stmt, 6), stale=stale)
        stmt.close()
        return row^

    def get_impulse(mut self, run_id: String, impulse_id: String) raises -> Impulse:
        self._require_run(run_id)
        var stmt = self.db.query("SELECT id,run_id,impulse_type,payload,metadata,created_at,updated_at FROM impulses WHERE run_id=? AND id=?")
        stmt.bind_text(1, run_id); stmt.bind_text(2, impulse_id)
        return self._read_impulse(stmt)

    def list_impulse_records(mut self, run_id: String, impulse_type: String = "", limit: Int = -1) raises -> List[Impulse]:
        if limit < -1: raise Error(String(SQLiteError(code=1, message="domain store: limit must be non-negative")))
        self._require_run(run_id)
        var sql = "SELECT id,run_id,impulse_type,payload,metadata,created_at,updated_at FROM impulses WHERE run_id=?"
        if impulse_type != "": sql += " AND impulse_type=?"
        sql += " ORDER BY created_at ASC, id ASC"
        if limit >= 0: sql += " LIMIT ?"
        var stmt = self.db.query(sql); stmt.bind_text(1, run_id)
        var index = 2
        if impulse_type != "": stmt.bind_text(index, impulse_type); index += 1
        if limit >= 0: stmt.bind_int(index, limit)
        var result = List[Impulse]()
        while stmt.step(): result.append(Impulse(id=self._text(stmt, 0), run_id=self._text(stmt, 1), impulse_type=self._text(stmt, 2), payload=self._text(stmt, 3), metadata=self._text(stmt, 4), created_at=self._text(stmt, 5), updated_at=self._text(stmt, 6))^)
        stmt.close()
        return result^

    def get_impulse_type(mut self, run_id: String, impulse_type_id: String) raises -> ImpulseType:
        self._require_run(run_id)
        var stmt = self.db.query("SELECT id,run_id,title,description,media_types,value_schema_json,metadata,created_at,updated_at FROM impulse_types WHERE run_id=? AND id=?")
        stmt.bind_text(1, run_id); stmt.bind_text(2, impulse_type_id)
        return self._read_impulse_type(stmt)

    def list_impulse_type_records(mut self, run_id: String, limit: Int = -1) raises -> List[ImpulseType]:
        if limit < -1: raise Error(String(SQLiteError(code=1, message="domain store: limit must be non-negative")))
        self._require_run(run_id)
        var sql = "SELECT id,run_id,title,description,media_types,value_schema_json,metadata,created_at,updated_at FROM impulse_types WHERE run_id=? ORDER BY id ASC"
        if limit >= 0: sql += " LIMIT ?"
        var stmt = self.db.query(sql); stmt.bind_text(1, run_id)
        if limit >= 0: stmt.bind_int(2, limit)
        var result = List[ImpulseType]()
        while stmt.step(): result.append(ImpulseType(id=self._text(stmt, 0), run_id=self._text(stmt, 1), title=self._text(stmt, 2), description=self._text(stmt, 3), media_types=self._text(stmt, 4), value_schema=self._text(stmt, 5), metadata=self._text(stmt, 6), created_at=self._text(stmt, 7), updated_at=self._text(stmt, 8))^)
        stmt.close()
        return result^

    def get_impulse_relation(mut self, run_id: String, relation_id: String) raises -> ImpulseRelation:
        self._require_run(run_id)
        var stmt = self.db.query("SELECT id,run_id,relation_type,source_impulse_id,target_impulse_id,metadata,created_at FROM impulse_relations WHERE run_id=? AND id=?")
        stmt.bind_text(1, run_id); stmt.bind_text(2, relation_id)
        return self._read_impulse_relation(stmt)

    def list_impulse_relation_records(mut self, run_id: String, impulse_id: String = "", relation_type: String = "", limit: Int = -1) raises -> List[ImpulseRelation]:
        if limit < -1: raise Error(String(SQLiteError(code=1, message="domain store: limit must be non-negative")))
        var sql = "SELECT id,run_id,relation_type,source_impulse_id,target_impulse_id,metadata,created_at FROM impulse_relations WHERE run_id=?"
        if impulse_id != "": sql += " AND (source_impulse_id=? OR target_impulse_id=?)"
        self._require_run(run_id)
        if relation_type != "": sql += " AND relation_type=?"
        sql += " ORDER BY created_at ASC, id ASC"
        if limit >= 0: sql += " LIMIT ?"
        var stmt = self.db.query(sql); stmt.bind_text(1, run_id)
        var index = 2
        if impulse_id != "": stmt.bind_text(index, impulse_id); stmt.bind_text(index + 1, impulse_id); index += 2
        if relation_type != "": stmt.bind_text(index, relation_type); index += 1
        if limit >= 0: stmt.bind_int(index, limit)
        var result = List[ImpulseRelation]()
        while stmt.step(): result.append(ImpulseRelation(id=self._text(stmt, 0), run_id=self._text(stmt, 1), relation_type=self._text(stmt, 2), source_impulse_id=self._text(stmt, 3), target_impulse_id=self._text(stmt, 4), metadata=self._text(stmt, 5), created_at=self._text(stmt, 6))^)
        stmt.close()
        return result^

    def get_association(mut self, run_id: String, association_id: String) raises -> Association:
        self._require_run(run_id)
        var stmt = self.db.query("SELECT id,run_id,kind,impulse_id,values_json,metadata,created_at FROM associations WHERE run_id=? AND id=?")
        stmt.bind_text(1, run_id); stmt.bind_text(2, association_id)
        return self._read_association(stmt)

    def list_association_records(mut self, run_id: String, impulse_id: String = "", kind: String = "", limit: Int = -1) raises -> List[Association]:
        self._require_run(run_id)
        if limit < -1: raise Error(String(SQLiteError(code=1, message="domain store: limit must be non-negative")))
        var sql = "SELECT id,run_id,kind,impulse_id,values_json,metadata,created_at FROM associations WHERE run_id=?"
        if impulse_id != "": sql += " AND impulse_id=?"
        if kind != "": sql += " AND kind=?"
        sql += " ORDER BY created_at ASC, id ASC"
        if limit >= 0: sql += " LIMIT ?"
        var stmt = self.db.query(sql); stmt.bind_text(1, run_id)
        var index = 2
        if impulse_id != "": stmt.bind_text(index, impulse_id); index += 1
        if kind != "": stmt.bind_text(index, kind); index += 1
        if limit >= 0: stmt.bind_int(index, limit)
        var result = List[Association]()
        while stmt.step(): result.append(Association(id=self._text(stmt, 0), run_id=self._text(stmt, 1), kind=self._text(stmt, 2), impulse_id=self._text(stmt, 3), values=self._text(stmt, 4), metadata=self._text(stmt, 5), created_at=self._text(stmt, 6))^)
        stmt.close()
        return result^

    def get_reaction(mut self, run_id: String, reaction_id: String) raises -> Reaction:
        self._require_run(run_id)
        var stmt = self.db.query("SELECT id,run_id,kind,uri,impulse_id,media_type,size_bytes,content_hash,metadata,created_at FROM reactions WHERE run_id=? AND id=?")
        stmt.bind_text(1, run_id); stmt.bind_text(2, reaction_id)
        return self._read_reaction(stmt)

    def list_reaction_records(mut self, run_id: String, impulse_id: String = "", kind: String = "", limit: Int = -1) raises -> List[Reaction]:
        self._require_run(run_id)
        if limit < -1: raise Error(String(SQLiteError(code=1, message="domain store: limit must be non-negative")))
        var sql = "SELECT id,run_id,kind,uri,impulse_id,media_type,size_bytes,content_hash,metadata,created_at FROM reactions WHERE run_id=?"
        if impulse_id != "": sql += " AND impulse_id=?"
        if kind != "": sql += " AND kind=?"
        sql += " ORDER BY created_at ASC, id ASC"
        if limit >= 0: sql += " LIMIT ?"
        var stmt = self.db.query(sql); stmt.bind_text(1, run_id)
        var index = 2
        if impulse_id != "": stmt.bind_text(index, impulse_id); index += 1
        if kind != "": stmt.bind_text(index, kind); index += 1
        if limit >= 0: stmt.bind_int(index, limit)
        var result = List[Reaction]()
        while stmt.step(): result.append(Reaction(id=self._text(stmt, 0), run_id=self._text(stmt, 1), kind=self._text(stmt, 2), uri=self._text(stmt, 3), impulse_id=self._text(stmt, 4), media_type=self._text(stmt, 5), size_bytes=(-1 if stmt.column_null(6) else stmt.column_int(6)), content_hash=self._text(stmt, 7), metadata=self._text(stmt, 8), created_at=self._text(stmt, 9))^)
        stmt.close()
        return result^
    def referenced_reaction_digests(mut self) raises -> List[String]:
        """Return every normalized valid CAS digest referenced by reaction rows."""
        var result = List[String]()
        var stmt = self.db.query("SELECT content_hash,uri FROM reactions ORDER BY id ASC")
        while stmt.step():
            var candidates = List[String]()
            if not stmt.column_null(0):
                var candidate = self._text(stmt, 0)
                if candidate.startswith("sha256:") and candidate.byte_length() == 71:
                    var hash_uri = "fala-reaction://sha256/"
                    for index in range(7, candidate.byte_length()): hash_uri += String(candidate[byte=index])
                    var hash_digest = reaction_digest_or_empty(hash_uri)
                    if hash_digest != "": candidates.append(hash_digest)
            var uri_digest = reaction_digest_or_empty(self._text(stmt, 1))
            if uri_digest != "": candidates.append(uri_digest)
            for digest in candidates:
                var duplicate = False
                for prior in result:
                    if prior == digest:
                        duplicate = True
                        break
                if not duplicate: result.append(digest)
        stmt.close()
        return result^


    def get_homeostat(mut self, run_id: String, homeostat_id: String) raises -> Homeostat:
        self._require_run(run_id)
        var stmt = self.db.query("SELECT id,run_id,kind,impulse_id,status,values_json,metadata,attempt,max_attempts,created_at,updated_at FROM homeostats WHERE run_id=? AND id=?")
        stmt.bind_text(1, run_id); stmt.bind_text(2, homeostat_id)
        return self._read_homeostat(stmt)

    def list_homeostat_records(mut self, run_id: String, impulse_id: String = "", status: String = "", limit: Int = -1) raises -> List[Homeostat]:
        self._require_run(run_id)
        if limit < -1: raise Error(String(SQLiteError(code=1, message="domain store: limit must be non-negative")))
        var sql = "SELECT id,run_id,kind,impulse_id,status,values_json,metadata,attempt,max_attempts,created_at,updated_at FROM homeostats WHERE run_id=?"
        if impulse_id != "": sql += " AND impulse_id=?"
        if status != "": sql += " AND status=?"
        sql += " ORDER BY updated_at ASC, id ASC"
        if limit >= 0: sql += " LIMIT ?"
        var stmt = self.db.query(sql); stmt.bind_text(1, run_id)
        var index = 2
        if impulse_id != "": stmt.bind_text(index, impulse_id); index += 1
        if status != "": stmt.bind_text(index, status); index += 1
        if limit >= 0: stmt.bind_int(index, limit)
        var result = List[Homeostat]()
        while stmt.step(): result.append(Homeostat(id=self._text(stmt, 0), run_id=self._text(stmt, 1), kind=self._text(stmt, 2), impulse_id=self._text(stmt, 3), status=self._text(stmt, 4), values=self._text(stmt, 5), metadata=self._text(stmt, 6), attempt=stmt.column_int(7), max_attempts=stmt.column_int(8), created_at=self._text(stmt, 9), updated_at=self._text(stmt, 10))^)
        stmt.close()
        return result^

    def get_projection(mut self, run_id: String, name: String) raises -> Projection:
        self._require_run(run_id)
        var stmt = self.db.query("SELECT p.id,p.run_id,p.name,p.version,p.data,p.source_event_sequence,p.updated_at,CASE WHEN EXISTS (SELECT 1 FROM runtime_events e WHERE e.run_id=p.run_id AND e.sequence>p.source_event_sequence) THEN 1 ELSE 0 END FROM projections p WHERE p.run_id=? AND p.name=?")
        stmt.bind_text(1, run_id); stmt.bind_text(2, name)
        if not stmt.step():
            stmt.close()
            raise Error(String(SQLiteError(code=1, message="domain store: projection not found")))
        var stale = False
        if stmt.column_int(7) != 0: stale = True
        var row = Projection(id=self._text(stmt, 0), run_id=self._text(stmt, 1), name=self._text(stmt, 2), version=stmt.column_int(3), data=self._text(stmt, 4), source_event_sequence=stmt.column_int(5), updated_at=self._text(stmt, 6), stale=stale)
        stmt.close()
        return row^

    def list_projection_records(mut self, run_id: String, name: String = "", limit: Int = -1) raises -> List[Projection]:
        self._require_run(run_id)
        if limit < -1: raise Error(String(SQLiteError(code=1, message="domain store: limit must be non-negative")))
        var sql = "SELECT p.id,p.run_id,p.name,p.version,p.data,p.source_event_sequence,p.updated_at,CASE WHEN EXISTS (SELECT 1 FROM runtime_events e WHERE e.run_id=p.run_id AND e.sequence>p.source_event_sequence) THEN 1 ELSE 0 END FROM projections p WHERE p.run_id=?"
        if name != "": sql += " AND p.name=?"
        sql += " ORDER BY p.name ASC"
        if limit >= 0: sql += " LIMIT ?"
        var stmt = self.db.query(sql); stmt.bind_text(1, run_id)
        var index = 2
        if name != "": stmt.bind_text(index, name); index += 1
        if limit >= 0: stmt.bind_int(index, limit)
        var result = List[Projection]()
        while stmt.step():
            var stale = False
            if stmt.column_int(7) != 0: stale = True
            result.append(Projection(id=self._text(stmt, 0), run_id=self._text(stmt, 1), name=self._text(stmt, 2), version=stmt.column_int(3), data=self._text(stmt, 4), source_event_sequence=stmt.column_int(5), updated_at=self._text(stmt, 6), stale=stale)^)
        stmt.close()
        return result^


