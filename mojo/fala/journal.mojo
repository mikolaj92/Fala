"""Native SQLite journal for the Fala runtime.

The journal deliberately deals in typed rows and JSON text.  Callers own the
serialization of payloads; this module owns transaction boundaries,
idempotency, event ordering, and process leases.
"""
from std.collections import List
from fala.sqlite import Connection, Statement, SQLiteError
from emberjson import Value, Object, to_string
from fala.json import canonical_json_text, json_values_equal
from fala.reactions import content_address_json
from fala.schema import initialize_native_schema

from fala.status import ProcessStatus, RunStatus, can_transition_process, can_transition_run, can_replay_terminal_process



def _schema_number(value: Value) -> Float64:
    if value.is_float(): return value.float()
    if value.is_int(): return Float64(value.int())
    if value.is_uint(): return Float64(value.uint())
    return 0.0

def _schema_kind_matches(value: Value, kind: String) -> Bool:
    if kind == "object": return value.is_object()
    if kind == "array": return value.is_array()
    if kind == "string": return value.is_string()
    if kind == "boolean": return value.is_bool()
    if kind == "number": return value.is_int() or value.is_uint() or value.is_float()
    if kind == "integer":
        if value.is_int() or value.is_uint(): return True
        if value.is_float():
            var numeric = value.float()
            if numeric != numeric or numeric < -9223372036854775808.0 or numeric >= 9223372036854775808.0:
                return False
            return Float64(Int(numeric)) == numeric
        return False
    if kind == "null": return value.is_null()
    return False

def _schema_type_matches(value: Value, schema: Value) raises -> Bool:
    if not schema.is_object() or "type" not in schema.object(): return True
    var type_value = schema.object()["type"].copy()
    if type_value.is_string(): return _schema_kind_matches(value, type_value.string())
    if type_value.is_array():
        var matched = False
        for member in type_value.array():
            if not member.is_string(): return False
            if _schema_kind_matches(value, member.string()): matched = True
        return matched
    return False

def _validate_output_schema_value(value: Value, schema: Value, path: String) raises:
    if not schema.is_object(): raise Error("expected schema object")
    var schema_object = schema.object().copy()
    if "const" in schema_object:
        var expected_const = schema_object["const"].copy()
        if not json_values_equal(value, expected_const^):
            raise Error("output does not match schema const at " + path)
    if "enum" in schema_object:
        var values = schema_object["enum"].copy()
        if values.is_array():
            var found = False
            for candidate in values.array():
                if json_values_equal(value, candidate): found = True
            if not found: raise Error("output does not match schema enum at " + path)
    if not _schema_type_matches(value, schema): raise Error("output does not match schema type at " + path)
    var numeric = value.is_int() or value.is_uint() or value.is_float()
    if numeric:
        var number = _schema_number(value)
        if "minimum" in schema_object:
            var minimum = schema_object["minimum"].copy()
            if (minimum.is_int() or minimum.is_uint() or minimum.is_float()) and number < _schema_number(minimum): raise Error("output is below schema minimum at " + path)
        if "maximum" in schema_object:
            var maximum = schema_object["maximum"].copy()
            if (maximum.is_int() or maximum.is_uint() or maximum.is_float()) and number > _schema_number(maximum): raise Error("output exceeds schema maximum at " + path)
    if value.is_string():
        var string_length = 0
        for _ in value.string().codepoint_slices(): string_length += 1
        if "minLength" in schema_object:
            var minimum = schema_object["minLength"].copy()
            if (minimum.is_int() or minimum.is_uint()) and string_length < Int(_schema_number(minimum)): raise Error("output is shorter than schema minLength at " + path)
        if "maxLength" in schema_object:
            var maximum = schema_object["maxLength"].copy()
            if (maximum.is_int() or maximum.is_uint()) and string_length > Int(_schema_number(maximum)): raise Error("output exceeds schema maxLength at " + path)
    if value.is_array() and "items" in schema_object:
        var item_schema = schema_object["items"].copy()
        for index in range(len(value.array())): _validate_output_schema_value(value.array()[index], item_schema, path + "/" + String(index))
    if value.is_object() and "required" in schema_object:
        var required = schema_object["required"].copy()
        if required.is_array():
            for key in required.array():
                if key.is_string() and key.string() not in value.object(): raise Error("missing required output field " + key.string() + " at " + path)
    if value.is_object() and "additionalProperties" in schema_object and schema_object["additionalProperties"].is_bool() and not schema_object["additionalProperties"].bool():
        var properties = Object(capacity=0)
        if "properties" in schema_object and schema_object["properties"].is_object(): properties = schema_object["properties"].object().copy()
        for pair in value.object().items():
            if pair.key not in properties: raise Error("output contains additional property " + pair.key + " at " + path)
    if value.is_object() and "properties" in schema_object and schema_object["properties"].is_object():
        for pair in schema_object["properties"].object().items():
            if pair.key in value.object(): _validate_output_schema_value(value.object()[pair.key], pair.value, path + "/" + pair.key)


@fieldwise_init
struct RunRow(Copyable, Movable):
    var id: String
    var status: String
    var title: String
    var metadata: String
    var created_at: String
    var updated_at: String
@fieldwise_init
struct RunTransitionResult(Copyable, Movable):
    var run: RunRow
    var replayed: Bool


@fieldwise_init
struct RunRecord(Copyable, Movable):
    """Complete schema-backed run projection; empty strings represent SQL NULL."""
    var id: String
    var status: String
    var title: String
    var package_id: String
    var package_version: String
    var package_digest: String
    var correlation_path_id: String
    var correlation_path_digest: String
    var runtime_version: String
    var backend_version: String
    var schema_version: Int
    var metadata: String
    var created_at: String
    var updated_at: String
    var started_at: String
    var finished_at: String

@fieldwise_init
struct CommandRow(Copyable, Movable):
    var run_id: String
    var id: String
    var command_type: String
    var idempotency_key: String
    var actor: String
    var correlation_id: String
    var causation_id: String
    var payload: String
    var created_at: String


@fieldwise_init
struct CommandResult(Copyable, Movable):
    var command: CommandRow
    var replayed: Bool

@fieldwise_init
struct EventInput(Copyable, Movable):
    """Typed event fields accepted by submit_command; empty optional values inherit command metadata."""
    var id: String
    var event_type: String
    var payload: String
    var created_at: String
    var impulse_id: String
    var process_id: String
    var schema_version: Int
    var actor: String
    var correlation_id: String
    var causation_id: String

@fieldwise_init
struct CorrelationChildTransition(Copyable, Movable):
    """Deterministic child mutation applied with one correlation transaction."""
    var process_id: String
    var target_status: String
    var input_json: String
    var error_json: String

@fieldwise_init
struct EventRow(Copyable, Movable):
    var run_id: String
    var sequence: Int
    var id: String
    var event_type: String
    var payload: String
    var created_at: String
    var impulse_id: String
    var process_id: String
    var command_id: String
    var schema_version: Int
    var actor: String
    var correlation_id: String
    var causation_id: String
@fieldwise_init
struct CommandSubmission(Copyable, Movable):
    var command: CommandRow
    var events: List[EventRow]
    var replayed: Bool
@fieldwise_init
struct ProcessTransitionResult(Copyable, Movable):
    var process: ProcessRow
    var submission: CommandSubmission


@fieldwise_init
struct ProcessRow(Copyable, Movable):
    var run_id: String
    var id: String
    var process_type: String
    var impulse_id: String
    var status: String
    var priority: Int
    var attempt: Int
    var max_attempts: Int
    var available_at: String
    var lease_owner: String
    var lease_expires_at: String
    var input_json: String
    var output_json: String
    var error_json: String
    var metadata: String
    var created_at: String
    var updated_at: String
    var started_at: String
    var finished_at: String
    var output_schema_json: String


struct NativeJournal(Movable):
    var db: Connection

    def __init__(out self, path: String) raises SQLiteError:
        self.db = Connection(path)
    def __del__(deinit self):
        try:
            self.db.close()
        except e:
            pass

    @staticmethod
    def open(path: String) raises SQLiteError -> NativeJournal:
        return NativeJournal(path)

    def initialize(mut self) raises SQLiteError:
        initialize_native_schema(self.db)
    def close(mut self) raises SQLiteError:
        self.db.close()

    @staticmethod
    def _text(mut stmt: Statement, index: Int) raises SQLiteError -> String:
        if stmt.column_null(index):
            return String("")
        return stmt.column_text(index)
    def _json_quote(mut self, value: String) -> String:
        var result = String("\"")
        for i in range(value.byte_length()):
            var ch = value[byte=i]
            if ch == '\\': result += "\\\\"
            elif ch == '"': result += "\\\""
            elif ch == '\n': result += "\\n"
            elif ch == '\r': result += "\\r"
            elif ch == '\t': result += "\\t"
            else: result += ch
        return result + "\""
    def _require_run(mut self, run_id: String) raises SQLiteError:
        if run_id == "":
            raise SQLiteError(code=1, message="journal: run_id must not be empty")
        var stmt = self.db.query("SELECT 1 FROM runs WHERE id=?")
        stmt.bind_text(1, run_id)
        if not stmt.step():
            raise SQLiteError(code=1, message="journal: unknown run")
    def _process_id_from_payload(mut self, payload: String, fallback: String) -> String:
        try:
            var parsed = Value(parse_string=payload)
            if parsed.is_object() and "process_id" in parsed.object():
                var value = parsed.object()["process_id"].copy()
                if value.is_string() and value.string() != "":
                    return value.string()
        except err:
            pass
        return fallback
    def _process_attempt_from_payload(mut self, payload: String, fallback: Int) -> Int:
        try:
            var parsed = Value(parse_string=payload)
            if parsed.is_object() and "attempt" in parsed.object():
                var value = parsed.object()["attempt"].copy()
                if value.is_int(): return Int(value.int())
                if value.is_uint(): return Int(value.uint())
        except err:
            pass
        return fallback
    def _process_lease_from_payload(mut self, payload: String, fallback: String) -> String:
        try:
            var parsed = Value(parse_string=payload)
            if parsed.is_object() and "lease_expires_at" in parsed.object():
                var value = parsed.object()["lease_expires_at"].copy()
                if value.is_string(): return value.string()
        except err:
            pass
        return fallback
    def _json_field_from_payload(mut self, payload: String, key: String, fallback: String) -> String:
        try:
            var parsed = Value(parse_string=payload)
            if parsed.is_object() and key in parsed.object():
                var value = parsed.object()[key].copy()
                return canonical_json_text(to_string(value^))
        except err:
            pass
        return fallback
    def _content_digest(mut self, json_text: String) raises SQLiteError -> String:
        try:
            return content_address_json(json_text)
        except err:
            raise SQLiteError(code=1, message="journal: unable to digest JSON payload")
    def _canonical_json_field(mut self, value: String, field: String) raises SQLiteError -> String:
        try:
            return canonical_json_text(value)
        except err:
            raise SQLiteError(code=1, message="journal: invalid " + field + " JSON")


    def transition_run_status(
        mut self,
        run_id: String,
        target: String,
        at: String,
        idempotency_key: String,
        reason: String = "",
    ) raises SQLiteError -> RunTransitionResult:
        """Persist one run status transition with an idempotent command/event."""
        self._require_run(run_id)
        if target == "" or at == "" or idempotency_key == "":
            raise SQLiteError(code=1, message="journal: run transition requires target, timestamp, and idempotency key")
        var current_stmt = self.db.query("SELECT id,status,title,metadata,created_at,updated_at FROM runs WHERE id=?")
        current_stmt.bind_text(1, run_id)
        if not current_stmt.step(): raise SQLiteError(code=1, message="journal: unknown run")
        var current = RunRow(id=self._text(current_stmt,0), status=self._text(current_stmt,1), title=self._text(current_stmt,2), metadata=self._text(current_stmt,3), created_at=self._text(current_stmt,4), updated_at=self._text(current_stmt,5))
        var durable_reason = reason if reason != "" else target
        var payload = "{\"target\":" + self._json_quote(target) + ",\"reason\":" + self._json_quote(durable_reason) + "}"
        var existing = self.db.query("SELECT command_type,payload,created_at FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
        existing.bind_text(1, run_id); existing.bind_text(2, idempotency_key)
        if existing.step():
            if self._text(existing,0) != "run." + target or self._text(existing,1) != payload or self._text(existing,2) != at:
                raise SQLiteError(code=1, message="journal: run transition idempotency conflict")
            return RunTransitionResult(run=current^, replayed=True)
        var from_status = RunStatus(current.status)
        var to_status = RunStatus(target)
        if not from_status.is_known() or not to_status.is_known() or not can_transition_run(from_status, to_status):
            raise SQLiteError(code=1, message="journal: illegal run transition " + current.status + " -> " + target)
        self.db.begin_immediate()
        try:
            var command = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,payload,created_at) VALUES (?,?,?,?,?,?,?)")
            command.bind_text(1,run_id); command.bind_text(2,idempotency_key); command.bind_text(3,"run." + target); command.bind_text(4,idempotency_key); command.bind_null(5); command.bind_text(6,payload); command.bind_text(7,at); _ = command.step()
            var update_sql = "UPDATE runs SET status=?,updated_at=?"
            if target == "active": update_sql += ",started_at=COALESCE(started_at,?)"
            elif to_status.is_terminal(): update_sql += ",finished_at=?"
            update_sql += " WHERE id=? AND status=?"
            var update = self.db.query(update_sql); update.bind_text(1,target); update.bind_text(2,at)
            var bind_index = 3
            if target == "active" or to_status.is_terminal(): update.bind_text(bind_index,at); bind_index += 1
            update.bind_text(bind_index,run_id); update.bind_text(bind_index+1,current.status); _ = update.step()
            if self.db.changes() != 1: raise SQLiteError(code=1, message="journal: run transition lost ownership")
            var next_stmt = self.db.query("SELECT COALESCE(MAX(sequence),0)+1 FROM runtime_events WHERE run_id=?"); next_stmt.bind_text(1,run_id)
            if not next_stmt.step(): raise SQLiteError(code=1, message="journal: unable to allocate run event sequence")
            var event = self.db.query("INSERT INTO runtime_events (run_id,sequence,id,event_type,schema_version,command_id,payload,created_at) VALUES (?,?,?,?,1,?,?,?)")
            event.bind_text(1,run_id); event.bind_int(2,next_stmt.column_int(0)); event.bind_text(3,idempotency_key + ":event"); event.bind_text(4,"run." + target); event.bind_text(5,idempotency_key); event.bind_text(6,payload); event.bind_text(7,at); _ = event.step()
            self.db.commit()
        except err:
            self.db.rollback()
            raise SQLiteError(code=1, message="journal: run transition failed")
        var final_stmt = self.db.query("SELECT id,status,title,metadata,created_at,updated_at FROM runs WHERE id=?"); final_stmt.bind_text(1,run_id)
        if not final_stmt.step(): raise SQLiteError(code=1, message="journal: transitioned run is missing")
        var result = RunRow(id=self._text(final_stmt,0), status=self._text(final_stmt,1), title=self._text(final_stmt,2), metadata=self._text(final_stmt,3), created_at=self._text(final_stmt,4), updated_at=self._text(final_stmt,5))
        return RunTransitionResult(run=result^, replayed=False)

    def _read_event(mut self, mut stmt: Statement) raises SQLiteError -> EventRow:
        if not stmt.step():
            raise SQLiteError(code=1, message="journal: event row not found")
        return EventRow(
            run_id=self._text(stmt, 0), sequence=stmt.column_int(1),
            id=self._text(stmt, 2), event_type=self._text(stmt, 3),
            schema_version=stmt.column_int(4), impulse_id=self._text(stmt, 5),
            process_id=self._text(stmt, 6), command_id=self._text(stmt, 7),
            actor=self._text(stmt, 8), correlation_id=self._text(stmt, 9),
            causation_id=self._text(stmt, 10), payload=self._text(stmt, 11),
            created_at=self._text(stmt, 12),
        )

    def _append_event_in_tx(
        mut self, run_id: String, event_id: String, event_type: String,
        payload: String, created_at: String, impulse_id: String = "",
        process_id: String = "", command_id: String = "", schema_version: Int = 1,
        actor: String = "", correlation_id: String = "", causation_id: String = "",
        allow_existing: Bool = True,
    ) raises SQLiteError -> EventRow:
        if event_id == "" or event_type == "" or created_at == "":
            raise SQLiteError(code=1, message="journal: event id, type, and created_at must not be empty")
        if schema_version < 1:
            raise SQLiteError(code=1, message="journal: schema_version must be positive")
        if event_id == "" or event_type == "":
            raise SQLiteError(code=1, message="journal: event id and type must not be empty")
        if schema_version < 1:
            raise SQLiteError(code=1, message="journal: schema_version must be positive")
        var existing = self.db.query("SELECT run_id,sequence,id,event_type,schema_version,impulse_id,process_id,command_id,actor,correlation_id,causation_id,payload,created_at FROM runtime_events WHERE run_id=? AND id=?")
        existing.bind_text(1, run_id); existing.bind_text(2, event_id)
        if existing.step():
            if self._text(existing,3) != event_type or self._text(existing,5) != impulse_id or self._text(existing,6) != process_id or self._text(existing,7) != command_id or self._text(existing,8) != actor or self._text(existing,9) != correlation_id or self._text(existing,10) != causation_id or self._text(existing,11) != payload or self._text(existing,12) != created_at or existing.column_int(4) != schema_version:
                raise SQLiteError(code=1, message="journal: event id already exists with different contents")
            return EventRow(run_id=self._text(existing,0), sequence=existing.column_int(1), id=self._text(existing,2), event_type=self._text(existing,3), schema_version=existing.column_int(4), impulse_id=self._text(existing,5), process_id=self._text(existing,6), command_id=self._text(existing,7), actor=self._text(existing,8), correlation_id=self._text(existing,9), causation_id=self._text(existing,10), payload=self._text(existing,11), created_at=self._text(existing,12))
        var next_stmt = self.db.query("SELECT COALESCE(MAX(sequence),0)+1 FROM runtime_events WHERE run_id=?")
        next_stmt.bind_text(1,run_id)
        if not next_stmt.step():
            raise SQLiteError(code=1, message="journal: unable to allocate event sequence")
        var sequence = next_stmt.column_int(0)
        var insert = self.db.query("INSERT INTO runtime_events (run_id,sequence,id,event_type,schema_version,impulse_id,process_id,command_id,actor,correlation_id,causation_id,payload,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)")
        insert.bind_text(1,run_id); insert.bind_int(2,sequence); insert.bind_text(3,event_id); insert.bind_text(4,event_type); insert.bind_int(5,schema_version)
        if impulse_id == "": insert.bind_null(6)
        else: insert.bind_text(6,impulse_id)
        if process_id == "": insert.bind_null(7)
        else: insert.bind_text(7,process_id)
        if command_id == "": insert.bind_null(8)
        else: insert.bind_text(8,command_id)
        if actor == "": insert.bind_null(9)
        else: insert.bind_text(9,actor)
        if correlation_id == "": insert.bind_null(10)
        else: insert.bind_text(10,correlation_id)
        if causation_id == "": insert.bind_null(11)
        else: insert.bind_text(11,causation_id)
        insert.bind_text(12,payload); insert.bind_text(13,created_at)
        _ = insert.step()
        var read = self.db.query("SELECT run_id,sequence,id,event_type,schema_version,impulse_id,process_id,command_id,actor,correlation_id,causation_id,payload,created_at FROM runtime_events WHERE run_id=? AND id=?")
        read.bind_text(1,run_id); read.bind_text(2,event_id)
        return self._read_event(read)
    def _read_command(mut self, mut stmt: Statement) raises SQLiteError -> CommandRow:
        if not stmt.step():
            raise SQLiteError(code=1, message="journal: command row not found")
        return CommandRow(
            run_id=self._text(stmt, 0), id=self._text(stmt, 1),
            command_type=self._text(stmt, 2), idempotency_key=self._text(stmt, 3),
            actor=self._text(stmt, 4), correlation_id=self._text(stmt, 5),
            causation_id=self._text(stmt, 6), payload=self._text(stmt, 7),
            created_at=self._text(stmt, 8),
        )

    def _read_run_record(mut self, mut stmt: Statement) raises SQLiteError -> RunRecord:
        if not stmt.step():
            raise SQLiteError(code=1, message="journal: run row not found")
        return RunRecord(
            id=self._text(stmt, 0), status=self._text(stmt, 1), title=self._text(stmt, 2),
            package_id=self._text(stmt, 3), package_version=self._text(stmt, 4),
            package_digest=self._text(stmt, 5), correlation_path_id=self._text(stmt, 6),
            correlation_path_digest=self._text(stmt, 7), runtime_version=self._text(stmt, 8),
            backend_version=self._text(stmt, 9), schema_version=stmt.column_int(10),
            metadata=self._text(stmt, 11), created_at=self._text(stmt, 12),
            updated_at=self._text(stmt, 13), started_at=self._text(stmt, 14),
            finished_at=self._text(stmt, 15),
        )

    def get_run_record(mut self, run_id: String) raises SQLiteError -> RunRecord:
        var stmt = self.db.query("SELECT id,status,title,package_id,package_version,package_digest,correlation_path_id,correlation_path_digest,runtime_version,backend_version,schema_version,metadata,created_at,updated_at,started_at,finished_at FROM runs WHERE id=?")
        stmt.bind_text(1, run_id)
        return self._read_run_record(stmt)

    def _read_process(mut self, mut stmt: Statement) raises SQLiteError -> ProcessRow:
        if not stmt.step():
            raise SQLiteError(code=1, message="journal: process row not found")
        return ProcessRow(
            run_id=self._text(stmt, 0), id=self._text(stmt, 1),
            process_type=self._text(stmt, 2), impulse_id=self._text(stmt, 3),
            status=self._text(stmt, 4), priority=stmt.column_int(5),
            attempt=stmt.column_int(6), max_attempts=stmt.column_int(7),
            available_at=self._text(stmt, 8), lease_owner=self._text(stmt, 9),
            lease_expires_at=self._text(stmt, 10), input_json=self._text(stmt, 11),
            output_json=self._text(stmt, 12), error_json=self._text(stmt, 13),
            metadata=self._text(stmt, 14), created_at=self._text(stmt, 15),
            updated_at=self._text(stmt, 16), started_at=self._text(stmt, 17),
            finished_at=self._text(stmt, 18), output_schema_json=self._text(stmt, 19),
        )

    def create_run(
        mut self,
        run_id: String,
        status: String,
        metadata: String,
        created_at: String,
        title: String = "",
        updated_at: String = "",
        idempotency_key: String = "",
    ) raises SQLiteError -> RunRow:
        if run_id == "" or status == "":
            raise SQLiteError(code=1, message="journal: run id and status must not be empty")
        if created_at == "":
            raise SQLiteError(code=1, message="journal: run creation timestamp must not be empty")
        var update = updated_at if updated_at != "" else created_at
        var key = idempotency_key if idempotency_key != "" else "run.create"
        var command_id = "run.create:" + run_id
        var payload = "{\"metadata\":" + self._json_quote(metadata) + ",\"status\":" + self._json_quote(status) + ",\"title\":" + self._json_quote(title) + "}"
        self.db.begin_immediate()
        try:
            var existing = self.db.query("SELECT status,title,metadata,created_at,updated_at FROM runs WHERE id=?")
            existing.bind_text(1, run_id)
            if existing.step():
                if self._text(existing,0) != status or self._text(existing,1) != title or self._text(existing,2) != metadata or self._text(existing,3) != created_at or self._text(existing,4) != update:
                    raise SQLiteError(code=1, message="journal: run already exists with different contents")
                var prior = self.db.query("SELECT id,command_type,idempotency_key,payload,created_at FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
                prior.bind_text(1,run_id); prior.bind_text(2,key)
                if not prior.step() or self._text(prior,0) != command_id or self._text(prior,1) != "run.create" or self._text(prior,2) != key or self._text(prior,3) != payload or self._text(prior,4) != created_at:
                    raise SQLiteError(code=1, message="journal: run creation idempotency conflict")
                var prior_event = self.db.query("SELECT id,event_type,payload,created_at FROM runtime_events WHERE run_id=? AND command_id=?")
                prior_event.bind_text(1,run_id); prior_event.bind_text(2,command_id)
                if not prior_event.step() or self._text(prior_event,0) != command_id + ":event" or self._text(prior_event,1) != "run.created" or self._text(prior_event,2) != payload or self._text(prior_event,3) != created_at:
                    raise SQLiteError(code=1, message="journal: run creation event replay conflict")
                self.db.commit()
            else:
                var insert = self.db.query("INSERT INTO runs (id,status,title,metadata,created_at,updated_at,schema_version) VALUES (?,?,?,?,?,?,6)")
                insert.bind_text(1,run_id); insert.bind_text(2,status); insert.bind_text(3,title); insert.bind_text(4,metadata); insert.bind_text(5,created_at); insert.bind_text(6,update); _ = insert.step()
                var command = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,payload,created_at) VALUES (?,?,? ,?,NULL,?,?)")
                command.bind_text(1,run_id); command.bind_text(2,command_id); command.bind_text(3,"run.create"); command.bind_text(4,key); command.bind_text(5,payload); command.bind_text(6,created_at); _ = command.step()
                var event = self.db.query("INSERT INTO runtime_events (run_id,sequence,id,event_type,schema_version,command_id,payload,created_at) VALUES (?,1,?,'run.created',1,?,?,?)")
                event.bind_text(1,run_id); event.bind_text(2,command_id + ":event"); event.bind_text(3,command_id); event.bind_text(4,payload); event.bind_text(5,created_at); _ = event.step()
                self.db.commit()
        except err:
            self.db.rollback()
            var detail = String(err)
            if detail.find("different contents") >= 0 or detail.find("idempotency conflict") >= 0 or detail.find("event replay conflict") >= 0:
                raise err^
            raise SQLiteError(code=1, message="journal: create_run failed: " + detail)
        var read = self.db.query("SELECT id,status,title,metadata,created_at,updated_at FROM runs WHERE id=?")
        read.bind_text(1,run_id)
        if not read.step(): raise SQLiteError(code=1, message="journal: created run is missing")
        return RunRow(id=self._text(read,0), status=self._text(read,1), title=self._text(read,2), metadata=self._text(read,3), created_at=self._text(read,4), updated_at=self._text(read,5))

    def append_command(
        mut self, run_id: String, command_id: String, command_type: String,
        idempotency_key: String, payload: String, created_at: String,
        actor: String = "", correlation_id: String = "", causation_id: String = "",

    ) raises SQLiteError -> CommandResult:
        self._require_run(run_id)
        if command_id == "" or command_type == "" or idempotency_key == "" or created_at == "":
            raise SQLiteError(code=1, message="journal: command id, type, idempotency_key, and created_at must not be empty")
        self.db.begin_immediate()
        try:
            var insert = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at) VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(run_id,idempotency_key) DO NOTHING")
            insert.bind_text(1,run_id); insert.bind_text(2,command_id); insert.bind_text(3,command_type); insert.bind_text(4,idempotency_key)
            if actor == "": insert.bind_null(5)
            else: insert.bind_text(5,actor)
            if correlation_id == "": insert.bind_null(6)
            else: insert.bind_text(6,correlation_id)
            if causation_id == "": insert.bind_null(7)
            else: insert.bind_text(7,causation_id)
            insert.bind_text(8,payload); insert.bind_text(9,created_at); _ = insert.step()
            var inserted = self.db.changes() == 1
            var read = self.db.query("SELECT run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
            read.bind_text(1,run_id); read.bind_text(2,idempotency_key)
            var command = self._read_command(read)
            if command.command_type != command_type or command.actor != actor or command.correlation_id != correlation_id or command.causation_id != causation_id or command.payload != payload or command.created_at != created_at:
                raise SQLiteError(code=1, message="journal: idempotency key already exists with different contents")
            var replayed = not inserted
            self.db.commit(); return CommandResult(command=command^, replayed=replayed)
        except err:
            self.db.rollback()
            var detail = String(err)
            if detail.find("idempotency key already exists with different contents") >= 0:
                raise err^
            raise SQLiteError(code=1, message="journal: append_command failed")

    def submit_command(
        mut self, run_id: String, command_id: String, command_type: String,
        idempotency_key: String, payload: String, created_at: String,
        events: List[EventInput] = List[EventInput](), actor: String = "",
        correlation_id: String = "", causation_id: String = "",
    ) raises SQLiteError -> CommandSubmission:
        """Atomically persist a command and its event batch with idempotent replay."""
        self.db.begin_immediate()
        try:
            var existing = self.db.query("SELECT run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
            existing.bind_text(1, run_id); existing.bind_text(2, idempotency_key)
            if existing.step():
                var stored = CommandRow(
                    run_id=self._text(existing, 0), id=self._text(existing, 1),
                    command_type=self._text(existing, 2), idempotency_key=self._text(existing, 3),
                    actor=self._text(existing, 4), correlation_id=self._text(existing, 5),
                    causation_id=self._text(existing, 6), payload=self._text(existing, 7),
                    created_at=self._text(existing, 8),
                )
                var replay_events = List[EventRow]()
                self.db.commit()
                return CommandSubmission(command=stored^, replayed=True, events=replay_events^)
            if command_type == "run.create":
                raise SQLiteError(code=1, message="journal: run.create commands must use create_run")
            self._require_run(run_id)
            if command_id == "" or command_type == "" or idempotency_key == "" or created_at == "":
                raise SQLiteError(code=1, message="journal: command id, type, idempotency_key, and created_at must not be empty")
            var existing_id = self.db.query("SELECT id FROM runtime_commands WHERE run_id=? AND id=?")
            existing_id.bind_text(1, run_id); existing_id.bind_text(2, command_id)
            if existing_id.step():
                raise SQLiteError(code=1, message="journal: command id already exists with different idempotency key")
            var insert = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at) VALUES (?,?,?,?,?,?,?,?,?)")
            insert.bind_text(1,run_id); insert.bind_text(2,command_id); insert.bind_text(3,command_type); insert.bind_text(4,idempotency_key)
            if actor == "": insert.bind_null(5)
            else: insert.bind_text(5,actor)
            if correlation_id == "": insert.bind_null(6)
            else: insert.bind_text(6,correlation_id)
            if causation_id == "": insert.bind_null(7)
            else: insert.bind_text(7,causation_id)
            insert.bind_text(8,payload); insert.bind_text(9,created_at); _ = insert.step()
            var stored_events = List[EventRow]()
            for item in events:
                var event_actor = item.actor if item.actor != "" else actor
                var event_correlation = item.correlation_id if item.correlation_id != "" else correlation_id
                var event_causation = item.causation_id if item.causation_id != "" else causation_id
                var event = self._append_event_in_tx(run_id, item.id, item.event_type, item.payload, item.created_at, item.impulse_id, item.process_id, command_id, item.schema_version, event_actor, event_correlation, event_causation)
                stored_events.append(event^)
            self.db.commit()
            var command = CommandRow(run_id=run_id, id=command_id, command_type=command_type, idempotency_key=idempotency_key, actor=actor, correlation_id=correlation_id, causation_id=causation_id, payload=payload, created_at=created_at)
            return CommandSubmission(command=command^, replayed=False, events=stored_events^)
        except err:
            self.db.rollback()
            var detail = String(err)
            if detail.find("command id already exists with different idempotency key") >= 0 or detail.find("run.create commands must use create_run") >= 0 or detail.find("event id already exists with different contents") >= 0 or detail.find("command id, type, idempotency_key, and created_at must not be empty") >= 0:
                raise err^
            raise SQLiteError(code=1, message="journal: submit_command failed: " + detail)

    def get_command_by_idempotency(mut self, run_id: String, idempotency_key: String) raises SQLiteError -> CommandRow:
        self._require_run(run_id)
        var stmt = self.db.query("SELECT run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
        stmt.bind_text(1, run_id); stmt.bind_text(2, idempotency_key)
        return self._read_command(stmt)

    def get_command(mut self, run_id: String, command_id: String) raises SQLiteError -> CommandRow:
        self._require_run(run_id)
        var stmt = self.db.query("SELECT run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at FROM runtime_commands WHERE run_id=? AND id=?")
        stmt.bind_text(1, run_id); stmt.bind_text(2, command_id)
        return self._read_command(stmt)

    def list_commands(mut self, run_id: String, command_type: String = "", actor: String = "", limit: Int = 0) raises SQLiteError -> List[CommandRow]:
        self._require_run(run_id)
        if limit < 0: raise SQLiteError(code=1, message="journal: command limit must be non-negative")
        var result = List[CommandRow]()
        var sql = "SELECT run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at FROM runtime_commands WHERE run_id=?"
        if command_type != "": sql += " AND command_type=?"
        if actor != "": sql += " AND actor=?"
        sql += " ORDER BY created_at ASC, id ASC"
        if limit > 0: sql += " LIMIT " + String(limit)
        var stmt = self.db.query(sql); stmt.bind_text(1, run_id)
        var index = 2
        if command_type != "": stmt.bind_text(index, command_type); index += 1
        if actor != "": stmt.bind_text(index, actor)
        while stmt.step():
            result.append(CommandRow(run_id=self._text(stmt,0), id=self._text(stmt,1), command_type=self._text(stmt,2), idempotency_key=self._text(stmt,3), actor=self._text(stmt,4), correlation_id=self._text(stmt,5), causation_id=self._text(stmt,6), payload=self._text(stmt,7), created_at=self._text(stmt,8))^)
        return result^

    def append_event(
        mut self, run_id: String, event_id: String, event_type: String,
        payload: String, created_at: String, impulse_id: String = "",
        process_id: String = "", command_id: String = "", schema_version: Int = 1,
        actor: String = "", correlation_id: String = "", causation_id: String = "",
    ) raises SQLiteError -> EventRow:
        self._require_run(run_id); self.db.begin_immediate()
        try:
            var event = self._append_event_in_tx(run_id,event_id,event_type,payload,created_at,impulse_id,process_id,command_id,schema_version,actor,correlation_id,causation_id)
            self.db.commit(); return event^
        except err:
            self.db.rollback()
            var detail = String(err)
            if detail.find("event id already exists with different contents") >= 0:
                raise err^
            raise SQLiteError(code=1, message="journal: append_event failed")

    def list_events(
        mut self, run_id: String, impulse_id: String = "", process_id: String = "",
        after_sequence: Int = -1, limit: Int = 0, event_type: String = "",
    ) raises SQLiteError -> List[EventRow]:
        self._require_run(run_id)
        if after_sequence < -1 or limit < 0: raise SQLiteError(code=1, message="journal: invalid event filter")
        var result = List[EventRow]()
        var sql = "SELECT run_id,sequence,id,event_type,schema_version,impulse_id,process_id,command_id,actor,correlation_id,causation_id,payload,created_at FROM runtime_events WHERE run_id=?"
        if impulse_id != "": sql = sql + " AND impulse_id=?"
        if process_id != "": sql = sql + " AND process_id=?"
        if event_type != "": sql = sql + " AND event_type=?"
        if after_sequence >= 0: sql = sql + " AND sequence>?"
        sql = sql + " ORDER BY sequence ASC"
        if limit > 0: sql = sql + " LIMIT " + String(limit)
        var stmt = self.db.query(sql); stmt.bind_text(1,run_id)
        var index = 2
        if impulse_id != "": stmt.bind_text(index,impulse_id); index += 1
        if process_id != "": stmt.bind_text(index,process_id); index += 1
        if event_type != "": stmt.bind_text(index,event_type); index += 1
        if after_sequence >= 0: stmt.bind_int(index,after_sequence)
        while stmt.step():
            result.append(EventRow(run_id=self._text(stmt,0), sequence=stmt.column_int(1), id=self._text(stmt,2), event_type=self._text(stmt,3), schema_version=stmt.column_int(4), impulse_id=self._text(stmt,5), process_id=self._text(stmt,6), command_id=self._text(stmt,7), actor=self._text(stmt,8), correlation_id=self._text(stmt,9), causation_id=self._text(stmt,10), payload=self._text(stmt,11), created_at=self._text(stmt,12))^)
        return result^
    def schedule_process(
        mut self, run_id: String, process_id: String, process_type: String,
        created_at: String, input_json: String = "{}", metadata: String = "{}",
        impulse_id: String = "", priority: Int = 0, max_attempts: Int = 1,
        available_at: String = "", output_schema_json: String = "{}",
        idempotency_key: String = "", actor: String = "",
    ) raises SQLiteError -> ProcessRow:
        self._require_run(run_id)
        if process_id == "" or process_type == "" or created_at == "" or max_attempts < 1:
            raise SQLiteError(code=1, message="journal: invalid process")
        var due = available_at if available_at != "" else created_at
        var key = idempotency_key if idempotency_key != "" else "process.schedule:" + process_id
        var command_id = key
        self.db.begin_immediate()
        try:
            var prior = self.db.query("SELECT id,command_type,idempotency_key,payload,created_at FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
            prior.bind_text(1,run_id); prior.bind_text(2,key)
            if prior.step():
                var replay_id = self._process_id_from_payload(self._text(prior,3), "")
                if replay_id == "":
                    raise SQLiteError(code=1, message="journal: schedule replay payload missing process_id")
                self.db.commit()
                return self.get_process(run_id, replay_id)
            var normalized_input = self._canonical_json_field(input_json, "process input")
            var normalized_metadata = self._canonical_json_field(metadata, "process metadata")
            var normalized_schema = self._canonical_json_field(output_schema_json, "process output schema")
            var payload = "{\"process_id\":" + self._json_quote(process_id) + ",\"process_type\":" + self._json_quote(process_type) + ",\"input\":" + normalized_input + ",\"metadata\":" + normalized_metadata + "}"
            var existing = self.db.query("SELECT status,process_type,impulse_id,priority,max_attempts,available_at,input_json,metadata,created_at,output_schema_json FROM processes WHERE run_id=? AND id=?")
            existing.bind_text(1,run_id); existing.bind_text(2,process_id)
            if existing.step():
                if self._text(existing,1) != process_type or self._text(existing,2) != impulse_id or existing.column_int(3) != priority or existing.column_int(4) != max_attempts or self._text(existing,5) != due or self._text(existing,6) != normalized_input or self._text(existing,7) != normalized_metadata or self._text(existing,8) != created_at or self._text(existing,9) != normalized_schema:
                    raise SQLiteError(code=1, message="journal: process already exists with different contents")
                raise SQLiteError(code=1, message="journal: process scheduling idempotency conflict")
            var insert = self.db.query("INSERT INTO processes (run_id,id,process_type,impulse_id,status,priority,attempt,max_attempts,available_at,input_json,output_json,error_json,metadata,created_at,updated_at,output_schema_json) VALUES (?,?,?,?,'ready',?,0,?,?,?,'{}','{}',?,?,?,?)")
            insert.bind_text(1,run_id); insert.bind_text(2,process_id); insert.bind_text(3,process_type)
            if impulse_id == "": insert.bind_null(4)
            else: insert.bind_text(4,impulse_id)
            insert.bind_int(5,priority); insert.bind_int(6,max_attempts); insert.bind_text(7,due); insert.bind_text(8,normalized_input); insert.bind_text(9,normalized_metadata); insert.bind_text(10,created_at); insert.bind_text(11,created_at); insert.bind_text(12,normalized_schema); _ = insert.step()
            var command = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,payload,created_at) VALUES (?,?,?,?,?,?,?)")
            command.bind_text(1,run_id); command.bind_text(2,command_id); command.bind_text(3,"process.schedule"); command.bind_text(4,key)
            if actor == "": command.bind_null(5)
            else: command.bind_text(5,actor)
            command.bind_text(6,payload); command.bind_text(7,created_at); _ = command.step()
            _ = self._append_event_in_tx(run_id, command_id + ":event", "process.scheduled", payload, created_at, impulse_id, process_id, command_id, 1, actor, "", "")
            self.db.commit()
        except err:
            self.db.rollback(); var detail = String(err)
            if detail.find("different contents") >= 0 or detail.find("idempotency conflict") >= 0: raise err^
            raise SQLiteError(code=1, message="journal: schedule_process failed: " + detail)
        return self.get_process(run_id, process_id)

    def schedule_process_with_command(
        mut self,
        process: ProcessRow,
        command: CommandRow,
        events: List[EventInput] = List[EventInput](),
    ) raises SQLiteError -> CommandSubmission:
        """Atomically schedule a pending/ready process with a caller command and events."""
        if command.run_id != process.run_id:
            raise SQLiteError(code=1, message="process.schedule command run_id must match process.run_id")
        if command.command_type != "process.schedule":
            raise SQLiteError(code=1, message="schedule_process requires command_type 'process.schedule'")
        if command.id == "" or command.idempotency_key == "" or command.created_at == "":
            raise SQLiteError(code=1, message="journal: command id, idempotency_key, and created_at must not be empty")
        self.db.begin_immediate()
        try:
            var existing = self.db.query("SELECT run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
            existing.bind_text(1, command.run_id); existing.bind_text(2, command.idempotency_key)
            if existing.step():
                var stored = CommandRow(
                    run_id=self._text(existing, 0), id=self._text(existing, 1),
                    command_type=self._text(existing, 2), idempotency_key=self._text(existing, 3),
                    actor=self._text(existing, 4), correlation_id=self._text(existing, 5),
                    causation_id=self._text(existing, 6), payload=self._text(existing, 7),
                    created_at=self._text(existing, 8),
                )
                self.db.commit()
                return CommandSubmission(command=stored^, events=List[EventRow](), replayed=True)
            self._require_run(process.run_id)
            if process.id == "" or process.process_type == "" or process.created_at == "" or process.max_attempts < 1:
                raise SQLiteError(code=1, message="journal: invalid process")
            if process.status != "pending" and process.status != "ready":
                raise SQLiteError(code=1, message="schedule_process requires process status 'pending' or 'ready'")
            var normalized_input = self._canonical_json_field(process.input_json, "process input")
            var normalized_metadata = self._canonical_json_field(process.metadata, "process metadata")
            var normalized_schema = self._canonical_json_field(process.output_schema_json, "process output schema")
            var prior_process = self.db.query("SELECT 1 FROM processes WHERE run_id=? AND id=?")
            prior_process.bind_text(1, process.run_id); prior_process.bind_text(2, process.id)
            if prior_process.step():
                raise SQLiteError(code=1, message="Process already exists: " + process.id)
            var insert = self.db.query("INSERT INTO processes (run_id,id,process_type,impulse_id,status,priority,attempt,max_attempts,available_at,lease_owner,lease_expires_at,input_json,output_json,error_json,metadata,created_at,updated_at,started_at,finished_at,output_schema_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            insert.bind_text(1, process.run_id); insert.bind_text(2, process.id); insert.bind_text(3, process.process_type)
            if process.impulse_id == "": insert.bind_null(4)
            else: insert.bind_text(4, process.impulse_id)
            insert.bind_text(5, process.status); insert.bind_int(6, process.priority); insert.bind_int(7, process.attempt); insert.bind_int(8, process.max_attempts); insert.bind_text(9, process.available_at)
            if process.lease_owner == "": insert.bind_null(10)
            else: insert.bind_text(10, process.lease_owner)
            if process.lease_expires_at == "": insert.bind_null(11)
            else: insert.bind_text(11, process.lease_expires_at)
            insert.bind_text(12, normalized_input); insert.bind_text(13, process.output_json); insert.bind_text(14, process.error_json); insert.bind_text(15, normalized_metadata); insert.bind_text(16, process.created_at); insert.bind_text(17, process.updated_at)
            if process.started_at == "": insert.bind_null(18)
            else: insert.bind_text(18, process.started_at)
            if process.finished_at == "": insert.bind_null(19)
            else: insert.bind_text(19, process.finished_at)
            insert.bind_text(20, normalized_schema); _ = insert.step()
            var command_insert = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at) VALUES (?,?,?,?,?,?,?,?,?)")
            command_insert.bind_text(1, command.run_id); command_insert.bind_text(2, command.id); command_insert.bind_text(3, command.command_type); command_insert.bind_text(4, command.idempotency_key)
            if command.actor == "": command_insert.bind_null(5)
            else: command_insert.bind_text(5, command.actor)
            if command.correlation_id == "": command_insert.bind_null(6)
            else: command_insert.bind_text(6, command.correlation_id)
            if command.causation_id == "": command_insert.bind_null(7)
            else: command_insert.bind_text(7, command.causation_id)
            command_insert.bind_text(8, command.payload); command_insert.bind_text(9, command.created_at); _ = command_insert.step()
            var stored_events = List[EventRow]()
            for item in events:
                var event_actor = item.actor if item.actor != "" else command.actor
                var event_correlation = item.correlation_id if item.correlation_id != "" else command.correlation_id
                var event_causation = item.causation_id if item.causation_id != "" else command.causation_id
                var event = self._append_event_in_tx(process.run_id, item.id, item.event_type, item.payload, item.created_at, item.impulse_id, item.process_id, command.id, item.schema_version, event_actor, event_correlation, event_causation)
                stored_events.append(event^)
            self.db.commit()
            return CommandSubmission(command=command.copy(), events=stored_events^, replayed=False)
        except err:
            self.db.rollback()
            raise SQLiteError(code=1, message="journal: schedule_process_with_command failed: " + String(err))

    def transition_process_with_command(
        mut self, run_id: String, process_id: String, target_status: String,
        command: CommandRow, output_json: String = "{}", error_json: String = "{}",
        available_at: String = "", input_json: String = "", events: List[EventInput] = List[EventInput](),
    ) raises SQLiteError -> ProcessTransitionResult:
        if command.run_id != run_id: raise SQLiteError(code=1, message="process transition command run_id must match run_id")
        if command.id == "" or command.idempotency_key == "" or command.created_at == "": raise SQLiteError(code=1, message="journal: command id, idempotency_key, and created_at must not be empty")
        var expected_type = "process.ready" if target_status == "ready" else ("process.complete" if target_status == "succeeded" else ("process.fail" if target_status == "failed" else ("process.retry" if target_status == "retry_wait" else ("process.wait" if target_status == "waiting" else ("process.cancel_requested" if target_status == "cancel_requested" else ("process.cancel" if target_status == "cancelled" else ("process.timeout" if target_status == "timed_out" else "")))))))
        if expected_type == "" or command.command_type != expected_type: raise SQLiteError(code=1, message="journal: invalid process transition command")
        self.db.begin_immediate()
        try:
            var existing = self.db.query("SELECT run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
            existing.bind_text(1,run_id); existing.bind_text(2,command.idempotency_key)
            if existing.step():
                var stored = CommandRow(
                    run_id=self._text(existing, 0), id=self._text(existing, 1),
                    command_type=self._text(existing, 2), idempotency_key=self._text(existing, 3),
                    actor=self._text(existing, 4), correlation_id=self._text(existing, 5),
                    causation_id=self._text(existing, 6), payload=self._text(existing, 7),
                    created_at=self._text(existing, 8),
                )
                var replay_payload = command.payload
                var stored_payload = self._canonical_json_field(stored.payload, "stored process command")
                var requested_payload = self._canonical_json_field(replay_payload, "process command")
                var stored_process_id = self._process_id_from_payload(stored_payload, "")
                var requested_process_id = self._process_id_from_payload(requested_payload, "")
                var stored_attempt = self._process_attempt_from_payload(stored_payload, -1)
                var requested_attempt = self._process_attempt_from_payload(requested_payload, -1)
                var payload_conflict = stored_payload != requested_payload
                if stored_attempt >= 0 and requested_attempt >= 0 and stored_attempt != requested_attempt:
                    payload_conflict = True
                if stored_process_id == "" or requested_process_id == "" or stored_process_id != process_id or requested_process_id != process_id or stored.command_type != expected_type or stored.actor != command.actor or stored.correlation_id != command.correlation_id or stored.causation_id != command.causation_id or stored.created_at != command.created_at or payload_conflict:
                    raise SQLiteError(code=1, message="journal: transition idempotency conflict")
                var replay = self.get_process(run_id, stored_process_id)
                self.db.commit(); return ProcessTransitionResult(process=replay^, submission=CommandSubmission(command=stored^, events=List[EventRow](), replayed=True))
            self._require_run(run_id)
            var current = self.get_process(run_id, process_id)
            var command_process_id = self._process_id_from_payload(command.payload, "")
            if command_process_id == "" or command_process_id != process_id: raise SQLiteError(code=1, message="journal: transition process_id is required and must match")
            var command_attempt = self._process_attempt_from_payload(command.payload, -1)
            if command_attempt >= 0 and current.attempt != command_attempt: raise SQLiteError(code=1, message="journal: transition attempt conflict")
            if target_status == "succeeded" or target_status == "failed":
                if current.status != "running" and current.status != "waiting": raise SQLiteError(code=1, message="journal: process not running or waiting")
                if current.lease_owner != "" and current.lease_owner != command.actor: raise SQLiteError(code=1, message="journal: process lease is held by another actor")
            elif target_status == "retry_wait":
                if current.status != "running" and current.status != "failed": raise SQLiteError(code=1, message="journal: process cannot be retried")
                if current.lease_owner != "" and current.lease_owner != command.actor: raise SQLiteError(code=1, message="journal: process lease is held by another actor")
                if current.attempt >= current.max_attempts: raise SQLiteError(code=1, message="journal: process retry attempts exhausted")
            elif target_status == "waiting":
                if current.status != "running": raise SQLiteError(code=1, message="journal: process cannot wait")
                if current.lease_owner != "" and current.lease_owner != command.actor: raise SQLiteError(code=1, message="journal: process lease is held by another actor")
            elif target_status == "ready":
                if current.status != "pending": raise SQLiteError(code=1, message="journal: process cannot become ready")
            elif target_status == "cancel_requested" or target_status == "cancelled" or target_status == "timed_out":
                if not can_transition_process(ProcessStatus(current.status), ProcessStatus(target_status)):
                    raise SQLiteError(code=1, message="journal: illegal process cancellation transition")
                if current.lease_owner != "" and current.lease_owner != command.actor:
                    raise SQLiteError(code=1, message="journal: process lease is held by another actor")
            elif ProcessStatus(current.status).is_terminal():
                raise SQLiteError(code=1, message="journal: process status is terminal")
            var normalized_output = self._canonical_json_field(output_json, "process output")
            var normalized_error = self._canonical_json_field(error_json, "process error")
            # Caller-command payload is audit data; validate it, but persist it verbatim.
            var payload = command.payload if command.payload != "" else "{\"process_id\":" + self._json_quote(process_id) + "}"
            if command.payload != "":
                var normalized_payload = self._canonical_json_field(command.payload, "process command")
                if normalized_payload == "": raise SQLiteError(code=1, message="journal: invalid process command JSON")
            var insert = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at) VALUES (?,?,?,?,?,?,?,?,?)")
            insert.bind_text(1,run_id); insert.bind_text(2,command.id); insert.bind_text(3,command.command_type); insert.bind_text(4,command.idempotency_key)
            if command.actor == "": insert.bind_null(5)
            else: insert.bind_text(5,command.actor)
            if command.correlation_id == "": insert.bind_null(6)
            else: insert.bind_text(6,command.correlation_id)
            if command.causation_id == "": insert.bind_null(7)
            else: insert.bind_text(7,command.causation_id)
            var update = self.db.query("UPDATE processes SET status=?,output_json=CASE WHEN ? IN ('succeeded','waiting') THEN ? WHEN ?='failed' THEN '{}' ELSE output_json END,error_json=CASE WHEN ? IN ('failed','cancelled','timed_out','cancel_requested') THEN ? WHEN ?='retry_wait' AND ?<>'{}' THEN ? ELSE error_json END,available_at=CASE WHEN ?='retry_wait' THEN CASE WHEN ?='' THEN ? ELSE ? END ELSE available_at END,input_json=CASE WHEN ?='ready' AND ?<>'' THEN ? ELSE input_json END,lease_owner=NULL,lease_expires_at=NULL,finished_at=CASE WHEN ?='retry_wait' THEN NULL WHEN ? IN ('succeeded','failed','cancelled','timed_out') THEN ? ELSE finished_at END,updated_at=? WHERE run_id=? AND id=? AND status=?")
            insert.bind_text(8,payload); insert.bind_text(9,command.created_at); _ = insert.step()
            update.bind_text(1,target_status); update.bind_text(2,target_status); update.bind_text(3,normalized_output); update.bind_text(4,target_status); update.bind_text(5,target_status); update.bind_text(6,normalized_error); update.bind_text(7,target_status); update.bind_text(8,normalized_error); update.bind_text(9,normalized_error); update.bind_text(10,target_status); update.bind_text(11,available_at); update.bind_text(12,command.created_at); update.bind_text(13,available_at); update.bind_text(14,target_status); update.bind_text(15,input_json); update.bind_text(16,input_json); update.bind_text(17,target_status); update.bind_text(18,target_status); update.bind_text(19,command.created_at); update.bind_text(20,command.created_at); update.bind_text(21,run_id); update.bind_text(22,process_id); update.bind_text(23,current.status); _ = update.step()
            if self.db.changes() != 1: raise SQLiteError(code=1, message="journal: process transition changed concurrently")
            var stored_events = List[EventRow]()
            for item in events:
                var event_actor = item.actor if item.actor != "" else command.actor
                var event_correlation = item.correlation_id if item.correlation_id != "" else command.correlation_id
                var event_causation = item.causation_id if item.causation_id != "" else command.causation_id
                var event = self._append_event_in_tx(run_id,item.id,item.event_type,item.payload,item.created_at,item.impulse_id,item.process_id,command.id,item.schema_version,event_actor,event_correlation,event_causation)
                stored_events.append(event^)
            self.db.commit(); var updated = self.get_process(run_id, process_id)
            var returned_command = command.copy()
            return ProcessTransitionResult(process=updated^, submission=CommandSubmission(command=returned_command^, events=stored_events^, replayed=False))
        except err:
            self.db.rollback(); raise SQLiteError(code=1, message="journal: transition_process_with_command failed: " + String(err))

    def claim_next_ready(
        mut self, run_id: String, worker_id: String, now: String,
        lease_expires_at: String, idempotency_key: String = "",
        all_runs: Bool = False,
    ) raises SQLiteError -> Optional[ProcessRow]:
        if run_id == "" and not all_runs:
            raise SQLiteError(code=1, message="journal: claim requires run_id or all_runs")
        if run_id != "": self._require_run(run_id)
        if worker_id == "" or now == "" or lease_expires_at == "" or lease_expires_at <= now:
            raise SQLiteError(code=1, message="journal: invalid queue claim")
        if run_id == "" and all_runs and idempotency_key != "":
            raise SQLiteError(code=1, message="journal: idempotency_key requires run_id")
        self.db.begin_immediate()
        try:
            if idempotency_key != "":
                var prior_claim = self.db.query("SELECT run_id,command_type,actor,created_at,payload FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
                prior_claim.bind_text(1, run_id); prior_claim.bind_text(2, idempotency_key)
                if prior_claim.step():
                    var stored_lease = self._process_lease_from_payload(self._text(prior_claim, 4), "")
                    if self._text(prior_claim, 1) != "process.claim" or self._text(prior_claim, 2) != worker_id or self._text(prior_claim, 3) != now or stored_lease != lease_expires_at:
                        raise SQLiteError(code=1, message="journal: claim idempotency conflict")
                    var replay_run = self._text(prior_claim, 0)
                    var replay_id = self._process_id_from_payload(self._text(prior_claim, 4), "")
                    if replay_id == "": raise SQLiteError(code=1, message="journal: claim replay payload missing process_id")
                    var replay_process = self.get_process(replay_run, replay_id)
                    self.db.commit()
                    return replay_process^
            var expired_sql = "SELECT run_id,id,impulse_id,attempt,lease_owner,input_json FROM processes WHERE status='running' AND lease_expires_at IS NOT NULL AND lease_expires_at<>'' AND lease_expires_at<=? AND attempt>=max_attempts"
            if run_id != "": expired_sql += " AND run_id=?"
            expired_sql += " ORDER BY priority DESC,available_at ASC,created_at ASC,id ASC"
            var expired = self.db.query(expired_sql)
            expired.bind_text(1, now)
            if run_id != "": expired.bind_text(2, run_id)
            while expired.step():
                var expired_run = self._text(expired, 0)
                var failed_id = self._text(expired, 1)
                var attempt = expired.column_int(3)
                var fail_key = "process.fail:" + failed_id + ":" + String(attempt)
                var expired_error = "{\"type\":\"lease_expired\",\"message\":\"process lease expired with no attempts left\",\"lease_owner\":" + self._json_quote(self._text(expired, 4)) + "}"
                var input_digest = ""
                var error_digest = ""
                try:
                    input_digest = self._content_digest(self._text(expired, 5))
                    error_digest = self._content_digest(expired_error)
                except err:
                    raise SQLiteError(code=1, message="journal: unable to digest expired process payload")
                var expired_payload = "{\"process_id\":" + self._json_quote(failed_id) + ",\"attempt\":" + String(attempt) + ",\"input_digest\":" + self._json_quote(input_digest) + ",\"error_digest\":" + self._json_quote(error_digest) + "}"
                var update_expired = self.db.query("UPDATE processes SET status='failed',lease_owner=NULL,lease_expires_at=NULL,finished_at=?,updated_at=?,error_json=? WHERE run_id=? AND id=? AND status='running' AND attempt>=max_attempts")
                update_expired.bind_text(1, now); update_expired.bind_text(2, now); update_expired.bind_text(3, expired_error); update_expired.bind_text(4, expired_run); update_expired.bind_text(5, failed_id); _ = update_expired.step()
                if self.db.changes() == 1:
                    var fail_command = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,payload,created_at) VALUES (?,?,?,?,?,?,?)")
                    fail_command.bind_text(1, expired_run); fail_command.bind_text(2, fail_key); fail_command.bind_text(3, "process.fail"); fail_command.bind_text(4, fail_key); fail_command.bind_text(5, worker_id); fail_command.bind_text(6, "{\"process_id\":" + self._json_quote(failed_id) + "}"); fail_command.bind_text(7, now); _ = fail_command.step()
                    _ = self._append_event_in_tx(expired_run, fail_key + ":event", "process.failed", expired_payload, now, self._text(expired, 2), failed_id, fail_key, 1, worker_id, "", "")
            var next = self.db.query("SELECT run_id,id FROM processes WHERE attempt<max_attempts AND (status='ready' OR (status='retry_wait' AND available_at<=?) OR (status='running' AND lease_expires_at IS NOT NULL AND lease_expires_at<=?))" + (" AND run_id=?" if run_id != "" else "") + " ORDER BY priority DESC,available_at ASC,created_at ASC,id ASC LIMIT 1")
            next.bind_text(1,now); next.bind_text(2,now)
            if run_id != "": next.bind_text(3,run_id)
            if not next.step():
                self.db.commit()
                return {}
            var selected_run = self._text(next, 0)
            var process_id = self._text(next, 1)
            var claim_update = self.db.query("UPDATE processes SET status='running',attempt=attempt+1,lease_owner=?,lease_expires_at=?,started_at=COALESCE(started_at,?),updated_at=? WHERE run_id=? AND id=? AND attempt<max_attempts AND (status='ready' OR (status='retry_wait' AND available_at<=?) OR (status='running' AND lease_expires_at IS NOT NULL AND lease_expires_at<=?))")
            claim_update.bind_text(1, worker_id); claim_update.bind_text(2, lease_expires_at); claim_update.bind_text(3, now); claim_update.bind_text(4, now); claim_update.bind_text(5, selected_run); claim_update.bind_text(6, process_id); claim_update.bind_text(7, now); claim_update.bind_text(8, now); _ = claim_update.step()
            if self.db.changes() != 1: raise SQLiteError(code=1, message="journal: process is not claimable or its attempts are exhausted")
            var row = self.get_process(selected_run, process_id)
            var command_id = idempotency_key if idempotency_key != "" else "process.claim:" + process_id + ":" + String(row.attempt)
            var payload = "{\"process_id\":" + self._json_quote(process_id) + ",\"worker_id\":" + self._json_quote(worker_id) + ",\"attempt\":" + String(row.attempt) + ",\"lease_expires_at\":" + self._json_quote(lease_expires_at) + "}"
            var claim_command = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,payload,created_at) VALUES (?,?,?,?,?,?,?)")
            claim_command.bind_text(1, selected_run); claim_command.bind_text(2, command_id); claim_command.bind_text(3, "process.claim"); claim_command.bind_text(4, command_id); claim_command.bind_text(5, worker_id); claim_command.bind_text(6, payload); claim_command.bind_text(7, now); _ = claim_command.step()
            _ = self._append_event_in_tx(selected_run, command_id + ":event", "process.claimed", payload, now, row.impulse_id, process_id, command_id, 1, worker_id, "", "")
            self.db.commit()
            return row^
        except err:
            self.db.rollback()
            raise SQLiteError(code=1, message="journal: claim_next_ready failed: " + String(err))

    def claim_process(mut self, run_id: String, process_id: String, worker_id: String, now: String, lease_expires_at: String, idempotency_key: String = "") raises SQLiteError -> ProcessRow:
        if run_id == "": raise SQLiteError(code=1, message="journal: run_id must not be empty")
        if process_id == "": raise SQLiteError(code=1, message="journal: process_id must not be empty")
        if worker_id == "": raise SQLiteError(code=1, message="journal: worker_id must not be empty")
        if now == "": raise SQLiteError(code=1, message="journal: claim timestamp must not be empty")
        if lease_expires_at == "" or lease_expires_at <= now: raise SQLiteError(code=1, message="journal: claim lease must expire after claim timestamp")
        self.db.begin_immediate()
        try:
            if idempotency_key != "":
                var prior = self.db.query("SELECT command_type,actor,payload,created_at FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
                prior.bind_text(1,run_id); prior.bind_text(2,idempotency_key)
                if prior.step():
                    var stored_process_id = self._process_id_from_payload(self._text(prior,2), "")
                    if stored_process_id == "": raise SQLiteError(code=1, message="journal: claim replay payload missing process_id")
                    var stored_lease = self._process_lease_from_payload(self._text(prior,2), "")
                    if stored_process_id != process_id or self._text(prior,0) != "process.claim" or self._text(prior,1) != worker_id or self._text(prior,3) != now or stored_lease != lease_expires_at:
                        raise SQLiteError(code=1, message="journal: claim idempotency conflict")
                    var replay = self.get_process(run_id, stored_process_id)
                    self.db.commit()
                    return replay^
            var stmt = self.db.query("UPDATE processes SET status='running',attempt=attempt+1,lease_owner=?,lease_expires_at=?,started_at=COALESCE(started_at,?),updated_at=? WHERE run_id=? AND id=? AND attempt < max_attempts AND (status='ready' OR (status='retry_wait' AND available_at<=?) OR (status='running' AND lease_expires_at IS NOT NULL AND lease_expires_at<=?))")
            stmt.bind_text(1,worker_id); stmt.bind_text(2,lease_expires_at); stmt.bind_text(3,now); stmt.bind_text(4,now); stmt.bind_text(5,run_id); stmt.bind_text(6,process_id); stmt.bind_text(7,now); stmt.bind_text(8,now); _ = stmt.step()
            if self.db.changes() != 1: raise SQLiteError(code=1, message="journal: process is not claimable or its attempts are exhausted")
            var row = self.get_process(run_id, process_id)
            var command_id = idempotency_key if idempotency_key != "" else "process.claim:" + process_id + ":" + String(row.attempt)
            var payload = "{\"process_id\":" + self._json_quote(process_id) + ",\"worker_id\":" + self._json_quote(worker_id) + ",\"attempt\":" + String(row.attempt) + ",\"lease_expires_at\":" + self._json_quote(lease_expires_at) + "}"
            var command = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,payload,created_at) VALUES (?,?,?,?,?,?,?)")
            command.bind_text(1,run_id); command.bind_text(2,command_id); command.bind_text(3,"process.claim"); command.bind_text(4,command_id); command.bind_text(5,worker_id); command.bind_text(6,payload); command.bind_text(7,now); _ = command.step()
            _ = self._append_event_in_tx(run_id, command_id + ":event", "process.claimed", payload, now, row.impulse_id, process_id, command_id, 1, worker_id, "", "")
            self.db.commit(); return row^
        except err:
            self.db.rollback(); raise SQLiteError(code=1, message="journal: claim_process failed: " + String(err))
    def complete_process(mut self, run_id: String, process_id: String, worker_id: String, completed_at: String, output_json: String = "{}", error_json: String = "{}") raises SQLiteError -> ProcessRow:
        self._require_run(run_id)
        var current = self.get_process(run_id, process_id)
        if current.output_schema_json != "":
            try:
                var output = Value(parse_string=output_json)
                var schema = Value(parse_string=current.output_schema_json)
                _validate_output_schema_value(output, schema, "/output_json")
            except err:
                raise SQLiteError(code=1, message="journal: output does not match output_schema_json: " + String(err))
        return self._transition_process(run_id, process_id, "succeeded", worker_id, completed_at, output_json, error_json)

    def advance_correlation_ready(mut self, run_id: String, process_id: String, input_json: String) raises SQLiteError -> ProcessRow:
        """Atomically and idempotently promote one correlation process to ready."""
        var current = self.get_process(run_id, process_id)
        var key = "process.ready:" + process_id
        var command = CommandRow(
            run_id=run_id,
            id=key,
            command_type="process.ready",
            idempotency_key=key,
            actor="correlation",
            correlation_id="",
            causation_id="",
            created_at=current.created_at,
            payload="{\"process_id\":" + self._json_quote(process_id) + "}",
        )
        _ = self.transition_process_with_command(run_id, process_id, "ready", command, "{}", "{}", "", input_json)
        return self.get_process(run_id, process_id)

    def cancel_correlation_dead(mut self, run_id: String, process_id: String, error_json: String = "{}") raises SQLiteError -> ProcessRow:
        """Atomically and idempotently cancel a process with a dead upstream marker."""
        var current = self.get_process(run_id, process_id)
        var key = "process.cancel:" + process_id + ":dead"
        var command = CommandRow(
            run_id=run_id,
            id=key,
            command_type="process.cancel",
            idempotency_key=key,
            actor="correlation",
            correlation_id="",
            causation_id="",
            created_at=current.created_at,

            payload="{\"process_id\":" + self._json_quote(process_id) + ",\"attempt\":" + String(current.attempt) + "}",
        )
        _ = self.transition_process_with_command(run_id, process_id, "cancelled", command, "{}", error_json)
        return self.get_process(run_id, process_id)
    def apply_correlation_children(
        mut self,
        run_id: String,
        children: List[CorrelationChildTransition],
        actor: String = "correlation",
        at: String = "",
        correlation_id: String = "",
        causation_id: String = "",
    ) raises SQLiteError -> List[ProcessRow]:
        """Apply deterministic ready/dead children in one transaction.

        The terminal source transition may already be committed by the driver;
        callers use this as an atomic, idempotent repair boundary for children.
        """
        self._require_run(run_id)
        if actor == "": raise SQLiteError(code=1, message="journal: correlation actor must not be empty")
        var result = List[ProcessRow]()
        self.db.begin_immediate()
        try:
            for child in children:
                if child.process_id == "": raise SQLiteError(code=1, message="journal: correlation child id must not be empty")
                if child.target_status != "ready" and child.target_status != "cancelled": raise SQLiteError(code=1, message="journal: unsupported correlation child status")
                var current = self.get_process(run_id, child.process_id)
                var key = "process.ready:" + child.process_id if child.target_status == "ready" else "process.cancel:" + child.process_id + ":dead"
                var command_type = "process.ready" if child.target_status == "ready" else "process.cancel"
                var transition_at = at if at != "" else current.created_at
                var payload = "{\"process_id\":" + self._json_quote(child.process_id)
                var normalized_error = self._canonical_json_field(child.error_json, "correlation child error")
                if child.target_status == "cancelled": payload += ",\"attempt\":" + String(current.attempt) + ",\"error\":" + normalized_error
                payload += "}"
                var existing = self.db.query("SELECT command_type,actor,correlation_id,causation_id,payload,created_at FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
                existing.bind_text(1,run_id); existing.bind_text(2,key)
                if existing.step():
                    if self._text(existing,0) != command_type or self._text(existing,1) != actor or self._text(existing,2) != correlation_id or self._text(existing,3) != causation_id or self._text(existing,4) != payload or self._text(existing,5) != transition_at: raise SQLiteError(code=1, message="journal: correlation child idempotency conflict")
                    result.append(current^); continue
                if current.status != "pending": raise SQLiteError(code=1, message="journal: correlation child is not pending")
                var update = self.db.query("UPDATE processes SET status=?,input_json=CASE WHEN ?='ready' AND ?<>'' THEN ? ELSE input_json END,error_json=CASE WHEN ?='cancelled' THEN ? ELSE error_json END,finished_at=CASE WHEN ?='cancelled' THEN ? ELSE finished_at END,updated_at=? WHERE run_id=? AND id=? AND status='pending'")
                update.bind_text(1,child.target_status); update.bind_text(2,child.target_status); update.bind_text(3,child.input_json); update.bind_text(4,child.input_json); update.bind_text(5,child.target_status); update.bind_text(6,normalized_error); update.bind_text(7,child.target_status); update.bind_text(8,transition_at); update.bind_text(9,transition_at); update.bind_text(10,run_id); update.bind_text(11,child.process_id); _ = update.step()
                if self.db.changes() != 1: raise SQLiteError(code=1, message="journal: correlation child transition lost ownership")
                var command = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at) VALUES (?,?,?,?,?,?,?,?,?)")
                command.bind_text(1,run_id); command.bind_text(2,key); command.bind_text(3,command_type); command.bind_text(4,key); command.bind_text(5,actor)
                if correlation_id == "": command.bind_null(6)
                else: command.bind_text(6,correlation_id)
                if causation_id == "": command.bind_null(7)
                else: command.bind_text(7,causation_id)
                command.bind_text(8,payload); command.bind_text(9,transition_at); _ = command.step()
                var event_type = "process.ready" if child.target_status == "ready" else "process.cancelled"
                _ = self._append_event_in_tx(run_id,key + ":event",event_type,payload,transition_at,current.impulse_id,child.process_id,key,1,actor,correlation_id,causation_id)
                var updated = self.get_process(run_id, child.process_id)
                result.append(updated^)
            self.db.commit()
        except err:
            self.db.rollback(); raise SQLiteError(code=1, message="journal: correlation child transaction failed: " + String(err))
        return result^


    def get_process(mut self, run_id: String, process_id: String) raises SQLiteError -> ProcessRow:
        self._require_run(run_id)
        var stmt = self.db.query("SELECT run_id,id,process_type,impulse_id,status,priority,attempt,max_attempts,available_at,lease_owner,lease_expires_at,input_json,output_json,error_json,metadata,created_at,updated_at,started_at,finished_at,output_schema_json FROM processes WHERE run_id=? AND id=?")
        stmt.bind_text(1,run_id); stmt.bind_text(2,process_id); return self._read_process(stmt)


    def list_processes(mut self, run_id: String, status: String = "", impulse_id: String = "", limit: Int = 0) raises SQLiteError -> List[ProcessRow]:
        self._require_run(run_id)
        if limit < 0: raise SQLiteError(code=1, message="journal: process limit must be non-negative")
        var result = List[ProcessRow]()
        var sql = "SELECT run_id,id,process_type,impulse_id,status,priority,attempt,max_attempts,available_at,lease_owner,lease_expires_at,input_json,output_json,error_json,metadata,created_at,updated_at,started_at,finished_at,output_schema_json FROM processes WHERE run_id=?"
        if status != "": sql = sql + " AND status=?"
        if impulse_id != "": sql = sql + " AND impulse_id=?"
        sql = sql + " ORDER BY priority DESC, available_at ASC, created_at ASC, id ASC"
        if limit > 0: sql = sql + " LIMIT " + String(limit)
        var stmt = self.db.query(sql); stmt.bind_text(1,run_id)
        var index = 2
        if status != "": stmt.bind_text(index,status); index += 1
        if impulse_id != "": stmt.bind_text(index,impulse_id)
        while stmt.step():
            result.append(ProcessRow(run_id=self._text(stmt,0), id=self._text(stmt,1), process_type=self._text(stmt,2), impulse_id=self._text(stmt,3), status=self._text(stmt,4), priority=stmt.column_int(5), attempt=stmt.column_int(6), max_attempts=stmt.column_int(7), available_at=self._text(stmt,8), lease_owner=self._text(stmt,9), lease_expires_at=self._text(stmt,10), input_json=self._text(stmt,11), output_json=self._text(stmt,12), error_json=self._text(stmt,13), metadata=self._text(stmt,14), created_at=self._text(stmt,15), updated_at=self._text(stmt,16), started_at=self._text(stmt,17), finished_at=self._text(stmt,18), output_schema_json=self._text(stmt,19))^)
        return result^
    def wait_process(mut self, run_id: String, process_id: String, actor: String, at: String, output_json: String = "{}", idempotency_key: String = "") raises SQLiteError -> ProcessRow:
        return self._transition_process(run_id, process_id, "waiting", actor, at, output_json, "{}", "", idempotency_key)

    def request_cancel_process(mut self, run_id: String, process_id: String, actor: String, at: String, error_json: String = "{}", idempotency_key: String = "") raises SQLiteError -> ProcessRow:
        return self._transition_process(run_id, process_id, "cancel_requested", actor, at, "{}", error_json, "", idempotency_key)
    def _transition_process(mut self, run_id: String, process_id: String, to_status: String, actor: String, at: String, output_json: String, error_json: String, available_at: String = "", idempotency_key: String = "") raises SQLiteError -> ProcessRow:
        if process_id == "": raise SQLiteError(code=1, message="journal: process_id must not be empty")
        if at == "": raise SQLiteError(code=1, message="journal: transition timestamp must not be empty")
        var target_status = ProcessStatus(to_status)
        if not target_status.is_known():
            raise SQLiteError(code=1, message="journal: unsupported process transition status " + to_status)
        var expected_type = "process.wait" if to_status == "waiting" else ("process.complete" if to_status == "succeeded" else ("process.fail" if to_status == "failed" else ("process.retry" if to_status == "retry_wait" else ("process.cancel_requested" if to_status == "cancel_requested" else ("process.cancel" if to_status == "cancelled" else ("process.timeout" if to_status == "timed_out" else "process.ready"))))))
        self._require_run(run_id)
        var current = self.get_process(run_id, process_id)
        if actor == "": raise SQLiteError(code=1, message="journal: actor must not be empty")
        var normalized_output = self._canonical_json_field(output_json, "process output")
        var normalized_error = self._canonical_json_field(error_json, "process error")
        var due = available_at if available_at != "" else at
        var effective_key = idempotency_key
        if effective_key == "": effective_key = "process." + process_id + ":" + to_status + ":" + at
        var prior = self.db.query("SELECT command_type,actor,payload,created_at FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
        prior.bind_text(1,run_id); prior.bind_text(2,effective_key)
        if prior.step():
            var stored_payload = self._canonical_json_field(self._text(prior, 2), "stored process command")
            var stored_process_id = self._process_id_from_payload(stored_payload, "")
            var stored_attempt = self._process_attempt_from_payload(stored_payload, -1)
            # The reference `ready` command payload is process_id-only.  Other
            # transition payloads carry attempt identity and must reject a
            # malformed stored payload before any replay is returned.
            var payload_conflict = to_status != "ready" and stored_attempt < 0
            if to_status == "succeeded" or to_status == "waiting":
                payload_conflict = payload_conflict or self._json_field_from_payload(stored_payload, "output", "") != normalized_output
            if to_status == "failed" or to_status == "cancel_requested" or to_status == "cancelled" or to_status == "timed_out":
                payload_conflict = payload_conflict or self._json_field_from_payload(stored_payload, "error", "") != normalized_error
            if to_status == "retry_wait":
                payload_conflict = payload_conflict or self._json_field_from_payload(stored_payload, "available_at", "") != self._json_quote(due)
                if normalized_error != "{}":
                    payload_conflict = payload_conflict or self._json_field_from_payload(stored_payload, "error", "") != normalized_error
            if stored_process_id != process_id or self._text(prior, 0) != expected_type or self._text(prior, 1) != actor or self._text(prior, 3) != at or payload_conflict:
                raise SQLiteError(code=1, message="journal: process transition idempotency conflict")
            return self.get_process(run_id, stored_process_id)
        var stored_output = normalized_output
        if to_status == "failed": stored_output = "{}"
        elif to_status == "cancelled" or to_status == "timed_out" or to_status == "retry_wait": stored_output = current.output_json
        var stored_error = normalized_error
        if to_status == "succeeded" or to_status == "waiting": stored_error = "{}"
        if to_status == "retry_wait" and normalized_error == "{}": stored_error = current.error_json
        var expected_payload_raw = "{\"process_id\":" + self._json_quote(process_id) + ",\"attempt\":" + String(current.attempt) + ",\"output\":" + stored_output + ",\"error\":" + stored_error
        if to_status == "retry_wait": expected_payload_raw += ",\"available_at\":" + self._json_quote(due)
        expected_payload_raw += "}"
        if current.attempt < 0 or current.max_attempts < 1 or current.attempt > current.max_attempts:
            raise SQLiteError(code=1, message="journal: invalid process attempts")
        var legal = can_transition_process(ProcessStatus(current.status), target_status)
        if to_status == "retry_wait" and current.status == "failed": legal = current.lease_owner == "" and current.attempt < current.max_attempts
        if not legal: raise SQLiteError(code=1, message="journal: illegal process transition " + current.status + " -> " + to_status)
        if to_status == "retry_wait" and current.attempt >= current.max_attempts: raise SQLiteError(code=1, message="journal: process retry attempts exhausted")
        if (to_status == "succeeded" or to_status == "failed" or to_status == "retry_wait") and current.lease_owner != "" and current.lease_owner != actor:
            raise SQLiteError(code=1, message="journal: process lease is held by another actor")
        self.db.begin_immediate()
        try:
            var tx_current = self.get_process(run_id, process_id)
            if tx_current.status != current.status or tx_current.attempt != current.attempt or tx_current.lease_owner != current.lease_owner or tx_current.lease_expires_at != current.lease_expires_at or tx_current.output_json != current.output_json or tx_current.error_json != current.error_json:
                raise SQLiteError(code=1, message="journal: process transition changed concurrently")
            var stmt = self.db.query("UPDATE processes SET status=?,output_json=CASE WHEN ?='succeeded' THEN ? WHEN ?='failed' THEN '{}' WHEN ?='waiting' THEN ? ELSE output_json END,error_json=CASE WHEN ? IN ('succeeded','waiting') THEN '{}' WHEN ? IN ('failed','cancelled','timed_out','cancel_requested') THEN ? WHEN ?='retry_wait' AND ?<>'{}' THEN ? ELSE error_json END,available_at=CASE WHEN ?='retry_wait' THEN ? ELSE available_at END,lease_owner=NULL,lease_expires_at=NULL,finished_at=CASE WHEN ?='retry_wait' THEN NULL WHEN ? IN ('succeeded','failed','cancelled','timed_out') THEN ? ELSE finished_at END,updated_at=? WHERE run_id=? AND id=? AND status=?")
            stmt.bind_text(1,to_status); stmt.bind_text(2,to_status); stmt.bind_text(3,stored_output); stmt.bind_text(4,to_status); stmt.bind_text(5,to_status); stmt.bind_text(6,normalized_output); stmt.bind_text(7,to_status); stmt.bind_text(8,to_status); stmt.bind_text(9,stored_error); stmt.bind_text(10,to_status); stmt.bind_text(11,stored_error); stmt.bind_text(12,stored_error); stmt.bind_text(13,to_status); stmt.bind_text(14,due); stmt.bind_text(15,to_status); stmt.bind_text(16,to_status); stmt.bind_text(17,at); stmt.bind_text(18,at); stmt.bind_text(19,run_id); stmt.bind_text(20,process_id); stmt.bind_text(21,current.status); _ = stmt.step()
            if self.db.changes() != 1:
                raise SQLiteError(code=1, message="journal: process transition changed concurrently")
            var command = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,payload,created_at) VALUES (?,?,?,?,?,?,?)")
            command.bind_text(1,run_id); command.bind_text(2,effective_key); command.bind_text(3,expected_type); command.bind_text(4,effective_key); command.bind_text(5,actor); command.bind_text(6,expected_payload_raw); command.bind_text(7,at); _ = command.step()
            var event_type = "process.waiting" if to_status == "waiting" else ("process.completed" if to_status == "succeeded" else ("process.failed" if to_status == "failed" else ("process.retry_scheduled" if to_status == "retry_wait" else ("process.cancel_requested" if to_status == "cancel_requested" else ("process.cancelled" if to_status == "cancelled" else ("process.timed_out" if to_status == "timed_out" else "process.ready"))))))
            _ = self._append_event_in_tx(run_id, effective_key + ":event", event_type, expected_payload_raw, at, current.impulse_id, process_id, effective_key, 1, actor, "", "")
            self.db.commit()
        except err:
            self.db.rollback(); raise SQLiteError(code=1, message="journal: process transition failed: " + String(err))
        return self.get_process(run_id,process_id)

    def fail_process(mut self, run_id: String, process_id: String, actor: String, at: String, error_json: String = "{}") raises SQLiteError -> ProcessRow:
        return self._transition_process(run_id,process_id,"failed",actor,at,"{}",error_json)

    def retry_process(mut self, run_id: String, process_id: String, actor: String, at: String, available_at: String, error_json: String = "{}") raises SQLiteError -> ProcessRow:
        return self._transition_process(run_id,process_id,"retry_wait",actor,at,"{}",error_json,available_at)

    def cancel_process(mut self, run_id: String, process_id: String, actor: String, at: String, error_json: String = "{}") raises SQLiteError -> ProcessRow:
        return self._transition_process(run_id,process_id,"cancelled",actor,at,"{}",error_json)

    def timeout_process(mut self, run_id: String, process_id: String, actor: String, at: String, error_json: String = "{}") raises SQLiteError -> ProcessRow:
        return self._transition_process(run_id,process_id,"timed_out",actor,at,"{}",error_json)
    def park_homeostat_process(
        mut self,
        run_id: String,
        homeostat_id: String,
        process_id: String,
        actor: String,
        at: String,
        output_json: String = "{}",
        metadata_json: String = "{}",
        idempotency_key: String = "",
    ) raises SQLiteError -> ProcessRow:
        """Atomically persist an open homeostat and park its claimed process."""
        self._require_run(run_id)
        if homeostat_id == "" or process_id == "" or actor == "" or at == "":
            raise SQLiteError(code=1, message="journal: homeostat park requires ids, actor, and timestamp")
        var normalized_output = self._canonical_json_field(output_json, "homeostat output")
        var normalized_metadata = self._canonical_json_field(metadata_json, "homeostat metadata")
        var key = idempotency_key if idempotency_key != "" else "homeostat.open:" + homeostat_id
        var payload = "{\"homeostat_id\":" + self._json_quote(homeostat_id) + ",\"process_id\":" + self._json_quote(process_id) + ",\"output\":" + normalized_output + ",\"metadata\":" + normalized_metadata + "}"
        self.db.begin_immediate()
        try:
            var existing = self.db.query("SELECT command_type,payload,created_at FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
            existing.bind_text(1, run_id); existing.bind_text(2, key)
            if existing.step():
                if self._text(existing, 0) != "homeostat.open" or self._text(existing, 1) != payload or self._text(existing, 2) != at:
                    raise SQLiteError(code=1, message="journal: homeostat open idempotency conflict")
                var replay = self.get_process(run_id, process_id)
                self.db.commit()
                return replay^
            var process_stmt = self.db.query("SELECT run_id,id,process_type,impulse_id,status,priority,attempt,max_attempts,available_at,lease_owner,lease_expires_at,input_json,output_json,error_json,metadata,created_at,updated_at,started_at,finished_at,output_schema_json FROM processes WHERE run_id=? AND id=?")
            process_stmt.bind_text(1, run_id); process_stmt.bind_text(2, process_id)
            var process = self._read_process(process_stmt)
            if process.status != "running" or process.lease_owner != actor:
                raise SQLiteError(code=1, message="journal: homeostat park requires an owned running process")
            var homeostat = self.db.query("SELECT status,impulse_id,values_json,metadata,attempt,max_attempts FROM homeostats WHERE run_id=? AND id=?")
            homeostat.bind_text(1, run_id); homeostat.bind_text(2, homeostat_id)
            if homeostat.step():
                raise SQLiteError(code=1, message="journal: homeostat already exists")
            var insert_homeostat = self.db.query("INSERT INTO homeostats (run_id,id,kind,impulse_id,status,values_json,metadata,attempt,max_attempts,created_at,updated_at) VALUES (?,?,? ,?,'open',?,?,?,?,?,?)")
            insert_homeostat.bind_text(1, run_id); insert_homeostat.bind_text(2, homeostat_id); insert_homeostat.bind_text(3, "manual_homeostat")
            if process.impulse_id == "": insert_homeostat.bind_null(4)
            else: insert_homeostat.bind_text(4, process.impulse_id)
            insert_homeostat.bind_text(5, normalized_output); insert_homeostat.bind_text(6, normalized_metadata); insert_homeostat.bind_int(7, process.attempt); insert_homeostat.bind_int(8, process.max_attempts); insert_homeostat.bind_text(9, at); insert_homeostat.bind_text(10, at); _ = insert_homeostat.step()
            var update_process = self.db.query("UPDATE processes SET status='waiting',output_json=?,error_json='{}',lease_owner=NULL,lease_expires_at=NULL,finished_at=NULL,updated_at=? WHERE run_id=? AND id=? AND status='running' AND lease_owner=?")
            update_process.bind_text(1, normalized_output); update_process.bind_text(2, at); update_process.bind_text(3, run_id); update_process.bind_text(4, process_id); update_process.bind_text(5, actor); _ = update_process.step()
            if self.db.changes() != 1: raise SQLiteError(code=1, message="journal: homeostat park lost process ownership")
            var command = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,payload,created_at) VALUES (?,?,?,?,?,?,?)")
            command.bind_text(1,run_id); command.bind_text(2,key); command.bind_text(3,"homeostat.open"); command.bind_text(4,key); command.bind_text(5,actor); command.bind_text(6,payload); command.bind_text(7,at); _ = command.step()
            _ = self._append_event_in_tx(run_id, key + ":event", "homeostat.opened", payload, at, process.impulse_id, process_id, key, 1, actor, "", "")
            var wait_key = key + ":process"
            var wait_command = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,payload,created_at) VALUES (?,?,?,?,?,?,?)")
            wait_command.bind_text(1,run_id); wait_command.bind_text(2,wait_key); wait_command.bind_text(3,"process.wait"); wait_command.bind_text(4,wait_key); wait_command.bind_text(5,actor); wait_command.bind_text(6,payload); wait_command.bind_text(7,at); _ = wait_command.step()
            _ = self._append_event_in_tx(run_id, wait_key + ":event", "process.waiting", payload, at, process.impulse_id, process_id, wait_key, 1, actor, "", "")
            self.db.commit()
        except err:
            self.db.rollback()
            var detail = String(err)
            if detail.find("homeostat open idempotency conflict") >= 0 or detail.find("homeostat open contents conflict") >= 0:
                raise err^
            raise SQLiteError(code=1, message="journal: park homeostat process failed: " + detail)
        return self.get_process(run_id, process_id)

    def transition_homeostat_process(
        mut self,
        run_id: String,
        homeostat_id: String,
        process_id: String,
        homeostat_status: String,
        process_status: String,
        actor: String,
        at: String,
        output_json: String = "{}",
        error_json: String = "{}",
        idempotency_key: String = "",
    ) raises SQLiteError -> ProcessRow:
        """Atomically finish an open homeostat and its waiting process."""
        self._require_run(run_id)
        if homeostat_status != "completed" and homeostat_status != "cancelled" and homeostat_status != "expired":
            raise SQLiteError(code=1, message="journal: invalid homeostat terminal status")
        if process_status != "succeeded" and process_status != "cancelled" and process_status != "timed_out":
            raise SQLiteError(code=1, message="journal: invalid homeostat process status")
        if (homeostat_status == "completed" and process_status != "succeeded") or (homeostat_status == "cancelled" and process_status != "cancelled") or (homeostat_status == "expired" and process_status != "timed_out"):
            raise SQLiteError(code=1, message="journal: homeostat and process terminal statuses must agree")
        if homeostat_id == "" or process_id == "" or actor == "" or at == "":
            raise SQLiteError(code=1, message="journal: homeostat transition requires ids, actor, and timestamp")
        var normalized_output = self._canonical_json_field(output_json, "homeostat output")
        var normalized_error = self._canonical_json_field(error_json, "homeostat error")
        var command_type = "homeostat.expire"
        if homeostat_status == "completed": command_type = "homeostat.complete"
        elif homeostat_status == "cancelled": command_type = "homeostat.cancel"
        var key = idempotency_key if idempotency_key != "" else command_type + ":" + homeostat_id
        var payload = "{\"homeostat_id\":" + self._json_quote(homeostat_id) + ",\"process_id\":" + self._json_quote(process_id) + ",\"status\":" + self._json_quote(homeostat_status) + ",\"output\":" + normalized_output + ",\"error\":" + normalized_error + "}"
        self.db.begin_immediate()
        try:
            var existing = self.db.query("SELECT command_type,payload,created_at FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
            existing.bind_text(1, run_id); existing.bind_text(2, key)
            if existing.step():
                if self._text(existing, 0) != command_type or self._text(existing, 1) != payload or self._text(existing, 2) != at:
                    raise SQLiteError(code=1, message="journal: homeostat transition idempotency conflict")
                var replay = self.get_process(run_id, process_id)
                self.db.commit()
                return replay^
            var homeostat = self.db.query("SELECT status FROM homeostats WHERE run_id=? AND id=?")
            homeostat.bind_text(1,run_id); homeostat.bind_text(2,homeostat_id)
            if not homeostat.step(): raise SQLiteError(code=1, message="journal: homeostat not found")
            if self._text(homeostat,0) != "open": raise SQLiteError(code=1, message="journal: homeostat is not open")
            var process_stmt = self.db.query("SELECT run_id,id,process_type,impulse_id,status,priority,attempt,max_attempts,available_at,lease_owner,lease_expires_at,input_json,output_json,error_json,metadata,created_at,updated_at,started_at,finished_at,output_schema_json FROM processes WHERE run_id=? AND id=?")
            process_stmt.bind_text(1,run_id); process_stmt.bind_text(2,process_id)
            var process = self._read_process(process_stmt)
            if process.status != "waiting" or process.lease_owner != "" or process.lease_expires_at != "":
                raise SQLiteError(code=1, message="journal: homeostat transition requires an unleased waiting process")
            var update_homeostat = self.db.query("UPDATE homeostats SET status=?,values_json=?,updated_at=? WHERE run_id=? AND id=? AND status='open'")
            update_homeostat.bind_text(1,homeostat_status); update_homeostat.bind_text(2,normalized_output if process_status == "succeeded" else normalized_error); update_homeostat.bind_text(3,at); update_homeostat.bind_text(4,run_id); update_homeostat.bind_text(5,homeostat_id); _ = update_homeostat.step()
            if self.db.changes() != 1: raise SQLiteError(code=1, message="journal: homeostat transition lost ownership")
            var update_process = self.db.query("UPDATE processes SET status=?,output_json=?,error_json=?,finished_at=?,updated_at=? WHERE run_id=? AND id=? AND status='waiting' AND lease_owner IS NULL")
            update_process.bind_text(1,process_status); update_process.bind_text(2,normalized_output if process_status == "succeeded" else "{}"); update_process.bind_text(3,normalized_error if process_status != "succeeded" else "{}"); update_process.bind_text(4,at); update_process.bind_text(5,at); update_process.bind_text(6,run_id); update_process.bind_text(7,process_id); _ = update_process.step()
            if self.db.changes() != 1: raise SQLiteError(code=1, message="journal: homeostat process transition lost ownership")
            var command = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,payload,created_at) VALUES (?,?,?,?,?,?,?)")
            command.bind_text(1,run_id); command.bind_text(2,key); command.bind_text(3,command_type); command.bind_text(4,key); command.bind_text(5,actor); command.bind_text(6,payload); command.bind_text(7,at); _ = command.step()
            _ = self._append_event_in_tx(run_id, key + ":event", "homeostat." + homeostat_status, payload, at, process.impulse_id, process_id, key, 1, actor, "", "")
            _ = self._append_event_in_tx(run_id, key + ":process-event", "process." + process_status, payload, at, process.impulse_id, process_id, key, 1, actor, "", "")
            self.db.commit()
        except err:
            self.db.rollback()
            var detail = String(err)
            if detail.find("homeostat transition idempotency conflict") >= 0:
                raise err^
            raise SQLiteError(code=1, message="journal: transition homeostat process failed: " + detail)
        return self.get_process(run_id, process_id)

    def reopen_homeostat_process(
        mut self,
        run_id: String,
        homeostat_id: String,
        process_id: String,
        actor: String,
        at: String,
        idempotency_key: String = "",
    ) raises SQLiteError -> ProcessRow:
        """Atomically reopen a terminal homeostat/process pair when budget remains."""
        self._require_run(run_id)
        if homeostat_id == "" or process_id == "" or actor == "" or at == "":
            raise SQLiteError(code=1, message="journal: homeostat reopen requires ids, actor, and timestamp")
        var key = idempotency_key
        self.db.begin_immediate()
        try:
            var homeostat = self.db.query("SELECT status,attempt,max_attempts FROM homeostats WHERE run_id=? AND id=?")
            homeostat.bind_text(1,run_id); homeostat.bind_text(2,homeostat_id)
            if not homeostat.step(): raise SQLiteError(code=1, message="journal: homeostat not found")
            var old_status = self._text(homeostat,0)
            var attempt = homeostat.column_int(1); var max_attempts = homeostat.column_int(2)
            if key == "":
                var reopen_attempt = attempt
                if old_status != "open": reopen_attempt += 1
                key = "homeostat.reopen:" + homeostat_id + ":" + String(reopen_attempt)
            var payload = "{\"homeostat_id\":" + self._json_quote(homeostat_id) + ",\"process_id\":" + self._json_quote(process_id) + "}"
            var existing = self.db.query("SELECT command_type,payload,created_at FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
            existing.bind_text(1,run_id); existing.bind_text(2,key)
            if existing.step():
                if self._text(existing,0) != "homeostat.reopen" or self._text(existing,1) != payload or self._text(existing,2) != at:
                    raise SQLiteError(code=1, message="journal: homeostat reopen idempotency conflict")
                var replay = self.get_process(run_id,process_id); self.db.commit(); return replay^
            if old_status != "completed" and old_status != "cancelled" and old_status != "expired": raise SQLiteError(code=1, message="journal: homeostat is not terminal")
            if attempt >= max_attempts: raise SQLiteError(code=1, message="journal: homeostat attempts exhausted")
            var process_stmt = self.db.query("SELECT run_id,id,process_type,impulse_id,status,priority,attempt,max_attempts,available_at,lease_owner,lease_expires_at,input_json,output_json,error_json,metadata,created_at,updated_at,started_at,finished_at,output_schema_json FROM processes WHERE run_id=? AND id=?")
            process_stmt.bind_text(1,run_id); process_stmt.bind_text(2,process_id)
            var process = self._read_process(process_stmt)
            if process.status != "succeeded" and process.status != "cancelled" and process.status != "timed_out": raise SQLiteError(code=1, message="journal: process is not terminal")
            if process.attempt >= process.max_attempts: raise SQLiteError(code=1, message="journal: process attempts exhausted")
            var update_homeostat = self.db.query("UPDATE homeostats SET status='open',attempt=attempt+1,updated_at=? WHERE run_id=? AND id=? AND status=? AND attempt < max_attempts")
            update_homeostat.bind_text(1,at); update_homeostat.bind_text(2,run_id); update_homeostat.bind_text(3,homeostat_id); update_homeostat.bind_text(4,old_status); _ = update_homeostat.step()
            if self.db.changes() != 1: raise SQLiteError(code=1, message="journal: homeostat reopen lost ownership")
            var update_process = self.db.query("UPDATE processes SET status='waiting',output_json='{}',error_json='{}',available_at=?,lease_owner=NULL,lease_expires_at=NULL,finished_at=NULL,updated_at=? WHERE run_id=? AND id=? AND status=? AND attempt < max_attempts")
            update_process.bind_text(1,at); update_process.bind_text(2,at); update_process.bind_text(3,run_id); update_process.bind_text(4,process_id); update_process.bind_text(5,process.status); _ = update_process.step()
            if self.db.changes() != 1: raise SQLiteError(code=1, message="journal: homeostat process reopen lost ownership")
            var command = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,payload,created_at) VALUES (?,?,?,?,?,?,?)")
            command.bind_text(1,run_id); command.bind_text(2,key); command.bind_text(3,"homeostat.reopen"); command.bind_text(4,key); command.bind_text(5,actor); command.bind_text(6,payload); command.bind_text(7,at); _ = command.step()
            _ = self._append_event_in_tx(run_id,key + ":event","homeostat.reopened",payload,at,process.impulse_id,process_id,key,1,actor,"","")
            _ = self._append_event_in_tx(run_id,key + ":process-event","process.waiting",payload,at,process.impulse_id,process_id,key,1,actor,"","")
            self.db.commit()
        except err:
            self.db.rollback(); var detail = String(err)
            if detail.find("homeostat reopen idempotency conflict") >= 0: raise err^
            raise SQLiteError(code=1, message="journal: reopen homeostat process failed: " + detail)
        return self.get_process(run_id,process_id)
