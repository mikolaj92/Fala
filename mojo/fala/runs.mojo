"""Native run lifecycle persistence helpers.

This module is deliberately thin over ``NativeJournal`` and its SQLite
connection.  Every lifecycle operation performs its command, run-row update,
and event append in one explicit transaction.  Idempotency is resolved before
mutating a run, so replays are no-ops.
"""
from .journal import NativeJournal, RunRow, CommandRow
from .sqlite import Statement, SQLiteError
from .status import RunStatus, can_transition_run


@fieldwise_init
struct RunLifecycleRecord(Copyable, Movable):
    """Complete run row projection for schema-backed lifecycle metadata.

    ``RunRow`` remains the compatibility projection returned by existing
    lifecycle mutations.  This record exposes the nullable run columns without
    changing those callers or inventing persistence outside the runs table.
    Empty strings represent SQL NULL optional columns.
    """
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
struct RunCreateResult(Copyable, Movable):
    """Persisted run projection and exact create command submission."""
    var run: RunLifecycleRecord
    var command: CommandRow
    var replayed: Bool

struct RunLifecycle(Movable):
    var journal: NativeJournal

    def __init__(out self, path: String) raises:
        self.journal = NativeJournal.open(path)
    def __del__(deinit self):
        try:
            self.journal.close()
        except e:
            pass
    def close(mut self) raises:
        self.journal.close()

    @staticmethod
    def open(path: String) raises -> RunLifecycle:
        return RunLifecycle(path)

    def initialize(mut self) raises:
        self.journal.initialize()

    def _text(mut self, mut statement: Statement, index: Int) raises -> String:
        if statement.column_null(index):
            return String("")
        return statement.column_text(index)
    def _bind_nullable(mut self, mut statement: Statement, index: Int, value: String) raises:
        if value == "":
            statement.bind_null(index)
        else:
            statement.bind_text(index, value)
    def _json_nullable_quote(self, value: String) -> String:
        if value == "": return "null"
        return self._json_quote(value)

    def _read_record(mut self, mut statement: Statement) raises -> RunLifecycleRecord:
        if not statement.step():
            raise Error(String(SQLiteError(code=1, message="Unknown run")))
        return RunLifecycleRecord(
            id=self._text(statement, 0),
            status=self._text(statement, 1),
            title=self._text(statement, 2),
            package_id=self._text(statement, 3),
            package_version=self._text(statement, 4),
            package_digest=self._text(statement, 5),
            correlation_path_id=self._text(statement, 6),
            correlation_path_digest=self._text(statement, 7),
            runtime_version=self._text(statement, 8),
            backend_version=self._text(statement, 9),
            schema_version=statement.column_int(10),
            metadata=self._text(statement, 11),
            created_at=self._text(statement, 12),
            updated_at=self._text(statement, 13),
            started_at=self._text(statement, 14),
            finished_at=self._text(statement, 15),
        )

    def get_record(mut self, run_id: String) raises -> RunLifecycleRecord:
        var statement = self.journal.db.query(
            "SELECT id,status,title,package_id,package_version,package_digest,correlation_path_id,correlation_path_digest,runtime_version,backend_version,schema_version,metadata,created_at,updated_at,started_at,finished_at FROM runs WHERE id=?"
        )
        statement.bind_text(1, run_id)
        return self._read_record(statement)


    def _read_run(mut self, mut statement: Statement) raises -> RunRow:
        if not statement.step():
            raise Error(String(SQLiteError(code=1, message="Unknown run")))
        return RunRow(
            id=self._text(statement, 0),
            status=self._text(statement, 1),
            title=self._text(statement, 2),
            metadata=self._text(statement, 3),
            created_at=self._text(statement, 4),
            updated_at=self._text(statement, 5),
        )

    def _find_run(mut self, run_id: String) raises -> RunRow:
        var statement = self.journal.db.query(
            "SELECT id,status,title,metadata,created_at,updated_at FROM runs WHERE id=?"
        )
        statement.bind_text(1, run_id)
        return self._read_run(statement)

    def _command_exists(mut self, run_id: String, key: String) raises -> Bool:
        var statement = self.journal.db.query(
            "SELECT id FROM runtime_commands WHERE run_id=? AND idempotency_key=?"
        )
        statement.bind_text(1, run_id)
        statement.bind_text(2, key)
        return statement.step()
    def _json_quote(self, value: String) -> String:
        var result = String("\"")
        for ch in value.codepoint_slices():
            if ch == '\\': result += "\\\\"
            elif ch == '"': result += "\\\""
            elif ch == '\n': result += "\\n"
            elif ch == '\r': result += "\\r"
            elif ch == '\t': result += "\\t"
            else: result += ch
        return result + "\""

    def _transition_payload(mut self, target: String, reason: String, reason_present: Bool = True) -> String:
        var encoded_reason = self._json_quote(reason) if reason_present else "null"
        return "{\"reason\":" + encoded_reason + ",\"target\":" + self._json_quote(target) + "}"
    def _create_payload(mut self, run_id: String, status: String) -> String:
        return "{\"run_id\":" + self._json_quote(run_id) + ",\"status\":" + self._json_quote(status) + "}"
    def _identity_mismatch(
        self,
        package_id: String,
        package_version: String,
        package_digest: String,
        correlation_path_id: String,
        correlation_path_digest: String,
        runtime_version: String,
        backend_version: String,
        stored: RunLifecycleRecord,
    ) -> Bool:
        return (
            package_id != stored.package_id
            or package_version != stored.package_version
            or package_digest != stored.package_digest
            or correlation_path_id != stored.correlation_path_id
            or correlation_path_digest != stored.correlation_path_digest
            or runtime_version != stored.runtime_version
            or backend_version != stored.backend_version
        )


    def _transition(
        mut self,
        run_id: String,
        target: String,
        command_type: String,
        event_type: String,
        now: String,
        idempotency_key: String,
        command_id: String = "",
        reason: String = "", reason_present: Bool = True,
        actor: String = "",
    ) raises -> RunRow:
        if run_id == "" or idempotency_key == "":
            raise Error(String(SQLiteError(code=1, message="run lifecycle requires run_id and idempotency_key")))
        var current = self._find_run(run_id)
        # Replays must match every command and event-defining field.  A key
        # is not a license to silently accept a different transition.
        var expected_payload = self._transition_payload(target, reason, reason_present)
        if self._command_exists(run_id, idempotency_key):
            var prior = self.journal.db.query("SELECT id,command_type,actor,payload,created_at FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
            prior.bind_text(1, run_id); prior.bind_text(2, idempotency_key)
            if not prior.step():
                raise Error(String(SQLiteError(code=1, message="run lifecycle idempotency conflict")))
            var prior_id = self._text(prior, 0)
            if prior_id != (command_id if command_id != "" else idempotency_key) or self._text(prior, 1) != command_type or self._text(prior, 2) != actor or self._text(prior, 3) != expected_payload or self._text(prior, 4) != now:
                raise Error(String(SQLiteError(code=1, message="run lifecycle idempotency conflict")))
            var event = self.journal.db.query("SELECT id,event_type,actor,payload,created_at FROM runtime_events WHERE run_id=? AND command_id=?")
            event.bind_text(1, run_id); event.bind_text(2, prior_id)
            if not event.step() or self._text(event, 0) != prior_id + ":event" or self._text(event, 1) != event_type or self._text(event, 2) != actor or self._text(event, 3) != expected_payload or self._text(event, 4) != now:
                raise Error(String(SQLiteError(code=1, message="run lifecycle idempotency conflict")))
            return current^
        var from_status = RunStatus(current.status)
        var to_status = RunStatus(target)
        if not from_status.is_known() or not to_status.is_known():
            raise Error(String(SQLiteError(code=1, message="Unknown run status")))
        if not can_transition_run(from_status, to_status):
            raise Error(String(SQLiteError(code=1, message="Invalid run status transition: " + current.status + " -> " + target)))

        var cid = command_id if command_id != "" else idempotency_key
        var eid = cid + ":event"
        self.journal.db.begin()
        try:
            var command = self.journal.db.query(
                "INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at) VALUES (?,?,?,?,?,'','',?,?)"
            )
            command.bind_text(1, run_id)
            command.bind_text(2, cid)
            command.bind_text(3, command_type)
            command.bind_text(4, idempotency_key)
            if actor == "": command.bind_null(5)
            else: command.bind_text(5, actor)
            command.bind_text(6, expected_payload)
            command.bind_text(7, now)
            _ = command.step()

            var update_sql = "UPDATE runs SET status=?,updated_at=?"
            if target == "active":
                update_sql += ",started_at=COALESCE(started_at,?)"
            elif to_status.is_terminal():
                update_sql += ",finished_at=?"
            update_sql += " WHERE id=? AND status=?"
            var update = self.journal.db.query(update_sql)
            update.bind_text(1, target)
            update.bind_text(2, now)
            var index = 3
            if target == "active" or to_status.is_terminal():
                update.bind_text(index, now)
                index += 1
            update.bind_text(index, run_id)
            update.bind_text(index + 1, current.status)
            _ = update.step()
            if self.journal.db.changes() != 1:
                raise Error(String(SQLiteError(code=1, message="run changed concurrently")))

            var next = self.journal.db.query(
                "SELECT COALESCE(MAX(sequence),0)+1 FROM runtime_events WHERE run_id=?"
            )
            next.bind_text(1, run_id)
            if not next.step():
                raise Error(String(SQLiteError(code=1, message="unable to allocate event sequence")))
            var sequence = next.column_int(0)
            var event = self.journal.db.query(
                "INSERT INTO runtime_events (run_id,sequence,id,event_type,schema_version,command_id,actor,payload,created_at) VALUES (?,?,?,?,1,?,?,?,?)"
            )
            event.bind_text(1, run_id)
            event.bind_int(2, sequence)
            event.bind_text(3, eid)
            event.bind_text(4, event_type)
            event.bind_text(5, cid)
            if actor == "": event.bind_null(6)
            else: event.bind_text(6, actor)
            event.bind_text(7, expected_payload)
            event.bind_text(8, now)
            _ = event.step()
            self.journal.db.commit()
        except err:
            self.journal.db.rollback()
            raise Error(String(SQLiteError(code=1, message="run lifecycle transition failed")))
        return self._find_run(run_id)

    def create_result(
        mut self,
        run_id: String,
        created_at: String,
        metadata: String = "{}",
        title: String = "",
        status: String = "created",
        idempotency_key: String = "run.create",
        command_id: String = "",
        package_id: String = "",
        package_version: String = "",
        package_digest: String = "",
        correlation_path_id: String = "",
        correlation_path_digest: String = "",
        runtime_version: String = "",
        backend_version: String = "",
        actor: String = "",
    ) raises -> RunCreateResult:
        if run_id == "" or idempotency_key == "":
            raise Error(String(SQLiteError(code=1, message="run creation requires run_id and idempotency_key")))
        if not RunStatus(status).is_known():
            raise Error(String(SQLiteError(code=1, message="Unknown run status: " + status)))
        var cid = command_id if command_id != "" else idempotency_key
        var eid = cid + ":event"
        var replayed = False
        self.journal.db.begin_immediate()
        try:
            var prior = self.journal.db.query("SELECT id FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
            prior.bind_text(1, run_id)
            prior.bind_text(2, idempotency_key)
            replayed = prior.step()
            if not replayed:
                var existing = self.journal.db.query("SELECT id FROM runs WHERE id=?")
                existing.bind_text(1, run_id)
                if existing.step():
                    raise Error(String(SQLiteError(code=1, message="run already exists")))
                var insert = self.journal.db.query("INSERT INTO runs (id,status,title,package_id,package_version,package_digest,correlation_path_id,correlation_path_digest,runtime_version,backend_version,metadata,created_at,updated_at,schema_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,6)")
                insert.bind_text(1, run_id); insert.bind_text(2, status)
                self._bind_nullable(insert, 3, title); self._bind_nullable(insert, 4, package_id); self._bind_nullable(insert, 5, package_version); self._bind_nullable(insert, 6, package_digest); self._bind_nullable(insert, 7, correlation_path_id); self._bind_nullable(insert, 8, correlation_path_digest); self._bind_nullable(insert, 9, runtime_version); self._bind_nullable(insert, 10, backend_version)
                insert.bind_text(11, metadata); insert.bind_text(12, created_at); insert.bind_text(13, created_at); _ = insert.step()
                var create_payload = self._create_payload(run_id, status)
                var command = self.journal.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at) VALUES (?,?, 'run.create',?,?,'','',?,?)")
                command.bind_text(1, run_id); command.bind_text(2, cid); command.bind_text(3, idempotency_key)
                if actor == "": command.bind_null(4)
                else: command.bind_text(4, actor)
                command.bind_text(5, create_payload); command.bind_text(6, created_at); _ = command.step()
                var event = self.journal.db.query("INSERT INTO runtime_events (run_id,sequence,id,event_type,schema_version,command_id,actor,payload,created_at) VALUES (?,1,?,'run.created',1,?,?,?,?)")
                event.bind_text(1, run_id); event.bind_text(2, eid); event.bind_text(3, cid)
                if actor == "": event.bind_null(4)
                else: event.bind_text(4, actor)
                event.bind_text(5, create_payload); event.bind_text(6, created_at); _ = event.step()
            self.journal.db.commit()
        except err:
            self.journal.db.rollback()
            raise Error(String(SQLiteError(code=1, message="run creation failed")))
        var stored = self.get_record(run_id)
        if self._identity_mismatch(package_id, package_version, package_digest, correlation_path_id, correlation_path_digest, runtime_version, backend_version, stored):
            raise Error(String(SQLiteError(code=1, message="run lifecycle identity mismatch")))
        var command = self.journal.get_command_by_idempotency(run_id, idempotency_key)
        return RunCreateResult(run=stored^, command=command^, replayed=replayed)

    def create(
        mut self,
        run_id: String,
        created_at: String,
        metadata: String = "{}",
        title: String = "",
        status: String = "created",
        idempotency_key: String = "run.create",
        command_id: String = "",
        package_id: String = "",
        package_version: String = "",
        package_digest: String = "",
        correlation_path_id: String = "",
        correlation_path_digest: String = "",
        runtime_version: String = "",
        backend_version: String = "",
        actor: String = "",
    ) raises -> RunRow:
        var result = self.create_result(run_id, created_at, metadata, title, status, idempotency_key, command_id, package_id, package_version, package_digest, correlation_path_id, correlation_path_digest, runtime_version, backend_version, actor)
        return RunRow(id=result.run.id, status=result.run.status, title=result.run.title, metadata=result.run.metadata, created_at=result.run.created_at, updated_at=result.run.updated_at)

    def start(mut self, run_id: String, now: String, idempotency_key: String = "run.start") raises -> RunRow:
        return self._transition(run_id, "active", "run.start", "run.started", now, idempotency_key)

    def wait(mut self, run_id: String, now: String, idempotency_key: String = "run.wait") raises -> RunRow:
        return self._transition(run_id, "waiting", "run.wait", "run.waiting", now, idempotency_key)

    def complete(mut self, run_id: String, now: String, idempotency_key: String = "run.complete") raises -> RunRow:
        return self._transition(run_id, "completed", "run.complete", "run.completed", now, idempotency_key)

    def fail(mut self, run_id: String, now: String, idempotency_key: String = "run.fail") raises -> RunRow:
        return self._transition(run_id, "failed", "run.fail", "run.failed", now, idempotency_key)

    def request_cancel(mut self, run_id: String, now: String, idempotency_key: String = "run.cancel.request", reason: String = "cancel_requested", reason_present: Bool = True, actor: String = "") raises -> RunRow:
        return self._transition(run_id, "cancel_requested", "run.cancel", "run.cancel_requested", now, idempotency_key, reason=reason, reason_present=reason_present, actor=actor)

    def cancel(mut self, run_id: String, now: String, idempotency_key: String = "run.cancel", reason: String = "cancelled", actor: String = "") raises -> RunRow:
        return self._transition(run_id, "cancelled", "run.cancel", "run.cancelled", now, idempotency_key, reason=reason, actor=actor)

    def timeout(mut self, run_id: String, now: String, idempotency_key: String = "run.timeout", reason: String = "timed_out") raises -> RunRow:
        return self._transition(run_id, "timed_out", "run.timeout", "run.timed_out", now, idempotency_key, reason=reason)


def main():
    pass
