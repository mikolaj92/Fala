"""Native SQLite domain records for Essential Fala + co-located sink helpers.

Core path: accept_impulse, record_*, put/get/list domain rows, homeostat/save_projection.
Operator ops (retention, GC, bridge, projection rebuild) are also methods here for
storage access, but the **canonical import surface** for those APIs is:
`ops_maintenance`, `ops_bridge`, `ops_projections` — not required for
package → impulse → run_until_idle → JournalPort.
"""

from std.collections import List
from std.pathlib import Path
from emberjson import Value, Object, to_string
from fala.sqlite import Connection, Statement, SQLiteError
from fala.schema import initialize_native_schema
from fala.json import canonical_json_text
from fala.reactions import FileReactionStore, reaction_digest_or_empty

from fala.domain import (
    Impulse,
    ImpulseType,
    ImpulseRelation,
    Association,
    Reaction,
    Homeostat,
    Projection,
    RuntimeRef,
    RunRef,
    EventRef,
    RuntimeBudget,
    BridgeDelivery,
    bridge_status_transition_allowed,
)

def _quote(value: String) -> String:
    var result = "\""
    for i in range(value.byte_length()):
        var ch = value[byte=i]
        if ch == "\"":
            result += "\\\""
        elif ch == "\\":
            result += "\\\\"
        elif ch == "\n":
            result += "\\n"
        elif ch == "\r":
            result += "\\r"
        elif ch == "\t":
            result += "\\t"
        elif ch < " ":
            result += "\\u0000"
        else:
            result += String(ch)
    result += "\""
    return result

def _bind_nullable(mut stmt: Statement, index: Int, value: String) raises SQLiteError:
    if value == "":
        stmt.bind_null(index)
    else:
        stmt.bind_text(index, value)
def _nullable_json(mut stmt: Statement, index: Int) raises SQLiteError -> String:
    if stmt.column_null(index):
        return "null"
    return _quote(stmt.column_text(index))
def _canonical_reaction_metadata(value: String) raises SQLiteError -> String:
    try:
        var parsed = Value(parse_string=value)
        if not parsed.is_object():
            raise Error("reaction metadata must be an object")
        return canonical_json_text(to_string(parsed))
    except err:
        raise SQLiteError(code=1, message="domain store: reaction metadata must be a JSON object")
def _reaction_journal_payload(row: Reaction) -> String:
    var content_hash = "null"
    if row.content_hash != "": content_hash = _quote(row.content_hash)
    return "{\"reaction_id\":" + _quote(row.id) + ",\"kind\":" + _quote(row.kind) + ",\"uri\":" + _quote(row.uri) + ",\"content_hash\":" + content_hash + "}"




struct RunDeleteCounts(Copyable, Movable):
    """Deterministic row counts produced by an atomic run deletion."""

    var run_id: String
    var bridge_inbox: Int
    var bridge_outbox: Int
    var projections: Int
    var homeostats: Int
    var processes: Int
    var reactions: Int
    var associations: Int
    var impulse_relations: Int
    var impulse_types: Int
    var impulses: Int
    var runtime_events: Int
    var runtime_commands: Int
    var runs: Int

    def __init__(out self, run_id: String):
        self.run_id = run_id
        self.bridge_inbox = 0
        self.bridge_outbox = 0
        self.projections = 0
        self.homeostats = 0
        self.processes = 0
        self.reactions = 0
        self.associations = 0
        self.impulse_relations = 0
        self.impulse_types = 0
        self.impulses = 0
        self.runtime_events = 0
        self.runtime_commands = 0
        self.runs = 0

    def total(self) -> Int:
        return self.bridge_inbox + self.bridge_outbox + self.projections + self.homeostats + self.processes + self.reactions + self.associations + self.impulse_relations + self.impulse_types + self.impulses + self.runtime_events + self.runtime_commands + self.runs
    def accumulate(mut self, other: Self):
        self.bridge_inbox += other.bridge_inbox
        self.bridge_outbox += other.bridge_outbox
        self.projections += other.projections
        self.homeostats += other.homeostats
        self.processes += other.processes
        self.reactions += other.reactions
        self.associations += other.associations
        self.impulse_relations += other.impulse_relations
        self.impulse_types += other.impulse_types
        self.impulses += other.impulses
        self.runtime_events += other.runtime_events
        self.runtime_commands += other.runtime_commands
        self.runs += other.runs


struct RunRetentionCandidate(Copyable, Movable):
    var run_id: String
    var status: String
    var created_at: String
    var updated_at: String
    var finished_at: String

    def __init__(out self, run_id: String, status: String, created_at: String, updated_at: String, finished_at: String):
        self.run_id = run_id
        self.status = status
        self.created_at = created_at
        self.updated_at = updated_at
        self.finished_at = finished_at


struct RunRetentionItem(Copyable, Movable):
    var run_id: String
    var status: String
    var created_at: String
    var updated_at: String
    var finished_at: String
    var deleted: Bool
    var row_counts: RunDeleteCounts

    def __init__(out self, candidate: RunRetentionCandidate):
        self.run_id = candidate.run_id
        self.status = candidate.status
        self.created_at = candidate.created_at
        self.updated_at = candidate.updated_at
        self.finished_at = candidate.finished_at
        self.deleted = False
        self.row_counts = RunDeleteCounts(candidate.run_id)


struct RunRetentionPlan(Copyable, Movable):
    var dry_run: Bool
    var before: String
    var statuses: List[String]
    var candidate_count: Int
    var deleted_run_count: Int
    var row_counts: RunDeleteCounts
    var runs: List[RunRetentionItem]

    def __init__(out self, before: String, var statuses: List[String], dry_run: Bool = True):
        self.dry_run = dry_run
        self.before = before
        self.statuses = statuses.copy()
        self.candidate_count = 0
        self.deleted_run_count = 0
        self.row_counts = RunDeleteCounts("")
        self.runs = List[RunRetentionItem]()



struct ReactionGarbageCollectionPlan(Copyable, Movable):
    var dry_run: Bool
    var reaction_root: String
    var run_ids: List[String]
    var scanned_run_ids: List[String]
    var referenced_count: Int
    var blob_count: Int
    var kept_count: Int
    var candidate_count: Int
    var deleted_count: Int
    var bytes_reclaimable: Int
    var bytes_reclaimed: Int
    var candidates: List[String]
    var collectable: List[String]
    var deleted: List[String]

    def __init__(out self, dry_run: Bool = True):
        self.dry_run = dry_run
        self.reaction_root = ""
        self.run_ids = List[String]()
        self.scanned_run_ids = List[String]()
        self.referenced_count = 0
        self.blob_count = 0
        self.kept_count = 0
        self.candidate_count = 0
        self.deleted_count = 0
        self.bytes_reclaimable = 0
        self.bytes_reclaimed = 0
        self.candidates = List[String]()
        self.collectable = List[String]()
        self.deleted = List[String]()



struct JournalMaintenancePlan(Copyable, Movable):
    var dry_run: Bool
    var older_than_days: Float64
    var keep_last: Int
    var vacuum: Bool
    var before: String
    var retention: RunRetentionPlan
    var reaction_gc: ReactionGarbageCollectionPlan
    var vacuumed: Bool

    def __init__(
        out self,
        older_than_days: Float64,
        keep_last: Int,
        vacuum: Bool,
        dry_run: Bool,
        before: String,
        retention: RunRetentionPlan,
        reaction_gc: ReactionGarbageCollectionPlan,
    ):
        self.dry_run = dry_run
        self.older_than_days = older_than_days
        self.keep_last = keep_last
        self.vacuum = vacuum
        self.before = before
        self.retention = retention.copy()
        self.reaction_gc = reaction_gc.copy()
        self.vacuumed = False


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
@fieldwise_init
struct BridgeEnqueueResult(Copyable, Movable):
    var delivery: BridgeDelivery
    var submission: CommandSubmission

@fieldwise_init
struct ProjectionRebuildResult(Copyable, Movable):
    var projections: List[Projection]
    var submission: CommandSubmission

struct NativeDomainStore(Movable):
    """Connection-owning store for the schema-v6 domain tables."""

    var db: Connection

    def __init__(out self, path: String) raises SQLiteError:
        self.db = Connection(path)
    def __del__(deinit self):
        try:
            self.db.close()
        except e:
            pass


    def close(mut self) raises SQLiteError:
        self.db.close()
    @staticmethod
    def open(path: String) raises SQLiteError -> NativeDomainStore:
        return NativeDomainStore(path)

    def initialize(mut self) raises SQLiteError:
        initialize_native_schema(self.db)


    def delete_run(mut self, run_id: String) raises SQLiteError -> RunDeleteCounts:
        """Delete one run and every run-scoped row atomically.

        Append-only journal triggers are suspended only inside this transaction
        and recreated before commit. Unknown runs are rejected without writes.
        Deletion order follows foreign-key dependencies and count fields follow
        the schema table names for deterministic retention reporting.
        """
        var counts = RunDeleteCounts(run_id)
        self.db.begin_immediate()
        try:
            # Lock the database before inspecting the run or suspending the
            # append-only triggers.  SQLite rolls back trigger DDL together
            # with the row deletes, so every failure restores both triggers.
            self._require_run(run_id)
            self.db.execute("DROP TRIGGER IF EXISTS runtime_events_no_delete")
            self.db.execute("DROP TRIGGER IF EXISTS runtime_commands_no_delete")

            var stmt = self.db.query("DELETE FROM bridge_inbox WHERE run_id=?")
            stmt.bind_text(1, run_id); _ = stmt.step(); counts.bridge_inbox = self.db.changes()
            stmt = self.db.query("DELETE FROM bridge_outbox WHERE run_id=?")
            stmt.bind_text(1, run_id); _ = stmt.step(); counts.bridge_outbox = self.db.changes()
            stmt = self.db.query("DELETE FROM projections WHERE run_id=?")
            stmt.bind_text(1, run_id); _ = stmt.step(); counts.projections = self.db.changes()
            stmt = self.db.query("DELETE FROM homeostats WHERE run_id=?")
            stmt.bind_text(1, run_id); _ = stmt.step(); counts.homeostats = self.db.changes()
            stmt = self.db.query("DELETE FROM processes WHERE run_id=?")
            stmt.bind_text(1, run_id); _ = stmt.step(); counts.processes = self.db.changes()
            stmt = self.db.query("DELETE FROM reactions WHERE run_id=?")
            stmt.bind_text(1, run_id); _ = stmt.step(); counts.reactions = self.db.changes()
            stmt = self.db.query("DELETE FROM associations WHERE run_id=?")
            stmt.bind_text(1, run_id); _ = stmt.step(); counts.associations = self.db.changes()
            stmt = self.db.query("DELETE FROM impulse_relations WHERE run_id=?")
            stmt.bind_text(1, run_id); _ = stmt.step(); counts.impulse_relations = self.db.changes()
            stmt = self.db.query("DELETE FROM impulse_types WHERE run_id=?")
            stmt.bind_text(1, run_id); _ = stmt.step(); counts.impulse_types = self.db.changes()
            stmt = self.db.query("DELETE FROM impulses WHERE run_id=?")
            stmt.bind_text(1, run_id); _ = stmt.step(); counts.impulses = self.db.changes()
            stmt = self.db.query("DELETE FROM runtime_events WHERE run_id=?")
            stmt.bind_text(1, run_id); _ = stmt.step(); counts.runtime_events = self.db.changes()
            stmt = self.db.query("DELETE FROM runtime_commands WHERE run_id=?")
            stmt.bind_text(1, run_id); _ = stmt.step(); counts.runtime_commands = self.db.changes()
            stmt = self.db.query("DELETE FROM runs WHERE id=?")
            stmt.bind_text(1, run_id); _ = stmt.step(); counts.runs = self.db.changes()

            self.db.execute("CREATE TRIGGER IF NOT EXISTS runtime_events_no_delete BEFORE DELETE ON runtime_events BEGIN SELECT RAISE(ABORT, 'runtime_events is append-only'); END")
            self.db.execute("CREATE TRIGGER IF NOT EXISTS runtime_commands_no_delete BEFORE DELETE ON runtime_commands BEGIN SELECT RAISE(ABORT, 'runtime_commands is append-only'); END")
            self.db.commit()
        except err:
            # Rollback includes the trigger drops, retaining the append-only
            # guards when a run is unknown or any delete fails.
            self.db.rollback()
            raise SQLiteError(code=1, message="domain store: delete_run failed")
        return counts^

    @staticmethod
    def _default_retention_statuses() -> List[String]:
        var result = List[String]()
        result.append("completed")
        result.append("failed")
        result.append("cancelled")
        result.append("timed_out")
        return result^

    @staticmethod
    def _contains_string(values: List[String], wanted: String) -> Bool:
        for value in values:
            if value == wanted:
                return True
        return False

    def run_retention(
        mut self,
        before: String,
        statuses: List[String] = List[String](),
        dry_run: Bool = True,
        keep_run_ids: List[String] = List[String](),
    ) raises SQLiteError -> RunRetentionPlan:
        """Select old terminal runs and optionally delete them atomically.

        Timestamps are compared by SQLite julianday, preserving ISO-8601
        offset semantics. Selection and output order are deterministic;
        dry runs never report deletion counts.
        """
        if before == "":
            raise SQLiteError(code=1, message="domain store: retention before must not be empty")
        var cutoff_check = self.db.query("SELECT julianday(?) IS NOT NULL")
        cutoff_check.bind_text(1, before)
        var cutoff_valid = cutoff_check.step() and cutoff_check.column_int(0) == 1
        cutoff_check.close()
        if not cutoff_valid:
            raise SQLiteError(code=1, message="domain store: retention before must be a valid timestamp")
        var selected = statuses.copy()
        if len(selected) == 0:
            selected = self._default_retention_statuses()
        for status in selected:
            if not self._contains_string(self._default_retention_statuses(), status):
                raise SQLiteError(code=1, message="domain store: retention status must be terminal")

        var plan = RunRetentionPlan(before, selected.copy(), dry_run)
        var stmt = self.db.query("SELECT id,status,created_at,updated_at,finished_at,COALESCE(finished_at,updated_at,created_at) FROM runs WHERE julianday(COALESCE(finished_at,updated_at,created_at)) < julianday(?) ORDER BY created_at ASC,id ASC")
        stmt.bind_text(1, before)
        var candidates = List[RunRetentionCandidate]()
        while stmt.step():
            var status = self._text(stmt, 1)
            var run_id = self._text(stmt, 0)
            if not self._contains_string(selected, status):
                continue
            if self._contains_string(keep_run_ids, run_id):
                continue
            candidates.append(RunRetentionCandidate(
                run_id, status, self._text(stmt, 2), self._text(stmt, 3), self._text(stmt, 4)
            )^)
        stmt.close()
        plan.candidate_count = len(candidates)
        for candidate in candidates:
            var item = RunRetentionItem(candidate)
            if not dry_run:
                item.row_counts = self.delete_run(candidate.run_id)
                item.deleted = item.row_counts.runs == 1
                if item.deleted:
                    plan.deleted_run_count += 1
                    plan.row_counts.accumulate(item.row_counts)
            plan.runs.append(item^)
        return plan^

    def maintain_journal(
        mut self,
        older_than_days: Float64,
        keep_last: Int = -1,
        vacuum: Bool = True,
        dry_run: Bool = True,
        reaction_root: String = "",
    ) raises SQLiteError -> JournalMaintenancePlan:
        """Run native retention, CAS garbage collection, and optional VACUUM."""
        if older_than_days < 0.0:
            raise SQLiteError(code=1, message="domain store: older_than_days must be non-negative")
        if keep_last < -1:
            raise SQLiteError(code=1, message="domain store: keep_last must be non-negative")

        var cutoff = self.db.query("SELECT datetime('now', '-' || ? || ' days')")
        cutoff.bind_real(1, older_than_days)
        if not cutoff.step():
            cutoff.close()
            raise SQLiteError(code=1, message="domain store: unable to derive maintenance cutoff")
        var before = cutoff.column_text(0)
        cutoff.close()

        var keep_run_ids = List[String]()
        if keep_last > 0:
            var recent = self.db.query("SELECT id,status FROM runs ORDER BY julianday(COALESCE(finished_at,updated_at,created_at)) DESC,id DESC")
            while recent.step() and len(keep_run_ids) < keep_last:
                var status = self._text(recent, 1)
                if self._contains_string(self._default_retention_statuses(), status):
                    keep_run_ids.append(self._text(recent, 0))
            recent.close()

        var retention = self.run_retention(
            before, statuses=List[String](), dry_run=dry_run, keep_run_ids=keep_run_ids.copy()
        )
        var reaction_gc = ReactionGarbageCollectionPlan(dry_run)
        reaction_gc.reaction_root = reaction_root
        if reaction_root != "":
            try:
                var reaction_store = FileReactionStore(reaction_root)
                var referenced = self.referenced_reaction_digests()
                reaction_gc.referenced_count = len(referenced)
                var locations = reaction_store.list_blobs()
                var candidate_sizes = List[Int]()
                reaction_gc.blob_count = len(locations)
                for location in locations:
                    var blob_path = Path(location)
                    var digest = blob_path.name()
                    var is_referenced = False
                    for reference_digest in referenced:
                        if reference_digest == digest:
                            is_referenced = True
                            break
                    if not is_referenced:
                        reaction_gc.candidates.append(digest)
                        var blob_size = len(blob_path.read_bytes())
                        candidate_sizes.append(blob_size)
                        reaction_gc.bytes_reclaimable += blob_size
                    else:
                        reaction_gc.kept_count += 1
                reaction_gc.candidate_count = len(reaction_gc.candidates)
                if not dry_run:
                    reaction_gc.deleted = reaction_store.delete_blobs(reaction_gc.candidates.copy())
                    reaction_gc.deleted_count = len(reaction_gc.deleted)
                    for deleted_digest in reaction_gc.deleted:
                        for index in range(len(reaction_gc.candidates)):
                            if reaction_gc.candidates[index] == deleted_digest:
                                reaction_gc.bytes_reclaimed += candidate_sizes[index]
                                break
            except err:
                raise SQLiteError(code=1, message="domain store: reaction GC failed: " + String(err))

        var plan = JournalMaintenancePlan(
            older_than_days, keep_last, vacuum, dry_run, before, retention, reaction_gc
        )
        if vacuum and not dry_run:
            self.db.execute("VACUUM")
            plan.vacuumed = True
        return plan^

    def _require_run(mut self, run_id: String) raises SQLiteError:
        if run_id == "":
            raise SQLiteError(code=1, message="domain store: run_id must not be empty")
        var stmt = self.db.query("SELECT 1 FROM runs WHERE id=?")
        stmt.bind_text(1, run_id)
        var found = stmt.step()
        stmt.close()
        if not found:
            raise SQLiteError(code=1, message="domain store: unknown run")


    @staticmethod
    def _text(mut stmt: Statement, index: Int) raises SQLiteError -> String:
        if stmt.column_null(index):
            return String("")
        return stmt.column_text(index)
    def _domain_command_start(
        mut self, run_id: String, command_id: String, command_type: String,
        idempotency_key: String, payload: String, created_at: String,
        actor: String = "", correlation_id: String = "", causation_id: String = "",
    ) raises SQLiteError -> DomainCommandStart:
        self._require_run(run_id)
        if command_id == "" or command_type == "" or idempotency_key == "" or created_at == "":
            raise SQLiteError(code=1, message="domain store: command fields must not be empty")
        self.db.begin_immediate()
        try:
            var existing = self.db.query("SELECT run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
            existing.bind_text(1, run_id); existing.bind_text(2, idempotency_key)
            if existing.step():
                var prior = CommandRow(run_id=self._text(existing, 0), id=self._text(existing, 1), command_type=self._text(existing, 2), idempotency_key=self._text(existing, 3), actor=self._text(existing, 4), correlation_id=self._text(existing, 5), causation_id=self._text(existing, 6), payload=self._text(existing, 7), created_at=self._text(existing, 8))
                existing.close()
                if prior.command_type != command_type or prior.actor != actor or prior.correlation_id != correlation_id or prior.causation_id != causation_id or prior.payload != payload or prior.created_at != created_at:
                    raise SQLiteError(code=1, message="domain store: command idempotency conflict")
                self.db.commit()
                return DomainCommandStart(command=prior^, replayed=True)
            existing.close()
            var by_id = self.db.query("SELECT 1 FROM runtime_commands WHERE run_id=? AND id=?")
            by_id.bind_text(1, run_id); by_id.bind_text(2, command_id)
            if by_id.step():
                by_id.close()
                raise SQLiteError(code=1, message="domain store: command id already exists")
            by_id.close()
            var insert = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at) VALUES (?,?,?,?,?,?,?,?,?)")
            insert.bind_text(1, run_id); insert.bind_text(2, command_id); insert.bind_text(3, command_type); insert.bind_text(4, idempotency_key)
            _bind_nullable(insert, 5, actor); _bind_nullable(insert, 6, correlation_id); _bind_nullable(insert, 7, causation_id); insert.bind_text(8, payload); insert.bind_text(9, created_at); _ = insert.step(); insert.close()
            var command = CommandRow(run_id=run_id, id=command_id, command_type=command_type, idempotency_key=idempotency_key, actor=actor, correlation_id=correlation_id, causation_id=causation_id, payload=payload, created_at=created_at)
            return DomainCommandStart(command=command^, replayed=False)
        except err:
            self.db.rollback()
            raise err^

    def _append_domain_event_in_tx(mut self, command: CommandRow, item: EventInput) raises SQLiteError -> EventRow:
        if item.id == "" or item.event_type == "" or item.created_at == "" or item.schema_version < 1:
            raise SQLiteError(code=1, message="domain store: invalid event")
        var existing = self.db.query("SELECT run_id,sequence,id,event_type,schema_version,impulse_id,process_id,command_id,actor,correlation_id,causation_id,payload,created_at FROM runtime_events WHERE run_id=? AND id=?")
        existing.bind_text(1, command.run_id); existing.bind_text(2, item.id)
        if existing.step():
            var prior = EventRow(run_id=self._text(existing, 0), sequence=existing.column_int(1), id=self._text(existing, 2), event_type=self._text(existing, 3), schema_version=existing.column_int(4), impulse_id=self._text(existing, 5), process_id=self._text(existing, 6), command_id=self._text(existing, 7), actor=self._text(existing, 8), correlation_id=self._text(existing, 9), causation_id=self._text(existing, 10), payload=self._text(existing, 11), created_at=self._text(existing, 12))
            existing.close()
            var actor = item.actor if item.actor != "" else command.actor
            var correlation = item.correlation_id if item.correlation_id != "" else command.correlation_id
            var causation = item.causation_id if item.causation_id != "" else command.causation_id
            if prior.event_type != item.event_type or prior.impulse_id != item.impulse_id or prior.process_id != item.process_id or prior.command_id != command.id or prior.actor != actor or prior.correlation_id != correlation or prior.causation_id != causation or prior.payload != item.payload or prior.created_at != item.created_at or prior.schema_version != item.schema_version:
                raise SQLiteError(code=1, message="domain store: event idempotency conflict")
            return prior^
        existing.close()
        var next = self.db.query("SELECT COALESCE(MAX(sequence),0)+1 FROM runtime_events WHERE run_id=?")
        next.bind_text(1, command.run_id)
        if not next.step():
            next.close(); raise SQLiteError(code=1, message="domain store: event sequence unavailable")
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
    ) raises SQLiteError -> ImpulseAcceptanceResult:
        """Persist an impulse and its acceptance command/event as one unit.

        The idempotency key is run-scoped and becomes the durable command id;
        replays return the existing rows while any payload or identity mismatch
        fails before commit.  The impulse primary key is never overwritten.
        """
        if not row.is_valid() or idempotency_key == "" or created_at == "":
            raise SQLiteError(code=1, message="domain store: invalid impulse acceptance")
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
                    raise SQLiteError(code=1, message="domain store: impulse acceptance idempotency conflict")
                var prior_impulse = self.db.query("SELECT id,run_id,impulse_type,payload,metadata,created_at,updated_at FROM impulses WHERE run_id=? AND id=?")
                prior_impulse.bind_text(1, row.run_id); prior_impulse.bind_text(2, row.id)
                var stored_impulse = self._read_impulse(prior_impulse)
                if stored_impulse.id != row.id or stored_impulse.run_id != row.run_id or stored_impulse.impulse_type != row.impulse_type or stored_impulse.payload != row.payload or stored_impulse.metadata != row.metadata or stored_impulse.created_at != row.created_at or stored_impulse.updated_at != row.updated_at:
                    raise SQLiteError(code=1, message="domain store: impulse id already exists with different contents")
                var prior_event = self.db.query("SELECT run_id,sequence,id,event_type,schema_version,impulse_id,process_id,command_id,actor,correlation_id,causation_id,payload,created_at FROM runtime_events WHERE run_id=? AND id=?")
                prior_event.bind_text(1, row.run_id); prior_event.bind_text(2, event_id)
                if not prior_event.step():
                    raise SQLiteError(code=1, message="domain store: impulse acceptance event is missing")
                var stored_event = EventRow(run_id=self._text(prior_event, 0), sequence=prior_event.column_int(1), id=self._text(prior_event, 2), event_type=self._text(prior_event, 3), schema_version=prior_event.column_int(4), impulse_id=self._text(prior_event, 5), process_id=self._text(prior_event, 6), command_id=self._text(prior_event, 7), actor=self._text(prior_event, 8), correlation_id=self._text(prior_event, 9), causation_id=self._text(prior_event, 10), payload=self._text(prior_event, 11), created_at=self._text(prior_event, 12))
                if stored_event.event_type != event_type or stored_event.impulse_id != row.id or stored_event.command_id != stored_command.id or stored_event.payload != payload or stored_event.created_at != created_at:
                    raise SQLiteError(code=1, message="domain store: impulse acceptance event conflict")
                var replay_events = List[EventRow](); replay_events.append(stored_event^)
                self.db.commit()
                return ImpulseAcceptanceResult(impulse=stored_impulse^, command=stored_command^, events=replay_events^, replayed=True)
            var existing_id = self.db.query("SELECT id FROM runtime_commands WHERE run_id=? AND id=?")
            existing_id.bind_text(1, row.run_id); existing_id.bind_text(2, command_id)
            if existing_id.step():
                raise SQLiteError(code=1, message="domain store: impulse acceptance command id already exists")
            var existing_impulse = self.db.query("SELECT 1 FROM impulses WHERE run_id=? AND id=?")
            existing_impulse.bind_text(1, row.run_id); existing_impulse.bind_text(2, row.id)
            if existing_impulse.step():
                raise SQLiteError(code=1, message="domain store: impulse id already exists")
            var command = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at) VALUES (?,?,?,?,?,?,?,?,?)")
            command.bind_text(1, row.run_id); command.bind_text(2, command_id); command.bind_text(3, command_type); command.bind_text(4, idempotency_key)
            _bind_nullable(command, 5, actor); _bind_nullable(command, 6, correlation_id); _bind_nullable(command, 7, causation_id); command.bind_text(8, payload); command.bind_text(9, created_at); _ = command.step()
            var impulse = self.db.query("INSERT INTO impulses (run_id,id,impulse_type,payload,metadata,created_at,updated_at) VALUES (?,?,?,?,?,?,?)")
            impulse.bind_text(1, row.run_id); impulse.bind_text(2, row.id); impulse.bind_text(3, row.impulse_type); impulse.bind_text(4, row.payload); impulse.bind_text(5, row.metadata); impulse.bind_text(6, row.created_at); impulse.bind_text(7, row.updated_at); _ = impulse.step()
            var next = self.db.query("SELECT COALESCE(MAX(sequence),0)+1 FROM runtime_events WHERE run_id=?"); next.bind_text(1, row.run_id)
            if not next.step(): raise SQLiteError(code=1, message="domain store: unable to allocate impulse event sequence")
            var event = self.db.query("INSERT INTO runtime_events (run_id,sequence,id,event_type,schema_version,impulse_id,command_id,actor,correlation_id,causation_id,payload,created_at) VALUES (?,?,?, ?,1,?,?,?,?,?,?,?)")
            event.bind_text(1, row.run_id); event.bind_int(2, next.column_int(0)); event.bind_text(3, event_id); event.bind_text(4, event_type); event.bind_text(5, row.id); event.bind_text(6, command_id)
            _bind_nullable(event, 7, actor); _bind_nullable(event, 8, correlation_id); _bind_nullable(event, 9, causation_id); event.bind_text(10, payload); event.bind_text(11, created_at); _ = event.step()
            self.db.commit()
            var stored_events = List[EventRow]()
            var result_event = self.db.query("SELECT run_id,sequence,id,event_type,schema_version,impulse_id,process_id,command_id,actor,correlation_id,causation_id,payload,created_at FROM runtime_events WHERE run_id=? AND id=?")
            result_event.bind_text(1, row.run_id); result_event.bind_text(2, event_id)
            if not result_event.step(): raise SQLiteError(code=1, message="domain store: stored impulse event is missing")
            stored_events.append(EventRow(run_id=self._text(result_event, 0), sequence=result_event.column_int(1), id=self._text(result_event, 2), event_type=self._text(result_event, 3), schema_version=result_event.column_int(4), impulse_id=self._text(result_event, 5), process_id=self._text(result_event, 6), command_id=self._text(result_event, 7), actor=self._text(result_event, 8), correlation_id=self._text(result_event, 9), causation_id=self._text(result_event, 10), payload=self._text(result_event, 11), created_at=self._text(result_event, 12))^)
            var stored = Impulse(id=row.id, run_id=row.run_id, impulse_type=row.impulse_type, payload=row.payload, metadata=row.metadata, created_at=row.created_at, updated_at=row.updated_at)
            var command_row = CommandRow(run_id=row.run_id, id=command_id, command_type=command_type, idempotency_key=idempotency_key, actor=actor, correlation_id=correlation_id, causation_id=causation_id, payload=payload, created_at=created_at)
            return ImpulseAcceptanceResult(impulse=stored^, command=command_row^, events=stored_events^, replayed=False)
        except err:
            self.db.rollback()
            raise err^

    def put_impulse(mut self, row: Impulse) raises SQLiteError:
        if not row.is_valid():
            raise SQLiteError(code=1, message="domain store: invalid impulse")
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
            raise SQLiteError(code=1, message="domain store: put_impulse failed")

    def list_impulses(mut self, run_id: String) raises SQLiteError -> List[String]:
        self._require_run(run_id)
        var result = List[String]()
        var stmt = self.db.query("SELECT id,impulse_type,payload,metadata,created_at,updated_at FROM impulses WHERE run_id=? ORDER BY created_at ASC, id ASC")
        stmt.bind_text(1, run_id)
        while stmt.step():
            var item = "{\"id\":" + _quote(self._text(stmt, 0)) + ",\"run_id\":" + _quote(run_id) + ",\"impulse_type\":" + _quote(self._text(stmt, 1)) + ",\"payload\":" + self._text(stmt, 2) + ",\"metadata\":" + self._text(stmt, 3) + ",\"created_at\":" + _quote(self._text(stmt, 4)) + ",\"updated_at\":" + _quote(self._text(stmt, 5)) + "}"
            result.append(item^)
        return result^

    def put_impulse_type(mut self, row: ImpulseType) raises SQLiteError:
        if not row.is_valid():
            raise SQLiteError(code=1, message="domain store: invalid impulse type")
        self._require_run(row.run_id)
        self.db.begin()
        try:
            var stmt = self.db.query("INSERT INTO impulse_types (run_id,id,title,description,media_types,value_schema_json,metadata,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(run_id,id) DO UPDATE SET title=excluded.title,description=excluded.description,media_types=excluded.media_types,value_schema_json=excluded.value_schema_json,metadata=excluded.metadata,created_at=excluded.created_at,updated_at=excluded.updated_at")
            stmt.bind_text(1, row.run_id); stmt.bind_text(2, row.id); _bind_nullable(stmt, 3, row.title); _bind_nullable(stmt, 4, row.description)
            stmt.bind_text(5, row.media_types); stmt.bind_text(6, row.value_schema); stmt.bind_text(7, row.metadata); stmt.bind_text(8, row.created_at); stmt.bind_text(9, row.updated_at)
            _ = stmt.step(); self.db.commit()
        except err:
            self.db.rollback()
            raise SQLiteError(code=1, message="domain store: put_impulse_type failed")

    def list_impulse_types(mut self, run_id: String) raises SQLiteError -> List[String]:
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
    ) raises SQLiteError -> CommandSubmission:
        if command_type != "impulse_type.register":
            raise SQLiteError(code=1, message="register_impulse_type requires command_type 'impulse_type.register'")
        if not row.is_valid(): raise SQLiteError(code=1, message="domain store: invalid impulse type")
        var start = self._domain_command_start(row.run_id, command_id, command_type, idempotency_key, row.to_json(), created_at, actor, correlation_id, causation_id)
        var command = start.command.copy()
        var stored_events = List[EventRow]()
        if start.replayed:
            var prior = self.get_impulse_type(row.run_id, row.id)
            if prior.to_json() != row.to_json(): raise SQLiteError(code=1, message="domain store: impulse type idempotency conflict")
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
    ) raises SQLiteError -> CommandSubmission:
        if command_type != "impulse_relation.record": raise SQLiteError(code=1, message="record_impulse_relation requires command_type 'impulse_relation.record'")
        if not row.is_valid(): raise SQLiteError(code=1, message="domain store: invalid impulse relation")
        var start = self._domain_command_start(row.run_id, command_id, command_type, idempotency_key, row.to_json(), created_at, actor, correlation_id, causation_id)
        var command = start.command.copy(); var stored_events = List[EventRow]()
        if start.replayed:
            var prior = self.get_impulse_relation(row.run_id, row.id)
            if prior.to_json() != row.to_json(): raise SQLiteError(code=1, message="domain store: impulse relation idempotency conflict")
            return CommandSubmission(command=command^, events=stored_events^, replayed=True)
        try:
            var existing = self.db.query("SELECT 1 FROM impulse_relations WHERE run_id=? AND id=?"); existing.bind_text(1, row.run_id); existing.bind_text(2, row.id)
            if existing.step(): existing.close(); raise SQLiteError(code=1, message="domain store: impulse relation already exists")
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
    ) raises SQLiteError -> CommandSubmission:
        if command_type != "association.record": raise SQLiteError(code=1, message="record_association requires command_type 'association.record'")
        if not row.is_valid(): raise SQLiteError(code=1, message="domain store: invalid association")
        var start = self._domain_command_start(row.run_id, command_id, command_type, idempotency_key, row.to_json(), created_at, actor, correlation_id, causation_id)
        var command = start.command.copy(); var stored_events = List[EventRow]()
        if start.replayed:
            var prior = self.get_association(row.run_id, row.id)
            if prior.to_json() != row.to_json(): raise SQLiteError(code=1, message="domain store: association idempotency conflict")
            return CommandSubmission(command=command^, events=stored_events^, replayed=True)
        try:
            var existing = self.db.query("SELECT 1 FROM associations WHERE run_id=? AND id=?"); existing.bind_text(1, row.run_id); existing.bind_text(2, row.id)
            if existing.step(): existing.close(); raise SQLiteError(code=1, message="domain store: association already exists")
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
    ) raises SQLiteError -> CommandSubmission:
        if command_type != "reaction.record": raise SQLiteError(code=1, message="record_reaction requires command_type 'reaction.record'")
        if not row.is_valid(): raise SQLiteError(code=1, message="domain store: invalid reaction")
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
            if prior.to_json() != normalized.to_json(): raise SQLiteError(code=1, message="domain store: reaction idempotency conflict")
            return CommandSubmission(command=command^, events=stored_events^, replayed=True)
        try:
            var existing = self.db.query("SELECT 1 FROM reactions WHERE run_id=? AND id=?"); existing.bind_text(1, normalized.run_id); existing.bind_text(2, normalized.id)
            if existing.step(): existing.close(); raise SQLiteError(code=1, message="domain store: reaction already exists")
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
    ) raises SQLiteError -> CommandSubmission:
        if command_type != "homeostat.save" and command_type != "homeostat.open":
            raise SQLiteError(code=1, message="save_homeostat requires command_type 'homeostat.save' or 'homeostat.open'")
        if not row.is_valid(): raise SQLiteError(code=1, message="domain store: invalid homeostat")
        var start = self._domain_command_start(row.run_id, command_id, command_type, idempotency_key, row.to_json(), created_at, actor, correlation_id, causation_id)
        var command = start.command.copy()
        var stored_events = List[EventRow]()
        if start.replayed:
            var prior = self.get_homeostat(row.run_id, row.id)
            if prior.to_json() != row.to_json(): raise SQLiteError(code=1, message="domain store: homeostat idempotency conflict")
            return CommandSubmission(command=command^, events=stored_events^, replayed=True)
        try:
            var existing = self.db.query("SELECT 1 FROM homeostats WHERE run_id=? AND id=?")
            existing.bind_text(1, row.run_id); existing.bind_text(2, row.id)
            if existing.step():
                existing.close()
                raise SQLiteError(code=1, message="domain store: homeostat already exists")
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
    ) raises SQLiteError -> HomeostatTransitionResult:
        if row.status != "completed" and row.status != "cancelled" and row.status != "expired":
            raise SQLiteError(code=1, message="domain store: invalid homeostat terminal status")
        var expected = "homeostat.complete"
        if row.status == "cancelled": expected = "homeostat.cancel"
        elif row.status == "expired": expected = "homeostat.expire"
        if command_type != expected:
            raise SQLiteError(code=1, message="transition_homeostat requires command_type '" + expected + "'")
        if not row.is_valid(): raise SQLiteError(code=1, message="domain store: invalid homeostat")
        var payload = row.to_json()
        var start = self._domain_command_start(row.run_id, command_id, command_type, idempotency_key, payload, created_at, actor, correlation_id, causation_id)
        var command = start.command.copy()
        var stored_events = List[EventRow]()
        if start.replayed:
            var replayed = self.get_homeostat(row.run_id, row.id)
            return HomeostatTransitionResult(homeostat=replayed^, submission=CommandSubmission(command=command^, events=stored_events^, replayed=True))
        try:
            var current = self.get_homeostat(row.run_id, row.id)
            if current.status != "open": raise SQLiteError(code=1, message="domain store: homeostat is not open")
            var at = row.updated_at
            if at == "": at = created_at
            var update = self.db.query("UPDATE homeostats SET status=?,values_json=?,updated_at=? WHERE run_id=? AND id=? AND status='open'")
            update.bind_text(1, row.status); update.bind_text(2, row.values); update.bind_text(3, at); update.bind_text(4, row.run_id); update.bind_text(5, row.id); _ = update.step(); update.close()
            if self.db.changes() != 1: raise SQLiteError(code=1, message="domain store: homeostat transition lost ownership")
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
    ) raises SQLiteError -> CommandSubmission:
        if command_type != "projection.save":
            raise SQLiteError(code=1, message="save_projection requires command_type 'projection.save'")
        if not row.is_valid(): raise SQLiteError(code=1, message="domain store: invalid projection")
        var start = self._domain_command_start(row.run_id, command_id, command_type, idempotency_key, row.to_json(), created_at, actor, correlation_id, causation_id)
        var command = start.command.copy()
        var stored_events = List[EventRow]()
        if start.replayed:
            var prior = self.get_projection(row.run_id, row.name)
            if prior.id != row.id or prior.run_id != row.run_id or prior.name != row.name or prior.version != row.version or prior.data != row.data or prior.source_event_sequence != row.source_event_sequence or prior.updated_at != row.updated_at:
                raise SQLiteError(code=1, message="domain store: projection idempotency conflict")
            return CommandSubmission(command=command^, events=stored_events^, replayed=True)
        try:
            var insert = self.db.query("INSERT INTO projections (run_id,name,id,version,data,source_event_sequence,updated_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(run_id,name) DO UPDATE SET id=excluded.id,version=excluded.version,data=excluded.data,source_event_sequence=excluded.source_event_sequence,updated_at=excluded.updated_at")
            insert.bind_text(1, row.run_id); insert.bind_text(2, row.name); insert.bind_text(3, row.id); insert.bind_int(4, row.version); insert.bind_text(5, row.data); insert.bind_int(6, row.source_event_sequence); insert.bind_text(7, row.updated_at); _ = insert.step(); insert.close()
            for item in events: stored_events.append(self._append_domain_event_in_tx(command, item)^)
            self.db.commit()
            return CommandSubmission(command=command^, events=stored_events^, replayed=False)
        except err:
            self.db.rollback(); raise err^

    def rebuild_projections_with_command(
        mut self, run_id: String, names: List[String], command_id: String,
        command_type: String, idempotency_key: String, created_at: String,
        updated_at: String = "", events: List[EventInput] = List[EventInput](),
        actor: String = "", correlation_id: String = "", causation_id: String = "",
    ) raises SQLiteError -> ProjectionRebuildResult:
        if command_type != "projection.rebuild":
            raise SQLiteError(code=1, message="rebuild_projections_with_command requires command_type 'projection.rebuild'")
        self._require_run(run_id)
        var requested = List[String]()
        if len(names) == 0: requested.append("run_summary")
        else:
            for name in names:
                if name == "": raise SQLiteError(code=1, message="domain store: projection name must not be empty")
                if name != "run_summary": raise SQLiteError(code=1, message="Unknown projection rebuild name: " + name)
                var duplicate = False
                for prior in requested:
                    if prior == name: duplicate = True
                if not duplicate: requested.append(name)
        var payload = "{\"names\":["
        var first = True
        for name in requested:
            if not first: payload += ","
            first = False
            payload += _quote(name)
        payload += "]}"
        var start = self._domain_command_start(run_id, command_id, command_type, idempotency_key, payload, created_at, actor, correlation_id, causation_id)
        var command = start.command.copy()
        var result = List[Projection]()
        var stored_events = List[EventRow]()
        if start.replayed:
            for name in requested: result.append(self.get_projection(run_id, name)^)
            return ProjectionRebuildResult(projections=result^, submission=CommandSubmission(command=command^, events=stored_events^, replayed=True))
        try:
            for item in events: stored_events.append(self._append_domain_event_in_tx(command, item)^)
            var latest = self.db.query("SELECT COALESCE(MAX(sequence),0) FROM runtime_events WHERE run_id=?")
            latest.bind_text(1, run_id)
            if not latest.step(): raise SQLiteError(code=1, message="domain store: unable to read event watermark")
            var watermark = latest.column_int(0); latest.close()
            var effective_at = updated_at
            if effective_at == "": effective_at = created_at
            if effective_at == "":
                raise SQLiteError(code=2, message="domain store: projection rebuild timestamp must not be empty")
            for name in requested:
                var data = self._run_summary_data(run_id, watermark)
                var insert = self.db.query("INSERT INTO projections (run_id,name,id,version,data,source_event_sequence,updated_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(run_id,name) DO UPDATE SET data=excluded.data,source_event_sequence=excluded.source_event_sequence,updated_at=excluded.updated_at")
                insert.bind_text(1, run_id); insert.bind_text(2, name); insert.bind_text(3, name + ":" + run_id); insert.bind_int(4, 1); insert.bind_text(5, data); insert.bind_int(6, watermark); insert.bind_text(7, effective_at); _ = insert.step(); insert.close()
                result.append(self.get_projection(run_id, name)^)
            self.db.commit()
            return ProjectionRebuildResult(projections=result^, submission=CommandSubmission(command=command^, events=stored_events^, replayed=False))
        except err:
            self.db.rollback(); raise err^
    def put_impulse_relation(mut self, row: ImpulseRelation) raises SQLiteError:
        if not row.is_valid():
            raise SQLiteError(code=1, message="domain store: invalid impulse relation")
        self._require_run(row.run_id)
        self.db.begin()
        try:
            var stmt = self.db.query("INSERT INTO impulse_relations (run_id,id,relation_type,source_impulse_id,target_impulse_id,metadata,created_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(run_id,id) DO UPDATE SET relation_type=excluded.relation_type,source_impulse_id=excluded.source_impulse_id,target_impulse_id=excluded.target_impulse_id,metadata=excluded.metadata,created_at=excluded.created_at")
            stmt.bind_text(1, row.run_id); stmt.bind_text(2, row.id); stmt.bind_text(3, row.relation_type)
            stmt.bind_text(4, row.source_impulse_id); stmt.bind_text(5, row.target_impulse_id); stmt.bind_text(6, row.metadata); stmt.bind_text(7, row.created_at)
            _ = stmt.step(); self.db.commit()
        except err:
            self.db.rollback()
            raise SQLiteError(code=1, message="domain store: put_impulse_relation failed")

    def list_impulse_relations(mut self, run_id: String) raises SQLiteError -> List[String]:
        self._require_run(run_id)
        var result = List[String]()
        var stmt = self.db.query("SELECT id,relation_type,source_impulse_id,target_impulse_id,metadata,created_at FROM impulse_relations WHERE run_id=? ORDER BY created_at ASC, id ASC")
        stmt.bind_text(1, run_id)
        while stmt.step():
            var item = "{\"id\":" + _quote(self._text(stmt, 0)) + ",\"run_id\":" + _quote(run_id) + ",\"relation_type\":" + _quote(self._text(stmt, 1)) + ",\"source_impulse_id\":" + _quote(self._text(stmt, 2)) + ",\"target_impulse_id\":" + _quote(self._text(stmt, 3)) + ",\"metadata\":" + self._text(stmt, 4) + ",\"created_at\":" + _quote(self._text(stmt, 5)) + "}"
            result.append(item^)
        return result^

    def put_association(mut self, row: Association) raises SQLiteError:
        if not row.is_valid():
            raise SQLiteError(code=1, message="domain store: invalid association")
        self._require_run(row.run_id)
        self.db.begin()
        try:
            var stmt = self.db.query("INSERT INTO associations (run_id,id,kind,impulse_id,values_json,metadata,created_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(run_id,id) DO UPDATE SET kind=excluded.kind,impulse_id=excluded.impulse_id,values_json=excluded.values_json,metadata=excluded.metadata,created_at=excluded.created_at")
            stmt.bind_text(1, row.run_id); stmt.bind_text(2, row.id); stmt.bind_text(3, row.kind); _bind_nullable(stmt, 4, row.impulse_id)
            stmt.bind_text(5, row.values); stmt.bind_text(6, row.metadata); stmt.bind_text(7, row.created_at)
            _ = stmt.step(); self.db.commit()
        except err:
            self.db.rollback()
            raise SQLiteError(code=1, message="domain store: put_association failed")

    def list_associations(mut self, run_id: String) raises SQLiteError -> List[String]:
        self._require_run(run_id)
        var result = List[String]()
        var stmt = self.db.query("SELECT id,kind,impulse_id,values_json,metadata,created_at FROM associations WHERE run_id=? ORDER BY created_at ASC, id ASC")
        stmt.bind_text(1, run_id)
        while stmt.step():
            var item = "{\"id\":" + _quote(self._text(stmt, 0)) + ",\"run_id\":" + _quote(run_id) + ",\"kind\":" + _quote(self._text(stmt, 1)) + ",\"impulse_id\":" + _nullable_json(stmt, 2) + ",\"values\":" + self._text(stmt, 3) + ",\"metadata\":" + self._text(stmt, 4) + ",\"created_at\":" + _quote(self._text(stmt, 5)) + "}"
            result.append(item^)
        return result^

    def put_reaction(mut self, row: Reaction) raises SQLiteError:
        if not row.is_valid():
            raise SQLiteError(code=1, message="domain store: invalid reaction")
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
                raise SQLiteError(code=1, message="domain store: reaction id already exists with different data")
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
            raise SQLiteError(code=1, message="domain store: put_reaction failed")

    def list_reactions(mut self, run_id: String) raises SQLiteError -> List[String]:
        self._require_run(run_id)
        var result = List[String]()
        var stmt = self.db.query("SELECT id,kind,uri,impulse_id,media_type,size_bytes,content_hash,metadata,created_at FROM reactions WHERE run_id=? ORDER BY created_at ASC, id ASC")
        stmt.bind_text(1, run_id)
        while stmt.step():
            var item = "{\"id\":" + _quote(self._text(stmt, 0)) + ",\"run_id\":" + _quote(run_id) + ",\"kind\":" + _quote(self._text(stmt, 1)) + ",\"uri\":" + _quote(self._text(stmt, 2)) + ",\"impulse_id\":" + _nullable_json(stmt, 3) + ",\"media_type\":" + _nullable_json(stmt, 4) + ",\"size_bytes\":" + ("null" if stmt.column_null(5) else String(stmt.column_int(5))) + ",\"content_hash\":" + _nullable_json(stmt, 6) + ",\"metadata\":" + self._text(stmt, 7) + ",\"created_at\":" + _quote(self._text(stmt, 8)) + "}"
            result.append(item^)
        return result^

    def put_homeostat(mut self, row: Homeostat) raises SQLiteError:
        if not row.is_valid():
            raise SQLiteError(code=1, message="domain store: invalid homeostat")
        self._require_run(row.run_id)
        var existing = self.db.query("SELECT kind,impulse_id,status,values_json,metadata,attempt,max_attempts,created_at,updated_at FROM homeostats WHERE run_id=? AND id=?")
        existing.bind_text(1, row.run_id); existing.bind_text(2, row.id)
        if existing.step():
            var conflict = self._text(existing, 0) != row.kind or self._text(existing, 1) != row.impulse_id or self._text(existing, 2) != row.status or self._text(existing, 3) != row.values or self._text(existing, 4) != row.metadata or existing.column_int(5) != row.attempt or existing.column_int(6) != row.max_attempts or self._text(existing, 7) != row.created_at or self._text(existing, 8) != row.updated_at
            existing.close()
            if conflict:
                raise SQLiteError(code=1, message="domain store: homeostat id already exists with different data")
            return
        existing.close()
        self.db.begin()
        try:
            var stmt = self.db.query("INSERT INTO homeostats (run_id,id,kind,impulse_id,status,values_json,metadata,attempt,max_attempts,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)")
            stmt.bind_text(1, row.run_id); stmt.bind_text(2, row.id); stmt.bind_text(3, row.kind); _bind_nullable(stmt, 4, row.impulse_id); stmt.bind_text(5, row.status); stmt.bind_text(6, row.values); stmt.bind_text(7, row.metadata); stmt.bind_int(8, row.attempt); stmt.bind_int(9, row.max_attempts); stmt.bind_text(10, row.created_at); stmt.bind_text(11, row.updated_at)
            _ = stmt.step(); self.db.commit()
        except err:
            self.db.rollback()
            raise SQLiteError(code=1, message="domain store: put_homeostat failed")

    def list_homeostats(mut self, run_id: String) raises SQLiteError -> List[String]:
        self._require_run(run_id)
        var result = List[String]()
        var stmt = self.db.query("SELECT id,kind,impulse_id,status,values_json,metadata,attempt,max_attempts,created_at,updated_at FROM homeostats WHERE run_id=? ORDER BY updated_at ASC, id ASC")
        stmt.bind_text(1, run_id)
        while stmt.step():
            var item = "{\"id\":" + _quote(self._text(stmt, 0)) + ",\"run_id\":" + _quote(run_id) + ",\"kind\":" + _quote(self._text(stmt, 1)) + ",\"impulse_id\":" + _nullable_json(stmt, 2) + ",\"status\":" + _quote(self._text(stmt, 3)) + ",\"values\":" + self._text(stmt, 4) + ",\"metadata\":" + self._text(stmt, 5) + ",\"attempt\":" + String(stmt.column_int(6)) + ",\"max_attempts\":" + String(stmt.column_int(7)) + ",\"created_at\":" + _quote(self._text(stmt, 8)) + ",\"updated_at\":" + _quote(self._text(stmt, 9)) + "}"
            result.append(item^)
        return result^


    def put_projection(mut self, row: Projection) raises SQLiteError:
        if not row.is_valid():
            raise SQLiteError(code=1, message="domain store: invalid projection")
        self._require_run(row.run_id)
        self.db.begin()
        try:
            var stmt = self.db.query("INSERT INTO projections (run_id,name,id,version,data,source_event_sequence,updated_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(run_id,name) DO UPDATE SET id=excluded.id,version=excluded.version,data=excluded.data,source_event_sequence=excluded.source_event_sequence,updated_at=excluded.updated_at")
            stmt.bind_text(1, row.run_id); stmt.bind_text(2, row.name); stmt.bind_text(3, row.id); stmt.bind_int(4, row.version); stmt.bind_text(5, row.data); stmt.bind_int(6, row.source_event_sequence); stmt.bind_text(7, row.updated_at)
            _ = stmt.step(); self.db.commit()
        except err:
            self.db.rollback()
            raise SQLiteError(code=1, message="domain store: put_projection failed")

    def list_projections(mut self, run_id: String) raises SQLiteError -> List[String]:
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
    def put_bridge_delivery(mut self, row: BridgeDelivery) raises SQLiteError -> BridgeDelivery:
        if not row.is_valid():
            raise SQLiteError(code=1, message="domain store: invalid bridge delivery")
        self._validate_bridge_refs(row)
        self._require_run(row.run_id)
        return self._upsert_bridge_row("bridge_outbox", row)
    @staticmethod
    def _bridge_enqueue_payload(row: BridgeDelivery, include_status: Bool = False) -> String:
        var payload = "{\"delivery_id\":" + _quote(row.id) + ",\"impulse_id\":" + _quote(row.impulse.id) + ",\"source\":" + row.source.to_json() + ",\"target\":" + row.target.to_json() + ",\"pool_id\":"
        if row.pool_id == "": payload += "null"
        else: payload += _quote(row.pool_id)
        payload += ",\"budget\":" + row.budget.to_json()
        if not (row.event_ref.runtime.id == "runtime" and row.event_ref.run_id == "run" and row.event_ref.event_id == "" and row.event_ref.sequence == 0):
            payload += ",\"event_ref\":" + row.event_ref.to_json()
        if include_status:
            payload += ",\"status\":" + _quote(row.status) + ",\"attempts\":" + String(row.attempts)
        payload += "}"
        return payload

    def enqueue_bridge_delivery(
        mut self, row: BridgeDelivery, idempotency_key: String = "",
        actor: String = "", correlation_id: String = "", causation_id: String = "",
    ) raises SQLiteError -> BridgeEnqueueResult:
        """Atomically enqueue outbox, impulse, command, and event rows."""
        var delivery = row.copy()
        var delivery_key = idempotency_key if idempotency_key != "" else delivery.idempotency_key
        if delivery_key == "": raise SQLiteError(code=1, message="domain store: bridge enqueue idempotency key must not be empty")
        delivery.idempotency_key = delivery_key
        delivery.run_id = delivery.source.run_id
        if not delivery.is_valid() or delivery.status != "pending":
            raise SQLiteError(code=1, message="domain store: bridge enqueue requires a valid pending delivery")
        self._validate_bridge_refs(delivery, "bridge_outbox")
        self._require_run(delivery.run_id)
        if delivery.updated_at == "": delivery.updated_at = delivery.created_at
        if delivery.updated_at == "": raise SQLiteError(code=1, message="domain store: bridge enqueue updated_at must not be empty")
        self._validate_bridge_budget(delivery, next_attempt=False)
        var command_payload = String("")
        var event_payload = String("")
        try:
            command_payload = canonical_json_text(self._bridge_enqueue_payload(delivery))
            event_payload = canonical_json_text(self._bridge_enqueue_payload(delivery, True))
        except err:
            raise SQLiteError(code=1, message="domain store: bridge enqueue payload is invalid JSON")
        self.db.begin_immediate()
        try:
            var existing = self.db.query("SELECT run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
            existing.bind_text(1, delivery.run_id); existing.bind_text(2, delivery_key)
            if existing.step():
                var stored = CommandRow(run_id=self._text(existing,0), id=self._text(existing,1), command_type=self._text(existing,2), idempotency_key=self._text(existing,3), actor=self._text(existing,4), correlation_id=self._text(existing,5), causation_id=self._text(existing,6), payload=self._text(existing,7), created_at=self._text(existing,8))
                existing.close()
                if stored.command_type != "bridge.outbox.enqueue" or stored.actor != actor or stored.correlation_id != correlation_id or stored.causation_id != causation_id or stored.created_at != delivery.updated_at or not self._bridge_json_equal(stored.payload, command_payload):
                    raise SQLiteError(code=1, message="domain store: bridge idempotency conflict")
                var lookup = self.db.query("SELECT json_extract(?, '$.delivery_id')")
                lookup.bind_text(1, stored.payload)
                if not lookup.step() or lookup.column_text(0) == "":
                    lookup.close(); raise SQLiteError(code=1, message="domain store: bridge enqueue command identity is invalid")
                var stored_id = lookup.column_text(0); lookup.close()
                var replayed = self._get_bridge("bridge_outbox", delivery.run_id, stored_id)
                self.db.commit()
                return BridgeEnqueueResult(delivery=replayed^, submission=CommandSubmission(command=stored^, events=List[EventRow](), replayed=True))
            existing.close()
            var target_seen_stmt = self.db.query("SELECT 1 FROM bridge_outbox WHERE run_id=? AND json_extract(target_ref,'$.run_id')=? LIMIT 1")
            target_seen_stmt.bind_text(1, delivery.run_id); target_seen_stmt.bind_text(2, delivery.target.run_id)
            var target_seen = target_seen_stmt.step(); target_seen_stmt.close()
            if delivery.budget.spawned_runs_limited and not target_seen:
                var spawned = self.db.query("SELECT COUNT(DISTINCT json_extract(target_ref,'$.run_id')) FROM bridge_outbox WHERE run_id=? AND json_extract(target_ref,'$.run_id') IS NOT NULL")
                spawned.bind_text(1, delivery.run_id)
                if spawned.step() and spawned.column_int(0) >= delivery.budget.spawned_runs:
                    spawned.close(); raise SQLiteError(code=1, message="domain store: bridge spawned_runs budget exhausted")
                spawned.close()
            var impulse_check = self.db.query("SELECT run_id,impulse_type,payload,metadata,created_at,updated_at FROM impulses WHERE run_id=? AND id=?")
            impulse_check.bind_text(1, delivery.impulse.run_id); impulse_check.bind_text(2, delivery.impulse.id)
            var impulse_exists = impulse_check.step()
            if impulse_exists and (self._text(impulse_check,0) != delivery.impulse.run_id or self._text(impulse_check,1) != delivery.impulse.impulse_type or self._text(impulse_check,2) != delivery.impulse.payload or self._text(impulse_check,3) != delivery.impulse.metadata or self._text(impulse_check,4) != delivery.impulse.created_at or self._text(impulse_check,5) != delivery.impulse.updated_at):
                impulse_check.close(); raise SQLiteError(code=1, message="domain store: bridge impulse idempotency conflict")
            impulse_check.close()
            var source_json = delivery.source.to_json(); var target_json = delivery.target.to_json(); var impulse_json = delivery.impulse.to_json(); var event_ref_json = ""
            if not (delivery.event_ref.runtime.id == "runtime" and delivery.event_ref.run_id == "run" and delivery.event_ref.event_id == "" and delivery.event_ref.sequence == 0): event_ref_json = delivery.event_ref.to_json()
            var outbox = self.db.query("INSERT INTO bridge_outbox (run_id,id,idempotency_key,source_ref,target_ref,impulse_json,event_ref,pool_id,budget,status,attempts,metadata,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            outbox.bind_text(1,delivery.run_id); outbox.bind_text(2,delivery.id); outbox.bind_text(3,delivery_key); outbox.bind_text(4,source_json); outbox.bind_text(5,target_json); outbox.bind_text(6,impulse_json)
            if event_ref_json == "": outbox.bind_null(7)
            else: outbox.bind_text(7,event_ref_json)
            _bind_nullable(outbox,8,delivery.pool_id); outbox.bind_text(9,delivery.budget.to_json()); outbox.bind_text(10,"pending"); outbox.bind_int(11,delivery.attempts); outbox.bind_text(12,delivery.metadata); outbox.bind_text(13,delivery.created_at); outbox.bind_text(14,delivery.updated_at); _ = outbox.step(); outbox.close()
            if not impulse_exists:
                var impulse_insert = self.db.query("INSERT INTO impulses (run_id,id,impulse_type,payload,metadata,created_at,updated_at) VALUES (?,?,?,?,?,?,?)")
                impulse_insert.bind_text(1,delivery.impulse.run_id); impulse_insert.bind_text(2,delivery.impulse.id); impulse_insert.bind_text(3,delivery.impulse.impulse_type); impulse_insert.bind_text(4,delivery.impulse.payload); impulse_insert.bind_text(5,delivery.impulse.metadata); impulse_insert.bind_text(6,delivery.impulse.created_at); impulse_insert.bind_text(7,delivery.impulse.updated_at); _ = impulse_insert.step(); impulse_insert.close()
            var command = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at) VALUES (?,?,?,?,?,?,?,?,?)")
            command.bind_text(1,delivery.run_id); command.bind_text(2,delivery_key); command.bind_text(3,"bridge.outbox.enqueue"); command.bind_text(4,delivery_key); _bind_nullable(command,5,actor); _bind_nullable(command,6,correlation_id); _bind_nullable(command,7,causation_id); command.bind_text(8,command_payload); command.bind_text(9,delivery.updated_at); _ = command.step(); command.close()
            var command_row = CommandRow(run_id=delivery.run_id,id=delivery_key,command_type="bridge.outbox.enqueue",idempotency_key=delivery_key,actor=actor,correlation_id=correlation_id,causation_id=causation_id,payload=command_payload,created_at=delivery.updated_at)
            var event_input = EventInput(delivery_key + ":event", "bridge.outbox.enqueued", event_payload, delivery.updated_at, delivery.impulse.id, "", 1, actor, correlation_id, causation_id)
            var stored_event = self._append_domain_event_in_tx(command_row, event_input)
            var stored_events = List[EventRow](); stored_events.append(stored_event^)
            self.db.commit()
            var saved = self._get_bridge("bridge_outbox", delivery.run_id, delivery.id)
            return BridgeEnqueueResult(delivery=saved^, submission=CommandSubmission(command=command_row^, events=stored_events^, replayed=False))
        except err:
            self.db.rollback(); raise SQLiteError(code=1, message="domain store: bridge enqueue failed: " + String(err))

    def list_bridge_deliveries(mut self, run_id: String) raises SQLiteError -> List[String]:
        self._require_run(run_id)
        var result = List[String]()
        var stmt = self.db.query("SELECT id,idempotency_key,source_ref,target_ref,impulse_json,event_ref,pool_id,budget,status,attempts,metadata,created_at,updated_at FROM bridge_outbox WHERE run_id=? ORDER BY updated_at ASC, id ASC")
        stmt.bind_text(1, run_id)
        while stmt.step():
            var event_ref = self._text(stmt, 5)
            if event_ref == "": event_ref = "null"
            var pool_id = self._text(stmt, 6)
            var pool_json = "null"
            if pool_id != "": pool_json = _quote(pool_id)
            var item = "{\"id\":" + _quote(self._text(stmt, 0)) + ",\"run_id\":" + _quote(run_id) + ",\"idempotency_key\":" + _quote(self._text(stmt, 1)) + ",\"source\":" + self._text(stmt, 2) + ",\"target\":" + self._text(stmt, 3) + ",\"impulse\":" + self._text(stmt, 4) + ",\"event_ref\":" + event_ref + ",\"pool_id\":" + pool_json + ",\"budget\":" + self._text(stmt, 7) + ",\"status\":" + _quote(self._text(stmt, 8)) + ",\"attempts\":" + String(stmt.column_int(9)) + ",\"metadata\":" + self._text(stmt, 10) + ",\"created_at\":" + _quote(self._text(stmt, 11)) + ",\"updated_at\":" + _quote(self._text(stmt, 12)) + "}"
            result.append(item^)
        return result^

    @staticmethod
    def _validate_bridge_refs(row: BridgeDelivery, table: String = "bridge_outbox") raises SQLiteError:
        # Outbox rows are authored by their source run. Inbox rows are stored
        # under the target run, while source references remain remote identity.
        if table != "bridge_inbox" and table != "bridge_outbox":
            raise SQLiteError(code=1, message="domain store: invalid bridge table")
        if table == "bridge_inbox":
            if row.target.run_id != row.run_id:
                raise SQLiteError(code=1, message="domain store: bridge target run mismatch")
            return
        if row.source.run_id != row.run_id:
            raise SQLiteError(code=1, message="domain store: bridge source run mismatch")
        if row.impulse.run_id != row.run_id:
            raise SQLiteError(code=1, message="domain store: bridge impulse run mismatch")
        if not (row.event_ref.runtime.id == "runtime" and row.event_ref.run_id == "run" and row.event_ref.event_id == "" and row.event_ref.sequence == 0):
            if row.event_ref.run_id != row.run_id:
                raise SQLiteError(code=1, message="domain store: bridge event run mismatch")

    def put_inbox_delivery(mut self, row: BridgeDelivery) raises SQLiteError -> BridgeDelivery:
        if not row.is_valid():
            raise SQLiteError(code=1, message="domain store: invalid bridge delivery")
        self._validate_bridge_refs(row, "bridge_inbox")
        self._require_run(row.run_id)
        return self._upsert_bridge_row("bridge_inbox", row)
    @staticmethod
    def _validate_bridge_budget(row: BridgeDelivery, next_attempt: Bool = True) raises SQLiteError:
        if row.budget.runtime_hops_limited and row.budget.runtime_hops < 1:
            raise SQLiteError(code=1, message="domain store: bridge runtime hop budget exhausted")
        if row.budget.impulse_count_limited and row.budget.impulse_count < 1:
            raise SQLiteError(code=1, message="domain store: bridge impulse budget exhausted")
        if next_attempt and row.budget.attempts_limited and row.attempts + 1 > row.budget.attempts:
            raise SQLiteError(code=1, message="domain store: bridge attempts budget exhausted")

    @staticmethod
    def _normalize_imported_impulse(row: BridgeDelivery) raises SQLiteError -> Impulse:
        var metadata_value: Value
        try:
            metadata_value = Value(parse_string=row.impulse.metadata)
        except err:
            raise SQLiteError(code=1, message="domain store: imported impulse metadata must be a JSON object")
        if not metadata_value.is_object():
            raise SQLiteError(code=1, message="domain store: imported impulse metadata must be a JSON object")
        var metadata = metadata_value.object().copy()
        metadata["source_runtime_id"] = Value(row.source.runtime.id)
        metadata["source_run_id"] = Value(row.source.run_id)
        metadata["source_impulse_id"] = Value(row.impulse.id)
        var metadata_json: String
        try:
            metadata_json = canonical_json_text(to_string(Value(metadata^)))
        except err:
            raise SQLiteError(code=1, message="domain store: imported impulse metadata serialization failed")
        return Impulse(row.impulse.id, row.run_id, row.impulse.impulse_type, row.impulse.payload, metadata_json, row.impulse.created_at, row.impulse.updated_at)^


    def import_bridge_delivery(mut self, row: BridgeDelivery, idempotency_key: String = "") raises SQLiteError -> BridgeDelivery:
        """Normalize a remote delivery into a local inbox transaction."""
        var imported = row.copy()
        var delivery_key = idempotency_key if idempotency_key != "" else imported.idempotency_key
        if delivery_key == "": raise SQLiteError(code=1, message="domain store: bridge import idempotency key must not be empty")
        imported.idempotency_key = delivery_key
        imported.run_id = imported.target.run_id
        imported.status = "imported"
        if imported.updated_at == "": imported.updated_at = imported.created_at
        if imported.updated_at == "": raise SQLiteError(code=1, message="domain store: bridge import updated_at must not be empty")
        return self.import_inbox_delivery(imported)
    def import_inbox_delivery(mut self, row: BridgeDelivery) raises SQLiteError -> BridgeDelivery:
        """Atomically import an inbox row, consuming bridge delivery budget."""
        if not row.is_valid() or row.status != "imported":
            raise SQLiteError(code=1, message="domain store: inbox import requires valid imported delivery")
        self._validate_bridge_refs(row, "bridge_inbox")
        self._require_run(row.run_id)
        var imported_impulse = self._normalize_imported_impulse(row)
        self.db.begin_immediate()
        try:
            var collision = self.db.query("SELECT impulse_type,payload,metadata,created_at,updated_at FROM impulses WHERE run_id=? AND id=?")
            collision.bind_text(1, imported_impulse.run_id); collision.bind_text(2, imported_impulse.id)
            if collision.step():
                if self._text(collision, 0) != imported_impulse.impulse_type or self._text(collision, 1) != imported_impulse.payload or self._text(collision, 2) != imported_impulse.metadata or self._text(collision, 3) != imported_impulse.created_at or self._text(collision, 4) != imported_impulse.updated_at:
                    collision.close()
                    raise SQLiteError(code=1, message="domain store: bridge impulse idempotency conflict")
            collision.close()
            var existing = self.db.query("SELECT id,idempotency_key,source_ref,target_ref,impulse_json,pool_id,status FROM bridge_inbox WHERE run_id=? AND (id=? OR idempotency_key=?) ORDER BY CASE WHEN id=? THEN 0 ELSE 1 END LIMIT 1")
            existing.bind_text(1, row.run_id); existing.bind_text(2, row.id); existing.bind_text(3, row.idempotency_key); existing.bind_text(4, row.id)
            var has_existing = existing.step()
            var prior_id = String("")
            var prior_key = String("")
            var prior_source = String("")
            var prior_target = String("")
            var prior_impulse = String("")
            var prior_pool = String("")
            var prior_status = String("")
            if has_existing:
                prior_id = self._text(existing, 0); prior_key = self._text(existing, 1); prior_source = self._text(existing, 2); prior_target = self._text(existing, 3); prior_impulse = self._text(existing, 4); prior_pool = self._text(existing, 5); prior_status = self._text(existing, 6)
            existing.close()
            if has_existing and (prior_id != row.id or prior_key != row.idempotency_key or not self._bridge_json_equal(prior_source, row.source.to_json()) or not self._bridge_json_equal(prior_target, row.target.to_json()) or prior_pool != row.pool_id):
                raise SQLiteError(code=1, message="domain store: bridge idempotency conflict")
            if has_existing and prior_status == "imported":
                if not self._bridge_json_equal(prior_impulse, imported_impulse.to_json()):
                    raise SQLiteError(code=1, message="domain store: bridge idempotency conflict")
                self.db.commit()
                return self._get_bridge("bridge_inbox", row.run_id, prior_id)
            if has_existing and not bridge_status_transition_allowed(prior_status, row.status):
                raise SQLiteError(code=1, message="domain store: invalid bridge status transition")
            self._validate_bridge_budget(row, next_attempt=True)
            var next_budget = RuntimeBudget()
            try:
                next_budget = row.budget.consume(runtime_hops=1, impulse_count=1)
            except err:
                raise SQLiteError(code=1, message="domain store: bridge budget exhausted")
            var next_attempts = row.attempts + 1
            var source_json = row.source.to_json()
            var target_json = row.target.to_json()
            var impulse_json = imported_impulse.to_json()
            var event_json = ""
            if not (row.event_ref.runtime.id == "runtime" and row.event_ref.run_id == "run" and row.event_ref.event_id == "" and row.event_ref.sequence == 0):
                event_json = row.event_ref.to_json()
            var budget_json = next_budget.to_json()
            if has_existing:
                var update = self.db.query("UPDATE bridge_inbox SET impulse_json=?,event_ref=?,budget=?,status='imported',attempts=?,metadata=?,updated_at=? WHERE run_id=? AND id=?")
                update.bind_text(1, impulse_json)
                if event_json == "": update.bind_null(2)
                else: update.bind_text(2, event_json)
                update.bind_text(3, budget_json); update.bind_int(4, next_attempts); update.bind_text(5, row.metadata); update.bind_text(6, row.updated_at); update.bind_text(7, row.run_id); update.bind_text(8, prior_id); _ = update.step(); update.close()
                if self.db.changes() != 1: raise SQLiteError(code=1, message="domain store: inbox import lost ownership")
            else:
                var insert = self.db.query("INSERT INTO bridge_inbox (run_id,id,idempotency_key,source_ref,target_ref,impulse_json,event_ref,pool_id,budget,status,attempts,metadata,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
                insert.bind_text(1, row.run_id); insert.bind_text(2, row.id); insert.bind_text(3, row.idempotency_key); insert.bind_text(4, source_json); insert.bind_text(5, target_json); insert.bind_text(6, impulse_json)
                if event_json == "": insert.bind_null(7)
                else: insert.bind_text(7, event_json)
                _bind_nullable(insert, 8, row.pool_id); insert.bind_text(9, budget_json); insert.bind_text(10, row.status); insert.bind_int(11, next_attempts); insert.bind_text(12, row.metadata); insert.bind_text(13, row.created_at); insert.bind_text(14, row.updated_at); _ = insert.step(); insert.close()
            var impulse = self.db.query("INSERT INTO impulses (run_id,id,impulse_type,payload,metadata,created_at,updated_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(run_id,id) DO NOTHING")
            impulse.bind_text(1, imported_impulse.run_id); impulse.bind_text(2, imported_impulse.id); impulse.bind_text(3, imported_impulse.impulse_type); impulse.bind_text(4, imported_impulse.payload); impulse.bind_text(5, imported_impulse.metadata); impulse.bind_text(6, imported_impulse.created_at); impulse.bind_text(7, imported_impulse.updated_at); _ = impulse.step(); impulse.close()
            self._record_bridge_journal(row.run_id, row.idempotency_key, "bridge.inbox.import", self._bridge_operation_payload(row.id, "imported", next_attempts), "bridge.inbox.imported", row.updated_at, imported_impulse.id)
            self.db.commit()
        except err:
            self.db.rollback()
            var detail = String(err)
            if detail.find("idempotency conflict") >= 0 or detail.find("status transition") >= 0 or detail.find("budget") >= 0:
                raise err^
            raise SQLiteError(code=1, message="domain store: inbox import failed")
        return self._get_bridge("bridge_inbox", row.run_id, row.id)

    @staticmethod
    def _bridge_json_equal(left: String, right: String) raises SQLiteError -> Bool:
        var lhs = left
        var rhs = right
        if lhs == "": lhs = "null"
        if rhs == "": rhs = "null"
        try:
            return canonical_json_text(lhs) == canonical_json_text(rhs)
        except err:
            raise SQLiteError(code=1, message="domain store: invalid bridge JSON")


    def _record_bridge_journal(mut self, run_id: String, command_id: String, command_type: String, payload: String, event_type: String, created_at: String, impulse_id: String = "") raises SQLiteError:
        """Append a bridge command/event pair in the caller's transaction.

        Bridge operations use the transport idempotency key as the durable
        command identity. Replays compare the immutable operation payload and
        repair a missing event without creating a second sequence number.
        """
        if command_id == "" or command_type == "" or payload == "" or event_type == "" or created_at == "":
            raise SQLiteError(code=1, message="domain store: bridge journal fields must not be empty")
        var command = self.db.query("SELECT id,command_type,idempotency_key,payload FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
        command.bind_text(1, run_id); command.bind_text(2, command_id)
        var stored_id = command_id
        var stored_type = command_type
        var stored_payload = payload
        var command_exists = command.step()
        if command_exists:
            stored_id = self._text(command, 0)
            stored_type = self._text(command, 1)
            stored_payload = self._text(command, 3)
            command.close()
            if stored_type != command_type or not self._bridge_json_equal(stored_payload, payload):
                raise SQLiteError(code=1, message="domain store: bridge idempotency conflict")
        else:
            command.close()
            var insert = self.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,payload,created_at) VALUES (?,?,?,?,?,?)")
            insert.bind_text(1, run_id); insert.bind_text(2, command_id); insert.bind_text(3, command_type); insert.bind_text(4, command_id); insert.bind_text(5, payload); insert.bind_text(6, created_at)
            _ = insert.step(); insert.close()
        var event_id = stored_id + ":event"
        var prior_event = self.db.query("SELECT event_type,impulse_id,command_id,payload FROM runtime_events WHERE run_id=? AND id=?")
        prior_event.bind_text(1, run_id); prior_event.bind_text(2, event_id)
        if prior_event.step():
            var prior_type = self._text(prior_event, 0)
            var prior_impulse = self._text(prior_event, 1)
            var prior_command = self._text(prior_event, 2)
            var prior_payload = self._text(prior_event, 3)
            prior_event.close()
            if prior_type != event_type or prior_impulse != impulse_id or prior_command != stored_id or not self._bridge_json_equal(prior_payload, payload):
                raise SQLiteError(code=1, message="domain store: bridge event idempotency conflict")
            return
        prior_event.close()
        var next = self.db.query("SELECT COALESCE(MAX(sequence),0)+1 FROM runtime_events WHERE run_id=?")
        next.bind_text(1, run_id)
        if not next.step():
            next.close()
            raise SQLiteError(code=1, message="domain store: bridge event sequence unavailable")
        var sequence = next.column_int(0)
        var event = self.db.query("INSERT INTO runtime_events (run_id,sequence,id,event_type,schema_version,impulse_id,command_id,payload,created_at) VALUES (?,?,?, ?,1,?,?,?,?)")
        event.bind_text(1, run_id); event.bind_int(2, sequence); event.bind_text(3, event_id); event.bind_text(4, event_type)
        if impulse_id == "": event.bind_null(5)
        else: event.bind_text(5, impulse_id)
        event.bind_text(6, stored_id); event.bind_text(7, payload); event.bind_text(8, created_at)
        _ = event.step(); event.close()

    @staticmethod
    def _bridge_operation_payload(delivery_id: String, status: String, attempts: Int = -1) -> String:
        var payload = "{\"delivery_id\":" + _quote(delivery_id) + ",\"status\":" + _quote(status)
        if attempts >= 0: payload += ",\"attempts\":" + String(attempts)
        payload += "}"
        return payload

    def _upsert_bridge_row(mut self, table: String, row: BridgeDelivery) raises SQLiteError -> BridgeDelivery:
        self._validate_bridge_refs(row, table)
        if row.attempts < 0:
            raise SQLiteError(code=1, message="domain store: invalid bridge attempts")
        if table != "bridge_inbox" and table != "bridge_outbox":
            raise SQLiteError(code=1, message="domain store: invalid bridge table")
        var source_json = row.source.to_json()
        var target_json = row.target.to_json()
        var impulse_json = row.impulse.to_json()
        var event_json = ""
        if not (row.event_ref.runtime.id == "runtime" and row.event_ref.run_id == "run" and row.event_ref.event_id == "" and row.event_ref.sequence == 0):
            event_json = row.event_ref.to_json()
        var budget_json = row.budget.to_json()
        self.db.begin()
        var failure = ""
        try:
            # Check both unique identities before any mutation.  A duplicate key
            # is a replay only when every immutable payload field is identical.
            var existing = self.db.query("SELECT id,idempotency_key,source_ref,target_ref,impulse_json,event_ref,pool_id,budget,status,attempts,metadata,created_at,updated_at FROM " + table + " WHERE run_id=? AND (id=? OR idempotency_key=?) ORDER BY CASE WHEN id=? THEN 0 ELSE 1 END LIMIT 1")
            existing.bind_text(1, row.run_id); existing.bind_text(2, row.id); existing.bind_text(3, row.idempotency_key); existing.bind_text(4, row.id)
            if existing.step():
                var prior_id = self._text(existing, 0)
                var prior_key = self._text(existing, 1)
                var prior_status = self._text(existing, 8)
                var prior_attempts = existing.column_int(9)
                var prior_source = self._text(existing, 2)
                var prior_target = self._text(existing, 3)
                var prior_impulse = self._text(existing, 4)
                var prior_event = self._text(existing, 5)
                var prior_pool = self._text(existing, 6)
                var prior_budget = self._text(existing, 7)
                var prior_metadata = self._text(existing, 10)
                var same_payload = prior_id == row.id and prior_key == row.idempotency_key and self._bridge_json_equal(prior_source, source_json) and self._bridge_json_equal(prior_target, target_json) and self._bridge_json_equal(prior_impulse, impulse_json) and self._bridge_json_equal(prior_event, event_json) and prior_pool == row.pool_id and self._bridge_json_equal(prior_budget, budget_json) and self._bridge_json_equal(prior_metadata, row.metadata)
                existing.close()
                if not same_payload:
                    self.db.rollback(); raise SQLiteError(code=1, message="domain store: bridge idempotency conflict")
                if not bridge_status_transition_allowed(prior_status, row.status):
                    self.db.rollback(); raise SQLiteError(code=1, message="domain store: invalid bridge status transition")
                var next_attempts = prior_attempts
                if row.attempts > next_attempts: next_attempts = row.attempts
                if prior_status != row.status or next_attempts > prior_attempts:
                    var update = self.db.query("UPDATE " + table + " SET status=?,attempts=?,updated_at=? WHERE run_id=? AND id=?")
                    update.bind_text(1, row.status); update.bind_int(2, next_attempts); update.bind_text(3, row.updated_at); update.bind_text(4, row.run_id); update.bind_text(5, prior_id); _ = update.step(); update.close()
                self.db.commit()
                return self._get_bridge(table, row.run_id, prior_id)
            existing.close()
            var stmt = self.db.query("INSERT INTO " + table + " (run_id,id,idempotency_key,source_ref,target_ref,impulse_json,event_ref,pool_id,budget,status,attempts,metadata,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            stmt.bind_text(1, row.run_id); stmt.bind_text(2, row.id); stmt.bind_text(3, row.idempotency_key); stmt.bind_text(4, source_json); stmt.bind_text(5, target_json); stmt.bind_text(6, impulse_json)
            if event_json == "": stmt.bind_null(7)
            else: stmt.bind_text(7, event_json)
            _bind_nullable(stmt, 8, row.pool_id); stmt.bind_text(9, budget_json); stmt.bind_text(10, row.status); stmt.bind_int(11, row.attempts); stmt.bind_text(12, row.metadata); stmt.bind_text(13, row.created_at); stmt.bind_text(14, row.updated_at)
            _ = stmt.step(); stmt.close(); self.db.commit()
        except err:
            self.db.rollback()
            raise SQLiteError(code=1, message="domain store: bridge delivery write failed")
        if failure == "conflict":
            raise SQLiteError(code=1, message="domain store: bridge idempotency conflict")
        if failure == "transition":
            raise SQLiteError(code=1, message="domain store: invalid bridge status transition")
        return self._get_bridge(table, row.run_id, row.id)

    @staticmethod
    def _read_impulse(mut stmt: Statement) raises SQLiteError -> Impulse:
        if not stmt.step():
            stmt.close()
            raise SQLiteError(code=1, message="domain store: impulse not found")
        var row = Impulse(id=NativeDomainStore._text(stmt, 0), run_id=NativeDomainStore._text(stmt, 1), impulse_type=NativeDomainStore._text(stmt, 2), payload=NativeDomainStore._text(stmt, 3), metadata=NativeDomainStore._text(stmt, 4), created_at=NativeDomainStore._text(stmt, 5), updated_at=NativeDomainStore._text(stmt, 6))
        stmt.close()
        return row^

    @staticmethod
    def _read_impulse_type(mut stmt: Statement) raises SQLiteError -> ImpulseType:
        if not stmt.step():
            stmt.close()
            raise SQLiteError(code=1, message="domain store: impulse type not found")
        var row = ImpulseType(id=NativeDomainStore._text(stmt, 0), run_id=NativeDomainStore._text(stmt, 1), title=NativeDomainStore._text(stmt, 2), description=NativeDomainStore._text(stmt, 3), media_types=NativeDomainStore._text(stmt, 4), value_schema=NativeDomainStore._text(stmt, 5), metadata=NativeDomainStore._text(stmt, 6), created_at=NativeDomainStore._text(stmt, 7), updated_at=NativeDomainStore._text(stmt, 8))
        stmt.close()
        return row^

    @staticmethod
    def _read_impulse_relation(mut stmt: Statement) raises SQLiteError -> ImpulseRelation:
        if not stmt.step():
            stmt.close()
            raise SQLiteError(code=1, message="domain store: impulse relation not found")
        var row = ImpulseRelation(id=NativeDomainStore._text(stmt, 0), run_id=NativeDomainStore._text(stmt, 1), relation_type=NativeDomainStore._text(stmt, 2), source_impulse_id=NativeDomainStore._text(stmt, 3), target_impulse_id=NativeDomainStore._text(stmt, 4), metadata=NativeDomainStore._text(stmt, 5), created_at=NativeDomainStore._text(stmt, 6))
        stmt.close()
        return row^

    @staticmethod
    def _read_association(mut stmt: Statement) raises SQLiteError -> Association:
        if not stmt.step():
            stmt.close()
            raise SQLiteError(code=1, message="domain store: association not found")
        var row = Association(id=NativeDomainStore._text(stmt, 0), run_id=NativeDomainStore._text(stmt, 1), kind=NativeDomainStore._text(stmt, 2), impulse_id=NativeDomainStore._text(stmt, 3), values=NativeDomainStore._text(stmt, 4), metadata=NativeDomainStore._text(stmt, 5), created_at=NativeDomainStore._text(stmt, 6))
        stmt.close()
        return row^

    @staticmethod
    def _read_reaction(mut stmt: Statement) raises SQLiteError -> Reaction:
        if not stmt.step():
            stmt.close()
            raise SQLiteError(code=1, message="domain store: reaction not found")
        var row = Reaction(id=NativeDomainStore._text(stmt, 0), run_id=NativeDomainStore._text(stmt, 1), kind=NativeDomainStore._text(stmt, 2), uri=NativeDomainStore._text(stmt, 3), impulse_id=NativeDomainStore._text(stmt, 4), media_type=NativeDomainStore._text(stmt, 5), size_bytes=(-1 if stmt.column_null(6) else stmt.column_int(6)), content_hash=NativeDomainStore._text(stmt, 7), metadata=NativeDomainStore._text(stmt, 8), created_at=NativeDomainStore._text(stmt, 9))
        stmt.close()
        return row^

    @staticmethod
    def _read_homeostat(mut stmt: Statement) raises SQLiteError -> Homeostat:
        if not stmt.step():
            stmt.close()
            raise SQLiteError(code=1, message="domain store: homeostat not found")
        var row = Homeostat(id=NativeDomainStore._text(stmt, 0), run_id=NativeDomainStore._text(stmt, 1), kind=NativeDomainStore._text(stmt, 2), impulse_id=NativeDomainStore._text(stmt, 3), status=NativeDomainStore._text(stmt, 4), values=NativeDomainStore._text(stmt, 5), metadata=NativeDomainStore._text(stmt, 6), attempt=stmt.column_int(7), max_attempts=stmt.column_int(8), created_at=NativeDomainStore._text(stmt, 9), updated_at=NativeDomainStore._text(stmt, 10))
        stmt.close()
        return row^

    @staticmethod
    def _read_projection(mut stmt: Statement) raises SQLiteError -> Projection:
        if not stmt.step():
            stmt.close()
            raise SQLiteError(code=1, message="domain store: projection not found")
        var stale = False
        if stmt.column_int(7) != 0: stale = True
        var row = Projection(id=NativeDomainStore._text(stmt, 0), run_id=NativeDomainStore._text(stmt, 1), name=NativeDomainStore._text(stmt, 2), version=stmt.column_int(3), data=NativeDomainStore._text(stmt, 4), source_event_sequence=stmt.column_int(5), updated_at=NativeDomainStore._text(stmt, 6), stale=stale)
        stmt.close()
        return row^

    @staticmethod
    def _read_bridge(mut stmt: Statement) raises SQLiteError -> BridgeDelivery:
        if not stmt.step():
            stmt.close()
            raise SQLiteError(code=1, message="domain store: bridge delivery not found")
        var run_id = NativeDomainStore._text(stmt, 0)
        var source = RunRef(RuntimeRef(NativeDomainStore._text(stmt, 14), NativeDomainStore._text(stmt, 15), NativeDomainStore._text(stmt, 36)), NativeDomainStore._text(stmt, 18))
        var target = RunRef(RuntimeRef(NativeDomainStore._text(stmt, 16), NativeDomainStore._text(stmt, 17), NativeDomainStore._text(stmt, 37)), NativeDomainStore._text(stmt, 19))
        var impulse = Impulse(NativeDomainStore._text(stmt, 20), NativeDomainStore._text(stmt, 40), NativeDomainStore._text(stmt, 21), NativeDomainStore._text(stmt, 34), NativeDomainStore._text(stmt, 35), NativeDomainStore._text(stmt, 22), NativeDomainStore._text(stmt, 23))
        var event_ref = EventRef(RuntimeRef("runtime", "", "{}"), "run", "", 0)
        if not stmt.column_null(6):
            event_ref = EventRef(RuntimeRef(NativeDomainStore._text(stmt, 24), NativeDomainStore._text(stmt, 38), NativeDomainStore._text(stmt, 39)), NativeDomainStore._text(stmt, 25), NativeDomainStore._text(stmt, 26), stmt.column_int(27))
        var budget = RuntimeBudget(runtime_hops=stmt.column_int(28), spawned_runs=stmt.column_int(29), impulse_count=stmt.column_int(30), wall_time_seconds=stmt.column_int(31), attempts=stmt.column_int(32), reaction_bytes=stmt.column_int(33), runtime_hops_limited=not stmt.column_null(28), spawned_runs_limited=not stmt.column_null(29), impulse_count_limited=not stmt.column_null(30), wall_time_seconds_limited=not stmt.column_null(31), attempts_limited=not stmt.column_null(32), reaction_bytes_limited=not stmt.column_null(33))
        var row = BridgeDelivery(NativeDomainStore._text(stmt, 1), run_id, NativeDomainStore._text(stmt, 2), source^, target^, impulse^, event_ref^, NativeDomainStore._text(stmt, 7), budget^, NativeDomainStore._text(stmt, 9), stmt.column_int(10), NativeDomainStore._text(stmt, 11), NativeDomainStore._text(stmt, 12), NativeDomainStore._text(stmt, 13))
        stmt.close()
        return row^
    def get_impulse(mut self, run_id: String, impulse_id: String) raises SQLiteError -> Impulse:
        self._require_run(run_id)
        var stmt = self.db.query("SELECT id,run_id,impulse_type,payload,metadata,created_at,updated_at FROM impulses WHERE run_id=? AND id=?")
        stmt.bind_text(1, run_id); stmt.bind_text(2, impulse_id)
        return self._read_impulse(stmt)

    def list_impulse_records(mut self, run_id: String, impulse_type: String = "", limit: Int = -1) raises SQLiteError -> List[Impulse]:
        if limit < -1: raise SQLiteError(code=1, message="domain store: limit must be non-negative")
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

    def get_impulse_type(mut self, run_id: String, impulse_type_id: String) raises SQLiteError -> ImpulseType:
        self._require_run(run_id)
        var stmt = self.db.query("SELECT id,run_id,title,description,media_types,value_schema_json,metadata,created_at,updated_at FROM impulse_types WHERE run_id=? AND id=?")
        stmt.bind_text(1, run_id); stmt.bind_text(2, impulse_type_id)
        return self._read_impulse_type(stmt)

    def list_impulse_type_records(mut self, run_id: String, limit: Int = -1) raises SQLiteError -> List[ImpulseType]:
        if limit < -1: raise SQLiteError(code=1, message="domain store: limit must be non-negative")
        self._require_run(run_id)
        var sql = "SELECT id,run_id,title,description,media_types,value_schema_json,metadata,created_at,updated_at FROM impulse_types WHERE run_id=? ORDER BY id ASC"
        if limit >= 0: sql += " LIMIT ?"
        var stmt = self.db.query(sql); stmt.bind_text(1, run_id)
        if limit >= 0: stmt.bind_int(2, limit)
        var result = List[ImpulseType]()
        while stmt.step(): result.append(ImpulseType(id=self._text(stmt, 0), run_id=self._text(stmt, 1), title=self._text(stmt, 2), description=self._text(stmt, 3), media_types=self._text(stmt, 4), value_schema=self._text(stmt, 5), metadata=self._text(stmt, 6), created_at=self._text(stmt, 7), updated_at=self._text(stmt, 8))^)
        stmt.close()
        return result^

    def get_impulse_relation(mut self, run_id: String, relation_id: String) raises SQLiteError -> ImpulseRelation:
        self._require_run(run_id)
        var stmt = self.db.query("SELECT id,run_id,relation_type,source_impulse_id,target_impulse_id,metadata,created_at FROM impulse_relations WHERE run_id=? AND id=?")
        stmt.bind_text(1, run_id); stmt.bind_text(2, relation_id)
        return self._read_impulse_relation(stmt)

    def list_impulse_relation_records(mut self, run_id: String, impulse_id: String = "", relation_type: String = "", limit: Int = -1) raises SQLiteError -> List[ImpulseRelation]:
        if limit < -1: raise SQLiteError(code=1, message="domain store: limit must be non-negative")
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

    def get_association(mut self, run_id: String, association_id: String) raises SQLiteError -> Association:
        self._require_run(run_id)
        var stmt = self.db.query("SELECT id,run_id,kind,impulse_id,values_json,metadata,created_at FROM associations WHERE run_id=? AND id=?")
        stmt.bind_text(1, run_id); stmt.bind_text(2, association_id)
        return self._read_association(stmt)

    def list_association_records(mut self, run_id: String, impulse_id: String = "", kind: String = "", limit: Int = -1) raises SQLiteError -> List[Association]:
        self._require_run(run_id)
        if limit < -1: raise SQLiteError(code=1, message="domain store: limit must be non-negative")
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

    def get_reaction(mut self, run_id: String, reaction_id: String) raises SQLiteError -> Reaction:
        self._require_run(run_id)
        var stmt = self.db.query("SELECT id,run_id,kind,uri,impulse_id,media_type,size_bytes,content_hash,metadata,created_at FROM reactions WHERE run_id=? AND id=?")
        stmt.bind_text(1, run_id); stmt.bind_text(2, reaction_id)
        return self._read_reaction(stmt)

    def list_reaction_records(mut self, run_id: String, impulse_id: String = "", kind: String = "", limit: Int = -1) raises SQLiteError -> List[Reaction]:
        self._require_run(run_id)
        if limit < -1: raise SQLiteError(code=1, message="domain store: limit must be non-negative")
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
    def referenced_reaction_digests(mut self) raises SQLiteError -> List[String]:
        """Return normalized valid CAS digests from content hashes and URIs."""
        var result = List[String]()
        var stmt = self.db.query("SELECT content_hash,uri FROM reactions ORDER BY id ASC")
        while stmt.step():
            var digest = ""
            if not stmt.column_null(0):
                var candidate = self._text(stmt, 0)
                if candidate.startswith("sha256:") and candidate.byte_length() == 71:
                    try:
                        var suffix = ""
                        for index in range(7, candidate.byte_length()): suffix += String(candidate[byte=index])
                        digest = reaction_digest_or_empty("fala-reaction://sha256/" + suffix)
                    except:
                        digest = ""
            if digest == "":
                try:
                    digest = reaction_digest_or_empty(self._text(stmt, 1))
                except:
                    digest = ""
            if digest == "": continue
            var duplicate = False
            for prior in result:
                if prior == digest:
                    duplicate = True
                    break
            if not duplicate: result.append(digest)
        stmt.close()
        return result^

    def collect_reaction_garbage(mut self, reaction_root: String, run_id: String = "", dry_run: Bool = True) raises SQLiteError -> ReactionGarbageCollectionPlan:
        """Plan or delete unreferenced local CAS reaction blobs."""
        if reaction_root == "":
            raise SQLiteError(code=2, message="argument_error: --reaction-root is required")
        if run_id != "": self._require_run(run_id)
        var plan = ReactionGarbageCollectionPlan(dry_run)
        plan.reaction_root = reaction_root
        if run_id != "":
            plan.run_ids.append(run_id)
        else:
            var selected = self.db.query("SELECT id FROM runs ORDER BY id ASC")
            while selected.step(): plan.run_ids.append(self._text(selected, 0))
            selected.close()
        var all_runs = self.db.query("SELECT id FROM runs ORDER BY id ASC")
        while all_runs.step(): plan.scanned_run_ids.append(self._text(all_runs, 0))
        all_runs.close()
        try:
            var reaction_store = FileReactionStore(reaction_root)
            var referenced = self.referenced_reaction_digests()
            plan.referenced_count = len(referenced)
            var locations = reaction_store.list_blobs()
            var candidate_sizes = List[Int]()
            plan.blob_count = len(locations)
            for location in locations:
                var blob_path = Path(location)
                var digest = blob_path.name()
                var is_referenced = False
                for reference_digest in referenced:
                    if reference_digest == digest:
                        is_referenced = True
                        break
                if is_referenced:
                    plan.kept_count += 1
                else:
                    plan.collectable.append(digest)
                    try:
                        var blob_size = len(blob_path.read_bytes())
                        candidate_sizes.append(blob_size)
                        plan.bytes_reclaimable += blob_size
                    except err:
                        raise SQLiteError(code=1, message="domain store: reaction GC failed to measure blob bytes")
            plan.candidates = plan.collectable.copy()
            plan.candidate_count = len(plan.collectable)
            if not dry_run:
                plan.deleted = reaction_store.delete_blobs(plan.collectable.copy())
                plan.deleted_count = len(plan.deleted)
                for deleted_digest in plan.deleted:
                    for index in range(len(plan.collectable)):
                        if plan.collectable[index] == deleted_digest:
                            plan.bytes_reclaimed += candidate_sizes[index]
                            break
            return plan^
        except err:
            raise SQLiteError(code=1, message="domain store: reaction GC failed: " + String(err))

    def get_homeostat(mut self, run_id: String, homeostat_id: String) raises SQLiteError -> Homeostat:
        self._require_run(run_id)
        var stmt = self.db.query("SELECT id,run_id,kind,impulse_id,status,values_json,metadata,attempt,max_attempts,created_at,updated_at FROM homeostats WHERE run_id=? AND id=?")
        stmt.bind_text(1, run_id); stmt.bind_text(2, homeostat_id)
        return self._read_homeostat(stmt)

    def list_homeostat_records(mut self, run_id: String, impulse_id: String = "", status: String = "", limit: Int = -1) raises SQLiteError -> List[Homeostat]:
        self._require_run(run_id)
        if limit < -1: raise SQLiteError(code=1, message="domain store: limit must be non-negative")
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

    def get_projection(mut self, run_id: String, name: String) raises SQLiteError -> Projection:
        self._require_run(run_id)
        var stmt = self.db.query("SELECT p.id,p.run_id,p.name,p.version,p.data,p.source_event_sequence,p.updated_at,CASE WHEN EXISTS (SELECT 1 FROM runtime_events e WHERE e.run_id=p.run_id AND e.sequence>p.source_event_sequence) THEN 1 ELSE 0 END FROM projections p WHERE p.run_id=? AND p.name=?")
        stmt.bind_text(1, run_id); stmt.bind_text(2, name)
        if not stmt.step():
            stmt.close()
            raise SQLiteError(code=1, message="domain store: projection not found")
        var stale = False
        if stmt.column_int(7) != 0: stale = True
        var row = Projection(id=self._text(stmt, 0), run_id=self._text(stmt, 1), name=self._text(stmt, 2), version=stmt.column_int(3), data=self._text(stmt, 4), source_event_sequence=stmt.column_int(5), updated_at=self._text(stmt, 6), stale=stale)
        stmt.close()
        return row^

    def list_projection_records(mut self, run_id: String, name: String = "", limit: Int = -1) raises SQLiteError -> List[Projection]:
        self._require_run(run_id)
        if limit < -1: raise SQLiteError(code=1, message="domain store: limit must be non-negative")
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

    def list_bridge_inbox(mut self, run_id: String, status: String = "", limit: Int = -1) raises SQLiteError -> List[String]:
        self._require_run(run_id)
        if limit < -1: raise SQLiteError(code=1, message="domain store: limit must be non-negative")
        var sql = "SELECT id,idempotency_key,source_ref,target_ref,impulse_json,event_ref,pool_id,budget,status,attempts,metadata,created_at,updated_at FROM bridge_inbox WHERE run_id=?"
        if status != "": sql += " AND status=?"
        sql += " ORDER BY updated_at ASC, id ASC"
        if limit >= 0: sql += " LIMIT ?"
        var stmt = self.db.query(sql); stmt.bind_text(1, run_id)
        var index = 2
        if status != "": stmt.bind_text(index, status); index += 1
        if limit >= 0: stmt.bind_int(index, limit)
        var result = List[String]()
        while stmt.step():
            var event_ref = self._text(stmt, 5)
            if event_ref == "": event_ref = "null"
            var pool_id = self._text(stmt, 6)
            var pool_json = "null"
            if pool_id != "": pool_json = _quote(pool_id)
            var item = "{\"id\":" + _quote(self._text(stmt, 0)) + ",\"run_id\":" + _quote(run_id) + ",\"idempotency_key\":" + _quote(self._text(stmt, 1)) + ",\"source\":" + self._text(stmt, 2) + ",\"target\":" + self._text(stmt, 3) + ",\"impulse\":" + self._text(stmt, 4) + ",\"event_ref\":" + event_ref + ",\"pool_id\":" + pool_json + ",\"budget\":" + self._text(stmt, 7) + ",\"status\":" + _quote(self._text(stmt, 8)) + ",\"attempts\":" + String(stmt.column_int(9)) + ",\"metadata\":" + self._text(stmt, 10) + ",\"created_at\":" + _quote(self._text(stmt, 11)) + ",\"updated_at\":" + _quote(self._text(stmt, 12)) + "}"
            result.append(item^)
        stmt.close()
        return result^
    @staticmethod
    def _bridge_select(table: String) -> String:
        return "SELECT run_id,id,idempotency_key,source_ref,target_ref,impulse_json,event_ref,pool_id,budget,status,attempts,metadata,created_at,updated_at,json_extract(source_ref,'$.runtime.id'),json_extract(source_ref,'$.runtime.uri'),json_extract(target_ref,'$.runtime.id'),json_extract(target_ref,'$.runtime.uri'),json_extract(source_ref,'$.run_id'),json_extract(target_ref,'$.run_id'),json_extract(impulse_json,'$.id'),json_extract(impulse_json,'$.impulse_type'),json_extract(impulse_json,'$.created_at'),json_extract(impulse_json,'$.updated_at'),json_extract(event_ref,'$.runtime.id'),json_extract(event_ref,'$.run_id'),json_extract(event_ref,'$.event_id'),json_extract(event_ref,'$.sequence'),json_extract(budget,'$.runtime_hops'),json_extract(budget,'$.spawned_runs'),json_extract(budget,'$.impulse_count'),json_extract(budget,'$.wall_time_seconds'),json_extract(budget,'$.attempts'),json_extract(budget,'$.reaction_bytes'),json_extract(impulse_json,'$.payload'),json_extract(impulse_json,'$.metadata'),json_extract(source_ref,'$.runtime.metadata'),json_extract(target_ref,'$.runtime.metadata'),json_extract(event_ref,'$.runtime.uri'),json_extract(event_ref,'$.runtime.metadata'),json_extract(impulse_json,'$.run_id') FROM " + table

    def _get_bridge(mut self, table: String, run_id: String, delivery_id: String) raises SQLiteError -> BridgeDelivery:
        if table != "bridge_inbox" and table != "bridge_outbox": raise SQLiteError(code=1, message="domain store: invalid bridge table")
        self._require_run(run_id)
        var stmt = self.db.query(self._bridge_select(table) + " WHERE run_id=? AND id=?")
        stmt.bind_text(1, run_id); stmt.bind_text(2, delivery_id)
        return self._read_bridge(stmt)

    def get_outbox_delivery(mut self, run_id: String, delivery_id: String) raises SQLiteError -> BridgeDelivery:
        return self._get_bridge("bridge_outbox", run_id, delivery_id)

    def get_inbox_delivery(mut self, run_id: String, delivery_id: String) raises SQLiteError -> BridgeDelivery:
        return self._get_bridge("bridge_inbox", run_id, delivery_id)

    def list_bridge_records(mut self, table: String, run_id: String, status: String = "", limit: Int = -1) raises SQLiteError -> List[BridgeDelivery]:
        if limit < -1: raise SQLiteError(code=1, message="domain store: limit must be non-negative")
        if table != "bridge_inbox" and table != "bridge_outbox": raise SQLiteError(code=1, message="domain store: invalid bridge table")
        self._require_run(run_id)
        var sql = self._bridge_select(table) + " WHERE run_id=?"
        if status != "": sql += " AND status=?"
        sql += " ORDER BY updated_at ASC, id ASC"
        if limit >= 0: sql += " LIMIT ?"
        var stmt = self.db.query(sql); stmt.bind_text(1, run_id)
        var index = 2
        if status != "": stmt.bind_text(index, status); index += 1
        if limit >= 0: stmt.bind_int(index, limit)
        var result = List[BridgeDelivery]()
        while stmt.step(): result.append(self._read_bridge_current(stmt)^)
        stmt.close()
        return result^


    @staticmethod
    def _read_bridge_current(mut stmt: Statement) raises SQLiteError -> BridgeDelivery:
        # _read_bridge expects a positioned statement; duplicate the row through a
        # temporary query is avoided by this narrow JSON projection constructor.
        var run_id = NativeDomainStore._text(stmt, 0)
        var source = RunRef(RuntimeRef(NativeDomainStore._text(stmt, 14), NativeDomainStore._text(stmt, 15), NativeDomainStore._text(stmt, 36)), NativeDomainStore._text(stmt, 18))
        var target = RunRef(RuntimeRef(NativeDomainStore._text(stmt, 16), NativeDomainStore._text(stmt, 17), NativeDomainStore._text(stmt, 37)), NativeDomainStore._text(stmt, 19))
        var impulse_run_id = NativeDomainStore._text(stmt, 40)
        var impulse = Impulse(NativeDomainStore._text(stmt, 20), impulse_run_id, NativeDomainStore._text(stmt, 21), NativeDomainStore._text(stmt, 34), NativeDomainStore._text(stmt, 35), NativeDomainStore._text(stmt, 22), NativeDomainStore._text(stmt, 23))
        var event_ref = EventRef(RuntimeRef("runtime", "", "{}"), "run", "", 0)
        if not stmt.column_null(24):
            event_ref = EventRef(RuntimeRef(NativeDomainStore._text(stmt, 24), NativeDomainStore._text(stmt, 38), NativeDomainStore._text(stmt, 39)), NativeDomainStore._text(stmt, 25), NativeDomainStore._text(stmt, 26), stmt.column_int(27))
        var budget = RuntimeBudget(runtime_hops=stmt.column_int(28), spawned_runs=stmt.column_int(29), impulse_count=stmt.column_int(30), wall_time_seconds=stmt.column_int(31), attempts=stmt.column_int(32), reaction_bytes=stmt.column_int(33), runtime_hops_limited=not stmt.column_null(28), spawned_runs_limited=not stmt.column_null(29), impulse_count_limited=not stmt.column_null(30), wall_time_seconds_limited=not stmt.column_null(31), attempts_limited=not stmt.column_null(32), reaction_bytes_limited=not stmt.column_null(33))
        return BridgeDelivery(NativeDomainStore._text(stmt, 1), run_id, NativeDomainStore._text(stmt, 2), source^, target^, impulse^, event_ref^, NativeDomainStore._text(stmt, 7), budget^, NativeDomainStore._text(stmt, 9), stmt.column_int(10), NativeDomainStore._text(stmt, 11), NativeDomainStore._text(stmt, 12), NativeDomainStore._text(stmt, 13))

    def list_outbox_records(mut self, run_id: String, status: String = "", limit: Int = -1) raises SQLiteError -> List[BridgeDelivery]:
        return self.list_bridge_records("bridge_outbox", run_id, status, limit)

    def list_inbox_records(mut self, run_id: String, status: String = "", limit: Int = -1) raises SQLiteError -> List[BridgeDelivery]:
        return self.list_bridge_records("bridge_inbox", run_id, status, limit)

    def transition_bridge_delivery(mut self, table: String, run_id: String, delivery_id: String, to_status: String, updated_at: String, idempotency_key: String = "") raises SQLiteError -> BridgeDelivery:
        if to_status != "pending" and to_status != "delivered" and to_status != "imported" and to_status != "failed":
            raise SQLiteError(code=1, message="domain store: invalid bridge status")
        if updated_at == "": raise SQLiteError(code=1, message="domain store: bridge updated_at must not be empty")
        var current = self._get_bridge(table, run_id, delivery_id)
        if not bridge_status_transition_allowed(current.status, to_status): raise SQLiteError(code=1, message="domain store: invalid bridge status transition")
        if idempotency_key == "" and current.status == to_status: return current^
        self.db.begin_immediate()
        try:
            if current.status != to_status:
                var update = self.db.query("UPDATE " + table + " SET status=?,updated_at=? WHERE run_id=? AND id=? AND status=?")
                update.bind_text(1, to_status); update.bind_text(2, updated_at); update.bind_text(3, run_id); update.bind_text(4, delivery_id); update.bind_text(5, current.status); _ = update.step(); update.close()
                if self.db.changes() != 1: raise SQLiteError(code=1, message="domain store: bridge transition lost ownership")
            if idempotency_key != "":
                var payload = self._bridge_operation_payload(delivery_id, to_status, current.attempts)
                self._record_bridge_journal(run_id, idempotency_key, "bridge." + table + "." + to_status, payload, "bridge." + table + "." + to_status, updated_at)
            self.db.commit()
        except err:
            self.db.rollback(); raise err^
        return self._get_bridge(table, run_id, delivery_id)

    def claim_bridge_delivery(mut self, table: String, run_id: String, delivery_id: String, updated_at: String, idempotency_key: String = "") raises SQLiteError -> BridgeDelivery:
        return self.transition_bridge_delivery(table, run_id, delivery_id, "pending", updated_at, idempotency_key)

    def deliver_bridge_delivery(mut self, table: String, run_id: String, delivery_id: String, updated_at: String, idempotency_key: String = "") raises SQLiteError -> BridgeDelivery:
        if updated_at == "": raise SQLiteError(code=1, message="domain store: bridge updated_at must not be empty")
        var current = self._get_bridge(table, run_id, delivery_id)
        if current.status != "pending" and current.status != "delivered":
            raise SQLiteError(code=1, message="domain store: invalid bridge status transition")
        var expected_attempts = current.attempts
        if current.status == "pending": expected_attempts += 1
        var expected_payload = self._bridge_operation_payload(delivery_id, "delivered", expected_attempts)
        if idempotency_key != "":
            var prior = self.db.query("SELECT command_type,payload FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
            prior.bind_text(1, run_id); prior.bind_text(2, idempotency_key)
            if prior.step():
                var prior_type = self._text(prior, 0)
                var prior_payload = self._text(prior, 1)
                prior.close()
                if prior_type != "bridge." + table + ".delivered" or not self._bridge_json_equal(prior_payload, expected_payload):
                    raise SQLiteError(code=1, message="domain store: bridge idempotency conflict")
                return current^
            prior.close()
        if current.status == "delivered": return current^
        self._validate_bridge_budget(current, next_attempt=True)
        var next_budget = RuntimeBudget()
        try:
            next_budget = current.budget.consume(runtime_hops=1, impulse_count=1)
        except err:
            raise SQLiteError(code=1, message="domain store: bridge budget exhausted")
        var next_attempts = current.attempts + 1
        self.db.begin_immediate()
        try:
            var update = self.db.query("UPDATE " + table + " SET budget=?,status='delivered',attempts=?,updated_at=? WHERE run_id=? AND id=? AND status='pending'")
            update.bind_text(1, next_budget.to_json()); update.bind_int(2, next_attempts); update.bind_text(3, updated_at); update.bind_text(4, run_id); update.bind_text(5, delivery_id); _ = update.step(); update.close()
            if self.db.changes() != 1: raise SQLiteError(code=1, message="domain store: bridge delivery lost ownership")
            if idempotency_key != "":
                self._record_bridge_journal(run_id, idempotency_key, "bridge." + table + ".delivered", self._bridge_operation_payload(delivery_id, "delivered", next_attempts), "bridge." + table + ".delivered", updated_at, current.impulse.id)
            self.db.commit()
        except err:
            self.db.rollback(); raise err^
        return self._get_bridge(table, run_id, delivery_id)

    def retry_bridge_delivery(mut self, table: String, run_id: String, delivery_id: String, updated_at: String, idempotency_key: String = "") raises SQLiteError -> BridgeDelivery:
        if table != "bridge_inbox" and table != "bridge_outbox": raise SQLiteError(code=1, message="domain store: invalid bridge table")
        if updated_at == "": raise SQLiteError(code=1, message="domain store: bridge updated_at must not be empty")
        var current = self._get_bridge(table, run_id, delivery_id)
        if current.status != "pending": raise SQLiteError(code=1, message="domain store: invalid bridge status transition")
        var next_attempts = current.attempts + 1
        if idempotency_key != "":
            var existing = self.db.query("SELECT command_type,payload FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
            existing.bind_text(1, run_id); existing.bind_text(2, idempotency_key)
            if existing.step():
                var prior_type = self._text(existing, 0)
                var prior_payload = self._text(existing, 1)
                existing.close()
                var replay_payload = self._bridge_operation_payload(delivery_id, "pending", current.attempts)
                if prior_type != "bridge." + table + ".retry" or not self._bridge_json_equal(prior_payload, replay_payload):
                    raise SQLiteError(code=1, message="domain store: bridge idempotency conflict")
                return current^
            existing.close()
        var payload = self._bridge_operation_payload(delivery_id, "pending", next_attempts)
        self._validate_bridge_budget(current, next_attempt=True)
        self.db.begin_immediate()
        try:
            var update = self.db.query("UPDATE " + table + " SET attempts=attempts+1,updated_at=? WHERE run_id=? AND id=? AND status='pending'")
            update.bind_text(1, updated_at); update.bind_text(2, run_id); update.bind_text(3, delivery_id); _ = update.step(); update.close()
            if self.db.changes() != 1: raise SQLiteError(code=1, message="domain store: bridge retry lost ownership")
            if idempotency_key != "": self._record_bridge_journal(run_id, idempotency_key, "bridge." + table + ".retry", self._bridge_operation_payload(delivery_id, "pending", next_attempts), "bridge." + table + ".retry", updated_at, current.impulse.id)
            self.db.commit()
        except err:
            self.db.rollback(); raise err^
        return self._get_bridge(table, run_id, delivery_id)
    def _count_rows(mut self, table: String, run_id: String) raises SQLiteError -> Int:
        var stmt = self.db.query("SELECT COUNT(*) FROM " + table + " WHERE run_id=?")
        stmt.bind_text(1, run_id)
        if not stmt.step():
            stmt.close()
            return 0
        var result = stmt.column_int(0)
        stmt.close()
        return result

    def _group_json(mut self, table: String, column: String, run_id: String) raises SQLiteError -> String:
        var stmt = self.db.query("SELECT " + column + ",COUNT(*) FROM " + table + " WHERE run_id=? GROUP BY " + column + " ORDER BY " + column + " ASC")
        stmt.bind_text(1, run_id)
        var result = "{"
        var first = True
        while stmt.step():
            if not first: result += ","
            first = False
            result += _quote(self._text(stmt, 0)) + ":" + String(stmt.column_int(1))
        stmt.close()
        return result + "}"

    def _run_summary_data(mut self, run_id: String, source_sequence: Int) raises SQLiteError -> String:
        var event_types = self._group_json("runtime_events", "event_type", run_id)
        var impulse_types = self._group_json("impulses", "impulse_type", run_id)
        var homeostat_status = self._group_json("homeostats", "status", run_id)
        var process_status = self._group_json("processes", "status", run_id)
        var reaction_bytes_stmt = self.db.query("SELECT COALESCE(SUM(size_bytes),0) FROM reactions WHERE run_id=?")
        reaction_bytes_stmt.bind_text(1, run_id); _ = reaction_bytes_stmt.step()
        var reaction_bytes = reaction_bytes_stmt.column_int(0); reaction_bytes_stmt.close()
        var attempts_stmt = self.db.query("SELECT COALESCE(SUM(attempt),0) FROM processes WHERE run_id=?")
        attempts_stmt.bind_text(1, run_id); _ = attempts_stmt.step()
        var process_attempts = attempts_stmt.column_int(0); attempts_stmt.close()
        var input_stmt = self.db.query("SELECT COALESCE(SUM(LENGTH(input_json)),0) FROM processes WHERE run_id=?")
        input_stmt.bind_text(1, run_id); _ = input_stmt.step()
        var input_bytes = input_stmt.column_int(0); input_stmt.close()
        var output_stmt = self.db.query("SELECT COALESCE(SUM(LENGTH(output_json)),0) FROM processes WHERE run_id=?")
        output_stmt.bind_text(1, run_id); _ = output_stmt.step()
        var output_bytes = output_stmt.column_int(0); output_stmt.close()
        var bridge_commands = self.db.query("SELECT COUNT(*) FROM runtime_commands WHERE run_id=? AND command_type LIKE 'bridge.%'")
        bridge_commands.bind_text(1, run_id); _ = bridge_commands.step()
        var bridge_command_count = bridge_commands.column_int(0); bridge_commands.close()
        var bridge_delivery_count = self._count_rows("bridge_outbox", run_id) + self._count_rows("bridge_inbox", run_id)
        var spawned_stmt = self.db.query("SELECT COUNT(DISTINCT json_extract(target_ref,'$.run_id')) FROM bridge_outbox WHERE run_id=?")
        spawned_stmt.bind_text(1, run_id); _ = spawned_stmt.step()
        var spawned = spawned_stmt.column_int(0); spawned_stmt.close()
        var subprocess_stmt = self.db.query("SELECT COUNT(*) FROM processes WHERE run_id=? AND process_type='subprocess'")
        subprocess_stmt.bind_text(1, run_id); _ = subprocess_stmt.step()
        var subprocesses = subprocess_stmt.column_int(0); subprocess_stmt.close()
        var event_count = self._count_rows("runtime_events", run_id)
        return "{\"association_count\":" + String(self._count_rows("associations", run_id)) + ",\"event_count\":" + String(event_count) + ",\"event_type_counts\":" + event_types + ",\"homeostat_count\":" + String(self._count_rows("homeostats", run_id)) + ",\"homeostat_status_counts\":" + homeostat_status + ",\"impulse_count\":" + String(self._count_rows("impulses", run_id)) + ",\"impulse_type_counts\":" + impulse_types + ",\"process_count\":" + String(self._count_rows("processes", run_id)) + ",\"process_status_counts\":" + process_status + ",\"reaction_count\":" + String(self._count_rows("reactions", run_id)) + ",\"resource_accounting\":{\"bridge_command_count\":" + String(bridge_command_count) + ",\"bridge_delivery_count\":" + String(bridge_delivery_count) + ",\"process_attempts\":" + String(process_attempts) + ",\"process_input_bytes\":" + String(input_bytes) + ",\"process_output_bytes\":" + String(output_bytes) + ",\"reaction_bytes\":" + String(reaction_bytes) + ",\"spawned_run_count\":" + String(spawned) + ",\"subprocess_count\":" + String(subprocesses) + "},\"run_id\":" + _quote(run_id) + ",\"source_event_sequence\":" + String(source_sequence) + "}"
    def rebuild_projection(mut self, run_id: String, name: String, updated_at: String = "") raises SQLiteError -> Projection:
        var names = List[String](); names.append(name)
        var rebuilt = self.rebuild_projections(run_id, names, updated_at)
        var result = rebuilt[0].copy()
        return result^

    def rebuild_projections(mut self, run_id: String, names: List[String], updated_at: String = "") raises SQLiteError -> List[Projection]:
        self._require_run(run_id)
        var requested = names.copy()
        if len(requested) == 0: requested.append("run_summary")
        var result = List[Projection]()
        var effective_updated_at = updated_at
        if effective_updated_at == "":
            raise SQLiteError(code=2, message="domain store: projection rebuild timestamp must not be empty")
        self.db.begin()
        try:
            var latest = self.db.query("SELECT COALESCE(MAX(sequence),0) FROM runtime_events WHERE run_id=?"); latest.bind_text(1, run_id)
            if not latest.step(): raise SQLiteError(code=1, message="domain store: unable to read event watermark")
            var watermark = latest.column_int(0); latest.close()
            for name in requested:
                if name == "": raise SQLiteError(code=1, message="domain store: projection name must not be empty")
                var data = ""
                if name == "run_summary": data = self._run_summary_data(run_id, watermark)
                else: data = "{\"event_count\":" + String(watermark) + ",\"source_event_sequence\":" + String(watermark) + "}"
                var stmt = self.db.query("INSERT INTO projections (run_id,name,id,version,data,source_event_sequence,updated_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(run_id,name) DO UPDATE SET data=excluded.data,source_event_sequence=excluded.source_event_sequence,updated_at=excluded.updated_at")
                stmt.bind_text(1, run_id); stmt.bind_text(2, name); stmt.bind_text(3, name + ":" + run_id); stmt.bind_int(4, 1); stmt.bind_text(5, data); stmt.bind_int(6, watermark); stmt.bind_text(7, effective_updated_at); _ = stmt.step()
            self.db.commit()
        except err:
            self.db.rollback(); raise SQLiteError(code=1, message="domain store: projection rebuild failed")
        for name in requested: result.append(self.get_projection(run_id, name)^)
        return result^
