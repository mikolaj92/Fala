"""Optional journal ops: retention, maintenance, reaction GC (not Essential Fala).

Real implementations live here. NativeDomainStore keeps shared helpers
(_require_run, _text) and referenced_reaction_digests as private API for ops.
"""

from std.collections import List
from std.pathlib import Path
from fala.sqlite import SQLiteError
from fala.reactions import FileReactionStore
from fala.domain_store import NativeDomainStore

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

def _default_retention_statuses() -> List[String]:
    var result = List[String]()
    result.append("completed")
    result.append("failed")
    result.append("cancelled")
    result.append("timed_out")
    return result^

def _contains_string(values: List[String], wanted: String) -> Bool:
    for value in values:
        if value == wanted:
            return True
    return False

def delete_run(
    mut store: NativeDomainStore,
    run_id: String,
    terminal_only: Bool = False,
) raises SQLiteError -> RunDeleteCounts:
    """Delete one run and every run-scoped row atomically.

    Append-only journal triggers are suspended only inside this transaction
    and recreated before commit. Unknown runs are rejected without writes.
    When ``terminal_only`` is true, non-terminal statuses are rejected after
    BEGIN IMMEDIATE and before trigger suspension. Deletion order follows
    foreign-key dependencies and count fields follow the schema table names
    for deterministic retention reporting.
    """
    var counts = RunDeleteCounts(run_id)
    store.db.begin_immediate()
    try:
        # Lock the database before inspecting the run or suspending the
        # append-only triggers.  SQLite rolls back trigger DDL together
        # with the row deletes, so every failure restores both triggers.
        if run_id == "":
            raise SQLiteError(code=1, message="domain store: run_id must not be empty")
        var run = store.db.query("SELECT status FROM runs WHERE id=?")
        run.bind_text(1, run_id)
        if not run.step():
            run.close()
            raise SQLiteError(code=1, message="domain store: unknown run")
        var status = store._text(run, 0)
        run.close()
        if terminal_only and not _contains_string(_default_retention_statuses(), status):
            raise SQLiteError(code=1, message="domain store: run is not terminal")

        store.db.execute("DROP TRIGGER IF EXISTS runtime_events_no_delete")
        store.db.execute("DROP TRIGGER IF EXISTS runtime_commands_no_delete")

        var stmt = store.db.query("DELETE FROM bridge_inbox WHERE run_id=?")
        stmt.bind_text(1, run_id); _ = stmt.step(); counts.bridge_inbox = store.db.changes()
        stmt = store.db.query("DELETE FROM bridge_outbox WHERE run_id=?")
        stmt.bind_text(1, run_id); _ = stmt.step(); counts.bridge_outbox = store.db.changes()
        stmt = store.db.query("DELETE FROM projections WHERE run_id=?")
        stmt.bind_text(1, run_id); _ = stmt.step(); counts.projections = store.db.changes()
        stmt = store.db.query("DELETE FROM homeostats WHERE run_id=?")
        stmt.bind_text(1, run_id); _ = stmt.step(); counts.homeostats = store.db.changes()
        stmt = store.db.query("DELETE FROM processes WHERE run_id=?")
        stmt.bind_text(1, run_id); _ = stmt.step(); counts.processes = store.db.changes()
        stmt = store.db.query("DELETE FROM reactions WHERE run_id=?")
        stmt.bind_text(1, run_id); _ = stmt.step(); counts.reactions = store.db.changes()
        stmt = store.db.query("DELETE FROM associations WHERE run_id=?")
        stmt.bind_text(1, run_id); _ = stmt.step(); counts.associations = store.db.changes()
        stmt = store.db.query("DELETE FROM impulse_relations WHERE run_id=?")
        stmt.bind_text(1, run_id); _ = stmt.step(); counts.impulse_relations = store.db.changes()
        stmt = store.db.query("DELETE FROM impulse_types WHERE run_id=?")
        stmt.bind_text(1, run_id); _ = stmt.step(); counts.impulse_types = store.db.changes()
        stmt = store.db.query("DELETE FROM impulses WHERE run_id=?")
        stmt.bind_text(1, run_id); _ = stmt.step(); counts.impulses = store.db.changes()
        stmt = store.db.query("DELETE FROM runtime_events WHERE run_id=?")
        stmt.bind_text(1, run_id); _ = stmt.step(); counts.runtime_events = store.db.changes()
        stmt = store.db.query("DELETE FROM runtime_commands WHERE run_id=?")
        stmt.bind_text(1, run_id); _ = stmt.step(); counts.runtime_commands = store.db.changes()
        stmt = store.db.query("DELETE FROM runs WHERE id=?")
        stmt.bind_text(1, run_id); _ = stmt.step(); counts.runs = store.db.changes()

        store.db.execute("CREATE TRIGGER IF NOT EXISTS runtime_events_no_delete BEFORE DELETE ON runtime_events BEGIN SELECT RAISE(ABORT, 'runtime_events is append-only'); END")
        store.db.execute("CREATE TRIGGER IF NOT EXISTS runtime_commands_no_delete BEFORE DELETE ON runtime_commands BEGIN SELECT RAISE(ABORT, 'runtime_commands is append-only'); END")
        store.db.commit()
    except err:
        # Rollback includes the trigger drops, retaining the append-only
        # guards when a run is unknown or any delete fails.
        store.db.rollback()
        # Preserve specific validation diagnostics for host callers.
        var detail = String(err)
        if detail.find("run_id must not be empty") >= 0:
            raise SQLiteError(code=1, message="domain store: run_id must not be empty")
        if detail.find("unknown run") >= 0:
            raise SQLiteError(code=1, message="domain store: unknown run")
        if detail.find("not terminal") >= 0:
            raise SQLiteError(code=1, message="domain store: run is not terminal")
        raise SQLiteError(code=1, message="domain store: delete_run failed")
    return counts^


def delete_terminal_run(mut store: NativeDomainStore, run_id: String) raises SQLiteError -> RunDeleteCounts:
    """Delete one terminal run after BEGIN IMMEDIATE status validation."""
    return delete_run(store, run_id, terminal_only=True)



def run_retention(
    mut store: NativeDomainStore,
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
    var cutoff_check = store.db.query("SELECT julianday(?) IS NOT NULL")
    cutoff_check.bind_text(1, before)
    var cutoff_valid = cutoff_check.step() and cutoff_check.column_int(0) == 1
    cutoff_check.close()
    if not cutoff_valid:
        raise SQLiteError(code=1, message="domain store: retention before must be a valid timestamp")
    var selected = statuses.copy()
    if len(selected) == 0:
        selected = _default_retention_statuses()
    for status in selected:
        if not _contains_string(_default_retention_statuses(), status):
            raise SQLiteError(code=1, message="domain store: retention status must be terminal")

    var plan = RunRetentionPlan(before, selected.copy(), dry_run)
    var stmt = store.db.query("SELECT id,status,created_at,updated_at,finished_at,COALESCE(finished_at,updated_at,created_at) FROM runs WHERE julianday(COALESCE(finished_at,updated_at,created_at)) < julianday(?) ORDER BY created_at ASC,id ASC")
    stmt.bind_text(1, before)
    var candidates = List[RunRetentionCandidate]()
    while stmt.step():
        var status = store._text(stmt, 1)
        var run_id = store._text(stmt, 0)
        if not _contains_string(selected, status):
            continue
        if _contains_string(keep_run_ids, run_id):
            continue
        candidates.append(RunRetentionCandidate(
            run_id, status, store._text(stmt, 2), store._text(stmt, 3), store._text(stmt, 4)
        )^)
    stmt.close()
    plan.candidate_count = len(candidates)
    for candidate in candidates:
        var item = RunRetentionItem(candidate)
        if not dry_run:
            item.row_counts = delete_run(store, candidate.run_id)
            item.deleted = item.row_counts.runs == 1
            if item.deleted:
                plan.deleted_run_count += 1
                plan.row_counts.accumulate(item.row_counts)
        plan.runs.append(item^)
    return plan^

def maintain_journal(
    mut store: NativeDomainStore,
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

    var cutoff = store.db.query("SELECT datetime('now', '-' || ? || ' days')")
    cutoff.bind_real(1, older_than_days)
    if not cutoff.step():
        cutoff.close()
        raise SQLiteError(code=1, message="domain store: unable to derive maintenance cutoff")
    var before = cutoff.column_text(0)
    cutoff.close()

    var keep_run_ids = List[String]()
    if keep_last > 0:
        var recent = store.db.query("SELECT id,status FROM runs ORDER BY julianday(COALESCE(finished_at,updated_at,created_at)) DESC,id DESC")
        while recent.step() and len(keep_run_ids) < keep_last:
            var status = store._text(recent, 1)
            if _contains_string(_default_retention_statuses(), status):
                keep_run_ids.append(store._text(recent, 0))
        recent.close()

    var retention = run_retention(store, 
        before, statuses=List[String](), dry_run=dry_run, keep_run_ids=keep_run_ids.copy()
    )
    var reaction_gc = ReactionGarbageCollectionPlan(dry_run)
    reaction_gc.reaction_root = reaction_root
    if reaction_root != "":
        try:
            var reaction_store = FileReactionStore(reaction_root)
            var referenced = store.referenced_reaction_digests()
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
        store.db.execute("VACUUM")
        plan.vacuumed = True
    return plan^

def collect_reaction_garbage(mut store: NativeDomainStore, reaction_root: String, run_id: String = "", dry_run: Bool = True) raises SQLiteError -> ReactionGarbageCollectionPlan:
    """Plan or delete unreferenced local CAS reaction blobs."""
    if reaction_root == "":
        raise SQLiteError(code=2, message="argument_error: --reaction-root is required")
    if run_id != "": store._require_run(run_id)
    var plan = ReactionGarbageCollectionPlan(dry_run)
    plan.reaction_root = reaction_root
    if run_id != "":
        plan.run_ids.append(run_id)
    else:
        var selected = store.db.query("SELECT id FROM runs ORDER BY id ASC")
        while selected.step(): plan.run_ids.append(store._text(selected, 0))
        selected.close()
    var all_runs = store.db.query("SELECT id FROM runs ORDER BY id ASC")
    while all_runs.step(): plan.scanned_run_ids.append(store._text(all_runs, 0))
    all_runs.close()
    try:
        var reaction_store = FileReactionStore(reaction_root)
        var referenced = store.referenced_reaction_digests()
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

