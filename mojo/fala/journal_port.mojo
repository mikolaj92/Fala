"""Event-stream Journal port types (Python 0.2.2 fala.journal.types).

Core durability contract — no SQLite. Sinks implement append_batch / claim_next
against these structures. See docs/EVENT_STREAM_CORE.md and
docs/MOJO_EVENT_STREAM_MIGRATION.md.
"""

from std.collections import List
from std.collections import Dict


@fieldwise_init
struct StateFact(Copyable, Movable):
    """Materialized entity change in the same batch as its command unit."""

    var entity: String
    var op: String  # "upsert" | "delete"
    var key_id: String  # primary key string (usually entity id)
    var body_json: String  # full snapshot JSON for upsert; empty for delete


@fieldwise_init
struct CommandRecord(Copyable, Movable):
    var id: String
    var run_id: String
    var command_type: String
    var idempotency_key: String
    var actor: String
    var correlation_id: String
    var causation_id: String
    var payload_json: String
    var created_at: String


@fieldwise_init
struct EventRecord(Copyable, Movable):
    var id: String
    var run_id: String
    var event_type: String
    var schema_version: Int
    var impulse_id: String
    var process_id: String
    var sequence: Int  # 0 means unassigned until journal accept
    var command_id: String
    var actor: String
    var correlation_id: String
    var causation_id: String
    var payload_json: String
    var created_at: String


struct CommandUnit(Copyable, Movable):
    var command: CommandRecord
    var events: List[EventRecord]
    var facts: List[StateFact]

    def __init__(out self, command: CommandRecord):
        self.command = command.copy()
        self.events = List[EventRecord]()
        self.facts = List[StateFact]()

    def __init__(out self, *, copy: Self):
        self.command = copy.command.copy()
        self.events = copy.events.copy()
        self.facts = copy.facts.copy()


struct JournalBatch(Copyable, Movable):
    """Atomic durability unit — one or more CommandUnits."""

    var journal_seq: Int  # 0 = unassigned until accept
    var run_id: String
    var units: List[CommandUnit]
    var stream_id: String
    var parent_stream_id: String
    var parent_process_id: String

    def __init__(out self, run_id: String, units: List[CommandUnit]):
        self.journal_seq = 0
        self.run_id = run_id
        self.units = units.copy()
        self.stream_id = ""
        self.parent_stream_id = ""
        self.parent_process_id = ""

    def __init__(out self, *, copy: Self):
        self.journal_seq = copy.journal_seq
        self.run_id = copy.run_id
        self.units = copy.units.copy()
        self.stream_id = copy.stream_id
        self.parent_stream_id = copy.parent_stream_id
        self.parent_process_id = copy.parent_process_id


@fieldwise_init
struct AppendResult(Copyable, Movable):
    var batch: JournalBatch
    var replayed: Bool
    var units: List[CommandUnit]


struct ClaimRequest(Copyable, Movable):
    var worker_id: String
    var run_id: String  # empty + all_runs=True claims across runs
    var lease_seconds: Float64
    var all_runs: Bool
    var now: Float64  # caller wall/sim clock; used for lease/available checks

    def __init__(
        out self,
        worker_id: String,
        run_id: String = "",
        lease_seconds: Float64 = 300.0,
        all_runs: Bool = False,
        now: Float64 = 0.0,
    ):
        self.worker_id = worker_id
        self.run_id = run_id
        self.lease_seconds = lease_seconds
        self.all_runs = all_runs
        self.now = now

    def __init__(out self, *, copy: Self):
        self.worker_id = copy.worker_id
        self.run_id = copy.run_id
        self.lease_seconds = copy.lease_seconds
        self.all_runs = copy.all_runs
        self.now = copy.now


@fieldwise_init
struct ClaimResult(Copyable, Movable):
    """process_id empty means nothing claimed (batch may still hold reaps)."""

    var process_id: String
    var run_id: String
    var has_batch: Bool
    var batch: JournalBatch
    var replayed: Bool


def leading_command(batch: JournalBatch) raises -> CommandRecord:
    if len(batch.units) < 1:
        raise Error("JournalBatch.units must be non-empty")
    return batch.units[0].command.copy()


def leading_idempotency_key(batch: JournalBatch) raises -> String:
    var command = leading_command(batch)
    return command.run_id + "\0" + command.idempotency_key
