"""In-memory JournalPort sink — core durability without SQLite.

Single-process maps. Claim policy reuses pure helpers from processes.mojo.
"""

from std.collections import List
from std.collections import Dict
from fala.journal_port import (
    AppendResult,
    ClaimRequest,
    ClaimResult,
    CommandRecord,
    CommandUnit,
    EventRecord,
    JournalBatch,
)
from fala.processes import (
    ProcessRecord,
    claim_process,
    ready_processes,
)
from fala.status import ProcessStatus


struct InMemoryJournal(Movable):
    """Memory-only JournalPort implementation."""

    var stream_id: String
    var journal_seq: Int
    var batches: List[JournalBatch]
    var commands: Dict[String, CommandRecord]
    var events_by_run: Dict[String, List[EventRecord]]
    var event_seq_by_run: Dict[String, Int]
    var processes: Dict[String, ProcessRecord]

    def __init__(out self, stream_id: String = "memory://local"):
        self.stream_id = stream_id
        self.journal_seq = 0
        self.batches = List[JournalBatch]()
        self.commands = Dict[String, CommandRecord]()
        self.events_by_run = Dict[String, List[EventRecord]]()
        self.event_seq_by_run = Dict[String, Int]()
        self.processes = Dict[String, ProcessRecord]()

    def runtime_uri(self) -> String:
        if self.stream_id.startswith("memory://"):
            return self.stream_id
        return "memory://" + self.stream_id

    def _cmd_key(self, run_id: String, idempotency_key: String) -> String:
        return run_id + "\0" + idempotency_key

    def seed_process(mut self, process: ProcessRecord):
        self.processes[process.id] = process.copy()

    def get_process(self, process_id: String) raises -> ProcessRecord:
        if process_id not in self.processes:
            raise Error("unknown process: " + process_id)
        return self.processes[process_id].copy()

    def append_batch(mut self, batch: JournalBatch) raises -> AppendResult:
        if len(batch.units) < 1:
            raise Error("JournalBatch.units must be non-empty")
        var lead = batch.units[0].command.copy()
        var lead_key = self._cmd_key(lead.run_id, lead.idempotency_key)
        if lead_key in self.commands:
            var stored = self.commands[lead_key].copy()
            var empty_units = List[CommandUnit]()
            for _ in range(len(batch.units)):
                empty_units.append(CommandUnit(stored.copy()))
            return AppendResult(
                batch=batch.copy(),
                replayed=True,
                units=empty_units^,
            )

        for index in range(1, len(batch.units)):
            var other = batch.units[index].command.copy()
            var nk = self._cmd_key(other.run_id, other.idempotency_key)
            if nk in self.commands:
                raise Error(
                    "Corrupt partial history: non-leading idempotency key exists"
                )

        var assigned_units = List[CommandUnit]()
        for unit_index in range(len(batch.units)):
            var unit = batch.units[unit_index].copy()
            var cmd = unit.command.copy()
            var ckey = self._cmd_key(cmd.run_id, cmd.idempotency_key)
            if ckey in self.commands:
                raise Error("Duplicate command idempotency key within batch")
            var assigned_events = self._assign_events(cmd, unit.events)
            self.commands[ckey] = cmd.copy()
            var out_unit = CommandUnit(cmd^)
            out_unit.events = assigned_events^
            out_unit.facts = unit.facts.copy()
            assigned_units.append(out_unit^)

        self.journal_seq += 1
        var stored_batch = batch.copy()
        stored_batch.journal_seq = self.journal_seq
        stored_batch.units = assigned_units.copy()
        if stored_batch.stream_id == "":
            stored_batch.stream_id = self.runtime_uri()
        self.batches.append(stored_batch.copy())
        return AppendResult(
            batch=stored_batch^,
            replayed=False,
            units=assigned_units^,
        )

    def _assign_events(
        mut self, command: CommandRecord, events: List[EventRecord]
    ) raises -> List[EventRecord]:
        var run_id = command.run_id
        var seq = 0
        if run_id in self.event_seq_by_run:
            seq = self.event_seq_by_run[run_id]
        var out = List[EventRecord]()
        var bucket = List[EventRecord]()
        if run_id in self.events_by_run:
            bucket = self.events_by_run[run_id].copy()
        for event_index in range(len(events)):
            seq += 1
            var event = events[event_index].copy()
            event.run_id = run_id
            event.command_id = command.id
            event.sequence = seq
            if event.actor == "":
                event.actor = command.actor
            if event.correlation_id == "":
                event.correlation_id = command.correlation_id
            if event.causation_id == "":
                event.causation_id = command.causation_id
            bucket.append(event.copy())
            out.append(event^)
        self.event_seq_by_run[run_id] = seq
        self.events_by_run[run_id] = bucket^
        return out^

    def claim_next(mut self, request: ClaimRequest) raises -> ClaimResult:
        if request.lease_seconds <= 0.0:
            raise Error("lease_seconds must be greater than zero")
        if request.run_id == "" and not request.all_runs:
            raise Error("claim_next requires run_id or all_runs=True")

        var now = request.now
        if now < 0.0:
            raise Error("claim_next now must be non-negative")

        var units = List[CommandUnit]()
        var batch_run_id = request.run_id
        if batch_run_id == "":
            batch_run_id = "mixed"

        var reaped_ids = List[String]()
        for entry in self.processes.items():
            var row = entry.value.copy()
            if request.run_id != "" and row.run_id != request.run_id:
                continue
            if (
                row.status == ProcessStatus.running()
                and row.lease_is_expired(now)
                and not row.attempts_remaining()
            ):
                reaped_ids.append(row.id)

        for rid_index in range(len(reaped_ids)):
            var pid = reaped_ids[rid_index]
            var row = self.processes[pid].copy()
            row.status = ProcessStatus.failed()
            row.lease_owner = ""
            row.lease_expires_at = 0.0
            self.processes[pid] = row.copy()
            var fail_cmd = CommandRecord(
                id="process.fail:" + pid + ":" + String(row.attempt),
                run_id=row.run_id,
                command_type="process.fail",
                idempotency_key="process.fail:" + pid + ":" + String(row.attempt),
                actor=request.worker_id,
                correlation_id="",
                causation_id="",
                payload_json="{\"process_id\":\"" + pid + "\"}",
                created_at="",
            )
            var fail_event = EventRecord(
                id=fail_cmd.id + ":event",
                run_id=row.run_id,
                event_type="process.failed",
                schema_version=1,
                impulse_id="",
                process_id=pid,
                sequence=0,
                command_id="",
                actor=request.worker_id,
                correlation_id="",
                causation_id="",
                payload_json=fail_cmd.payload_json,
                created_at="",
            )
            var unit = CommandUnit(fail_cmd^)
            unit.events.append(fail_event^)
            units.append(unit^)
            batch_run_id = row.run_id

        var candidates = List[ProcessRecord]()
        for entry in self.processes.items():
            var row = entry.value.copy()
            if request.run_id != "" and row.run_id != request.run_id:
                continue
            candidates.append(row^)

        var ordered = ready_processes(candidates, now)
        var claimed_id = ""
        var claimed_run = ""
        if len(ordered) > 0:
            var chosen = ordered[0].copy()
            var claimed = claim_process(
                chosen, request.worker_id, now, request.lease_seconds
            )
            self.processes[claimed.id] = claimed.copy()
            claimed_id = claimed.id
            claimed_run = claimed.run_id
            batch_run_id = claimed.run_id
            var claim_cmd = CommandRecord(
                id="process.claim:" + claimed.id + ":" + String(claimed.attempt),
                run_id=claimed.run_id,
                command_type="process.claim",
                idempotency_key="process.claim:"
                + claimed.id
                + ":"
                + String(claimed.attempt),
                actor=request.worker_id,
                correlation_id="",
                causation_id="",
                payload_json="{\"process_id\":\""
                + claimed.id
                + "\",\"worker_id\":\""
                + request.worker_id
                + "\"}",
                created_at="",
            )
            var claim_event = EventRecord(
                id=claim_cmd.id + ":event",
                run_id=claimed.run_id,
                event_type="process.claimed",
                schema_version=1,
                impulse_id="",
                process_id=claimed.id,
                sequence=0,
                command_id="",
                actor=request.worker_id,
                correlation_id="",
                causation_id="",
                payload_json=claim_cmd.payload_json,
                created_at="",
            )
            var claim_unit = CommandUnit(claim_cmd^)
            claim_unit.events.append(claim_event^)
            units.append(claim_unit^)

        if len(units) < 1:
            var empty_batch = JournalBatch(batch_run_id, List[CommandUnit]())
            return ClaimResult(
                process_id="",
                run_id="",
                has_batch=False,
                batch=empty_batch^,
                replayed=False,
            )

        var batch = JournalBatch(batch_run_id, units^)
        var append = self.append_batch(batch^)
        return ClaimResult(
            process_id=claimed_id,
            run_id=claimed_run,
            has_batch=True,
            batch=append.batch.copy(),
            replayed=append.replayed,
        )

    def list_events(
        self, run_id: String, after_sequence: Int = 0, limit: Int = 0
    ) raises -> List[EventRecord]:
        var out = List[EventRecord]()
        if run_id not in self.events_by_run:
            return out^
        var events = self.events_by_run[run_id].copy()
        for index in range(len(events)):
            var event = events[index].copy()
            if event.sequence <= after_sequence:
                continue
            out.append(event^)
            if limit > 0 and len(out) >= limit:
                break
        return out^

    def get_command_by_idempotency(
        self, run_id: String, idempotency_key: String
    ) raises -> CommandRecord:
        var key = self._cmd_key(run_id, idempotency_key)
        if key not in self.commands:
            raise Error("command not found")
        return self.commands[key].copy()

    def has_command(self, run_id: String, idempotency_key: String) -> Bool:
        return self._cmd_key(run_id, idempotency_key) in self.commands

    def load(
        self, run_id: String = "", after_journal_seq: Int = 0
    ) raises -> List[JournalBatch]:
        var out = List[JournalBatch]()
        for index in range(len(self.batches)):
            var batch = self.batches[index].copy()
            if after_journal_seq > 0 and batch.journal_seq <= after_journal_seq:
                continue
            if run_id != "" and batch.run_id != run_id:
                continue
            out.append(batch^)
        return out^

    def import_stored_batch(mut self, batch: JournalBatch) raises:
        """Rebuild index from a durable accepted batch (Jsonl reopen path).

        Commands and events are restored as stored (sequences already assigned).
        Does not re-run append_batch assignment logic.
        """
        if len(batch.units) < 1:
            raise Error("import_stored_batch requires non-empty units")
        var stored = batch.copy()
        if stored.journal_seq <= 0:
            self.journal_seq += 1
            stored.journal_seq = self.journal_seq
        elif stored.journal_seq > self.journal_seq:
            self.journal_seq = stored.journal_seq
        if stored.stream_id == "":
            stored.stream_id = self.runtime_uri()
        for unit_index in range(len(stored.units)):
            var unit = stored.units[unit_index].copy()
            var cmd = unit.command.copy()
            var ckey = self._cmd_key(cmd.run_id, cmd.idempotency_key)
            self.commands[ckey] = cmd.copy()
            var bucket = List[EventRecord]()
            if cmd.run_id in self.events_by_run:
                bucket = self.events_by_run[cmd.run_id].copy()
            var max_seq = 0
            if cmd.run_id in self.event_seq_by_run:
                max_seq = self.event_seq_by_run[cmd.run_id]
            for ei in range(len(unit.events)):
                var event = unit.events[ei].copy()
                if event.run_id == "":
                    event.run_id = cmd.run_id
                if event.command_id == "":
                    event.command_id = cmd.id
                if event.sequence > max_seq:
                    max_seq = event.sequence
                bucket.append(event^)
            self.events_by_run[cmd.run_id] = bucket^
            self.event_seq_by_run[cmd.run_id] = max_seq
        self.batches.append(stored^)
