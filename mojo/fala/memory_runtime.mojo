"""Memory-backed runtime mutators over InMemoryJournal (core, no SQLite).

Provides create_run / schedule / complete / fail / apply correlation advance
using JournalPort batches. Process extras (effector_id, conduction, output)
live beside ProcessRecord.
"""

from std.collections import List
from std.collections import Dict

from fala.correlation import (
    CorrelationAdvancePlan,
    CorrelationExecutionState,
    CorrelationInstantiationPlan,
    CorrelationPathSpec,
    CorrelationProcessPlan,
    advance_correlation_states,
    instantiate_correlation_path,
)
from fala.domain import Impulse
from fala.journal_port import (
    AppendResult,
    ClaimRequest,
    ClaimResult,
    CommandRecord,
    CommandUnit,
    EventRecord,
    JournalBatch,
)
from fala.memory_journal import InMemoryJournal
from fala.processes import ProcessRecord, transition_process
from fala.status import ProcessStatus, RunStatus, can_transition_run


@fieldwise_init
struct ProcessExtra(Copyable, Movable):
    var effector_id: String
    var conduction: List[String]
    var input_json: String
    var output_json: String
    var config_json: String
    var metadata_json: String


@fieldwise_init
struct RunRow(Copyable, Movable):
    var id: String
    var status: String
    var title: String
    var metadata_json: String


struct MemoryRuntime(Movable):
    """In-memory AutonomousCorrelator-shaped surface for core smokes."""

    var journal: InMemoryJournal
    var runs: Dict[String, RunRow]
    var impulses: Dict[String, Impulse]
    var extras: Dict[String, ProcessExtra]  # process_id -> extra
    var clock: Float64

    def __init__(out self, stream_id: String = "memory://runtime"):
        self.journal = InMemoryJournal(stream_id)
        self.runs = Dict[String, RunRow]()
        self.impulses = Dict[String, Impulse]()
        self.extras = Dict[String, ProcessExtra]()
        self.clock = 0.0

    def runtime_uri(self) -> String:
        return self.journal.runtime_uri()

    def _tick(mut self) -> Float64:
        self.clock += 1.0
        return self.clock

    def create_run(
        mut self,
        run_id: String,
        title: String = "",
        metadata_json: String = "{}",
        idempotency_key: String = "",
    ) raises -> AppendResult:
        var key = idempotency_key
        if key == "":
            key = "run.create:" + run_id
        if self.journal.has_command(run_id, key):
            var empty = List[CommandUnit]()
            var batch = JournalBatch(run_id, empty^)
            return AppendResult(batch=batch^, replayed=True, units=List[CommandUnit]())

        var cmd = CommandRecord(
            id=key,
            run_id=run_id,
            command_type="run.create",
            idempotency_key=key,
            actor="runtime",
            correlation_id="",
            causation_id="",
            payload_json="{\"run_id\":\"" + run_id + "\"}",
            created_at="",
        )
        var event = EventRecord(
            id=key + ":event",
            run_id=run_id,
            event_type="run.created",
            schema_version=1,
            impulse_id="",
            process_id="",
            sequence=0,
            command_id="",
            actor="runtime",
            correlation_id="",
            causation_id="",
            payload_json=cmd.payload_json,
            created_at="",
        )
        var unit = CommandUnit(cmd^)
        unit.events.append(event^)
        var units = List[CommandUnit]()
        units.append(unit^)
        var batch = JournalBatch(run_id, units^)
        var result = self.journal.append_batch(batch^)
        if not result.replayed:
            self.runs[run_id] = RunRow(
                id=run_id,
                status="created",
                title=title,
                metadata_json=metadata_json,
            )
        return result^

    def set_run_status(
        mut self, run_id: String, status: String, idempotency_key: String = ""
    ) raises:
        if run_id not in self.runs:
            raise Error("unknown run: " + run_id)
        var row = self.runs[run_id].copy()
        var from_status = RunStatus(row.status)
        var to_status = RunStatus(status)
        if not can_transition_run(from_status, to_status):
            raise Error("illegal run transition")
        var key = idempotency_key
        if key == "":
            key = "run.status.set:" + run_id + ":" + status
        var cmd = CommandRecord(
            id=key,
            run_id=run_id,
            command_type="run.status.set",
            idempotency_key=key,
            actor="runtime",
            correlation_id="",
            causation_id="",
            payload_json="{\"status\":\"" + status + "\"}",
            created_at="",
        )
        var event = EventRecord(
            id=key + ":event",
            run_id=run_id,
            event_type="run.status.changed",
            schema_version=1,
            impulse_id="",
            process_id="",
            sequence=0,
            command_id="",
            actor="runtime",
            correlation_id="",
            causation_id="",
            payload_json=cmd.payload_json,
            created_at="",
        )
        var unit = CommandUnit(cmd^)
        unit.events.append(event^)
        var units = List[CommandUnit]()
        units.append(unit^)
        _ = self.journal.append_batch(JournalBatch(run_id, units^))
        row.status = status
        self.runs[run_id] = row^

    def accept_impulse(mut self, impulse: Impulse, idempotency_key: String = "") raises:
        if impulse.run_id not in self.runs:
            raise Error("unknown run for impulse")
        var key = idempotency_key
        if key == "":
            key = "impulse.accept:" + impulse.id
        var cmd = CommandRecord(
            id=key,
            run_id=impulse.run_id,
            command_type="impulse.accept",
            idempotency_key=key,
            actor="runtime",
            correlation_id="",
            causation_id="",
            payload_json="{\"impulse_id\":\"" + impulse.id + "\"}",
            created_at="",
        )
        var event = EventRecord(
            id=key + ":event",
            run_id=impulse.run_id,
            event_type="impulse.accepted",
            schema_version=1,
            impulse_id=impulse.id,
            process_id="",
            sequence=0,
            command_id="",
            actor="runtime",
            correlation_id="",
            causation_id="",
            payload_json=cmd.payload_json,
            created_at="",
        )
        var unit = CommandUnit(cmd^)
        unit.events.append(event^)
        var units = List[CommandUnit]()
        units.append(unit^)
        var result = self.journal.append_batch(JournalBatch(impulse.run_id, units^))
        if not result.replayed:
            self.impulses[impulse.id] = impulse.copy()

    def schedule_process_plan(
        mut self, plan: CorrelationProcessPlan
    ) raises -> AppendResult:
        var key = plan.idempotency_key
        if key == "":
            key = "process.schedule:" + plan.id
        if self.journal.has_command(plan.run_id, key):
            var empty = List[CommandUnit]()
            return AppendResult(
                batch=JournalBatch(plan.run_id, empty^),
                replayed=True,
                units=List[CommandUnit](),
            )
        var status = ProcessStatus(plan.status)
        var process = ProcessRecord(
            id=plan.id,
            run_id=plan.run_id,
            status=status,
            priority=plan.priority,
            attempt=0,
            max_attempts=plan.max_attempts,
            available_at=0.0,
            created_at=self.clock,
        )
        var cmd = CommandRecord(
            id=key,
            run_id=plan.run_id,
            command_type="process.schedule",
            idempotency_key=key,
            actor="runtime",
            correlation_id="",
            causation_id="",
            payload_json="{\"process_id\":\"" + plan.id + "\"}",
            created_at="",
        )
        var event = EventRecord(
            id=key + ":event",
            run_id=plan.run_id,
            event_type="process.scheduled",
            schema_version=1,
            impulse_id="",
            process_id=plan.id,
            sequence=0,
            command_id="",
            actor="runtime",
            correlation_id="",
            causation_id="",
            payload_json=cmd.payload_json,
            created_at="",
        )
        var unit = CommandUnit(cmd^)
        unit.events.append(event^)
        var units = List[CommandUnit]()
        units.append(unit^)
        var result = self.journal.append_batch(JournalBatch(plan.run_id, units^))
        if not result.replayed:
            self.journal.seed_process(process)
            self.extras[plan.id] = ProcessExtra(
                effector_id=plan.effector_id,
                conduction=plan.conduction.copy(),
                input_json=plan.input_json,
                output_json="{}",
                config_json=plan.config_json,
                metadata_json=plan.metadata_json,
            )
        return result^

    def instantiate_path(
        mut self, path: CorrelationPathSpec, run_id: String, max_attempts: Int = 1
    ) raises -> CorrelationInstantiationPlan:
        var plan = instantiate_correlation_path(path, run_id, max_attempts=max_attempts)
        for index in range(len(plan.processes)):
            _ = self.schedule_process_plan(plan.processes[index].copy())
        return plan^

    def complete_process(
        mut self,
        process_id: String,
        actor: String,
        output_json: String,
        idempotency_key: String = "",
    ) raises:
        var process = self.journal.get_process(process_id)
        var finished = transition_process(
            process, ProcessStatus.succeeded(), actor
        )
        finished.lease_owner = ""
        finished.lease_expires_at = 0.0
        var key = idempotency_key
        if key == "":
            key = "process.complete:" + process_id + ":" + String(finished.attempt)
        var cmd = CommandRecord(
            id=key,
            run_id=finished.run_id,
            command_type="process.complete",
            idempotency_key=key,
            actor=actor,
            correlation_id="",
            causation_id="",
            payload_json="{\"process_id\":\"" + process_id + "\"}",
            created_at="",
        )
        var event = EventRecord(
            id=key + ":event",
            run_id=finished.run_id,
            event_type="process.completed",
            schema_version=1,
            impulse_id="",
            process_id=process_id,
            sequence=0,
            command_id="",
            actor=actor,
            correlation_id="",
            causation_id="",
            payload_json=cmd.payload_json,
            created_at="",
        )
        var unit = CommandUnit(cmd^)
        unit.events.append(event^)
        var units = List[CommandUnit]()
        units.append(unit^)
        _ = self.journal.append_batch(JournalBatch(finished.run_id, units^))
        self.journal.seed_process(finished)
        if process_id in self.extras:
            var extra = self.extras[process_id].copy()
            extra.output_json = output_json
            self.extras[process_id] = extra^

    def fail_process(
        mut self, process_id: String, actor: String, error_json: String = "{}"
    ) raises:
        var process = self.journal.get_process(process_id)
        var finished = transition_process(process, ProcessStatus.failed(), actor)
        finished.lease_owner = ""
        finished.lease_expires_at = 0.0
        var key = "process.fail:" + process_id + ":" + String(finished.attempt)
        var cmd = CommandRecord(
            id=key,
            run_id=finished.run_id,
            command_type="process.fail",
            idempotency_key=key,
            actor=actor,
            correlation_id="",
            causation_id="",
            payload_json=error_json,
            created_at="",
        )
        var event = EventRecord(
            id=key + ":event",
            run_id=finished.run_id,
            event_type="process.failed",
            schema_version=1,
            impulse_id="",
            process_id=process_id,
            sequence=0,
            command_id="",
            actor=actor,
            correlation_id="",
            causation_id="",
            payload_json=error_json,
            created_at="",
        )
        var unit = CommandUnit(cmd^)
        unit.events.append(event^)
        var units = List[CommandUnit]()
        units.append(unit^)
        _ = self.journal.append_batch(JournalBatch(finished.run_id, units^))
        self.journal.seed_process(finished)
        if process_id in self.extras:
            var extra = self.extras[process_id].copy()
            extra.output_json = error_json
            self.extras[process_id] = extra^

    def _execution_states(self, run_id: String) raises -> List[CorrelationExecutionState]:
        var states = List[CorrelationExecutionState]()
        for entry in self.journal.processes.items():
            var process = entry.value.copy()
            if process.run_id != run_id:
                continue
            var effector_id = ""
            var conduction = List[String]()
            var output_json = "{}"
            var input_json = "{}"
            if process.id in self.extras:
                var extra = self.extras[process.id].copy()
                effector_id = extra.effector_id
                conduction = extra.conduction.copy()
                output_json = extra.output_json
                input_json = extra.input_json
            states.append(
                CorrelationExecutionState(
                    process_id=process.id,
                    effector_id=effector_id,
                    status=process.status.value,
                    attempt=process.attempt,
                    max_attempts=process.max_attempts,
                    output_json=output_json,
                    input_json=input_json,
                    reactions_json="[]",
                    conduction=conduction^,
                )
            )
        return states^

    def advance(mut self, path: CorrelationPathSpec, run_id: String) raises -> CorrelationAdvancePlan:
        var states = self._execution_states(run_id)
        var plan = advance_correlation_states(path, states)
        for index in range(len(plan.readied)):
            var ready = plan.readied[index].copy()
            var process = self.journal.get_process(ready.id)
            if process.status != ProcessStatus.pending():
                continue
            process.status = ProcessStatus.ready()
            self.journal.seed_process(process)
            # Persist ready command
            var key = "process.ready:" + ready.id
            if not self.journal.has_command(run_id, key):
                var cmd = CommandRecord(
                    id=key,
                    run_id=run_id,
                    command_type="process.ready",
                    idempotency_key=key,
                    actor="runtime",
                    correlation_id="",
                    causation_id="",
                    payload_json="{\"process_id\":\"" + ready.id + "\"}",
                    created_at="",
                )
                var event = EventRecord(
                    id=key + ":event",
                    run_id=run_id,
                    event_type="process.readied",
                    schema_version=1,
                    impulse_id="",
                    process_id=ready.id,
                    sequence=0,
                    command_id="",
                    actor="runtime",
                    correlation_id="",
                    causation_id="",
                    payload_json=cmd.payload_json,
                    created_at="",
                )
                var unit = CommandUnit(cmd^)
                unit.events.append(event^)
                var units = List[CommandUnit]()
                units.append(unit^)
                _ = self.journal.append_batch(JournalBatch(run_id, units^))
            # Inject conduction into input when extras exist
            if ready.id in self.extras:
                var extra = self.extras[ready.id].copy()
                # Minimal conduction JSON: merge upstream outputs by effector id
                var conduction_body = "{"
                var first = True
                for entry in self.extras.items():
                    var up = entry.value.copy()
                    var up_id = entry.key
                    # match by effector_id in conduction list
                    var is_dep = False
                    for dep in extra.conduction:
                        if dep == up.effector_id:
                            is_dep = True
                    var up_status = self.journal.get_process(up_id).status.copy()
                    if is_dep and (up_status == ProcessStatus.succeeded() or up_status == ProcessStatus.failed() or up_status == ProcessStatus.cancelled() or up_status == ProcessStatus.timed_out()):
                        if not first:
                            conduction_body += ","
                        first = False
                        conduction_body += "\"" + up.effector_id + "\":" + up.output_json
                conduction_body += "}"
                extra.input_json = "{\"conduction\":" + conduction_body + "}"
                self.extras[ready.id] = extra^
        return plan^

    def claim_next(
        mut self, worker_id: String, run_id: String, lease_seconds: Float64 = 300.0
    ) raises -> ClaimResult:
        return self.journal.claim_next(
            ClaimRequest(worker_id, run_id, lease_seconds, False, self.clock)
        )
