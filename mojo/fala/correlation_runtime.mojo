"""Explicit native correlation run lifecycle orchestration.

This boundary intentionally accepts already-open durable state, a materialized
plan, explicit adapter bindings, and a native function registry.  It does not
invent transports, clocks, or adapter mappings, and it does not claim atomicity
across the individual journal operations.
"""

from std.collections import List

from fala.adapters import AdapterSpec, NativeFunctionRegistry
from fala.native_driver import AdapterBinding
from fala.correlation import CorrelationInstantiationPlan
from fala.correlation_persistence import (
    validate_correlation_persistence_plan,
    persist_correlation_plan,
)
from fala.journal import NativeJournal, ProcessRow
from fala.native_driver import (
    RunFinalizationResult,
    RunUntilIdleResult,
    drive_correlation_until_idle,
    finalize_run,
    persist_adapter_binding,
)
from fala.sqlite import SQLiteError
from fala.status import RunStatus


struct CorrelationRuntimeResult(Copyable, Movable):
    """Deterministic result of one explicit correlation lifecycle invocation."""

    var run_status: String
    var replayed: Bool
    var drive_result: RunUntilIdleResult
    var finalization_result: RunFinalizationResult

    def __init__(
        out self,
        run_status: String = "",
        replayed: Bool = False,
        drive_result: RunUntilIdleResult = RunUntilIdleResult(),
        finalization_result: RunFinalizationResult = RunFinalizationResult(),
    ):
        self.run_status = run_status
        self.replayed = replayed
        self.drive_result = drive_result.copy()
        self.finalization_result = finalization_result.copy()


def _binding_index(bindings: List[AdapterBinding], run_id: String, process_id: String) -> Int:
    """Return the sole deterministic binding index, or -1 when absent."""
    var found = -1
    for index in range(len(bindings)):
        if bindings[index].run_id == run_id and bindings[index].process_id == process_id:
            if found >= 0:
                return -2
            found = index
    return found


def _validate_bindings(plan: CorrelationInstantiationPlan, bindings: List[AdapterBinding]) raises:
    """Require exactly one explicit, valid binding for every planned process."""
    if len(bindings) != len(plan.processes):
        raise SQLiteError(code=2, message="correlation runtime: bindings must cover every plan process exactly once")
    for binding in bindings:
        if binding.run_id == "":
            raise SQLiteError(code=2, message="correlation runtime: binding run_id must not be empty")
        if binding.run_id != plan.run_id:
            raise SQLiteError(code=2, message="correlation runtime: binding run_id differs from plan")
        if binding.process_id == "":
            raise SQLiteError(code=2, message="correlation runtime: binding process_id must not be empty")
        var validation = binding.adapter.validate()
        if not validation.is_ok():
            raise SQLiteError(code=2, message=validation.message)
    for item in plan.processes:
        var index = _binding_index(bindings, plan.run_id, item.id)
        if index == -1:
            raise SQLiteError(code=2, message="correlation runtime: missing adapter binding for process " + item.id)
        if index == -2:
            raise SQLiteError(code=2, message="correlation runtime: duplicate adapter binding for process " + item.id)


def _terminal_result(mut journal: NativeJournal, run_id: String, status: String) raises -> CorrelationRuntimeResult:
    """Construct a copy-safe terminal replay without mutating durable state."""
    var rows = journal.list_processes(run_id)
    var completed = 0
    var failed = 0
    var waiting = 0
    var incomplete = 0
    for row in rows:
        if row.status == "succeeded":
            completed += 1
        elif row.status == "failed" or row.status == "cancelled" or row.status == "timed_out":
            failed += 1
        elif row.status == "waiting":
            waiting += 1
            incomplete += 1
        else:
            incomplete += 1
    var drive = RunUntilIdleResult(
        ok=True,
        ticks=0,
        stopped_reason="already_terminal",
        completed=List[ProcessRow](),
        failed=List[ProcessRow](),
        waiting=List[ProcessRow](),
        deadlocked=False,
        deadlocks=List[List[String]](),
    )
    var finalized = RunFinalizationResult(
        status=status,
        reason="already_terminal",
        already_terminal=True,
        total_count=len(rows),
        completed_count=completed,
        failed_count=failed,
        waiting_count=waiting,
        incomplete_count=incomplete,
    )
    return CorrelationRuntimeResult(
        run_status=status,
        replayed=True,
        drive_result=drive.copy(),
        finalization_result=finalized.copy(),
    )


def run_correlation_path(
    mut journal: NativeJournal,
    run_id: String,
    plan: CorrelationInstantiationPlan,
    bindings: List[AdapterBinding],
    registry: NativeFunctionRegistry,
    created_at: String,
    worker_id: String,
    now: String,
    lease_expires_at: String,
    max_ticks: Int,
    metadata: String = "{}",
) raises -> CorrelationRuntimeResult:
    """Create/replay, persist, drive, and finalize one native correlation path.

    The journal operations are deliberately sequential and independently
    durable; this wrapper does not claim atomic cross-database behavior.
    """
    if run_id == "":
        raise SQLiteError(code=2, message="correlation runtime: run_id must not be empty")
    if plan.run_id != run_id:
        raise SQLiteError(code=2, message="correlation runtime: plan run_id differs from run_id")
    if created_at == "" or worker_id == "" or now == "" or lease_expires_at == "":
        raise SQLiteError(code=2, message="correlation runtime: timestamps and worker_id must not be empty")
    if lease_expires_at <= now:
        raise SQLiteError(code=2, message="correlation runtime: lease_expires_at must be after now")
    if max_ticks < 1:
        raise SQLiteError(code=2, message="correlation runtime: max_ticks must be greater than zero")

    # get_run_record reports a missing row through this one narrow diagnostic;
    # every other SQLite failure is rethrown unchanged.
    var existing = RunStatus.created()
    var has_existing = False
    var existing_status = ""
    try:
        var record = journal.get_run_record(run_id)
        has_existing = True
        existing_status = record.status
        existing = RunStatus(record.status)
    except err:
        var detail = String(err)
        if detail.find("journal: run row not found") < 0:
            raise err^
    if has_existing and existing.is_terminal():
        # Terminal replay intentionally stops before any create/persist/drive
        # operation, but inputs are still validated at this API boundary.
        var terminal_plan_diagnostic = validate_correlation_persistence_plan(plan)
        if terminal_plan_diagnostic.code != "":
            raise Error(terminal_plan_diagnostic.__str__())
        _validate_bindings(plan, bindings)
        return _terminal_result(journal, run_id, existing_status)

    var plan_diagnostic = validate_correlation_persistence_plan(plan)
    if plan_diagnostic.code != "":
        raise Error(plan_diagnostic.__str__())
    _validate_bindings(plan, bindings)

    # Create only when the preflight lookup proved the run absent.  Existing
    # non-terminal runs resume their durable plan without a conflicting
    # run.create payload.
    if not has_existing:
        _ = journal.create_run(run_id, "created", metadata, created_at)
    _ = persist_correlation_plan(journal, plan, created_at)
    var processes = List[ProcessRow]()
    var adapters = List[AdapterSpec]()
    for item in plan.processes:
        var binding_index = _binding_index(bindings, run_id, item.id)
        var binding = bindings[binding_index].copy()
        persist_adapter_binding(journal, binding, now)
        var process = journal.get_process(run_id, item.id)
        processes.append(process^)
        adapters.append(binding.adapter.copy())

    var driven = drive_correlation_until_idle(
        journal,
        processes^,
        adapters^,
        worker_id,
        now,
        lease_expires_at,
        max_ticks,
        registry,
        plan,
    )
    var finalized = finalize_run(journal, run_id, driven.stopped_reason, max_ticks, now)
    var final_record = journal.get_run_record(run_id)
    return CorrelationRuntimeResult(
        run_status=final_record.status,
        replayed=False,
        drive_result=driven^,
        finalization_result=finalized^,
    )
