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
from fala.journal import NativeJournal, ProcessRow, RunRecord
from fala.native_driver import (
    RunFinalizationResult,
    RunUntilIdleResult,
    drive_correlation_until_idle,
    finalize_run,
    load_adapter_bindings,
    persist_adapter_bindings,
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


def _identity_fields_present(
    package_id: String,
    package_version: String,
    package_digest: String,
    correlation_path_id: String,
    correlation_path_digest: String,
    runtime_version: String,
    backend_version: String,
) -> Bool:
    return (
        package_id != ""
        and package_version != ""
        and package_digest != ""
        and correlation_path_id != ""
        and correlation_path_digest != ""
        and runtime_version != ""
        and backend_version != ""
    )


def _identity_matches_record(
    record: RunRecord,
    package_id: String,
    package_version: String,
    package_digest: String,
    correlation_path_id: String,
    correlation_path_digest: String,
    runtime_version: String,
    backend_version: String,
) -> Bool:
    return (
        record.id != ""
        and record.package_id == package_id
        and record.package_version == package_version
        and record.package_digest == package_digest
        and record.correlation_path_id == correlation_path_id
        and record.correlation_path_digest == correlation_path_digest
        and record.runtime_version == runtime_version
        and record.backend_version == backend_version
    )


def _require_run_identity(
    record: RunRecord,
    run_id: String,
    package_id: String,
    package_version: String,
    package_digest: String,
    correlation_path_id: String,
    correlation_path_digest: String,
    runtime_version: String,
    backend_version: String,
) raises:
    """Fail closed before drive/finalize when durable identity is missing or mismatched."""
    if record.id != run_id:
        raise SQLiteError(code=2, message="correlation runtime: durable run id mismatch")
    if not _identity_fields_present(
        package_id,
        package_version,
        package_digest,
        correlation_path_id,
        correlation_path_digest,
        runtime_version,
        backend_version,
    ):
        raise SQLiteError(code=2, message="correlation runtime: requested run identity is incomplete")
    if not _identity_fields_present(
        record.package_id,
        record.package_version,
        record.package_digest,
        record.correlation_path_id,
        record.correlation_path_digest,
        record.runtime_version,
        record.backend_version,
    ):
        raise SQLiteError(code=2, message="correlation runtime: durable run identity is incomplete")
    if not _identity_matches_record(
        record,
        package_id,
        package_version,
        package_digest,
        correlation_path_id,
        correlation_path_digest,
        runtime_version,
        backend_version,
    ):
        raise SQLiteError(code=2, message="correlation runtime: durable run identity mismatch")


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
    package_id: String = "",
    package_version: String = "",
    package_digest: String = "",
    correlation_path_id: String = "",
    correlation_path_digest: String = "",
    runtime_version: String = "",
    backend_version: String = "",
) raises -> CorrelationRuntimeResult:
    """Create/replay, persist, drive, and finalize one native correlation path.

    The journal operations are deliberately sequential and independently
    durable; this wrapper does not claim atomic cross-database behavior.
    Durable package/path/runtime identity is compared on every create and
    replay before scheduling, driving, or finalizing.
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

    var requested_path_id = correlation_path_id if correlation_path_id != "" else plan.correlation_path_id
    if requested_path_id == "" or plan.correlation_path_id == "":
        raise SQLiteError(code=2, message="correlation runtime: correlation_path_id must not be empty")
    if requested_path_id != plan.correlation_path_id:
        raise SQLiteError(code=2, message="correlation runtime: plan correlation_path_id differs from requested identity")

    # get_run_record reports a missing row through this one narrow diagnostic;
    # every other SQLite failure is rethrown unchanged.
    var existing = RunStatus.created()
    var has_existing = False
    var existing_status = ""
    var existing_record = RunRecord(
        id="",
        status="",
        title="",
        package_id="",
        package_version="",
        package_digest="",
        correlation_path_id="",
        correlation_path_digest="",
        runtime_version="",
        backend_version="",
        schema_version=0,
        metadata="",
        created_at="",
        updated_at="",
        started_at="",
        finished_at="",
    )
    try:
        existing_record = journal.get_run_record(run_id)
        has_existing = True
        existing_status = existing_record.status
        existing = RunStatus(existing_record.status)
    except err:
        var detail = String(err)
        if detail.find("journal: run row not found") < 0:
            raise err^
    if has_existing:
        # Terminal, non-terminal, and idempotent reuses all compare durable
        # identity before any further create/persist/drive/finalize work.
        _require_run_identity(
            existing_record,
            run_id,
            package_id,
            package_version,
            package_digest,
            requested_path_id,
            correlation_path_digest,
            runtime_version,
            backend_version,
        )
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
        if not _identity_fields_present(
            package_id,
            package_version,
            package_digest,
            requested_path_id,
            correlation_path_digest,
            runtime_version,
            backend_version,
        ):
            raise SQLiteError(code=2, message="correlation runtime: requested run identity is incomplete")
        _ = journal.create_run(
            run_id,
            "created",
            metadata,
            created_at,
            package_id=package_id,
            package_version=package_version,
            package_digest=package_digest,
            correlation_path_id=requested_path_id,
            correlation_path_digest=correlation_path_digest,
            runtime_version=runtime_version,
            backend_version=backend_version,
        )
        var created_record = journal.get_run_record(run_id)
        _require_run_identity(
            created_record,
            run_id,
            package_id,
            package_version,
            package_digest,
            requested_path_id,
            correlation_path_digest,
            runtime_version,
            backend_version,
        )
    _ = persist_correlation_plan(journal, plan, created_at)
    var processes = List[ProcessRow]()
    var adapters = List[AdapterSpec]()
    var persisted_bindings = List[AdapterBinding]()
    var first_host_drive = False
    if has_existing:
        persisted_bindings = load_adapter_bindings(journal, run_id)
        if len(persisted_bindings) == 0:
            first_host_drive = existing_status == "created"
            var existing_processes = journal.list_processes(run_id)
            if len(existing_processes) != len(plan.processes):
                first_host_drive = False
            for process in existing_processes:
                if process.status != "ready" and process.status != "pending":
                    first_host_drive = False
                if process.attempt != 0 or process.started_at != "" or process.finished_at != "" or process.lease_owner != "" or process.lease_expires_at != "":
                    first_host_drive = False
            if not first_host_drive:
                raise SQLiteError(code=1, message="correlation runtime: missing persisted adapter bindings after execution started")
        elif len(persisted_bindings) != len(plan.processes):
            raise SQLiteError(code=1, message="correlation runtime: persisted adapter bindings are incomplete")
    if not has_existing or first_host_drive:
        persist_adapter_bindings(journal, bindings, now)
    for item in plan.processes:
        var binding_index = _binding_index(bindings, run_id, item.id)
        var binding = bindings[binding_index].copy()
        if has_existing and not first_host_drive:
            binding_index = _binding_index(persisted_bindings, run_id, item.id)
            binding = persisted_bindings[binding_index].copy()
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
