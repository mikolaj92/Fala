from std.os import remove
from std.collections import List

from fala import (
    AdapterBinding,
    AdapterSpec,
    CorrelationEffectorSpec,
    CorrelationInstantiationPlan,
    CorrelationPathSpec,
    CorrelationRuntimeResult,
    NativeFunctionRegistry,
    NativeJournal,
    instantiate_correlation_path,
    load_adapter_bindings,
    persist_adapter_binding,
    persist_adapter_bindings,
    persist_correlation_plan,
    run_correlation_path,
)


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("correlation runtime smoke: " + message)


def _cleanup(path: String):
    try:
        remove(path)
    except err:
        pass
    try:
        remove(path + "-wal")
    except err:
        pass
    try:
        remove(path + "-shm")
    except err:
        pass


def _bindings(run_id: String, path_id: String) -> List[AdapterBinding]:
    var bindings = List[AdapterBinding]()
    bindings.append(AdapterBinding(path_id + ":root", AdapterSpec.native_function("native.correlation"), run_id))
    bindings.append(AdapterBinding(path_id + ":leaf", AdapterSpec.native_function("native.correlation"), run_id))
    return bindings^


# Deterministic durable identity used by this smoke. Digests are fixed strings
# (not content digests) so the test remains hermetic without package files.
comptime SMOKE_PACKAGE_ID: String = "smoke.correlation"
comptime SMOKE_PACKAGE_VERSION: String = "1"
comptime SMOKE_PACKAGE_DIGEST: String = "pkg-digest-smoke"
comptime SMOKE_PATH_DIGEST: String = "path-digest-smoke"
comptime SMOKE_RUNTIME_VERSION: String = "0.7.15"
comptime SMOKE_BACKEND_VERSION: String = "native-sqlite"


def _create_identified_run(mut journal: NativeJournal, run_id: String, path_id: String, metadata: String = "{}", created_at: String = "2026-01-01T00:00:00Z") raises:
    _ = journal.create_run(
        run_id,
        "created",
        metadata,
        created_at,
        package_id=SMOKE_PACKAGE_ID,
        package_version=SMOKE_PACKAGE_VERSION,
        package_digest=SMOKE_PACKAGE_DIGEST,
        correlation_path_id=path_id,
        correlation_path_digest=SMOKE_PATH_DIGEST,
        runtime_version=SMOKE_RUNTIME_VERSION,
        backend_version=SMOKE_BACKEND_VERSION,
    )


def _run_identified(
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
    return run_correlation_path(
        journal,
        run_id,
        plan,
        bindings,
        registry,
        created_at,
        worker_id,
        now,
        lease_expires_at,
        max_ticks,
        metadata,
        package_id=SMOKE_PACKAGE_ID,
        package_version=SMOKE_PACKAGE_VERSION,
        package_digest=SMOKE_PACKAGE_DIGEST,
        correlation_path_id=plan.correlation_path_id,
        correlation_path_digest=SMOKE_PATH_DIGEST,
        runtime_version=SMOKE_RUNTIME_VERSION,
        backend_version=SMOKE_BACKEND_VERSION,
    )


def _native_correlation(input_json: String, config_json: String) raises -> String:
    return "{\"value\":1}"



def main() raises:
    # This exercises the complete native wrapper boundary with only a typed
    # registry: create, persist plan/bindings, drive the DAG, finalize, reopen,
    # and replay a terminal run without duplicating durable rows.
    var path = "/tmp/fala-correlation-runtime-smoke.sqlite"
    _cleanup(path)

    var effectors = List[CorrelationEffectorSpec]()
    effectors.append(CorrelationEffectorSpec.create("root", "native")^)
    var upstream = List[String](); upstream.append("root")
    effectors.append(CorrelationEffectorSpec.create("leaf", "native", upstream^)^)
    var correlation_path = CorrelationPathSpec("runtime", effectors^)
    var plan = instantiate_correlation_path(correlation_path, "runtime-run")

    var registry = NativeFunctionRegistry()
    registry.register("native.correlation", _native_correlation)
    var bindings = _bindings("runtime-run", plan.correlation_path_id)

    var journal = NativeJournal.open(path)
    journal.initialize()
    var rejected_equal_lease = False
    var equal_lease_diagnostic = ""
    try:
        var invalid_lease = _run_identified(
            journal,
            "equal-lease-run",
            instantiate_correlation_path(correlation_path, "equal-lease-run"),
            bindings,
            registry,
            "2026-01-01T00:00:00Z",
            "runtime-worker",
            "2026-01-01T00:00:01Z",
            "2026-01-01T00:00:01Z",
            4,
        )
        _ = invalid_lease
    except err:
        rejected_equal_lease = True
        equal_lease_diagnostic = String(err)
    _check(rejected_equal_lease and equal_lease_diagnostic.find("lease_expires_at must be after now") >= 0, "equal lease rejected")
    var equal_run_missing = False
    try:
        var equal_run = journal.get_run_record("equal-lease-run")
        _ = equal_run
    except err:
        equal_run_missing = String(err).find("run row not found") >= 0
    _check(equal_run_missing, "equal lease does not persist run")
    # Correlation plans are durable process rows; explicit adapter bindings are
    # stored in the same rows under the reserved metadata marker.
    var equal_plan_rows = journal.db.query("SELECT COUNT(*) FROM processes WHERE run_id=?")
    equal_plan_rows.bind_text(1, "equal-lease-run")
    _check(equal_plan_rows.step() and equal_plan_rows.column_int(0) == 0, "equal lease does not persist correlation plan rows")
    equal_plan_rows.close()
    var equal_binding_rows = journal.db.query("SELECT COUNT(*) FROM processes WHERE run_id=? AND metadata LIKE '%\"__adapter_binding\"%'")
    equal_binding_rows.bind_text(1, "equal-lease-run")
    _check(equal_binding_rows.step() and equal_binding_rows.column_int(0) == 0, "equal lease does not persist adapter binding rows")
    equal_binding_rows.close()
    var first = _run_identified(
        journal,
        "runtime-run",
        plan,
        bindings,
        registry,
        "2026-01-01T00:00:00Z",
        "runtime-worker",
        "2026-01-01T00:00:01Z",
        "2026-01-01T00:01:00Z",
        4,
        "{\"source\":\"smoke\"}",
    )
    _check(not first.replayed and first.run_status == "completed", "initial run finalized")
    _check(first.drive_result.ticks == 2 and first.drive_result.stopped_reason == "idle", "bounded native DAG drained")
    var first_rows = journal.list_processes("runtime-run")
    _check(len(first_rows) == 2 and first_rows[0].status == "succeeded" and first_rows[1].status == "succeeded", "both process rows succeeded")
    # A producer may durably create the run and plan before the native host's
    # first drive. Zero bindings means the host has not bound the plan yet.
    var first_host_plan = instantiate_correlation_path(correlation_path, "first-host-run")
    var first_host_bindings = _bindings("first-host-run", first_host_plan.correlation_path_id)
    _create_identified_run(journal, "first-host-run", first_host_plan.correlation_path_id)
    _ = persist_correlation_plan(journal, first_host_plan, "2026-01-01T00:00:00Z")
    var first_host_drive = _run_identified(
        journal,
        "first-host-run",
        first_host_plan,
        first_host_bindings,
        registry,
        "2026-01-01T00:00:00Z",
        "runtime-worker",
        "2026-01-01T00:00:01Z",
        "2026-01-01T00:01:00Z",
        4,
    )
    _check(first_host_drive.run_status == "completed", "first host drive binds an existing unbound plan")
    # A mid-batch lookup failure must roll back earlier binding writes.
    var rollback_plan = instantiate_correlation_path(correlation_path, "rollback-run")
    var rollback_bindings = _bindings("rollback-run", rollback_plan.correlation_path_id)
    _create_identified_run(journal, "rollback-run", rollback_plan.correlation_path_id)
    _ = persist_correlation_plan(journal, rollback_plan, "2026-01-01T00:00:00Z")
    rollback_bindings[1].process_id = "missing-process"
    var rollback_rejected = False
    try:
        persist_adapter_bindings(journal, rollback_bindings, "2026-01-01T00:00:00Z")
    except err:
        rollback_rejected = True
    _check(rollback_rejected, "mid-batch missing process is rejected")
    var bindings_after_rollback = load_adapter_bindings(journal, "rollback-run")
    _check(len(bindings_after_rollback) == 0, "mid-batch failure rolls back the first binding")
    # Any non-zero incomplete binding set proves a partial prior host write and
    # must remain fail-closed rather than mixing old and current bindings.
    var partial_plan = instantiate_correlation_path(correlation_path, "partial-binding-run")
    var partial_bindings = _bindings("partial-binding-run", partial_plan.correlation_path_id)
    _create_identified_run(journal, "partial-binding-run", partial_plan.correlation_path_id)
    _ = persist_correlation_plan(journal, partial_plan, "2026-01-01T00:00:00Z")
    persist_adapter_binding(journal, partial_bindings[0], "2026-01-01T00:00:00Z")
    var partial_rejected = False
    var partial_diagnostic = ""
    try:
        var partial_drive = _run_identified(
            journal,
            "partial-binding-run",
            partial_plan,
            partial_bindings,
            registry,
            "2026-01-01T00:00:00Z",
            "runtime-worker",
            "2026-01-01T00:00:01Z",
            "2026-01-01T00:01:00Z",
            4,
        )
        _ = partial_drive
    except err:
        partial_rejected = True
        partial_diagnostic = String(err)
    _check(partial_rejected and partial_diagnostic.find("persisted adapter bindings are incomplete") >= 0, "partial persisted bindings fail closed")
    # Zero bindings after any execution evidence is corruption, not first drive.
    var missing_after_start_plan = instantiate_correlation_path(correlation_path, "missing-after-start-run")
    var missing_after_start_bindings = _bindings("missing-after-start-run", missing_after_start_plan.correlation_path_id)
    _create_identified_run(journal, "missing-after-start-run", missing_after_start_plan.correlation_path_id)
    _ = persist_correlation_plan(journal, missing_after_start_plan, "2026-01-01T00:00:00Z")
    var touched = journal.db.query("UPDATE processes SET attempt=1,started_at=?,updated_at=? WHERE run_id=? AND id=?")
    touched.bind_text(1, "2026-01-01T00:00:01Z")
    touched.bind_text(2, "2026-01-01T00:00:01Z")
    touched.bind_text(3, "missing-after-start-run")
    touched.bind_text(4, missing_after_start_plan.correlation_path_id + ":root")
    _ = touched.step()
    touched.close()
    var missing_after_start_rejected = False
    var missing_after_start_diagnostic = ""
    try:
        var missing_after_start_drive = _run_identified(
            journal,
            "missing-after-start-run",
            missing_after_start_plan,
            missing_after_start_bindings,
            registry,
            "2026-01-01T00:00:00Z",
            "runtime-worker",
            "2026-01-01T00:00:02Z",
            "2026-01-01T00:01:00Z",
            4,
        )
        _ = missing_after_start_drive
    except err:
        missing_after_start_rejected = True
        missing_after_start_diagnostic = String(err)
    _check(missing_after_start_rejected and missing_after_start_diagnostic.find("missing persisted adapter bindings after execution started") >= 0, "zero bindings after execution evidence fail closed")
    # Persist a non-terminal run before simulating a host restart.
    var resume_plan = instantiate_correlation_path(correlation_path, "resume-run")
    var persisted_resume_bindings = _bindings("resume-run", resume_plan.correlation_path_id)
    _create_identified_run(journal, "resume-run", resume_plan.correlation_path_id)
    _ = persist_correlation_plan(journal, resume_plan, "2026-01-01T00:00:00Z")
    for binding in persisted_resume_bindings:
        persist_adapter_binding(journal, binding, "2026-01-01T00:00:00Z")
    journal.close()

    var reopened = NativeJournal.open(path)
    reopened.initialize()
    # Replay uses the durable adapter contract even when the restarted host
    # resolves different current adapter settings.
    var changed_resume_bindings = List[AdapterBinding]()
    changed_resume_bindings.append(AdapterBinding(resume_plan.correlation_path_id + ":root", AdapterSpec.native_function("native.changed"), "resume-run"))
    changed_resume_bindings.append(AdapterBinding(resume_plan.correlation_path_id + ":leaf", AdapterSpec.native_function("native.changed"), "resume-run"))
    var resumed = _run_identified(
        reopened,
        "resume-run",
        resume_plan,
        changed_resume_bindings,
        registry,
        "2026-01-01T00:00:00Z",
        "restarted-worker",
        "2026-01-01T00:00:02Z",
        "2026-01-01T00:01:00Z",
        4,
    )
    _check(resumed.run_status == "completed" and resumed.drive_result.ticks == 2, "non-terminal replay drives persisted bindings")
    var replay = _run_identified(
        reopened,
        "runtime-run",
        plan,
        bindings,
        registry,
        "2026-01-01T00:00:00Z",
        "runtime-worker",
        "2026-01-01T00:00:01Z",
        "2026-01-01T00:01:00Z",
        4,
        "{\"source\":\"smoke\"}",
    )
    _check(replay.replayed and replay.run_status == "completed", "terminal replay marker")
    _check(replay.drive_result.ticks == 0 and replay.drive_result.stopped_reason == "already_terminal", "terminal replay does not drive")
    _check(len(reopened.list_processes("runtime-run")) == 2, "terminal replay does not duplicate rows")
    reopened.close()
    _cleanup(path)
    print("correlation runtime smoke ok: durable create drive finalize reopen replay")
