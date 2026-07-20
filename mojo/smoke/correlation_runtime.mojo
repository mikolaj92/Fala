from std.os import remove
from std.collections import List

from fala import (
    AdapterBinding,
    AdapterSpec,
    CorrelationEffectorSpec,
    CorrelationPathSpec,
    NativeFunctionRegistry,
    NativeJournal,
    instantiate_correlation_path,
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
        var invalid_lease = run_correlation_path(
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
    var first = run_correlation_path(
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
    journal.close()

    var reopened = NativeJournal.open(path)
    reopened.initialize()
    var replay = run_correlation_path(
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
