"""Multi-claim + multi-workspace composition smoke (no fleet)."""

from std.collections import List
from fala.adapters import AdapterSpec, NativeFunctionRegistry
from fala.journal import NativeJournal, ProcessRow
from fala.native_driver import drive_ready_batch
from fala.memory_driver import MemoryDriver
from fala.correlation import CorrelationEffectorSpec, CorrelationPathSpec
from fala.domain import Impulse
from fala.status import ProcessStatus
from std.os import remove


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("concurrency composition smoke: " + message)


def _static_ok(input_json: String, config_json: String) raises -> String:
    return "{\"ok\":true}"


def _clean(path: String):
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


def _multi_claim() raises:
    var path = "/tmp/fala-concurrency-claim.sqlite"
    _clean(path)
    var journal = NativeJournal.open(path)
    journal.initialize()
    var run_id = "run-claim"
    _ = journal.create_run(run_id, "created", "{}", "2026-01-01T00:00:00Z")
    _ = journal.transition_run_status(run_id, "active", "2026-01-01T00:00:01Z", "seed:active")
    _ = journal.schedule_process(
        run_id, run_id + ":a", "native_function", "2026-01-01T00:00:00Z",
        "{}", "{}", "", 10, 1,
    )
    _ = journal.schedule_process(
        run_id, run_id + ":b", "native_function", "2026-01-01T00:00:00Z",
        "{}", "{}", "", 5, 1,
    )
    var ordered = List[ProcessRow]()
    ordered.append(journal.get_process(run_id, run_id + ":a"))
    ordered.append(journal.get_process(run_id, run_id + ":b"))
    var adapters = List[AdapterSpec]()
    adapters.append(AdapterSpec.native_function("ok"))
    adapters.append(AdapterSpec.native_function("ok"))
    var registry = NativeFunctionRegistry()
    registry.register("ok", _static_ok)
    var batch = drive_ready_batch(
        journal,
        ordered,
        adapters,
        "worker-1",
        "2026-01-01T00:00:02Z",
        "2026-01-01T00:01:02Z",
        2,
        registry,
    )
    _check(batch.ticks == 2, "batch claimed both processes, ticks=" + String(batch.ticks))
    var after = journal.list_processes(run_id)
    var succeeded = 0
    for row in after:
        if row.status == "succeeded":
            succeeded += 1
    _check(succeeded == 2, "both processes succeeded via multi-claim")
    journal.close()
    _clean(path)


def _multi_workspace() raises:
    var left = MemoryDriver("memory://left")
    var right = MemoryDriver("memory://right")
    _ = left.runtime.create_run("run-left")
    _ = right.runtime.create_run("run-right")
    left.runtime.accept_impulse(Impulse("i-l", "run-left", "t", "{}"))
    right.runtime.accept_impulse(Impulse("i-r", "run-right", "t", "{}"))
    left.runtime.set_run_status("run-left", "active")
    right.runtime.set_run_status("run-right", "active")
    var le = CorrelationEffectorSpec.create("step", "work")
    var re = CorrelationEffectorSpec.create("step", "work")
    var left_effectors = List[CorrelationEffectorSpec]()
    left_effectors.append(le^)
    var right_effectors = List[CorrelationEffectorSpec]()
    right_effectors.append(re^)
    var left_path = CorrelationPathSpec("ws-left", left_effectors^)
    var right_path = CorrelationPathSpec("ws-right", right_effectors^)
    _ = left.runtime.instantiate_path(left_path, "run-left", max_attempts=1)
    _ = right.runtime.instantiate_path(right_path, "run-right", max_attempts=1)
    left.register_output("step", "{\"side\":\"left\"}")
    right.register_output("step", "{\"side\":\"right\"}")
    _ = left.drive_until_idle(left_path, "run-left")
    _ = right.drive_until_idle(right_path, "run-right")
    _check(left.runtime.journal.stream_id == "memory://left", "left journal identity")
    _check(right.runtime.journal.stream_id == "memory://right", "right journal identity")
    _check(
        left.runtime.journal.stream_id != right.runtime.journal.stream_id,
        "separate journals",
    )
    _ = ProcessStatus.succeeded()


def main() raises:
    _multi_claim()
    _multi_workspace()
    print("concurrency composition smoke ok: multi-claim multi-workspace")
