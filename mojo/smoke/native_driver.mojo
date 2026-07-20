from std.collections import List

from fala.adapters import AdapterSpec, NativeFunctionRegistry
from fala.journal import NativeJournal, ProcessRow
from fala.status import ProcessStatus
from fala.native_driver import AdapterBinding, drive_once, maintain_process, run_until_idle, finalize_run, drive_bound_queue, persist_adapter_binding
from fala.processes import ProcessRecord, retry_backoff_seconds


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("native driver smoke: " + message)

def _native_bound(input_json: String, config_json: String) raises -> String:
    return "{\"bound\":true}"


def _native_idle(input_json: String, config_json: String) raises -> String:
    return "{\"ok\":true}"


def _one_process(mut journal: NativeJournal, run_id: String, process_id: String, max_attempts: Int = 2) raises -> ProcessRow:
    _ = journal.schedule_process(
        run_id,
        process_id,
        "native",
        "2026-01-01T00:00:00Z",
        "{}",
        "{}",
        "",
        1,
        max_attempts,
        "2026-01-01T00:00:00Z",
    )
    return journal.get_process(run_id, process_id)


def main() raises:
    # Pure retry policy is deterministic and capped before any durable write.
    var backoff_one = ProcessRecord(
        "backoff-one", "driver-retry", ProcessStatus.running(), 0, 1, 8, 0.0, 0.0, "", 0.0
    )
    var backoff_three = ProcessRecord(
        "backoff-three", "driver-retry", ProcessStatus.running(), 0, 3, 8, 0.0, 0.0, "", 0.0
    )
    var backoff_capped = ProcessRecord(
        "backoff-capped", "driver-retry", ProcessStatus.running(), 0, 8, 8, 0.0, 0.0, "", 0.0
    )
    _check(
        retry_backoff_seconds(backoff_one, 2.0, 5.0) == 2.0
            and retry_backoff_seconds(backoff_three, 2.0, 5.0) == 5.0
            and retry_backoff_seconds(backoff_capped, 2.0, 5.0) == 5.0,
        "retry backoff doubles deterministically and caps",
    )
    var rejected_backoff = False
    try:
        _ = retry_backoff_seconds(backoff_one, -1.0, 5.0)
    except err:
        rejected_backoff = True
    _check(rejected_backoff, "negative retry base is rejected")
    # Retry due time follows the reference default: absent backoff is due at
    # the transition timestamp, so the next bounded tick can reclaim it.
    var journal = NativeJournal(":memory:\0")
    journal.initialize()
    _ = journal.create_run("driver-retry", "active", "{}", "2026-01-01T00:00:00Z")
    var retry_row = _one_process(journal, "driver-retry", "retry", 2)
    var registry = NativeFunctionRegistry()
    var missing = AdapterSpec.native_function("native.missing")
    var first = drive_once(
        journal,
        retry_row,
        missing,
        "driver-worker",
        "2026-01-01T00:00:01Z",
        "2026-01-01T00:01:00Z",
        registry,
    )
    var waiting = journal.get_process("driver-retry", "retry")
    _check(
        first.failed
            and first.error.code == "native_function_not_registered"
            and first.failure_rows[0].status == "retry_wait"
            and waiting.status == "retry_wait"
            and waiting.attempt == 1
            and waiting.available_at == "2026-01-01T00:00:01Z"
            and waiting.error_json.find("native_function_not_registered") >= 0,
        "retry due timestamp and first persisted failure",
    )
    var second = drive_once(
        journal,
        waiting,
        missing,
        "driver-worker",
        "2026-01-01T00:00:02Z",
        "2026-01-01T00:01:00Z",
        registry,
    )
    var failed = journal.get_process("driver-retry", "retry")
    var retry_commands = journal.list_commands("driver-retry", "process.retry")
    var failed_commands = journal.list_commands("driver-retry", "process.fail")
    var retry_events = journal.list_events("driver-retry", "", "retry", -1, 0, "process.retry_scheduled")
    var failed_events = journal.list_events("driver-retry", "", "retry", -1, 0, "process.failed")
    _check(
        second.failed
            and second.failure_rows[0].status == "failed"
            and failed.status == "failed"
            and failed.attempt == 2
            and failed.error_json.find("native_function_not_registered") >= 0
            and len(retry_commands) == 1
            and len(failed_commands) == 1
            and len(retry_events) == 1
            and len(failed_events) == 1,
        "terminal failure and durable retry history",
    )

    # Empty caller bindings reload the explicit durable mapping and dispatch
    # through the typed native registry without inferring an adapter.
    _ = journal.create_run("driver-binding", "active", "{}", "2026-01-01T00:05:00Z")
    _ = _one_process(journal, "driver-binding", "bound", 1)
    var bound_adapter = AdapterSpec.native_function("native.bound")
    persist_adapter_binding(
        journal,
        AdapterBinding("bound", bound_adapter, "driver-binding"),
        "2026-01-01T00:05:01Z",
    )
    registry.register("native.bound", _native_bound)
    var reloaded_drive = drive_bound_queue(
        journal,
        List[AdapterBinding](),
        "binding-worker",
        "2026-01-01T00:05:02Z",
        "2026-01-01T00:06:00Z",
        1,
        registry,
        "driver-binding",
    )
    var bound_after = journal.get_process("driver-binding", "bound")
    _check(
        reloaded_drive.ticks == 1
            and reloaded_drive.completed
            and bound_after.status == "succeeded"
            and bound_after.output_json.find("\"bound\":true") >= 0,
        "reloaded registry binding dispatches and completes",
    )

    # Bounded native execution reports max_ticks while work remains, then
    # reports idle after the exact final tick drains the durable queue.
    _ = journal.create_run("driver-idle", "active", "{}", "2026-01-01T00:07:00Z")
    var idle_first_row = _one_process(journal, "driver-idle", "first", 1)
    var idle_second_row = _one_process(journal, "driver-idle", "second", 1)
    var idle_rows = List[ProcessRow]()
    var idle_adapters = List[AdapterSpec]()
    idle_rows.append(idle_first_row^)
    idle_adapters.append(AdapterSpec.native_function("native.idle")^)
    idle_rows.append(idle_second_row^)
    idle_adapters.append(AdapterSpec.native_function("native.idle")^)
    registry.register("native.idle", _native_idle)
    var bounded_idle = run_until_idle(
        journal,
        idle_rows,
        idle_adapters,
        "idle-worker",
        "2026-01-01T00:07:01Z",
        "2026-01-01T00:08:00Z",
        1,
        registry,
    )
    _check(
        not bounded_idle.ok
            and bounded_idle.ticks == 1
            and bounded_idle.stopped_reason == "max_ticks"
            and len(bounded_idle.completed) == 1
            and journal.get_process("driver-idle", "second").status == "ready",
        "bounded drive reports max_ticks with queued work",
    )
    var drained_idle = run_until_idle(
        journal,
        idle_rows,
        idle_adapters,
        "idle-worker",
        "2026-01-01T00:07:02Z",
        "2026-01-01T00:08:00Z",
        1,
        registry,
    )
    _check(
        drained_idle.ok
            and drained_idle.ticks == 1
            and drained_idle.stopped_reason == "idle"
            and len(drained_idle.completed) == 2
            and journal.get_process("driver-idle", "first").status == "succeeded"
            and journal.get_process("driver-idle", "second").status == "succeeded",
        "exact final tick drains queue and reports idle",
    )
    var empty_rows = List[ProcessRow]()
    var empty_adapters = List[AdapterSpec]()
    var empty_idle = run_until_idle(
        journal,
        empty_rows,
        empty_adapters,
        "idle-worker",
        "2026-01-01T00:07:03Z",
        "2026-01-01T00:08:00Z",
        1,
        registry,
    )
    _check(
        empty_idle.ok and empty_idle.ticks == 0 and empty_idle.stopped_reason == "idle",
        "empty drive is deterministic idle",
    )
    # The returned idle result carries the complete persisted wait graph, not only compact compatibility fields.
    _ = journal.create_run("driver-graph", "active", "{}", "2026-01-01T00:09:00Z")
    _ = journal.schedule_process("driver-graph", "cycle-a", "native", "2026-01-01T00:09:00Z", "{}", "{\"wait_for_processes\":[\"cycle-b\"]}", "", 0, 1)
    _ = journal.schedule_process("driver-graph", "cycle-b", "native", "2026-01-01T00:09:00Z", "{}", "{\"wait_for_processes\":[\"cycle-a\"]}", "", 0, 1)
    _ = journal.schedule_process("driver-graph", "unrelated-waiter", "native", "2026-01-01T00:09:00Z", "{}", "{\"wait_for_processes\":[\"missing\"]}", "", 0, 1)
    _ = journal.claim_process("driver-graph", "cycle-a", "graph-worker", "2026-01-01T00:09:01Z", "2026-01-01T00:10:00Z")
    _ = journal.wait_process("driver-graph", "cycle-a", "graph-worker", "2026-01-01T00:09:02Z")
    _ = journal.claim_process("driver-graph", "cycle-b", "graph-worker", "2026-01-01T00:09:01Z", "2026-01-01T00:10:00Z")
    _ = journal.wait_process("driver-graph", "cycle-b", "graph-worker", "2026-01-01T00:09:02Z")
    _ = journal.claim_process("driver-graph", "unrelated-waiter", "graph-worker", "2026-01-01T00:09:01Z", "2026-01-01T00:10:00Z")
    _ = journal.wait_process("driver-graph", "unrelated-waiter", "graph-worker", "2026-01-01T00:09:02Z")
    var graph_rows = List[ProcessRow]()
    graph_rows.append(journal.get_process("driver-graph", "cycle-a")^)
    graph_rows.append(journal.get_process("driver-graph", "cycle-b")^)
    graph_rows.append(journal.get_process("driver-graph", "unrelated-waiter")^)
    var graph_adapters = List[AdapterSpec]()
    graph_adapters.append(AdapterSpec.native_function("native.idle")^)
    graph_adapters.append(AdapterSpec.native_function("native.idle")^)
    graph_adapters.append(AdapterSpec.native_function("native.idle")^)
    var graph_idle = run_until_idle(
        journal, graph_rows, graph_adapters, "graph-worker",
        "2026-01-01T00:09:02Z", "2026-01-01T00:10:00Z", 1, registry
    )
    _check(
        graph_idle.deadlocked
            and len(graph_idle.deadlocks) == 1
            and len(graph_idle.deadlocks[0]) == 2
            and graph_idle.deadlocks[0][0] == "cycle-a"
            and graph_idle.deadlocks[0][1] == "cycle-b"
            and len(graph_idle.wait_diagnostic.wait_edges) == 3
            and graph_idle.wait_diagnostic.wait_edges["cycle-a"][0] == "cycle-b"
            and graph_idle.wait_diagnostic.wait_edges["cycle-b"][0] == "cycle-a"
            and graph_idle.wait_diagnostic.wait_edges["unrelated-waiter"][0] == "missing"
            and len(graph_idle.wait_diagnostic.blocked) == 3
            and graph_idle.wait_diagnostic.blocked[0].process_id == "cycle-a"
            and graph_idle.wait_diagnostic.blocked[1].process_id == "cycle-b"
            and graph_idle.wait_diagnostic.blocked[2].process_id == "unrelated-waiter",
        "run-until-idle returns the exact wait graph cycle and edges",
    )

    # Expired leases are first returned to retry_wait, then become terminal on
    # expiry of the final attempt. Both transitions must persist their errors.
    _ = journal.create_run("driver-lease", "active", "{}", "2026-01-01T00:10:00Z")
    var lease_row = _one_process(journal, "driver-lease", "lease", 2)
    var lease_claim = journal.claim_process(
        "driver-lease",
        "lease",
        "old-worker",
        "2026-01-01T00:10:01Z",
        "2026-01-01T00:10:02Z",
    )
    var lease_retry = maintain_process(
        journal,
        lease_claim,
        "old-worker",
        "2026-01-01T00:10:03Z",
        "2026-01-01T00:10:03Z",
    )
    _check(
        lease_retry.status == "retry_wait"
            and lease_retry.attempt == 1
            and lease_retry.available_at == "2026-01-01T00:10:03Z"
            and lease_retry.error_json.find("lease_expired") >= 0,
        "expired lease retries at transition time",
    )
    var lease_claim_two = journal.claim_process(
        "driver-lease",
        "lease",
        "old-worker",
        "2026-01-01T00:10:03Z",
        "2026-01-01T00:10:04Z",
    )
    var lease_failed = maintain_process(
        journal,
        lease_claim_two,
        "old-worker",
        "2026-01-01T00:10:05Z",
        "2026-01-01T00:10:05Z",
    )
    var lease_commands = journal.list_commands("driver-lease", "process.retry")
    var lease_fail_commands = journal.list_commands("driver-lease", "process.fail")
    var lease_fail_events = journal.list_events("driver-lease", "", "lease", -1, 0, "process.failed")
    _check(
        lease_failed.status == "failed"
            and lease_failed.attempt == 2
            and lease_failed.lease_owner == ""
            and lease_failed.lease_expires_at == ""
            and lease_failed.error_json.find("lease_expired") >= 0
            and len(lease_commands) == 1
            and len(lease_fail_commands) == 1
            and len(lease_fail_events) == 1,
        "lease expiry terminal failure is durable",
    )

    # Stop controls are side-effect free on an active run, while an already
    # terminal run takes precedence over the stop request.
    _ = journal.create_run("driver-stop", "active", "{}", "2026-01-01T00:20:00Z")
    var stop_row = _one_process(journal, "driver-stop", "queued", 1)
    var stop_rows = List[ProcessRow]()
    var stop_adapters = List[AdapterSpec]()
    stop_rows.append(stop_row^)
    stop_adapters.append(AdapterSpec.native_function("native.missing")^)
    var stopped = run_until_idle(
        journal,
        stop_rows,
        stop_adapters,
        "stop-worker",
        "2026-01-01T00:20:01Z",
        "2026-01-01T00:21:00Z",
        1,
        registry,
        stop=True,
    )
    _check(
        stopped.ok
            and stopped.ticks == 0
            and stopped.stopped_reason == "stopped"
            and journal.get_process("driver-stop", "queued").status == "ready",
        "active stop has exact precedence and no claim",
    )
    _ = journal.create_run("driver-terminal", "active", "{}", "2026-01-01T00:21:00Z")
    var terminal_row = _one_process(journal, "driver-terminal", "terminal", 1)
    _ = journal.claim_process("driver-terminal", "terminal", "terminal-worker", "2026-01-01T00:21:01Z", "2026-01-01T00:22:00Z")
    _ = journal.fail_process("driver-terminal", "terminal", "terminal-worker", "2026-01-01T00:21:02Z", "{\"code\":\"terminal\"}")
    _ = finalize_run(journal, "driver-terminal", "failed", 1, "2026-01-01T00:21:03Z")
    var terminal_rows = List[ProcessRow]()
    var terminal_adapters = List[AdapterSpec]()
    terminal_rows.append(terminal_row^)
    terminal_adapters.append(missing^)
    var terminal_stop = run_until_idle(
        journal,
        terminal_rows,
        terminal_adapters,
        "stop-worker",
        "2026-01-01T00:21:04Z",
        "2026-01-01T00:22:00Z",
        1,
        registry,
        stop=True,
    )
    _check(
        terminal_stop.ok
            and terminal_stop.ticks == 0
            and terminal_stop.stopped_reason == "already_terminal",
        "already-terminal precedence over stop",
    )
    print("native driver smoke success: retry backoff binding reload lease terminal stop precedence bounded max_ticks idle durable failures")
