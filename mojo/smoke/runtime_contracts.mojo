from std.collections import List

from fala.domain import Impulse
from fala.domain_store import NativeDomainStore
from fala.journal import NativeJournal, ProcessRow
from fala.adapters import AdapterSpec, NativeFunctionRegistry
from fala.native_driver import drive_once
from fala.schema import initialize_native_schema


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("runtime contracts smoke: " + message)


def _expect_error(mut store: NativeDomainStore, row: Impulse, key: String, at: String) raises -> Bool:
    var failed = False
    try:
        _ = store.accept_impulse(row, key, at)
    except err:
        failed = True
    return failed


def _expect_journal_error(mut journal: NativeJournal, run_id: String, process_id: String, actor: String, at: String) raises -> Bool:
    var failed = False
    try:
        _ = journal.complete_process(run_id, process_id, actor, at, "{\"done\":true}")
    except err:
        failed = True
    return failed


def main() raises:
    # Run-scoped impulse acceptance persists a typed row and acceptance event.
    var store = NativeDomainStore(":memory:\0")
    initialize_native_schema(store.db)
    _ = store.db.execute("INSERT INTO runs (id,status,metadata,created_at,updated_at,schema_version) VALUES ('run-scope','active','{}','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z',6)")
    store.db.commit()
    var impulse = Impulse(
        id="impulse-scope", run_id="run-scope", impulse_type="arbitration_case",
        payload="{\"claim_id\":\"CLM-1\"}", metadata="{\"tenant\":\"acme\"}",
        created_at="2026-01-01T00:00:01Z", updated_at="2026-01-01T00:00:01Z",
    )
    var accepted = store.accept_impulse(impulse, "impulse.accept:impulse-scope", "2026-01-01T00:00:01Z", "operator:mika")
    _check(accepted.impulse.run_id == "run-scope" and not accepted.replayed, "run-scoped impulse acceptance")
    var looked_up = store.get_impulse("run-scope", "impulse-scope")
    _check(looked_up.id == "impulse-scope" and looked_up.payload.find("CLM-1") >= 0, "impulse lookup preserves payload")
    var listed = store.list_impulse_records("run-scope", "", -1)
    _check(len(listed) == 1 and listed[0].id == "impulse-scope", "impulse list is isolated")
    var replay = store.accept_impulse(impulse, "impulse.accept:impulse-scope", "2026-01-01T00:00:01Z", "operator:mika")
    _check(replay.replayed and len(store.list_impulse_records("run-scope", "", -1)) == 1, "impulse acceptance replay is idempotent")
    _check(_expect_error(store, Impulse(id="impulse-foreign", run_id="run-other", impulse_type="case"), "impulse.accept:foreign", "2026-01-01T00:00:02Z"), "foreign impulse run rejected")

    # NativeJournal process lifecycle: deterministic queue order and boundaries.
    var journal = NativeJournal(":memory:\0")
    journal.initialize()
    _ = journal.create_run("run-process", "active", "{}", "2026-01-01T00:01:00Z")
    _ = journal.schedule_process("run-process", "process-low", "native", "2026-01-01T00:01:01Z", "{}", "{}", "", 1, 1, "2026-01-01T00:01:01Z")
    _ = journal.schedule_process("run-process", "process-high", "native", "2026-01-01T00:01:02Z", "{}", "{}", "", 5, 1, "2026-01-01T00:01:02Z")
    _ = journal.schedule_process("run-process", "process-high-tie", "native", "2026-01-01T00:01:03Z", "{}", "{}", "", 5, 1, "2026-01-01T00:01:02Z")
    var ordered = journal.list_processes("run-process")
    _check(len(ordered) == 3 and ordered[0].id == "process-high" and ordered[1].id == "process-high-tie" and ordered[2].id == "process-low", "priority and timestamp ordering")
    var claimed = journal.claim_process("run-process", "process-high", "worker-a", "2026-01-01T00:01:10Z", "2026-01-01T00:02:00Z", "claim:high")
    _check(claimed.status == "running" and claimed.attempt == 1 and claimed.lease_owner == "worker-a", "claim acquires lease and increments attempt")
    _check(_expect_journal_error(journal, "run-process", "process-high", "worker-b", "2026-01-01T00:01:11Z"), "complete rejects foreign lease actor")
    var completed = journal.complete_process("run-process", "process-high", "worker-a", "2026-01-01T00:01:12Z", "{\"done\":true}")
    _check(completed.status == "succeeded" and completed.output_json.find("done") >= 0 and completed.lease_owner == "", "complete releases lease and persists output")
    _check(_expect_journal_error(journal, "run-process", "process-high", "worker-a", "2026-01-01T00:01:13Z"), "terminal process cannot complete twice")

    # Retry is due at its supplied timestamp, then cancel is a terminal boundary.
    _ = journal.schedule_process("run-process", "process-retry", "native", "2026-01-01T00:02:00Z", "{}", "{}", "", 3, 2, "2026-01-01T00:02:00Z")
    var retry_claim = journal.claim_process("run-process", "process-retry", "worker-a", "2026-01-01T00:02:01Z", "2026-01-01T00:02:02Z")
    var retry_wait = journal.retry_process("run-process", retry_claim.id, "worker-a", "2026-01-01T00:02:03Z", "2026-01-01T00:02:05Z", "{\"code\":\"transient\"}")
    _check(retry_wait.status == "retry_wait" and retry_wait.attempt == 1 and retry_wait.available_at == "2026-01-01T00:02:05Z" and retry_wait.lease_owner == "", "retry persists due time and clears lease")
    var retry_claim_two = journal.claim_process("run-process", "process-retry", "worker-a", "2026-01-01T00:02:05Z", "2026-01-01T00:02:06Z")
    var cancelled = journal.cancel_process("run-process", retry_claim_two.id, "worker-a", "2026-01-01T00:02:07Z", "{\"reason\":\"operator\"}")
    _check(cancelled.status == "cancelled" and cancelled.attempt == 2 and cancelled.error_json.find("operator") >= 0 and cancelled.lease_owner == "", "cancel is terminal and durable")
    var retry_events = journal.list_events("run-process", "", "process-retry", -1, 0, "process.retry_scheduled")
    var cancel_events = journal.list_events("run-process", "", "process-retry", -1, 0, "process.cancelled")
    _check(len(retry_events) == 1 and len(cancel_events) == 1 and retry_events[0].sequence < cancel_events[0].sequence, "process event ordering is deterministic")

    # Unsupported Python execution reports a typed adapter error and never fabricates output.
    _ = journal.create_run("run-unsupported", "active", "{}", "2026-01-01T00:03:00Z")
    var unsupported_row = journal.schedule_process("run-unsupported", "process-python", "python_function", "2026-01-01T00:03:01Z", "{}", "{}", "", 0, 1, "2026-01-01T00:03:01Z")
    var registry = NativeFunctionRegistry()
    var unsupported = drive_once(journal, unsupported_row, AdapterSpec.python_function("tests.test_fala_driver._driver_double"), "worker-native", "2026-01-01T00:03:02Z", "2026-01-01T00:04:00Z", registry)
    var unsupported_after = journal.get_process("run-unsupported", "process-python")
    var unsupported_commands = journal.list_commands("run-unsupported", "process.fail", "", 0)
    var unsupported_events = journal.list_events("run-unsupported", "", "process-python", -1, 0, "process.failed")
    _check(unsupported.failed and unsupported.ticks == 1 and unsupported.error.code == "unsupported_python_function" and unsupported_after.status == "failed" and unsupported_after.attempt == 1 and unsupported_after.output_json == "{}" and unsupported_after.error_json.find("unsupported_python_function") >= 0 and len(unsupported_commands) == 1 and unsupported_commands[0].command_type == "process.fail" and len(unsupported_events) == 1 and unsupported_events[0].event_type == "process.failed", "unsupported execution has typed failure, claim, and durable failure event")

    print("runtime contracts smoke ok")
