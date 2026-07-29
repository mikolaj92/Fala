from std.os import remove
from fala.journal import NativeJournal, ProcessRow, EventInput, CommandRow
from fala.runs import RunLifecycle
from fala.native_driver import drive_once, drive_until_idle, maintain_process, drive_all_runs, AdapterBinding, run_until_idle, diagnose_waits, observe_run_boundary, close_delegations, finalize_run
from fala.adapters import AdapterKind, AdapterSpec, NativeFunctionRegistry, EffectorResult, AdapterError, adapter_result_json
from fala.domain import Homeostat, RuntimeBudget
from fala.status import ProcessStatus, RunStatus, can_replay_terminal_process

from std.collections import List
def _native_echo(input_json: String, config_json: String) raises -> String:
    return "{\"ok\":true,\"metadata\":{\"user\":\"keep\"},\"adapter\":{\"user\":\"authored\"}}"

def _native_collision(input_json: String, config_json: String) raises -> String:
    return "{\"value\":2,\"adapter\":{\"user\":\"authored\"}}"
def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("native semantics probe: " + message)

def _cleanup(path: String):
    try:
        remove(path)
    except err:
        pass


def _expect_run_conflict(mut lifecycle: RunLifecycle) raises:
    var conflict = False
    try:
        _ = lifecycle.create(
            "run-replay",
            "2026-01-01T00:00:00Z",
            "{\"different\":true}",
            "conflict",
            "created",
            "create-other",
        )
    except err:
        conflict = True
    _check(conflict, "run creation conflict rejected")


def main() raises:
    # Run creation is idempotent for the same command key and rejects a
    # conflicting command for an already existing run.
    var lifecycle = RunLifecycle.open(":memory:\0")
    _check(ProcessStatus("succeeded").is_terminal() and not ProcessStatus("running").is_terminal(), "process terminal status predicates")
    _check(RunStatus("completed").is_terminal() and not RunStatus("active").is_terminal(), "run terminal status predicates")
    _check(can_replay_terminal_process(ProcessStatus("failed"), ProcessStatus("failed")) and not can_replay_terminal_process(ProcessStatus("failed"), ProcessStatus("cancelled")), "terminal replay predicate")
    lifecycle.initialize()
    var first = lifecycle.create(
        "run-replay",
        "2026-01-01T00:00:00Z",
        "{\"source\":\"probe\"}",
        "replay",
        "created",
        "create-once",
    )
    var replay = lifecycle.create(
        "run-replay",
        "2026-01-01T00:00:00Z",
        "{\"source\":\"probe\"}",
        "replay",
        "created",
        "create-once",
    )
    _check(first.id == replay.id and replay.status == "created", "run creation replay")
    var result_first = lifecycle.create_result("run-result", "2026-01-01T00:00:01Z", "{\"source\":\"first\"}", "result", "created", "result-once", actor="creator")
    var result_replay = lifecycle.create_result("run-result", "2026-01-01T00:00:02Z", "{\"source\":\"second\"}", "changed", "created", "result-once", actor="other")
    _check(not result_first.replayed and result_replay.replayed and result_replay.run.title == "result" and result_replay.command.id == result_first.command.id and result_replay.command.actor == "creator", "atomic create-result replay status")
    var record = lifecycle.get_record("run-replay")
    _check(record.id == "run-replay" and record.title == "replay", "run lifecycle record projection")
    _check(record.package_id == "" and record.finished_at == "", "run lifecycle nullable metadata")
    _expect_run_conflict(lifecycle)
    lifecycle.close()

    var journal = NativeJournal.open(":memory:\0")
    journal.initialize()

    # Fresh initialization must expose the core run table.
    var table_check = journal.db.query(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='runs'"
    )
    _check(table_check.step() and table_check.column_int(0) == 1, "fresh schema initialization")

    var run = journal.create_run(
        "run-semantics", "created", "{}", "2026-01-01T00:00:00Z"
    )
    _check(run.id == "run-semantics", "journal run creation")
    var journal_replay = journal.create_run(
        "run-semantics", "created", "{}", "2026-01-01T00:00:00Z"
    )
    _check(journal_replay.id == run.id, "journal run replay")
    var journal_conflict = False
    try:
        _ = journal.create_run(
            "run-semantics", "failed", "{}", "2026-01-01T00:00:00Z"
        )
    except err:
        journal_conflict = True
    _check(journal_conflict, "journal run creation conflict")
    var command = journal.append_command(
        "run-semantics", "command-1", "impulse.create", "key-1", "{}",
        "2026-01-01T00:00:00Z", "actor-1", "corr-1", "cause-0"
    )
    _check(command.command.id == "command-1" and not command.replayed, "event command seed")

    # Event metadata is persisted, SQL NULLs round-trip as empty optional
    # fields, and filters/limits retain deterministic sequence ordering.
    var event_one = journal.append_event(
        "run-semantics", "event-1", "impulse.created", "{\"n\":1}",
        "2026-01-01T00:00:01Z", "impulse-1", "process-1", "command-1", 2,
        "actor-1", "corr-1", "cause-1"
    )
    _check(
        event_one.sequence == 2 and event_one.actor == "actor-1"
            and event_one.correlation_id == "corr-1"
            and event_one.schema_version == 2,
        "event metadata",
    )
    var command_replay = journal.append_command(
        "run-semantics", "command-replay", "impulse.create", "key-1", "{}",
        "2026-01-01T00:00:00Z", "actor-1", "corr-1", "cause-0"
    )
    _check(command_replay.replayed and command_replay.command.id == "command-1", "command replay returns stored command")
    # Command/event batches are all-or-nothing: a conflicting event rolls back
    # both the command and any earlier events in the batch.
    var atomic_events = List[EventInput]()
    atomic_events.append(EventInput("atomic-first", "probe.atomic", "{\"n\":1}", "2026-01-01T00:00:04Z", "", "", 1, "", "", ""))
    atomic_events.append(EventInput("event-1", "impulse.created", "{\"conflict\":true}", "2026-01-01T00:00:04Z", "", "", 1, "", "", ""))
    var atomic_failed = False
    try:
        _ = journal.submit_command("run-semantics", "atomic-command", "probe.atomic", "atomic-key", "{}", "2026-01-01T00:00:04Z", atomic_events)
    except err:
        atomic_failed = True
    _check(atomic_failed, "atomic command conflict rejected")
    var atomic_command_count = journal.db.query("SELECT count(*) FROM runtime_commands WHERE run_id=? AND id=?")
    atomic_command_count.bind_text(1, "run-semantics"); atomic_command_count.bind_text(2, "atomic-command")
    var atomic_event_count = journal.db.query("SELECT count(*) FROM runtime_events WHERE run_id=? AND id=?")
    atomic_event_count.bind_text(1, "run-semantics"); atomic_event_count.bind_text(2, "atomic-first")
    _check(atomic_command_count.step() and atomic_command_count.column_int(0) == 0 and atomic_event_count.step() and atomic_event_count.column_int(0) == 0, "atomic command rollback")
    # Submission inherits command actor/correlation/causation when event fields
    # are omitted, while preserving the canonical command id on replay.
    var inherited_events = List[EventInput]()
    inherited_events.append(EventInput("inherited-event", "probe.inherited", "{\"ok\":true}", "2026-01-01T00:00:03Z", "", "", 1, "", "", ""))
    var inherited = journal.submit_command("run-semantics", "command-inherited", "probe.inherited", "inherited-key", "{}", "2026-01-01T00:00:03Z", inherited_events, "actor-inherited", "corr-inherited", "cause-inherited")
    _check(len(inherited.events) == 1 and inherited.events[0].actor == "actor-inherited" and inherited.events[0].correlation_id == "corr-inherited" and inherited.events[0].causation_id == "cause-inherited" and inherited.events[0].command_id == "command-inherited", "event metadata inheritance")
    var inherited_replay = journal.submit_command("run-semantics", "command-inherited-replay", "probe.inherited", "inherited-key", "{}", "2026-01-01T00:00:03Z", inherited_events, "actor-inherited", "corr-inherited", "cause-inherited")
    _check(inherited_replay.replayed and inherited_replay.command.id == "command-inherited" and len(inherited_replay.events) == 0, "alternate command id replay")
    var conflicting_key = False
    try:
        var replay_changed = journal.submit_command("run-semantics", "command-conflict", "probe.other", "inherited-key", "{\"changed\":true}", "2026-01-01T00:00:03Z", List[EventInput](), "actor-inherited", "corr-inherited", "cause-inherited")
        conflicting_key = replay_changed.replayed and replay_changed.command.id == "command-inherited"
    except err:
        conflicting_key = False
    _check(conflicting_key, "submit replay ignores changed caller fields")
    var event_two = journal.append_event(
        "run-semantics", "event-2", "process.started", "{\"n\":2}",
        "2026-01-01T00:00:02Z", "", "process-2", "", 1, "", "", ""
    )
    var event_three = journal.append_event(
        "run-semantics", "event-3", "process.finished", "{\"n\":3}",
        "2026-01-01T00:00:03Z", "impulse-1", "process-1", "", 1, "", "", ""
    )
    _check(event_two.impulse_id == "" and event_two.command_id == "", "event NULL roundtrip")
    var filtered = journal.list_events("run-semantics", "impulse-1", "process-1", -1, 1)
    _check(len(filtered) == 1 and filtered[0].id == "event-1", "event filter and limit")
    var all_events = journal.list_events("run-semantics", "", "", 3, 0)
    _check(len(all_events) == 2 and all_events[0].id == "event-2" and all_events[1].id == "event-3", "event sequence filter")
    _ = event_three

    # Queue order is priority descending, then available/created time, then id.
    _ = journal.schedule_process(
        "run-semantics", "fifo-low", "native", "2026-01-01T00:00:00Z",
        "{}", "{}", "", 1, 1, "2026-01-01T00:00:00Z"
    )
    _ = journal.schedule_process(
        "run-semantics", "fifo-new", "native", "2026-01-01T00:00:02Z",
        "{}", "{}", "", 9, 1, "2026-01-01T00:00:02Z"
    )
    _ = journal.schedule_process(
        "run-semantics", "fifo-old", "native", "2026-01-01T00:00:01Z",
        "{}", "{}", "", 9, 1, "2026-01-01T00:00:01Z"
    )
    var queued = journal.list_processes("run-semantics", "ready", "")
    _check(len(queued) == 3 and queued[0].id == "fifo-old" and queued[1].id == "fifo-new", "process priority FIFO ordering")
    var claimed = journal.claim_process(
        "run-semantics", "fifo-old", "worker-a", "2026-01-01T00:00:04Z", "2026-01-01T00:01:00Z"
    )
    _check(claimed.status == "running" and claimed.attempt == 1 and claimed.lease_owner == "worker-a", "process claim")
    var completed = journal.complete_process(
        "run-semantics", "fifo-old", "worker-a", "2026-01-01T00:00:05Z", "{\"ok\":true}"
    )
    _check(completed.status == "succeeded" and completed.output_json == "{\"ok\":true}", "process complete")
    # Claim replay fingerprints include the requested lease expiry for both direct and queue claims.
    _ = journal.schedule_process(
        "run-semantics", "claim-replay-lease", "native", "2026-01-01T00:00:05Z",
        "{}", "{}", "", 50, 1, "2026-01-01T00:00:05Z"
    )
    var claim_replay_first = journal.claim_process(
        "run-semantics", "claim-replay-lease", "worker-lease", "2026-01-01T00:00:06Z", "2026-01-01T00:01:00Z", "claim-replay-lease-key"
    )
    var claim_replay_commands_before = journal.db.query("SELECT count(*) FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
    claim_replay_commands_before.bind_text(1, "run-semantics"); claim_replay_commands_before.bind_text(2, "claim-replay-lease-key")
    var claim_replay_events_before = journal.list_events("run-semantics", "", "claim-replay-lease", -1, 0, "process.claimed")
    var claim_replay_lease_conflict = False
    try:
        _ = journal.claim_process(
            "run-semantics", "claim-replay-lease", "worker-lease", "2026-01-01T00:00:06Z", "2026-02-01T00:01:00Z", "claim-replay-lease-key"
        )
    except err:
        claim_replay_lease_conflict = True
    var claim_replay_after = journal.get_process("run-semantics", "claim-replay-lease")
    var claim_replay_events_after = journal.list_events("run-semantics", "", "claim-replay-lease", -1, 0, "process.claimed")
    _check(
        claim_replay_first.lease_expires_at == "2026-01-01T00:01:00Z"
        and claim_replay_lease_conflict
        and claim_replay_after.status == "running"
        and claim_replay_after.attempt == claim_replay_first.attempt
        and claim_replay_after.lease_owner == "worker-lease"
        and claim_replay_after.lease_expires_at == "2026-01-01T00:01:00Z"
        and claim_replay_commands_before.step() and claim_replay_commands_before.column_int(0) == 1
        and len(claim_replay_events_before) == 1 and len(claim_replay_events_after) == len(claim_replay_events_before),
        "direct claim lease replay conflict is atomic",
    )
    _ = journal.create_run("claim-queue-replay", "active", "{}", "2026-01-01T00:00:06Z")
    _ = journal.schedule_process(
        "claim-queue-replay", "queue-claim-replay-lease", "native", "2026-01-01T00:00:06Z",
        "{}", "{}", "", 50, 1, "2026-01-01T00:00:06Z"
    )
    _ = journal.claim_next_ready(
        "claim-queue-replay", "worker-queue", "2026-01-01T00:00:07Z", "2026-01-01T00:02:00Z", "queue-claim-replay-lease-key"
    )
    var queue_claim_commands_before = journal.db.query("SELECT count(*) FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
    queue_claim_commands_before.bind_text(1, "claim-queue-replay"); queue_claim_commands_before.bind_text(2, "queue-claim-replay-lease-key")
    var queue_claim_events_before = journal.list_events("claim-queue-replay", "", "queue-claim-replay-lease", -1, 0, "process.claimed")
    var queue_claim_lease_conflict = False
    try:
        _ = journal.claim_next_ready(
            "claim-queue-replay", "worker-queue", "2026-01-01T00:00:07Z", "2026-02-01T00:02:00Z", "queue-claim-replay-lease-key"
        )
    except err:
        queue_claim_lease_conflict = True
    var queue_claim_after = journal.get_process("claim-queue-replay", "queue-claim-replay-lease")
    var queue_claim_events_after = journal.list_events("claim-queue-replay", "", "queue-claim-replay-lease", -1, 0, "process.claimed")
    _check(
        queue_claim_lease_conflict
        and queue_claim_after.status == "running"
        and queue_claim_after.attempt == 1
        and queue_claim_after.lease_owner == "worker-queue"
        and queue_claim_after.lease_expires_at == "2026-01-01T00:02:00Z"
        and queue_claim_commands_before.step() and queue_claim_commands_before.column_int(0) == 1
        and len(queue_claim_events_before) == 1 and len(queue_claim_events_after) == len(queue_claim_events_before),
        "queue claim lease replay conflict is atomic",
    )
    # Malformed direct transition payloads reject before mutation; a valid keyed
    # transition then replays without adding another command.
    _ = journal.schedule_process(
        "run-semantics", "malformed-direct", "native", "2026-01-01T00:00:05Z",
        "{}", "{}", "", 1, 1, "2026-01-01T00:00:05Z"
    )
    var malformed_direct_claim = journal.claim_process(
        "run-semantics", "malformed-direct", "worker-a", "2026-01-01T00:00:05Z", "2026-01-01T00:01:00Z"
    )
    var malformed_direct = False
    try:
        _ = journal.wait_process(malformed_direct_claim.run_id, malformed_direct_claim.id, "worker-a", "2026-01-01T00:00:06Z", "{bad-json", "malformed-direct-wait")
    except err:
        malformed_direct = True
    var malformed_direct_row = journal.get_process("run-semantics", "malformed-direct")
    _check(malformed_direct and malformed_direct_row.status == "running" and malformed_direct_row.output_json == "{}" and malformed_direct_row.error_json == "{}", "malformed direct transition does not mutate")
    var direct_wait = journal.wait_process("run-semantics", "malformed-direct", "worker-a", "2026-01-01T00:00:06Z", "{\"paused\":true}", "malformed-direct-wait")
    var direct_wait_replay = journal.wait_process("run-semantics", "malformed-direct", "worker-a", "2026-01-01T00:00:06Z", "{\"paused\":true}", "malformed-direct-wait")
    _check(direct_wait.status == "waiting" and direct_wait_replay.status == "waiting", "valid direct transition replay")
    _ = journal.schedule_process(
        "run-semantics", "command-malformed", "native", "2026-01-01T00:00:36Z",
        "{}", "{}", "", 1, 1, "2026-01-01T00:00:36Z"
    )
    var command_malformed_claim = journal.claim_process(
        "run-semantics", "command-malformed", "command-worker", "2026-01-01T00:00:36Z", "2026-01-01T00:01:00Z"
    )
    var malformed_command = False
    try:
        _ = journal.transition_process_with_command(
            command_malformed_claim.run_id, command_malformed_claim.id, "waiting",
            CommandRow(run_id="run-semantics", id="command-malformed-wait", command_type="process.wait", idempotency_key="command-malformed-wait", actor="command-worker", correlation_id="", causation_id="", payload="{\"process_id\":\"command-malformed\",\"attempt\":1}", created_at="2026-01-01T00:00:37Z"),
            "{\"paused\":true}", "{bad-json"
        )
    except err:
        malformed_command = True
    var malformed_command_row = journal.get_process("run-semantics", "command-malformed")
    _check(malformed_command and malformed_command_row.status == "running" and malformed_command_row.output_json == "{}" and malformed_command_row.error_json == "{}", "malformed command transition does not mutate")
    var command_wait = journal.transition_process_with_command(
        command_malformed_claim.run_id, command_malformed_claim.id, "waiting",
        CommandRow(run_id="run-semantics", id="command-malformed-wait", command_type="process.wait", idempotency_key="command-malformed-wait", actor="command-worker", correlation_id="", causation_id="", payload="{\"process_id\":\"command-malformed\",\"attempt\":1}", created_at="2026-01-01T00:00:37Z"),
        "{\"paused\":true}", "{}"
    )
    var command_wait_replay = journal.transition_process_with_command(
        command_malformed_claim.run_id, command_malformed_claim.id, "waiting",
        CommandRow(run_id="run-semantics", id="command-malformed-replay", command_type="process.wait", idempotency_key="command-malformed-wait", actor="command-worker", correlation_id="", causation_id="", payload="{\"process_id\":\"command-malformed\",\"attempt\":1}", created_at="2026-01-01T00:00:37Z"),
        "{\"paused\":true}", "{}"
    )
    _check(command_wait.process.status == "waiting" and command_wait_replay.submission.replayed, "valid command transition replay")
    # Ready transitions have a reference-specific process_id-only payload.
    var ready_process = ProcessRow(run_id="run-semantics", id="command-ready", process_type="native", impulse_id="", status="pending", priority=1, attempt=0, max_attempts=1, available_at="2026-01-01T00:00:37Z", lease_owner="", lease_expires_at="", input_json="{}", output_json="{}", error_json="{}", metadata="{}", created_at="2026-01-01T00:00:37Z", updated_at="2026-01-01T00:00:37Z", started_at="", finished_at="", output_schema_json="{}")
    _ = journal.schedule_process_with_command(
        ready_process,
        CommandRow(run_id="run-semantics", id="command-ready-schedule", command_type="process.schedule", idempotency_key="command-ready-schedule", actor="scheduler", correlation_id="", causation_id="", payload="{\"process_id\":\"command-ready\"}", created_at="2026-01-01T00:00:37Z")
    )
    var ready_once = journal.transition_process_with_command(
        "run-semantics", "command-ready", "ready",
        CommandRow(run_id="run-semantics", id="command-ready-once", command_type="process.ready", idempotency_key="command-ready-key", actor="scheduler", correlation_id="corr-ready", causation_id="cause-ready", payload="{\"process_id\":\"command-ready\"}", created_at="2026-01-01T00:00:38Z")
    )
    var ready_replay = journal.transition_process_with_command(
        "run-semantics", "command-ready", "ready",
        CommandRow(run_id="run-semantics", id="command-ready-replay", command_type="process.ready", idempotency_key="command-ready-key", actor="scheduler", correlation_id="corr-ready", causation_id="cause-ready", payload="{\"process_id\":\"command-ready\"}", created_at="2026-01-01T00:00:38Z")
    )
    var ready_process_conflict = False
    try:
        _ = journal.transition_process_with_command(
            "run-semantics", "command-ready", "ready",
            CommandRow(run_id="run-semantics", id="command-ready-process-conflict", command_type="process.ready", idempotency_key="command-ready-key", actor="scheduler", correlation_id="corr-ready", causation_id="cause-ready", payload="{\"process_id\":\"other-process\"}", created_at="2026-01-01T00:00:38Z")
        )
    except err:
        ready_process_conflict = True
    var ready_extra_conflict = False
    try:
        _ = journal.transition_process_with_command(
            "run-semantics", "command-ready", "ready",
            CommandRow(run_id="run-semantics", id="command-ready-extra-conflict", command_type="process.ready", idempotency_key="command-ready-key", actor="scheduler", correlation_id="corr-ready", causation_id="cause-ready", payload="{\"process_id\":\"command-ready\",\"extra\":true}", created_at="2026-01-01T00:00:38Z")
        )
    except err:
        ready_extra_conflict = True
    var ready_row = journal.get_process("run-semantics", "command-ready")
    _check(
        ready_once.process.status == "ready"
        and ready_replay.process.status == "ready"
        and ready_replay.submission.replayed
        and ready_process_conflict and ready_extra_conflict
        and ready_row.status == "ready"
        and ready_row.input_json == "{}",
        "ready command replay identity and conflicts",
    )
    # Keyed retry transitions replay the stored command and reject changed
    # actor/timestamp identity without mutating the process or audit rows.
    _ = journal.schedule_process(
        "run-semantics", "command-retry", "native", "2026-01-01T00:00:38Z",
        "{}", "{}", "", 1, 2, "2026-01-01T00:00:38Z"
    )
    var command_retry_claim = journal.claim_process(
        "run-semantics", "command-retry", "command-worker", "2026-01-01T00:00:38Z", "2026-01-01T00:01:00Z"
    )
    var command_retry = journal.transition_process_with_command(
        command_retry_claim.run_id, command_retry_claim.id, "retry_wait",
        CommandRow(run_id="run-semantics", id="command-retry-once", command_type="process.retry", idempotency_key="command-retry-once", actor="command-worker", correlation_id="", causation_id="", payload="{\"process_id\":\"command-retry\",\"attempt\":1}", created_at="2026-01-01T00:00:39Z"),
        "{}", "{\"retry\":true}", "2026-01-01T00:00:40Z"
    )
    var command_retry_replay = journal.transition_process_with_command(
        command_retry_claim.run_id, command_retry_claim.id, "retry_wait",
        CommandRow(run_id="run-semantics", id="command-retry-replay", command_type="process.retry", idempotency_key="command-retry-once", actor="command-worker", correlation_id="", causation_id="", payload="{\"process_id\":\"command-retry\",\"attempt\":1}", created_at="2026-01-01T00:00:39Z"),
        "{}", "{\"changed\":true}", "2026-01-01T00:00:40Z"
    )
    var command_retry_actor_conflict = False
    try:
        _ = journal.transition_process_with_command(
            command_retry_claim.run_id, command_retry_claim.id, "retry_wait",
            CommandRow(run_id="run-semantics", id="command-retry-actor", command_type="process.retry", idempotency_key="command-retry-once", actor="other-worker", correlation_id="", causation_id="", payload="{\"process_id\":\"command-retry\",\"attempt\":1}", created_at="2026-01-01T00:00:39Z"),
            "{}", "{\"retry\":true}", "2026-01-01T00:00:40Z"
        )
    except err:
        command_retry_actor_conflict = True
    var command_retry_time_conflict = False
    try:
        _ = journal.transition_process_with_command(
            command_retry_claim.run_id, command_retry_claim.id, "retry_wait",
            CommandRow(run_id="run-semantics", id="command-retry-time", command_type="process.retry", idempotency_key="command-retry-once", actor="command-worker", correlation_id="", causation_id="", payload="{\"process_id\":\"command-retry\",\"attempt\":1}", created_at="2026-01-01T00:00:41Z"),
            "{}", "{\"retry\":true}", "2026-01-01T00:00:40Z"
        )
    except err:
        command_retry_time_conflict = True
    var command_retry_payload_conflict = False
    try:
        _ = journal.transition_process_with_command(
            command_retry_claim.run_id, command_retry_claim.id, "retry_wait",
            CommandRow(run_id="run-semantics", id="command-retry-payload", command_type="process.retry", idempotency_key="command-retry-once", actor="command-worker", correlation_id="", causation_id="", payload="{\"process_id\":\"command-retry\",\"attempt\":1,\"different\":true}", created_at="2026-01-01T00:00:39Z"),
            "{}", "{\"retry\":true}", "2026-01-01T00:00:40Z"
        )
    except err:
        command_retry_payload_conflict = True
    var command_retry_row = journal.get_process("run-semantics", "command-retry")
    var command_retry_commands = journal.list_commands("run-semantics", "process.retry")
    var command_retry_count = 0
    for item in command_retry_commands:
        if item.idempotency_key == "command-retry-once": command_retry_count += 1
    _check(
        command_retry.process.status == "retry_wait"
        and command_retry_replay.process.status == "retry_wait"
        and command_retry_replay.submission.replayed
        and command_retry_actor_conflict and command_retry_time_conflict and command_retry_payload_conflict
        and command_retry_row.status == "retry_wait"
        and command_retry_row.attempt == 1
        and command_retry_count == 1,
        "keyed retry replay and conflicts are atomic",
    )



    # Explicit retry transitions to retry_wait, becomes claimable at its due
    # time, and can then complete on the next attempt.
    _ = journal.schedule_process(
        "run-semantics", "retry-me", "native", "2026-01-01T00:00:06Z",
        "{}", "{}", "", 4, 2, "2026-01-01T00:00:06Z"
    )
    var retry_claim = journal.claim_process(
        "run-semantics", "retry-me", "worker-b", "2026-01-01T00:00:06Z", "2026-01-01T00:00:07Z"
    )
    var retry_wait = journal.retry_process(
        "run-semantics", "retry-me", "worker-b", "2026-01-01T00:00:06Z", "2026-01-01T00:00:10Z", "{\"retry\":true}"
    )
    var retry_due_conflict = False
    try:
        _ = journal.retry_process("run-semantics", "retry-me", "worker-b", "2026-01-01T00:00:06Z", "2026-01-01T00:00:11Z", "{\"retry\":true}")
    except err:
        retry_due_conflict = True
    _check(retry_due_conflict, "retry replay available_at conflict")
    var retry_command = journal.get_command_by_idempotency("run-semantics", "process.retry-me:retry_wait:2026-01-01T00:00:06Z")
    var retry_events = journal.list_events("run-semantics", "", "retry-me", -1, 0, "process.retry_scheduled")
    _check(retry_claim.attempt == 1 and retry_wait.status == "retry_wait" and retry_wait.available_at == "2026-01-01T00:00:10Z" and retry_command.payload.find("\"process_id\":\"retry-me\"") >= 0 and retry_command.payload.find("\"attempt\":1") >= 0 and retry_command.payload.find("\"output\":{}") >= 0 and retry_command.payload.find("\"error\":{\"retry\":true}") >= 0 and retry_command.payload.find("\"available_at\":\"2026-01-01T00:00:10Z\"") >= 0 and len(retry_events) == 1 and retry_events[0].payload.find("\"process_id\":\"retry-me\"") >= 0 and retry_events[0].payload.find("\"attempt\":1") >= 0 and retry_events[0].payload.find("\"output\":{}") >= 0 and retry_events[0].payload.find("\"error\":{\"retry\":true}") >= 0 and retry_events[0].payload.find("\"available_at\":\"2026-01-01T00:00:10Z\"") >= 0, "retry payload fields")
    var retry_again = journal.claim_process(
        "run-semantics", "retry-me", "worker-b", "2026-01-01T00:00:10Z", "2026-01-01T00:00:11Z"
    )
    _check(retry_again.attempt == 2, "retry claim")
    var retry_historical_replay = journal.retry_process(
        "run-semantics", "retry-me", "worker-b", "2026-01-01T00:00:06Z", "2026-01-01T00:00:10Z", "{\"retry\":true}"
    )
    _check(retry_historical_replay.status == "running" and retry_historical_replay.attempt == 2, "retry replay after next claim")
    var retry_done = journal.complete_process(
        "run-semantics", "retry-me", "worker-b", "2026-01-01T00:00:12Z"
    )
    _check(retry_done.status == "succeeded", "retry completion")
    # Process cancellation requests are durable, idempotent lifecycle transitions.
    _ = journal.schedule_process(
        "run-semantics", "cancel-request-boundary", "native", "2026-01-01T00:00:24Z",
        "{}", "{}", "", 1, 1, "2026-01-01T00:00:24Z"
    )
    var cancel_request_first = journal.request_cancel_process(
        "run-semantics", "cancel-request-boundary", "operator", "2026-01-01T00:00:24Z",
        "{\"reason\":\"operator\"}", "cancel-request-boundary-key"
    )
    var cancel_request_replay = journal.request_cancel_process(
        "run-semantics", "cancel-request-boundary", "operator", "2026-01-01T00:00:24Z",
        "{\"reason\":\"operator\"}", "cancel-request-boundary-key"
    )
    var cancel_request_payload_conflict = False
    try:
        _ = journal.request_cancel_process(
            "run-semantics", "cancel-request-boundary", "operator", "2026-01-01T00:00:24Z",
            "{\"reason\":\"changed\"}", "cancel-request-boundary-key"
        )
    except err:
        cancel_request_payload_conflict = True
    var cancel_request_row = journal.get_process("run-semantics", "cancel-request-boundary")
    var cancel_request_commands = journal.db.query(
        "SELECT count(*) FROM runtime_commands WHERE run_id=? AND idempotency_key=?"
    )
    cancel_request_commands.bind_text(1, "run-semantics")
    cancel_request_commands.bind_text(2, "cancel-request-boundary-key")
    var cancel_request_events = journal.list_events("run-semantics", "", "cancel-request-boundary", -1, 0, "process.cancel_requested")
    _check(
        cancel_request_first.status == "cancel_requested"
        and cancel_request_replay.status == "cancel_requested"
        and cancel_request_row.status == "cancel_requested"
        and cancel_request_row.lease_owner == ""
        and cancel_request_commands.step()
        and cancel_request_commands.column_int(0) == 1
        and cancel_request_payload_conflict
        and len(cancel_request_events) == 1,
        "cancel request transition and replay",
    )
    # A keyed cancellation request rejects conflicting actor and timestamp calls atomically.
    var cancel_conflict_row_before = journal.get_process("run-semantics", "cancel-request-boundary")
    var cancel_conflict_commands_before = journal.db.query(
        "SELECT count(*) FROM runtime_commands WHERE run_id=? AND idempotency_key=?"
    )
    cancel_conflict_commands_before.bind_text(1, "run-semantics")
    cancel_conflict_commands_before.bind_text(2, "cancel-request-boundary-key")
    var cancel_conflict_events_before = journal.list_events("run-semantics", "", "cancel-request-boundary", -1, 0, "process.cancel_requested")
    _check(cancel_conflict_commands_before.step() and len(cancel_conflict_events_before) == 1, "cancel conflict baseline")
    var cancel_actor_conflict = False
    try:
        _ = journal.request_cancel_process(
            "run-semantics", "cancel-request-boundary", "other-operator", "2026-01-01T00:00:24Z",
            "{\"reason\":\"operator\"}", "cancel-request-boundary-key"
        )
    except err:
        cancel_actor_conflict = True
    var cancel_timestamp_conflict = False
    try:
        _ = journal.request_cancel_process(
            "run-semantics", "cancel-request-boundary", "operator", "2026-01-01T00:00:25Z",
            "{\"reason\":\"operator\"}", "cancel-request-boundary-key"
        )
    except err:
        cancel_timestamp_conflict = True
    var cancel_conflict_row_after = journal.get_process("run-semantics", "cancel-request-boundary")
    var cancel_conflict_commands_after = journal.db.query(
        "SELECT count(*) FROM runtime_commands WHERE run_id=? AND idempotency_key=?"
    )
    cancel_conflict_commands_after.bind_text(1, "run-semantics")
    cancel_conflict_commands_after.bind_text(2, "cancel-request-boundary-key")
    var cancel_conflict_events_after = journal.list_events("run-semantics", "", "cancel-request-boundary", -1, 0, "process.cancel_requested")
    _check(
        cancel_actor_conflict and cancel_timestamp_conflict
        and cancel_conflict_row_after.status == cancel_conflict_row_before.status
        and cancel_conflict_row_after.error_json == cancel_conflict_row_before.error_json
        and cancel_conflict_row_after.lease_owner == cancel_conflict_row_before.lease_owner
        and cancel_conflict_commands_after.step() and cancel_conflict_commands_after.column_int(0) == cancel_conflict_commands_before.column_int(0)
        and len(cancel_conflict_events_after) == len(cancel_conflict_events_before),
        "cancel request conflicts are atomic",
    )
    var cancelled_request = journal.cancel_process(
        "run-semantics", "cancel-request-boundary", "operator", "2026-01-01T00:00:26Z", "{\"reason\":\"confirmed\"}"
    )
    _check(cancelled_request.status == "cancelled" and cancelled_request.lease_owner == "" and cancelled_request.lease_expires_at == "" and cancelled_request.finished_at != "", "cancel request becomes terminal")
    var cancelled_request_event = journal.list_events("run-semantics", "", "cancel-request-boundary", -1, 0, "process.cancelled")
    _check(len(cancelled_request_event) == 1 and cancelled_request_event[0].payload.find("\"process_id\":\"cancel-request-boundary\"") >= 0 and cancelled_request_event[0].payload.find("\"attempt\":0") >= 0 and cancelled_request_event[0].payload.find("\"output\":{}") >= 0 and cancelled_request_event[0].payload.find("\"error\":{\"reason\":\"confirmed\"}") >= 0, "cancelled event payload fields")
    # An empty worker identity is rejected before a claim command/event is written.
    _ = journal.schedule_process("run-semantics", "empty-worker-claim", "native", "2026-01-01T00:00:27Z", "{}", "{}", "", 1, 1, "2026-01-01T00:00:27Z")
    var empty_claim_failed = False
    try:
        _ = journal.claim_process("run-semantics", "empty-worker-claim", "", "2026-01-01T00:00:27Z", "2026-01-01T00:01:00Z", "empty-worker-claim-key")
    except err:
        empty_claim_failed = True
    var empty_claim_row = journal.get_process("run-semantics", "empty-worker-claim")
    var empty_claim_commands = journal.db.query("SELECT count(*) FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
    empty_claim_commands.bind_text(1, "run-semantics"); empty_claim_commands.bind_text(2, "empty-worker-claim-key")
    var empty_claim_events = journal.list_events("run-semantics", "", "empty-worker-claim", -1, 0, "process.claimed")
    _check(empty_claim_failed and empty_claim_row.status == "ready" and empty_claim_row.attempt == 0 and empty_claim_commands.step() and empty_claim_commands.column_int(0) == 0 and len(empty_claim_events) == 0, "empty worker claim rejected atomically")
    # Claim guards reject empty timestamps and non-increasing leases before any mutation.
    _ = journal.schedule_process("run-semantics", "empty-claim-time", "native", "2026-01-01T00:00:27Z", "{}", "{}", "", 1, 1, "2026-01-01T00:00:27Z")
    var empty_now_failed = False
    try:
        _ = journal.claim_process("run-semantics", "empty-claim-time", "worker-a", "", "2026-01-01T00:01:00Z", "empty-now-key")
    except err:
        empty_now_failed = True
    var empty_now_row = journal.get_process("run-semantics", "empty-claim-time")
    _check(empty_now_failed and empty_now_row.status == "ready" and empty_now_row.attempt == 0 and empty_now_row.lease_owner == "", "empty claim timestamp rejected atomically")
    _ = journal.schedule_process("run-semantics", "empty-claim-lease", "native", "2026-01-01T00:00:27Z", "{}", "{}", "", 1, 1, "2026-01-01T00:00:27Z")
    var empty_lease_failed = False
    try:
        _ = journal.claim_process("run-semantics", "empty-claim-lease", "worker-a", "2026-01-01T00:00:27Z", "", "empty-lease-key")
    except err:
        empty_lease_failed = True
    var empty_lease_row = journal.get_process("run-semantics", "empty-claim-lease")
    _check(empty_lease_failed and empty_lease_row.status == "ready" and empty_lease_row.attempt == 0 and empty_lease_row.lease_owner == "", "empty claim lease rejected atomically")
    _ = journal.schedule_process("run-semantics", "equal-claim-lease", "native", "2026-01-01T00:00:27Z", "{}", "{}", "", 1, 1, "2026-01-01T00:00:27Z")
    var equal_lease_failed = False
    try:
        _ = journal.claim_process("run-semantics", "equal-claim-lease", "worker-a", "2026-01-01T00:00:27Z", "2026-01-01T00:00:27Z", "equal-lease-key")
    except err:
        equal_lease_failed = True
    var equal_lease_row = journal.get_process("run-semantics", "equal-claim-lease")
    _check(equal_lease_failed and equal_lease_row.status == "ready" and equal_lease_row.attempt == 0 and equal_lease_row.lease_owner == "", "equal claim lease rejected atomically")
    _ = journal.schedule_process("run-semantics", "past-claim-lease", "native", "2026-01-01T00:00:27Z", "{}", "{}", "", 1, 1, "2026-01-01T00:00:27Z")
    var past_lease_failed = False
    try:
        _ = journal.claim_process("run-semantics", "past-claim-lease", "worker-a", "2026-01-01T00:00:28Z", "2026-01-01T00:00:27Z", "past-lease-key")
    except err:
        past_lease_failed = True
    var past_lease_row = journal.get_process("run-semantics", "past-claim-lease")
    _check(past_lease_failed and past_lease_row.status == "ready" and past_lease_row.attempt == 0 and past_lease_row.lease_owner == "", "past claim lease rejected atomically")
    # A legacy failed row retaining a lease cannot be retried by a different actor.
    _ = journal.schedule_process("run-semantics", "failed-held-lease", "native", "2026-01-01T00:00:28Z", "{}", "{}", "", 1, 2, "2026-01-01T00:00:28Z")
    var failed_lease_claim = journal.claim_process("run-semantics", "failed-held-lease", "failed-worker", "2026-01-01T00:00:28Z", "2026-01-01T00:01:00Z")
    var failed_lease_fixture = journal.db.query("UPDATE processes SET status='failed', error_json=?, updated_at=? WHERE run_id=? AND id=?")
    failed_lease_fixture.bind_text(1, "{\"reason\":\"legacy\"}"); failed_lease_fixture.bind_text(2, "2026-01-01T00:00:29Z"); failed_lease_fixture.bind_text(3, "run-semantics"); failed_lease_fixture.bind_text(4, "failed-held-lease"); _ = failed_lease_fixture.step()
    var failed_lease_before = journal.get_process("run-semantics", "failed-held-lease")
    var failed_retry_rejected = False
    try:
        _ = journal.retry_process("run-semantics", "failed-held-lease", "other-worker", "2026-01-01T00:00:30Z", "2026-01-01T00:00:31Z", "{\"reason\":\"retry\"}")
    except err:
        failed_retry_rejected = True
    var failed_lease_after = journal.get_process("run-semantics", "failed-held-lease")
    var failed_retry_commands = journal.list_events("run-semantics", "", "failed-held-lease", -1, 0, "process.retry_scheduled")
    _check(failed_lease_claim.status == "running" and failed_lease_before.status == "failed" and failed_lease_before.lease_owner == "failed-worker" and failed_retry_rejected and failed_lease_after.status == failed_lease_before.status and failed_lease_after.lease_owner == failed_lease_before.lease_owner and len(failed_retry_commands) == 0, "failed leased process rejects foreign retry")
    # Cancel and timeout preserve an existing output payload, matching the reference transition contract.
    _ = journal.schedule_process(
        "run-semantics", "cancel-output", "native", "2026-01-01T00:00:26Z",
        "{}", "{}", "", 1, 1, "2026-01-01T00:00:26Z"
    )
    var cancel_claim = journal.claim_process(
        "run-semantics", "cancel-output", "worker-a", "2026-01-01T00:00:26Z", "2026-01-01T00:00:27Z"
    )
    var cancel_wait = journal.wait_process(
        cancel_claim.run_id, cancel_claim.id, "worker-a", "2026-01-01T00:00:26Z", "{\"prior\":true}", "cancel-output-wait"
    )
    var cancelled_output = journal.cancel_process(
        cancel_wait.run_id, cancel_wait.id, "worker-a", "2026-01-01T00:00:27Z", "{\"cancelled\":true}"
    )
    var cancel_command = journal.get_command_by_idempotency("run-semantics", "process.cancel-output:cancelled:2026-01-01T00:00:27Z")
    _check(cancel_command.payload.find("\"process_id\":\"cancel-output\"") >= 0 and cancel_command.payload.find("\"attempt\":1") >= 0 and cancel_command.payload.find("\"output\":{\"prior\":true}") >= 0 and cancel_command.payload.find("\"error\":{\"cancelled\":true}") >= 0, "cancel command payload fields")
    var cancel_events = journal.list_events("run-semantics", "", "cancel-output", -1, 0, "process.cancelled")
    _check(len(cancel_events) == 1 and cancel_events[0].payload.find("\"process_id\":\"cancel-output\"") >= 0 and cancel_events[0].payload.find("\"attempt\":1") >= 0 and cancel_events[0].payload.find("\"output\":{\"prior\":true}") >= 0 and cancel_events[0].payload.find("\"error\":{\"cancelled\":true}") >= 0, "cancel event payload fields")
    _ = journal.schedule_process(
        "run-semantics", "timeout-output", "native", "2026-01-01T00:00:28Z",
        "{}", "{}", "", 1, 1, "2026-01-01T00:00:28Z"
    )
    var timeout_claim = journal.claim_process(
        "run-semantics", "timeout-output", "worker-a", "2026-01-01T00:00:28Z", "2026-01-01T00:00:29Z"
    )
    var timeout_wait = journal.wait_process(
        timeout_claim.run_id, timeout_claim.id, "worker-a", "2026-01-01T00:00:28Z", "{\"prior\":true}", "timeout-output-wait"
    )
    var timed_out_output = journal.timeout_process(
        timeout_wait.run_id, timeout_wait.id, "worker-a", "2026-01-01T00:00:29Z", "{\"timed_out\":true}"
    )
    var timeout_command = journal.get_command_by_idempotency("run-semantics", "process.timeout-output:timed_out:2026-01-01T00:00:29Z")
    var timeout_output_events = journal.list_events("run-semantics", "", "timeout-output", -1, 0, "process.timed_out")
    _check(timeout_command.payload.find("\"process_id\":\"timeout-output\"") >= 0 and timeout_command.payload.find("\"attempt\":1") >= 0 and timeout_command.payload.find("\"output\":{\"prior\":true}") >= 0 and timeout_command.payload.find("\"error\":{\"timed_out\":true}") >= 0, "timeout command payload fields")
    _check(timed_out_output.status == "timed_out" and timed_out_output.lease_owner == "" and timed_out_output.lease_expires_at == "" and timed_out_output.finished_at != "", "timeout terminal fields")
    var timeout_replay = journal.timeout_process(timeout_wait.run_id, timeout_wait.id, "worker-a", "2026-01-01T00:00:29Z", "{\"timed_out\":true}")
    _check(timeout_replay.status == "timed_out", "timeout generated-key replay")
    var timeout_payload_conflict = False
    try:
        _ = journal.timeout_process(timeout_wait.run_id, timeout_wait.id, "worker-a", "2026-01-01T00:00:29Z", "{\"changed\":true}")
    except err:
        timeout_payload_conflict = True
    _check(timeout_payload_conflict, "timeout generated-key payload conflict")
    var timeout_conflict = False
    try:
        _ = journal.timeout_process(timeout_wait.run_id, timeout_wait.id, "other-worker", "2026-01-01T00:00:29Z", "{\"timed_out\":true}")
    except err:
        timeout_conflict = True
    _check(timeout_conflict, "timeout generated-key actor conflict")
    var timeout_timestamp_conflict = False
    try:
        _ = journal.timeout_process(timeout_wait.run_id, timeout_wait.id, "worker-a", "2026-01-01T00:00:30Z", "{\"changed\":true}")
    except err:
        timeout_timestamp_conflict = True
    _check(timeout_timestamp_conflict, "timeout generated-key timestamp conflict")
    _check(len(timeout_output_events) == 1 and timeout_output_events[0].payload.find("\"process_id\":\"timeout-output\"") >= 0 and timeout_output_events[0].payload.find("\"attempt\":1") >= 0 and timeout_output_events[0].payload.find("\"output\":{\"prior\":true}") >= 0 and timeout_output_events[0].payload.find("\"error\":{\"timed_out\":true}") >= 0, "timeout event payload fields")
    _ = journal.create_run("command-path", "active", "{}", "2026-01-01T00:00:29Z")
    # Exercise the caller-command transition path directly; cancel/timeout must preserve waiting output there too.
    _ = journal.schedule_process(
        "command-path", "command-cancel", "native", "2026-01-01T00:00:30Z",
        "{}", "{}", "", 1, 1, "2026-01-01T00:00:30Z"
    )
    var command_cancel_claim = journal.claim_process(
        "command-path", "command-cancel", "command-worker", "2026-01-01T00:00:30Z", "2026-01-01T00:01:00Z"
    )
    _ = journal.transition_process_with_command(
        command_cancel_claim.run_id, command_cancel_claim.id, "waiting",
        CommandRow(run_id="command-path", id="command-cancel-wait", command_type="process.wait", idempotency_key="command-cancel-wait", actor="command-worker", correlation_id="", causation_id="", payload="{\"process_id\":\"command-cancel\",\"attempt\":1}", created_at="2026-01-01T00:00:31Z"),
        "{\"prior\":true}", "{}"
    )
    var command_cancel = journal.transition_process_with_command(
        command_cancel_claim.run_id, command_cancel_claim.id, "cancelled",
        CommandRow(run_id="command-path", id="command-cancel-terminal", command_type="process.cancel", idempotency_key="command-cancel-terminal", actor="command-worker", correlation_id="", causation_id="", payload="{\"process_id\":\"command-cancel\",\"attempt\":1}", created_at="2026-01-01T00:00:32Z"),
        "{}", "{\"cancelled\":true}"
    )
    _check(command_cancel.process.status == "cancelled" and command_cancel.process.output_json == "{\"prior\":true}", "command cancel preserves output")
    var command_cancel_audit = journal.get_command_by_idempotency("command-path", "command-cancel-terminal")
    _check(command_cancel_audit.payload == "{\"process_id\":\"command-cancel\",\"attempt\":1}", "caller cancel payload remains caller-owned")
    _ = journal.schedule_process(
        "command-path", "command-timeout", "native", "2026-01-01T00:00:33Z",
        "{}", "{}", "", 1, 1, "2026-01-01T00:00:33Z"
    )
    var command_timeout_claim = journal.claim_process(
        "command-path", "command-timeout", "command-worker", "2026-01-01T00:00:33Z", "2026-01-01T00:01:00Z"
    )
    _ = journal.transition_process_with_command(
        command_timeout_claim.run_id, command_timeout_claim.id, "waiting",
        CommandRow(run_id="command-path", id="command-timeout-wait", command_type="process.wait", idempotency_key="command-timeout-wait", actor="command-worker", correlation_id="", causation_id="", payload="{\"process_id\":\"command-timeout\",\"attempt\":1}", created_at="2026-01-01T00:00:34Z"),
        "{\"prior\":true}", "{}"
    )
    var command_timeout = journal.transition_process_with_command(
        command_timeout_claim.run_id, command_timeout_claim.id, "timed_out",
        CommandRow(run_id="command-path", id="command-timeout-terminal", command_type="process.timeout", idempotency_key="command-timeout-terminal", actor="command-worker", correlation_id="", causation_id="", payload="{\"process_id\":\"command-timeout\",\"attempt\":1}", created_at="2026-01-01T00:00:35Z"),
        "{}", "{\"timed_out\":true}"
    )
    _check(command_timeout.process.status == "timed_out" and command_timeout.process.output_json == "{\"prior\":true}", "command timeout preserves output")
    var command_timeout_audit = journal.get_command_by_idempotency("command-path", "command-timeout-terminal")
    _check(command_timeout_audit.payload == "{\"process_id\":\"command-timeout\",\"attempt\":1}", "caller timeout payload remains caller-owned")



    # Lease maintenance returns an expired process to retry_wait, then marks
    # it failed once its final attempt expires.
    _ = journal.schedule_process(
        "run-semantics", "lease-expiry", "native", "2026-01-01T00:00:13Z",
        "{}", "{}", "", 2, 2, "2026-01-01T00:00:13Z"
    )
    var lease_claim = journal.claim_process(
        "run-semantics", "lease-expiry", "worker-c", "2026-01-01T00:00:20Z", "2026-01-01T00:00:21Z"
    )
    var lease_retry = maintain_process(
        journal, lease_claim, "worker-c", "2026-01-01T00:00:22Z", "2026-01-01T00:00:22Z"
    )
    _check(lease_retry.status == "retry_wait" and lease_retry.attempt == 1, "expired lease retry maintenance")
    var lease_claim_two = journal.claim_process(
        "run-semantics", "lease-expiry", "worker-c", "2026-01-01T00:00:22Z", "2026-01-01T00:00:23Z"
    )
    var lease_failed = maintain_process(
        journal, lease_claim_two, "worker-c", "2026-01-01T00:00:24Z", "2026-01-01T00:00:24Z"
    )
    _check(lease_failed.status == "failed" and lease_failed.lease_owner == "", "expired lease terminal maintenance")
    # Stale same-worker snapshots cannot maintain a newer lease.
    _ = journal.schedule_process(
        "run-semantics", "stale-maintenance", "native", "2026-01-01T00:00:24Z",
        "{}", "{}", "", 5, 3, "2026-01-01T00:00:24Z"
    )
    var stale_first = journal.claim_process(
        "run-semantics", "stale-maintenance", "worker-stale",
        "2026-01-01T00:00:25Z", "2026-01-01T00:00:26Z"
    )
    var stale_second = journal.claim_process(
        "run-semantics", "stale-maintenance", "worker-stale",
        "2026-01-01T00:00:27Z", "2026-01-01T00:00:28Z"
    )
    var stale_commands_before = journal.list_commands("run-semantics", "process.retry")
    var stale_events_before = journal.list_events("run-semantics", "", "stale-maintenance", -1, 0, "process.retry_scheduled")
    var stale_rejected = False
    try:
        _ = maintain_process(journal, stale_first, "worker-stale", "2026-01-01T00:00:27Z", "2026-01-01T00:00:27Z")
    except err:
        stale_rejected = True
    var stale_after = journal.get_process("run-semantics", "stale-maintenance")
    var stale_commands_after = journal.list_commands("run-semantics", "process.retry")
    var stale_events_after = journal.list_events("run-semantics", "", "stale-maintenance", -1, 0, "process.retry_scheduled")
    _check(
        stale_rejected and stale_second.attempt == 2 and stale_after.status == "running"
        and stale_after.attempt == 2 and stale_after.lease_owner == "worker-stale"
        and stale_after.lease_expires_at == "2026-01-01T00:00:28Z"
        and len(stale_commands_after) == len(stale_commands_before)
        and len(stale_events_after) == len(stale_events_before),
        "stale maintenance lease fencing"
    )
    # Expired running rows without an owner remain reclaimable by the queue;
    # the atomic claim installs the new owner and lease.
    _ = journal.create_run("ownerless-run", "active", "{}", "2026-01-01T00:00:25Z")
    _ = journal.schedule_process(
        "ownerless-run", "ownerless-expired", "native", "2026-01-01T00:00:25Z",
        "{}", "{}", "", 5, 2, "2026-01-01T00:00:25Z"
    )
    var ownerless_claim = journal.claim_process(
        "ownerless-run", "ownerless-expired", "ownerless-worker",
        "2026-01-01T00:00:26Z", "2026-01-01T00:00:27Z"
    )
    var ownerless_fixture = journal.db.query("UPDATE processes SET lease_owner=NULL,lease_expires_at=? WHERE run_id=? AND id=?")
    ownerless_fixture.bind_text(1, "2026-01-01T00:00:26Z"); ownerless_fixture.bind_text(2, "ownerless-run"); ownerless_fixture.bind_text(3, "ownerless-expired"); _ = ownerless_fixture.step()
    var ownerless_commands_before = journal.list_commands("ownerless-run", "process.claim")
    var ownerless_events_before = journal.list_events("ownerless-run", "", "ownerless-expired", -1, 0, "process.claimed")
    var ownerless_result = journal.claim_next_ready(
        "ownerless-run", "new-owner", "2026-01-01T00:00:27Z", "2026-01-01T00:00:28Z"
    )
    var ownerless_after = journal.get_process("ownerless-run", "ownerless-expired")
    var ownerless_commands_after = journal.list_commands("ownerless-run", "process.claim")
    var ownerless_events_after = journal.list_events("ownerless-run", "", "ownerless-expired", -1, 0, "process.claimed")
    _check(
        ownerless_result and ownerless_result.value().status == "running" and ownerless_after.status == "running"
        and ownerless_after.attempt == ownerless_claim.attempt + 1 and ownerless_after.lease_owner == "new-owner"
        and ownerless_after.lease_expires_at == "2026-01-01T00:00:28Z"
        and len(ownerless_commands_after) == len(ownerless_commands_before) + 1
        and len(ownerless_events_after) == len(ownerless_events_before) + 1,
        "ownerless expired lease is reclaimed atomically"
    )

    # Native registry ownership persists across drive calls and native output completes.
    var registry = NativeFunctionRegistry()
    registry.register("native.echo", _native_echo)
    _ = journal.schedule_process(
        "run-semantics", "native-ok", "native", "2026-01-01T00:00:25Z",
        "{\"value\":1,\"config\":{\"limit\":2}}", "{}", "", 8, 1,
        "2026-01-01T00:00:25Z"
    )
    var native_row = journal.get_process("run-semantics", "native-ok")
    var native_result = drive_once(
        journal, native_row, AdapterSpec.native_function("native.echo"),
        "worker-native", "2026-01-01T00:00:25Z", "2026-01-01T00:01:00Z", registry
    )
    var native_stored = journal.get_process("run-semantics", "native-ok")
    _check(native_result.completed and native_stored.status == "succeeded" and native_stored.output_json.find("\"metadata\":{\"user\":\"keep\"}") >= 0 and native_stored.output_json.find("\"adapter\":") >= 0 and native_stored.output_json.find("\"registry_ref\":\"native.echo\"") >= 0 and native_stored.output_json.find("\"returncode\":0") >= 0, "persistent native registry drive preserves output metadata and telemetry")
    var serialized_result = adapter_result_json(EffectorResult(success=True, output_json="{\"value\":1,\"metadata\":{\"user\":\"keep\"}}", stdout="out", stderr="err", returncode=7, waiting=False, homeostat_id="homeostat:smoke", metadata_json="{\"trace\":\"native\"}", error=AdapterError.none()))
    _check(serialized_result.find("\"success\":true") >= 0 and serialized_result.find("\"waiting\":false") >= 0 and serialized_result.find("\"output\":{\"value\":1,\"metadata\":{\"user\":\"keep\"}}") >= 0 and serialized_result.find("\"stdout\":\"out\"") >= 0 and serialized_result.find("\"stderr\":\"err\"") >= 0 and serialized_result.find("\"returncode\":7") >= 0 and serialized_result.find("\"homeostat_id\":\"homeostat:smoke\"") >= 0 and serialized_result.find("\"metadata\":{\"trace\":\"native\"}") >= 0 and serialized_result.find("\"error_code\":\"\"") >= 0 and serialized_result.find("\"error_message\":\"\"") >= 0, "adapter result envelope preserves output metadata and telemetry fields")
    # Authored top-level "adapter" in registry output is overwritten by reserved telemetry (reference behavior).
    registry.register("native.collision", _native_collision)
    _ = journal.schedule_process("run-semantics", "native-collision", "native", "2026-01-01T00:00:25Z", "{}", "{}", "", 1, 1, "2026-01-01T00:00:25Z")
    var coll_row = journal.get_process("run-semantics", "native-collision")
    _ = drive_once(journal, coll_row, AdapterSpec.native_function("native.collision"), "worker-coll", "2026-01-01T00:00:25Z", "2026-01-01T00:01:00Z", registry)
    var coll_stored = journal.get_process("run-semantics", "native-collision")
    _check(coll_stored.status == "succeeded" and coll_stored.output_json == "{\"adapter\":{\"metadata\":{\"registry_ref\":\"native.collision\"},\"returncode\":0,\"stderr\":\"\",\"stdout\":\"\"},\"value\":2}", "authored top-level adapter is overwritten by reserved telemetry")
    # Homeostat attempt budgets support explicit reopen and terminal exhaustion.
    var reopened = Homeostat(id="h-reopen", run_id="run-semantics", kind="manual_homeostat", status="completed", attempt=0, max_attempts=2).reopened("2026-01-01T00:00:26Z")
    _check(reopened.status == "open" and reopened.attempt == 1 and reopened.max_attempts == 2, "homeostat reopen attempt")
    var exhausted = Homeostat(id="h-exhausted", run_id="run-semantics", kind="manual_homeostat", status="expired", attempt=2, max_attempts=2)
    _check(not exhausted.can_reopen(), "homeostat exhaustion")
    var bounded_budget = RuntimeBudget(runtime_hops=1, attempts=2)
    _check(bounded_budget.runtime_hops_limited and bounded_budget.attempts_limited and bounded_budget.allows(runtime_hops=1, attempts=2), "presence-aware runtime budget")
    var consumed = bounded_budget.consume(runtime_hops=1, attempts=1)
    _check(consumed.runtime_hops == 0 and consumed.attempts == 1, "runtime budget consumption")

    # Manual homeostat parking is durable and clears the worker lease.
    _ = journal.schedule_process(
        "run-semantics", "manual-wait", "native", "2026-01-01T00:00:26Z",
        "{}", "{}", "", 7, 1, "2026-01-01T00:00:26Z"
    )
    var manual_row = journal.get_process("run-semantics", "manual-wait")
    var manual_result = drive_once(
        journal, manual_row, AdapterSpec.manual_homeostat(),
        "worker-homeostat", "2026-01-01T00:00:26Z", "2026-01-01T00:01:00Z", registry
    )
    var manual_stored = journal.get_process("run-semantics", "manual-wait")
    _check(manual_result.waiting and manual_stored.status == "waiting" and manual_stored.lease_owner == "", "manual homeostat waiting lease clear")

    # Unknown adapter kind fails preflight and is durably failed without effector output.
    _ = journal.schedule_process(
        "run-semantics", "unknown-fail", "native", "2026-01-01T00:00:27Z",
        "{}", "{}", "", 6, 2, "2026-01-01T00:00:27Z"
    )
    # Bounded drive selects strict priority/FIFO order and leaves lower work queued.
    _ = journal.schedule_process(
        "run-semantics", "drive-low", "native", "2026-01-01T00:00:30Z",
        "{}", "{}", "", 3, 1, "2026-01-01T00:00:30Z"
    )
    _ = journal.schedule_process(
        "run-semantics", "drive-high", "native", "2026-01-01T00:00:31Z",
        "{}", "{}", "", 9, 1, "2026-01-01T00:00:31Z"
    )
    var drive_rows = List[ProcessRow]()
    var drive_adapters = List[AdapterSpec]()
    drive_rows.append(journal.get_process("run-semantics", "drive-low"))
    drive_adapters.append(AdapterSpec.native_function("native.echo"))
    drive_rows.append(journal.get_process("run-semantics", "drive-high"))
    drive_adapters.append(AdapterSpec.native_function("native.echo"))
    var bounded = drive_until_idle(
        journal, drive_rows, drive_adapters, "worker-bounded",
        "2026-01-01T00:00:31Z", "2026-01-01T00:01:00Z", 1, registry
    )
    var drive_low_stored = journal.get_process("run-semantics", "drive-low")
    var drive_high_stored = journal.get_process("run-semantics", "drive-high")
    _check(bounded.ticks == 1 and bounded.process_id == "drive-high" and drive_high_stored.status == "succeeded" and drive_low_stored.status == "ready", "bounded strict priority drive")
    var unknown_row = journal.get_process("run-semantics", "unknown-fail")
    var unknown_result = drive_once(
        journal, unknown_row, AdapterSpec(AdapterKind("python_function")),
        "worker-unknown", "2026-01-01T00:00:27Z", "2026-01-01T00:01:00Z", registry
    )
    var unknown_failed = journal.get_process("run-semantics", "unknown-fail")
    _check(
        unknown_result.failed
            and unknown_result.ticks == 1
            and unknown_result.error.code == "invalid_adapter"
            and unknown_failed.status == "failed"
            and unknown_failed.attempt == 1
            and unknown_failed.error_json.find("unknown adapter kind") >= 0
            and unknown_result.failure_rows[0].status == "failed",
        "unknown adapter kind durably fails before dispatch"
    )
    # This durable unsupported-transport assertion is hermetic: the semantics
    # smoke does not build or discover a process host from ambient artifacts.
    var subprocess_command = List[String]()
    subprocess_command.append("echo")
    subprocess_command.append("native")
    _ = journal.schedule_process(
        "run-semantics", "subprocess-unavailable", "native", "2026-01-01T00:00:28Z",
        "{}", "{}", "", 5, 1, "2026-01-01T00:00:28Z"
    )
    _ = journal.schedule_process(
        "run-semantics", "fala-runtime-removed", "native", "2026-01-01T00:00:29Z",
        "{}", "{}", "", 5, 1, "2026-01-01T00:00:29Z"
    )
    var subprocess_row = journal.get_process("run-semantics", "subprocess-unavailable")
    var fala_runtime_row = journal.get_process("run-semantics", "fala-runtime-removed")
    var subprocess_result = drive_once(
        journal, subprocess_row, AdapterSpec.subprocess(subprocess_command),
        "worker-subprocess", "2026-01-01T00:00:28Z", "2026-01-01T00:01:00Z", registry
    )
    var fala_runtime_spec = AdapterSpec(AdapterKind("fala_runtime"))
    var fala_runtime_result = drive_once(
        journal, fala_runtime_row, fala_runtime_spec,
        "worker-fala-runtime", "2026-01-01T00:00:29Z", "2026-01-01T00:01:00Z", registry
    )
    var subprocess_failed = journal.get_process("run-semantics", "subprocess-unavailable")
    var fala_runtime_failed = journal.get_process("run-semantics", "fala-runtime-removed")
    _check(
        subprocess_result.failed
            and subprocess_result.ticks == 1
            and subprocess_result.error.code == "subprocess_transport_unavailable"
            and subprocess_failed.status == "failed"
            and subprocess_failed.attempt == 1
            and subprocess_failed.error_json.find("subprocess_transport_unavailable") >= 0
            and subprocess_result.failure_rows[0].output_json == "{}",
        "subprocess transport unavailable durably fails without output"
    )
    _check(
        fala_runtime_result.failed
            and fala_runtime_result.ticks == 1
            and fala_runtime_result.error.code == "unsupported_fala_runtime"
            and fala_runtime_failed.status == "failed"
            and fala_runtime_failed.attempt == 1
            and fala_runtime_failed.error_json.find("unsupported_fala_runtime") >= 0
            and fala_runtime_result.failure_rows[0].output_json == "{}",
        "fala_runtime is rejected as not part of Fala"
    )
    # All-run scanning uses only explicit run/process bindings and honors a
    # bounded tick budget without claiming the unbound row.
    # A nonempty idempotency key cannot replay an all-run claim because the
    # durable uniqueness scope is (run_id, idempotency_key).
    var ambiguous_all_run_claim = False
    try:
        _ = journal.claim_next_ready(
            "", "worker-all", "2026-01-01T00:00:31Z",
            "2026-01-01T00:01:00Z", "all-run-ambiguous", True
        )
    except err:
        ambiguous_all_run_claim = True
    _check(ambiguous_all_run_claim, "all-run idempotency requires run id")
    _ = journal.create_run("run-other", "active", "{}", "2026-01-01T00:00:31Z")
    _ = journal.schedule_process("run-other", "other-native", "native", "impulse", "{}", "{}", "", 2, 1, "2026-01-01T00:00:31Z")
    var all_bindings = List[AdapterBinding]()
    all_bindings.append(AdapterBinding("drive-low", AdapterSpec.native_function("native.echo"), "run-semantics"))
    all_bindings.append(AdapterBinding("other-native", AdapterSpec.native_function("native.echo"), "run-other"))
    var all_report = drive_all_runs(journal, all_bindings, "worker-all", "2026-01-01T00:00:31Z", "2026-01-01T00:01:00Z", registry, 1)
    _check(all_report.ticks == 1 and all_report.bounded and all_report.runs_scanned == 2, "all-run bounded explicit scan")
    _check(journal.get_process("run-other", "other-native").status == "ready", "all-run budget leaves second row queued")

    # A bounded run with one queued row must report max_ticks; the next
    # exact-budget tick succeeds and must not be relabeled as max_ticks.
    _ = journal.create_run("result-run", "active", "{}", "2026-01-01T00:00:32Z")
    _ = journal.schedule_process(
        "result-run", "result-first", "native", "2026-01-01T00:00:32Z",
        "{}", "{}", "", 10, 1, "2026-01-01T00:00:32Z"
    )
    _ = journal.schedule_process(
        "result-run", "result-second", "native", "2026-01-01T00:00:32Z",
        "{}", "{}", "", 1, 1, "2026-01-01T00:00:32Z"
    )
    var result_rows = List[ProcessRow]()
    var result_adapters = List[AdapterSpec]()
    result_rows.append(journal.get_process("result-run", "result-first"))
    result_adapters.append(AdapterSpec.native_function("native.echo"))
    result_rows.append(journal.get_process("result-run", "result-second"))
    result_adapters.append(AdapterSpec.native_function("native.echo"))
    var native_run = run_until_idle(
        journal, result_rows, result_adapters, "worker-result",
        "2026-01-01T00:00:32Z", "2026-01-01T00:01:00Z", 1, registry
    )
    _check(native_run.stopped_reason == "max_ticks" and not native_run.ok and native_run.ticks == 1, "run-until-idle max-ticks result")
    var native_terminal = run_until_idle(
        journal, result_rows, result_adapters, "worker-result",
        "2026-01-01T00:00:33Z", "2026-01-01T00:01:00Z", 1, registry
    )
    _check(native_terminal.stopped_reason == "idle" and native_terminal.ok and native_terminal.ticks == 1 and len(native_terminal.completed) == 2, "run-until-idle exact-budget success")
    _ = journal.create_run("retry-exhaustion", "active", "{}", "2026-01-01T00:00:33Z")
    _ = journal.schedule_process("retry-exhaustion", "retry-native", "native", "2026-01-01T00:00:33Z", "{}", "{}", "", 5, 2, "2026-01-01T00:00:33Z")
    var retry_exhaustion_rows = List[ProcessRow]()
    var retry_exhaustion_adapters = List[AdapterSpec]()
    retry_exhaustion_rows.append(journal.get_process("retry-exhaustion", "retry-native"))
    retry_exhaustion_adapters.append(AdapterSpec.native_function("native.missing"))
    var retry_exhaustion = run_until_idle(journal, retry_exhaustion_rows, retry_exhaustion_adapters, "worker-retry", "2026-01-01T00:00:33Z", "2026-01-01T00:01:00Z", 2, registry)
    var retry_exhaustion_row = journal.get_process("retry-exhaustion", "retry-native")
    _check(retry_exhaustion.ticks == 2 and retry_exhaustion.stopped_reason == "idle" and retry_exhaustion.ok and len(retry_exhaustion.failed) == 2 and retry_exhaustion.failed[0].status == "retry_wait" and retry_exhaustion.failed[1].status == "failed" and retry_exhaustion_row.attempt == 2, "retry exhaustion remains idle after terminal failure")
    _ = finalize_run(journal, "retry-exhaustion", "failed", 2, "2026-01-01T00:00:34Z")
    var terminal_replay_driver = run_until_idle(journal, retry_exhaustion_rows, retry_exhaustion_adapters, "worker-retry", "2026-01-01T00:00:34Z", "2026-01-01T00:01:00Z", 2, registry)
    _check(terminal_replay_driver.ticks == 0 and terminal_replay_driver.stopped_reason == "already_terminal" and terminal_replay_driver.ok and not terminal_replay_driver.deadlocked, "terminal run replay is a no-op")
    var empty_processes = List[ProcessRow]()
    var empty_adapters = List[AdapterSpec]()
    var empty_drive = run_until_idle(journal, empty_processes, empty_adapters, "worker-empty", "2026-01-01T00:00:33Z", "2026-01-01T00:01:00Z", 1, registry)
    _check(empty_drive.ticks == 0 and empty_drive.stopped_reason == "idle" and empty_drive.ok, "empty run-until-idle is deterministic")

    _ = journal.create_run("closure-run", "active", "{}", "2026-01-01T00:00:31Z")
    _ = journal.schedule_process("closure-run", "expired-terminal", "native", "2026-01-01T00:00:31Z", "{}", "{}", "", 1, 1, "2026-01-01T00:00:31Z")
    _ = journal.claim_process("closure-run", "expired-terminal", "old-worker", "2026-01-01T00:00:32Z", "2026-01-01T00:00:33Z")
    var closure_bindings = List[AdapterBinding]()
    closure_bindings.append(AdapterBinding("expired-terminal", AdapterSpec.native_function("native.echo"), "closure-run"))
    var closure_report = drive_all_runs(journal, closure_bindings, "worker-closure", "2026-01-01T00:00:34Z", "2026-01-01T00:01:00Z", registry, 1)
    _check(closure_report.ticks == 1 and not closure_report.idle and closure_report.failed and journal.get_process("closure-run", "expired-terminal").status == "failed", "closure-only drive is not idle")
    _ = journal.schedule_process(
        "run-semantics", "explicit-wait", "native", "2026-01-01T00:01:02Z",
        "{}", "{}", "", 4, 1, "2026-01-01T00:01:02Z"
    )
    var explicit_claim = journal.claim_process("run-semantics", "explicit-wait", "wait-worker", "2026-01-01T00:01:03Z", "2026-01-01T00:02:00Z")
    var explicit_wait = journal.wait_process(explicit_claim.run_id, explicit_claim.id, "wait-worker", "2026-01-01T00:01:04Z", "{\"paused\":true}", "explicit-wait-key")
    var explicit_replay = journal.wait_process(explicit_claim.run_id, explicit_claim.id, "wait-worker", "2026-01-01T00:01:04Z", "{\"paused\":true}", "explicit-wait-key")
    var explicit_command = journal.get_command_by_idempotency("run-semantics", "explicit-wait-key")
    var explicit_command_count = journal.db.query("SELECT count(*) FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
    explicit_command_count.bind_text(1, "run-semantics"); explicit_command_count.bind_text(2, "explicit-wait-key")
    var wait_events = journal.list_events("run-semantics", "", "explicit-wait", -1, 0, "process.waiting")
    _check(explicit_wait.status == "waiting" and explicit_replay.status == "waiting" and explicit_command.idempotency_key == "explicit-wait-key" and explicit_command_count.step() and explicit_command_count.column_int(0) == 1 and len(wait_events) == 1, "explicit wait idempotent replay")
    var wait_diag = diagnose_waits(journal, "run-semantics")
    var saw_manual_wait = False
    var saw_explicit_wait = False
    for blocked_id in wait_diag.blocked_process_ids:
        if blocked_id == "manual-wait": saw_manual_wait = True
        if blocked_id == "explicit-wait": saw_explicit_wait = True
    _check(not wait_diag.deadlocked and wait_diag.code == "waiting_without_known_blocker" and wait_diag.reason == "waiting_without_known_blocker" and saw_manual_wait and saw_explicit_wait, "durable wait diagnosis")

    # Same-timestamp event IDs remain distinct and event replay is idempotent.
    var same_time_one = journal.append_event(
        "run-semantics", "same-time-1", "probe.same", "{}",
        "2026-01-01T00:00:29Z"
    )
    var same_time_replay = journal.append_event(
        "run-semantics", "same-time-1", "probe.same", "{}",
        "2026-01-01T00:00:29Z"
    )
    var same_time_two = journal.append_event(
        "run-semantics", "same-time-2", "probe.same", "{}",
        "2026-01-01T00:00:29Z"
    )
    _check(same_time_replay.sequence == same_time_one.sequence and same_time_two.sequence == same_time_one.sequence + 1 and same_time_two.id != same_time_one.id, "same timestamp event ids")

    # Cancellation and timeout persist terminal payloads/events across reopen;
    # replaying the same terminal transition is idempotent.
    var lifecycle_path = "/tmp/fala-native-lifecycle-smoke.sqlite"
    _cleanup(lifecycle_path)
    var durable = NativeJournal.open(lifecycle_path)
    durable.initialize()
    _ = durable.create_run("run-lifecycle", "created", "{}", "2026-01-01T00:00:32Z")
    _ = durable.schedule_process("run-lifecycle", "cancel-reopen", "native", "2026-01-01T00:00:32Z", "{}", "{}", "", 1, 1, "2026-01-01T00:00:32Z")
    var cancelled = durable.cancel_process("run-lifecycle", "cancel-reopen", "operator", "2026-01-01T00:00:33Z", "{\"reason\":\"operator\"}")
    var cancelled_replay = durable.cancel_process("run-lifecycle", "cancel-reopen", "operator", "2026-01-01T00:00:33Z", "{\"reason\":\"operator\"}")
    _check(cancelled.status == "cancelled" and cancelled_replay.status == "cancelled", "cancellation terminal replay")
    _ = durable.schedule_process("run-lifecycle", "timeout-reopen", "native", "2026-01-01T00:00:34Z", "{}", "{}", "", 1, 1, "2026-01-01T00:00:34Z")
    var timed_out = durable.timeout_process("run-lifecycle", "timeout-reopen", "timer", "2026-01-01T00:00:35Z", "{\"reason\":\"deadline\"}")
    _check(timed_out.status == "timed_out", "timeout terminal transition")
    _ = durable.schedule_process("run-lifecycle", "wait-reopen", "native", "2026-01-01T00:00:36Z", "{}", "{}", "", 1, 1, "2026-01-01T00:00:36Z")
    var wait_claim = durable.claim_process("run-lifecycle", "wait-reopen", "wait-operator", "2026-01-01T00:00:37Z", "2026-01-01T00:01:00Z")
    var wait_once = durable.wait_process(wait_claim.run_id, wait_claim.id, "wait-operator", "2026-01-01T00:00:38Z", "{\"paused\":true}", "wait-reopen-once")
    _check(wait_once.status == "waiting", "durable wait transition")
    durable.close()
    var reopened_journal = NativeJournal.open(lifecycle_path)
    reopened_journal.initialize()
    _check(reopened_journal.get_process("run-lifecycle", "cancel-reopen").status == "cancelled", "cancellation survives reopen")
    _check(reopened_journal.get_process("run-lifecycle", "timeout-reopen").status == "timed_out", "timeout survives reopen")
    var lifecycle_events = reopened_journal.list_events("run-lifecycle", "", "cancel-reopen", -1, 0, "process.cancelled")
    var timeout_reopen_events = reopened_journal.list_events("run-lifecycle", "", "timeout-reopen", -1, 0, "process.timed_out")
    var wait_replay = reopened_journal.wait_process("run-lifecycle", "wait-reopen", "wait-operator", "2026-01-01T00:00:38Z", "{\"paused\":true}", "wait-reopen-once")
    var wait_reopen_commands = reopened_journal.list_commands("run-lifecycle", "process.wait")
    var wait_reopen_events = reopened_journal.list_events("run-lifecycle", "", "wait-reopen", -1, 0, "process.waiting")
    _check(wait_replay.status == "waiting" and len(wait_reopen_commands) == 1 and len(wait_reopen_events) == 1, "durable wait replay survives reopen")
    _check(len(lifecycle_events) == 1 and len(timeout_reopen_events) == 1, "terminal events survive reopen")
    reopened_journal.close()
    _cleanup(lifecycle_path)
    # Durable homeostat parking, terminal completion, replay/conflict, and reopen.
    var homeostat_path = "/tmp/fala-native-homeostat-lifecycle-smoke.sqlite"
    _cleanup(homeostat_path)
    var homeostat_journal = NativeJournal.open(homeostat_path)
    homeostat_journal.initialize()
    _ = homeostat_journal.create_run("run-homeostat", "created", "{}", "2026-01-01T00:00:40Z")
    _ = homeostat_journal.schedule_process(
        "run-homeostat", "homeostat-process", "native", "2026-01-01T00:00:40Z",
        "{}", "{}", "", 1, 3, "2026-01-01T00:00:40Z"
    )
    _ = homeostat_journal.claim_process(
        "run-homeostat", "homeostat-process", "homeostat-worker",
        "2026-01-01T00:00:41Z", "2026-01-01T00:01:00Z"
    )
    var parked = homeostat_journal.park_homeostat_process(
        "run-homeostat", "homeostat-open", "homeostat-process", "homeostat-worker",
        "2026-01-01T00:00:42Z", "{\"prompt\":\"approve\"}", "{\"source\":\"smoke\"}", "homeostat-open-once"
    )
    _check(parked.status == "waiting" and parked.lease_owner == "", "durable homeostat parks waiting process")
    var open_homeostat = homeostat_journal.db.query("SELECT status,attempt,max_attempts FROM homeostats WHERE run_id=? AND id=?")
    open_homeostat.bind_text(1, "run-homeostat"); open_homeostat.bind_text(2, "homeostat-open")
    _check(open_homeostat.step() and open_homeostat.column_text(0) == "open" and open_homeostat.column_int(1) == 1 and open_homeostat.column_int(2) == 3, "durable open homeostat row")
    var opened_events = homeostat_journal.list_events("run-homeostat", "", "homeostat-process", -1, 0, "homeostat.opened")
    var waiting_events = homeostat_journal.list_events("run-homeostat", "", "homeostat-process", -1, 0, "process.waiting")
    var opened_commands = homeostat_journal.list_commands("run-homeostat", "homeostat.open")
    var waiting_commands = homeostat_journal.list_commands("run-homeostat", "process.wait")
    _check(len(opened_events) == 1 and len(waiting_events) == 1 and len(opened_commands) == 1 and len(waiting_commands) == 1, "homeostat park event and command counts")
    var terminal = homeostat_journal.transition_homeostat_process(
        "run-homeostat", "homeostat-open", "homeostat-process", "completed", "succeeded",
        "homeostat-worker", "2026-01-01T00:00:43Z", "{\"approved\":true}", "{}", "homeostat-complete-once"
    )
    _check(terminal.status == "succeeded", "homeostat terminal completion")
    var terminal_replay = homeostat_journal.transition_homeostat_process(
        "run-homeostat", "homeostat-open", "homeostat-process", "completed", "succeeded",
        "homeostat-worker", "2026-01-01T00:00:43Z", "{\"approved\":true}", "{}", "homeostat-complete-once"
    )
    _check(terminal_replay.status == "succeeded", "homeostat terminal replay")
    var terminal_events = homeostat_journal.list_events("run-homeostat", "", "homeostat-process", -1, 0, "homeostat.completed")
    var terminal_process_events = homeostat_journal.list_events("run-homeostat", "", "homeostat-process", -1, 0, "process.succeeded")
    var terminal_commands = homeostat_journal.list_commands("run-homeostat", "homeostat.complete")
    _check(len(terminal_events) == 1 and len(terminal_process_events) == 1 and len(terminal_commands) == 1, "homeostat terminal event and command counts")
    var terminal_conflict = False
    try:
        _ = homeostat_journal.transition_homeostat_process(
            "run-homeostat", "homeostat-open", "homeostat-process", "completed", "succeeded",
            "homeostat-worker", "2026-01-01T00:00:43Z", "{\"approved\":false}", "{}", "homeostat-complete-once"
        )
    except err:
        terminal_conflict = True
    _check(terminal_conflict, "conflicting homeostat terminal replay rejected")
    # Cancellation and expiration preserve typed terminal pairs and durable values.
    _ = homeostat_journal.schedule_process("run-homeostat", "homeostat-cancel-process", "native", "2026-01-01T00:00:44Z", "{}", "{}", "", 1, 2, "2026-01-01T00:00:44Z")
    _ = homeostat_journal.claim_process("run-homeostat", "homeostat-cancel-process", "homeostat-worker", "2026-01-01T00:00:45Z", "2026-01-01T00:01:00Z")
    _ = homeostat_journal.park_homeostat_process("run-homeostat", "homeostat-cancel", "homeostat-cancel-process", "homeostat-worker", "2026-01-01T00:00:46Z", "{\"prompt\":\"cancel\"}", "{}", "homeostat-cancel-open")
    var cancelled_homeostat = homeostat_journal.transition_homeostat_process("run-homeostat", "homeostat-cancel", "homeostat-cancel-process", "cancelled", "cancelled", "homeostat-worker", "2026-01-01T00:00:47Z", "{}", "{\"reason\":\"operator\"}", "homeostat-cancel-once")
    var cancelled_homeostat_row = homeostat_journal.db.query("SELECT status,values_json FROM homeostats WHERE run_id=? AND id=?")
    cancelled_homeostat_row.bind_text(1, "run-homeostat"); cancelled_homeostat_row.bind_text(2, "homeostat-cancel")
    var cancelled_homeostat_events = homeostat_journal.list_events("run-homeostat", "", "homeostat-cancel-process", -1, 0, "homeostat.cancelled")
    _check(cancelled_homeostat.status == "cancelled" and cancelled_homeostat_row.step() and cancelled_homeostat_row.column_text(0) == "cancelled" and cancelled_homeostat_row.column_text(1) == "{\"reason\":\"operator\"}" and len(cancelled_homeostat_events) == 1, "homeostat cancellation persists values")
    _ = homeostat_journal.schedule_process("run-homeostat", "homeostat-expire-process", "native", "2026-01-01T00:00:48Z", "{}", "{}", "", 1, 2, "2026-01-01T00:00:48Z")
    _ = homeostat_journal.claim_process("run-homeostat", "homeostat-expire-process", "homeostat-worker", "2026-01-01T00:00:49Z", "2026-01-01T00:01:00Z")
    _ = homeostat_journal.park_homeostat_process("run-homeostat", "homeostat-expire", "homeostat-expire-process", "homeostat-worker", "2026-01-01T00:00:50Z", "{\"prompt\":\"expire\"}", "{}", "homeostat-expire-open")
    var expired_homeostat = homeostat_journal.transition_homeostat_process("run-homeostat", "homeostat-expire", "homeostat-expire-process", "expired", "timed_out", "homeostat-worker", "2026-01-01T00:00:51Z", "{}", "{\"reason\":\"timeout\"}", "homeostat-expire-once")
    var expired_homeostat_events = homeostat_journal.list_events("run-homeostat", "", "homeostat-expire-process", -1, 0, "homeostat.expired")
    _check(expired_homeostat.status == "timed_out" and len(expired_homeostat_events) == 1, "homeostat expiration persists terminal status")
    homeostat_journal.close()
    var reopened_homeostat_journal = NativeJournal.open(homeostat_path)
    reopened_homeostat_journal.initialize()
    var reopened_homeostat = reopened_homeostat_journal.db.query("SELECT status,attempt,max_attempts FROM homeostats WHERE run_id=? AND id=?")
    reopened_homeostat.bind_text(1, "run-homeostat"); reopened_homeostat.bind_text(2, "homeostat-open")
    var reopened_terminal_process = reopened_homeostat_journal.get_process("run-homeostat", "homeostat-process")
    _check(reopened_homeostat.step() and reopened_homeostat.column_text(0) == "completed" and reopened_terminal_process.status == "succeeded", "homeostat and process survive reopen")
    var reopened_terminal_events = reopened_homeostat_journal.list_events("run-homeostat", "", "homeostat-process", -1, 0)
    var reopened_terminal_commands = reopened_homeostat_journal.list_commands("run-homeostat")
    _check(len(reopened_terminal_events) == 6 and len(reopened_terminal_commands) == 16, "homeostat reopen event and command totals")
    var first_reopen = reopened_homeostat_journal.reopen_homeostat_process(
        "run-homeostat", "homeostat-open", "homeostat-process", "homeostat-worker",
        "2026-01-01T00:00:49Z"
    )
    var first_reopen_row = reopened_homeostat_journal.db.query("SELECT status,attempt,max_attempts FROM homeostats WHERE run_id=? AND id=?")
    first_reopen_row.bind_text(1, "run-homeostat"); first_reopen_row.bind_text(2, "homeostat-open")
    _check(first_reopen_row.step() and first_reopen_row.column_text(0) == "open" and first_reopen_row.column_int(1) == 2 and first_reopen_row.column_int(2) == 3 and first_reopen.status == "waiting", "first omitted-key reopen increments attempt and waits")
    var first_claim_rejected = False
    try:
        _ = reopened_homeostat_journal.claim_process("run-homeostat", "homeostat-process", "homeostat-worker-2", "2026-01-01T00:00:50Z", "2026-01-01T00:01:00Z")
    except err:
        first_claim_rejected = True
    var first_waiting = reopened_homeostat_journal.get_process("run-homeostat", "homeostat-process")
    _check(first_claim_rejected and first_waiting.status == "waiting" and first_waiting.lease_owner == "", "first reopen process is non-claimable")
    var first_reopen_key = "homeostat.reopen:homeostat-open:2"
    var first_reopen_replay = reopened_homeostat_journal.reopen_homeostat_process(
        "run-homeostat", "homeostat-open", "homeostat-process", "homeostat-worker",
        "2026-01-01T00:00:49Z", first_reopen_key
    )
    var reopen_commands_once = reopened_homeostat_journal.list_commands("run-homeostat", "homeostat.reopen")
    var reopen_events_once = reopened_homeostat_journal.list_events("run-homeostat", "", "homeostat-process", -1, 0, "homeostat.reopened")
    _check(first_reopen_replay.status == "waiting" and len(reopen_commands_once) == 1 and len(reopen_events_once) == 1, "derived reopen replay has no duplicate command or event")
    var second_terminal = reopened_homeostat_journal.transition_homeostat_process(
        "run-homeostat", "homeostat-open", "homeostat-process", "completed", "succeeded",
        "homeostat-worker", "2026-01-01T00:00:51Z", "{\"approved\":false}", "{}", "homeostat-complete-second"
    )
    _check(second_terminal.status == "succeeded", "second terminal transition permits reopen")
    var second_reopen = reopened_homeostat_journal.reopen_homeostat_process(
        "run-homeostat", "homeostat-open", "homeostat-process", "homeostat-worker",
        "2026-01-01T00:00:52Z"
    )
    var second_reopen_row = reopened_homeostat_journal.db.query("SELECT status,attempt,max_attempts FROM homeostats WHERE run_id=? AND id=?")
    second_reopen_row.bind_text(1, "run-homeostat"); second_reopen_row.bind_text(2, "homeostat-open")
    _check(second_reopen_row.step() and second_reopen_row.column_text(0) == "open" and second_reopen_row.column_int(1) == 3 and second_reopen_row.column_int(2) == 3 and second_reopen.status == "waiting", "second omitted-key reopen derives a distinct key")
    var second_reopen_key = "homeostat.reopen:homeostat-open:3"
    var second_reopen_replay = reopened_homeostat_journal.reopen_homeostat_process(
        "run-homeostat", "homeostat-open", "homeostat-process", "homeostat-worker",
        "2026-01-01T00:00:52Z", second_reopen_key
    )
    var reopen_commands = reopened_homeostat_journal.list_commands("run-homeostat", "homeostat.reopen")
    var reopen_events = reopened_homeostat_journal.list_events("run-homeostat", "", "homeostat-process", -1, 0, "homeostat.reopened")
    _check(second_reopen_replay.status == "waiting" and len(reopen_commands) == 2 and len(reopen_events) == 2 and first_reopen_key != second_reopen_key, "second derived reopen replay remains idempotent")
    var explicit_reopen_conflict = False
    try:
        _ = reopened_homeostat_journal.reopen_homeostat_process(
            "run-homeostat", "homeostat-open", "other-process", "homeostat-worker",
            "2026-01-01T00:00:52Z", second_reopen_key
        )
    except err:
        explicit_reopen_conflict = True
    _check(explicit_reopen_conflict, "explicit reopen conflicting identity rejected")
    reopened_homeostat_journal.close()
    _cleanup(homeostat_path)
    # Explicit parent/target journals close delegation waits from durable child boundaries.
    var parent_path = "/tmp/fala-native-delegation-parent.sqlite"
    var target_path = "/tmp/fala-native-delegation-target.sqlite"
    _cleanup(parent_path); _cleanup(target_path)
    var parent_journal = NativeJournal.open(parent_path); parent_journal.initialize()
    var target_journal = NativeJournal.open(target_path); target_journal.initialize()
    _ = parent_journal.create_run("parent", "active", "{}", "2026-01-01T00:01:00Z")
    _ = target_journal.create_run("child-ok", "active", "{}", "2026-01-01T00:01:00Z")
    _ = target_journal.schedule_process("child-ok", "child-process", "native", "2026-01-01T00:01:01Z")
    _ = target_journal.claim_process("child-ok", "child-process", "child-worker", "2026-01-01T00:01:02Z", "2026-01-01T00:02:00Z")
    _ = target_journal.complete_process("child-ok", "child-process", "child-worker", "2026-01-01T00:01:03Z", "{\"value\":1}")
    _ = parent_journal.schedule_process("parent", "delegation-ok", "native", "2026-01-01T00:01:01Z", "{}", "{}", "", 1, 1)
    _ = parent_journal.claim_process("parent", "delegation-ok", "parent-worker", "2026-01-01T00:01:02Z", "2026-01-01T00:02:00Z")
    _ = parent_journal.park_homeostat_process("parent", "delegation-ok-homeostat", "delegation-ok", "parent-worker", "2026-01-01T00:01:03Z", "{\"delivery_id\":\"delivery-ok\",\"target_run_id\":\"child-ok\"}")
    var observed = observe_run_boundary(target_journal, "child-ok")
    _check(observed.derived_status == "completed" and observed.run_id == "child-ok", "completed child boundary")
    var closed_ok = close_delegations(parent_journal, target_journal, "parent", "parent-closer", "2026-01-01T00:01:04Z")
    _check(len(closed_ok) == 1 and closed_ok[0].status == "succeeded" and closed_ok[0].output_json.find("run.boundary") >= 0, "completed child closes parent")
    var replay_ok = close_delegations(parent_journal, target_journal, "parent", "parent-closer", "2026-01-01T00:01:04Z")
    _check(len(replay_ok) == 0, "repeated delegation closure is a no-op")
    _ = target_journal.create_run("child-failed", "active", "{}", "2026-01-01T00:01:00Z")
    _ = target_journal.schedule_process("child-failed", "child-failed-process", "native", "2026-01-01T00:01:01Z")
    _ = target_journal.claim_process("child-failed", "child-failed-process", "child-worker", "2026-01-01T00:01:02Z", "2026-01-01T00:02:00Z")
    _ = target_journal.fail_process("child-failed", "child-failed-process", "child-worker", "2026-01-01T00:01:03Z", "{\"reason\":\"boom\"}")
    _ = parent_journal.schedule_process("parent", "delegation-failed", "native", "2026-01-01T00:01:01Z", "{}", "{}", "", 1, 1)
    _ = parent_journal.claim_process("parent", "delegation-failed", "parent-worker", "2026-01-01T00:01:02Z", "2026-01-01T00:02:00Z")
    _ = parent_journal.park_homeostat_process("parent", "delegation-failed-homeostat", "delegation-failed", "parent-worker", "2026-01-01T00:01:03Z", "{\"delivery_id\":\"delivery-failed\",\"target_run_id\":\"child-failed\"}")
    var closed_failed = close_delegations(parent_journal, target_journal, "parent", "parent-closer", "2026-01-01T00:01:04Z")
    _check(len(closed_failed) == 1 and closed_failed[0].status == "failed" and closed_failed[0].error_json.find("DelegatedRunNotCompleted") >= 0 and closed_failed[0].error_json.find("run.boundary") >= 0, "failed child fails parent")
    _ = target_journal.create_run("child-waiting", "active", "{}", "2026-01-01T00:01:00Z")
    _ = target_journal.schedule_process("child-waiting", "child-waiting-process", "native", "2026-01-01T00:01:01Z")
    _ = parent_journal.schedule_process("parent", "delegation-waiting", "native", "2026-01-01T00:01:01Z", "{}", "{}", "", 1, 1)
    _ = parent_journal.claim_process("parent", "delegation-waiting", "parent-worker", "2026-01-01T00:01:02Z", "2026-01-01T00:02:00Z")
    _ = parent_journal.park_homeostat_process("parent", "delegation-waiting-homeostat", "delegation-waiting", "parent-worker", "2026-01-01T00:01:03Z", "{\"delivery_id\":\"delivery-waiting\",\"target_run_id\":\"child-waiting\"}")
    var closed_waiting = close_delegations(parent_journal, target_journal, "parent", "parent-closer", "2026-01-01T00:01:04Z")
    _check(len(closed_waiting) == 0 and parent_journal.get_process("parent", "delegation-waiting").status == "waiting", "in-flight child remains waiting")
    parent_journal.close(); target_journal.close(); _cleanup(parent_path); _cleanup(target_path)
    print("journal smoke success: atomic rollback sequence metadata replay conflict")
