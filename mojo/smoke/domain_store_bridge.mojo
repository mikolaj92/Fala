from fala.domain_store import NativeDomainStore
from fala.domain import BridgeDelivery, EventRef, Impulse, RuntimeRef, RunRef, RuntimeBudget
from std.os import remove



def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("domain store bridge smoke: " + message)


def _seed_run(mut store: NativeDomainStore, run_id: String) raises:
    var stmt = store.db.query("INSERT INTO runs (id,status,metadata,created_at,updated_at,schema_version) VALUES (?, 'completed', '{}', ?, ?, 6)")
    stmt.bind_text(1, run_id)
    stmt.bind_text(2, "2026-01-01T00:00:00Z")
    stmt.bind_text(3, "2026-01-01T00:00:00Z")
    _ = stmt.step()
    stmt.close()
    store.db.commit()


def _delivery(run_id: String, row_id: String, key: String, status: String = "pending", created_at: String = "2026-01-01T00:00:00Z", payload: String = "{\"value\":1}", metadata: String = "{}") -> BridgeDelivery:
    var runtime = RuntimeRef("runtime-a", "memory://runtime-a", "{\"region\":\"source\"}")
    var source = RunRef(runtime.copy(), run_id)
    var target = RunRef(RuntimeRef("runtime-b", "memory://runtime-b", "{\"region\":\"target\"}"), "target-run")
    var impulse = Impulse("impulse-1", run_id, "bridge.test", payload, "{}", created_at, created_at)
    return BridgeDelivery(
        id=row_id,
        run_id=run_id,
        idempotency_key=key,
        source=source^,
        target=target^,
        impulse=impulse^,
        pool_id="pool-a",
        budget=RuntimeBudget(runtime_hops=1, impulse_count=1, attempts=2, reaction_bytes=32),
        status=status,
        attempts=0,
        metadata=metadata,
        created_at=created_at,
        updated_at=created_at,
    )


def main() raises:
    var bridge_path = "/tmp/fala-domain-store-bridge-smoke.sqlite"
    try:
        remove(bridge_path)
    except err:
        pass
    try:
        remove(bridge_path + "-wal")
    except err:
        pass
    try:
        remove(bridge_path + "-shm")
    except err:
        pass
    var store = NativeDomainStore.open(bridge_path)
    store.initialize()
    _seed_run(store, "target-run")
    _seed_run(store, "run-bridge")
    var enqueue = _delivery("run-bridge", "enqueue-1", "", "pending", "2026-01-01T00:00:01Z", "{\"value\":7}")
    enqueue.impulse.id = "enqueue-impulse"
    enqueue.updated_at = ""
    enqueue.budget = RuntimeBudget(runtime_hops=2, impulse_count=2, spawned_runs=1, attempts=0, reaction_bytes=32, attempts_limited=True)
    var enqueue_before = enqueue.to_json()
    var enqueued = store.enqueue_bridge_delivery(enqueue, "enqueue-key", "bridge-test", "corr-1", "cause-1")
    _check(enqueue.to_json() == enqueue_before, "enqueue does not mutate caller")
    _check(enqueued.delivery.status == "pending" and enqueued.delivery.idempotency_key == "enqueue-key" and enqueued.delivery.updated_at == enqueued.delivery.created_at and not enqueued.submission.replayed, "fresh bridge enqueue")
    _check(enqueued.submission.command.command_type == "bridge.outbox.enqueue" and len(enqueued.submission.events) == 1 and enqueued.submission.events[0].event_type == "bridge.outbox.enqueued" and enqueued.submission.command.actor == "bridge-test" and enqueued.submission.command.correlation_id == "corr-1" and enqueued.submission.command.causation_id == "cause-1" and enqueued.submission.events[0].actor == "bridge-test" and enqueued.submission.events[0].correlation_id == "corr-1" and enqueued.submission.events[0].causation_id == "cause-1", "enqueue command and event metadata inheritance")
    var command_count_stmt = store.db.query("SELECT COUNT(*) FROM runtime_commands WHERE run_id=? AND command_type=?")
    command_count_stmt.bind_text(1, "run-bridge"); command_count_stmt.bind_text(2, "bridge.outbox.enqueue")
    var command_count = 0
    if command_count_stmt.step(): command_count = command_count_stmt.column_int(0)
    command_count_stmt.close()
    var event_count_stmt = store.db.query("SELECT COUNT(*) FROM runtime_events WHERE run_id=? AND impulse_id=? AND event_type=?")
    event_count_stmt.bind_text(1, "run-bridge"); event_count_stmt.bind_text(2, "enqueue-impulse"); event_count_stmt.bind_text(3, "bridge.outbox.enqueued")
    var event_count = 0
    if event_count_stmt.step(): event_count = event_count_stmt.column_int(0)
    event_count_stmt.close()
    var metadata_command = store.db.query("SELECT actor,correlation_id,causation_id FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
    metadata_command.bind_text(1, "run-bridge"); metadata_command.bind_text(2, "enqueue-key")
    _check(metadata_command.step() and metadata_command.column_text(0) == "bridge-test" and metadata_command.column_text(1) == "corr-1" and metadata_command.column_text(2) == "cause-1", "persisted enqueue command metadata")
    metadata_command.close()
    var metadata_event = store.db.query("SELECT actor,correlation_id,causation_id FROM runtime_events WHERE run_id=? AND id=?")
    metadata_event.bind_text(1, "run-bridge"); metadata_event.bind_text(2, "enqueue-key:event")
    _check(metadata_event.step() and metadata_event.column_text(0) == "bridge-test" and metadata_event.column_text(1) == "corr-1" and metadata_event.column_text(2) == "cause-1", "persisted enqueue event metadata")
    metadata_event.close()
    var attempts_zero = _delivery("run-bridge", "attempts-zero", "attempts-zero-key")
    attempts_zero.budget = RuntimeBudget(runtime_hops=1, impulse_count=1, attempts=0, attempts_limited=True)
    _ = store.put_bridge_delivery(attempts_zero)
    var attempts_zero_rejected = False
    try:
        _ = store.retry_bridge_delivery("bridge_outbox", "run-bridge", "attempts-zero", "2026-01-01T00:00:01Z", "bridge.retry:attempts-zero")
    except err:
        attempts_zero_rejected = True
    var attempts_zero_after = store.get_outbox_delivery("run-bridge", "attempts-zero")
    _check(attempts_zero_rejected and attempts_zero_after.attempts == 0 and attempts_zero_after.status == "pending", "explicit zero attempts budget rejected without mutation")
    var second_target = _delivery("run-bridge", "second-target", "second-target-key")
    second_target.target.run_id = "other-target"
    second_target.impulse.id = "second-target-impulse"
    second_target.budget = RuntimeBudget(runtime_hops=1, impulse_count=1, spawned_runs=1, spawned_runs_limited=True)
    var before_second_target = len(store.list_bridge_deliveries("run-bridge"))
    var second_target_rejected = False
    try:
        _ = store.enqueue_bridge_delivery(second_target, "second-target-key", "bridge-test", "corr-2", "cause-2")
    except err:
        second_target_rejected = True
    _check(second_target_rejected and len(store.list_bridge_deliveries("run-bridge")) == before_second_target, "distinct spawned target exhausts budget")
    var preexisting_impulse = Impulse(id="preexisting-impulse", run_id="run-bridge", impulse_type="bridge.test", payload="{\"value\":8}", metadata="{}", created_at="2026-01-01T00:00:01Z", updated_at="2026-01-01T00:00:01Z")
    store.put_impulse(preexisting_impulse)
    var impulse_rows_before = store.list_impulses("run-bridge")
    var optional_impulse = _delivery("run-bridge", "optional-impulse", "optional-impulse-key", "pending", "2026-01-01T00:00:01Z", "{\"value\":8}")
    optional_impulse.impulse = preexisting_impulse.copy()
    var optional_saved = store.enqueue_bridge_delivery(optional_impulse, "optional-impulse-key", "bridge-test", "corr-3", "cause-3")
    var impulse_rows_after = store.list_impulses("run-bridge")
    _check(not optional_saved.submission.replayed and len(impulse_rows_after) == len(impulse_rows_before), "identical preexisting impulse is not duplicated")
    _check(command_count == 1 and event_count == 1 and enqueued.submission.events[0].sequence == 1, "enqueue journal persistence")
    enqueue.run_id = "alternate-owner"
    var enqueue_replay = store.enqueue_bridge_delivery(enqueue, "enqueue-key", "bridge-test", "corr-1", "cause-1")
    _check(enqueue_replay.submission.replayed and enqueue_replay.delivery.run_id == "run-bridge" and enqueue.run_id == "alternate-owner" and enqueue_replay.delivery.attempts == 0 and enqueue_replay.delivery.budget.runtime_hops == 2, "enqueue replay normalizes owner without mutating caller")
    var enqueue_conflict = False
    enqueue.target.run_id = "other-target"
    try:
        _ = store.enqueue_bridge_delivery(enqueue, "enqueue-key", "bridge-test", "corr-1", "cause-1")
    except err:
        enqueue_conflict = True
    var command_count_after = store.db.query("SELECT COUNT(*) FROM runtime_commands WHERE run_id=? AND command_type=?")
    command_count_after.bind_text(1, "run-bridge"); command_count_after.bind_text(2, "bridge.outbox.enqueue")
    var command_total = 0
    if command_count_after.step(): command_total = command_count_after.column_int(0)
    var bad_enqueue = _delivery("run-bridge", "bad-enqueue", "bad-enqueue-key")
    bad_enqueue.impulse.run_id = "other-run"
    var bad_enqueue_rejected = False
    try:
        _ = store.enqueue_bridge_delivery(bad_enqueue, "bad-enqueue-key", "bridge-test", "corr-1", "cause-1")
    except err:
        bad_enqueue_rejected = True
    _check(bad_enqueue_rejected, "enqueue impulse run mismatch rejected")
    command_count_after.close()
    _check(enqueue_conflict and command_total == 2, "enqueue immutable conflict rollback")
    # Inbox rows are owned by the target run; source references remain remote.
    var inbox = _delivery("target-run", "inbox-1", "inbox-key")
    inbox.source.run_id = "run-bridge"
    inbox.impulse.run_id = "run-bridge"
    var inbox_saved = store.put_inbox_delivery(inbox)
    _check(inbox_saved.status == "pending" and inbox_saved.source.run_id == "run-bridge", "inbox creation preserves remote source")
    var inbox_loaded = store.get_inbox_delivery("target-run", "inbox-1")
    _check(inbox_loaded.run_id == "target-run" and inbox_loaded.impulse.run_id == "run-bridge", "inbox get decodes cross-run impulse")
    _check(inbox_loaded.source.run_id == "run-bridge" and inbox_loaded.target.run_id == "target-run", "inbox get preserves source and target runs")
    var inbox_decoded = store.list_inbox_records("target-run")
    _check(len(inbox_decoded) == 1 and inbox_decoded[0].run_id == "target-run" and inbox_decoded[0].impulse.run_id == "run-bridge", "inbox list decodes cross-run impulse")
    _check(inbox_decoded[0].source.run_id == "run-bridge" and inbox_decoded[0].target.run_id == "target-run", "inbox list preserves source and target runs")
    var imported = _delivery("target-run", "inbox-1", "inbox-key", "imported")
    imported.source.run_id = "run-bridge"
    imported.impulse.run_id = "target-run"
    imported.event_ref = EventRef(RuntimeRef("runtime-a", "memory://runtime-a", "{}"), "run-bridge", "source-event", 3)
    var imported_saved = store.import_inbox_delivery(imported)
    _check(imported_saved.status == "imported" and imported_saved.impulse.run_id == "target-run", "inbox import validates target run")
    _check(imported_saved.budget.runtime_hops == 0 and imported_saved.budget.impulse_count == 0 and imported_saved.attempts == 1, "inbox import consumes hop impulse and attempt")
    var imported_replay = store.import_inbox_delivery(imported)
    var inbox_rows = store.list_bridge_inbox("target-run", "imported")
    _check(imported_replay.status == "imported" and imported_replay.attempts == 1 and imported_replay.budget.runtime_hops == 0 and imported_replay.budget.impulse_count == 0 and len(inbox_rows) == 1 and inbox_rows[0].find("\"idempotency_key\":\"inbox-key\"") >= 0, "duplicate import terminal replay preserves budget")
    var wrapped = _delivery("run-bridge", "wrapped-inbox", "wrapped-source-key")
    wrapped.impulse.id = "wrapped-impulse"
    wrapped.source.run_id = "run-bridge"
    wrapped.impulse.run_id = "run-bridge"
    var wrapped_before = wrapped.to_json()
    var wrapped_saved = store.import_bridge_delivery(wrapped, "wrapped-local-key")
    _check(wrapped.to_json() == wrapped_before, "bridge import does not mutate caller")
    _check(wrapped_saved.run_id == "target-run" and wrapped_saved.status == "imported" and wrapped_saved.idempotency_key == "wrapped-local-key" and wrapped_saved.attempts == 1 and wrapped_saved.budget.runtime_hops == 0 and wrapped_saved.budget.impulse_count == 0, "bridge import wrapper normalizes and consumes once")
    _check(wrapped_saved.impulse.run_id == "target-run" and wrapped_saved.impulse.metadata.find("source_runtime_id") >= 0 and wrapped_saved.impulse.metadata.find("source_run_id") >= 0 and wrapped_saved.impulse.metadata.find("source_impulse_id") >= 0, "bridge import wrapper persists source metadata")
    var wrapped_replay = store.import_bridge_delivery(wrapped, "wrapped-local-key")
    _check(wrapped_replay.attempts == 1 and wrapped_replay.budget.runtime_hops == 0 and wrapped_replay.budget.impulse_count == 0, "bridge import wrapper replay preserves consumption")
    var collision = _delivery("run-bridge", "collision-inbox", "collision-source-key")
    collision.impulse.id = "wrapped-impulse"
    collision.impulse.run_id = "run-bridge"
    collision.impulse.payload = "{\"value\":999}"
    collision.source.run_id = "run-bridge"
    var collision_rejected = False
    try:
        _ = store.import_bridge_delivery(collision, "collision-local-key")
    except err:
        collision_rejected = True
    _check(collision_rejected, "bridge import rejects target impulse collision")
    var bad_target = _delivery("target-run", "bad-target", "bad-target-key")
    bad_target.target.run_id = "other-run"
    var bad_target_rejected = False
    try:
        _ = store.put_inbox_delivery(bad_target)
    except err:
        bad_target_rejected = True
    _check(bad_target_rejected, "inbox target run mismatch rejected")
    var missing_inbox_run_rejected = False
    try:
        _ = store.list_bridge_inbox("missing-run")
    except err:
        missing_inbox_run_rejected = True
    _check(missing_inbox_run_rejected, "unknown inbox run rejected")

    var first = _delivery("run-bridge", "outbox-1", "outbox-key", "pending", "2026-01-01T00:00:02Z")
    first.budget = RuntimeBudget(runtime_hops=2, impulse_count=2, attempts=4, reaction_bytes=32)
    var second = _delivery("run-bridge", "outbox-2", "outbox-key-2", "pending", "2026-01-01T00:00:01Z")
    _ = store.put_bridge_delivery(first)
    _ = store.put_bridge_delivery(second)
    var listed = store.list_bridge_deliveries("run-bridge")
    _check(len(listed) == 5, "outbox creation count=" + String(len(listed)))
    _check(listed[0].find("\"id\":\"attempts-zero\"") >= 0 and listed[1].find("\"id\":\"enqueue-1\"") >= 0 and listed[2].find("\"id\":\"optional-impulse\"") >= 0 and listed[3].find("\"id\":\"outbox-2\"") >= 0 and listed[4].find("\"id\":\"outbox-1\"") >= 0, "deterministic created_at listing")
    var loaded = store.get_outbox_delivery("run-bridge", "outbox-1")
    _check(loaded.source.runtime.uri == "memory://runtime-a" and loaded.source.runtime.metadata == "{\"region\":\"source\"}", "source runtime metadata round-trip")
    _check(loaded.target.runtime.uri == "memory://runtime-b" and loaded.target.runtime.metadata == "{\"region\":\"target\"}", "target runtime metadata round-trip")
    var bad_source = _delivery("run-bridge", "bad-source", "bad-source-key")
    bad_source.source.run_id = "other-run"
    var bad_source_rejected = False
    try:
        _ = store.put_bridge_delivery(bad_source)
    except err:
        bad_source_rejected = True
    _check(bad_source_rejected, "source run mismatch rejected")
    var bad_impulse = _delivery("run-bridge", "bad-impulse", "bad-impulse-key")
    bad_impulse.impulse.run_id = "other-run"
    var bad_impulse_rejected = False
    try:
        _ = store.put_bridge_delivery(bad_impulse)
    except err:
        bad_impulse_rejected = True
    _check(bad_impulse_rejected, "impulse run mismatch rejected")
    var event_delivery = _delivery("run-bridge", "event-1", "event-key")
    event_delivery.event_ref = EventRef(RuntimeRef("runtime-c", "memory://runtime-c", "{\"region\":\"event\"}"), "run-bridge", "event-1", 7)
    var event_saved = store.put_bridge_delivery(event_delivery)
    var event_loaded = store.get_outbox_delivery("run-bridge", "event-1")
    _check(event_saved.event_ref.runtime.uri == "memory://runtime-c" and event_loaded.event_ref.runtime.metadata == "{\"region\":\"event\"}", "event runtime metadata round-trip")
    _check(event_loaded.event_ref.sequence == 7, "event sequence round-trip")
    var bad_event = _delivery("run-bridge", "bad-event", "bad-event-key")
    bad_event.event_ref = EventRef(RuntimeRef("runtime-c", "memory://runtime-c", "{\"region\":\"event\"}"), "other-run", "event-1", 1)
    var bad_event_rejected = False
    try:
        _ = store.put_bridge_delivery(bad_event)
    except err:
        bad_event_rejected = True
    _check(bad_event_rejected, "event run mismatch rejected")
    # Claim and retry keep canonical pending status; retry increments attempts
    # once and an idempotency replay does not increment it again.
    var claimed = store.claim_bridge_delivery("bridge_outbox", "run-bridge", "outbox-1", "2026-01-01T00:00:03Z", "bridge.claim:outbox-1")
    _check(claimed.status == "pending" and claimed.attempts == 0, "pending claim")
    var retried = store.retry_bridge_delivery("bridge_outbox", "run-bridge", "outbox-1", "2026-01-01T00:00:04Z", "bridge.retry:outbox-1")
    _check(retried.status == "pending" and retried.attempts == 1, "pending retry increments attempts")
    var retried_replay = store.retry_bridge_delivery("bridge_outbox", "run-bridge", "outbox-1", "2026-01-01T00:00:05Z", "bridge.retry:outbox-1")
    _check(retried_replay.status == "pending" and retried_replay.attempts == 1, "retry idempotency replay")
    var retried_again = store.retry_bridge_delivery("bridge_outbox", "run-bridge", "outbox-1", "2026-01-01T00:00:05Z", "bridge.retry:outbox-1-second")
    _check(retried_again.status == "pending" and retried_again.attempts == 2, "second retry increments attempts")
    var old_retry_conflict = False
    try:
        _ = store.retry_bridge_delivery("bridge_outbox", "run-bridge", "outbox-1", "2026-01-01T00:00:06Z", "bridge.retry:outbox-1")
    except err:
        old_retry_conflict = True
    _check(old_retry_conflict, "replaying an earlier retry key conflicts after a later retry")
    var delivered_saved = store.deliver_bridge_delivery("bridge_outbox", "run-bridge", "outbox-1", "2026-01-01T00:00:02Z", "bridge.deliver:outbox-1")
    _check(delivered_saved.status == "delivered" and delivered_saved.attempts == 3 and delivered_saved.budget.runtime_hops == 1 and delivered_saved.budget.impulse_count == 1, "delivery consumes one hop and impulse and increments attempts")
    var delivered_replay = store.deliver_bridge_delivery("bridge_outbox", "run-bridge", "outbox-1", "2026-01-01T00:00:03Z", "bridge.deliver:outbox-1")
    _check(delivered_replay.status == "delivered" and delivered_replay.attempts == 3 and delivered_replay.budget.runtime_hops == 1 and delivered_replay.budget.impulse_count == 1, "identical delivery replay preserves budget")
    var exhausted = _delivery("run-bridge", "budget-exhausted", "budget-exhausted-key")
    exhausted.budget = RuntimeBudget(runtime_hops=1, impulse_count=1, attempts=1)
    exhausted.attempts = 1
    _ = store.put_bridge_delivery(exhausted)
    var exhausted_rejected = False
    try:
        _ = store.deliver_bridge_delivery("bridge_outbox", "run-bridge", "budget-exhausted", "2026-01-01T00:00:07Z", "bridge.deliver:budget-exhausted")
    except err:
        exhausted_rejected = True
    _check(exhausted_rejected, "delivery rejects attempts exhaustion")
    var hop_exhausted = _delivery("run-bridge", "hop-exhausted", "hop-exhausted-key")
    hop_exhausted.budget = RuntimeBudget(runtime_hops=0, impulse_count=2, attempts=2, runtime_hops_limited=True, impulse_count_limited=True)
    _ = store.put_bridge_delivery(hop_exhausted)
    var hop_rejected = False
    try:
        _ = store.deliver_bridge_delivery("bridge_outbox", "run-bridge", "hop-exhausted", "2026-01-01T00:00:08Z", "bridge.deliver:hop-exhausted")
    except err:
        hop_rejected = True
    var hop_after = store.get_outbox_delivery("run-bridge", "hop-exhausted")
    _check(hop_rejected and hop_after.status == "pending" and hop_after.attempts == 0 and hop_after.budget.runtime_hops == 0, "delivery rejects exhausted runtime hops without mutation")
    var impulse_exhausted = _delivery("run-bridge", "impulse-exhausted", "impulse-exhausted-key")
    impulse_exhausted.budget = RuntimeBudget(runtime_hops=2, impulse_count=0, attempts=2, runtime_hops_limited=True, impulse_count_limited=True)
    _ = store.put_bridge_delivery(impulse_exhausted)
    var impulse_rejected = False
    try:
        _ = store.deliver_bridge_delivery("bridge_outbox", "run-bridge", "impulse-exhausted", "2026-01-01T00:00:09Z", "bridge.deliver:impulse-exhausted")
    except err:
        impulse_rejected = True
    var impulse_after = store.get_outbox_delivery("run-bridge", "impulse-exhausted")
    _check(impulse_rejected and impulse_after.status == "pending" and impulse_after.attempts == 0 and impulse_after.budget.impulse_count == 0, "delivery rejects exhausted impulses without mutation")

    # Claimed/retrying are not bridge statuses and must be rejected before mutation.
    var claimed_rejected = False
    try:
        _ = store.put_bridge_delivery(_delivery("run-bridge", "outbox-1", "outbox-key", "claimed", "2026-01-01T00:00:02Z"))
    except err:
        claimed_rejected = True
    var retry_terminal = False
    try:
        _ = store.retry_bridge_delivery("bridge_outbox", "run-bridge", "outbox-1", "2026-01-01T00:00:06Z", "bridge.retry:outbox-1-terminal")
    except err:
        retry_terminal = True
    _check(retry_terminal, "terminal retry rejected")
    var claim_terminal = False
    try:
        _ = store.claim_bridge_delivery("bridge_outbox", "run-bridge", "outbox-1", "2026-01-01T00:00:06Z", "bridge.claim:outbox-1-terminal")
    except err:
        claim_terminal = True
    _check(claim_terminal, "terminal claim rejected")
    _check(claimed_rejected, "claimed status rejected")
    var retrying_rejected = False
    try:
        _ = store.put_bridge_delivery(_delivery("run-bridge", "outbox-1", "outbox-key", "retrying", "2026-01-01T00:00:02Z"))
    except err:
        retrying_rejected = True
    _check(retrying_rejected, "retrying status rejected")

    # Terminal-to-terminal changes and conflicting duplicate payloads roll back.
    var illegal_transition = False
    try:
        _ = store.put_bridge_delivery(_delivery("run-bridge", "outbox-1", "outbox-key", "imported", "2026-01-01T00:00:02Z"))
    except err:
        illegal_transition = True
    _check(illegal_transition, "terminal transition rejected")
    var before_conflict = store.list_bridge_deliveries("run-bridge")
    var conflict = False
    try:
        _ = store.put_bridge_delivery(_delivery("run-bridge", "outbox-1", "outbox-key", "delivered", "2026-01-01T00:00:02Z", "{\"value\":999}"))
    except err:
        conflict = True
    _check(conflict, "conflicting replay rejected")
    var after_conflict = store.list_bridge_deliveries("run-bridge")
    _check(len(after_conflict) == len(before_conflict) and after_conflict[1] == before_conflict[1], "conflicting replay rollback")

    # Pending may advance to imported and failed as legal terminal outcomes.
    _ = store.put_bridge_delivery(_delivery("run-bridge", "outbox-2", "outbox-key-2", "failed", "2026-01-01T00:00:01Z"))
    var failed_replay = store.put_bridge_delivery(_delivery("run-bridge", "outbox-2", "outbox-key-2", "failed", "2026-01-01T00:00:01Z"))
    _check(failed_replay.status == "failed", "pending to failed and replay")

    store.close()
    var reopened = NativeDomainStore.open(bridge_path)
    reopened.initialize()
    var reopened_delivery = reopened.get_outbox_delivery("run-bridge", "optional-impulse")
    _check(reopened_delivery.status == "pending" and reopened_delivery.impulse.id == "preexisting-impulse" and len(reopened.list_bridge_deliveries("run-bridge")) >= 3, "bridge rows survive close and reopen")
    reopened.close()
    remove(bridge_path)
    try:
        remove(bridge_path + "-wal")
    except err:
        pass
    try:
        remove(bridge_path + "-shm")
    except err:
        pass
    print("domain store bridge smoke ok")
