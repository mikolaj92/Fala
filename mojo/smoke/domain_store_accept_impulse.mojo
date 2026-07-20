from fala.domain_store import NativeDomainStore
from fala.domain import Impulse


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("domain store accept smoke: " + message)


def _seed_run(mut store: NativeDomainStore, run_id: String) raises:
    var stmt = store.db.query("INSERT INTO runs (id,status,metadata,created_at,updated_at,schema_version) VALUES (?, 'active', '{}', ?, ?, 6)")
    stmt.bind_text(1, run_id)
    stmt.bind_text(2, "2026-01-01T00:00:00Z")
    stmt.bind_text(3, "2026-01-01T00:00:00Z")
    _ = stmt.step()
    stmt.close()
    store.db.commit()


def main() raises:
    var store = NativeDomainStore.open(":memory:\0")
    store.initialize()
    _seed_run(store, "accept-run")
    var impulse = Impulse("impulse-1", "accept-run", "demo", "{\"value\":1}", "{}", "2026-01-01T00:00:01Z", "2026-01-01T00:00:01Z")
    var first = store.accept_impulse(impulse, "accept-key", "2026-01-01T00:00:01Z", "smoke")
    _check(not first.replayed and first.command.command_type == "impulse.accept" and len(first.events) == 1, "initial result")
    var replay = store.accept_impulse(impulse, "accept-key", "2026-01-01T00:00:01Z", "smoke")
    _check(replay.replayed and replay.command.id == "accept-key" and len(replay.events) == 1, "idempotent replay")
    var counts = store.db.query("SELECT COUNT(*) FROM impulses WHERE run_id=?")
    counts.bind_text(1, "accept-run"); _ = counts.step(); _check(counts.column_int(0) == 1, "persisted impulse")
    counts.close()
    var event_counts = store.db.query("SELECT COUNT(*) FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
    event_counts.bind_text(1, "accept-run"); event_counts.bind_text(2, "accept-key"); _ = event_counts.step(); _check(event_counts.column_int(0) == 1, "persisted command")
    event_counts.close()
    var conflict = False
    try:
        _ = store.accept_impulse(Impulse("impulse-1", "accept-run", "demo", "{\"value\":2}", "{}", "2026-01-01T00:00:01Z", "2026-01-01T00:00:01Z"), "accept-key", "2026-01-01T00:00:01Z", "smoke")
    except err:
        conflict = True
    _check(conflict, "conflicting replay rejected")
    _seed_run(store, "rollback-run")
    var blocker = store.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,payload,created_at) VALUES (?,?,?,?,?,?)")
    blocker.bind_text(1, "rollback-run"); blocker.bind_text(2, "rollback-key:event"); blocker.bind_text(3, "block"); blocker.bind_text(4, "blocker"); blocker.bind_text(5, "{}"); blocker.bind_text(6, "2026-01-01T00:00:02Z"); _ = blocker.step(); blocker.close()
    var rollback_event = store.db.query("INSERT INTO runtime_events (run_id,sequence,id,event_type,schema_version,command_id,payload,created_at) VALUES (?,?,?,?,1,?,?,?)")
    rollback_event.bind_text(1, "rollback-run"); rollback_event.bind_int(2, 1); rollback_event.bind_text(3, "rollback-key:event"); rollback_event.bind_text(4, "block"); rollback_event.bind_text(5, "rollback-key:event"); rollback_event.bind_text(6, "{}"); rollback_event.bind_text(7, "2026-01-01T00:00:02Z"); _ = rollback_event.step(); rollback_event.close(); store.db.commit()
    var rolled_back = False
    try:
        _ = store.accept_impulse(Impulse("rollback-impulse", "rollback-run", "demo", "{}", "{}", "2026-01-01T00:00:03Z", "2026-01-01T00:00:03Z"), "rollback-key", "2026-01-01T00:00:03Z")
    except err:
        rolled_back = True
    _check(rolled_back, "event collision fails acceptance")
    var rollback_count = store.db.query("SELECT COUNT(*) FROM impulses WHERE run_id=?")
    rollback_count.bind_text(1, "rollback-run"); _ = rollback_count.step(); _check(rollback_count.column_int(0) == 0, "failed acceptance rolls back impulse")
    rollback_count.close()
    store.close()
    print("domain store accept impulse smoke ok")
