from fala.bridge_transport import deliver_local_bridge
from fala.domain import BridgeDelivery, EventRef, Impulse, RuntimeBudget, RuntimeRef, RunRef
from fala.domain_store import NativeDomainStore
from std.os import remove


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("bridge transport smoke: " + message)


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


def _seed_run(mut store: NativeDomainStore, run_id: String) raises:
    var stmt = store.db.query("INSERT INTO runs (id,status,metadata,created_at,updated_at,schema_version) VALUES (?, 'completed', '{}', ?, ?, 6)")
    stmt.bind_text(1, run_id)
    stmt.bind_text(2, "2026-01-01T00:00:00Z")
    stmt.bind_text(3, "2026-01-01T00:00:00Z")
    _ = stmt.step()
    stmt.close()
    store.db.commit()


def _delivery(row_id: String, impulse_id: String, value: String) -> BridgeDelivery:
    var source = RunRef(RuntimeRef("runtime-source", "file://source", "{}"), "source-run")
    var target = RunRef(RuntimeRef("runtime-target", "file://target", "{}"), "target-run")
    var impulse = Impulse(impulse_id, "source-run", "bridge.smoke", "{\"value\":" + value + "}", "{}", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z")
    return BridgeDelivery(
        id=row_id,
        run_id="source-run",
        idempotency_key="source-key-" + row_id,
        source=source^,
        target=target^,
        impulse=impulse^,
        event_ref=EventRef(RuntimeRef("runtime", "", "{}"), "run", "", 0),
        pool_id="",
        budget=RuntimeBudget(runtime_hops=2, impulse_count=2),
        status="pending",
        attempts=0,
        metadata="{}",
        created_at="2026-01-01T00:00:00Z",
        updated_at="2026-01-01T00:00:00Z",
    )


def main() raises:
    var source_path = "/tmp/fala-bridge-transport-source.sqlite"
    var target_path = "/tmp/fala-bridge-transport-target.sqlite"
    _clean(source_path)
    _clean(target_path)

    var source = NativeDomainStore.open(source_path)
    source.initialize()
    _seed_run(source, "source-run")
    var first = _delivery("bridge-1", "impulse-1", "1")
    _ = source.put_bridge_delivery(first)
    source.close()

    # Initialize target before delivery to prove target schema setup is explicit.
    var target = NativeDomainStore.open(target_path)
    target.initialize()
    _seed_run(target, "target-run")
    target.close()

    # Fresh delivery imports before committing the source transition.
    var fresh = deliver_local_bridge(source_path, target_path, "source-run", "bridge-1", "2026-01-01T00:00:01Z")
    _check(fresh.source_delivery.status == "delivered" and fresh.imported_delivery.status == "imported", "fresh local delivery")
    _check(not fresh.source_replayed and not fresh.imported_replayed, "fresh delivery replay markers")

    var replay = deliver_local_bridge(source_path, target_path, "source-run", "bridge-1", "2026-01-01T00:00:02Z")
    _check(replay.source_replayed and replay.imported_replayed, "stable delivery replay")
    _check(replay.source_delivery.attempts == fresh.source_delivery.attempts, "source replay does not consume again")

    var invalid = False
    try:
        _ = deliver_local_bridge("bad\npath", target_path, "source-run", "bridge-1", "2026-01-01T00:00:03Z")
    except err:
        invalid = True
    _check(invalid, "invalid path rejected")

    var same_path = False
    try:
        _ = deliver_local_bridge(source_path, source_path, "source-run", "bridge-1", "2026-01-01T00:00:03Z")
    except err:
        same_path = True
    _check(same_path, "source and target path collision rejected")

    # Source-defined terminal marker for executable smoke runners.
    print("bridge transport smoke: ok")
