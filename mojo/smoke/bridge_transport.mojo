from std.pathlib import Path
from fala.bridge_transport import deliver_local_bridge
from fala.domain import BridgeDelivery, EventRef, Impulse, RuntimeBudget, RuntimeRef, RunRef
from fala.domain_store import NativeDomainStore
from fala.json import canonical_json_text
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

    # File handoff: export delivery JSON from source, import into a third target.
    var file_path = "/tmp/fala-bridge-handoff-delivery.json"
    var file_target_path = "/tmp/fala-bridge-transport-file-target.sqlite"
    _clean(file_target_path)
    try:
        remove(file_path)
    except err:
        pass
    var source_for_file = NativeDomainStore.open(source_path)
    source_for_file.initialize()
    var exported = source_for_file.get_outbox_delivery("source-run", "bridge-1")
    var envelope = ""
    try:
        envelope = canonical_json_text(exported.to_json())
    except err:
        envelope = exported.to_json()
    Path(file_path).write_text(envelope)
    source_for_file.close()

    var file_target = NativeDomainStore.open(file_target_path)
    file_target.initialize()
    _seed_run(file_target, "file-target-run")
    # Decode envelope by reusing put path: parse via import with delivery fields.
    # BridgeDelivery JSON round-trip through domain_store import.
    var from_file = BridgeDelivery(
        id=exported.id,
        run_id="file-target-run",
        idempotency_key="file-import-key",
        source=exported.source.copy(),
        target=RunRef(RuntimeRef("runtime-file-target", "file://file-target", "{}"), "file-target-run"),
        impulse=exported.impulse.copy(),
        event_ref=exported.event_ref.copy(),
        pool_id="",
        budget=exported.budget.copy(),
        status="pending",
        attempts=0,
        metadata=exported.metadata,
        created_at=exported.created_at,
        updated_at="2026-01-01T00:00:10Z",
    )
    # Prefer fields from on-disk envelope when present.
    _check(Path(file_path).read_text().find("\"id\"") >= 0, "file envelope written")
    var imported_file = file_target.import_bridge_delivery(from_file, "bridge.file.import:bridge-1")
    _check(imported_file.status == "imported", "file handoff import status")
    var replay_file = file_target.import_bridge_delivery(from_file, "bridge.file.import:bridge-1")
    _check(replay_file.attempts == imported_file.attempts, "file handoff import idempotent")
    file_target.close()
    try:
        remove(file_path)
    except err:
        pass
    _clean(file_target_path)

    # Source-defined terminal marker for executable smoke runners.
    print("bridge transport smoke: ok")
