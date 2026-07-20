"""Explicit same-process local SQLite bridge transport.

This module coordinates two :class:`NativeDomainStore` instances only when both
paths are local SQLite paths.  It imports into the target before marking the
source outbox row delivered, and uses stable idempotency keys so a retry is
recoverable.  The two SQLite transactions are not atomic with one another:
this is not a network transport and does not claim two-database atomicity.
"""

from std.ffi import CStringSlice, external_call
from std.memory import UnsafePointer, alloc
from std.pathlib import Path, cwd
from fala.domain import BridgeDelivery
from fala.domain_store import NativeDomainStore
from fala.json import canonical_json_text
from fala.sqlite import SQLiteError


def _json_quote(value: String) -> String:
    var result = "\""
    for i in range(value.byte_length()):
        var ch = value[byte=i]
        if ch == "\"": result += "\\\""
        elif ch == "\\": result += "\\\\"
        elif ch == "\n": result += "\\n"
        elif ch == "\r": result += "\\r"
        elif ch == "\t": result += "\\t"
        elif ch < " ": result += "\\u0000"
        else: result += String(ch)
    result += "\""
    return result


def _realpath(path: Path, label: String) raises -> Path:
    var source_text = path.__fspath__() + "\0"
    var source_c = CStringSlice(source_text)
    var buffer = alloc[UInt8](4096)
    var resolved = external_call["realpath", UnsafePointer[UInt8, MutUntrackedOrigin]](source_c.unsafe_ptr(), buffer)
    if Int(resolved) == 0:
        buffer.free()
        raise SQLiteError(code=2, message="bridge_transport: unable to resolve " + label + " path")
    var resolved_text = String(unsafe_from_utf8_ptr=resolved)
    buffer.free()
    return Path(resolved_text)


def _canonical_local_path(path: String, label: String) raises SQLiteError -> Path:
    _validate_local_path(path, label)
    try:
        var expanded = Path(path).expanduser()
        var absolute = expanded
        if not path.startswith("/"):
            absolute = cwd() / expanded
        if absolute.exists():
            return _realpath(absolute, label)
        return absolute
    except err:
        raise SQLiteError(code=2, message="bridge_transport: unable to resolve " + label + " path")


def _reject_same_path(source_path: String, target_path: String) raises SQLiteError:
    var source_identity = _canonical_local_path(source_path, "source")
    var target_identity = _canonical_local_path(target_path, "target")
    if source_identity.__fspath__() == target_identity.__fspath__():
        raise SQLiteError(code=2, message="bridge_transport: source and target paths must differ")


def _validate_local_path(path: String, label: String) raises SQLiteError:
    if path == "":
        raise SQLiteError(code=2, message="bridge_transport: " + label + " path must not be empty")
    if path.find("\0") >= 0 or path.find("\n") >= 0 or path.find("\r") >= 0:
        raise SQLiteError(code=2, message="bridge_transport: " + label + " path contains NUL or newline")
    if path == ":memory:" or path.startswith("file:") or path.find("://") >= 0:
        raise SQLiteError(code=2, message="bridge_transport: " + label + " path must be a local SQLite file")


def _exact_bridge_command(mut store: NativeDomainStore, run_id: String, key: String, command_type: String, payload: String) raises SQLiteError -> Bool:
    if key == "": return False
    var stmt = store.db.query("SELECT command_type,payload FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
    stmt.bind_text(1, run_id)
    stmt.bind_text(2, key)
    if not stmt.step():
        stmt.close()
        return False
    var stored_type = stmt.column_text(0)
    var stored_payload = stmt.column_text(1)
    stmt.close()
    if stored_type != command_type: return False
    var expected_canonical: String
    try:
        expected_canonical = canonical_json_text(payload)
    except err:
        raise SQLiteError(code=1, message="bridge replay payload invalid")
    var stored_canonical: String
    try:
        stored_canonical = canonical_json_text(stored_payload)
    except err:
        raise SQLiteError(code=1, message="bridge replay payload invalid")
    return stored_canonical == expected_canonical


def _bridge_operation_payload(delivery_id: String, status: String, attempts: Int) -> String:
    return "{\"delivery_id\":" + _json_quote(delivery_id) + ",\"status\":" + _json_quote(status) + ",\"attempts\":" + String(attempts) + "}"



@fieldwise_init
struct BridgeTransportResult(Copyable, Movable):
    """Result of one local bridge attempt and its independently replayable steps."""

    var source_delivery: BridgeDelivery
    var imported_delivery: BridgeDelivery
    var source_replayed: Bool
    var imported_replayed: Bool



def _close_source(mut store: NativeDomainStore):
    try:
        store.close()
    except err:
        pass


def deliver_local_bridge(
    source_path: String,
    target_path: String,
    source_run_id: String,
    delivery_id: String,
    updated_at: String,
    delivery_key: String = "",
    import_key: String = "",
) raises SQLiteError -> BridgeTransportResult:
    """Import one source outbox row into a local target SQLite database.

    ``source_path`` and ``target_path`` are explicit local paths.  The target
    schema is initialized before reading or importing the source row.  The
    target import is committed before the source delivery transition, so a
    failure can be retried without losing the imported row.  ``delivery_key``
    defaults to ``bridge.deliver:<delivery_id>`` and ``import_key`` defaults to
    ``bridge.import:<delivery_id>``; callers may supply stable keys explicitly.
    ``updated_at`` is required because this orchestration does not invent a
    clock or timestamp.
    """
    _reject_same_path(source_path, target_path)
    if source_run_id == "":
        raise SQLiteError(code=2, message="bridge_transport: source run id must not be empty")
    if delivery_id == "":
        raise SQLiteError(code=2, message="bridge_transport: delivery id must not be empty")
    if updated_at == "":
        raise SQLiteError(code=2, message="bridge_transport: updated_at must not be empty")

    var stable_delivery_key = delivery_key
    if stable_delivery_key == "":
        stable_delivery_key = "bridge.deliver:" + delivery_id
    var stable_import_key = import_key
    if stable_import_key == "":
        stable_import_key = "bridge.import:" + delivery_id

    var source = NativeDomainStore.open(source_path)
    try:
        source.initialize()
        var target = NativeDomainStore.open(target_path)
        try:
            # Initialize both connections before reading the source row.  The
            # target import still commits first; no two-db transaction exists.
            target.initialize()
            var current = source.get_outbox_delivery(source_run_id, delivery_id)
            var source_replay_payload = _bridge_operation_payload(delivery_id, "delivered", current.attempts if current.status == "delivered" else current.attempts + 1)
            var source_replayed = _exact_bridge_command(source, source_run_id, stable_delivery_key, "bridge.bridge_outbox.delivered", source_replay_payload)
            var target_run_id = current.target.run_id
            var import_attempts = current.attempts + 1
            try:
                var prior_import = target.get_inbox_delivery(target_run_id, delivery_id)
                import_attempts = prior_import.attempts
            except err:
                pass
            var import_replay_payload = _bridge_operation_payload(delivery_id, "imported", import_attempts)
            var imported_replayed = _exact_bridge_command(target, target_run_id, stable_import_key, "bridge.inbox.import", import_replay_payload)
            var imported = target.import_bridge_delivery(current, stable_import_key)
            # Target import precedes source delivery: a source failure leaves a
            # durable inbox row that the next attempt can safely replay.
            var delivered = source.deliver_bridge_delivery(
                "bridge_outbox",
                source_run_id,
                delivery_id,
                updated_at,
                stable_delivery_key,
            )
            target.close()
            source.close()
            return BridgeTransportResult(
                source_delivery=delivered^,
                imported_delivery=imported^,
                source_replayed=source_replayed,
                imported_replayed=imported_replayed,
            )
        except err:
            _close_source(target)
            _close_source(source)
            raise err^
    except err:
        _close_source(source)
        raise err^


# Descriptive alias for callers that use the domain-store operation name.
def deliver_local_bridge_delivery(
    source_path: String,
    target_path: String,
    source_run_id: String,
    delivery_id: String,
    updated_at: String,
    delivery_key: String = "",
    import_key: String = "",
) raises SQLiteError -> BridgeTransportResult:
    return deliver_local_bridge(
        source_path,
        target_path,
        source_run_id,
        delivery_id,
        updated_at,
        delivery_key,
        import_key,
    )
