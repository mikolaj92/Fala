"""Optional multi-Fala bridge outbox/inbox (not Essential Fala).

Real implementations. Uses NativeDomainStore private helpers:
_require_run, _text, _domain_command_start, _append_domain_event_in_tx, put_impulse.
"""

from std.collections import List
from emberjson import Value, to_string
from fala.sqlite import Statement, SQLiteError
from fala.domain import (
    BridgeDelivery, Impulse, RuntimeBudget, RuntimeRef, RunRef, EventRef,
    bridge_status_transition_allowed,
)
from fala.journal import CommandRow, EventInput, EventRow, CommandSubmission
from fala.domain_store import NativeDomainStore
from fala.json import canonical_json_text

def _quote(value: String) -> String:
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

def _bind_nullable(mut stmt: Statement, index: Int, value: String) raises SQLiteError:
    if value == "":
        stmt.bind_null(index)
    else:
        stmt.bind_text(index, value)

@fieldwise_init
struct BridgeEnqueueResult(Copyable, Movable):
    var delivery: BridgeDelivery
    var submission: CommandSubmission

def put_bridge_delivery(mut store: NativeDomainStore, row: BridgeDelivery) raises SQLiteError -> BridgeDelivery:
    if not row.is_valid():
        raise SQLiteError(code=1, message="domain store: invalid bridge delivery")
    _validate_bridge_refs(row)
    store._require_run(row.run_id)
    return _upsert_bridge_row(store, "bridge_outbox", row)
def _bridge_enqueue_payload(row: BridgeDelivery, include_status: Bool = False) -> String:
    var payload = "{\"delivery_id\":" + _quote(row.id) + ",\"impulse_id\":" + _quote(row.impulse.id) + ",\"source\":" + row.source.to_json() + ",\"target\":" + row.target.to_json() + ",\"pool_id\":"
    if row.pool_id == "": payload += "null"
    else: payload += _quote(row.pool_id)
    payload += ",\"budget\":" + row.budget.to_json()
    if not (row.event_ref.runtime.id == "runtime" and row.event_ref.run_id == "run" and row.event_ref.event_id == "" and row.event_ref.sequence == 0):
        payload += ",\"event_ref\":" + row.event_ref.to_json()
    if include_status:
        payload += ",\"status\":" + _quote(row.status) + ",\"attempts\":" + String(row.attempts)
    payload += "}"
    return payload
def enqueue_bridge_delivery(
    mut store: NativeDomainStore, row: BridgeDelivery, idempotency_key: String = "",
    actor: String = "", correlation_id: String = "", causation_id: String = "",
) raises SQLiteError -> BridgeEnqueueResult:
    """Atomically enqueue outbox, impulse, command, and event rows."""
    var delivery = row.copy()
    var delivery_key = idempotency_key if idempotency_key != "" else delivery.idempotency_key
    if delivery_key == "": raise SQLiteError(code=1, message="domain store: bridge enqueue idempotency key must not be empty")
    delivery.idempotency_key = delivery_key
    delivery.run_id = delivery.source.run_id
    if not delivery.is_valid() or delivery.status != "pending":
        raise SQLiteError(code=1, message="domain store: bridge enqueue requires a valid pending delivery")
    _validate_bridge_refs(delivery, "bridge_outbox")
    store._require_run(delivery.run_id)
    if delivery.updated_at == "": delivery.updated_at = delivery.created_at
    if delivery.updated_at == "": raise SQLiteError(code=1, message="domain store: bridge enqueue updated_at must not be empty")
    _validate_bridge_budget(delivery, next_attempt=False)
    var command_payload = String("")
    var event_payload = String("")
    try:
        command_payload = canonical_json_text(_bridge_enqueue_payload(delivery))
        event_payload = canonical_json_text(_bridge_enqueue_payload(delivery, True))
    except err:
        raise SQLiteError(code=1, message="domain store: bridge enqueue payload is invalid JSON")
    store.db.begin_immediate()
    try:
        var existing = store.db.query("SELECT run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
        existing.bind_text(1, delivery.run_id); existing.bind_text(2, delivery_key)
        if existing.step():
            var stored = CommandRow(run_id=store._text(existing,0), id=store._text(existing,1), command_type=store._text(existing,2), idempotency_key=store._text(existing,3), actor=store._text(existing,4), correlation_id=store._text(existing,5), causation_id=store._text(existing,6), payload=store._text(existing,7), created_at=store._text(existing,8))
            existing.close()
            if stored.command_type != "bridge.outbox.enqueue" or stored.actor != actor or stored.correlation_id != correlation_id or stored.causation_id != causation_id or stored.created_at != delivery.updated_at or not _bridge_json_equal(stored.payload, command_payload):
                raise SQLiteError(code=1, message="domain store: bridge idempotency conflict")
            var lookup = store.db.query("SELECT json_extract(?, '$.delivery_id')")
            lookup.bind_text(1, stored.payload)
            if not lookup.step() or lookup.column_text(0) == "":
                lookup.close(); raise SQLiteError(code=1, message="domain store: bridge enqueue command identity is invalid")
            var stored_id = lookup.column_text(0); lookup.close()
            var replayed = _get_bridge(store, "bridge_outbox", delivery.run_id, stored_id)
            store.db.commit()
            return BridgeEnqueueResult(delivery=replayed^, submission=CommandSubmission(command=stored^, events=List[EventRow](), replayed=True))
        existing.close()
        var target_seen_stmt = store.db.query("SELECT 1 FROM bridge_outbox WHERE run_id=? AND json_extract(target_ref,'$.run_id')=? LIMIT 1")
        target_seen_stmt.bind_text(1, delivery.run_id); target_seen_stmt.bind_text(2, delivery.target.run_id)
        var target_seen = target_seen_stmt.step(); target_seen_stmt.close()
        if delivery.budget.spawned_runs_limited and not target_seen:
            var spawned = store.db.query("SELECT COUNT(DISTINCT json_extract(target_ref,'$.run_id')) FROM bridge_outbox WHERE run_id=? AND json_extract(target_ref,'$.run_id') IS NOT NULL")
            spawned.bind_text(1, delivery.run_id)
            if spawned.step() and spawned.column_int(0) >= delivery.budget.spawned_runs:
                spawned.close(); raise SQLiteError(code=1, message="domain store: bridge spawned_runs budget exhausted")
            spawned.close()
        var impulse_check = store.db.query("SELECT run_id,impulse_type,payload,metadata,created_at,updated_at FROM impulses WHERE run_id=? AND id=?")
        impulse_check.bind_text(1, delivery.impulse.run_id); impulse_check.bind_text(2, delivery.impulse.id)
        var impulse_exists = impulse_check.step()
        if impulse_exists and (store._text(impulse_check,0) != delivery.impulse.run_id or store._text(impulse_check,1) != delivery.impulse.impulse_type or store._text(impulse_check,2) != delivery.impulse.payload or store._text(impulse_check,3) != delivery.impulse.metadata or store._text(impulse_check,4) != delivery.impulse.created_at or store._text(impulse_check,5) != delivery.impulse.updated_at):
            impulse_check.close(); raise SQLiteError(code=1, message="domain store: bridge impulse idempotency conflict")
        impulse_check.close()
        var source_json = delivery.source.to_json(); var target_json = delivery.target.to_json(); var impulse_json = delivery.impulse.to_json(); var event_ref_json = ""
        if not (delivery.event_ref.runtime.id == "runtime" and delivery.event_ref.run_id == "run" and delivery.event_ref.event_id == "" and delivery.event_ref.sequence == 0): event_ref_json = delivery.event_ref.to_json()
        var outbox = store.db.query("INSERT INTO bridge_outbox (run_id,id,idempotency_key,source_ref,target_ref,impulse_json,event_ref,pool_id,budget,status,attempts,metadata,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        outbox.bind_text(1,delivery.run_id); outbox.bind_text(2,delivery.id); outbox.bind_text(3,delivery_key); outbox.bind_text(4,source_json); outbox.bind_text(5,target_json); outbox.bind_text(6,impulse_json)
        if event_ref_json == "": outbox.bind_null(7)
        else: outbox.bind_text(7,event_ref_json)
        _bind_nullable(outbox,8,delivery.pool_id); outbox.bind_text(9,delivery.budget.to_json()); outbox.bind_text(10,"pending"); outbox.bind_int(11,delivery.attempts); outbox.bind_text(12,delivery.metadata); outbox.bind_text(13,delivery.created_at); outbox.bind_text(14,delivery.updated_at); _ = outbox.step(); outbox.close()
        if not impulse_exists:
            var impulse_insert = store.db.query("INSERT INTO impulses (run_id,id,impulse_type,payload,metadata,created_at,updated_at) VALUES (?,?,?,?,?,?,?)")
            impulse_insert.bind_text(1,delivery.impulse.run_id); impulse_insert.bind_text(2,delivery.impulse.id); impulse_insert.bind_text(3,delivery.impulse.impulse_type); impulse_insert.bind_text(4,delivery.impulse.payload); impulse_insert.bind_text(5,delivery.impulse.metadata); impulse_insert.bind_text(6,delivery.impulse.created_at); impulse_insert.bind_text(7,delivery.impulse.updated_at); _ = impulse_insert.step(); impulse_insert.close()
        var command = store.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at) VALUES (?,?,?,?,?,?,?,?,?)")
        command.bind_text(1,delivery.run_id); command.bind_text(2,delivery_key); command.bind_text(3,"bridge.outbox.enqueue"); command.bind_text(4,delivery_key); _bind_nullable(command,5,actor); _bind_nullable(command,6,correlation_id); _bind_nullable(command,7,causation_id); command.bind_text(8,command_payload); command.bind_text(9,delivery.updated_at); _ = command.step(); command.close()
        var command_row = CommandRow(run_id=delivery.run_id,id=delivery_key,command_type="bridge.outbox.enqueue",idempotency_key=delivery_key,actor=actor,correlation_id=correlation_id,causation_id=causation_id,payload=command_payload,created_at=delivery.updated_at)
        var event_input = EventInput(delivery_key + ":event", "bridge.outbox.enqueued", event_payload, delivery.updated_at, delivery.impulse.id, "", 1, actor, correlation_id, causation_id)
        var stored_event = store._append_domain_event_in_tx(command_row, event_input)
        var stored_events = List[EventRow](); stored_events.append(stored_event^)
        store.db.commit()
        var saved = _get_bridge(store, "bridge_outbox", delivery.run_id, delivery.id)
        return BridgeEnqueueResult(delivery=saved^, submission=CommandSubmission(command=command_row^, events=stored_events^, replayed=False))
    except err:
        store.db.rollback(); raise SQLiteError(code=1, message="domain store: bridge enqueue failed: " + String(err))
def list_bridge_deliveries(mut store: NativeDomainStore, run_id: String) raises SQLiteError -> List[String]:
    store._require_run(run_id)
    var result = List[String]()
    var stmt = store.db.query("SELECT id,idempotency_key,source_ref,target_ref,impulse_json,event_ref,pool_id,budget,status,attempts,metadata,created_at,updated_at FROM bridge_outbox WHERE run_id=? ORDER BY updated_at ASC, id ASC")
    stmt.bind_text(1, run_id)
    while stmt.step():
        var event_ref = store._text(stmt, 5)
        if event_ref == "": event_ref = "null"
        var pool_id = store._text(stmt, 6)
        var pool_json = "null"
        if pool_id != "": pool_json = _quote(pool_id)
        var item = "{\"id\":" + _quote(store._text(stmt, 0)) + ",\"run_id\":" + _quote(run_id) + ",\"idempotency_key\":" + _quote(store._text(stmt, 1)) + ",\"source\":" + store._text(stmt, 2) + ",\"target\":" + store._text(stmt, 3) + ",\"impulse\":" + store._text(stmt, 4) + ",\"event_ref\":" + event_ref + ",\"pool_id\":" + pool_json + ",\"budget\":" + store._text(stmt, 7) + ",\"status\":" + _quote(store._text(stmt, 8)) + ",\"attempts\":" + String(stmt.column_int(9)) + ",\"metadata\":" + store._text(stmt, 10) + ",\"created_at\":" + _quote(store._text(stmt, 11)) + ",\"updated_at\":" + _quote(store._text(stmt, 12)) + "}"
        result.append(item^)
    return result^

def _validate_bridge_refs(row: BridgeDelivery, table: String = "bridge_outbox") raises SQLiteError:
    # Outbox rows are authored by their source run. Inbox rows are stored
    # under the target run, while source references remain remote identity.
    if table != "bridge_inbox" and table != "bridge_outbox":
        raise SQLiteError(code=1, message="domain store: invalid bridge table")
    if table == "bridge_inbox":
        if row.target.run_id != row.run_id:
            raise SQLiteError(code=1, message="domain store: bridge target run mismatch")
        return
    if row.source.run_id != row.run_id:
        raise SQLiteError(code=1, message="domain store: bridge source run mismatch")
    if row.impulse.run_id != row.run_id:
        raise SQLiteError(code=1, message="domain store: bridge impulse run mismatch")
    if not (row.event_ref.runtime.id == "runtime" and row.event_ref.run_id == "run" and row.event_ref.event_id == "" and row.event_ref.sequence == 0):
        if row.event_ref.run_id != row.run_id:
            raise SQLiteError(code=1, message="domain store: bridge event run mismatch")
def put_inbox_delivery(mut store: NativeDomainStore, row: BridgeDelivery) raises SQLiteError -> BridgeDelivery:
    if not row.is_valid():
        raise SQLiteError(code=1, message="domain store: invalid bridge delivery")
    _validate_bridge_refs(row, "bridge_inbox")
    store._require_run(row.run_id)
    return _upsert_bridge_row(store, "bridge_inbox", row)
def _validate_bridge_budget(row: BridgeDelivery, next_attempt: Bool = True) raises SQLiteError:
    if row.budget.runtime_hops_limited and row.budget.runtime_hops < 1:
        raise SQLiteError(code=1, message="domain store: bridge runtime hop budget exhausted")
    if row.budget.impulse_count_limited and row.budget.impulse_count < 1:
        raise SQLiteError(code=1, message="domain store: bridge impulse budget exhausted")
    if next_attempt and row.budget.attempts_limited and row.attempts + 1 > row.budget.attempts:
        raise SQLiteError(code=1, message="domain store: bridge attempts budget exhausted")

def _normalize_imported_impulse(row: BridgeDelivery) raises SQLiteError -> Impulse:
    var metadata_value: Value
    try:
        metadata_value = Value(parse_string=row.impulse.metadata)
    except err:
        raise SQLiteError(code=1, message="domain store: imported impulse metadata must be a JSON object")
    if not metadata_value.is_object():
        raise SQLiteError(code=1, message="domain store: imported impulse metadata must be a JSON object")
    var metadata = metadata_value.object().copy()
    metadata["source_runtime_id"] = Value(row.source.runtime.id)
    metadata["source_run_id"] = Value(row.source.run_id)
    metadata["source_impulse_id"] = Value(row.impulse.id)
    var metadata_json: String
    try:
        metadata_json = canonical_json_text(to_string(Value(metadata^)))
    except err:
        raise SQLiteError(code=1, message="domain store: imported impulse metadata serialization failed")
    return Impulse(row.impulse.id, row.run_id, row.impulse.impulse_type, row.impulse.payload, metadata_json, row.impulse.created_at, row.impulse.updated_at)^
def import_bridge_delivery(mut store: NativeDomainStore, row: BridgeDelivery, idempotency_key: String = "") raises SQLiteError -> BridgeDelivery:
    """Normalize a remote delivery into a local inbox transaction."""
    var imported = row.copy()
    var delivery_key = idempotency_key if idempotency_key != "" else imported.idempotency_key
    if delivery_key == "": raise SQLiteError(code=1, message="domain store: bridge import idempotency key must not be empty")
    imported.idempotency_key = delivery_key
    imported.run_id = imported.target.run_id
    imported.status = "imported"
    if imported.updated_at == "": imported.updated_at = imported.created_at
    if imported.updated_at == "": raise SQLiteError(code=1, message="domain store: bridge import updated_at must not be empty")
    return import_inbox_delivery(store, imported)
def import_inbox_delivery(mut store: NativeDomainStore, row: BridgeDelivery) raises SQLiteError -> BridgeDelivery:
    """Atomically import an inbox row, consuming bridge delivery budget."""
    if not row.is_valid() or row.status != "imported":
        raise SQLiteError(code=1, message="domain store: inbox import requires valid imported delivery")
    _validate_bridge_refs(row, "bridge_inbox")
    store._require_run(row.run_id)
    var imported_impulse = _normalize_imported_impulse(row)
    store.db.begin_immediate()
    try:
        var collision = store.db.query("SELECT impulse_type,payload,metadata,created_at,updated_at FROM impulses WHERE run_id=? AND id=?")
        collision.bind_text(1, imported_impulse.run_id); collision.bind_text(2, imported_impulse.id)
        if collision.step():
            if store._text(collision, 0) != imported_impulse.impulse_type or store._text(collision, 1) != imported_impulse.payload or store._text(collision, 2) != imported_impulse.metadata or store._text(collision, 3) != imported_impulse.created_at or store._text(collision, 4) != imported_impulse.updated_at:
                collision.close()
                raise SQLiteError(code=1, message="domain store: bridge impulse idempotency conflict")
        collision.close()
        var existing = store.db.query("SELECT id,idempotency_key,source_ref,target_ref,impulse_json,pool_id,status FROM bridge_inbox WHERE run_id=? AND (id=? OR idempotency_key=?) ORDER BY CASE WHEN id=? THEN 0 ELSE 1 END LIMIT 1")
        existing.bind_text(1, row.run_id); existing.bind_text(2, row.id); existing.bind_text(3, row.idempotency_key); existing.bind_text(4, row.id)
        var has_existing = existing.step()
        var prior_id = String("")
        var prior_key = String("")
        var prior_source = String("")
        var prior_target = String("")
        var prior_impulse = String("")
        var prior_pool = String("")
        var prior_status = String("")
        if has_existing:
            prior_id = store._text(existing, 0); prior_key = store._text(existing, 1); prior_source = store._text(existing, 2); prior_target = store._text(existing, 3); prior_impulse = store._text(existing, 4); prior_pool = store._text(existing, 5); prior_status = store._text(existing, 6)
        existing.close()
        if has_existing and (prior_id != row.id or prior_key != row.idempotency_key or not _bridge_json_equal(prior_source, row.source.to_json()) or not _bridge_json_equal(prior_target, row.target.to_json()) or prior_pool != row.pool_id):
            raise SQLiteError(code=1, message="domain store: bridge idempotency conflict")
        if has_existing and prior_status == "imported":
            if not _bridge_json_equal(prior_impulse, imported_impulse.to_json()):
                raise SQLiteError(code=1, message="domain store: bridge idempotency conflict")
            store.db.commit()
            return _get_bridge(store, "bridge_inbox", row.run_id, prior_id)
        if has_existing and not bridge_status_transition_allowed(prior_status, row.status):
            raise SQLiteError(code=1, message="domain store: invalid bridge status transition")
        _validate_bridge_budget(row, next_attempt=True)
        var next_budget = RuntimeBudget()
        try:
            next_budget = row.budget.consume(runtime_hops=1, impulse_count=1)
        except err:
            raise SQLiteError(code=1, message="domain store: bridge budget exhausted")
        var next_attempts = row.attempts + 1
        var source_json = row.source.to_json()
        var target_json = row.target.to_json()
        var impulse_json = imported_impulse.to_json()
        var event_json = ""
        if not (row.event_ref.runtime.id == "runtime" and row.event_ref.run_id == "run" and row.event_ref.event_id == "" and row.event_ref.sequence == 0):
            event_json = row.event_ref.to_json()
        var budget_json = next_budget.to_json()
        if has_existing:
            var update = store.db.query("UPDATE bridge_inbox SET impulse_json=?,event_ref=?,budget=?,status='imported',attempts=?,metadata=?,updated_at=? WHERE run_id=? AND id=?")
            update.bind_text(1, impulse_json)
            if event_json == "": update.bind_null(2)
            else: update.bind_text(2, event_json)
            update.bind_text(3, budget_json); update.bind_int(4, next_attempts); update.bind_text(5, row.metadata); update.bind_text(6, row.updated_at); update.bind_text(7, row.run_id); update.bind_text(8, prior_id); _ = update.step(); update.close()
            if store.db.changes() != 1: raise SQLiteError(code=1, message="domain store: inbox import lost ownership")
        else:
            var insert = store.db.query("INSERT INTO bridge_inbox (run_id,id,idempotency_key,source_ref,target_ref,impulse_json,event_ref,pool_id,budget,status,attempts,metadata,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
            insert.bind_text(1, row.run_id); insert.bind_text(2, row.id); insert.bind_text(3, row.idempotency_key); insert.bind_text(4, source_json); insert.bind_text(5, target_json); insert.bind_text(6, impulse_json)
            if event_json == "": insert.bind_null(7)
            else: insert.bind_text(7, event_json)
            _bind_nullable(insert, 8, row.pool_id); insert.bind_text(9, budget_json); insert.bind_text(10, row.status); insert.bind_int(11, next_attempts); insert.bind_text(12, row.metadata); insert.bind_text(13, row.created_at); insert.bind_text(14, row.updated_at); _ = insert.step(); insert.close()
        var impulse = store.db.query("INSERT INTO impulses (run_id,id,impulse_type,payload,metadata,created_at,updated_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(run_id,id) DO NOTHING")
        impulse.bind_text(1, imported_impulse.run_id); impulse.bind_text(2, imported_impulse.id); impulse.bind_text(3, imported_impulse.impulse_type); impulse.bind_text(4, imported_impulse.payload); impulse.bind_text(5, imported_impulse.metadata); impulse.bind_text(6, imported_impulse.created_at); impulse.bind_text(7, imported_impulse.updated_at); _ = impulse.step(); impulse.close()
        _record_bridge_journal(store, row.run_id, row.idempotency_key, "bridge.inbox.import", _bridge_operation_payload(row.id, "imported", next_attempts), "bridge.inbox.imported", row.updated_at, imported_impulse.id)
        store.db.commit()
    except err:
        store.db.rollback()
        var detail = String(err)
        if detail.find("idempotency conflict") >= 0 or detail.find("status transition") >= 0 or detail.find("budget") >= 0:
            raise err^
        raise SQLiteError(code=1, message="domain store: inbox import failed")
    return _get_bridge(store, "bridge_inbox", row.run_id, row.id)

def _bridge_json_equal(left: String, right: String) raises SQLiteError -> Bool:
    var lhs = left
    var rhs = right
    if lhs == "": lhs = "null"
    if rhs == "": rhs = "null"
    try:
        return canonical_json_text(lhs) == canonical_json_text(rhs)
    except err:
        raise SQLiteError(code=1, message="domain store: invalid bridge JSON")
def _record_bridge_journal(mut store: NativeDomainStore, run_id: String, command_id: String, command_type: String, payload: String, event_type: String, created_at: String, impulse_id: String = "") raises SQLiteError:
    """Append a bridge command/event pair in the caller's transaction.

    Bridge operations use the transport idempotency key as the durable
    command identity. Replays compare the immutable operation payload and
    repair a missing event without creating a second sequence number.
    """
    if command_id == "" or command_type == "" or payload == "" or event_type == "" or created_at == "":
        raise SQLiteError(code=1, message="domain store: bridge journal fields must not be empty")
    var command = store.db.query("SELECT id,command_type,idempotency_key,payload FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
    command.bind_text(1, run_id); command.bind_text(2, command_id)
    var stored_id = command_id
    var stored_type = command_type
    var stored_payload = payload
    var command_exists = command.step()
    if command_exists:
        stored_id = store._text(command, 0)
        stored_type = store._text(command, 1)
        stored_payload = store._text(command, 3)
        command.close()
        if stored_type != command_type or not _bridge_json_equal(stored_payload, payload):
            raise SQLiteError(code=1, message="domain store: bridge idempotency conflict")
    else:
        command.close()
        var insert = store.db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,payload,created_at) VALUES (?,?,?,?,?,?)")
        insert.bind_text(1, run_id); insert.bind_text(2, command_id); insert.bind_text(3, command_type); insert.bind_text(4, command_id); insert.bind_text(5, payload); insert.bind_text(6, created_at)
        _ = insert.step(); insert.close()
    var event_id = stored_id + ":event"
    var prior_event = store.db.query("SELECT event_type,impulse_id,command_id,payload FROM runtime_events WHERE run_id=? AND id=?")
    prior_event.bind_text(1, run_id); prior_event.bind_text(2, event_id)
    if prior_event.step():
        var prior_type = store._text(prior_event, 0)
        var prior_impulse = store._text(prior_event, 1)
        var prior_command = store._text(prior_event, 2)
        var prior_payload = store._text(prior_event, 3)
        prior_event.close()
        if prior_type != event_type or prior_impulse != impulse_id or prior_command != stored_id or not _bridge_json_equal(prior_payload, payload):
            raise SQLiteError(code=1, message="domain store: bridge event idempotency conflict")
        return
    prior_event.close()
    var next = store.db.query("SELECT COALESCE(MAX(sequence),0)+1 FROM runtime_events WHERE run_id=?")
    next.bind_text(1, run_id)
    if not next.step():
        next.close()
        raise SQLiteError(code=1, message="domain store: bridge event sequence unavailable")
    var sequence = next.column_int(0)
    var event = store.db.query("INSERT INTO runtime_events (run_id,sequence,id,event_type,schema_version,impulse_id,command_id,payload,created_at) VALUES (?,?,?, ?,1,?,?,?,?)")
    event.bind_text(1, run_id); event.bind_int(2, sequence); event.bind_text(3, event_id); event.bind_text(4, event_type)
    if impulse_id == "": event.bind_null(5)
    else: event.bind_text(5, impulse_id)
    event.bind_text(6, stored_id); event.bind_text(7, payload); event.bind_text(8, created_at)
    _ = event.step(); event.close()

def _bridge_operation_payload(delivery_id: String, status: String, attempts: Int = -1) -> String:
    var payload = "{\"delivery_id\":" + _quote(delivery_id) + ",\"status\":" + _quote(status)
    if attempts >= 0: payload += ",\"attempts\":" + String(attempts)
    payload += "}"
    return payload
def _upsert_bridge_row(mut store: NativeDomainStore, table: String, row: BridgeDelivery) raises SQLiteError -> BridgeDelivery:
    _validate_bridge_refs(row, table)
    if row.attempts < 0:
        raise SQLiteError(code=1, message="domain store: invalid bridge attempts")
    if table != "bridge_inbox" and table != "bridge_outbox":
        raise SQLiteError(code=1, message="domain store: invalid bridge table")
    var source_json = row.source.to_json()
    var target_json = row.target.to_json()
    var impulse_json = row.impulse.to_json()
    var event_json = ""
    if not (row.event_ref.runtime.id == "runtime" and row.event_ref.run_id == "run" and row.event_ref.event_id == "" and row.event_ref.sequence == 0):
        event_json = row.event_ref.to_json()
    var budget_json = row.budget.to_json()
    store.db.begin()
    var failure = ""
    try:
        # Check both unique identities before any mutation.  A duplicate key
        # is a replay only when every immutable payload field is identical.
        var existing = store.db.query("SELECT id,idempotency_key,source_ref,target_ref,impulse_json,event_ref,pool_id,budget,status,attempts,metadata,created_at,updated_at FROM " + table + " WHERE run_id=? AND (id=? OR idempotency_key=?) ORDER BY CASE WHEN id=? THEN 0 ELSE 1 END LIMIT 1")
        existing.bind_text(1, row.run_id); existing.bind_text(2, row.id); existing.bind_text(3, row.idempotency_key); existing.bind_text(4, row.id)
        if existing.step():
            var prior_id = store._text(existing, 0)
            var prior_key = store._text(existing, 1)
            var prior_status = store._text(existing, 8)
            var prior_attempts = existing.column_int(9)
            var prior_source = store._text(existing, 2)
            var prior_target = store._text(existing, 3)
            var prior_impulse = store._text(existing, 4)
            var prior_event = store._text(existing, 5)
            var prior_pool = store._text(existing, 6)
            var prior_budget = store._text(existing, 7)
            var prior_metadata = store._text(existing, 10)
            var same_payload = prior_id == row.id and prior_key == row.idempotency_key and _bridge_json_equal(prior_source, source_json) and _bridge_json_equal(prior_target, target_json) and _bridge_json_equal(prior_impulse, impulse_json) and _bridge_json_equal(prior_event, event_json) and prior_pool == row.pool_id and _bridge_json_equal(prior_budget, budget_json) and _bridge_json_equal(prior_metadata, row.metadata)
            existing.close()
            if not same_payload:
                store.db.rollback(); raise SQLiteError(code=1, message="domain store: bridge idempotency conflict")
            if not bridge_status_transition_allowed(prior_status, row.status):
                store.db.rollback(); raise SQLiteError(code=1, message="domain store: invalid bridge status transition")
            var next_attempts = prior_attempts
            if row.attempts > next_attempts: next_attempts = row.attempts
            if prior_status != row.status or next_attempts > prior_attempts:
                var update = store.db.query("UPDATE " + table + " SET status=?,attempts=?,updated_at=? WHERE run_id=? AND id=?")
                update.bind_text(1, row.status); update.bind_int(2, next_attempts); update.bind_text(3, row.updated_at); update.bind_text(4, row.run_id); update.bind_text(5, prior_id); _ = update.step(); update.close()
            store.db.commit()
            return _get_bridge(store, table, row.run_id, prior_id)
        existing.close()
        var stmt = store.db.query("INSERT INTO " + table + " (run_id,id,idempotency_key,source_ref,target_ref,impulse_json,event_ref,pool_id,budget,status,attempts,metadata,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
        stmt.bind_text(1, row.run_id); stmt.bind_text(2, row.id); stmt.bind_text(3, row.idempotency_key); stmt.bind_text(4, source_json); stmt.bind_text(5, target_json); stmt.bind_text(6, impulse_json)
        if event_json == "": stmt.bind_null(7)
        else: stmt.bind_text(7, event_json)
        _bind_nullable(stmt, 8, row.pool_id); stmt.bind_text(9, budget_json); stmt.bind_text(10, row.status); stmt.bind_int(11, row.attempts); stmt.bind_text(12, row.metadata); stmt.bind_text(13, row.created_at); stmt.bind_text(14, row.updated_at)
        _ = stmt.step(); stmt.close(); store.db.commit()
    except err:
        store.db.rollback()
        raise SQLiteError(code=1, message="domain store: bridge delivery write failed")
    if failure == "conflict":
        raise SQLiteError(code=1, message="domain store: bridge idempotency conflict")
    if failure == "transition":
        raise SQLiteError(code=1, message="domain store: invalid bridge status transition")
    return _get_bridge(store, table, row.run_id, row.id)

def _read_bridge(mut stmt: Statement) raises SQLiteError -> BridgeDelivery:
    if not stmt.step():
        stmt.close()
        raise SQLiteError(code=1, message="domain store: bridge delivery not found")
    var run_id = NativeDomainStore._text(stmt, 0)
    var source = RunRef(RuntimeRef(NativeDomainStore._text(stmt, 14), NativeDomainStore._text(stmt, 15), NativeDomainStore._text(stmt, 36)), NativeDomainStore._text(stmt, 18))
    var target = RunRef(RuntimeRef(NativeDomainStore._text(stmt, 16), NativeDomainStore._text(stmt, 17), NativeDomainStore._text(stmt, 37)), NativeDomainStore._text(stmt, 19))
    var impulse = Impulse(NativeDomainStore._text(stmt, 20), NativeDomainStore._text(stmt, 40), NativeDomainStore._text(stmt, 21), NativeDomainStore._text(stmt, 34), NativeDomainStore._text(stmt, 35), NativeDomainStore._text(stmt, 22), NativeDomainStore._text(stmt, 23))
    var event_ref = EventRef(RuntimeRef("runtime", "", "{}"), "run", "", 0)
    if not stmt.column_null(6):
        event_ref = EventRef(RuntimeRef(NativeDomainStore._text(stmt, 24), NativeDomainStore._text(stmt, 38), NativeDomainStore._text(stmt, 39)), NativeDomainStore._text(stmt, 25), NativeDomainStore._text(stmt, 26), stmt.column_int(27))
    var budget = RuntimeBudget(runtime_hops=stmt.column_int(28), spawned_runs=stmt.column_int(29), impulse_count=stmt.column_int(30), wall_time_seconds=stmt.column_int(31), attempts=stmt.column_int(32), reaction_bytes=stmt.column_int(33), runtime_hops_limited=not stmt.column_null(28), spawned_runs_limited=not stmt.column_null(29), impulse_count_limited=not stmt.column_null(30), wall_time_seconds_limited=not stmt.column_null(31), attempts_limited=not stmt.column_null(32), reaction_bytes_limited=not stmt.column_null(33))
    var row = BridgeDelivery(NativeDomainStore._text(stmt, 1), run_id, NativeDomainStore._text(stmt, 2), source^, target^, impulse^, event_ref^, NativeDomainStore._text(stmt, 7), budget^, NativeDomainStore._text(stmt, 9), stmt.column_int(10), NativeDomainStore._text(stmt, 11), NativeDomainStore._text(stmt, 12), NativeDomainStore._text(stmt, 13))
    stmt.close()
    return row^
def list_bridge_inbox(mut store: NativeDomainStore, run_id: String, status: String = "", limit: Int = -1) raises SQLiteError -> List[String]:
    store._require_run(run_id)
    if limit < -1: raise SQLiteError(code=1, message="domain store: limit must be non-negative")
    var sql = "SELECT id,idempotency_key,source_ref,target_ref,impulse_json,event_ref,pool_id,budget,status,attempts,metadata,created_at,updated_at FROM bridge_inbox WHERE run_id=?"
    if status != "": sql += " AND status=?"
    sql += " ORDER BY updated_at ASC, id ASC"
    if limit >= 0: sql += " LIMIT ?"
    var stmt = store.db.query(sql); stmt.bind_text(1, run_id)
    var index = 2
    if status != "": stmt.bind_text(index, status); index += 1
    if limit >= 0: stmt.bind_int(index, limit)
    var result = List[String]()
    while stmt.step():
        var event_ref = store._text(stmt, 5)
        if event_ref == "": event_ref = "null"
        var pool_id = store._text(stmt, 6)
        var pool_json = "null"
        if pool_id != "": pool_json = _quote(pool_id)
        var item = "{\"id\":" + _quote(store._text(stmt, 0)) + ",\"run_id\":" + _quote(run_id) + ",\"idempotency_key\":" + _quote(store._text(stmt, 1)) + ",\"source\":" + store._text(stmt, 2) + ",\"target\":" + store._text(stmt, 3) + ",\"impulse\":" + store._text(stmt, 4) + ",\"event_ref\":" + event_ref + ",\"pool_id\":" + pool_json + ",\"budget\":" + store._text(stmt, 7) + ",\"status\":" + _quote(store._text(stmt, 8)) + ",\"attempts\":" + String(stmt.column_int(9)) + ",\"metadata\":" + store._text(stmt, 10) + ",\"created_at\":" + _quote(store._text(stmt, 11)) + ",\"updated_at\":" + _quote(store._text(stmt, 12)) + "}"
        result.append(item^)
    stmt.close()
    return result^
def _bridge_select(table: String) -> String:
    return "SELECT run_id,id,idempotency_key,source_ref,target_ref,impulse_json,event_ref,pool_id,budget,status,attempts,metadata,created_at,updated_at,json_extract(source_ref,'$.runtime.id'),json_extract(source_ref,'$.runtime.uri'),json_extract(target_ref,'$.runtime.id'),json_extract(target_ref,'$.runtime.uri'),json_extract(source_ref,'$.run_id'),json_extract(target_ref,'$.run_id'),json_extract(impulse_json,'$.id'),json_extract(impulse_json,'$.impulse_type'),json_extract(impulse_json,'$.created_at'),json_extract(impulse_json,'$.updated_at'),json_extract(event_ref,'$.runtime.id'),json_extract(event_ref,'$.run_id'),json_extract(event_ref,'$.event_id'),json_extract(event_ref,'$.sequence'),json_extract(budget,'$.runtime_hops'),json_extract(budget,'$.spawned_runs'),json_extract(budget,'$.impulse_count'),json_extract(budget,'$.wall_time_seconds'),json_extract(budget,'$.attempts'),json_extract(budget,'$.reaction_bytes'),json_extract(impulse_json,'$.payload'),json_extract(impulse_json,'$.metadata'),json_extract(source_ref,'$.runtime.metadata'),json_extract(target_ref,'$.runtime.metadata'),json_extract(event_ref,'$.runtime.uri'),json_extract(event_ref,'$.runtime.metadata'),json_extract(impulse_json,'$.run_id') FROM " + table
def _get_bridge(mut store: NativeDomainStore, table: String, run_id: String, delivery_id: String) raises SQLiteError -> BridgeDelivery:
    if table != "bridge_inbox" and table != "bridge_outbox": raise SQLiteError(code=1, message="domain store: invalid bridge table")
    store._require_run(run_id)
    var stmt = store.db.query(_bridge_select(table) + " WHERE run_id=? AND id=?")
    stmt.bind_text(1, run_id); stmt.bind_text(2, delivery_id)
    return _read_bridge(stmt)
def get_outbox_delivery(mut store: NativeDomainStore, run_id: String, delivery_id: String) raises SQLiteError -> BridgeDelivery:
    return _get_bridge(store, "bridge_outbox", run_id, delivery_id)
def get_inbox_delivery(mut store: NativeDomainStore, run_id: String, delivery_id: String) raises SQLiteError -> BridgeDelivery:
    return _get_bridge(store, "bridge_inbox", run_id, delivery_id)
def list_bridge_records(mut store: NativeDomainStore, table: String, run_id: String, status: String = "", limit: Int = -1) raises SQLiteError -> List[BridgeDelivery]:
    if limit < -1: raise SQLiteError(code=1, message="domain store: limit must be non-negative")
    if table != "bridge_inbox" and table != "bridge_outbox": raise SQLiteError(code=1, message="domain store: invalid bridge table")
    store._require_run(run_id)
    var sql = _bridge_select(table) + " WHERE run_id=?"
    if status != "": sql += " AND status=?"
    sql += " ORDER BY updated_at ASC, id ASC"
    if limit >= 0: sql += " LIMIT ?"
    var stmt = store.db.query(sql); stmt.bind_text(1, run_id)
    var index = 2
    if status != "": stmt.bind_text(index, status); index += 1
    if limit >= 0: stmt.bind_int(index, limit)
    var result = List[BridgeDelivery]()
    while stmt.step(): result.append(_read_bridge_current(stmt)^)
    stmt.close()
    return result^


def _read_bridge_current(mut stmt: Statement) raises SQLiteError -> BridgeDelivery:
    # _read_bridge expects a positioned statement; duplicate the row through a
    # temporary query is avoided by this narrow JSON projection constructor.
    var run_id = NativeDomainStore._text(stmt, 0)
    var source = RunRef(RuntimeRef(NativeDomainStore._text(stmt, 14), NativeDomainStore._text(stmt, 15), NativeDomainStore._text(stmt, 36)), NativeDomainStore._text(stmt, 18))
    var target = RunRef(RuntimeRef(NativeDomainStore._text(stmt, 16), NativeDomainStore._text(stmt, 17), NativeDomainStore._text(stmt, 37)), NativeDomainStore._text(stmt, 19))
    var impulse_run_id = NativeDomainStore._text(stmt, 40)
    var impulse = Impulse(NativeDomainStore._text(stmt, 20), impulse_run_id, NativeDomainStore._text(stmt, 21), NativeDomainStore._text(stmt, 34), NativeDomainStore._text(stmt, 35), NativeDomainStore._text(stmt, 22), NativeDomainStore._text(stmt, 23))
    var event_ref = EventRef(RuntimeRef("runtime", "", "{}"), "run", "", 0)
    if not stmt.column_null(24):
        event_ref = EventRef(RuntimeRef(NativeDomainStore._text(stmt, 24), NativeDomainStore._text(stmt, 38), NativeDomainStore._text(stmt, 39)), NativeDomainStore._text(stmt, 25), NativeDomainStore._text(stmt, 26), stmt.column_int(27))
    var budget = RuntimeBudget(runtime_hops=stmt.column_int(28), spawned_runs=stmt.column_int(29), impulse_count=stmt.column_int(30), wall_time_seconds=stmt.column_int(31), attempts=stmt.column_int(32), reaction_bytes=stmt.column_int(33), runtime_hops_limited=not stmt.column_null(28), spawned_runs_limited=not stmt.column_null(29), impulse_count_limited=not stmt.column_null(30), wall_time_seconds_limited=not stmt.column_null(31), attempts_limited=not stmt.column_null(32), reaction_bytes_limited=not stmt.column_null(33))
    return BridgeDelivery(NativeDomainStore._text(stmt, 1), run_id, NativeDomainStore._text(stmt, 2), source^, target^, impulse^, event_ref^, NativeDomainStore._text(stmt, 7), budget^, NativeDomainStore._text(stmt, 9), stmt.column_int(10), NativeDomainStore._text(stmt, 11), NativeDomainStore._text(stmt, 12), NativeDomainStore._text(stmt, 13))
def list_outbox_records(mut store: NativeDomainStore, run_id: String, status: String = "", limit: Int = -1) raises SQLiteError -> List[BridgeDelivery]:
    return list_bridge_records(store, "bridge_outbox", run_id, status, limit)
def list_inbox_records(mut store: NativeDomainStore, run_id: String, status: String = "", limit: Int = -1) raises SQLiteError -> List[BridgeDelivery]:
    return list_bridge_records(store, "bridge_inbox", run_id, status, limit)
def transition_bridge_delivery(mut store: NativeDomainStore, table: String, run_id: String, delivery_id: String, to_status: String, updated_at: String, idempotency_key: String = "") raises SQLiteError -> BridgeDelivery:
    if to_status != "pending" and to_status != "delivered" and to_status != "imported" and to_status != "failed":
        raise SQLiteError(code=1, message="domain store: invalid bridge status")
    if updated_at == "": raise SQLiteError(code=1, message="domain store: bridge updated_at must not be empty")
    var current = _get_bridge(store, table, run_id, delivery_id)
    if not bridge_status_transition_allowed(current.status, to_status): raise SQLiteError(code=1, message="domain store: invalid bridge status transition")
    if idempotency_key == "" and current.status == to_status: return current^
    store.db.begin_immediate()
    try:
        if current.status != to_status:
            var update = store.db.query("UPDATE " + table + " SET status=?,updated_at=? WHERE run_id=? AND id=? AND status=?")
            update.bind_text(1, to_status); update.bind_text(2, updated_at); update.bind_text(3, run_id); update.bind_text(4, delivery_id); update.bind_text(5, current.status); _ = update.step(); update.close()
            if store.db.changes() != 1: raise SQLiteError(code=1, message="domain store: bridge transition lost ownership")
        if idempotency_key != "":
            var payload = _bridge_operation_payload(delivery_id, to_status, current.attempts)
            _record_bridge_journal(store, run_id, idempotency_key, "bridge." + table + "." + to_status, payload, "bridge." + table + "." + to_status, updated_at)
        store.db.commit()
    except err:
        store.db.rollback(); raise err^
    return _get_bridge(store, table, run_id, delivery_id)
def claim_bridge_delivery(mut store: NativeDomainStore, table: String, run_id: String, delivery_id: String, updated_at: String, idempotency_key: String = "") raises SQLiteError -> BridgeDelivery:
    return transition_bridge_delivery(store, table, run_id, delivery_id, "pending", updated_at, idempotency_key)
def deliver_bridge_delivery(mut store: NativeDomainStore, table: String, run_id: String, delivery_id: String, updated_at: String, idempotency_key: String = "") raises SQLiteError -> BridgeDelivery:
    if updated_at == "": raise SQLiteError(code=1, message="domain store: bridge updated_at must not be empty")
    var current = _get_bridge(store, table, run_id, delivery_id)
    if current.status != "pending" and current.status != "delivered":
        raise SQLiteError(code=1, message="domain store: invalid bridge status transition")
    var expected_attempts = current.attempts
    if current.status == "pending": expected_attempts += 1
    var expected_payload = _bridge_operation_payload(delivery_id, "delivered", expected_attempts)
    if idempotency_key != "":
        var prior = store.db.query("SELECT command_type,payload FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
        prior.bind_text(1, run_id); prior.bind_text(2, idempotency_key)
        if prior.step():
            var prior_type = store._text(prior, 0)
            var prior_payload = store._text(prior, 1)
            prior.close()
            if prior_type != "bridge." + table + ".delivered" or not _bridge_json_equal(prior_payload, expected_payload):
                raise SQLiteError(code=1, message="domain store: bridge idempotency conflict")
            return current^
        prior.close()
    if current.status == "delivered": return current^
    _validate_bridge_budget(current, next_attempt=True)
    var next_budget = RuntimeBudget()
    try:
        next_budget = current.budget.consume(runtime_hops=1, impulse_count=1)
    except err:
        raise SQLiteError(code=1, message="domain store: bridge budget exhausted")
    var next_attempts = current.attempts + 1
    store.db.begin_immediate()
    try:
        var update = store.db.query("UPDATE " + table + " SET budget=?,status='delivered',attempts=?,updated_at=? WHERE run_id=? AND id=? AND status='pending'")
        update.bind_text(1, next_budget.to_json()); update.bind_int(2, next_attempts); update.bind_text(3, updated_at); update.bind_text(4, run_id); update.bind_text(5, delivery_id); _ = update.step(); update.close()
        if store.db.changes() != 1: raise SQLiteError(code=1, message="domain store: bridge delivery lost ownership")
        if idempotency_key != "":
            _record_bridge_journal(store, run_id, idempotency_key, "bridge." + table + ".delivered", _bridge_operation_payload(delivery_id, "delivered", next_attempts), "bridge." + table + ".delivered", updated_at, current.impulse.id)
        store.db.commit()
    except err:
        store.db.rollback(); raise err^
    return _get_bridge(store, table, run_id, delivery_id)
def retry_bridge_delivery(mut store: NativeDomainStore, table: String, run_id: String, delivery_id: String, updated_at: String, idempotency_key: String = "") raises SQLiteError -> BridgeDelivery:
    if table != "bridge_inbox" and table != "bridge_outbox": raise SQLiteError(code=1, message="domain store: invalid bridge table")
    if updated_at == "": raise SQLiteError(code=1, message="domain store: bridge updated_at must not be empty")
    var current = _get_bridge(store, table, run_id, delivery_id)
    if current.status != "pending": raise SQLiteError(code=1, message="domain store: invalid bridge status transition")
    var next_attempts = current.attempts + 1
    if idempotency_key != "":
        var existing = store.db.query("SELECT command_type,payload FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
        existing.bind_text(1, run_id); existing.bind_text(2, idempotency_key)
        if existing.step():
            var prior_type = store._text(existing, 0)
            var prior_payload = store._text(existing, 1)
            existing.close()
            var replay_payload = _bridge_operation_payload(delivery_id, "pending", current.attempts)
            if prior_type != "bridge." + table + ".retry" or not _bridge_json_equal(prior_payload, replay_payload):
                raise SQLiteError(code=1, message="domain store: bridge idempotency conflict")
            return current^
        existing.close()
    var payload = _bridge_operation_payload(delivery_id, "pending", next_attempts)
    _validate_bridge_budget(current, next_attempt=True)
    store.db.begin_immediate()
    try:
        var update = store.db.query("UPDATE " + table + " SET attempts=attempts+1,updated_at=? WHERE run_id=? AND id=? AND status='pending'")
        update.bind_text(1, updated_at); update.bind_text(2, run_id); update.bind_text(3, delivery_id); _ = update.step(); update.close()
        if store.db.changes() != 1: raise SQLiteError(code=1, message="domain store: bridge retry lost ownership")
        if idempotency_key != "": _record_bridge_journal(store, run_id, idempotency_key, "bridge." + table + ".retry", _bridge_operation_payload(delivery_id, "pending", next_attempts), "bridge." + table + ".retry", updated_at, current.impulse.id)
        store.db.commit()
    except err:
        store.db.rollback(); raise err^
    return _get_bridge(store, table, run_id, delivery_id)

