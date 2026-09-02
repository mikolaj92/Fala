"""Native CLI operational maintenance, projection, and bridge commands."""
from std.collections import List
from std.pathlib import Path, cwd
from std.os import remove
from std.ffi import CStringSlice, c_int, external_call
from fala.sqlite import SQLiteError
from fala.json import parse_json, canonical_json_text, quote_json_string as _quote
from fala.domain import Impulse, RuntimeBudget, BridgeDelivery, EventRef, RuntimeRef, RunRef
from fala.ops_maintenance import (
    JournalMaintenancePlan, RunDeleteCounts, RunRetentionPlan, RunRetentionItem,
    ReactionGarbageCollectionPlan, collect_reaction_garbage, maintain_journal,
)
from fala.ops_projections import rebuild_projections
from fala.ops_bridge import import_bridge_delivery, get_outbox_delivery, deliver_bridge_delivery
from fala.bridge_transport import deliver_local_bridge
from emberjson import Value, Object, to_string
from fala.native_cli_parse import (
    _safe, _flag, _has_option, _validate, _bool_option, _maintenance_number,
    _maintenance_integer, _path,
)

def _count_json(counts: RunDeleteCounts) -> String:
    return "{\"run_id\":" + _quote(counts.run_id) + ",\"bridge_inbox\":" + String(counts.bridge_inbox) + ",\"bridge_outbox\":" + String(counts.bridge_outbox) + ",\"projections\":" + String(counts.projections) + ",\"homeostats\":" + String(counts.homeostats) + ",\"processes\":" + String(counts.processes) + ",\"reactions\":" + String(counts.reactions) + ",\"associations\":" + String(counts.associations) + ",\"impulse_relations\":" + String(counts.impulse_relations) + ",\"impulse_types\":" + String(counts.impulse_types) + ",\"impulses\":" + String(counts.impulses) + ",\"runtime_events\":" + String(counts.runtime_events) + ",\"runtime_commands\":" + String(counts.runtime_commands) + ",\"runs\":" + String(counts.runs) + "}"

def _maintenance_json(plan: JournalMaintenancePlan) -> String:
    var runs = "["
    var first = True
    for item in plan.retention.runs:
        if not first: runs += ","
        first = False
        var deleted = "false"
        if item.deleted: deleted = "true"
        runs += "{\"run_id\":" + _quote(item.run_id) + ",\"status\":" + _quote(item.status) + ",\"created_at\":" + _quote(item.created_at) + ",\"updated_at\":" + _quote(item.updated_at) + ",\"finished_at\":" + _quote(item.finished_at) + ",\"deleted\":" + deleted + ",\"row_counts\":" + _count_json(item.row_counts) + "}"
    runs += "]"
    var candidates = "["
    first = True
    for digest in plan.reaction_gc.candidates:
        if not first: candidates += ","
        first = False
        candidates += _quote(digest)
    candidates += "]"
    var deleted_reactions = "["
    first = True
    for digest in plan.reaction_gc.deleted:
        if not first: deleted_reactions += ","
        first = False
        deleted_reactions += _quote(digest)
    deleted_reactions += "]"
    var dry_run = "false"
    if plan.dry_run: dry_run = "true"
    var vacuum = "false"
    if plan.vacuum: vacuum = "true"
    var vacuumed = "false"
    if plan.vacuumed: vacuumed = "true"
    var gc_dry_run = "false"
    if plan.reaction_gc.dry_run: gc_dry_run = "true"
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"maintenance\",\"maintenance\":{\"dry_run\":" + dry_run + ",\"older_than_days\":" + String(plan.older_than_days) + ",\"keep_last\":" + String(plan.keep_last) + ",\"vacuum\":" + vacuum + ",\"before\":" + _quote(plan.before) + ",\"retention\":{\"dry_run\":" + dry_run + ",\"before\":" + _quote(plan.retention.before) + ",\"candidate_count\":" + String(plan.retention.candidate_count) + ",\"deleted_run_count\":" + String(plan.retention.deleted_run_count) + ",\"row_counts\":" + _count_json(plan.retention.row_counts) + ",\"runs\":" + runs + "},\"reaction_gc\":{\"dry_run\":" + gc_dry_run + ",\"referenced_count\":" + String(plan.reaction_gc.referenced_count) + ",\"blob_count\":" + String(plan.reaction_gc.blob_count) + ",\"candidate_count\":" + String(plan.reaction_gc.candidate_count) + ",\"deleted_count\":" + String(plan.reaction_gc.deleted_count) + ",\"candidates\":" + candidates + ",\"deleted\":" + deleted_reactions + "},\"vacuumed\":" + vacuumed + "}}"

def _gc_json(plan: ReactionGarbageCollectionPlan) -> String:
    var run_ids = "["
    var first = True
    for value in plan.run_ids:
        if not first: run_ids += ","
        first = False
        run_ids += _quote(value)
    run_ids += "]"
    var scanned = "["
    first = True
    for value in plan.scanned_run_ids:
        if not first: scanned += ","
        first = False
        scanned += _quote(value)
    scanned += "]"
    var collectable = "["
    first = True
    for value in plan.collectable:
        if not first: collectable += ","
        first = False
        collectable += _quote(value)
    collectable += "]"
    var deleted = "["
    first = True
    for value in plan.deleted:
        if not first: deleted += ","
        first = False
        deleted += _quote(value)
    deleted += "]"
    var dry_run = "false"
    if plan.dry_run: dry_run = "true"
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"gc\",\"dry_run\":" + dry_run + ",\"reaction_root\":" + _quote(plan.reaction_root) + ",\"run_ids\":" + run_ids + ",\"scanned_run_ids\":" + scanned + ",\"referenced_count\":" + String(plan.referenced_count) + ",\"blob_count\":" + String(plan.blob_count) + ",\"kept_count\":" + String(plan.kept_count) + ",\"collectable_count\":" + String(plan.candidate_count) + ",\"deleted_count\":" + String(plan.deleted_count) + ",\"bytes_reclaimable\":" + String(plan.bytes_reclaimable) + ",\"bytes_reclaimed\":" + String(plan.bytes_reclaimed) + ",\"collectable\":" + collectable + ",\"deleted\":" + deleted + "}"

def _gc(command: String) raises -> String:
    if _has_option(command, "--older-than"):
        return _error("native_boundary", "gc --older-than requires native filesystem metadata support")
    var path = _path(command)
    var reaction_root = _flag(command, "--reaction-root", "")
    var run_id = _flag(command, "--run-id", "")
    var dry_run = not _has_option(command, "--delete")
    if _has_option(command, "--dry-run"): dry_run = True
    var store = NativeDomainStore.open(path)
    store.initialize()
    var plan = collect_reaction_garbage(store, reaction_root, run_id, dry_run)
    store.close()
    return _gc_json(plan)
def _maintain_journal(command: String) raises -> String:
    var path = _path(command)
    var older_than_days = _maintenance_number(command, "--older-than-days", -1.0)
    if older_than_days < 0.0: raise Error(String(SQLiteError(code=2, message="argument_error: --older-than-days is required and must be non-negative")))
    var retention_keep_last = _maintenance_integer(command, "--keep-last", -1)
    if retention_keep_last < -1: raise Error(String(SQLiteError(code=2, message="argument_error: --keep-last must be non-negative")))
    var vacuum = True
    if _has_option(command, "--no-vacuum"): vacuum = False
    if _has_option(command, "--vacuum"): vacuum = True
    var dry_run = True
    if _has_option(command, "--delete"): dry_run = False
    if _has_option(command, "--dry-run"): dry_run = True
    var reaction_root = _flag(command, "--reaction-root", "")
    var store = NativeDomainStore.open(path)
    store.initialize()
    var plan = maintain_journal(store, older_than_days, retention_keep_last, vacuum, dry_run, reaction_root)
    store.close()
    return _maintenance_json(plan)

def _projection_rebuild(command: String) raises -> String:
    var path = _path(command)
    var run_id = _flag(command, "--run-id")
    if run_id == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
    var names = List[String]()
    var requested = _flag(command, "--name", "")
    if requested != "": names.append(requested)
    var updated_at = _flag(command, "--now")
    var store = NativeDomainStore.open(path)
    store.initialize()
    var rebuilt = rebuild_projections(store, run_id, names, updated_at)
    store.close()
    var items = "["
    var first = True
    for projection in rebuilt:
        if not first: items += ","
        first = False
        items += projection.to_json()
    items += "]"
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"projections\",\"count\":" + String(len(rebuilt)) + ",\"projections\":" + items + "}"
def _bridge_path(path: String, label: String) raises -> Path:
    if path == "": raise Error(String(SQLiteError(code=2, message="argument_error: " + label + " path is required")))
    if not _safe(path) or path.find("://") >= 0 or path == ":memory:" or path.startswith("file:"):
        raise Error(String(SQLiteError(code=2, message="unsafe_path: invalid " + label + " path")))
    try:
        var value = Path(path).expanduser()
        if not path.startswith("/"): value = cwd() / value
        return value
    except err:
        raise Error(String(SQLiteError(code=2, message="unsafe_path: invalid " + label + " path")))

def _bridge_string(value: Value, path: String) raises -> String:
    if not value.is_string(): raise Error("invalid_type at " + path + ": expected string")
    return value.string()

def _bridge_int(value: Value, path: String) raises -> Int:
    if value.is_int(): return Int(value.int())
    if value.is_uint(): return Int(value.uint())
    raise Error("invalid_type at " + path + ": expected integer")

def _bridge_object(value: Value, path: String) raises -> Object:
    if not value.is_object(): raise Error("invalid_type at " + path + ": expected object")
    return value.object().copy()

def _bridge_known(object: Object, names: List[String], path: String) raises:
    for pair in object.items():
        var known = False
        for name in names:
            if pair.key == name: known = True
        if not known: raise Error("unknown_field at " + path + "/" + pair.key)

def _bridge_runtime(value: Value, path: String) raises -> RuntimeRef:
    var object = _bridge_object(value, path)
    _bridge_known(object, ["id", "uri", "metadata"], path)
    if "id" not in object: raise Error("missing_field at " + path + "/id")
    var id = _bridge_string(object["id"].copy(), path + "/id")
    var uri = ""
    if "uri" in object: uri = _bridge_string(object["uri"].copy(), path + "/uri")
    var metadata = "{}"
    if "metadata" in object:
        var m = object["metadata"].copy()
        if not m.is_object(): raise Error("invalid_type at " + path + "/metadata: expected object")
        metadata = canonical_json_text(to_string(m^))
    return RuntimeRef(id, uri, metadata)

def _bridge_run(value: Value, path: String) raises -> RunRef:
    var object = _bridge_object(value, path)
    _bridge_known(object, ["runtime", "run_id"], path)
    if "runtime" not in object or "run_id" not in object: raise Error("missing_field at " + path)
    return RunRef(_bridge_runtime(object["runtime"].copy(), path + "/runtime"), _bridge_string(object["run_id"].copy(), path + "/run_id"))

def _bridge_event(value: Value, path: String) raises -> EventRef:
    if value.is_null(): return EventRef(RuntimeRef("runtime"), "run")
    var object = _bridge_object(value, path)
    _bridge_known(object, ["runtime", "run_id", "event_id", "sequence"], path)
    if "runtime" not in object or "run_id" not in object or "event_id" not in object or "sequence" not in object: raise Error("missing_field at " + path)
    return EventRef(_bridge_runtime(object["runtime"].copy(), path + "/runtime"), _bridge_string(object["run_id"].copy(), path + "/run_id"), _bridge_string(object["event_id"].copy(), path + "/event_id"), _bridge_int(object["sequence"].copy(), path + "/sequence"))

def _bridge_impulse(value: Value, path: String) raises -> Impulse:
    var object = _bridge_object(value, path)
    _bridge_known(object, ["id", "run_id", "impulse_type", "payload", "metadata", "created_at", "updated_at"], path)
    for name in ["id", "run_id", "impulse_type", "payload", "metadata", "created_at", "updated_at"]:
        if name not in object: raise Error("missing_field at " + path + "/" + name)
    var payload = object["payload"].copy(); var metadata = object["metadata"].copy()
    if not payload.is_object() and not payload.is_array(): raise Error("invalid_type at " + path + "/payload")
    if not metadata.is_object(): raise Error("invalid_type at " + path + "/metadata")
    return Impulse(_bridge_string(object["id"].copy(), path + "/id"), _bridge_string(object["run_id"].copy(), path + "/run_id"), _bridge_string(object["impulse_type"].copy(), path + "/impulse_type"), canonical_json_text(to_string(payload^)), canonical_json_text(to_string(metadata^)), _bridge_string(object["created_at"].copy(), path + "/created_at"), _bridge_string(object["updated_at"].copy(), path + "/updated_at"))

def _bridge_budget(value: Value, path: String) raises -> RuntimeBudget:
    var object = _bridge_object(value, path)
    var names: List[String] = ["runtime_hops", "spawned_runs", "impulse_count", "wall_time_seconds", "attempts", "reaction_bytes"]
    _bridge_known(object, names, path)
    var values = List[Int](length=6, fill=0); var limited = List[Bool](length=6, fill=False)
    for i in range(6):
        if names[i] in object:
            var item = object[names[i]].copy()
            if not item.is_null(): values[i] = _bridge_int(item^, path + "/" + names[i]); limited[i] = True
    return RuntimeBudget(values[0], values[1], values[2], values[3], values[4], values[5], limited[0], limited[1], limited[2], limited[3], limited[4], limited[5])

def _decode_bridge_delivery(text: String) raises -> BridgeDelivery:
    var root = Value(parse_string=text)
    var object = _bridge_object(root, "/")
    var names: List[String] = ["id", "run_id", "idempotency_key", "source", "target", "impulse", "event_ref", "pool_id", "budget", "status", "attempts", "metadata", "created_at", "updated_at"]
    _bridge_known(object, names, "")
    for name in names:
        if name not in object: raise Error("missing_field at /" + name)
    var pool = ""
    if not object["pool_id"].is_null(): pool = _bridge_string(object["pool_id"].copy(), "/pool_id")
    var metadata = object["metadata"].copy()
    if not metadata.is_object(): raise Error("invalid_type at /metadata")
    var row = BridgeDelivery(_bridge_string(object["id"].copy(), "/id"), _bridge_string(object["run_id"].copy(), "/run_id"), _bridge_string(object["idempotency_key"].copy(), "/idempotency_key"), _bridge_run(object["source"].copy(), "/source"), _bridge_run(object["target"].copy(), "/target"), _bridge_impulse(object["impulse"].copy(), "/impulse"), _bridge_event(object["event_ref"].copy(), "/event_ref"), pool, _bridge_budget(object["budget"].copy(), "/budget"), _bridge_string(object["status"].copy(), "/status"), _bridge_int(object["attempts"].copy(), "/attempts"), canonical_json_text(to_string(metadata^)), _bridge_string(object["created_at"].copy(), "/created_at"), _bridge_string(object["updated_at"].copy(), "/updated_at"))
    if not row.is_valid(): raise Error("invalid_value at /: invalid BridgeDelivery")
    return row^
def _bridge_atomic_write(path: Path, text: String) raises:
    var temp = Path(path.__fspath__() + ".tmp")
    try:
        temp.write_text(text + "\n")
        var source_text = temp.__fspath__() + "\0"
        var target_text = path.__fspath__() + "\0"
        var source_c = CStringSlice(source_text)
        var target_c = CStringSlice(target_text)
        var result = external_call["rename", c_int](source_c, target_c)
        if result != 0: raise Error(String(SQLiteError(code=1, message="bridge export failed")))
    except err:
        try: remove(temp)
        except: pass
        raise Error(String(SQLiteError(code=1, message="bridge export failed")))
def _bridge_export(command: String) raises -> String:
    var db_path = _bridge_path(_flag(command, "--db"), "database")
    var out_path = _bridge_path(_flag(command, "--out"), "output")
    if db_path.__fspath__() == out_path.__fspath__(): raise Error(String(SQLiteError(code=2, message="unsafe_path: database and output paths must differ")))
    var store = NativeDomainStore.open(db_path.__fspath__()); store.initialize()
    var delivery_id = _flag(command, "--delivery-id")
    var lookup = store.db.query("SELECT run_id FROM bridge_outbox WHERE id=?")
    lookup.bind_text(1, delivery_id)
    if not lookup.step():
        lookup.close(); store.close(); return "{\"ok\":false,\"runtime\":\"mojo\",\"error\":{\"type\":\"not_found\",\"message\":\"bridge delivery not found\"}}"
    var run_id = lookup.column_text(0); lookup.close()
    var row = get_outbox_delivery(store, run_id, delivery_id)
    var text = ""
    try:
        text = canonical_json_text(row.to_json())
    except err:
        store.close(); raise Error(String(SQLiteError(code=1, message="bridge export failed")))
    store.close()
    _bridge_atomic_write(out_path, text)
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"bridge\",\"delivery\":" + text + "}"

def _bridge_import(command: String) raises -> String:
    var db_path = _bridge_path(_flag(command, "--db"), "database")
    var file_path = _bridge_path(_flag(command, "--file"), "input")
    var text = ""
    try:
        text = file_path.read_text()
    except err:
        raise Error(String(SQLiteError(code=1, message="bridge import failed: unable to read input")))
    var row: BridgeDelivery
    try: row = _decode_bridge_delivery(text)
    except err: raise Error(String(SQLiteError(code=2, message="invalid_json: malformed BridgeDelivery")))
    var key = _flag(command, "--idempotency-key", "bridge.file.import:" + row.id)
    if key == "": raise Error(String(SQLiteError(code=2, message="argument_error: idempotency key must not be empty")))
    var store = NativeDomainStore.open(db_path.__fspath__()); store.initialize(); var imported = import_bridge_delivery(store, row, key); store.close()
    var serialized = ""
    try: serialized = imported.to_json()
    except err: raise Error(String(SQLiteError(code=1, message="bridge import failed")))
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"bridge\",\"delivery\":" + serialized + "}"


def _error(kind: String, message: String = "") -> String:
    var detail = message
    if detail == "": detail = kind
    return "{\"ok\":false,\"runtime\":\"mojo\",\"error\":{\"type\":" + _quote(kind) + ",\"message\":" + _quote(detail) + "}}"


def _bridge_deliver(command: String) raises -> String:
    var source_path = _flag(command, "--db")
    var target_path = _flag(command, "--target-db")
    var run_id = _flag(command, "--run-id")
    var delivery_id = _flag(command, "--delivery-id")
    var updated_at = _flag(command, "--now")
    var delivery_key = _flag(command, "--idempotency-key", "")
    var import_key = _flag(command, "--import-idempotency-key", "")
    var result = deliver_local_bridge(
        source_path, target_path, run_id, delivery_id, updated_at,
        delivery_key, import_key,
    )
    return "{\"ok\":true,\"runtime\":\"mojo\",\"delivered\":" + result.source_delivery.to_json() + ",\"imported\":" + result.imported_delivery.to_json() + ",\"delivery_replayed\":" + ("true" if result.source_replayed else "false") + ",\"import_replayed\":" + ("true" if result.imported_replayed else "false") + "}"
