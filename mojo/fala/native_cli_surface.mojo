"""Python-free native operator CLI helpers."""
from std.collections import List
from fala.journal import RunRow, NativeJournal, EventInput, ProcessRow
from fala.schema import initialize_native_schema, SCHEMA_VERSION, table_names, SchemaStatus, schema_status
from fala.sqlite import Connection, Statement, SQLiteError
from fala.json import parse_json, canonical_json_text, quote_json_string as _quote
from fala.domain import Impulse, Association, Reaction, RuntimeBudget, BridgeDelivery, EventRef, RuntimeRef, RunRef
from fala.native_driver import diagnose_waits, diagnose_wait_graph, observe_run_boundary
from fala.reactions import FileReactionStore, ReactionBlob
from emberjson import Value, Object, to_string
from std.pathlib import Path, cwd
from fala.domain_store import NativeDomainStore
from fala.ops_maintenance import (
    JournalMaintenancePlan, RunDeleteCounts, RunRetentionPlan, RunRetentionItem,
    ReactionGarbageCollectionPlan, collect_reaction_garbage, maintain_journal,
)
from fala.ops_projections import rebuild_projections
from fala.ops_bridge import import_bridge_delivery, get_outbox_delivery, deliver_bridge_delivery
from std.os import makedirs, remove
from std.ffi import CStringSlice, c_int, external_call
from fala.bridge_transport import deliver_local_bridge
from fala.runs import RunLifecycle, RunLifecycleRecord
from fala.native_cli_parse import (
    _safe, _word, _count, _flag, _flag_alias, _has_option, _validate, _bool_option,
    _limit, _after_sequence, _maintenance_number, _maintenance_integer,
    _parent_directory, _path, _require_db_value, _json, _metadata_value, _repeat_values,
    _string_array,
)

def _init(command: String) raises -> String:
    var db_path = _path(command)
    var reaction_root = _flag(command, "--reaction-root", ".fala/reactions")
    if not _safe(reaction_root):
        raise Error(String(SQLiteError(code=2, message="unsafe_path: invalid reaction root path")))
    try:
        makedirs(Path(_parent_directory(db_path)), exist_ok=True)
        makedirs(Path(reaction_root) / "blobs" / "sha256", exist_ok=True)
        _ = initialize_database(db_path)
    except err:
        raise Error(String(SQLiteError(code=1, message="init failed: " + String(err))))
    return "{\"ok\":true,\"runtime\":\"mojo\",\"db\":" + _quote(db_path) + ",\"reaction_root\":" + _quote(reaction_root) + ",\"schema_version\":" + String(SCHEMA_VERSION) + "}"

def initialize_database(path: String) raises -> String:
    var connection = Connection(path)
    initialize_native_schema(connection)
    connection.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"database\":" + _quote(path) + "}"


def _schema_model() -> String:
    var tables = "["
    var first = True
    for name in table_names():
        if not first: tables += ","
        first = False
        tables += _quote(name)
    tables += "]"
    return "{\"ok\":true,\"runtime\":\"mojo\",\"schema\":{\"version\":" + String(SCHEMA_VERSION) + ",\"tables\":" + tables + "}}"
def _schema_status_json(status: SchemaStatus) -> String:
    var missing = "["
    var first = True
    for name in status.missing_tables:
        if not first: missing += ","
        first = False
        missing += _quote(name)
    missing += "]"
    var process_id = "false"
    if status.runtime_events_has_process_id: process_id = "true"
    var event_schema = "false"
    if status.runtime_events_has_schema_version: event_schema = "true"
    var current = "false"
    if status.is_current(): current = "true"
    return "{\"current_version\":" + String(status.current_version) + ",\"latest_version\":" + String(status.latest_version) + ",\"user_version\":" + String(status.user_version) + ",\"migration_version\":" + String(status.migration_version) + ",\"missing_tables\":" + missing + ",\"runtime_events_has_process_id\":" + process_id + ",\"runtime_events_has_schema_version\":" + event_schema + ",\"current\":" + current + "}"


def _migration_metadata(mut connection: Connection) raises -> String:
    var table = connection.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='schema_migrations'")
    if not table.step():
        table.close()
        return "null"
    table.close()
    var stmt = connection.query("SELECT id,version,name,applied_at FROM schema_migrations WHERE id='runtime_backend'")
    if not stmt.step():
        stmt.close()
        return "null"
    var result = "{\"id\":" + _quote(stmt.column_text(0)) + ",\"version\":" + String(stmt.column_int(1)) + ",\"name\":" + _quote(stmt.column_text(2)) + ",\"applied_at\":" + _quote(stmt.column_text(3)) + "}"
    stmt.close()
    return result


def _status(path: String) raises -> String:
    var connection = Connection(path)
    var status = schema_status(connection)
    var current = "false"
    if status.is_current(): current = "true"
    var user_version = status.user_version
    var status_json = _schema_status_json(status^)
    var tables = 0
    var count = connection.query("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    if count.step(): tables = count.column_int(0)
    var migration = _migration_metadata(connection)
    connection.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"database\":" + _quote(path) + ",\"schema_version\":" + String(user_version) + ",\"expected_version\":" + String(SCHEMA_VERSION) + ",\"table_count\":" + String(tables) + ",\"schema\":" + status_json + ",\"migration\":" + migration + ",\"current\":" + current + "}"


def _vacuum(path: String) raises -> String:
    var connection = Connection(path)
    connection.execute("VACUUM")
    connection.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"database\":" + _quote(path) + ",\"vacuumed\":true}"


from fala.native_cli_inspect import (
    _event_schema_max, _events_validate_schema, _runs, _command_rows,
    _run_inspect, _run_observe, _diagnose_waits, _impulse_inspect,
    _command_inspect, _process_inspect, _domain_inspect, _table, _rows,
    _trace,
)

def _integer_option(command: String, name: String, default: Int) raises -> Int:
    var raw = _flag(command, name, "")
    if raw == "": return default
    try:
        var parsed = parse_json(raw)
        if parsed.value.is_int(): return Int(parsed.value.int())
        if parsed.value.is_uint(): return Int(parsed.value.uint())
    except err:
        pass
    raise Error(String(SQLiteError(code=2, message="argument_error: invalid integer value for " + name)))

def _impulse_create(command: String) raises -> String:
    var run_id = _flag(command, "--run-id"); var impulse_id = _flag(command, "--impulse-id"); var impulse_type = _flag(command, "--impulse-type")
    if run_id == "" or impulse_id == "" or impulse_type == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id, --impulse-id, and --impulse-type are required")))
    var payload = _flag_alias(command, "--payload", "--payload-json", "{}"); var metadata = _flag_alias(command, "--metadata", "--metadata-json", "{}"); _json(payload); _json(metadata)
    var now = _flag(command, "--now"); var key = _flag(command, "--idempotency-key", "impulse.accept:" + impulse_id)
    var row = Impulse(id=impulse_id, run_id=run_id, impulse_type=impulse_type, payload=payload, metadata=metadata, created_at=now, updated_at=now)
    var store = NativeDomainStore.open(_path(command)); store.initialize(); var accepted = store.accept_impulse(row, key, now, _flag(command, "--actor"), _flag(command, "--correlation-id"), _flag(command, "--causation-id")); store.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"impulse\",\"id\":" + _quote(accepted.impulse.id) + ",\"run_id\":" + _quote(run_id) + ",\"replayed\":" + ("true" if accepted.replayed else "false") + ",\"command_id\":" + _quote(accepted.command.id) + ",\"event_id\":" + _quote(accepted.events[0].id) + "}"

def _process_schedule(command: String) raises -> String:
    var run_id = _flag(command, "--run-id"); var process_id = _flag(command, "--process-id"); var process_type = _flag(command, "--process-type")
    if run_id == "" or process_id == "" or process_type == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id, --process-id, and --process-type are required")))
    var input_json = _flag_alias(command, "--input", "--input-json", "{}"); var metadata = _flag_alias(command, "--metadata", "--metadata-json", "{}"); var output_schema = _flag(command, "--output-schema", "{}"); _json(input_json); _json(metadata); _json(output_schema)
    var now = _flag(command, "--now"); var journal = NativeJournal.open(_path(command)); journal.initialize()
    var row = journal.schedule_process(run_id, process_id, process_type, now, input_json, metadata, _flag(command, "--impulse-id"), _integer_option(command, "--priority", 0), _integer_option(command, "--max-attempts", 1), _flag(command, "--available-at", now), output_schema, _flag(command, "--idempotency-key", "process.schedule:" + process_id), _flag(command, "--actor")); journal.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"process\",\"id\":" + _quote(row.id) + ",\"run_id\":" + _quote(row.run_id) + ",\"status\":" + _quote(row.status) + "}"

def _process_transition(command: String, target: String) raises -> String:
    var run_id = _flag(command, "--run-id"); var process_id = _flag(command, "--process-id"); var actor = _flag(command, "--actor", "cli"); var now = _flag(command, "--now")
    if run_id == "" or process_id == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id and --process-id are required")))
    var error_json = _flag_alias(command, "--error", "--error-json", "{}"); _json(error_json); var journal = NativeJournal.open(_path(command)); journal.initialize(); var row = ProcessRow(run_id="", id="", process_type="", impulse_id="", status="", priority=0, attempt=0, max_attempts=1, available_at="", lease_owner="", lease_expires_at="", input_json="{}", output_json="{}", error_json="{}", metadata="{}", created_at="", updated_at="", started_at="", finished_at="", output_schema_json="{}")
    if target == "cancel": row = journal.cancel_process(run_id, process_id, actor, now, error_json)
    else: row = journal.timeout_process(run_id, process_id, actor, now, error_json)
    journal.close(); return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"process\",\"id\":" + _quote(row.id) + ",\"run_id\":" + _quote(row.run_id) + ",\"status\":" + _quote(row.status) + "}"

def _association_append(command: String) raises -> String:
    var run_id = _flag(command, "--run-id"); var association_id = _flag(command, "--association-id"); var kind = _flag(command, "--kind")
    if run_id == "" or association_id == "" or kind == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id, --association-id, and --kind are required")))
    var values = _flag_alias(command, "--values", "--values-json", "{}"); var metadata = _flag_alias(command, "--metadata", "--metadata-json", "{}"); _json(values); _json(metadata)
    var row = Association(id=association_id, run_id=run_id, kind=kind, impulse_id=_flag(command, "--impulse-id"), values=values, metadata=metadata, created_at=_flag(command, "--now")); var store = NativeDomainStore.open(_path(command)); store.initialize(); store.put_association(row); store.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"association\",\"id\":" + _quote(association_id) + ",\"run_id\":" + _quote(run_id) + "}"
def _reaction_blob(root: String, content: List[UInt8], filename: String, metadata: String) raises -> ReactionBlob:
    try:
        var store = FileReactionStore(root)
        return store.put_bytes_raw(content, filename, metadata)
    except err:
        raise Error(String(SQLiteError(code=2, message="argument_error: unable to persist reaction: " + String(err))))
def _reaction_record(command: String) raises -> String:
    var run_id = _flag(command, "--run-id")
    var reaction_kind = _flag(command, "--kind")
    var input_path = _flag(command, "--path")
    var reaction_root = _flag(command, "--reaction-root")
    if run_id == "" or reaction_kind == "" or input_path == "" or reaction_root == "":
        raise Error(String(SQLiteError(code=2, message="argument_error: --run-id, --kind, --path, and --reaction-root are required")))
    if not _safe(input_path) or not _safe(reaction_root):
        raise Error(String(SQLiteError(code=2, message="argument_error: invalid reaction path")))
    var metadata_raw = _flag(command, "--metadata-json", "{}")
    var metadata = String("")
    try:
        metadata = canonical_json_text(metadata_raw)
        var parsed = parse_json(metadata)
        if not parsed.value.is_object(): raise Error("metadata must be an object")
    except err:
        raise Error(String(SQLiteError(code=2, message="invalid_json: metadata")))
    var source = Path(input_path)
    if not source.exists() or not source.is_file():
        raise Error(String(SQLiteError(code=2, message="argument_error: reaction path must be a file")))
    var content = List[UInt8]()
    try:
        content = source.read_bytes()
    except err:
        raise Error(String(SQLiteError(code=2, message="argument_error: unable to read reaction path")))
    var blob = _reaction_blob(reaction_root, content, source.name(), metadata)
    var metadata_value = Value()
    try:
        metadata_value = Value(parse_string=blob.metadata)
    except err:
        raise Error(String(SQLiteError(code=1, message="reaction metadata serialization failed")))
    if not metadata_value.is_object():
        raise Error(String(SQLiteError(code=1, message="reaction metadata serialization failed")))
    metadata_value.object()["reaction_store"] = Value(reaction_root)
    var persisted_metadata = String("")
    try:
        persisted_metadata = canonical_json_text(to_string(metadata_value))
    except err:
        raise Error(String(SQLiteError(code=1, message="reaction metadata serialization failed")))
    var reaction_id = _flag(command, "--reaction-id", "")
    if reaction_id == "": reaction_id = "reaction:" + blob.digest
    var key = _flag(command, "--idempotency-key", "")
    if key == "": key = "reaction.record:" + reaction_id
    var now = _flag(command, "--now")
    var row = Reaction(id=reaction_id, run_id=run_id, kind=reaction_kind, uri=blob.uri, impulse_id=_flag(command, "--impulse-id"), media_type=_flag(command, "--media-type"), size_bytes=blob.size_bytes, content_hash="sha256:" + blob.digest, metadata=persisted_metadata, created_at=now)
    var events = List[EventInput]()
    events.append(EventInput(id=key + ":event", event_type="reaction.recorded", payload=row.to_json(), created_at=now, impulse_id=row.impulse_id, process_id="", schema_version=1, actor="", correlation_id="", causation_id=""))
    var store = NativeDomainStore.open(_path(command))
    try:
        var submission = store.record_reaction(row, key, "reaction.record", key, now, events)
        var response_row = row.copy()
        if submission.replayed:
            response_row = store.get_reaction(run_id, reaction_id)
        store.close()
        var command_json = "{\"run_id\":" + _quote(submission.command.run_id) + ",\"id\":" + _quote(submission.command.id) + ",\"command_type\":" + _quote(submission.command.command_type) + ",\"idempotency_key\":" + _quote(submission.command.idempotency_key) + ",\"actor\":" + (_quote(submission.command.actor) if submission.command.actor != "" else "null") + ",\"correlation_id\":" + (_quote(submission.command.correlation_id) if submission.command.correlation_id != "" else "null") + ",\"causation_id\":" + (_quote(submission.command.causation_id) if submission.command.causation_id != "" else "null") + ",\"payload\":" + submission.command.payload + ",\"created_at\":" + _quote(submission.command.created_at) + "}"
        var event_json = "null"
        if len(submission.events) > 0:
            var event = submission.events[0].copy()
            event_json = "{\"run_id\":" + _quote(event.run_id) + ",\"sequence\":" + String(event.sequence) + ",\"id\":" + _quote(event.id) + ",\"event_type\":" + _quote(event.event_type) + ",\"schema_version\":" + String(event.schema_version) + ",\"command_id\":" + _quote(event.command_id) + ",\"payload\":" + event.payload + ",\"created_at\":" + _quote(event.created_at) + "}"
        return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"reaction\",\"replayed\":" + ("true" if submission.replayed else "false") + ",\"reaction\":" + response_row.to_json() + ",\"command\":" + command_json + ",\"event\":" + event_json + "}"
    except err:
        try:
            store.close()
        except close_err:
            pass
        raise err^


def _homeostat_transition(command: String, operation: String) raises -> String:
    var run_id = _flag(command, "--run-id"); var homeostat_id = _flag(command, "--homeostat-id"); var process_id = _flag(command, "--process-id"); var actor = _flag(command, "--actor", "cli"); var now = _flag(command, "--now")
    if run_id == "" or homeostat_id == "" or process_id == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id, --homeostat-id, and --process-id are required")))
    var output = _flag(command, "--output", "{}"); var error_json = _flag_alias(command, "--error", "--error-json", "{}"); var metadata = _flag_alias(command, "--metadata", "--metadata-json", "{}"); _json(output); _json(error_json); _json(metadata)
    var key = _flag(command, "--idempotency-key", "homeostat." + operation + ":" + homeostat_id)
    if operation == "reopen" and not _has_option(command, "--idempotency-key"): key = ""
    var journal = NativeJournal.open(_path(command)); journal.initialize(); var row = ProcessRow(run_id="", id="", process_type="", impulse_id="", status="", priority=0, attempt=0, max_attempts=1, available_at="", lease_owner="", lease_expires_at="", input_json="{}", output_json="{}", error_json="{}", metadata="{}", created_at="", updated_at="", started_at="", finished_at="", output_schema_json="{}")
    if operation == "open": row = journal.park_homeostat_process(run_id, homeostat_id, process_id, actor, now, output, metadata, key)
    elif operation == "reopen": row = journal.reopen_homeostat_process(run_id, homeostat_id, process_id, actor, now, key)
    else:
        var hs = "expired"; var ps = "timed_out"
        if operation == "complete": hs = "completed"; ps = "succeeded"
        elif operation == "cancel": hs = "cancelled"; ps = "cancelled"
        row = journal.transition_homeostat_process(run_id, homeostat_id, process_id, hs, ps, actor, now, output, error_json, key)
    journal.close(); return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"homeostat\",\"id\":" + _quote(homeostat_id) + ",\"process_id\":" + _quote(row.id) + ",\"status\":" + _quote(row.status) + "}"


def _homeostat_domain_values(command: String) raises -> String:
    var items = _repeat_values(command, "--value")
    var values = Object(capacity=len(items))
    for item in items:
        var equals = item.find("=")
        if equals <= 0:
            raise Error(String(SQLiteError(code=2, message="Invalid value '" + item + "'; expected key=value")))
        var key = String(item[byte=0:equals])
        var value = String(item[byte=equals + 1:])
        values[key] = Value(value)
    try:
        return canonical_json_text(to_string(Value(values^)))
    except err:
        raise Error(String(SQLiteError(code=2, message="invalid_json")))


def _homeostat_domain(command: String, operation: String) raises -> String:
    if operation == "open":
        var values = _flag(command, "--values-json", "{}")
        var metadata = _flag(command, "--metadata-json", "{}")
        try:
            var values_parsed = parse_json(values)
            var metadata_parsed = parse_json(metadata)
            if not values_parsed.value.is_object() or not metadata_parsed.value.is_object():
                raise Error("homeostat JSON must be an object")
            _ = canonical_json_text(to_string(values_parsed.value))
            _ = canonical_json_text(to_string(metadata_parsed.value))
        except err:
            raise Error(String(SQLiteError(code=2, message="invalid_json")))
        if _flag(command, "--homeostat-id", "") == "":
            return _error("native_boundary", "homeostat open requires a native identifier generator")
    else:
        _ = _homeostat_domain_values(command)
    return _error("native_boundary", "homeostat mutations require a native clock source")


def _create(command: String) raises -> String:
    var path = _path(command)

    var run_id = _flag(command, "--run-id")
    if run_id == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required; native run-id generation is unavailable")))
    var metadata = _metadata_value(command)
    var lifecycle = RunLifecycle.open(path)
    lifecycle.initialize()
    var result = lifecycle.create_result(run_id, _flag(command,"--now"), metadata, _flag(command,"--title"), "created", _flag(command,"--idempotency-key","run.create"), package_id=_flag(command,"--package-id"), package_version=_flag(command,"--package-version"), package_digest=_flag(command,"--package-digest"), correlation_path_id=_flag(command,"--correlation-path-id"), correlation_path_digest=_flag(command,"--correlation-path-digest"), runtime_version=_flag(command,"--runtime-version"), backend_version=_flag(command,"--backend-version"), actor="cli:user")
    lifecycle.close()
    var run = "{\"id\":" + _quote(result.run.id) + ",\"status\":" + _quote(result.run.status) + ",\"title\":" + ("null" if result.run.title == "" else _quote(result.run.title)) + ",\"package_id\":" + ("null" if result.run.package_id == "" else _quote(result.run.package_id)) + ",\"package_version\":" + ("null" if result.run.package_version == "" else _quote(result.run.package_version)) + ",\"package_digest\":" + ("null" if result.run.package_digest == "" else _quote(result.run.package_digest)) + ",\"correlation_path_id\":" + ("null" if result.run.correlation_path_id == "" else _quote(result.run.correlation_path_id)) + ",\"correlation_path_digest\":" + ("null" if result.run.correlation_path_digest == "" else _quote(result.run.correlation_path_digest)) + ",\"runtime_version\":" + ("null" if result.run.runtime_version == "" else _quote(result.run.runtime_version)) + ",\"backend_version\":" + ("null" if result.run.backend_version == "" else _quote(result.run.backend_version)) + ",\"schema_version\":" + String(result.run.schema_version) + ",\"metadata\":" + result.run.metadata + ",\"created_at\":" + _quote(result.run.created_at) + ",\"updated_at\":" + _quote(result.run.updated_at) + ",\"started_at\":" + ("null" if result.run.started_at == "" else _quote(result.run.started_at)) + ",\"finished_at\":" + ("null" if result.run.finished_at == "" else _quote(result.run.finished_at)) + "}"
    var cmd = "{\"run_id\":" + _quote(result.command.run_id) + ",\"id\":" + _quote(result.command.id) + ",\"command_type\":" + _quote(result.command.command_type) + ",\"idempotency_key\":" + _quote(result.command.idempotency_key) + ",\"actor\":" + ("null" if result.command.actor == "" else _quote(result.command.actor)) + ",\"correlation_id\":" + ("null" if result.command.correlation_id == "" else _quote(result.command.correlation_id)) + ",\"causation_id\":" + ("null" if result.command.causation_id == "" else _quote(result.command.causation_id)) + ",\"payload\":" + result.command.payload + ",\"created_at\":" + _quote(result.command.created_at) + "}"
    return "{\"ok\":true,\"run\":" + run + ",\"command\":" + cmd + ",\"replayed\":" + ("true" if result.replayed else "false") + "}"
    
from fala.native_cli_ops import (
    _gc, _maintain_journal, _projection_rebuild, _bridge_export,
    _bridge_import, _error, _bridge_deliver,
)

def _transition(command: String, operation: String) raises -> String:
    var path = _path(command)
    var run_id = _flag(command, "--run-id")
    if run_id == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
    var now = _flag(command, "--now")
    var key = _flag(command, "--idempotency-key", "run." + operation)
    var lifecycle = RunLifecycle.open(path)
    lifecycle.initialize()
    var row = RunRow(id="", status="", title="", metadata="", created_at="", updated_at="")
    if operation == "start":
        row = lifecycle.start(run_id, now, key)
    elif operation == "wait":
        row = lifecycle.wait(run_id, now, key)
    elif operation == "complete":
        row = lifecycle.complete(run_id, now, key)
    elif operation == "fail":
        row = lifecycle.fail(run_id, now, key)
    elif operation == "request_cancel":
        row = lifecycle.request_cancel(run_id, now, key, reason=_flag(command, "--reason", "cancel_requested"), reason_present=_has_option(command, "--reason"), actor="cli:user")
    elif operation == "cancel":
        row = lifecycle.request_cancel(run_id, now, key, reason=_flag(command, "--reason", ""), reason_present=_has_option(command, "--reason"), actor="cli:user")
    elif operation == "timeout":
        row = lifecycle.timeout(run_id, now, key)
    else:
        lifecycle.close()
        raise Error(String(SQLiteError(code=2, message="argument_error: unknown lifecycle operation")))
    lifecycle.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"run\",\"id\":" + _quote(row.id) + ",\"status\":" + _quote(row.status) + "}"

from fala.graph_tools import graph_expand, graph_validate, graph_fingerprint, graph_diff


def dispatch_native_command(command: String) raises -> String:
    try:
        var first = _word(command, 0)
        var second = _word(command, 1)
        # Progressive disclosure: `ops <cmd>...` is an alias for operator tools.
        if first == "ops" and second != "":
            var rest = second
            var idx = 2
            while idx < _count(command):
                rest += " " + _word(command, idx)
                idx += 1
            return dispatch_native_command(rest)
        if first == "graph":
            if second != "expand" and second != "validate" and second != "fingerprint" and second != "diff": return _error("unsupported_command")
            _validate(command, "graph")
            if second == "expand": return "{\"graph\":" + graph_expand(_flag(command, "--package")) + ",\"ok\":true,\"runtime\":\"mojo\"}"
            if second == "validate":
                var report = graph_validate(_flag(command, "--package"))
                return "{\"ok\":" + ("true" if report.find("\"valid\":true") >= 0 else "false") + ",\"report\":" + report + ",\"runtime\":\"mojo\"}"
            if second == "fingerprint": return "{\"fingerprint\":" + _quote(graph_fingerprint(_flag(command, "--package"))) + ",\"ok\":true,\"runtime\":\"mojo\"}"
            return "{\"diff\":" + graph_diff(_flag(command, "--before"), _flag(command, "--after")) + ",\"ok\":true,\"runtime\":\"mojo\"}"
        if first == "init": _validate(command, "init"); return _init(command)
        if first == "gc":
            _validate(command, "gc")
            return _gc(command)
        elif first == "archive-run" or first == "archive-gc": return _error("native_boundary", first + " requires native filesystem archive support")
        if first == "run-until-idle": return _error("native_boundary", "run-until-idle requires a native execution adapter registry")
        if first == "replay-execution": return _error("native_boundary", "replay-execution requires a native execution replay host")
        if first == "doctor" and (_has_option(command, "--package") or _has_option(command, "--output")): return _error("native_boundary", "doctor package/YAML and output paths require native filesystem support")
        if first == "schema": _validate(command, "schema", True)
        elif first == "db":
            _require_db_value(command, "db")
            if second != "status" and _has_option(command, "--ensure-schema"): return _error("argument_error", "--ensure-schema is supported only for db status")
            _validate(command, "db", True)
        elif first == "doctor":
            _require_db_value(command, "doctor")
            _validate(command, "doctor", True)
        elif first == "events" and second == "validate-schema": _validate(command, "event-schema")
        elif first == "projections" and second == "rebuild": _validate(command, "projection")
        elif first == "runs" and second == "list": _validate(command, "run-list")
        elif first == "runs" and second == "observe": _validate(command, "run-observe")
        elif first == "runs" and second == "start": _validate(command, "transition"); return _transition(command, "start")
        elif first == "runs" and second == "wait": _validate(command, "transition"); return _transition(command, "wait")
        elif first == "runs" and second == "complete": _validate(command, "transition"); return _transition(command, "complete")
        elif first == "runs" and second == "fail": _validate(command, "transition"); return _transition(command, "fail")
        elif first == "runs" and second == "request-cancel": _validate(command, "transition"); return _transition(command, "request_cancel")
        elif first == "runs" and second == "cancel": _validate(command, "transition"); return _transition(command, "cancel")
        elif first == "runs" and second == "timeout": _validate(command, "transition"); return _transition(command, "timeout")
        elif first == "homeostats" and second == "list": _validate(command, "homeostats-list")
        elif first == "homeostats" and second == "inspect": return _error("unsupported_command")
        if (first == "commands" or first == "events" or first == "processes" or first == "impulses" or first == "impulse-types" or first == "impulse-relations" or first == "relations" or first == "associations" or first == "reactions" or first == "projections" or first == "bridge" or first == "bridges" or first == "runs") and second == "inspect": _validate(command, "inspect")
        if command == "commands list" or command.startswith("commands list "): _validate(command, "commands-list"); return _command_rows(_path(command), command)
        elif first == "reactions" and second == "record": _validate(command, "reaction-record"); return _reaction_record(command)
        if command == "trace" or command.startswith("trace "): _validate(command, "trace"); return _trace(_path(command), command)
        if command == "diagnose-waits" or command.startswith("diagnose-waits "): _validate(command, "diagnose-waits"); return _diagnose_waits(_path(command), _flag(command, "--run-id"), _flag(command, "--impulse-id"))
        if first == "bridge" and second == "deliver": _validate(command, "bridge-deliver"); return _bridge_deliver(command)
        if first == "bridge" and (second == "export" or second == "import"): 
            if second == "export": _validate(command, "bridge-export"); return _bridge_export(command)
            _validate(command, "bridge-import"); return _bridge_import(command)
        if command == "commands inspect" or command.startswith("commands inspect "): return _command_inspect(_path(command), _flag(command, "--run-id"), _flag(command, "--command-id"))
        if command == "impulses inspect" or command.startswith("impulses inspect "): return _impulse_inspect(_path(command), _flag(command, "--run-id"), _flag(command, "--impulse-id"))
        elif first == "maintain-journal": _validate(command, "maintenance")
        if command == "schema impulse": return "{\"ok\":true,\"runtime\":\"mojo\",\"schema\":\"impulse\"}"
        if command == "processes inspect" or command.startswith("processes inspect "): return _process_inspect(_path(command), _flag(command, "--run-id"), _flag(command, "--process-id"))
        if command == "impulse-types inspect" or command.startswith("impulse-types inspect "): return _domain_inspect(_path(command), "impulse_type", "impulse_types", _flag(command, "--run-id"), _flag(command, "--impulse-type-id"), "--impulse-type-id")
        if command == "impulse-relations inspect" or command.startswith("impulse-relations inspect ") or command == "relations inspect" or command.startswith("relations inspect "): return _domain_inspect(_path(command), "impulse_relation", "impulse_relations", _flag(command, "--run-id"), _flag(command, "--relation-id"), "--relation-id")
        if command == "reactions inspect" or command.startswith("reactions inspect "): return _domain_inspect(_path(command), "reaction", "reactions", _flag(command, "--run-id"), _flag(command, "--reaction-id"), "--reaction-id")
        if command == "associations inspect" or command.startswith("associations inspect "): return _domain_inspect(_path(command), "association", "associations", _flag(command, "--run-id"), _flag(command, "--association-id"), "--association-id")
        if command == "schema model" or command.startswith("schema model "): return _schema_model()
        if command == "schema fala-package" or command.startswith("schema fala-package "): return _error("native_boundary", "schema fala-package requires a native model schema encoder")
        if command == "db init" or command == "db migrate": return _error("argument_error", "--db is required")
        if command.startswith("db init "): return initialize_database(_path(command))
        if command.startswith("db migrate "): return initialize_database(_path(command))
        if command == "runs list" or command.startswith("runs list "): return _runs(_path(command), _flag(command,"--status"), _flag(command,"--run-id"), _limit(command), _bool_option(command, "--jsonl"))
        if command == "runs observe" or command.startswith("runs observe "):
            return _run_observe(_path(command), _flag(command, "--run-id"))
        if command == "runs inspect" or command.startswith("runs inspect "): _validate(command, "inspect"); return _run_inspect(_path(command), _flag(command, "--run-id"))
        if command == "db status" or command.startswith("db status "):
            var status_path = _path(command)
            if _has_option(command, "--ensure-schema"): _ = initialize_database(status_path)
            return _status(status_path)
        if command == "db schema" or command.startswith("db schema "): return _schema_model()
        if command == "db vacuum" or command.startswith("db vacuum "): return _vacuum(_path(command))
        if command == "maintain-journal" or command.startswith("maintain-journal "): return _maintain_journal(command)
        if command == "create-run" or command.startswith("create-run "): _validate(command, "create"); return _create(command)
        if command == "runs create" or command.startswith("runs create ") or command == "run create" or command.startswith("run create "): return _error("legacy_alias", "use create-run")
        if command == "projections rebuild" or command.startswith("projections rebuild "): return _projection_rebuild(command)
        if command == "impulses create" or command.startswith("impulses create "): _validate(command, "impulse-create"); return _impulse_create(command)
        if command == "processes schedule" or command.startswith("processes schedule "): _validate(command, "process-schedule"); return _process_schedule(command)
        if command == "processes cancel" or command.startswith("processes cancel "): _validate(command, "process-transition"); return _process_transition(command, "cancel")
        if command == "processes timeout" or command.startswith("processes timeout "): _validate(command, "process-transition"); return _process_transition(command, "timeout")
        if command == "homeostats expire" or command.startswith("homeostats expire ") or command == "homeostat expire" or command.startswith("homeostat expire "): _validate(command, "homeostat-domain-transition"); return _homeostat_domain(command, "expire")
        if command == "homeostats reopen" or command.startswith("homeostats reopen ") or command == "homeostat reopen" or command.startswith("homeostat reopen "): _validate(command, "homeostat-transition"); return _homeostat_transition(command, "reopen")
        if command == "associations append" or command.startswith("associations append "): _validate(command, "association-append"); return _association_append(command)
        if command == "homeostats open" or command.startswith("homeostats open ") or command == "homeostat open" or command.startswith("homeostat open "): _validate(command, "homeostat-domain-open"); return _homeostat_domain(command, "open")
        if command == "homeostats complete" or command.startswith("homeostats complete ") or command == "homeostat complete" or command.startswith("homeostat complete "): _validate(command, "homeostat-domain-transition"); return _homeostat_domain(command, "complete")
        if command == "homeostats cancel" or command.startswith("homeostats cancel ") or command == "homeostat cancel" or command.startswith("homeostat cancel "): _validate(command, "homeostat-domain-transition"); return _homeostat_domain(command, "cancel")
        if command == "projections list" or command.startswith("projections list "): _validate(command, "projections-list"); return _rows(_path(command), "projections", "projections", command)
        if command == "commands list" or command.startswith("commands list "): _validate(command, "commands-list"); return _command_rows(_path(command), command)
        if command == "events list" or command.startswith("events list "): _validate(command, "events-list"); return _rows(_path(command), "events", "runtime_events", command)
        if command == "processes list" or command.startswith("processes list "): _validate(command, "processes-list"); return _rows(_path(command), "processes", "processes", command)
        if command == "impulses list" or command.startswith("impulses list "): _validate(command, "impulses-list"); return _rows(_path(command), "impulses", "impulses", command)
        if command == "events validate-schema" or command.startswith("events validate-schema "): return _events_validate_schema(_path(command), command)
        if command == "impulse-types list" or command.startswith("impulse-types list "): _validate(command, "impulse-types-list"); return _rows(_path(command), "impulse-types", "impulse_types", command)
        if command == "impulse-relations list" or command.startswith("impulse-relations list "): _validate(command, "impulse-relations-list"); return _rows(_path(command), "impulse-relations", "impulse_relations", command)
        if command == "relations list" or command.startswith("relations list "): _validate(command, "impulse-relations-list"); return _rows(_path(command), "relations", "impulse_relations", command)
        if command == "associations list" or command.startswith("associations list "): _validate(command, "associations-list"); return _rows(_path(command), "associations", "associations", command)
        if command == "reactions list" or command.startswith("reactions list "): _validate(command, "reactions-list"); return _rows(_path(command), "reactions", "reactions", command)
        if command == "homeostats list" or command.startswith("homeostats list "): _validate(command, "homeostats-list"); return _rows(_path(command), "homeostats", "homeostats", command)
        if command == "bridge list" or command.startswith("bridge list "):
            _validate(command, "bridge-list")
            var bridge_table = "bridge_outbox"
            if _flag(command, "--box", "outbox") == "inbox": bridge_table = "bridge_inbox"
            return _rows(_path(command), "bridge", bridge_table, command)
        if command == "bridges list" or command.startswith("bridges list "):
            _validate(command, "bridge-list")
            var bridges_table = "bridge_outbox"
            if _flag(command, "--box", "outbox") == "inbox": bridges_table = "bridge_inbox"
            return _rows(_path(command), "bridges", bridges_table, command)
        if command == "export" or command.startswith("export ") or command.startswith("export-html") or command.startswith("export-bundle"):
            return _error("native_boundary", "export requires a native file encoder")
        if command == "doctor" or command.startswith("doctor "):
            var doctor_path = _path(command)
            if _has_option(command, "--ensure-schema"): _ = initialize_database(doctor_path)
            return _status(doctor_path)
        return _error("unsupported_command")
    except err:
        var detail = String(err)
        if detail.find("unsafe_path") >= 0: return _error("unsafe_path", "invalid database path")
        if detail.find("invalid_json") >= 0: return _error("invalid_json", "invalid JSON input")
        if detail.find("argument_error") >= 0 or detail.find("Invalid value '") >= 0: return _error("argument_error", detail)
        return _error("storage_error", "native command failed")


from .native_cli_help import cli_surface_help

def dispatch_command(command: String) raises -> String:
    return dispatch_native_command(command)

