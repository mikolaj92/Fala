"""Native CLI run, process, and domain lifecycle mutations."""
from std.collections import List
from std.pathlib import Path
from emberjson import Value, Object, to_string
from fala.journal import RunRow, NativeJournal, EventInput, ProcessRow
from fala.sqlite import SQLiteError
from fala.json import parse_json, canonical_json_text, quote_json_string as _quote
from fala.domain import Impulse, Association, Reaction
from fala.domain_store import NativeDomainStore
from fala.reactions import FileReactionStore, ReactionBlob
from fala.runs import RunLifecycle
from fala.native_cli_ops import _error
from fala.native_cli_parse import (
    _safe, _flag, _has_option, _path, _json, _metadata_value,
    _repeat_values, _integer_option,
)


def _impulse_create(command: String) raises -> String:
    var run_id = _flag(command, "--run-id"); var impulse_id = _flag(command, "--impulse-id"); var impulse_type = _flag(command, "--impulse-type")
    if run_id == "" or impulse_id == "" or impulse_type == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id, --impulse-id, and --impulse-type are required")))
    var payload = _flag(command, "--payload", "{}"); var metadata = _flag(command, "--metadata", "{}"); _json(payload); _json(metadata)
    var now = _flag(command, "--now"); var key = _flag(command, "--idempotency-key", "impulse.accept:" + impulse_id)
    var row = Impulse(id=impulse_id, run_id=run_id, impulse_type=impulse_type, payload=payload, metadata=metadata, created_at=now, updated_at=now)
    var store = NativeDomainStore.open(_path(command)); store.initialize(); var accepted = store.accept_impulse(row, key, now, _flag(command, "--actor"), _flag(command, "--correlation-id"), _flag(command, "--causation-id")); store.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"impulse\",\"id\":" + _quote(accepted.impulse.id) + ",\"run_id\":" + _quote(run_id) + ",\"replayed\":" + ("true" if accepted.replayed else "false") + ",\"command_id\":" + _quote(accepted.command.id) + ",\"event_id\":" + _quote(accepted.events[0].id) + "}"


def _process_schedule(command: String) raises -> String:
    var run_id = _flag(command, "--run-id"); var process_id = _flag(command, "--process-id"); var process_type = _flag(command, "--process-type")
    if run_id == "" or process_id == "" or process_type == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id, --process-id, and --process-type are required")))
    var input_json = _flag(command, "--input", "{}"); var metadata = _flag(command, "--metadata", "{}"); var output_schema = _flag(command, "--output-schema", "{}"); _json(input_json); _json(metadata); _json(output_schema)
    var now = _flag(command, "--now"); var journal = NativeJournal.open(_path(command)); journal.initialize()
    var row = journal.schedule_process(run_id, process_id, process_type, now, input_json, metadata, _flag(command, "--impulse-id"), _integer_option(command, "--priority", 0), _integer_option(command, "--max-attempts", 1), _flag(command, "--available-at", now), output_schema, _flag(command, "--idempotency-key", "process.schedule:" + process_id), _flag(command, "--actor")); journal.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"process\",\"id\":" + _quote(row.id) + ",\"run_id\":" + _quote(row.run_id) + ",\"status\":" + _quote(row.status) + "}"


def _process_transition(command: String, target: String) raises -> String:
    var run_id = _flag(command, "--run-id"); var process_id = _flag(command, "--process-id"); var actor = _flag(command, "--actor", "cli"); var now = _flag(command, "--now")
    if run_id == "" or process_id == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id and --process-id are required")))
    var error_json = _flag(command, "--error", "{}"); _json(error_json); var journal = NativeJournal.open(_path(command)); journal.initialize(); var row = ProcessRow(run_id="", id="", process_type="", impulse_id="", status="", priority=0, attempt=0, max_attempts=1, available_at="", lease_owner="", lease_expires_at="", input_json="{}", output_json="{}", error_json="{}", metadata="{}", created_at="", updated_at="", started_at="", finished_at="", output_schema_json="{}")
    if target == "cancel": row = journal.cancel_process(run_id, process_id, actor, now, error_json)
    else: row = journal.timeout_process(run_id, process_id, actor, now, error_json)
    journal.close(); return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"process\",\"id\":" + _quote(row.id) + ",\"run_id\":" + _quote(row.run_id) + ",\"status\":" + _quote(row.status) + "}"


def _association_append(command: String) raises -> String:
    var run_id = _flag(command, "--run-id"); var association_id = _flag(command, "--association-id"); var kind = _flag(command, "--kind")
    if run_id == "" or association_id == "" or kind == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id, --association-id, and --kind are required")))
    var values = _flag(command, "--values", "{}"); var metadata = _flag(command, "--metadata", "{}"); _json(values); _json(metadata)
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
    var metadata_raw = _flag(command, "--metadata", "{}")
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
    var output = _flag(command, "--output", "{}"); var error_json = _flag(command, "--error", "{}"); var metadata = _flag(command, "--metadata", "{}"); _json(output); _json(error_json); _json(metadata)
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
        var values = _flag(command, "--values", "{}")
        var metadata = _flag(command, "--metadata", "{}")
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
