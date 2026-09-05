"""Deterministic, read-only explanations derived from graph and journal facts."""

from emberjson import Value, to_string
from std.collections import List
from fala.json import canonical_json_text, quote_json_string as quote
from fala.package import load_package_json, load_package_toml
from fala.native_package import PackageManifest, PackageCorrelationPath
from fala.sqlite import Connection, Statement
from fala.schema import initialize_native_schema


def _load(path: String) raises -> PackageManifest:
    if path.endswith(".toml"): return load_package_toml(path)
    return load_package_json(path)


def _text(mut statement: Statement, index: Int) raises -> String:
    if statement.column_null(index): return ""
    return statement.column_text(index)


def _effector_id(metadata: Value) raises -> String:
    if metadata.is_object() and "effector_id" in metadata.object() and metadata.object()["effector_id"].is_string(): return metadata.object()["effector_id"].string()
    return ""


def _array_strings(value: Value) -> List[String]:
    var result = List[String]()
    if value.is_array():
        for item in value.array():
            if item.is_string(): result.append(item.string())
    return result^


def _select(value: Value, path: String) raises -> Value:
    var current = value.copy()
    for raw_segment in path.split("."):
        var segment = String(raw_segment)
        if not current.is_object() or segment not in current.object(): return Value()
        var next = current.object()[segment].copy()
        current = next^
    return current^


def _process_id(run_id: String, path_id: String, effector_id: String) -> String:
    return run_id + ":" + path_id + ":" + effector_id


def _find_path(manifest: PackageManifest, id: String) raises -> PackageCorrelationPath:
    for path in manifest.correlation_paths:
        if path.id == id: return path.copy()
    raise Error("explain.not_declared: correlation path is not declared")


def explain_run(db_path: String, package_path: String, run_id: String, process_filter: String = "", terminal_filter: String = "") raises -> String:
    """Explain process routing without exposing complete inputs, outputs, or secrets."""
    var manifest = _load(package_path)
    var connection = Connection(db_path)
    initialize_native_schema(connection)
    var run = connection.query("SELECT correlation_path_id FROM runs WHERE id=?")
    run.bind_text(1, run_id)
    if not run.step():
        run.close(); connection.close()
        return "{\"explanations\":[],\"ok\":false,\"reason\":\"not_materialized\",\"run_id\":" + quote(run_id) + "}"
    var path_id = _text(run, 0)
    run.close()
    var graph = _find_path(manifest, path_id)
    var selected_effector = process_filter
    if terminal_filter != "":
        selected_effector = ""
        for terminal in graph.terminals:
            if terminal.id == terminal_filter: selected_effector = terminal.source_effector
        if selected_effector == "":
            connection.close()
            return "{\"explanations\":[],\"ok\":false,\"reason\":\"not_declared\",\"terminal\":" + quote(terminal_filter) + "}"
    var rows = connection.query("SELECT id,status,attempt,max_attempts,available_at,lease_owner,lease_expires_at,input_json,output_json,error_json,metadata FROM processes WHERE run_id=? ORDER BY id ASC")
    rows.bind_text(1, run_id)
    var items = String("[")
    var first = True
    var matched = False
    while rows.step():
        var process_id = _text(rows, 0)
        var status = _text(rows, 1)
        var metadata = Value(parse_string=_text(rows, 10))
        var effector_id = _effector_id(metadata)
        if process_filter != "" and process_filter != process_id and process_filter != effector_id: continue
        if selected_effector != "" and selected_effector != effector_id: continue
        matched = True
        var reason = String("not_ready")
        if status == "waiting": reason = "waiting"
        elif status == "succeeded" or status == "failed" or status == "cancelled" or status == "timed_out": reason = "terminal"
        elif status == "skipped": reason = "condition_not_met"
        elif status == "ready" or status == "running": reason = status
        elif status == "retry_wait": reason = "not_ready"
        var conduction = List[String]()
        var when = Value()
        if metadata.is_object():
            if "__correlation_conduction" in metadata.object(): conduction = _array_strings(metadata.object()["__correlation_conduction"].copy())
            if "__correlation_when" in metadata.object(): when = metadata.object()["__correlation_when"].copy()
        var upstream = String("[")
        var upstream_first = True
        var missing = String("[")
        var missing_first = True
        for dependency in conduction:
            var dependency_id = _process_id(run_id, path_id, dependency)
            var source = connection.query("SELECT status FROM processes WHERE run_id=? AND id=?")
            source.bind_text(1, run_id); source.bind_text(2, dependency_id)
            var source_status = String("not_materialized")
            if source.step(): source_status = _text(source, 0)
            source.close()
            if not upstream_first: upstream += ","
            upstream_first = False
            upstream += "{\"effector_id\":" + quote(dependency) + ",\"process_id\":" + quote(dependency_id) + ",\"status\":" + quote(source_status) + "}"
            if source_status != "succeeded" and source_status != "failed" and source_status != "skipped" and source_status != "cancelled" and source_status != "timed_out":
                if not missing_first: missing += ","
                missing_first = False; missing += quote(dependency)
                if reason == "not_ready" and source_status == "failed": reason = "upstream_failed"
        upstream += "]"; missing += "]"
        var condition = String("null")
        if when.is_object() and "upstream" in when.object() and "path" in when.object() and "equals" in when.object():
            var source_effector = when.object()["upstream"].string()
            var json_path = when.object()["path"].string()
            var source = connection.query("SELECT status,output_json FROM processes WHERE run_id=? AND id=?")
            source.bind_text(1, run_id); source.bind_text(2, _process_id(run_id, path_id, source_effector))
            var observed = Value(); var source_status = String("not_materialized")
            if source.step():
                source_status = _text(source, 0)
                if source_status == "succeeded": observed = _select(Value(parse_string=_text(source, 1)), json_path)
            source.close()
            var observed_json = "null" if observed.is_null() else canonical_json_text(to_string(observed))
            condition = "{\"expected\":" + canonical_json_text(to_string(when.object()["equals"].copy())) + ",\"observed\":" + observed_json + ",\"path\":" + quote(json_path) + ",\"source_effector\":" + quote(source_effector) + ",\"source_status\":" + quote(source_status) + "}"
            if source_status == "failed" or source_status == "cancelled" or source_status == "timed_out": reason = "upstream_failed"
            elif source_status == "skipped" or observed_json != canonical_json_text(to_string(when.object()["equals"].copy())): reason = "condition_not_met"
        var events = String("["); var event_first = True
        var event_rows = connection.query("SELECT id FROM runtime_events WHERE run_id=? AND process_id=? ORDER BY sequence ASC")
        event_rows.bind_text(1, run_id); event_rows.bind_text(2, process_id)
        while event_rows.step():
            if not event_first: events += ","
            event_first = False; events += quote(_text(event_rows, 0))
        event_rows.close(); events += "]"
        if not first: items += ","
        first = False
        items += "{\"attempt\":" + String(rows.column_int(2)) + ",\"condition\":" + condition + ",\"effector_id\":" + quote(effector_id) + ",\"event_ids\":" + events + ",\"lease\":{\"expires_at\":" + ("null" if _text(rows, 6) == "" else quote(_text(rows, 6))) + ",\"owner\":" + ("null" if _text(rows, 5) == "" else quote(_text(rows, 5))) + "},\"max_attempts\":" + String(rows.column_int(3)) + ",\"missing\":" + missing + ",\"process_id\":" + quote(process_id) + ",\"reason\":" + quote(reason) + ",\"status\":" + quote(status) + ",\"upstream\":" + upstream + "}"
    rows.close(); connection.close(); items += "]"
    if not matched and process_filter != "": return "{\"explanations\":[],\"ok\":false,\"reason\":\"not_materialized\",\"run_id\":" + quote(run_id) + "}"
    return canonical_json_text("{\"explanations\":" + items + ",\"ok\":true,\"run_id\":" + quote(run_id) + "}")
