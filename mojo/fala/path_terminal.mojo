"""Shared production/rehearsal selection of an explicit typed path result."""
from emberjson import Value, to_string
from fala.native_package import PackageCorrelationPath
from fala.journal import ProcessRow, validate_json_schema_value
from fala.json import json_values_equal


@fieldwise_init
struct PathTerminalResult(Copyable, Movable):
    var id: String
    var values_json: String
    var evidence_json: String


def select_path_terminal(path: PackageCorrelationPath, rows: List[ProcessRow], process_prefix: String, require_match: Bool = True) raises -> PathTerminalResult:
    var result = PathTerminalResult(id="", values_json="{}", evidence_json="[]")
    var count = 0
    for terminal in path.terminals:
        for row in rows:
            if row.id != process_prefix + ":" + terminal.source_effector or row.status != terminal.status: continue
            var envelope = Value(parse_string=row.output_json if terminal.status == "succeeded" or terminal.status == "skipped" else row.error_json)
            var values = envelope.copy()
            if envelope.is_object() and "kind" in envelope.object() and envelope.object()["kind"].is_string() and envelope.object()["kind"].string() == "result" and "payload" in envelope.object():
                values = envelope.object()["payload"].copy()
            if terminal.when_json != "":
                var condition = Value(parse_string=terminal.when_json)
                var current = values.copy()
                for segment in condition.object()["path"].string().split("."):
                    var key = String(segment)
                    if not current.is_object() or key not in current.object():
                        raise Error("path.terminal.missing_field at /terminals/" + terminal.id + "/when: " + key)
                    var next = current.object()[key].copy()
                    current = next^
                if not json_values_equal(current, condition.object()["equals"]): continue
            validate_json_schema_value(values, Value(parse_string=terminal.output_schema_json), "/path_result/values")
            count += 1
            result.id = terminal.id
            result.values_json = to_string(values)
            if envelope.is_object() and "payload" in envelope.object() and envelope.object()["payload"].is_object() and "evidence" in envelope.object()["payload"].object() and envelope.object()["payload"].object()["evidence"].is_array():
                result.evidence_json = to_string(envelope.object()["payload"].object()["evidence"])
    if count > 1: raise Error("path.terminal.ambiguous: ambiguous declared path terminals")
    if count == 0 and len(path.terminals) > 0 and require_match: raise Error("path.terminal.missing: no declared path terminal matched")
    return result^
