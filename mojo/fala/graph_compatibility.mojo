"""Compatibility classification for active-run graph resume."""

from emberjson import Value
from fala.graph_tools import graph_diff, graph_fingerprint
from fala.json import canonical_json_text, quote_json_string as quote


def classify_graph_change(before_path: String, after_path: String) raises -> String:
    var diff = graph_diff(before_path, after_path)
    var value = Value(parse_string=diff)
    var classification = String("compatible_additive")
    if value.object()["equal"].bool() and graph_fingerprint(before_path) == graph_fingerprint(after_path): classification = "identical"
    for change in value.object()["changes"].array():
        var kind = change.object()["kind"].string()
        if kind == "node_removed" or kind == "path_removed" or kind == "terminal_removed" or kind == "edges_changed" or kind == "condition_changed" or kind == "capability_changed" or kind == "adapter_changed": classification = "forbidden_for_active_run"
        elif classification != "forbidden_for_active_run" and (kind == "terminal_added" or kind == "terminal_changed" or kind == "retry_changed" or kind == "timeout_changed" or kind == "runtime_policy_changed"): classification = "incompatible"
    return canonical_json_text("{\"classification\":" + quote(classification) + ",\"diff\":" + diff + ",\"new_fingerprint\":" + quote(graph_fingerprint(after_path)) + ",\"old_fingerprint\":" + quote(graph_fingerprint(before_path)) + "}")


def assert_resume_compatible(durable_fingerprint: String, current_path: String, before_path: String = "") raises -> String:
    var current = graph_fingerprint(current_path)
    if current == durable_fingerprint: return "identical"
    if before_path == "": raise Error("graph.resume_mismatch: durable fingerprint differs; restart required")
    var report = Value(parse_string=classify_graph_change(before_path, current_path))
    var classification = report.object()["classification"].string()
    if classification != "compatible_additive": raise Error("graph.resume_mismatch: " + classification)
    return classification
