"""Pure authoring tools for validated, fully materialized Fala graphs.

These functions only read package manifests. They never open a journal or invoke
an adapter, so graph review is safe before execution.
"""

from emberjson import Value, to_string
from std.pathlib import Path
from fala.json import canonical_json_text, quote_json_string
from fala.package import load_package_json, load_package_toml
from fala.native_package import PackageManifest, PackageCorrelationPath, PackageEffector, serialize_package_json
from fala.output_variants import finite_output_variants
from fala.reactions import sha256_bytes


def _load(path: String) raises -> PackageManifest:
    if path.endswith(".toml"): return load_package_toml(path)
    return load_package_json(path)


def graph_expand(path: String) raises -> String:
    """Return canonical JSON after bounded template materialization."""
    return serialize_package_json(_load(path))


def graph_fingerprint(path: String) raises -> String:
    """Fingerprint the canonical expanded graph, contracts, and policies."""
    return "sha256:" + sha256_bytes(graph_expand(path))


def _change(mut text: String, mut first: Bool, kind: String, path: String):
    if not first: text += ","
    text += "{\"kind\":" + quote_json_string(kind) + ",\"path\":" + quote_json_string(path) + "}"
    first = False


def _find_effector(path: PackageCorrelationPath, id: String) -> Int:
    for i in range(len(path.effectors)):
        if path.effectors[i].id == id: return i
    return -1


def _find_path(manifest: PackageManifest, id: String) -> Int:
    for i in range(len(manifest.correlation_paths)):
        if manifest.correlation_paths[i].id == id: return i
    return -1


def _same_strings(a: List[String], b: List[String]) -> Bool:
    if len(a) != len(b): return False
    for item in a:
        var found = False
        for other in b:
            if item == other: found = True
        if not found: return False
    return True


def _find_terminal(path: PackageCorrelationPath, id: String) -> Int:
    for i in range(len(path.terminals)):
        if path.terminals[i].id == id: return i
    return -1


def graph_diff(before_path: String, after_path: String) raises -> String:
    """Return stable semantic change classes rather than a textual diff."""
    var before = _load(before_path)
    var after = _load(after_path)
    var changes = String("[")
    var first = True
    for old_path in before.correlation_paths:
        var path_index = _find_path(after, old_path.id)
        if path_index < 0:
            _change(changes, first, "path_removed", "/correlation_paths/" + old_path.id)
            continue
        var new_path = after.correlation_paths[path_index].copy()
        for old in old_path.effectors:
            var index = _find_effector(new_path, old.id)
            var pointer = "/correlation_paths/" + old_path.id + "/effectors/" + old.id
            if index < 0:
                _change(changes, first, "node_removed", pointer)
                continue
            var new = new_path.effectors[index].copy()
            if not _same_strings(old.conduction, new.conduction): _change(changes, first, "edges_changed", pointer + "/conduction")
            if old.when_json != new.when_json: _change(changes, first, "condition_changed", pointer + "/when")
            if old.capability != new.capability: _change(changes, first, "capability_changed", pointer + "/capability")
            if old.retry_policy != new.retry_policy: _change(changes, first, "retry_changed", pointer + "/retry_policy")
            if old.timeout_seconds != new.timeout_seconds: _change(changes, first, "timeout_changed", pointer + "/timeout_seconds")
            if old.adapter_kind != new.adapter_kind or old.adapter_ref != new.adapter_ref: _change(changes, first, "adapter_changed", pointer + "/adapter")
        for new in new_path.effectors:
            if _find_effector(old_path, new.id) < 0: _change(changes, first, "node_added", "/correlation_paths/" + old_path.id + "/effectors/" + new.id)
        for old in old_path.terminals:
            var index = _find_terminal(new_path, old.id)
            var pointer = "/correlation_paths/" + old_path.id + "/terminals/" + old.id
            if index < 0: _change(changes, first, "terminal_removed", pointer)
            elif old.source_effector != new_path.terminals[index].source_effector or old.status != new_path.terminals[index].status or old.when_json != new_path.terminals[index].when_json or old.output_schema_json != new_path.terminals[index].output_schema_json:
                _change(changes, first, "terminal_changed", pointer)
        for new in new_path.terminals:
            if _find_terminal(old_path, new.id) < 0: _change(changes, first, "terminal_added", "/correlation_paths/" + old_path.id + "/terminals/" + new.id)
    for new_path in after.correlation_paths:
        if _find_path(before, new_path.id) < 0: _change(changes, first, "path_added", "/correlation_paths/" + new_path.id)
    if before.capabilities_json != after.capabilities_json: _change(changes, first, "capability_contracts_changed", "/capabilities")
    if before.runtime_json != after.runtime_json: _change(changes, first, "runtime_policy_changed", "/runtime")
    changes += "]"
    return canonical_json_text("{\"changes\":" + changes + ",\"equal\":" + ("true" if first else "false") + "}")




def _variant_covered(path: PackageCorrelationPath, effector: PackageEffector, value: String) -> Bool:
    for candidate in path.effectors:
        if effector.id in candidate.conduction:
            if candidate.when_json == "": return True
            if candidate.when_json.find(value) >= 0: return True
    for terminal in path.terminals:
        if terminal.source_effector == effector.id:
            if terminal.when_json == "": return True
            if terminal.when_json.find(value) >= 0: return True
    return False


def _coverage_diagnostic(path_index: Int, graph: PackageCorrelationPath, effector_index: Int, effector: PackageEffector) raises -> String:
    if not effector.output_schema_declared:
        return "{\"code\":\"contract_coverage_missing\",\"message\":\"output contract is required\",\"path\":\"/correlation_paths/" + String(path_index) + "/effectors/" + String(effector_index) + "\",\"effector\":" + quote_json_string(effector.id) + "}"
    var schema = Value(parse_string=effector.output_schema_json)
    if not schema.is_object() or len(schema.object()) == 0:
        return "{\"code\":\"contract_coverage_missing\",\"message\":\"a non-empty output_schema is required; {} is not a contract\",\"path\":\"/correlation_paths/" + String(path_index) + "/effectors/" + String(effector_index) + "/output_schema\",\"effector\":" + quote_json_string(effector.id) + "}"
    var variants = finite_output_variants(schema)
    if not variants.proven: return ""
    for value in variants.values_json:
        if not _variant_covered(graph, effector, value):
            return "{\"code\":\"contract_coverage_missing\",\"message\":\"output variant is not covered\",\"path\":\"/correlation_paths/" + String(path_index) + "/effectors/" + String(effector_index) + "/output_schema\",\"effector\":" + quote_json_string(effector.id) + ",\"field\":" + quote_json_string(variants.field_path) + ",\"variant\":" + quote_json_string(value) + "}"
    return ""


def _coverage_report(manifest: PackageManifest) raises -> String:
    var diagnostics = String("[")
    var first = True
    var guaranteed = True
    var unverified = String("[")
    var unverified_first = True
    for path_index in range(len(manifest.correlation_paths)):
        var graph = manifest.correlation_paths[path_index].copy()
        for effector_index in range(len(graph.effectors)):
            var effector = graph.effectors[effector_index].copy()
            var needs_verification = True
            if effector.output_schema_declared:
                var declared_schema = Value(parse_string=effector.output_schema_json)
                var declared_variants = finite_output_variants(declared_schema)
                needs_verification = not declared_variants.proven
            if needs_verification:
                guaranteed = False
                if not unverified_first: unverified += ","
                unverified += quote_json_string(effector.id)
                unverified_first = False
            var diagnostic = _coverage_diagnostic(path_index, graph, effector_index, effector)
            if diagnostic != "":
                if not first: diagnostics += ","
                diagnostics += diagnostic
                first = False
    diagnostics += "]"
    unverified += "]"
    return "{\"diagnostics\":" + diagnostics + ",\"guaranteed\":" + ("true" if guaranteed else "false") + ",\"unverified\":" + unverified + "}"


def graph_validate(path: String) -> String:
    """Return a stable validation report with source JSON pointers."""
    try:
        var manifest = _load(path)
        # Loading performs strict schema, reference, terminal and adapter checks.
        # Additional graph checks below reject dependency cycles and open waits.
        for path_index in range(len(manifest.correlation_paths)):
            var graph = manifest.correlation_paths[path_index].copy()
            for start in range(len(graph.effectors)):
                var seen = List[String]()
                var pending = List[String](graph.effectors[start].conduction)
                while len(pending) != 0:
                    var id = pending.pop()
                    if id == graph.effectors[start].id:
                        return "{\"diagnostics\":[{\"code\":\"graph.cycle\",\"message\":\"dependency cycle\",\"path\":\"/correlation_paths/" + String(path_index) + "/effectors/" + String(start) + "/conduction\"}],\"valid\":false}"
                    if id in seen: continue
                    seen.append(id)
                    var dependency = _find_effector(graph, id)
                    if dependency >= 0:
                        for parent in graph.effectors[dependency].conduction: pending.append(parent)
            for effector_index in range(len(graph.effectors)):
                var effector = graph.effectors[effector_index].copy()
                if effector.adapter_kind != "manual_homeostat": continue
                var closed = False
                for candidate in graph.effectors:
                    if effector.id in candidate.conduction: closed = True
                for terminal in graph.terminals:
                    if terminal.source_effector == effector.id: closed = True
                if not closed:
                    return "{\"diagnostics\":[{\"code\":\"graph.open_wait\",\"message\":\"manual wait has no downstream or terminal closure\",\"path\":\"/correlation_paths/" + String(path_index) + "/effectors/" + String(effector_index) + "/adapter\"}],\"valid\":false}"
        var coverage = _coverage_report(manifest)
        var valid = coverage.find("\"diagnostics\":[]") >= 0
        var guaranteed = coverage.find("\"guaranteed\":true") >= 0
        var unverified_start = coverage.find("\"unverified\":")
        var unverified = "[]"
        if unverified_start >= 0: unverified = String(coverage[byte=unverified_start + 13:coverage.byte_length() - 1])
        if valid: return "{\"diagnostics\":[],\"valid\":true,\"coverage_guaranteed\":" + ("true" if guaranteed else "false") + ",\"unverified\":" + unverified + "}"
        return "{\"diagnostics\":" + coverage + ",\"valid\":false}"
    except err:
        var message = String(err)
        var at = message.find(" at ")
        var colon = message.find(": ")
        var code = message if at < 0 else String(message[byte=0:at])
        var pointer = path if at < 0 else String(message[byte=at + 4:colon])
        var detail = message if colon < 0 else String(message[byte=colon + 2:])
        return "{\"diagnostics\":[{\"code\":" + quote_json_string(code) + ",\"message\":" + quote_json_string(detail) + ",\"path\":" + quote_json_string(pointer) + "}],\"valid\":false}"
