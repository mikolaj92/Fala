"""End-to-end native execution of the authored basic correlation package."""

from std.collections import List
from std.os import remove
from std.pathlib import Path
from emberjson import Object, Value, to_string

from fala import (
    AdapterBinding,
    AdapterSpec,
    CorrelationEffectorSpec,
    CorrelationInputField,
    CorrelationPathSpec,
    NativeFunctionRegistry,
    NativeJournal,
    PackageCorrelationPath,
    PackageManifest,
    instantiate_correlation_path,
    load_package_toml,
    run_correlation_path,
)


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("basic native example smoke: " + message)


def _cleanup(path: String):
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


def _source(input_json: String) raises -> String:
    var value = Value(parse_string=input_json)
    if not value.is_object():
        raise Error("input must be an object")
    if "source" not in value.object():
        return "unknown"
    var source = value.object()["source"].copy()
    if not source.is_string():
        raise Error("source must be a string")
    return source.string()


def _quote(value: String) -> String:
    var escaped = String()
    for ch in value.codepoint_slices():
        if ch == "\\":
            escaped += "\\\\"
        elif ch == "\"":
            escaped += "\\\""
        elif ch == "\n":
            escaped += "\\n"
        elif ch == "\r":
            escaped += "\\r"
        elif ch == "\t":
            escaped += "\\t"
        else:
            escaped += ch
    return "\"" + escaped + "\""


def _length(value: String) -> Int:
    var count = 0
    for _ in value.codepoint_slices():
        count += 1
    return count


def _uppercase(value: String) -> String:
    return value.upper()

def basic_ingest(input_json: String, config_json: String) raises -> String:
    var source = _source(input_json)
    return "{\"chars\":" + String(_length(source)) + ",\"source\":" + _quote(source) + "}"


def basic_enrich(input_json: String, config_json: String) raises -> String:
    var input = Value(parse_string=input_json)
    if not input.is_object() or "conduction" not in input.object():
        raise Error("enrich requires conduction")
    var conduction = input.object()["conduction"].copy()
    if not conduction.is_object() or "ingest" not in conduction.object():
        raise Error("enrich requires ingest conduction")
    var ingest = conduction.object()["ingest"].copy()
    if not ingest.is_object() or "source" not in ingest.object():
        raise Error("ingest conduction requires source")
    var source = ingest.object()["source"].copy()
    if not source.is_string():
        raise Error("ingest source must be a string")
    var text = source.string()
    return "{\"label\":" + _quote(_uppercase(text)) + ",\"source\":" + _quote(text) + "}"


def basic_export(input_json: String, config_json: String) raises -> String:
    var input = Value(parse_string=input_json)
    if not input.is_object() or "conduction" not in input.object():
        raise Error("export requires conduction")
    var conduction = input.object()["conduction"].copy()
    if not conduction.is_object() or "enrich" not in conduction.object():
        raise Error("export requires enrich conduction")
    var enrich = conduction.object()["enrich"].copy()
    if not enrich.is_object() or "label" not in enrich.object():
        raise Error("enrich conduction requires label")
    var label = enrich.object()["label"].copy()
    if not label.is_string():
        raise Error("enrich label must be a string")
    return "{\"label\":" + _quote(label.string()) + ",\"status\":\"ok\"}"


def _adapter_ref(package_path: PackageCorrelationPath, effector_id: String) raises -> String:
    for item in package_path.effectors:
        if item.id == effector_id:
            return item.adapter_ref
    raise Error("adapter ref is missing for " + effector_id)
def _binding_ref(bindings: List[AdapterBinding], process_id: String) raises -> String:
    for binding in bindings:
        if binding.process_id == process_id:
            return binding.adapter.`ref`
    raise Error("adapter binding is missing for " + process_id)

def _selected_path(manifest: PackageManifest) raises -> PackageCorrelationPath:
    for path in manifest.correlation_paths:
        if path.id == "basic_enrichment":
            return path.copy()
    raise Error("basic_enrichment path is missing")


def main() raises:
    var manifest_path = "../../examples/correlation-paths/basic/fala-package.toml"
    _ = Path(manifest_path).read_text()
    var manifest = load_package_toml(manifest_path)
    var package_path = _selected_path(manifest)
    _check(len(package_path.effectors) == 3, "manifest has three effectors")
    _check(_adapter_ref(package_path, "ingest") == "example.basic.ingest", "ingest adapter ref")
    _check(_adapter_ref(package_path, "enrich") == "example.basic.enrich", "enrich adapter ref")
    _check(_adapter_ref(package_path, "export") == "example.basic.export", "export adapter ref")

    var effectors = List[CorrelationEffectorSpec]()
    for item in package_path.effectors:
        _check(item.adapter_kind == "native_function", "manifest uses native adapter for " + item.id)
        var spec = CorrelationEffectorSpec.create(
            item.id,
            item.capability,
            item.conduction.copy(),
            item.timeout_seconds,
            item.config_json,
            "{}",
            "{}",
            List[String](),
        )
        effectors.append(spec^)
    var path = CorrelationPathSpec(
        package_path.id,
        effectors^,
        package_path.allow_feedback_cycles,
        package_path.accumulate_upstream_reactions,
    )
    var inputs = List[CorrelationInputField]()
    inputs.append(CorrelationInputField(key="source", value_json="\"hello native\""))
    var run_id = "basic-example-run"
    var plan = instantiate_correlation_path(path, run_id, input_fields=inputs^)

    var registry = NativeFunctionRegistry()
    registry.register("example.basic.ingest", basic_ingest)
    registry.register("example.basic.enrich", basic_enrich)
    registry.register("example.basic.export", basic_export)
    var bindings = List[AdapterBinding]()
    for index in range(len(plan.processes)):
        bindings.append(AdapterBinding(
            process_id=plan.processes[index].id,
            adapter=AdapterSpec.native_function(_adapter_ref(package_path, plan.processes[index].effector_id)),
            run_id=run_id,
        ))
    _check(len(bindings) == 3, "three adapter bindings")
    _check(_binding_ref(bindings, run_id + ":basic_enrichment:ingest") == "example.basic.ingest", "ingest binding ref")
    _check(_binding_ref(bindings, run_id + ":basic_enrichment:enrich") == "example.basic.enrich", "enrich binding ref")
    _check(_binding_ref(bindings, run_id + ":basic_enrichment:export") == "example.basic.export", "export binding ref")

    var db_path = "/tmp/fala-basic-example-native.sqlite"
    _cleanup(db_path)
    var journal = NativeJournal.open(db_path)
    journal.initialize()
    var first = run_correlation_path(
        journal,
        run_id,
        plan,
        bindings,
        registry,
        "2026-01-01T00:00:00Z",
        "basic-example-worker",
        "2026-01-01T00:00:01Z",
        "2026-01-01T00:01:00Z",
        8,
    )
    _check(not first.replayed and first.run_status == "completed", "initial package run completed")
    _check(first.drive_result.ticks == 3, "three native effectors executed")
    var ingest = journal.get_process(run_id, run_id + ":basic_enrichment:ingest")
    var enrich = journal.get_process(run_id, run_id + ":basic_enrichment:enrich")
    var export = journal.get_process(run_id, run_id + ":basic_enrichment:export")
    _check(ingest.status == "succeeded" and ingest.output_json.find("\"source\":\"hello native\"") >= 0 and ingest.output_json.find("\"chars\":12") >= 0, "ingest output")
    _check(enrich.status == "succeeded" and enrich.output_json.find("\"label\":\"HELLO NATIVE\"") >= 0, "enrich output")
    _check(export.status == "succeeded" and export.output_json.find("\"status\":\"ok\"") >= 0 and export.output_json.find("\"label\":\"HELLO NATIVE\"") >= 0, "export output")
    journal.close()

    var reopened = NativeJournal.open(db_path)
    reopened.initialize()
    var replay_registry = NativeFunctionRegistry()
    replay_registry.register("example.basic.ingest", basic_ingest)
    replay_registry.register("example.basic.enrich", basic_enrich)
    replay_registry.register("example.basic.export", basic_export)
    var replay = run_correlation_path(
        reopened,
        run_id,
        plan,
        bindings,
        replay_registry,
        "2026-01-01T00:00:00Z",
        "basic-example-worker",
        "2026-01-01T00:00:01Z",
        "2026-01-01T00:01:00Z",
        8,
    )
    _check(replay.replayed and replay.run_status == "completed", "terminal package replay")
    _check(replay.drive_result.ticks == 0 and len(reopened.list_processes(run_id)) == 3, "replay is idempotent")
    reopened.close()
    _cleanup(db_path)
    print("basic native example smoke ok: toml registry outputs terminal reopen replay")
