"""Offline whole-graph rehearsal against deterministic fixture outcomes.

The production adapter declarations are never dispatched. Only durable journal
transitions and the normal correlation advancement algorithm are used.
"""

from emberjson import Value, to_string
from std.collections import Dict, List
from std.pathlib import Path
from fala.json import canonical_json_text, quote_json_string as quote
from fala.package import load_package_json, load_package_toml
from fala.native_package import PackageManifest, PackageCorrelationPath
from fala.correlation import CorrelationEffectorSpec, CorrelationPathSpec, CorrelationInputField, instantiate_correlation_path
from fala.correlation_persistence import persist_correlation_plan
from fala.correlation_advance import advance_correlation
from fala.journal import NativeJournal, ProcessRow
from fala.graph_tools import graph_fingerprint
from fala.reactions import sha256_bytes


def _load(path: String) raises -> PackageManifest:
    if path.endswith(".toml"): return load_package_toml(path)
    return load_package_json(path)


def _selected(manifest: PackageManifest, id: String) raises -> PackageCorrelationPath:
    for path in manifest.correlation_paths:
        if path.id == id: return path.copy()
    raise Error("rehearsal.not_declared at /path_id: correlation path is not declared")


def _object(path: String) raises -> Value:
    var value = Value(parse_string=Path(path).read_text())
    if not value.is_object(): raise Error("rehearsal.fixture at /: expected object")
    return value^


def _outcome(fixtures: Value, effector: String, capability: String, attempt: Int) raises -> Value:
    var fixture_id = effector
    if "effectors" not in fixtures.object() or not fixtures.object()["effectors"].is_object(): raise Error("rehearsal.fixture at /effectors: expected object")
    if fixture_id not in fixtures.object()["effectors"].object() and capability != "" and capability in fixtures.object()["effectors"].object(): fixture_id = capability
    if fixture_id not in fixtures.object()["effectors"].object():
        raise Error("rehearsal.fixture at /effectors/" + effector + ": missing effector/capability fixture; production adapter refused")
    var sequence = fixtures.object()["effectors"].object()[fixture_id].copy()
    if not sequence.is_array() or len(sequence.array()) == 0: raise Error("rehearsal.fixture at /effectors/" + effector + ": expected nonempty outcome array")
    var index = attempt
    if index >= len(sequence.array()): index = len(sequence.array()) - 1
    var result = sequence.array()[index].copy()
    if not result.is_object() or "kind" not in result.object() or not result.object()["kind"].is_string(): raise Error("rehearsal.fixture at /effectors/" + effector + "/" + String(index) + ": outcome requires kind")
    return result^


def _terminal(path: PackageCorrelationPath, rows: List[ProcessRow]) -> String:
    for terminal in path.terminals:
        for row in rows:
            if row.id.endswith(":" + terminal.source_effector) and row.status == terminal.status: return terminal.id
    return ""


def rehearse_graph(package_path: String, fixture_path: String, path_id: String, journal_path: String, report_path: String, run_id: String = "rehearsal") raises -> String:
    var manifest = _load(package_path)
    var path = _selected(manifest, path_id)
    var fixture = _object(fixture_path)
    var fingerprint = graph_fingerprint(package_path)
    if "fingerprint" in fixture.object():
        if not fixture.object()["fingerprint"].is_string() or fixture.object()["fingerprint"].string() != fingerprint: raise Error("rehearsal.assertion at /fingerprint: expanded graph fingerprint differs")
    var specs = List[CorrelationEffectorSpec]()
    var max_attempts = Dict[String, Int]()
    for effector in path.effectors:
        specs.append(CorrelationEffectorSpec.create(effector.id, effector.capability, effector.conduction.copy(), effector.timeout_seconds, effector.config_json, "{}", "{\"retry_policy\":\"" + effector.retry_policy + "\"}", List[String](), effector.when_json).copy())
        if "effectors" in fixture.object() and fixture.object()["effectors"].is_object() and effector.id in fixture.object()["effectors"].object() and fixture.object()["effectors"].object()[effector.id].is_array(): max_attempts[effector.id] = len(fixture.object()["effectors"].object()[effector.id].array())
    var inputs = List[CorrelationInputField]()
    if "inputs" in fixture.object() and fixture.object()["inputs"].is_object():
        for pair in fixture.object()["inputs"].object().items(): inputs.append(CorrelationInputField(key=pair.key, value_json=to_string(pair.value.copy())))
    var plan = instantiate_correlation_path(CorrelationPathSpec(path.id, specs^, path.accumulate_upstream_reactions), run_id, correlation_path_id=path.id, input_fields=inputs^, max_attempts_by_effector=max_attempts^)
    var journal = NativeJournal.open(journal_path); journal.initialize()
    _ = journal.create_run(run_id, "created", "{\"rehearsal\":true}", "2026-01-01T00:00:00Z", package_id=manifest.id, package_version=manifest.version, package_digest=fingerprint, correlation_path_id=path.id, correlation_path_digest=fingerprint, runtime_version="rehearsal", backend_version="sqlite")
    _ = persist_correlation_plan(journal, plan, "2026-01-01T00:00:00Z")
    var order = String("["); var order_first = True; var ticks = 0; var changed = True
    while changed and ticks < 1024:
        changed = False
        _ = advance_correlation(journal, plan)
        var rows = journal.list_processes(run_id)
        for row in rows:
            if row.status != "ready" and row.status != "retry_wait": continue
            var effector = String("")
            for item in plan.processes:
                if item.id == row.id: effector = item.effector_id
            var capability = String("")
            for declared in path.effectors:
                if declared.id == effector: capability = declared.capability
            var outcome = _outcome(fixture.copy(), effector, capability, row.attempt)
            var claimed = journal.claim_process(run_id, row.id, "rehearsal", "2026-01-01T00:00:03Z", "2099-01-01T00:00:00Z")
            if not order_first: order += ","
            order_first = False; order += quote(effector + "#" + String(claimed.attempt)); ticks += 1
            var kind = outcome.object()["kind"].string()
            if kind == "result":
                var output = Value(parse_string="{}")
                if "output" in outcome.object(): output = outcome.object()["output"].copy()
                if not output.is_object(): raise Error("rehearsal.malformed_result at /effectors/" + effector + "/output: expected object")
                _ = journal.complete_process(run_id, row.id, "rehearsal", "2026-01-01T00:00:02Z", canonical_json_text(to_string(output)))
            elif kind == "failure" or kind == "timeout":
                var error = "{\"code\":" + quote("fixture_" + kind) + "}"
                if claimed.attempt < claimed.max_attempts: _ = journal.retry_process(run_id, row.id, "rehearsal", "2026-01-01T00:00:02Z", "2026-01-01T00:00:02Z", error)
                elif kind == "timeout": _ = journal.timeout_process(run_id, row.id, "rehearsal", "2026-01-01T00:00:02Z", error)
                else: _ = journal.fail_process(run_id, row.id, "rehearsal", "2026-01-01T00:00:02Z", error)
            elif kind == "wait": _ = journal.wait_process(run_id, row.id, "rehearsal", "2026-01-01T00:00:02Z", "{\"fixture_wait\":true}")
            else: raise Error("rehearsal.fixture at /effectors/" + effector + ": unknown outcome kind")
            changed = True
        if not changed: break
    order += "]"
    _ = advance_correlation(journal, plan)
    var final_rows = journal.list_processes(run_id)
    var terminal = _terminal(path, final_rows)
    var assertions = Value()
    if "assert" in fixture.object(): assertions = fixture.object()["assert"].copy()
    if assertions.is_object() and "terminal" in assertions.object() and assertions.object()["terminal"].string() != terminal: raise Error("rehearsal.assertion at /assert/terminal: expected " + assertions.object()["terminal"].string() + ", observed " + terminal)
    if assertions.is_object() and "forbidden_effectors" in assertions.object() and assertions.object()["forbidden_effectors"].is_array():
        for forbidden in assertions.object()["forbidden_effectors"].array():
            if forbidden.is_string() and order.find("\"" + forbidden.string() + "#") >= 0: raise Error("rehearsal.assertion at /assert/forbidden_effectors: attempted forbidden effector " + forbidden.string())
    if assertions.is_object() and "attempts" in assertions.object() and assertions.object()["attempts"].is_object():
        for expected in assertions.object()["attempts"].object().items():
            var observed = 0
            for row in final_rows:
                if row.id.endswith(":" + expected.key): observed = row.attempt
            if not expected.value.is_int() and not expected.value.is_uint(): raise Error("rehearsal.assertion at /assert/attempts/" + expected.key + ": expected integer")
            var wanted = Int(expected.value.int()) if expected.value.is_int() else Int(expected.value.uint())
            if observed != wanted: raise Error("rehearsal.assertion at /assert/attempts/" + expected.key + ": attempt count differs")
    var status = "completed" if terminal != "" else "waiting"
    for row in final_rows:
        if row.status == "failed" or row.status == "timed_out" or row.status == "cancelled": status = "failed"
    var report = canonical_json_text("{\"adapter_policy\":\"fixtures_only\",\"event_order\":" + order + ",\"fingerprint\":" + quote(fingerprint) + ",\"journal\":" + quote(journal_path) + ",\"ok\":true,\"run_id\":" + quote(run_id) + ",\"status\":" + quote(status) + ",\"terminal\":" + ("null" if terminal == "" else quote(terminal)) + ",\"ticks\":" + String(ticks) + "}")
    journal.close(); Path(report_path).write_text(report + "\n")
    return report
