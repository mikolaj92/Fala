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
from fala.graph_tools import graph_fingerprint, graph_validate
from fala.output_variants import finite_output_variants
from fala.json import json_values_equal


@fieldwise_init
struct RehearsalScenarioResult(Copyable, Movable):
    var id: String
    var run_id: String
    var terminal: String
    var status: String
    var ticks: Int
    var event_order_json: String


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


def _scenarios(fixture: Value) raises -> List[Value]:
    """Normalize the existing single-fixture form and the scenario form."""
    var result = List[Value]()
    if "scenarios" not in fixture.object():
        result.append(fixture.copy())
        return result^
    var value = fixture.object()["scenarios"].copy()
    if not value.is_array() or len(value.array()) == 0:
        raise Error("rehearsal.fixture at /scenarios: expected nonempty array")
    var index = 0
    for item in value.array():
        if not item.is_object():
            raise Error("rehearsal.fixture at /scenarios/" + String(index) + ": expected object")
        if "id" not in item.object() or not item.object()["id"].is_string() or item.object()["id"].string() == "":
            raise Error("rehearsal.fixture at /scenarios/" + String(index) + "/id: required nonempty string")
        result.append(item.copy())
        index += 1
    return result^


def _scenario_id(fixture: Value) raises -> String:
    if "id" in fixture.object() and fixture.object()["id"].is_string():
        return fixture.object()["id"].string()
    return "default"


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


def _fixture_payload(outcome: Value) raises -> Value:
    """Return the domain payload represented by a fixture result outcome."""
    if "output" not in outcome.object(): return Value(parse_string="{}")
    var payload = outcome.object()["output"].copy()
    if payload.is_object() and "protocol" in payload.object() and "message_kind" in payload.object() and "values" in payload.object() and payload.object()["values"].is_object():
        return payload.object()["values"].copy()
    return payload^


def _scenario_covers(
    fixture: Value,
    effector: String,
    capability: String,
    field_path: String,
    wanted_json: String,
) raises -> Bool:
    if "effectors" not in fixture.object() or not fixture.object()["effectors"].is_object(): return False
    var fixture_id = effector
    if fixture_id not in fixture.object()["effectors"].object() and capability != "" and capability in fixture.object()["effectors"].object(): fixture_id = capability
    if fixture_id not in fixture.object()["effectors"].object(): return False
    var sequence = fixture.object()["effectors"].object()[fixture_id].copy()
    if not sequence.is_array(): return False
    var wanted = Value(parse_string=wanted_json)
    for item in sequence.array():
        if not item.is_object() or "kind" not in item.object() or not item.object()["kind"].is_string() or item.object()["kind"].string() != "result": continue
        var payload = _fixture_payload(item.copy())
        if not payload.is_object(): continue
        var current = payload.copy()
        var found = True
        for segment_slice in field_path.split("."):
            var segment = String(segment_slice)
            if not current.is_object() or segment not in current.object():
                found = False
                break
            var next = current.object()[segment].copy()
            current = next^
        if found and json_values_equal(current, wanted): return True
    return False


def _fixture_missing_variants(
    path: PackageCorrelationPath,
    fixtures: List[Value],
) raises -> String:
    var missing = String("[")
    var first = True
    for effector in path.effectors:
        if effector.contract_mode == "legacy": continue
        var schema = Value(parse_string=effector.output_schema_json)
        var variants = finite_output_variants(schema)
        if not variants.proven: continue
        for wanted in variants.values_json:
            var covered = False
            for fixture in fixtures:
                if _scenario_covers(fixture.copy(), effector.id, effector.capability, variants.field_path, wanted):
                    covered = True
                    break
            if covered: continue
            if not first: missing += ","
            missing += "{\"effector\":" + quote(effector.id) + ",\"field\":" + quote(variants.field_path) + ",\"variant\":" + wanted + ",\"reason\":\"fixture_variant_missing\"}"
            first = False
    missing += "]"
    return missing^


def _scenario_json(result: RehearsalScenarioResult) -> String:
    return "{\"id\":" + quote(result.id) + ",\"run_id\":" + quote(result.run_id) + ",\"status\":" + quote(result.status) + ",\"terminal\":" + ("null" if result.terminal == "" else quote(result.terminal)) + ",\"ticks\":" + String(result.ticks) + ",\"event_order\":" + result.event_order_json + "}"


def _rehearse_scenario(
    manifest: PackageManifest,
    path: PackageCorrelationPath,
    fixture: Value,
    fingerprint: String,
    journal_path: String,
    run_id: String,
) raises -> RehearsalScenarioResult:
    var specs = List[CorrelationEffectorSpec]()
    var max_attempts = Dict[String, Int]()
    for effector in path.effectors:
        specs.append(CorrelationEffectorSpec.create(effector.id, effector.capability, effector.conduction.copy(), effector.timeout_seconds, effector.config_json, effector.output_schema_json, "{\"retry_policy\":\"" + effector.retry_policy + "\"}", List[String](), effector.when_json).copy())
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
                var output = _fixture_payload(outcome.copy())
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
    journal.close()
    return RehearsalScenarioResult(id=_scenario_id(fixture), run_id=run_id, terminal=terminal, status=status, ticks=ticks, event_order_json=order)


def _terminal(path: PackageCorrelationPath, rows: List[ProcessRow]) raises -> String:
    from fala.path_terminal import select_path_terminal
    var all_terminal = True
    for row in rows:
        if row.status not in ["succeeded", "skipped", "failed", "cancelled", "timed_out"]: all_terminal = False
    return select_path_terminal(path, rows, path.id, require_match=all_terminal).id


def rehearse_graph(package_path: String, fixture_path: String, path_id: String, journal_path: String, report_path: String, run_id: String = "rehearsal") raises -> String:
    var manifest = _load(package_path)
    var path = _selected(manifest, path_id)
    var fixture = _object(fixture_path)
    var fingerprint = graph_fingerprint(package_path)
    if "fingerprint" in fixture.object():
        if not fixture.object()["fingerprint"].is_string() or fixture.object()["fingerprint"].string() != fingerprint: raise Error("rehearsal.assertion at /fingerprint: expanded graph fingerprint differs")

    # The graph checker is deliberately before journal creation and before any
    # fixture outcome is consumed.  Rehearsal must not make an invalid graph
    # look safe merely because its happy-path fixture happens to pass.
    var graph_report = graph_validate(package_path)
    if graph_report.find("\"valid\":false") >= 0:
        var invalid_report = canonical_json_text("{\"adapter_policy\":\"fixtures_only\",\"fingerprint\":" + quote(fingerprint) + ",\"graph_validation\":" + graph_report + ",\"journal\":" + quote(journal_path) + ",\"missing\":[],\"ok\":false,\"run_id\":" + quote(run_id) + ",\"status\":\"not_run\",\"terminal\":null,\"ticks\":0}")
        Path(report_path).write_text(invalid_report + "\n")
        return invalid_report

    var fixtures = _scenarios(fixture^)
    var missing = _fixture_missing_variants(path, fixtures)
    if missing != "[]":
        var missing_report = canonical_json_text("{\"adapter_policy\":\"fixtures_only\",\"coverage_guaranteed\":false,\"fingerprint\":" + quote(fingerprint) + ",\"journal\":" + quote(journal_path) + ",\"missing\":" + missing + ",\"ok\":false,\"run_id\":" + quote(run_id) + ",\"status\":\"not_run\",\"terminal\":null,\"ticks\":0}")
        Path(report_path).write_text(missing_report + "\n")
        return missing_report

    var scenario_reports = String("[")
    var scenario_first = True
    var total_ticks = 0
    var all_completed = True
    var any_failed = False
    var first_terminal = String("")
    var first_order = String("[]")
    for index in range(len(fixtures)):
        var scenario = fixtures[index].copy()
        var scenario_id = _scenario_id(scenario)
        var scenario_run_id = run_id
        if len(fixtures) > 1: scenario_run_id = run_id + ":" + scenario_id
        var result = _rehearse_scenario(manifest, path, scenario^, fingerprint, journal_path, scenario_run_id)
        if not scenario_first: scenario_reports += ","
        scenario_first = False
        scenario_reports += _scenario_json(result)
        total_ticks += result.ticks
        if result.status != "completed": all_completed = False
        if result.status == "failed": any_failed = True
        if index == 0:
            first_terminal = result.terminal
            first_order = result.event_order_json
    scenario_reports += "]"
    var status = "completed"
    if any_failed: status = "failed"
    elif not all_completed: status = "waiting"
    var event_order = first_order
    if len(fixtures) > 1: event_order = scenario_reports
    var terminal = first_terminal if len(fixtures) == 1 else ""
    var graph_guaranteed = graph_report.find("\"coverage_guaranteed\":true") >= 0
    var report = canonical_json_text("{\"adapter_policy\":\"fixtures_only\",\"coverage_guaranteed\":" + ("true" if graph_guaranteed else "false") + ",\"event_order\":" + event_order + ",\"fingerprint\":" + quote(fingerprint) + ",\"journal\":" + quote(journal_path) + ",\"missing\":[],\"ok\":true,\"run_id\":" + quote(run_id) + ",\"scenarios\":" + scenario_reports + ",\"status\":" + quote(status) + ",\"terminal\":" + ("null" if terminal == "" else quote(terminal)) + ",\"ticks\":" + String(total_ticks) + "}")
    Path(report_path).write_text(report + "\n")
    return report
