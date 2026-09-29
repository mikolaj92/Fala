from std.os import remove
from std.pathlib import Path
from std.collections import List
from fala.native_cli_surface import dispatch_native_command
from fala.path_terminal import select_path_terminal
from fala.native_package import PackageCorrelationPath, PackageEffector, PackagePathTerminal
from fala.journal import ProcessRow


def expect(value: Bool, message: String) raises:
    if not value: raise Error(message)


def _row(id: String, status: String, output_json: String = "{}", error_json: String = "{}") -> ProcessRow:
    return ProcessRow(run_id="run", id=id, process_type="correlation", impulse_id="", status=status, priority=0, attempt=1, max_attempts=1, available_at="", lease_owner="", lease_expires_at="", input_json="{}", output_json=output_json, error_json=error_json, metadata="{}", created_at="", updated_at="", started_at="", finished_at="", output_schema_json="{}")


def _decide_path() raises -> PackageCorrelationPath:
    var terminals = List[PackagePathTerminal]()
    terminals.append(PackagePathTerminal(id="done", source_effector="decide", status="succeeded", when_json="{\"path\":\"route\",\"equals\":\"ready\"}", output_schema_json="{\"type\":\"object\",\"required\":[\"route\"]}"))
    terminals.append(PackagePathTerminal(id="waiting", source_effector="decide", status="succeeded", when_json="{\"path\":\"route\",\"equals\":\"wait\"}", output_schema_json="{\"type\":\"object\",\"required\":[\"route\"]}"))
    return PackageCorrelationPath("ship", List[PackageEffector](), terminals=terminals^)


def main() raises:
    var package = "/tmp/fala-rehearsal-package.json"; var fixture = "/tmp/fala-rehearsal-fixture.json"; var db = "/tmp/fala-rehearsal.sqlite"; var report = "/tmp/fala-rehearsal-report.json"
    for path in [db, db + "-wal", db + "-shm", report, db + "-bad", db + "-contract", db + "-condition", db + "-wait", db + "-timeout"]:
        try: remove(path)
        except: pass
    Path(package).write_text("{\"id\":\"delivery\",\"correlation_paths\":[{\"id\":\"ship\",\"effectors\":[{\"id\":\"gate\",\"output_schema\":{\"type\":\"object\",\"required\":[\"approved\"],\"properties\":{\"approved\":{\"type\":\"boolean\"}}},\"adapter\":{\"kind\":\"subprocess\",\"command\":[\"/must/not/run\"]},\"retry_policy\":\"automatic\"},{\"id\":\"publish\",\"output_schema\":{\"type\":\"object\",\"required\":[\"url\"],\"properties\":{\"url\":{\"type\":\"string\"}}},\"adapter\":{\"kind\":\"native_function\",\"ref\":\"must.not.run\"},\"conduction\":[\"gate\"],\"when\":{\"upstream\":\"gate\",\"path\":\"approved\",\"equals\":true}}],\"terminals\":[{\"id\":\"done\",\"source_effector\":\"publish\",\"status\":\"succeeded\",\"output_schema\":{\"type\":\"object\",\"required\":[\"url\"],\"properties\":{\"url\":{\"type\":\"string\"}}}}]}]}")
    Path(fixture).write_text("{\"effectors\":{\"gate\":[{\"kind\":\"failure\"},{\"kind\":\"result\",\"output\":{\"approved\":true}}],\"publish\":[{\"kind\":\"result\",\"output\":{\"url\":\"local\"}}]},\"assert\":{\"terminal\":\"done\",\"attempts\":{\"gate\":2,\"publish\":1},\"forbidden_effectors\":[\"merge_without_gate\"]}}")
    var command = "rehearse --package " + package + " --fixture " + fixture + " --path-id ship --journal " + db + " --report " + report + " --run-id acceptance"
    var first = dispatch_native_command(command)
    expect(first.find("\"terminal\":\"done\"") >= 0 and first.find("gate#2") >= 0 and first.find("fixtures_only") >= 0, "retry and terminal report")
    expect(Path(report).is_file() and Path(db).is_file(), "durable journal and report")
    # Recreate outputs and prove deterministic event order for the same fixture/fingerprint.
    try: remove(db)
    except: pass
    try: remove(report)
    except: pass
    var second = dispatch_native_command(command)
    expect(first == second, "deterministic rehearsal")
    Path(fixture).write_text("{\"effectors\":{\"gate\":[{\"kind\":\"result\",\"output\":{\"approved\":true}}],\"publish\":[{\"kind\":\"result\",\"output\":{}}]},\"assert\":{\"terminal\":\"wrong\"}}")
    var mismatch = dispatch_native_command(command.replace(db, db + "-bad").replace(report, report + "-bad"))
    expect(mismatch.find("\"ok\":false") >= 0, "terminal mismatch is non-success")
    var contracted = Path(package).read_text().replace('"id":"gate","output_schema":{"type":"object","required":["approved"],"properties":{"approved":{"type":"boolean"}}},"adapter"', '"id":"gate","output_schema":{"type":"object","required":["approved"],"properties":{"approved":{"const":true}}},"adapter"')
    Path(package).write_text(contracted)
    Path(fixture).write_text('{"effectors":{"gate":[{"kind":"result","output":{"approved":false}}],"publish":[{"kind":"result","output":{}}]}}')
    var invalid = dispatch_native_command(command.replace(db, db + "-contract").replace(report, report + "-contract"))
    expect(invalid.find('"ok":false') >= 0, "rehearsal must enforce frozen effector output schema")
    Path(package).write_text(contracted.replace('"id":"done","source_effector"', '"id":"done","when":{"path":"accepted","equals":true},"source_effector"'))
    Path(fixture).write_text('{"effectors":{"gate":[{"kind":"result","output":{"approved":true}}],"publish":[{"kind":"result","output":{"url":"local","accepted":false}}]},"assert":{"terminal":"done"}}')
    var wrong_condition = dispatch_native_command(command.replace(db, db + "-condition").replace(report, report + "-condition"))
    expect(wrong_condition.find('"ok":false') >= 0, "terminal when must be enforced")

    Path(package).write_text("{\"id\":\"delivery\",\"correlation_paths\":[{\"id\":\"ship\",\"effectors\":[{\"id\":\"gate\",\"output_schema\":{\"type\":\"object\",\"required\":[\"approved\"],\"properties\":{\"approved\":{\"type\":\"boolean\"}}},\"adapter\":{\"kind\":\"subprocess\",\"command\":[\"/must/not/run\"]}}]}]}")
    Path(fixture).write_text("{\"effectors\":{\"gate\":[{\"kind\":\"wait\"}]},\"assert\":{\"attempts\":{\"gate\":1}}}")
    var wait_report = dispatch_native_command(command.replace(db, db + "-wait").replace(report, report + "-wait"))
    expect(wait_report.find("\"ok\":true") >= 0 and wait_report.find("\"status\":\"waiting\"") >= 0 and wait_report.find("\"terminal\":null") >= 0, "wait fixture parks without a terminal")

    Path(package).write_text("{\"id\":\"delivery\",\"correlation_paths\":[{\"id\":\"ship\",\"effectors\":[{\"id\":\"gate\",\"output_schema\":{\"type\":\"object\",\"required\":[\"approved\"],\"properties\":{\"approved\":{\"type\":\"boolean\"}}},\"adapter\":{\"kind\":\"subprocess\",\"command\":[\"/must/not/run\"]}}],\"terminals\":[{\"id\":\"timed_out\",\"source_effector\":\"gate\",\"status\":\"timed_out\",\"output_schema\":{\"type\":\"object\",\"required\":[\"code\"],\"properties\":{\"code\":{\"type\":\"string\"}}}}]}]}")
    Path(fixture).write_text("{\"effectors\":{\"gate\":[{\"kind\":\"timeout\"}]},\"assert\":{\"terminal\":\"timed_out\",\"attempts\":{\"gate\":1}}}")
    var timeout_report = dispatch_native_command(command.replace(db, db + "-timeout").replace(report, report + "-timeout"))
    expect(timeout_report.find("\"ok\":true") >= 0 and timeout_report.find("\"status\":\"failed\"") >= 0 and timeout_report.find("\"terminal\":\"timed_out\"") >= 0, "timeout fixture is a failed timed_out hop")

    var decide = _decide_path()
    var rows = List[ProcessRow]()
    rows.append(_row("ship:decide", "succeeded", "{\"route\":\"ready\"}"))
    var ready = select_path_terminal(decide, rows, "ship")
    expect(ready.id == "done", "ready variant selects done")
    rows[0] = _row("ship:decide", "succeeded", "{\"kind\":\"result\",\"payload\":{\"route\":\"wait\"}}")
    var parked = select_path_terminal(decide, rows, "ship")
    expect(parked.id == "waiting", "wait variant selects waiting")
    var unmatched = False
    try:
        rows[0] = _row("ship:decide", "succeeded", "{\"route\":\"unknown\"}")
        _ = select_path_terminal(decide, rows, "ship")
    except err:
        unmatched = String(err).find("path.terminal.missing") >= 0
    expect(unmatched, "unknown decide variant is missing")
    var empty = select_path_terminal(decide, rows, "ship", require_match=False)
    expect(empty.id == "", "unmatched decide hop may stay empty")
    var ambiguous_terminals = List[PackagePathTerminal]()
    ambiguous_terminals.append(PackagePathTerminal(id="a", source_effector="decide", status="succeeded", when_json="", output_schema_json="{\"type\":\"object\"}"))
    ambiguous_terminals.append(PackagePathTerminal(id="b", source_effector="decide", status="succeeded", when_json="", output_schema_json="{\"type\":\"object\"}"))
    var ambiguous_path = PackageCorrelationPath("ship", List[PackageEffector](), terminals=ambiguous_terminals^)
    var ambiguous = False
    try:
        rows[0] = _row("ship:decide", "succeeded", "{}")
        _ = select_path_terminal(ambiguous_path, rows, "ship")
    except err:
        ambiguous = String(err).find("path.terminal.ambiguous") >= 0
    expect(ambiguous, "two matching terminals are ambiguous")
    var missing_field = False
    try:
        rows[0] = _row("ship:decide", "succeeded", "{}")
        _ = select_path_terminal(decide, rows, "ship")
    except err:
        missing_field = String(err).find("path.terminal.missing_field") >= 0
    expect(missing_field, "missing route fails closed")
    print("graph rehearsal smoke ok")
