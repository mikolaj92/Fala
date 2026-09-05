from std.os import remove
from std.pathlib import Path
from fala.native_cli_surface import dispatch_native_command


def expect(value: Bool, message: String) raises:
    if not value: raise Error(message)


def main() raises:
    var package = "/tmp/fala-rehearsal-package.json"; var fixture = "/tmp/fala-rehearsal-fixture.json"; var db = "/tmp/fala-rehearsal.sqlite"; var report = "/tmp/fala-rehearsal-report.json"
    for path in [db, db + "-wal", db + "-shm", report]:
        try: remove(path)
        except: pass
    Path(package).write_text("{\"id\":\"delivery\",\"correlation_paths\":[{\"id\":\"ship\",\"effectors\":[{\"id\":\"gate\",\"adapter\":{\"kind\":\"subprocess\",\"command\":[\"/must/not/run\"]},\"retry_policy\":\"automatic\"},{\"id\":\"publish\",\"adapter\":{\"kind\":\"native_function\",\"ref\":\"must.not.run\"},\"conduction\":[\"gate\"],\"when\":{\"upstream\":\"gate\",\"path\":\"approved\",\"equals\":true}}],\"terminals\":[{\"id\":\"done\",\"source_effector\":\"publish\",\"status\":\"succeeded\",\"output_schema\":{\"type\":\"object\"}}]}]}")
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
    print("graph rehearsal smoke ok")
