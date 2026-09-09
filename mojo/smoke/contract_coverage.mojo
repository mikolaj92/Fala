from std.os import remove
from std.pathlib import Path
from fala.graph_tools import graph_validate
from fala.native_cli_surface import dispatch_native_command


def expect(value: Bool, message: String) raises:
    if not value:
        raise Error(message)


def _clean(path: String):
    try:
        remove(path)
    except:
        pass


def main() raises:
    var root = "/tmp/fala-contract-coverage"
    var invalid = root + "-invalid.json"
    var valid = root + "-valid.json"
    var optout = root + "-optout.json"
    var fixture = root + "-fixture.json"
    var missing_fixture = root + "-missing-fixture.json"
    var unproven = root + "-unproven.json"
    var typo = root + "-typo.json"
    var journal = root + ".sqlite"
    var report = root + "-report.json"
    var missing_report = root + "-missing-report.json"
    for path in [invalid, valid, optout, fixture, missing_fixture, unproven, typo, journal, journal + "-wal", journal + "-shm", report, missing_report]:
        _clean(path)

    var source_schema = "{\"type\":\"object\",\"required\":[\"route\"],\"properties\":{\"route\":{\"type\":\"string\",\"enum\":[\"ready\",\"wait\"]}}}"
    var common = "\"output_schema\":" + source_schema + ",\"adapter\":{\"kind\":\"manual_homeostat\"}"
    Path(invalid).write_text("{\"id\":\"coverage\",\"version\":\"2\",\"correlation_paths\":[{\"id\":\"ship\",\"effectors\":[{\"id\":\"decide\"," + common + "},{\"id\":\"consume\",\"conduction\":[\"decide\"],\"when\":{\"upstream\":\"decide\",\"path\":\"route\",\"equals\":\"ready\"},\"output_schema\":{\"type\":\"object\",\"required\":[\"accepted\"]},\"adapter\":{\"kind\":\"manual_homeostat\"}}],\"terminals\":[{\"id\":\"done\",\"source_effector\":\"consume\",\"status\":\"succeeded\",\"output_schema\":{\"type\":\"object\"}}]}]}")
    var invalid_report = graph_validate(invalid)
    expect(invalid_report.find("contract_coverage_missing") >= 0 and invalid_report.find("decide") >= 0 and invalid_report.find("wait") >= 0, "unhandled output variant must be reported before execution")

    Path(valid).write_text("{\"id\":\"coverage\",\"version\":\"2\",\"correlation_paths\":[{\"id\":\"ship\",\"effectors\":[{\"id\":\"decide\"," + common + "},{\"id\":\"consume\",\"conduction\":[\"decide\"],\"when\":{\"upstream\":\"decide\",\"path\":\"route\",\"equals\":\"ready\"},\"output_schema\":{\"type\":\"object\",\"required\":[\"accepted\"]},\"adapter\":{\"kind\":\"manual_homeostat\"}}],\"terminals\":[{\"id\":\"waiting\",\"source_effector\":\"decide\",\"status\":\"succeeded\",\"when\":{\"path\":\"route\",\"equals\":\"wait\"},\"output_schema\":{\"type\":\"object\",\"required\":[\"route\"]}},{\"id\":\"done\",\"source_effector\":\"consume\",\"status\":\"succeeded\",\"output_schema\":{\"type\":\"object\",\"required\":[\"accepted\"]}}]}]}")
    var valid_report = graph_validate(valid)
    expect(valid_report.find("\"valid\":true") >= 0, "complete edge and terminal coverage must be accepted")

    Path(optout).write_text("{\"id\":\"legacy\",\"version\":\"2\",\"correlation_paths\":[{\"id\":\"ship\",\"effectors\":[{\"id\":\"legacy_step\",\"contract_mode\":\"legacy\",\"adapter\":{\"kind\":\"manual_homeostat\"}}],\"terminals\":[{\"id\":\"done\",\"source_effector\":\"legacy_step\",\"status\":\"succeeded\",\"output_schema\":{\"type\":\"object\"}}]}]}")
    var optout_report = graph_validate(optout)
    expect(optout_report.find("\"valid\":true") >= 0 and optout_report.find("coverage_guaranteed\":false") >= 0, "explicit opt-out must be visible and unverified")

    Path(unproven).write_text("{\"id\":\"unproven\",\"version\":\"2\",\"correlation_paths\":[{\"id\":\"ship\",\"effectors\":[{\"id\":\"step\",\"output_schema\":{\"type\":\"object\",\"required\":[\"route\"],\"properties\":{\"route\":{\"type\":\"string\"}}},\"adapter\":{\"kind\":\"manual_homeostat\"}}],\"terminals\":[{\"id\":\"done\",\"source_effector\":\"step\",\"status\":\"succeeded\",\"output_schema\":{\"type\":\"object\"}}]}]}")
    var unproven_report = graph_validate(unproven)
    expect(unproven_report.find("\"valid\":true") >= 0 and unproven_report.find("coverage_guaranteed\":false") >= 0 and unproven_report.find("step") >= 0, "unproven contract must be visible and not guaranteed")

    Path(typo).write_text("{\"id\":\"typo\",\"version\":\"2\",\"correlation_paths\":[{\"id\":\"ship\",\"effectors\":[{\"id\":\"decide\",\"output_schema\":{\"type\":\"object\",\"required\":[\"route\"],\"properties\":{\"route\":{\"type\":\"string\",\"enum\":[\"ready\"]}}},\"adapter\":{\"kind\":\"manual_homeostat\"}},{\"id\":\"consume\",\"conduction\":[\"decide\"],\"when\":{\"upstream\":\"decide\",\"path\":\"route\",\"equals\":\"notready\"},\"output_schema\":{\"type\":\"object\"},\"adapter\":{\"kind\":\"manual_homeostat\"}}],\"terminals\":[{\"id\":\"done\",\"source_effector\":\"consume\",\"status\":\"succeeded\",\"output_schema\":{\"type\":\"object\"}}]}]}")
    var typo_report = graph_validate(typo)
    expect(typo_report.find("contract_coverage_missing") >= 0 and typo_report.find("ready") >= 0, "variant typo must not count as coverage")

    Path(fixture).write_text("{\"scenarios\":[{\"id\":\"ready\",\"effectors\":{\"decide\":[{\"kind\":\"result\",\"output\":{\"route\":\"ready\"}}],\"consume\":[{\"kind\":\"result\",\"output\":{\"accepted\":true}}]},\"assert\":{\"terminal\":\"done\"}},{\"id\":\"wait\",\"effectors\":{\"decide\":[{\"kind\":\"result\",\"output\":{\"route\":\"wait\"}}]},\"assert\":{\"terminal\":\"waiting\",\"forbidden_effectors\":[\"consume\"]}}]}")
    var rehearsal_command = "rehearse --package " + valid + " --fixture " + fixture + " --path-id ship --journal " + journal + " --report " + report + " --run-id coverage"
    var rehearsal = dispatch_native_command(rehearsal_command)
    expect(rehearsal.find("\"ok\":true") >= 0 and rehearsal.find("\"missing\":[]") >= 0 and rehearsal.find("ready") >= 0 and rehearsal.find("wait") >= 0, "rehearsal must report every fixture variant")

    Path(missing_fixture).write_text("{\"scenarios\":[{\"id\":\"ready\",\"effectors\":{\"decide\":[{\"kind\":\"result\",\"output\":{\"route\":\"ready\"}}],\"consume\":[{\"kind\":\"result\",\"output\":{\"accepted\":true}}]}}]}")
    var missing_command = "rehearse --package " + valid + " --fixture " + missing_fixture + " --path-id ship --journal " + (journal + "-missing") + " --report " + missing_report + " --run-id missing"
    var missing = dispatch_native_command(missing_command)
    expect(missing.find("\"ok\":false") >= 0 and Path(missing_report).is_file() and Path(missing_report).read_text().find("wait") >= 0, "missing fixture variant must fail with a report")
    print("contract coverage smoke ok")
