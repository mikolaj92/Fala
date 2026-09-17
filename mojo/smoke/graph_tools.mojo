from std.pathlib import Path
from fala.graph_tools import graph_expand, graph_validate, graph_fingerprint, graph_diff
from fala.native_cli_surface import dispatch_native_command


def expect(value: Bool, message: String) raises:
    if not value: raise Error(message)


def main() raises:
    var root = "/tmp/fala-graph-tools"
    var a = root + "-a.json"
    var same = root + "-same.json"
    var changed = root + "-changed.json"
    var invalid = root + "-invalid.json"
    var package = "{\"id\":\"example\",\"capabilities\":[{\"id\":\"gate\"},{\"id\":\"merge\"}],\"path_templates\":[{\"id\":\"checks\",\"parameters\":{\"name\":\"string\"},\"effectors\":[{\"id\":\"${name}\",\"capability\":\"gate\",\"output_schema\":{\"type\":\"object\",\"required\":[\"ok\"],\"properties\":{\"ok\":{\"type\":\"boolean\"}}},\"adapter\":{\"kind\":\"manual_homeostat\"}}]}],\"correlation_paths\":[{\"id\":\"ship\",\"expansion\":{\"template\":\"checks\",\"max_items\":1,\"items\":[{\"name\":\"quality\"}]},\"terminals\":[{\"id\":\"blocked\",\"source_effector\":\"quality\",\"status\":\"succeeded\",\"output_schema\":{\"type\":\"object\",\"required\":[\"ok\"],\"properties\":{\"ok\":{\"type\":\"boolean\"}}}}]}]}"
    Path(a).write_text(package)
    Path(same).write_text(" { \"correlation_paths\" : [ { \"terminals\" : [ { \"output_schema\":{\"type\":\"object\",\"required\":[\"ok\"],\"properties\":{\"ok\":{\"type\":\"boolean\"}}}, \"status\":\"succeeded\",\"source_effector\":\"quality\",\"id\":\"blocked\"}],\"expansion\":{\"items\":[{\"name\":\"quality\"}],\"max_items\":1,\"template\":\"checks\"},\"id\":\"ship\"}],\"path_templates\":[{\"effectors\":[{\"output_schema\":{\"type\":\"object\",\"required\":[\"ok\"],\"properties\":{\"ok\":{\"type\":\"boolean\"}}},\"adapter\":{\"kind\":\"manual_homeostat\"},\"capability\":\"gate\",\"id\":\"${name}\"}],\"parameters\":{\"name\":\"string\"},\"id\":\"checks\"}],\"capabilities\":[{\"id\":\"gate\"},{\"id\":\"merge\"}],\"id\":\"example\" }")
    Path(changed).write_text(package.replace("\"capability\":\"gate\"", "\"capability\":\"merge\"").replace("\"blocked\"", "\"delivered\""))
    Path(invalid).write_text("{\"id\":\"bad\",\"correlation_paths\":[{\"id\":\"p\",\"effectors\":[{\"id\":\"publish\",\"output_schema\":{\"type\":\"object\",\"required\":[\"ok\"],\"properties\":{\"ok\":{\"type\":\"boolean\"}}},\"adapter\":{\"kind\":\"subprocess\",\"command\":[\"true\"]},\"conduction\":[\"missing\"]}]}]}")

    var expanded = graph_expand(a)
    expect(expanded.find("path_templates") < 0 and expanded.find("quality") >= 0, "expand must materialize templates")
    expect(graph_fingerprint(a) == graph_fingerprint(same), "formatting must not affect fingerprint")
    expect(graph_fingerprint(a) != graph_fingerprint(changed), "semantic change must affect fingerprint")
    var diff = graph_diff(a, changed)
    expect(diff.find("capability_changed") >= 0 and diff.find("terminal_removed") >= 0 and diff.find("terminal_added") >= 0, "semantic diff classifications")
    var nest_a = root + "-nest-a.json"
    var nest_b = root + "-nest-b.json"
    var nest = "{\"id\":\"nest\",\"correlation_paths\":[{\"id\":\"p\",\"effectors\":[{\"id\":\"child\",\"output_schema\":{\"type\":\"object\",\"required\":[\"ok\"],\"properties\":{\"ok\":{\"type\":\"boolean\"}}},\"config\":{\"x\":1},\"adapter\":{\"kind\":\"child_path\",\"package_ref\":\"child.json\",\"path_id\":\"inner\",\"journal_root\":\"/tmp/fala-children\",\"input_mapping\":{\"x\":\"x\"},\"terminal_mapping\":{\"done\":\"ok\"},\"lifetime_seconds\":1,\"retention\":\"keep\"}}]}]}"
    Path(nest_a).write_text(nest)
    Path(nest_b).write_text(nest.replace("\"path_id\":\"inner\"", "\"path_id\":\"other\"").replace("\"required\":[\"ok\"]", "\"required\":[\"ok\",\"extra\"]").replace("\"config\":{\"x\":1}", "\"config\":{\"x\":2}"))
    var nest_diff = graph_diff(nest_a, nest_b)
    expect(nest_diff.find("adapter_changed") >= 0 and nest_diff.find("contract_changed") >= 0 and nest_diff.find("config_changed") >= 0, "diff must see child_path, schema, and config")
    var valid = graph_validate(a)
    expect(valid.find("\"valid\":true") >= 0, "valid graph")
    var bad = graph_validate(invalid)
    expect(bad.find("manifest.dangling_reference") >= 0 and bad.find("/correlation_paths/0/effectors/0/conduction/0") >= 0, "exact diagnostic pointer")
    var shared_journal = root + "-shared-journal.json"
    Path(shared_journal).write_text("{\"id\":\"bad\",\"correlation_paths\":[{\"id\":\"p\",\"effectors\":[{\"id\":\"nest\",\"output_schema\":{\"type\":\"object\",\"required\":[\"ok\"],\"properties\":{\"ok\":{\"type\":\"boolean\"}}},\"adapter\":{\"kind\":\"child_path\",\"package_ref\":\"child.json\",\"path_id\":\"inner\",\"journal_root\":\"state.sqlite\",\"input_mapping\":{\"x\":\"x\"},\"terminal_mapping\":{\"done\":\"ok\"},\"lifetime_seconds\":1,\"retention\":\"keep\"}}]}]}")
    var shared = graph_validate(shared_journal)
    expect(shared.find("manifest.boundary") >= 0 and shared.find("/correlation_paths/0/effectors/0/adapter/journal_root") >= 0 and shared.find("\"valid\":false") >= 0, "child journal file is a load diagnostic")
    var cli = dispatch_native_command("graph fingerprint --package " + a)
    expect(cli.find("\"fingerprint\"") >= 0 and cli.find("\"ok\":true") >= 0, "CLI stable JSON")
    print("graph tools smoke ok")
