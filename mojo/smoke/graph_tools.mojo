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
    var package = "{\"id\":\"lokay\",\"capabilities\":[{\"id\":\"gate\"},{\"id\":\"merge\"}],\"path_templates\":[{\"id\":\"checks\",\"parameters\":{\"name\":\"string\"},\"effectors\":[{\"id\":\"${name}\",\"capability\":\"gate\",\"adapter\":{\"kind\":\"manual_homeostat\"}}]}],\"correlation_paths\":[{\"id\":\"ship\",\"expansion\":{\"template\":\"checks\",\"max_items\":1,\"items\":[{\"name\":\"quality\"}]},\"terminals\":[{\"id\":\"blocked\",\"source_effector\":\"quality\",\"status\":\"succeeded\",\"output_schema\":{\"type\":\"object\"}}]}]}"
    Path(a).write_text(package)
    Path(same).write_text(" { \"correlation_paths\" : [ { \"terminals\" : [ { \"output_schema\":{\"type\":\"object\"}, \"status\":\"succeeded\",\"source_effector\":\"quality\",\"id\":\"blocked\"}],\"expansion\":{\"items\":[{\"name\":\"quality\"}],\"max_items\":1,\"template\":\"checks\"},\"id\":\"ship\"}],\"path_templates\":[{\"effectors\":[{\"adapter\":{\"kind\":\"manual_homeostat\"},\"capability\":\"gate\",\"id\":\"${name}\"}],\"parameters\":{\"name\":\"string\"},\"id\":\"checks\"}],\"capabilities\":[{\"id\":\"gate\"},{\"id\":\"merge\"}],\"id\":\"lokay\" }")
    Path(changed).write_text(package.replace("\"capability\":\"gate\"", "\"capability\":\"merge\"").replace("\"blocked\"", "\"delivered\""))
    Path(invalid).write_text("{\"id\":\"bad\",\"correlation_paths\":[{\"id\":\"p\",\"effectors\":[{\"id\":\"publish\",\"adapter\":{\"kind\":\"subprocess\",\"command\":[\"true\"]},\"conduction\":[\"missing\"]}]}]}")

    var expanded = graph_expand(a)
    expect(expanded.find("path_templates") < 0 and expanded.find("quality") >= 0, "expand must materialize templates")
    expect(graph_fingerprint(a) == graph_fingerprint(same), "formatting must not affect fingerprint")
    expect(graph_fingerprint(a) != graph_fingerprint(changed), "semantic change must affect fingerprint")
    var diff = graph_diff(a, changed)
    expect(diff.find("capability_changed") >= 0 and diff.find("terminal_removed") >= 0 and diff.find("terminal_added") >= 0, "semantic diff classifications")
    var valid = graph_validate(a)
    expect(valid.find("\"valid\":true") >= 0, "valid graph")
    var bad = graph_validate(invalid)
    expect(bad.find("manifest.dangling_reference") >= 0 and bad.find("/correlation_paths/0/effectors/0/conduction/0") >= 0, "exact diagnostic pointer")
    var cli = dispatch_native_command("graph fingerprint --package " + a)
    expect(cli.find("\"fingerprint\"") >= 0 and cli.find("\"ok\":true") >= 0, "CLI stable JSON")
    print("graph tools smoke ok")
