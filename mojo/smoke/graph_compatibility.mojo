from std.pathlib import Path
from fala.graph_compatibility import classify_graph_change, assert_resume_compatible
from fala.graph_tools import graph_fingerprint


def expect(value: Bool, message: String) raises:
    if not value: raise Error(message)


def main() raises:
    var old = "/tmp/fala-compat-old.json"
    var additive = "/tmp/fala-compat-add.json"
    var breaking = "/tmp/fala-compat-break.json"
    Path(old).write_text("{\"id\":\"p\",\"correlation_paths\":[{\"id\":\"x\",\"effectors\":[{\"id\":\"gate\",\"output_schema\":{\"type\":\"object\",\"required\":[\"ok\"],\"properties\":{\"ok\":{\"type\":\"boolean\"}}},\"adapter\":{\"kind\":\"manual_homeostat\"}}],\"terminals\":[{\"id\":\"done\",\"source_effector\":\"gate\",\"status\":\"succeeded\",\"output_schema\":{\"type\":\"object\",\"required\":[\"ok\"],\"properties\":{\"ok\":{\"type\":\"boolean\"}}}}]}]}")
    Path(additive).write_text("{\"id\":\"p\",\"description\":\"metadata\",\"correlation_paths\":[{\"id\":\"x\",\"effectors\":[{\"id\":\"gate\",\"output_schema\":{\"type\":\"object\",\"required\":[\"ok\"],\"properties\":{\"ok\":{\"type\":\"boolean\"}}},\"adapter\":{\"kind\":\"manual_homeostat\"}}],\"terminals\":[{\"id\":\"done\",\"source_effector\":\"gate\",\"status\":\"succeeded\",\"output_schema\":{\"type\":\"object\",\"required\":[\"ok\"],\"properties\":{\"ok\":{\"type\":\"boolean\"}}}}]}]}")
    Path(breaking).write_text(Path(old).read_text().replace("\"gate\"", "\"quality\""))
    expect(classify_graph_change(old, additive).find("compatible_additive") >= 0, "description is compatible additive")
    expect(classify_graph_change(old, breaking).find("forbidden_for_active_run") >= 0, "gate removal forbidden")
    expect(classify_graph_change(old, breaking).find("incompatible") < 0, "node rename is forbidden, not a resume path")
    expect(assert_resume_compatible(graph_fingerprint(old), old) == "identical", "identical resume")
    var blocked = False
    try: _ = assert_resume_compatible(graph_fingerprint(old), breaking, old)
    except err: blocked = String(err).find("resume_mismatch") >= 0
    expect(blocked, "active resume fails closed")
    print("graph compatibility smoke ok")
