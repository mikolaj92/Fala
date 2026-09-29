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
    expect(classify_graph_change(old, old).find("\"classification\":\"identical\"") >= 0, "unchanged graph is identical")
    expect(assert_resume_compatible(graph_fingerprint(old), old) == "identical", "identical resume")
    expect(assert_resume_compatible(graph_fingerprint(old), additive, old) == "compatible_additive", "additive resume is allowed")
    var retry = "/tmp/fala-compat-retry.json"
    Path(retry).write_text(Path(old).read_text().replace("\"adapter\":{\"kind\":\"manual_homeostat\"}", "\"retry_policy\":\"none\",\"adapter\":{\"kind\":\"manual_homeostat\"}"))
    expect(classify_graph_change(old, retry).find("\"classification\":\"incompatible\"") >= 0, "retry change is incompatible")
    var timeout = "/tmp/fala-compat-timeout.json"
    Path(timeout).write_text(Path(old).read_text().replace("\"adapter\":{\"kind\":\"manual_homeostat\"}", "\"timeout_seconds\":5,\"adapter\":{\"kind\":\"manual_homeostat\"}"))
    expect(classify_graph_change(old, timeout).find("\"classification\":\"incompatible\"") >= 0, "timeout change is incompatible")
    var blocked = False
    try: _ = assert_resume_compatible(graph_fingerprint(old), breaking, old)
    except err: blocked = String(err).find("resume_mismatch") >= 0
    expect(blocked, "active resume fails closed")
    var missing_before = False
    try: _ = assert_resume_compatible(graph_fingerprint(old), breaking)
    except err: missing_before = String(err).find("resume_mismatch") >= 0
    expect(missing_before, "fingerprint mismatch without before-path fails closed")
    var retry_blocked = False
    try: _ = assert_resume_compatible(graph_fingerprint(old), retry, old)
    except err: retry_blocked = String(err).find("incompatible") >= 0
    expect(retry_blocked, "incompatible resume fails closed")
    print("graph compatibility smoke ok")
