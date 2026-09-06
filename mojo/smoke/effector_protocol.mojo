from std.pathlib import Path
from fala.effector_protocol import request_message, result_message, validate_message
from fala import AdapterSpec, EffectorRequest, execute_subprocess


def expect(value: Bool, message: String) raises:
    if not value: raise Error(message)


def main() raises:
    var one = request_message("run", "process", "execution", 2, "impulse", "proc:sha", "path:sha", "review", "{\"b\":2,\"a\":1}", "{}", "schema:sha")
    var two = request_message("run", "process", "execution", 2, "impulse", "proc:sha", "path:sha", "review", "{ \"a\": 1, \"b\": 2 }", "{}", "schema:sha")
    expect(one == two and validate_message(one, "effector.request") == one, "canonical stable request ID")
    Path("/tmp/fep-request.json").write_text(one)
    expect(validate_message(Path("/tmp/fep-request.json").read_text()) == one, "filesystem and direct transport match")
    var request_id_start = one.find("\"message_id\":\"") + 14
    var request_id_end = one.find("\"", request_id_start)
    var request_id = String(one[byte=request_id_start:request_id_end])
    var result = result_message(request_id, "execution", 2, values_json="{\"ok\":true}", evidence_refs_json="[\"evidence:1\"]")
    expect(validate_message(result, "effector.result") == result and result.find("\"request_id\":\"" + request_id) >= 0, "result causes exactly one request")
    var bad = False
    try: _ = validate_message(result.replace("fala-effector/1", "fala-effector/9"))
    except err: bad = String(err).find("unknown_protocol") >= 0
    expect(bad, "unknown protocol fails closed")
    bad = False
    try: _ = validate_message(result.replace("msg:sha256:", "msg:broken:"))
    except err: bad = String(err).find("digest_mismatch") >= 0
    expect(bad, "bad digest fails closed")
    # The filesystem adapter must not accept an unversioned result object.
    var command = List[String]()
    command.append("/bin/sh")
    command.append("-c")
    command.append("printf '%s' '{\"values\":{}}' > \"$FALA_EFFECTOR_OUTPUT_DIR/result.json\"")
    var adapter_result = execute_subprocess(EffectorRequest("fep-result", AdapterSpec.subprocess(command), "impulse", "{}", "{}"))
    expect(not adapter_result.success and adapter_result.error.code == "adapter_invalid_result", "unversioned result must fail closed")
    expect(adapter_result.error.message.find("/protocol") >= 0, "missing protocol has a clear adapter error")
    expect(adapter_result.output_json == "{}" and adapter_result.returncode == 0, "exit zero cannot impersonate a valid result")
    # Use an environment value, not shell interpolation of message contents.
    command[2] = "printf '%s' \"$FEP_RESULT\" > \"$FALA_EFFECTOR_OUTPUT_DIR/result.json\""
    var adapter = AdapterSpec.subprocess(command)
    for invalid in [one, result.replace("fala-effector/1", "fala-effector/9"), result.replace("msg:sha256:", "msg:broken:"), result.replace("\"ok\":true", "\"ok\":false"), "[]", "{broken"]:
        adapter.env["FEP_RESULT"] = invalid
        var rejected = execute_subprocess(EffectorRequest("fep-result", adapter, "impulse", "{}", "{}"))
        expect(not rejected.success and rejected.error.code == "adapter_invalid_result" and rejected.output_json == "{}", "invalid FEP result fails at adapter boundary")
    adapter.env["FEP_RESULT"] = "  " + result + "  "
    var accepted = execute_subprocess(EffectorRequest("fep-result", adapter, "impulse", "{}", "{}"))
    expect(accepted.success and accepted.output_json == result, "valid FEP result is canonical and unredacted")
    print("effector protocol smoke ok")
