from std.pathlib import Path
from fala.effector_protocol import request_message, result_message, validate_message
from fala import AdapterSpec, EffectorRequest, execute_subprocess


def expect(value: Bool, message: String) raises:
    if not value: raise Error(message)


def main() raises:
    var one = request_message("parent", "review", "review", "{\"b\":2,\"a\":1}")
    var two = request_message("parent", "review", "review", "{ \"a\": 1, \"b\": 2 }")
    expect(one == two and validate_message(one, "request") == one, "canonical stable request ID")
    Path("/tmp/fep-request.json").write_text(one)
    expect(validate_message(Path("/tmp/fep-request.json").read_text()) == one, "filesystem and direct transport match")
    var request_id_start = one.find("\"id\":\"") + 6
    var request_id_end = one.find("\"", request_id_start)
    var request_id = String(one[byte=request_id_start:request_id_end])
    var result = result_message("review", "parent", "review", request_id, "{\"ok\":true}")
    expect(validate_message(result, "result") == result and result.find("\"ref\":\"" + request_id) >= 0, "result points at exactly one request")
    var bad = False
    try: _ = validate_message(result.replace("\"protocol\":\"fala\"", "\"protocol\":\"unknown\""))
    except err: bad = String(err).find("unknown_protocol") >= 0
    expect(bad, "unknown protocol fails closed")
    bad = False
    try: _ = validate_message(result.replace("msg:sha256:", "msg:broken:"))
    except err: bad = String(err).find("digest_mismatch") >= 0
    expect(bad, "bad digest fails closed")
    var command = List[String]()
    command.append("/bin/sh")
    command.append("-c")
    command.append("printf '%s' '{\"payload\":{}}' > \"$FALA_EFFECTOR_OUTPUT_DIR/result.json\"")
    var adapter_result = execute_subprocess(EffectorRequest("protocol-result", AdapterSpec.subprocess(command), "impulse", "{}", "{}"))
    expect(not adapter_result.success and adapter_result.error.code == "adapter_invalid_result", "unversioned result must fail closed")
    expect(adapter_result.error.message.find("/protocol") >= 0 or adapter_result.error.message.find("unknown_protocol") >= 0, "missing protocol has a clear adapter error")
    expect(adapter_result.output_json == "{}" and adapter_result.returncode == 0, "exit zero cannot impersonate a valid result")
    command[2] = "printf '%s' \"$FALA_RESULT\" > \"$FALA_EFFECTOR_OUTPUT_DIR/result.json\""
    var adapter = AdapterSpec.subprocess(command)
    for invalid in [one, result.replace("\"protocol\":\"fala\"", "\"protocol\":\"unknown\""), result.replace("msg:sha256:", "msg:broken:"), result.replace("\"ok\":true", "\"ok\":false"), "[]", "{broken"]:
        adapter.env["FALA_RESULT"] = invalid
        var rejected = execute_subprocess(EffectorRequest("protocol-result", adapter, "impulse", "{}", "{}"))
        expect(not rejected.success and rejected.error.code == "adapter_invalid_result" and rejected.output_json == "{}", "invalid result fails at adapter boundary")
    adapter.env["FALA_RESULT"] = "  " + result + "  "
    var accepted = execute_subprocess(EffectorRequest("protocol-result", adapter, "impulse", "{}", "{}"))
    expect(accepted.success and accepted.output_json == result, "valid result is canonical and unredacted")
    print("effector protocol smoke ok")
