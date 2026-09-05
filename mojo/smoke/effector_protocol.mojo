from std.pathlib import Path
from fala.effector_protocol import request_message, result_message, validate_message


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
    print("effector protocol smoke ok")
