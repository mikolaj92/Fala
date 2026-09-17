from std.pathlib import Path
from emberjson import Value
from fala.effector_protocol import assert_answers, request_message, result_message, validate_message
from fala.journal import validate_json_schema_value


def expect(value: Bool, message: String) raises:
    if not value: raise Error(message)


def main() raises:
    var request_golden = Path("../../conformance/fala/request.valid.json").read_text().strip()
    var request = request_message("parent", "echo", "echo", "{\"text\":\"hello\"}", "{}", "echo-output", "1")
    expect(request == request_golden and validate_message(request) == request_golden, "Mojo request matches shared golden")
    var marker = "\"id\":\""; var start = request.find(marker) + marker.byte_length(); var finish = request.find("\"", start); var request_id = String(request[byte=start:finish])
    var result = result_message("echo", "parent", "echo", request_id, "{\"text\":\"hello\"}", "ok", "echo-output", "1")
    var result_golden = Path("../../conformance/fala/result.valid.json").read_text().strip()
    expect(result == result_golden and validate_message(result) == result_golden, "Mojo result matches shared golden")
    var empty = False
    try:
        _ = validate_message("{\"config\":{},\"contract_id\":\"echo-output\",\"contract_version\":\"1\",\"from\":\"\",\"id\":\"msg:sha256:bad\",\"job\":\"echo\",\"kind\":\"request\",\"payload\":{\"text\":\"hello\"},\"protocol\":\"fala\",\"to\":\"echo\"}")
    except err:
        empty = String(err).find("fep.required") >= 0 and String(err).find("/from") >= 0
    expect(empty, "empty from fails closed as fep.required at /from")
    var config_failed = False
    try:
        _ = validate_message("{\"config\":[],\"contract_id\":\"echo-output\",\"contract_version\":\"1\",\"from\":\"parent\",\"id\":\"msg:sha256:bad\",\"job\":\"echo\",\"kind\":\"request\",\"payload\":{\"text\":\"hello\"},\"protocol\":\"fala\",\"to\":\"echo\"}")
    except err:
        config_failed = String(err).find("fep.type_invalid") >= 0 and String(err).find("/config") >= 0
    expect(config_failed, "non-object config fails closed as fep.type_invalid at /config")
    expect(request.find("\"contract_id\":\"echo-output\"") >= 0 and request.find("\"contract_version\":\"1\"") >= 0, "contract pair is required on the request")
    var missing = False
    try:
        _ = validate_message("{\"config\":{},\"from\":\"parent\",\"id\":\"msg:sha256:bad\",\"job\":\"echo\",\"kind\":\"request\",\"payload\":{\"text\":\"hello\"},\"protocol\":\"fala\",\"to\":\"echo\"}")
    except err:
        missing = String(err).find("fep.required") >= 0 and String(err).find("/contract_id") >= 0
    expect(missing, "omitted contract pair fails closed as fep.required")
    var lone = False
    try:
        _ = validate_message("{\"config\":{},\"contract_id\":\"echo-output\",\"from\":\"parent\",\"id\":\"msg:sha256:bad\",\"job\":\"echo\",\"kind\":\"request\",\"payload\":{\"text\":\"hello\"},\"protocol\":\"fala\",\"to\":\"echo\"}")
    except err:
        lone = String(err).find("fep.required") >= 0 and String(err).find("/contract_version") >= 0
    expect(lone, "lone contract_id fails closed as fep.required")
    assert_answers(request, result)
    var mismatched = False
    try:
        assert_answers(request, result_message("echo", "parent", "echo", request_id, "{\"text\":\"hello\"}", "ok", "other-output", "1"))
    except err:
        mismatched = String(err).find("fep.contract_mismatch") >= 0
    expect(mismatched, "result must echo the request contract pair")
    var version_mismatch = False
    try:
        assert_answers(request, result_message("echo", "parent", "echo", request_id, "{\"text\":\"hello\"}", "ok", "echo-output", "2"))
    except err:
        version_mismatch = String(err).find("fep.contract_mismatch") >= 0
    expect(version_mismatch, "contract_version must echo exactly")
    var empty_pair = False
    try:
        _ = validate_message("{\"config\":{},\"contract_id\":\"\",\"contract_version\":\"1\",\"from\":\"parent\",\"id\":\"msg:sha256:bad\",\"job\":\"echo\",\"kind\":\"request\",\"payload\":{\"text\":\"hello\"},\"protocol\":\"fala\",\"to\":\"echo\"}")
    except err:
        empty_pair = String(err).find("fep.required") >= 0 and String(err).find("/contract_id") >= 0
    expect(empty_pair, "empty contract_id fails closed as fep.required")
    var ref_mismatch = False
    try:
        assert_answers(request, result_message("echo", "parent", "echo", "msg:sha256:0000000000000000000000000000000000000000000000000000000000000000", "{\"text\":\"hello\"}", "ok", "echo-output", "1"))
    except err:
        ref_mismatch = String(err).find("fep.ref_mismatch") >= 0
    expect(ref_mismatch, "result ref must equal request id")
    var job_mismatch = False
    try:
        assert_answers(request, result_message("echo", "parent", "other", request_id, "{\"text\":\"hello\"}", "ok", "echo-output", "1"))
    except err:
        job_mismatch = String(err).find("fep.job_mismatch") >= 0
    expect(job_mismatch, "result job must equal request job")
    var payload_cases = Value(parse_string=Path("../../conformance/fala/payload.cases.json").read_text())
    expect(payload_cases.is_array() and len(payload_cases.array()) > 0, "L2 corpus is a nonempty array")
    for index in range(len(payload_cases.array())):
        var vector = payload_cases.array()[index].copy()
        var name = vector.object()["name"].string()
        var accept = vector.object()["accept"].bool()
        var rejected = False
        try:
            validate_json_schema_value(vector.object()["payload"].copy(), vector.object()["schema"].copy(), "/payload")
        except:
            rejected = True
        if accept:
            expect(not rejected, "L2 valid rejected: " + name)
        else:
            expect(rejected, "L2 invalid accepted: " + name)
    print("protocol golden smoke ok")
