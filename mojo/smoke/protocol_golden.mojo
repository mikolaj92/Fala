from std.pathlib import Path
from fala.effector_protocol import request_message, result_message, validate_message


def expect(value: Bool, message: String) raises:
    if not value: raise Error(message)


def main() raises:
    var request_golden = Path("../../conformance/fala/request.valid.json").read_text().strip()
    var request = request_message("parent", "echo", "echo", "{\"text\":\"hello\"}")
    expect(request == request_golden and validate_message(request) == request_golden, "Mojo request matches shared golden")
    var marker = "\"id\":\""; var start = request.find(marker) + marker.byte_length(); var finish = request.find("\"", start); var request_id = String(request[byte=start:finish])
    var result = result_message("echo", "parent", "echo", request_id, "{\"text\":\"hello\"}")
    expect(result == Path("../../conformance/fala/result.valid.json").read_text().strip(), "Mojo result matches shared golden")
    var empty_from = False
    try:
        _ = validate_message("{\"config\":{},\"from\":\"\",\"id\":\"msg:sha256:ab48e1001217df9736d19a2781fc2b45e0b98a46217ea0181ee0e5377261f3c8\",\"job\":\"echo\",\"kind\":\"request\",\"payload\":{\"text\":\"hello\"},\"protocol\":\"fala\",\"to\":\"echo\"}")
    except err:
        empty_from = String(err).find("fep.required") >= 0 and String(err).find("/from") >= 0
    expect(empty_from, "empty from fails closed as fep.required")
    var config_failed = False
    try:
        _ = validate_message("{\"config\":[],\"from\":\"parent\",\"id\":\"msg:sha256:ab48e1001217df9736d19a2781fc2b45e0b98a46217ea0181ee0e5377261f3c8\",\"job\":\"echo\",\"kind\":\"request\",\"payload\":{\"text\":\"hello\"},\"protocol\":\"fala\",\"to\":\"echo\"}")
    except err:
        config_failed = String(err).find("fep.type_invalid") >= 0 and String(err).find("/config") >= 0
    expect(config_failed, "non-object config fails closed as fep.type_invalid at /config")
    print("protocol golden smoke ok")
