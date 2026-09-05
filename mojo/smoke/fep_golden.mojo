from std.pathlib import Path
from fala.effector_protocol import request_message, result_message, validate_message


def expect(value: Bool, message: String) raises:
    if not value: raise Error(message)


def main() raises:
    var request_golden = Path("../../conformance/fep-v1/request.valid.json").read_text().strip()
    var request = request_message("run-golden", "echo", "run-golden:echo", 1, "impulse-1", "sha256:process", "sha256:path", "echo", "{\"text\":\"hello\"}", "{}", "schema:sha256:echo")
    expect(request == request_golden and validate_message(request) == request_golden, "Mojo request matches shared golden")
    var marker = "\"message_id\":\""; var start = request.find(marker) + marker.byte_length(); var finish = request.find("\"", start); var request_id = String(request[byte=start:finish])
    var result = result_message(request_id, "run-golden:echo", 1, values_json="{\"text\":\"hello\"}")
    expect(result == Path("../../conformance/fep-v1/result.valid.json").read_text().strip(), "Mojo result matches shared golden")
    print("FEP golden smoke ok")
