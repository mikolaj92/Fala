from std.os import getenv
from std.pathlib import Path
from emberjson import Value, to_string
from fala.effector_protocol import result_message, validate_message
from fala.json import canonical_json_text


def main() raises:
    var request = Value(parse_string=validate_message(Path(getenv("FALA_EFFECTOR_MANIFEST").value()).read_text(), "effector.request"))
    var result = result_message(request.object()["message_id"].string(), request.object()["execution_id"].string(), Int(request.object()["attempt"].int()), values_json=canonical_json_text(to_string(request.object()["input"].copy())))
    Path(getenv("FALA_EFFECTOR_OUTPUT_DIR").value() + "/result.json").write_text(result)
