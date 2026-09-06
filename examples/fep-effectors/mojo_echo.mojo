from std.os import getenv
from std.pathlib import Path
from emberjson import Value, to_string
from fala.effector_protocol import result_message, validate_message
from fala.json import canonical_json_text


def main() raises:
    var request = Value(parse_string=validate_message(Path(getenv("FALA_EFFECTOR_MANIFEST")).read_text(), "effector.request"))
    var request_id = request.object()["message_id"].string().copy()
    var execution_id = request.object()["execution_id"].string().copy()
    var attempt = Int(request.object()["attempt"].int())
    var values = canonical_json_text(to_string(request.object()["input"].copy()))
    var result = result_message(request_id, execution_id, attempt, values_json=values)
    Path(getenv("FALA_EFFECTOR_OUTPUT_DIR") + "/result.json").write_text(result)
