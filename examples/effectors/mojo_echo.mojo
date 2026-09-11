from std.os import getenv
from std.pathlib import Path
from emberjson import Value, to_string
from fala.effector_protocol import result_message, validate_message
from fala.json import canonical_json_text


def main() raises:
    var request = Value(parse_string=validate_message(Path(getenv("FALA_EFFECTOR_MANIFEST")).read_text(), "request"))
    var sender = request.object()["to"].string().copy()
    var recipient = request.object()["from"].string().copy()
    var job = request.object()["job"].string().copy()
    var ref = request.object()["id"].string().copy()
    var payload = canonical_json_text(to_string(request.object()["payload"].copy()))
    var result = result_message(sender, recipient, job, ref, payload)
    Path(getenv("FALA_EFFECTOR_OUTPUT_DIR") + "/result.json").write_text(result)
