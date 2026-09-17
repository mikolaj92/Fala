from std.os import getenv
from std.pathlib import Path
from emberjson import Value, to_string
from fala.sdk import output
from fala.json import canonical_json_text


def main() raises:
    var request_text = Path(getenv("FALA_EFFECTOR_MANIFEST")).read_text()
    var request = Value(parse_string=request_text)
    var payload = canonical_json_text(to_string(request.object()["payload"].copy()))
    Path(getenv("FALA_EFFECTOR_OUTPUT_DIR") + "/result.json").write_text(output(request_text, payload))
