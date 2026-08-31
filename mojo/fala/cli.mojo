"""Native Fala command-line entrypoint.

The library dispatcher owns parsing, validation, persistence, and deterministic
JSON envelopes.  This process wrapper only joins argv tokens and prints one
result, never invoking a shell or Python runtime.
"""

from std.sys import argv
from fala.json import quote_json_string as _json_quote
from fala.native_cli_surface import dispatch_native_command, initialize_database


def _shell_quote(value: String) -> String:
    # Keep every argv item as one tokenizer token. Adjacent quoted segments
    # preserve spaces and apostrophes without leaking backslashes.
    var result = "'"
    for ch in value.codepoint_slices():
        if ch == "'": result += "'\"'\"'"
        else: result += ch
    result += "'"
    return result

def dispatch_command(command: String) raises -> String:
    """Compatibility entrypoint retained for embedded callers."""
    return dispatch_native_command(command)


def main() raises:
    var args = argv()
    var command = ""
    for i in range(1, len(args)):
        if command != "": command += " "
        # Keep command verbs/verbs unquoted because the dispatcher matches
        # their raw prefix; quote only user values and options.
        if i <= 2: command += String(args[i])
        else: command += _shell_quote(String(args[i]))
    var output = ""
    try:
        output = dispatch_native_command(command)
    except err:
        var detail = String(err)
        print("{\"ok\":false,\"runtime\":\"mojo\",\"error\":{\"type\":\"cli_failure\",\"message\":" + _json_quote(detail) + "}}")
        raise Error("native CLI command failed")
    print(output)
    if output.find("\"ok\":false") >= 0:
        raise Error("native CLI command failed")
