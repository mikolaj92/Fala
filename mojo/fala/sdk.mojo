"""Native, non-executable effector SDK boundary.

This module accepts JSON text rather than mappings so the Mojo surface stays
explicit and deterministic. Malformed JSON and wrong top-level types raise
stable ``sdk.invalid_json`` or ``sdk.invalid_type`` diagnostics. It never reads
environment/process state and does not invoke a handler: host execution is
represented by SdkUnavailableError.
"""

from emberjson import Array, Object, Value, to_string, write_pretty
from std.pathlib import Path
from .json import canonical_json_text


comptime INJECTED_INPUT_KEYS = "conduction,upstream_reactions"


struct SdkUnavailableError(Copyable, Movable):
    var code: String
    var message: String

    def __init__(out self, code: String = "sdk.execution_unavailable", message: String = "native effector execution is unavailable"):
        self.code = code
        self.message = message

    def is_unavailable(self) -> Bool:
        return self.code == "sdk.execution_unavailable"

    def __str__(self) -> String:
        return self.message


struct SdkError(Copyable, Movable):
    """Stable diagnostics for malformed SDK JSON inputs."""

    var code: String
    var path: String
    var message: String

    def __init__(out self, code: String, path: String, message: String):
        self.code = code
        self.path = path
        self.message = message

    def __str__(self) -> String:
        return self.code + " at " + self.path + ": " + self.message


def _raise_sdk(code: String, path: String, message: String) raises:
    raise Error(SdkError(code, path, message).__str__())


def _parse_value(text: String, path: String) raises -> Value:
    try:
        return Value(parse_string=text)
    except err:
        _raise_sdk("sdk.invalid_json", path, "malformed JSON")
    return Value(Object(capacity=0))


def _object_value(text: String, path: String = "/") raises -> Value:
    var parsed = _parse_value(text, path)
    if not parsed.is_object():
        _raise_sdk("sdk.invalid_type", path, "expected JSON object")
    return parsed^


def _array_value(text: String, path: String = "/") raises -> Value:
    var parsed = _parse_value(text, path)
    if not parsed.is_array():
        _raise_sdk("sdk.invalid_type", path, "expected JSON array")
    return parsed^


def _canonical_object(text: String, path: String = "/") raises -> String:
    var value = _object_value(text, path)
    return canonical_json_text(to_string(value^))


def _canonical_array(text: String, path: String = "/", objects_only: Bool = False) raises -> String:
    var value = _array_value(text, path)
    if not objects_only:
        return canonical_json_text(to_string(value^))
    var filtered = Array(capacity=len(value.array()))
    for item in value.array():
        if item.is_object():
            filtered.append(item.copy())
    return canonical_json_text(to_string(Value(filtered^)))


def _manifest_input(manifest_json: String) raises -> String:
    var manifest = _object_value(manifest_json, "/manifest")
    if "input" not in manifest.object():
        return "{}"
    var input = manifest.object()["input"].copy()
    if not input.is_object():
        _raise_sdk("sdk.invalid_type", "/manifest/input", "expected JSON object")
    return canonical_json_text(to_string(input^))


def _field_object(source_json: String, key: String, path: String = "/source") raises -> String:
    var source = _object_value(source_json, path)
    if key not in source.object():
        return "{}"
    var value = source.object()[key].copy()
    if not value.is_object():
        _raise_sdk("sdk.invalid_type", path + "/" + key, "expected JSON object")
    return canonical_json_text(to_string(value^))


def _field_array(source_json: String, key: String, objects_only: Bool = False, path: String = "/source") raises -> String:
    var source = _object_value(source_json, path)
    if key not in source.object():
        return "[]"
    var raw = source.object()[key].copy()
    if not raw.is_array():
        _raise_sdk("sdk.invalid_type", path + "/" + key, "expected JSON array")
    if not objects_only:
        return canonical_json_text(to_string(raw^))
    var filtered = Array(capacity=len(raw.array()))
    for item in raw.array():
        if item.is_object():
            filtered.append(item.copy())
    return canonical_json_text(to_string(Value(filtered^)))


def load_manifest(manifest_json: String) raises -> String:
    """Canonicalize a supplied manifest object; reject malformed/non-object input."""
    return _canonical_object(manifest_json, "/manifest")


def load_manifest_object(manifest_json: String) raises -> Value:
    """Return a parsed manifest object, rejecting malformed/non-object input."""
    return _object_value(manifest_json, "/manifest")


def input_values(manifest_json: String) raises -> String:
    return _manifest_input(manifest_json)


def declared_inputs(manifest_json: String) raises -> String:
    var source = Value(parse_string=_manifest_input(manifest_json))
    var declared = Object(capacity=len(source.object()))
    for pair in source.object().items():
        if pair.key != "conduction" and pair.key != "upstream_reactions":
            declared[pair.key] = pair.value.copy()
    return canonical_json_text(to_string(Value(declared^)))


def conduction(manifest_json: String) raises -> String:
    return _field_object(_manifest_input(manifest_json), "conduction", "/manifest/input")


def upstream_reactions(manifest_json: String) raises -> String:
    return _field_array(_manifest_input(manifest_json), "upstream_reactions", True, "/manifest/input")


def find_reaction(manifest_json: String, kind: String) raises -> String:
    var reactions = Value(parse_string=upstream_reactions(manifest_json))
    var index = len(reactions.array())
    while index > 0:
        index -= 1
        var reaction = reactions.array()[index].copy()
        if "kind" in reaction.object() and reaction.object()["kind"].is_string() and reaction.object()["kind"].string() == kind:
            return canonical_json_text(to_string(reaction^))
    return "{}"


def output_reactions(effector_output_json: String) raises -> String:
    return _field_array(effector_output_json, "reactions", True)


def find_output_reaction(effector_output_json: String, kind: String) raises -> String:
    var reactions = Value(parse_string=output_reactions(effector_output_json))
    var index = len(reactions.array())
    while index > 0:
        index -= 1
        var reaction = reactions.array()[index].copy()
        if "kind" in reaction.object() and reaction.object()["kind"].is_string() and reaction.object()["kind"].string() == kind:
            return canonical_json_text(to_string(reaction^))
    return "{}"


def output_metadata(effector_output_json: String) raises -> String:
    return _field_object(effector_output_json, "metadata")


def config(manifest_json: String) raises -> String:
    return _field_object(manifest_json, "config")


def output(
    values_json: String = "{}",
    associations_json: String = "[]",
    reactions_json: String = "[]",
    metadata_json: String = "{}",
) raises -> String:
    var envelope = Object(capacity=4)
    envelope["values"] = Value(parse_string=_canonical_object(values_json, "/values"))
    envelope["associations"] = Value(parse_string=_canonical_array(associations_json, "/associations", True))
    envelope["reactions"] = Value(parse_string=_canonical_array(reactions_json, "/reactions"))
    envelope["metadata"] = Value(parse_string=_canonical_object(metadata_json, "/metadata"))
    return canonical_json_text(to_string(Value(envelope^)))


def serialize_result(result_json: String) raises -> String:
    """Pretty-print a result object after recursively sorting its keys."""
    var canonical = Value(parse_string=_canonical_object(result_json, "/result"))
    return write_pretty(canonical)


def write_result(result_json: String, output_path: String) raises -> String:
    """Write deterministic pretty JSON to a caller-supplied native path."""
    var text = serialize_result(result_json)
    Path(output_path).write_text(text)
    return output_path


def run_manifest_effector() -> SdkUnavailableError:
    """Execution is intentionally unavailable without a native host boundary."""
    return SdkUnavailableError()
