"""Declaration validation for the JSON Schema subset actually evaluated by Fala.

Every accepted keyword is evaluated by the payload validator.  Unknown or
non-supported keywords are rejected at declaration time, so no stored contract
carries a silently ignored rule.  No reference resolution is claimed.
"""
from emberjson import Value
from std.collections import List


def _error(path: String, message: String, code: String) raises:
    raise Error(code + " at " + path + ": " + message)


def _kind(kind: String) -> Bool:
    return kind in ["null", "boolean", "object", "array", "number", "integer", "string"]


def _pointer(path: String, key: String) -> String:
    return path + "/" + key.replace("~", "~0").replace("/", "~1")


def validate_schema(schema: Value, path: String = "/output_schema", code: String = "schema.invalid") raises:
    """Reject any declaration the payload evaluator cannot fully execute."""
    if not schema.is_object(): _error(path, "expected schema object", code)
    for pair in schema.object().items():
        var key = pair.key
        var child = pair.value.copy()
        var pointer = _pointer(path, key)
        if key == "type":
            if child.is_string():
                if not _kind(child.string()): _error(pointer, "unknown type", code)
            elif child.is_array():
                if len(child.array()) == 0: _error(pointer, "empty type array", code)
                var seen = List[String]()
                for item in child.array():
                    if not item.is_string(): _error(pointer, "expected type name", code)
                    if not _kind(item.string()) or _contains(seen, item.string()):
                        _error(pointer, "unknown or duplicate type", code)
                    seen.append(item.string())
            else: _error(pointer, "expected type name or array", code)
        elif key == "properties":
            if not child.is_object(): _error(pointer, "expected object", code)
            for property in child.object().items():
                validate_schema(property.value, _pointer(pointer, property.key), code)
        elif key == "items":
            validate_schema(child, pointer, code)
        elif key == "oneOf" or key == "anyOf" or key == "allOf":
            if not child.is_array() or len(child.array()) == 0:
                _error(pointer, "expected nonempty schema array", code)
            for index in range(len(child.array())):
                validate_schema(child.array()[index], pointer + "/" + String(index), code)
        elif key == "required":
            if not child.is_array(): _error(pointer, "expected string array", code)
            var names = List[String]()
            for item in child.array():
                if not item.is_string(): _error(pointer, "expected field name", code)
                if _contains(names, item.string()): _error(pointer, "duplicate field", code)
                names.append(item.string())
        elif key == "additionalProperties":
            if not child.is_bool(): _error(pointer, "only boolean additionalProperties is supported", code)
        elif key == "enum":
            if not child.is_array() or len(child.array()) == 0:
                _error(pointer, "expected nonempty enum array", code)
        elif key == "const":
            pass
        elif key == "minimum" or key == "maximum":
            if not child.is_int() and not child.is_uint() and not child.is_float():
                _error(pointer, "expected number", code)
        elif key == "minLength" or key == "maxLength" or key == "minItems" or key == "maxItems":
            if child.is_int():
                if child.int() < 0: _error(pointer, "expected nonnegative integer", code)
            elif not child.is_uint(): _error(pointer, "expected nonnegative integer", code)
        elif key == "$schema" or key == "$id" or key == "$comment" or key == "title" or key == "description":
            if not child.is_string(): _error(pointer, "expected annotation string", code)
        elif key == "deprecated" or key == "readOnly" or key == "writeOnly":
            if not child.is_bool(): _error(pointer, "expected annotation boolean", code)
        else:
            _error(pointer, "unsupported JSON Schema keyword " + key, code)


def _contains(values: List[String], wanted: String) -> Bool:
    for value in values:
        if value == wanted: return True
    return False
