"""Fala's native JSON boundary backed by EmberJson."""

from emberjson import Array, Object, Value, to_string
from std.collections import List

# Keep the public Fala name while delegating parsing and serialization to the
# external EmberJson value model.
struct JsonValue(Copyable, Movable):
    var value: Value

    def __init__(out self, var value: Value):
        self.value = value^

    @staticmethod
    def null() -> JsonValue:
        return JsonValue(Value())

    @staticmethod
    def boolean(value: Bool) -> JsonValue:
        return JsonValue(Value(value))

    @staticmethod
    def integer(value: Int) -> JsonValue:
        return JsonValue(Value(value))

    @staticmethod
    def floating(value: Float64) -> JsonValue:
        return JsonValue(Value(value))

    @staticmethod
    def string(value: String) -> JsonValue:
        return JsonValue(Value(value))

    def serialize(self) -> String:
        return to_string(self.value)

    def __str__(self) -> String:
        return self.serialize()


def parse_json(json_text: String) raises -> JsonValue:
    var parsed = Value(parse_string=json_text)
    return JsonValue(parsed^)


def serialize_json(value: JsonValue) -> String:
    return value.serialize()


def quote_json_string(value: String) -> String:
    """Quote a JSON string through EmberJson's encoder.

    One spelling for quotes, backslashes, and C0 controls so CLI, journal,
    and adapter envelopes stay parseable and comparable.
    """
    return to_string(Value(value.copy()))


def canonical_json_text(json_text: String) raises -> String:
    var parsed = Value(parse_string=json_text)
    var canonical = _canonical_value(parsed^)
    return to_string(canonical)
def json_values_equal(left: Value, right: Value) raises -> Bool:
    """Compare JSON values using JSON numeric equality, not text spelling."""
    var left_numeric = left.is_int() or left.is_uint() or left.is_float()
    var right_numeric = right.is_int() or right.is_uint() or right.is_float()
    if left_numeric or right_numeric:
        if not left_numeric or not right_numeric:
            return False
        return _json_numbers_equal(left, right)
    if left.is_null() or right.is_null():
        return left.is_null() and right.is_null()
    if left.is_bool() or right.is_bool():
        return left.is_bool() and right.is_bool() and left.bool() == right.bool()
    if left.is_string() or right.is_string():
        return left.is_string() and right.is_string() and left.string() == right.string()
    if left.is_array() or right.is_array():
        if not left.is_array() or not right.is_array() or len(left.array()) != len(right.array()):
            return False
        for index in range(len(left.array())):
            if not json_values_equal(left.array()[index], right.array()[index]):
                return False
        return True
    if left.is_object() or right.is_object():
        if not left.is_object() or not right.is_object() or len(left.object()) != len(right.object()):
            return False
        for pair in left.object().items():
            if pair.key not in right.object():
                return False
            if not json_values_equal(pair.value, right.object()[pair.key]):
                return False
        return True
    return False


def _json_numbers_equal(left: Value, right: Value) -> Bool:
    if left.is_int() and right.is_int():
        return left.int() == right.int()
    if left.is_uint() and right.is_uint():
        return left.uint() == right.uint()
    if left.is_int() and right.is_uint():
        return left.int() >= 0 and UInt64(left.int()) == right.uint()
    if left.is_uint() and right.is_int():
        return right.int() >= 0 and left.uint() == UInt64(right.int())
    if left.is_float() and right.is_float():
        return left.float() == right.float()
    if left.is_float():
        return _json_float_integer_equal(left.float(), right)
    return _json_float_integer_equal(right.float(), left)


def _json_float_integer_equal(numeric: Float64, integer: Value) -> Bool:
    if numeric != numeric:
        return False
    if integer.is_int():
        if numeric < -9223372036854775808.0 or numeric >= 9223372036854775808.0:
            return False
        var converted = Int(numeric)
        return Float64(converted) == numeric and converted == Int(integer.int())
    if integer.is_uint():
        if numeric < 0.0 or numeric >= 18446744073709551616.0:
            return False
        var converted = UInt64(numeric)
        return Float64(converted) == numeric and converted == integer.uint()
    return False


def _canonical_value(var value: Value) raises -> Value:
    if value.is_array():
        var result = Array(capacity=len(value.array()))
        for item in value.array():
            var child = item.copy()
            var canonical_child = _canonical_value(child^)
            result.append(canonical_child^)
        return Value(result^)
    if value.is_object():
        var keys = List[String]()
        for key in value.object().keys():
            keys.append(key.copy())
        _sort_strings(keys)
        var result = Object(capacity=len(keys))
        for key in keys:
            var child = value.object()[key].copy()
            var canonical_child = _canonical_value(child^)
            result[key] = canonical_child^
        return Value(result^)
    return value^


def _sort_strings(mut values: List[String]):
    var i = 1
    while i < len(values):
        var key = values[i].copy()
        var j = i
        while j > 0 and values[j - 1] > key:
            values[j] = values[j - 1].copy()
            j -= 1
        values[j] = key^
        i += 1
