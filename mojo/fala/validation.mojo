"""Native validation helpers for Fala domain and runtime boundaries.

Validation is deliberately independent of the model layer.  Helpers return a
ValidationError whose empty code denotes success; callers can inspect the code,
path, and message without depending on exception interop.
"""

from std.collections import List
from .errors import ValidationError


def _ok() -> ValidationError:
    return ValidationError("", "", "")


def _fail(code: String, path: String, message: String) -> ValidationError:
    return ValidationError(code, path, message)


def is_valid_runtime_id(value: String) -> Bool:
    var n = value.byte_length()
    if n < 1 or n > 128:
        return False
    var first = value[byte=0]
    if not ((first >= "A" and first <= "Z") or (first >= "a" and first <= "z")):
        return False
    for i in range(1, n):
        var c = value[byte=i]
        if ((c >= "A" and c <= "Z") or (c >= "a" and c <= "z")
                or (c >= "0" and c <= "9") or c == "_" or c == "." or c == "-"):
            continue
        return False
    return True


def validate_runtime_id(value: String, path: String = "id") -> ValidationError:
    if not is_valid_runtime_id(value):
        return _fail("invalid_runtime_id", path, "must match ^[A-Za-z][A-Za-z0-9_.-]{0,127}$")
    return _ok()


def validate_positive_number(value: Float64, path: String) -> ValidationError:
    if value <= 0.0:
        return _fail("not_positive", path, "must be greater than 0")
    return _ok()


def validate_optional_positive_number(value: Float64, path: String) -> ValidationError:
    # Native models use -1 as the explicit unlimited/absent sentinel.
    if value < 0.0:
        return _ok()
    return validate_positive_number(value, path)


def _contains(values: List[String], value: String) -> Bool:
    for item in values:
        if item == value:
            return True
    return False


def validate_known_fields(fields: List[String], known: List[String], path: String = "") -> ValidationError:
    for field in fields:
        if not _contains(known, field):
            return _fail("unknown_field", path + "." + field, "unknown field: " + field)
    return _ok()


def validate_unique_values(values: List[String], path: String) -> ValidationError:
    for i in range(values.__len__()):
        for j in range(i + 1, values.__len__()):
            if values[i] == values[j]:
                return _fail("duplicate_value", path, "duplicate value: " + values[i])
    return _ok()


def validate_unique_ids(ids: List[String], path: String) -> ValidationError:
    var result = validate_unique_values(ids, path)
    if not result.is_ok():
        result.code = "duplicate_id"
        result.message = "duplicate id: " + ids[0]
        for i in range(ids.__len__()):
            for j in range(i + 1, ids.__len__()):
                if ids[i] == ids[j]:
                    result.message = "duplicate id: " + ids[i]
                    return ValidationError(result.code, result.path, result.message)
    return ValidationError(result.code, result.path, result.message)


def validate_known_references(refs: List[String], known: List[String], path: String) -> ValidationError:
    for ref in refs:
        if not _contains(known, ref):
            return _fail("unknown_reference", path, "reference unknown id: " + ref)
    return _ok()


def validate_no_self_reference(identifier: String, refs: List[String], path: String) -> ValidationError:
    for ref in refs:
        if ref == identifier:
            return _fail("self_reference", path, "cannot depend on itself")
    return _ok()


def validate_adapter_boundary(
    kind: String,
    command: String,
    adapter_ref: String,
    cwd: String,
    env_count: Int,
    inherit_env_count: Int,
    timeout_seconds: Float64,
) -> ValidationError:
    if kind == "fala_runtime":
        return _fail("unsupported_adapter_kind", "adapter.kind", "fala_runtime is not part of Fala; use subprocess with a separate journal")
    if kind != "manual_homeostat" and kind != "native_function" and kind != "subprocess":
        return _fail("unknown_adapter_kind", "adapter.kind", "unknown adapter kind: " + kind)
    if timeout_seconds >= 0.0:
        var positive = validate_positive_number(timeout_seconds, "adapter.timeout_seconds")
        if not positive.is_ok():
            return ValidationError(positive.code, positive.path, positive.message)
    if kind == "subprocess":
        if command == "":
            return _fail("adapter_boundary", "adapter.command", "subprocess adapter requires non-empty command")
        if adapter_ref != "":
            return _fail("adapter_boundary", "adapter.ref", "subprocess adapter cannot define ref")
        return _ok()
    if inherit_env_count > 0:
        return _fail("adapter_boundary", "adapter.inherit_env", kind + " adapter cannot define inherit_env")
    if kind == "native_function":
        if adapter_ref == "":
            return _fail("adapter_boundary", "adapter.ref", "native_function adapter requires ref")
        if command != "": return _fail("adapter_boundary", "adapter.command", "native_function adapter cannot define command")
        if cwd != "": return _fail("adapter_boundary", "adapter.cwd", "native_function adapter cannot define cwd")
        if env_count > 0: return _fail("adapter_boundary", "adapter.env", "native_function adapter cannot define env")
        return _ok()
    if kind == "manual_homeostat":
        if command != "": return _fail("adapter_boundary", "adapter.command", "manual_homeostat adapter cannot define command")
        if adapter_ref != "": return _fail("adapter_boundary", "adapter.ref", "manual_homeostat adapter cannot define ref")
        if cwd != "": return _fail("adapter_boundary", "adapter.cwd", "manual_homeostat adapter cannot define cwd")
        if env_count > 0: return _fail("adapter_boundary", "adapter.env", "manual_homeostat adapter cannot define env")
        if timeout_seconds >= 0.0: return _fail("adapter_boundary", "adapter.timeout_seconds", "manual_homeostat adapter cannot define timeout_seconds")
        return _ok()
    return _fail("unknown_adapter_kind", "adapter.kind", "unknown adapter kind: " + kind)


def validate_acyclic(ids: List[String], dependencies: List[List[String]], path: String = "") -> ValidationError:
    if ids.__len__() != dependencies.__len__():
        return _fail("invalid_graph", path, "ids and dependencies must have equal length")
    for i in range(ids.__len__()):
        var duplicate = validate_no_self_reference(ids[i], dependencies[i], path + "." + ids[i])
        if not duplicate.is_ok():
            return ValidationError(duplicate.code, duplicate.path, duplicate.message)
        for dependency in dependencies[i]:
            if not _contains(ids, dependency):
                return _fail("unknown_reference", path + "." + ids[i], "reference unknown id: " + dependency)
    # Repeatedly remove nodes with no remaining incoming dependency.  If nodes
    # remain, the dependency graph contains a cycle.
    var removed = List[Bool](length=ids.__len__(), fill=False)
    var count = 0
    while count < ids.__len__():
        var progressed = False
        for i in range(ids.__len__()):
            if removed[i]: continue
            var blocked = False
            for dependency in dependencies[i]:
                for j in range(ids.__len__()):
                    if ids[j] == dependency and not removed[j]: blocked = True
            if not blocked:
                removed[i] = True
                count += 1
                progressed = True
        if not progressed:
            return _fail("cycle", path, "graph contains a cycle")
    return _ok()
