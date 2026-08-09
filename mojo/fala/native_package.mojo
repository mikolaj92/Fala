"""Strict native JSON package manifest loader backed by EmberJson.

Native manifests are JSON only. The decoder rejects unknown fields and duplicate
keys (the EmberJson parser rejects duplicate object members), while preserving
nested config/schema values as canonical JSON text for downstream native APIs.
"""

from emberjson import Object, Value, to_string
from std.collections import Dict, List
from std.pathlib import Path, cwd
from fala.json import canonical_json_text

@fieldwise_init
struct _AdapterData(Copyable, Movable):
    var kind: String
    var reference: String
    var command: List[String]
    var cwd: String
    var env: Dict[String, String]
    var inherit_env: List[String]
    var timeout_seconds: Float64


struct PackageManifestError(Copyable, Movable):
    """Stable diagnostic data for callers that do not use raised Error text."""
    var code: String
    var path: String
    var message: String

    def __init__(out self, message: String, code: String = "manifest.invalid", path: String = ""):
        self.code = code
        self.path = path
        self.message = message

    def __str__(self) -> String:
        return self.code + " at " + self.path + ": " + self.message


struct PackageEffector(Copyable, Movable):
    var id: String
    var conduction: List[String]
    var capability: String
    var adapter_kind: String
    var adapter_ref: String
    var adapter_command: List[String]
    var adapter_cwd: String
    var adapter_env: Dict[String, String]
    var adapter_inherit_env: List[String]
    var timeout_seconds: Float64
    var config_json: String
    var retry_policy: String
    var title: String
    var description: String
    var tags: List[String]

    def __init__(out self, id: String, conduction: List[String] = List[String](), capability: String = "", adapter_kind: String = "", adapter_ref: String = "", adapter_command: List[String] = List[String](), adapter_cwd: String = "", adapter_env: Dict[String, String] = Dict[String, String](), adapter_inherit_env: List[String] = List[String](), timeout_seconds: Float64 = 0.0, config_json: String = "", title: String = "", description: String = "", tags: List[String] = List[String](), retry_policy: String = "automatic"):
        self.id = id
        self.conduction = conduction.copy()
        self.capability = capability
        self.adapter_kind = adapter_kind
        self.adapter_ref = adapter_ref
        self.adapter_command = adapter_command.copy()
        self.adapter_cwd = adapter_cwd
        self.adapter_env = adapter_env.copy()
        self.adapter_inherit_env = adapter_inherit_env.copy()
        self.timeout_seconds = timeout_seconds
        self.retry_policy = retry_policy
        self.config_json = config_json
        self.title = title
        self.description = description
        self.tags = tags.copy()


struct PackageCorrelationPath(Copyable, Movable):
    var id: String
    var effectors: List[PackageEffector]
    var title: String
    var description: String
    var tags: List[String]
    var accumulate_upstream_reactions: Bool

    def __init__(out self, id: String, effectors: List[PackageEffector], title: String = "", description: String = "", tags: List[String] = List[String](), accumulate_upstream_reactions: Bool = False):
        self.id = id
        self.effectors = effectors.copy()
        self.title = title
        self.description = description
        self.tags = tags.copy()
        self.accumulate_upstream_reactions = accumulate_upstream_reactions
struct PackageManifest(Copyable, Movable):
    """Validated strict JSON package manifest."""
    var id: String
    var version: String
    var correlation_paths: List[PackageCorrelationPath]
    var title: String
    var description: String
    var tags: List[String]
    var impulse_types_json: String
    var impulse_relations_json: String
    var association_kinds_json: String
    var reaction_kinds_json: String
    var capabilities_json: String
    var runtime_json: String

    def __init__(out self, id: String, version: String, correlation_paths: List[PackageCorrelationPath], title: String = "", description: String = "", tags: List[String] = List[String](), impulse_types_json: String = "[]", impulse_relations_json: String = "[]", association_kinds_json: String = "[]", reaction_kinds_json: String = "[]", capabilities_json: String = "[]", runtime_json: String = "null"):
        self.id = id
        self.version = version
        self.correlation_paths = correlation_paths.copy()
        self.title = title
        self.description = description
        self.tags = tags.copy()
        self.impulse_types_json = impulse_types_json
        self.impulse_relations_json = impulse_relations_json
        self.association_kinds_json = association_kinds_json
        self.reaction_kinds_json = reaction_kinds_json
        self.capabilities_json = capabilities_json
        self.runtime_json = runtime_json


def _pointer_token(value: String) -> String:
    var result = String("")
    for ch in value.codepoint_slices():
        if ch == "~": result += "~0"
        elif ch == "/": result += "~1"
        else: result += ch
    return result^


def _manifest_parent(path: String) raises -> String:
    var expanded = Path(path).expanduser()
    var text = expanded.__fspath__()
    if not path.startswith("/"):
        text = (cwd() / expanded).__fspath__()
    var last = -1
    var i = 0
    while i < text.byte_length():
        if String(text[byte=i]) == "/": last = i
        i += 1
    if last < 0: return String(".")
    if last == 0: return String("/")
    var parent = String("")
    for index in range(last): parent += text[byte=index]
    return parent^


def _normalize_cwd(value: String, manifest_parent: String, path: String) raises -> String:
    """Resolve cwd against the manifest while applying one lexical contract.

    Relative paths are rooted at the manifest directory.  Absolute paths remain
    absolute; empty and ``.`` segments (including duplicate separators) are
    removed, while traversal is rejected before any filesystem operation.
    """
    var source = value if value.startswith("/") else manifest_parent + "/" + value
    var absolute = source.startswith("/")
    var normalized = String("/") if absolute else String("")
    var start = 0
    var i = 0
    while i <= source.byte_length():
        var at_end = i == source.byte_length()
        var ch = String(source[byte=i]) if not at_end else String("/")
        if not at_end and (ch == "\0" or ch == "\n" or ch == "\r" or ch == "\t" or ch == "\v" or ch == "\f" or ch == "\b" or ch == "\a" or ch == "\x1b"):
            _error("manifest.value", path, "cwd must not contain control characters")
        if at_end or ch == "/":
            var segment = String(source[byte=start:i])
            if segment == "..": _error("manifest.value", path, "cwd must not contain traversal")
            if segment != "" and segment != ".":
                if normalized != "" and normalized != "/": normalized += "/"
                normalized += segment
            start = i + 1
        i += 1
    if normalized == "": normalized = "."
    return normalized^
def _base_env_key(value: String) -> Bool:
    return value == "PATH" or value == "HOME" or value == "TMPDIR" or value == "LANG" or value == "LC_ALL" or value == "TZ"


def _contains(values: List[String], wanted: String) -> Bool:
    for value in values:
        if value == wanted: return True
    return False


def _env_name(value: String, path: String) raises -> String:
    """Require portable environment variable names for deterministic launchers."""
    if value == "": _error("manifest.value", path, "environment key must be nonempty")
    var first = True
    for ch_slice in value.codepoint_slices():
        var ch = String(ch_slice)
        if first:
            if ch != "_" and (ch < "A" or ch > "Z") and (ch < "a" or ch > "z"):
                _error("manifest.value", path, "environment key must match ^[A-Za-z_][A-Za-z0-9_]*$")
            first = False
        elif ch != "_" and (ch < "A" or ch > "Z") and (ch < "a" or ch > "z") and (ch < "0" or ch > "9"):
            _error("manifest.value", path, "environment key must match ^[A-Za-z_][A-Za-z0-9_]*$")
    return value


def _validate_env_interpolation(value: String, path: String, inherited: List[String]) raises:
    var marker = value.find("${env:")
    if marker < 0: return
    if marker != 0 or not value.endswith("}"):
        _error("manifest.value", path, "environment interpolation must be exactly ${env:NAME}")
    var key = String(value[byte=6:value.byte_length() - 1])
    _ = _env_name(key, path)
    if not _contains(inherited, key) and not _base_env_key(key):
        _error("manifest.value", path, "environment interpolation is not allowlisted: " + key)


def _error(code: String, path: String, message: String) raises:
    raise Error(code + " at " + path + ": " + message)


def _required(ref root: Value, key: String, path: String) raises -> Value:
    if not root.is_object() or key not in root.object():
        _error("manifest.missing", path + "/" + _pointer_token(key), "required field is missing")
    return root.object()[key].copy()


def _optional(ref root: Value, key: String) raises -> Value:
    if root.is_object() and key in root.object():
        return root.object()[key].copy()
    return Value()


def _string(value: Value, path: String) raises -> String:
    if not value.is_string():
        _error("manifest.type", path, "expected string")
    return value.string().copy()

def _reject_null_fields(ref value: Value, fields: List[String], path: String) raises:
    if not value.is_object(): return
    for key in fields:
        if key in value.object() and value.object()[key].is_null():
            _error("manifest.type", path + "/" + _pointer_token(key), "explicit null is not allowed")


def _schema_number(value: Value, path: String) raises:
    if not value.is_int() and not value.is_uint() and not value.is_float():
        _error("manifest.type", path, "expected number")
    return

def _schema_string_array(value: Value, path: String) raises:
    if not value.is_array():
        _error("manifest.type", path, "expected array")
    var i = 0
    for item in value.array():
        _ = _string(item.copy(), path + "/" + String(i))
        i += 1


def _json_schema(value: Value, path: String) raises:
    """Validate the structural subset of Draft JSON Schema supported natively.

    EmberJson gives us lossless JSON values but no schema engine.  We therefore
    validate keyword shapes and recursively retain the schema, without claiming
    to evaluate constraints or resolve external references.
    """
    if not value.is_object():
        _error("manifest.type", path, "expected object")
    var object = value.object().copy()
    for pair in object.items():
        var key = pair.key
        var child = pair.value.copy()
        var child_path = path + "/" + _pointer_token(key)
        if key == "type":
            if child.is_string():
                var kind = child.string()
                if kind != "null" and kind != "boolean" and kind != "object" and kind != "array" and kind != "number" and kind != "integer" and kind != "string":
                    _error("manifest.value", child_path, "unsupported JSON Schema type")
            elif child.is_array():
                _schema_string_array(child^, child_path)
            else:
                _error("manifest.type", child_path, "expected string or array of strings")
        elif key == "properties" or key == "patternProperties" or key == "$defs" or key == "definitions" or key == "dependentSchemas":
            if not child.is_object(): _error("manifest.type", child_path, "expected object")
            for item in child.object().items():
                _json_schema(item.value.copy(), child_path + "/" + _pointer_token(item.key))
        elif key == "required":
            _schema_string_array(child^, child_path)
        elif key == "items" or key == "contains" or key == "propertyNames" or key == "additionalProperties" or key == "unevaluatedProperties" or key == "unevaluatedItems" or key == "not":
            if key == "additionalProperties" or key == "unevaluatedProperties":
                if child.is_bool(): continue
            _json_schema(child^, child_path)
        elif key == "prefixItems" or key == "allOf" or key == "anyOf" or key == "oneOf":
            if not child.is_array(): _error("manifest.type", child_path, "expected array of schemas")
            var i = 0
            for schema in child.array():
                _json_schema(schema.copy(), child_path + "/" + String(i))
                i += 1
        elif key == "enum":
            if not child.is_array(): _error("manifest.type", child_path, "expected array")
        elif key == "const" or key == "$comment" or key == "title" or key == "description" or key == "$id" or key == "$schema" or key == "$ref" or key == "$anchor" or key == "$dynamicRef":
            if key != "const": _ = _string(child^, child_path)
        elif key == "additionalItems" or key == "exclusiveMinimum" or key == "exclusiveMaximum":
            if key == "additionalItems" and child.is_bool(): continue
            _schema_number(child^, child_path)
        elif key == "minimum" or key == "maximum" or key == "multipleOf" or key == "minLength" or key == "maxLength" or key == "minItems" or key == "maxItems" or key == "minProperties" or key == "maxProperties":
            _schema_number(child^, child_path)
        elif key == "pattern" or key == "format" or key == "contentEncoding" or key == "contentMediaType" or key == "id":
            _ = _string(child^, child_path)
        elif key == "uniqueItems" or key == "deprecated" or key == "readOnly" or key == "writeOnly":
            if not child.is_bool(): _error("manifest.type", child_path, "expected boolean")
        elif key == "dependencies" or key == "dependentRequired":
            if not child.is_object(): _error("manifest.type", child_path, "expected object")
            for item in child.object().items():
                if item.value.is_array(): _schema_string_array(item.value.copy(), child_path + "/" + _pointer_token(item.key))
                else: _json_schema(item.value.copy(), child_path + "/" + _pointer_token(item.key))
        else:
            _error("manifest.unknown", child_path, "unknown JSON Schema keyword")


def _nonempty(value: Value, path: String) raises -> String:
    var result = _string(value, path)
    if result == "":
        _error("manifest.value", path, "must be nonempty")
    return result
def _package_version(ref root: Value) raises -> String:
    """Validate the native package version while retaining the current default."""
    var version = String("2")
    if not root.is_object() or "version" not in root.object(): return version
    var value = root.object()["version"].copy()
    if value.is_null():
        _error("manifest.type", "/version", "explicit null is not allowed")
    if value.is_int(): version = String(value.int())
    elif value.is_uint(): version = String(value.uint())
    elif value.is_string(): version = _nonempty(value^, "/version")
    else:
        _error("manifest.type", "/version", "expected string or integer")
    return version


def _strings(value: Value, path: String) raises -> List[String]:
    if not value.is_array():
        _error("manifest.type", path, "expected array of strings")
    var result = List[String]()
    var i = 0
    for item in value.array():
        var text = _nonempty(item.copy(), path + "/" + String(i))
        for prior in result:
            if prior == text:
                _error("manifest.duplicate", path + "/" + String(i), "duplicate value")
        result.append(text^)
        i += 1
    return result^


def _number(value: Value, path: String) raises -> Float64:
    if value.is_float():
        return value.float()
    if value.is_int():
        return Float64(value.int())
    if value.is_uint():
        return Float64(value.uint())
    _error("manifest.type", path, "expected number")
    return 0.0


def _known(ref value: Value, allowed: List[String], path: String) raises:
    for key in value.object().keys():
        var accepted = False
        for item in allowed:
            if key == item:
                accepted = True
                break
        if not accepted:
            _error("manifest.unknown", path + "/" + _pointer_token(key), "unknown field")


def _runtime_id(value: Value, path: String, label: String = "identifier") raises -> String:
    var result = _nonempty(value, path)
    if result.byte_length() > 128:
        _error("manifest.value", path, label + " must match ^[A-Za-z][A-Za-z0-9_.-]{0,127}$")
    var first = True
    for ch in result.codepoint_slices():
        if first:
            if ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz":
                _error("manifest.value", path, label + " must match ^[A-Za-z][A-Za-z0-9_.-]{0,127}$")
            first = False
        elif ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-":
            _error("manifest.value", path, label + " must match ^[A-Za-z][A-Za-z0-9_.-]{0,127}$")
    return result


def _required_nonnull(ref root: Value, key: String, path: String) raises -> Value:
    var value = _required(root, key, path)
    if value.is_null():
        _error("manifest.type", path + "/" + key, "explicit null is not allowed")
    return value^


def _optional_array(ref root: Value, key: String, path: String) raises -> Value:
    var value = _optional(root, key)
    if not value.is_null():
        if not value.is_array():
            _error("manifest.type", path + "/" + key, "expected array")
    elif root.is_object() and key in root.object():
        _error("manifest.type", path + "/" + key, "explicit null is not allowed")
    return value^


def _validate_ontology_fields(ref item: Value, path: String) raises:
    _reject_null_fields(item, ["title", "description", "tags", "media_types", "source_impulse_types", "target_impulse_types", "accepts_impulse_types", "accepts_reaction_kinds", "emits_impulse_types", "emits_reaction_kinds", "emits_association_kinds", "value_schema", "metadata_schema", "config_schema", "output_schema"], path)
    for key in ["title", "description"]:
        var text = _optional(item, key)
        if not text.is_null(): _ = _string(text^, path + "/" + key)
    for key in ["tags", "media_types", "source_impulse_types", "target_impulse_types", "accepts_impulse_types", "accepts_reaction_kinds", "emits_impulse_types", "emits_reaction_kinds", "emits_association_kinds"]:
        var values = _optional(item, key)
        if not values.is_null(): _ = _strings(values^, path + "/" + key)
    for key in ["value_schema", "metadata_schema", "config_schema", "output_schema"]:
        var schema = _optional(item, key)
        if not schema.is_null(): _json_schema(schema^, path + "/" + key)


def _ontology_list(value: Value, path: String, fields: List[String]) raises -> List[String]:
    var ids = List[String]()
    if value.is_null():
        return ids^
    if not value.is_array():
        _error("manifest.type", path, "expected array")
    var i = 0
    for item in value.array():
        var item_path = path + "/" + String(i)
        if not item.is_object():
            _error("manifest.type", item_path, "expected object")
        _known(item, fields, item_path)
        var id = _runtime_id(_required_nonnull(item, "id", item_path), item_path + "/id")
        _validate_ontology_fields(item, item_path)
        for prior in ids:
            if prior == id:
                _error("manifest.duplicate", item_path + "/id", "duplicate identifier")
        ids.append(id^)
        i += 1
    return ids^
def _validate_refs(ref item: Value, key: String, path: String, known: List[String], label: String) raises:
    var refs = _optional(item, key)
    if refs.is_null(): return
    var values = _strings(refs^, path + "/" + key)
    var i = 0
    for value in values:
        if not _contains(known, value):
            _error("manifest.dangling_reference", path + "/" + key + "/" + String(i), "unknown " + label + " '" + value + "'")
        i += 1


def _validate_ontology_refs(value: Value, path: String, key: String, known: List[String], label: String) raises:
    if value.is_null(): return
    if not value.is_array(): return
    var i = 0
    for item in value.array():
        if item.is_object(): _validate_refs(item, key, path + "/" + String(i), known, label)
        i += 1


def _validate_runtime(value: Value, path: String) raises:
    if value.is_null():
        return
    if not value.is_object():
        _error("manifest.type", path, "expected runtime object")
    _known(value, ["backend", "reaction_store"], path)
    var backend = _required_nonnull(value, "backend", path)
    if not backend.is_object():
        _error("manifest.type", path + "/backend", "expected object")
    _known(backend, ["kind", "path"], path + "/backend")
    var backend_kind = _nonempty(_required_nonnull(backend, "kind", path + "/backend"), path + "/backend/kind")
    if backend_kind != "sqlite":
        _error("manifest.unsupported", path + "/backend/kind", "unsupported runtime backend kind")
    _ = _nonempty(_required_nonnull(backend, "path", path + "/backend"), path + "/backend/path")
    var store = _required_nonnull(value, "reaction_store", path)
    if not store.is_object():
        _error("manifest.type", path + "/reaction_store", "expected object")
    _known(store, ["kind", "root"], path + "/reaction_store")
    var store_kind = _nonempty(_required_nonnull(store, "kind", path + "/reaction_store"), path + "/reaction_store/kind")
    if store_kind != "filesystem":
        _error("manifest.unsupported", path + "/reaction_store/kind", "unsupported reaction store kind")
    _ = _nonempty(_required_nonnull(store, "root", path + "/reaction_store"), path + "/reaction_store/root")


def _adapter(value: Value, path: String, manifest_parent: String) raises -> _AdapterData:
    if not value.is_object():
        _error("manifest.type", path, "expected adapter object")
    _known(value, ["kind", "ref", "command", "cwd", "env", "inherit_env", "timeout_seconds"], path)
    var kind = _nonempty(_required(value, "kind", path), path + "/kind")
    if kind == "fala_runtime":
        _error("manifest.unsupported", path + "/kind", "fala_runtime is not part of Fala; use subprocess with a separate journal")
    if kind != "subprocess" and kind != "native_function" and kind != "manual_homeostat":
        _error("manifest.unsupported", path + "/kind", "unsupported adapter kind")
    var reference = String("")
    var command = List[String]()
    var cwd = String("")
    var env = Dict[String, String]()
    var inherit_env = List[String]()
    var timeout = 0.0
    var timeout_present = False
    var item = _optional(value, "ref")
    if not item.is_null(): reference = _nonempty(item^, path + "/ref")
    item = _optional(value, "command")
    if not item.is_null(): command = _strings(item^, path + "/command")
    item = _optional(value, "cwd")
    if not item.is_null(): cwd = _normalize_cwd(_nonempty(item^, path + "/cwd"), manifest_parent, path + "/cwd")
    item = _optional(value, "inherit_env")
    if not item.is_null():
        inherit_env = _strings(item^, path + "/inherit_env")
        var inherit_index = 0
        for name in inherit_env:
            var name_path = path + "/inherit_env/" + String(inherit_index)
            _ = _env_name(name, name_path)
            var prior_index = 0
            for prior in inherit_env:
                if prior_index >= inherit_index: break
                if prior == name: _error("manifest.duplicate", name_path, "duplicate environment key")
                prior_index += 1
    item = _optional(value, "timeout_seconds")
    if not item.is_null():
        timeout_present = True
        timeout = _number(item^, path + "/timeout_seconds")
    item = _optional(value, "env")
    if not item.is_null():
        if not item.is_object(): _error("manifest.type", path + "/env", "expected object")
        for pair in item.object().items():
            var env_path = path + "/env/" + _pointer_token(pair.key)
            _ = _env_name(pair.key, env_path)
            var env_value = _string(pair.value.copy(), env_path)
            _validate_env_interpolation(env_value, env_path, inherit_env)
            env[pair.key] = env_value^
    if timeout_present and timeout == 0.0: _error("manifest.value", path + "/timeout_seconds", "must be greater than 0 when provided")
    if timeout < 0.0: _error("manifest.value", path + "/timeout_seconds", "must not be negative")
    if kind == "subprocess" and len(command) == 0: _error("manifest.value", path + "/command", "subprocess requires command")
    if kind == "subprocess" and reference != "": _error("manifest.boundary", path, "subprocess adapter cannot define ref")
    if kind == "native_function" and reference == "": _error("manifest.missing", path + "/ref", "native_function requires ref")
    if kind == "native_function" and (len(command) != 0 or cwd != "" or len(env) != 0 or len(inherit_env) != 0): _error("manifest.boundary", path, "native_function adapter has invalid boundary fields")
    if kind == "manual_homeostat" and (reference != "" or len(command) != 0 or cwd != "" or len(env) != 0 or len(inherit_env) != 0 or timeout != 0.0): _error("manifest.boundary", path, "manual_homeostat adapter has invalid boundary fields")
    if kind != "subprocess" and len(inherit_env) != 0: _error("manifest.boundary", path + "/inherit_env", "only subprocess adapters may inherit environment")
    return _AdapterData(kind=kind, reference=reference, command=command^, cwd=cwd, env=env^, inherit_env=inherit_env^, timeout_seconds=timeout)


def _effector(value: Value, path: String, manifest_parent: String, capabilities: List[String] = List[String]()) raises -> PackageEffector:
    if not value.is_object(): _error("manifest.type", path, "expected effector object")
    _known(value, ["id", "title", "description", "tags", "capability", "adapter", "conduction", "timeout_seconds", "retry_policy", "config"], path)
    var id = _runtime_id(_required_nonnull(value, "id", path), path + "/id", "effector id")
    var conduction = List[String]()
    var item = _optional(value, "conduction")
    if not item.is_null(): conduction = _strings(item^, path + "/conduction")
    for dependency in conduction:
        if dependency == id: _error("manifest.value", path + "/conduction", "effector cannot depend on itself")
    var capability = String("")
    item = _optional(value, "capability")
    if item.is_null():
        if len(capabilities) != 0: _error("manifest.missing", path + "/capability", "required field is missing")
    else:
        capability = _runtime_id(item^, path + "/capability", "capability id")
        if len(capabilities) != 0 and not _contains(capabilities, capability): _error("manifest.dangling_reference", path + "/capability", "unknown capability '" + capability + "'")
    var title = String(""); var description = String(""); var tags = List[String]()
    item = _optional(value, "title")
    if not item.is_null(): title = _string(item^, path + "/title")
    item = _optional(value, "description")
    if not item.is_null(): description = _string(item^, path + "/description")
    item = _optional(value, "tags")
    if not item.is_null(): tags = _strings(item^, path + "/tags")
    item = _optional(value, "adapter")
    if item.is_null(): _error("manifest.missing", path + "/adapter", "required field is missing")
    var adapter = _adapter(item^, path + "/adapter", manifest_parent)
    var timeout = adapter.timeout_seconds
    item = _optional(value, "timeout_seconds")
    if not item.is_null():
        timeout = _number(item^, path + "/timeout_seconds")
        if timeout < 0.0: _error("manifest.value", path + "/timeout_seconds", "must not be negative")
        if timeout == 0.0: _error("manifest.value", path + "/timeout_seconds", "must be greater than 0 when provided")
    var retry_policy = String("automatic")
    item = _optional(value, "retry_policy")
    if not item.is_null():
        retry_policy = _string(item^, path + "/retry_policy")
        if retry_policy != "automatic" and retry_policy != "none": _error("manifest.value", path + "/retry_policy", "expected automatic or none")
    var config_json = String("{}")
    item = _optional(value, "config")
    if not item.is_null():
        if not item.is_object(): _error("manifest.type", path + "/config", "expected object")
        config_json = canonical_json_text(to_string(item^))
    return PackageEffector(id=id, conduction=conduction, capability=capability, adapter_kind=adapter.kind, adapter_ref=adapter.reference, adapter_command=adapter.command.copy(), adapter_cwd=adapter.cwd, adapter_env=adapter.env.copy(), adapter_inherit_env=adapter.inherit_env.copy(), timeout_seconds=timeout, config_json=config_json, title=title, description=description, tags=tags, retry_policy=retry_policy)
def _path(value: Value, path: String, manifest_parent: String, capabilities: List[String] = List[String]()) raises -> PackageCorrelationPath:
    if not value.is_object(): _error("manifest.type", path, "expected correlation path object")
    _known(value, ["id", "title", "description", "tags", "effectors", "accumulate_upstream_reactions"], path)
    var id = _runtime_id(_required_nonnull(value, "id", path), path + "/id", "correlation path id")
    var effects_value = _required_nonnull(value, "effectors", path)
    if not effects_value.is_array() or len(effects_value.array()) == 0: _error("manifest.value", path + "/effectors", "must be nonempty array")
    var effectors = List[PackageEffector](); var i = 0
    for item in effects_value.array():
        var effector = _effector(item.copy(), path + "/effectors/" + String(i), manifest_parent, capabilities)
        for prior in effectors:
            if prior.id == effector.id: _error("manifest.duplicate", path + "/effectors/" + String(i) + "/id", "duplicate effector id")
        effectors.append(effector.copy()); i += 1
    for effector in effectors:
        for reference in effector.conduction:
            var found = False
            for candidate in effectors:
                if candidate.id == reference: found = True
            if not found: _error("manifest.dangling_reference", path + "/effectors/" + _pointer_token(effector.id) + "/conduction", "unknown effector '" + reference + "'")
    var title = String(""); var description = String(""); var tags = List[String](); var reactions = False
    var item = _optional(value, "title")
    if not item.is_null(): title = _string(item^, path + "/title")
    item = _optional(value, "description")
    if not item.is_null(): description = _string(item^, path + "/description")
    item = _optional(value, "tags")
    if not item.is_null(): tags = _strings(item^, path + "/tags")
    item = _optional(value, "accumulate_upstream_reactions")
    if not item.is_null():
        if not item.is_bool(): _error("manifest.type", path + "/accumulate_upstream_reactions", "expected boolean")
        reactions = item.bool()
    return PackageCorrelationPath(id=id, effectors=effectors, title=title, description=description, tags=tags, accumulate_upstream_reactions=reactions)


def _json_value(text: String) raises -> Value:
    return Value(parse_string=text)


def _effector_json(effector: PackageEffector) raises -> Value:
    var result = Object(capacity=14)
    result["id"] = Value(effector.id)
    if effector.title != "": result["title"] = Value(effector.title)
    if effector.description != "": result["description"] = Value(effector.description)
    if len(effector.tags) != 0:
        var tags_json = String("[")
        var i = 0
        for tag in effector.tags:
            if i != 0: tags_json += ","
            tags_json += to_string(Value(tag))
            i += 1
        tags_json += "]"
        result["tags"] = _json_value(tags_json^)
    if effector.capability != "": result["capability"] = Value(effector.capability)
    var adapter = Object(capacity=8)
    adapter["kind"] = Value(effector.adapter_kind)
    if effector.adapter_kind == "subprocess":
        var command_json = String("[")
        var command_index = 0
        for command in effector.adapter_command:
            if command_index != 0: command_json += ","
            command_json += to_string(Value(command))
            command_index += 1
        command_json += "]"
        adapter["command"] = _json_value(command_json^)
        if effector.adapter_cwd != "": adapter["cwd"] = Value(effector.adapter_cwd)
        if len(effector.adapter_inherit_env) != 0:
            var inherit_json = String("[")
            var inherit_index = 0
            for name in effector.adapter_inherit_env:
                if inherit_index != 0: inherit_json += ","
                inherit_json += to_string(Value(name))
                inherit_index += 1
            inherit_json += "]"
            adapter["inherit_env"] = _json_value(inherit_json^)
        if len(effector.adapter_env) != 0:
            var env = Object(capacity=len(effector.adapter_env))
            for pair in effector.adapter_env.items(): env[pair.key] = Value(pair.value)
            adapter["env"] = Value(env^)
    elif effector.adapter_kind == "native_function":
        adapter["ref"] = Value(effector.adapter_ref)
    result["adapter"] = Value(adapter^)
    if len(effector.conduction) != 0:
        var conduction_json = String("[")
        var conduction_index = 0
        for dependency in effector.conduction:
            if conduction_index != 0: conduction_json += ","
            conduction_json += to_string(Value(dependency))
            conduction_index += 1
        conduction_json += "]"
        result["conduction"] = _json_value(conduction_json^)
    if effector.timeout_seconds > 0.0: result["timeout_seconds"] = Value(effector.timeout_seconds)
    if effector.retry_policy != "automatic": result["retry_policy"] = Value(effector.retry_policy)
    result["config"] = _json_value(effector.config_json)
    return Value(result^)


def _path_json(path: PackageCorrelationPath) raises -> Value:
    var result = Object(capacity=8)
    result["id"] = Value(path.id)
    if path.title != "": result["title"] = Value(path.title)
    if path.description != "": result["description"] = Value(path.description)
    if len(path.tags) != 0:
        var tags_json = String("[")
        var i = 0
        for tag in path.tags:
            if i != 0: tags_json += ","
            tags_json += to_string(Value(tag))
            i += 1
        tags_json += "]"
        result["tags"] = _json_value(tags_json^)
    var effectors_json = String("[")
    var effector_index = 0
    for effector in path.effectors:
        if effector_index != 0: effectors_json += ","
        effectors_json += to_string(_effector_json(effector))
        effector_index += 1
    effectors_json += "]"
    result["effectors"] = _json_value(effectors_json^)
    if path.accumulate_upstream_reactions: result["accumulate_upstream_reactions"] = Value(True)
    return Value(result^)

def serialize_correlation_path_json(path: PackageCorrelationPath) raises -> String:
    """Canonical JSON projection for one selected correlation path."""
    return canonical_json_text(to_string(_path_json(path)))


def serialize_package_json(manifest: PackageManifest) raises -> String:
    """Serialize a validated manifest with deterministic EmberJson ordering."""
    var root = Object(capacity=13)
    root["id"] = Value(manifest.id)
    root["version"] = Value(manifest.version)
    if manifest.title != "": root["title"] = Value(manifest.title)
    if manifest.description != "": root["description"] = Value(manifest.description)
    if len(manifest.tags) != 0:
        var tags_json = String("[")
        var i = 0
        for tag in manifest.tags:
            if i != 0: tags_json += ","
            tags_json += to_string(Value(tag))
            i += 1
        tags_json += "]"
        root["tags"] = _json_value(tags_json^)
    root["impulse_types"] = _json_value(manifest.impulse_types_json)
    root["impulse_relations"] = _json_value(manifest.impulse_relations_json)
    root["association_kinds"] = _json_value(manifest.association_kinds_json)
    root["reaction_kinds"] = _json_value(manifest.reaction_kinds_json)
    root["capabilities"] = _json_value(manifest.capabilities_json)
    root["runtime"] = _json_value(manifest.runtime_json)
    var paths_json = String("[")
    var path_index = 0
    for path in manifest.correlation_paths:
        if path_index != 0: paths_json += ","
        paths_json += to_string(_path_json(path))
        path_index += 1
    paths_json += "]"
    root["correlation_paths"] = _json_value(paths_json^)
    return canonical_json_text(to_string(Value(root^)))



def validate_package_json_text(text: String, path: String = "<memory>") raises -> PackageManifest:
    """Parse and strictly validate manifest JSON without writing any file."""
    var root = Value()
    try:
        root = Value(parse_string=text)
    except err:
        _error("manifest.invalid", path, "invalid JSON manifest; YAML is unsupported")
    return _load_package_value(root^, path)


def load_package_json(path: String) raises -> PackageManifest:
    """Read and strictly validate one native JSON manifest."""
    var text = String("")
    try:
        text = Path(path).read_text()
    except err:
        _error("manifest.read", path, "unable to read manifest")
    return validate_package_json_text(text, path)




def _load_package_value(root: Value, path: String) raises -> PackageManifest:
    """Internal manifest schema validation implementation."""
    if not root.is_object(): _error("manifest.type", "/", "manifest must be a JSON object")
    _known(root, ["id", "version", "title", "description", "tags", "impulse_types", "impulse_relations", "association_kinds", "reaction_kinds", "capabilities", "runtime", "correlation_paths"], "")
    var id = _runtime_id(_required_nonnull(root, "id", ""), "/id", "package id")
    var version = _package_version(root)
    var manifest_parent = _manifest_parent(path)
    var capabilities_value = _optional(root, "capabilities")
    var capability_ids = _ontology_list(capabilities_value.copy(), "/capabilities", ["id", "title", "description", "tags", "accepts_impulse_types", "accepts_reaction_kinds", "emits_impulse_types", "emits_reaction_kinds", "emits_association_kinds", "config_schema", "output_schema"])
    var paths_value = _required_nonnull(root, "correlation_paths", "")
    if not paths_value.is_array(): _error("manifest.type", "/correlation_paths", "expected array")
    if len(paths_value.array()) == 0: _error("manifest.value", "/correlation_paths", "must be nonempty array")
    var paths = List[PackageCorrelationPath](); var i = 0
    for item in paths_value.array():
        var path_item = _path(item.copy(), "/correlation_paths/" + String(i), manifest_parent, capability_ids)
        for prior in paths:
            if prior.id == path_item.id: _error("manifest.duplicate", "/correlation_paths/" + String(i) + "/id", "duplicate correlation path id")
        paths.append(path_item.copy()); i += 1
    _validate_runtime(_optional(root, "runtime"), "/runtime")
    var impulse_ids = _ontology_list(_optional(root, "impulse_types"), "/impulse_types", ["id", "title", "description", "tags", "media_types", "value_schema", "metadata_schema"])
    var relation_ids = _ontology_list(_optional(root, "impulse_relations"), "/impulse_relations", ["id", "title", "description", "tags", "source_impulse_types", "target_impulse_types"])
    var association_ids = _ontology_list(_optional(root, "association_kinds"), "/association_kinds", ["id", "title", "description", "tags", "value_schema", "metadata_schema"])
    var reaction_ids = _ontology_list(_optional(root, "reaction_kinds"), "/reaction_kinds", ["id", "title", "description", "tags", "media_types", "value_schema", "metadata_schema"])
    _ = relation_ids
    var ontology = _optional(root, "impulse_relations")
    _validate_ontology_refs(ontology^, "/impulse_relations", "source_impulse_types", impulse_ids, "impulse type")
    _validate_ontology_refs(ontology^, "/impulse_relations", "target_impulse_types", impulse_ids, "impulse type")
    ontology = _optional(root, "capabilities")
    for key in ["accepts_impulse_types", "emits_impulse_types"]:
        _validate_ontology_refs(ontology^, "/capabilities", key, impulse_ids, "impulse type")
    for key in ["accepts_reaction_kinds", "emits_reaction_kinds"]:
        _validate_ontology_refs(ontology^, "/capabilities", key, reaction_ids, "reaction kind")
    _validate_ontology_refs(ontology^, "/capabilities", "emits_association_kinds", association_ids, "association kind")
    var title = String(""); var description = String(""); var tags = List[String]()
    var item = _optional(root, "title")
    if not item.is_null(): title = _string(item^, "/title")
    item = _optional(root, "description")
    if not item.is_null(): description = _string(item^, "/description")
    item = _optional(root, "tags")
    if not item.is_null(): tags = _strings(item^, "/tags")
    var impulse_types = _optional(root, "impulse_types").copy(); var impulse_relations = _optional(root, "impulse_relations").copy(); var association_kinds = _optional(root, "association_kinds").copy(); var reaction_kinds = _optional(root, "reaction_kinds").copy(); var capabilities = capabilities_value.copy(); var runtime = _optional(root, "runtime").copy()
    var impulse_types_json = String("[]"); var impulse_relations_json = String("[]"); var association_kinds_json = String("[]"); var reaction_kinds_json = String("[]"); var capabilities_json = String("[]"); var runtime_json = String("null")
    if not impulse_types.is_null(): impulse_types_json = canonical_json_text(to_string(impulse_types^))
    if not impulse_relations.is_null(): impulse_relations_json = canonical_json_text(to_string(impulse_relations^))
    if not association_kinds.is_null(): association_kinds_json = canonical_json_text(to_string(association_kinds^))
    if not reaction_kinds.is_null(): reaction_kinds_json = canonical_json_text(to_string(reaction_kinds^))
    if not capabilities.is_null(): capabilities_json = canonical_json_text(to_string(capabilities^))
    if not runtime.is_null(): runtime_json = canonical_json_text(to_string(runtime^))
    return PackageManifest(id=id, version=version, correlation_paths=paths, title=title, description=description, tags=tags, impulse_types_json=impulse_types_json, impulse_relations_json=impulse_relations_json, association_kinds_json=association_kinds_json, reaction_kinds_json=reaction_kinds_json, capabilities_json=capabilities_json, runtime_json=runtime_json)


def main():
    pass
