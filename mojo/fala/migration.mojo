"""Native package migration and preflight boundaries.

Legacy package JSON is converted in-memory by renaming the v1 vocabulary to
the current vocabulary, then canonicalized and optionally written as a new
manifest. Authored package manifests may be read from TOML; migration always
emits canonical JSON. The source is never modified.
"""
from emberjson import Array, Object, Value, to_string
from std.ffi import CStringSlice, c_int, external_call
from std.os import remove
from std.pathlib import Path
from fala.json import canonical_json_text
from fala.native_package import load_package_json, validate_package_json_text
from fala.reactions import sha256_bytes
from fala.toml import parse_toml_value


comptime _TOML_GUIDANCE = "native migration accepts strict JSON or TOML; authored manifests should use TOML"


def _atomic_rename(source: Path, target: Path) raises:
    """Commit a validated manifest with POSIX rename semantics."""
    var source_text = source.__fspath__() + "\0"
    var target_text = target.__fspath__() + "\0"
    var source_c = CStringSlice(source_text)
    var target_c = CStringSlice(target_text)
    var result = external_call["rename", c_int](source_c, target_c)
    if result != 0:
        raise Error("migration.atomic at " + target.__fspath__() + ": unable to commit destination")


def _cleanup(path: Path):
    try:
        remove(path)
    except:
        pass




struct MigrationReport(Copyable, Movable):
    var source_digest: String
    var migrated_digest: String
    var output_path: String
    var source_version: String
    var migrated: Bool
    var destination_version: String

    def __init__(out self, source_digest: String, migrated_digest: String, output_path: String, source_version: String, migrated: Bool, destination_version: String = ""):
        self.source_digest = source_digest
        self.migrated_digest = migrated_digest
        self.output_path = output_path
        self.source_version = source_version
        self.migrated = migrated
        self.destination_version = destination_version

def _legacy_key(key: String) -> String:
    if key == "carrier_types": return "impulse_types"
    if key == "carrier_relations": return "impulse_relations"
    if key == "observation_kinds": return "association_kinds"
    if key == "artifact_kinds": return "reaction_kinds"
    if key == "flows": return "correlation_paths"
    if key == "steps": return "effectors"
    if key == "needs": return "conduction"
    if key == "accepts_carrier_types": return "accepts_impulse_types"
    if key == "emits_carrier_types": return "emits_impulse_types"
    if key == "accepts_artifact_kinds": return "accepts_reaction_kinds"
    if key == "emits_artifact_kinds": return "emits_reaction_kinds"
    if key == "emits_observation_kinds": return "emits_association_kinds"
    if key == "artifact_store": return "reaction_store"
    return key


def _legacy_string(value: String) -> String:
    # URI migration is safe in preserved metadata as well as typed refs.
    if value.startswith("fala-artifact://"):
        var result = "fala-reaction://"
        var index = 16
        while index < value.byte_length():
            result += String(value[byte=index])
            index += 1
        return result
    return value


def _legacy_adapter_kind(value: String) raises -> String:
    var kind = value
    if kind == "manual_gate": kind = "manual_homeostat"
    if kind != "subprocess" and kind != "native_function" and kind != "python_function" and kind != "manual_homeostat" and kind != "fala_runtime":
        raise Error("migration.unsupported at /adapter/kind: unknown legacy adapter kind '" + value + "'")
    return kind


def _convert(var value: Value, adapter_context: Bool = False) raises -> Value:
    if value.is_array():
        var result = Array(capacity=len(value.array()))
        for item in value.array():
            var child = item.copy()
            var converted = _convert(child^, adapter_context)
            result.append(converted^)
        return Value(result^)
    if value.is_object():
        var result = Object(capacity=len(value.object()))
        for pair in value.object().items():
            var child = pair.value.copy()
            var child_context = pair.key == "adapter"
            var converted = _convert(child^, child_context)
            var key = _legacy_key(pair.key)
            if adapter_context and pair.key == "kind":
                if not converted.is_string():
                    raise Error("migration.type at /adapter/kind: legacy adapter kind must be a string")
                converted = Value(_legacy_adapter_kind(converted.string()))
            elif converted.is_string():
                converted = Value(_legacy_string(converted.string()))
            if key in result:
                raise Error("migration.collision at /" + key + ": legacy and native keys both define this field")
            result[key] = converted^
        return Value(result^)
    if value.is_string():
        return Value(_legacy_string(value.string()))
    return value^


def _source_version(ref source: Value, path: String) raises -> String:
    if not source.is_object():
        raise Error("migration.invalid at " + path + ": expected a JSON object")
    if "version" not in source.object():
        # Native package loading defaults an omitted version to current (2).
        return "2"
    var value = source.object()["version"].copy()
    if value.is_null():
        raise Error("migration.version at /version: null source version is not supported")
    var version = String("")
    if value.is_string(): version = value.string()
    elif value.is_int(): version = String(value.int())
    elif value.is_uint(): version = String(value.uint())
    else:
        raise Error("migration.version at /version: source version must be 1 or 2")
    if version != "1" and version != "2":
        raise Error("migration.version at /version: unsupported source version '" + version + "'")
    return version


def _destination_value(var converted: Value, source_version: String) raises -> Value:
    if source_version != "1": return converted^
    if not converted.is_object():
        raise Error("migration.invalid at /: converted package must be an object")
    var result = converted.object().copy()
    # Package migration is a one-way cutover: v1 input always emits v2.
    result["version"] = Value("2")
    return Value(result^)


def _read_json(path: String) raises -> Value:
    """Read authored TOML or strict JSON into the shared value model."""
    var text = Path(path).read_text()
    if path.endswith(".toml"):
        try:
            var parsed_toml = parse_toml_value(text, path)
            if not parsed_toml.is_object():
                raise Error("migration.invalid at " + path + ": expected a TOML object")
            return parsed_toml^
        except err:
            var message = String(err)
            if message.find("migration.") >= 0:
                raise err^
            raise Error("migration.invalid at " + path + ": invalid TOML manifest; " + message)
    var index = 0
    while index < text.byte_length():
        var c = String(text[byte=index])
        if c != " " and c != "\t" and c != "\n" and c != "\r":
            if c != "{":
                raise Error("migration.toml_required at " + path + ": " + _TOML_GUIDANCE)
            break
        index += 1
    if index == text.byte_length():
        raise Error("migration.invalid at " + path + ": empty input is not a JSON object")
    try:
        var parsed = Value(parse_string=text)
        if not parsed.is_object():
            raise Error("migration.invalid at " + path + ": expected a JSON object")
        return parsed^
    except err:
        var message = String(err)
        if message.find("migration.") >= 0:
            raise err^
        raise Error("migration.invalid at " + path + ": invalid JSON; " + _TOML_GUIDANCE)

def _read_source(path: String) raises -> Value:
    if path.endswith(".toml"):
        var text = Path(path).read_text()
        var parsed = parse_toml_value(text, path)
        if not parsed.is_object():
            raise Error("migration.invalid at " + path + ": expected a TOML object")
        return parsed^
    return _read_json(path)


struct _PreparedMigration(Copyable, Movable):
    var text: String
    var source_version: String

    def __init__(out self, text: String, source_version: String):
        self.text = text
        self.source_version = source_version
def _prepared_native(source: Value, path: String) raises -> _PreparedMigration:
    var source_version = _source_version(source, path)
    var converted = _convert(source.copy())
    var destination = _destination_value(converted^, source_version)
    var result = canonical_json_text(to_string(destination^))
    _ = validate_package_json_text(result.copy(), path)
    return _PreparedMigration(text=result, source_version=source_version)





def legacy_to_native_json(path: String) raises -> String:
    """Preflight one package and return canonical v2 JSON without writing."""
    var source = _read_source(path)
    var prepared = _prepared_native(source^, path)
    return prepared.text


def migrate_package_json(source_path: String, output_path: String) raises -> MigrationReport:
    """Convert legacy JSON or authored TOML to a new native manifest.

    The source remains untouched. Conversion and validation happen before an
    atomic rename, and every preflight artifact is removed on success or error.
    """
    if source_path == output_path:
        raise Error("migration.boundary at " + output_path + ": source and destination must differ")
    var check_path = output_path + ".preflight"
    var source_raw = Path(source_path).read_text()
    var source = _read_source(source_path)
    var source_text = canonical_json_text(to_string(source.copy()))
    var prepared = _prepared_native(source^, source_path)
    var migrated_text = prepared.text
    var source_version = prepared.source_version
    try:
        # Validate the exact bytes that will be committed before exposing the file.
        Path(check_path).write_text(migrated_text)
        var exact = load_package_json(check_path)
        var destination_version = exact.version.copy()
        if Path(output_path).exists() and Path(output_path).is_file() and Path(output_path).read_text() == migrated_text:
            _cleanup(Path(check_path))
            return MigrationReport(source_digest=sha256_bytes(source_raw), migrated_digest=sha256_bytes(migrated_text), output_path=output_path, source_version=source_version, migrated=source_text != migrated_text, destination_version=destination_version)
        _atomic_rename(Path(check_path), Path(output_path))
        _cleanup(Path(check_path))
        return MigrationReport(source_digest=sha256_bytes(source_raw), migrated_digest=sha256_bytes(migrated_text), output_path=output_path, source_version=source_version, migrated=source_text != migrated_text, destination_version=destination_version)
    except err:
        _cleanup(Path(check_path))
        raise err^
 
 
