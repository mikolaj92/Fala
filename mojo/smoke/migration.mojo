from std.pathlib import Path
from fala.json import canonical_json_text
from fala.migration import legacy_to_native_json, migrate_package_json
from fala.native_package import load_package_json
from fala.package import load_package_toml
from fala.reactions import sha256_bytes


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("migration smoke: " + message)


def _expect_error(path: String, needle: String) raises:
    var matched = False
    try:
        _ = legacy_to_native_json(path)
    except err:
        matched = String(err).find(needle) >= 0
    _check(matched, "diagnostic contains '" + needle + "'")


def main() raises:
    var root = "/tmp/fala-native-migration-smoke"
    var legacy_path = root + "-legacy.json"
    var native_path = root + "-native.json"
    var corrupt_path = root + "-corrupt.json"
    var toml_path = root + "-package.toml"
    var toml_native_path = root + "-package.json"
    var rollback_path = root + "-rollback.json"
    var invalid_path = root + "-invalid.json"
    var future_path = root + "-future.json"
    var unknown_path = root + "-unknown.json"
    var collision_path = root + "-collision.json"
    var null_path = root + "-null.json"
    # Legacy v1 vocabulary converts nested keys, URI refs and adapter kinds.
    var legacy = "{\"id\":\"legacy\",\"version\":\"1\",\"carrier_types\":[{\"id\":\"input\"}],\"flows\":[{\"id\":\"flow\",\"steps\":[{\"id\":\"step\",\"capability\":\"cap\",\"needs\":[],\"adapter\":{\"kind\":\"manual_gate\"}}]}],\"capabilities\":[{\"id\":\"cap\",\"accepts_carrier_types\":[\"input\"]}],\"runtime\":{\"backend\":{\"kind\":\"sqlite\",\"path\":\":memory:\"},\"artifact_store\":{\"kind\":\"filesystem\",\"root\":\"fala-artifact://root\"}}}"
    Path(legacy_path).write_text(legacy)
    var converted = legacy_to_native_json(legacy_path)
    _check(converted.find("impulse_types") >= 0 and converted.find("correlation_paths") >= 0, "legacy keys converted")
    _check(converted.find("manual_homeostat") >= 0 and converted.find("fala-reaction://root") >= 0 and converted.find("accepts_impulse_types") >= 0, "nested legacy data converted")
    var source_before = Path(legacy_path).read_text()
    var report = migrate_package_json(legacy_path, native_path)
    _check(report.migrated and report.output_path == native_path, "legacy migration report")
    _check(report.source_version == "1" and report.destination_version == "2", "migration versions")
    _check(Path(legacy_path).read_text() == source_before, "source remains unchanged")
    _check(not Path(native_path + ".preflight").exists(), "preflight artifact cleaned after commit")
    var reopened = load_package_json(native_path)
    _check(reopened.id == "legacy" and reopened.version == report.destination_version and reopened.correlation_paths[0].effectors[0].adapter_kind == "manual_homeostat", "reopen migrated package")
    _check(report.migrated_digest == sha256_bytes(Path(native_path).read_text()), "migrated hash preserved")
    _check(report.source_digest == sha256_bytes(source_before), "source hash preserved")
    var repeat_report = migrate_package_json(legacy_path, native_path)
    _check(repeat_report.migrated and repeat_report.destination_version == "2", "repeat destination handling")
    _check(not Path(native_path + ".preflight").exists(), "preflight artifact cleaned after repeat")
    # Version gates and collisions fail closed before schema conversion.
    Path(future_path).write_text("{\"id\":\"future\",\"version\":\"3\",\"correlation_paths\":[]}")
    _expect_error(future_path, "migration.version")
    Path(unknown_path).write_text("{\"id\":\"unknown\",\"version\":\"x\",\"correlation_paths\":[]}")
    _expect_error(unknown_path, "migration.version")
    Path(null_path).write_text("{\"id\":\"null\",\"version\":null,\"correlation_paths\":[]}")
    _expect_error(null_path, "migration.version")
    Path(collision_path).write_text("{\"id\":\"collision\",\"version\":\"1\",\"carrier_types\":[],\"impulse_types\":[],\"correlation_paths\":[{\"id\":\"flow\",\"effectors\":[{\"id\":\"step\",\"adapter\":{\"kind\":\"manual_gate\"}}]}]}")
    _expect_error(collision_path, "migration.collision")
    # A current strict JSON package is a no-op conversion with stable bytes.
    var native = "{\"correlation_paths\":[{\"effectors\":[{\"adapter\":{\"kind\":\"manual_homeostat\"},\"id\":\"step\"}],\"id\":\"flow\"}],\"id\":\"native\"}"
    Path(native_path).write_text(native)
    var native_report = migrate_package_json(native_path, native_path + ".copy")
    _check(not native_report.migrated, "new package is not rewritten semantically")
    _check(Path(native_path + ".copy").read_text() == canonical_json_text(native), "canonical JSON output")
    # Corrupt JSON remains an explicit typed boundary; TOML is the authored package format.
    Path(corrupt_path).write_text("{not-json")
    _expect_error(corrupt_path, "migration.invalid")
    Path(toml_path).write_text("id = \"legacy\"\nversion = \"2\"\n[[correlation_paths]]\nid = \"path\"\n[[correlation_paths.effectors]]\nid = \"eff\"\nadapter = { kind = \"manual_homeostat\" }\n")
    var toml_converted = legacy_to_native_json(toml_path)
    _check(toml_converted.find("\"id\":\"legacy\"") >= 0 and toml_converted.find("eff") >= 0, "TOML migration preflight")
    var toml_report = migrate_package_json(toml_path, toml_native_path)
    _check(not toml_report.migrated and toml_report.destination_version == "2", "TOML migration report")
    var toml_manifest = load_package_toml(toml_path)
    _check(toml_manifest.id == "legacy" and toml_manifest.version == "2" and len(toml_manifest.correlation_paths) == 1, "TOML authored package accepted")

    # Validation failure is a rollback boundary: existing destination bytes survive.
    Path(rollback_path).write_text("sentinel")
    Path(invalid_path).write_text("{\"id\":\"bad\",\"correlation_paths\":[]}")
    var preserved = False
    try:
        _ = migrate_package_json(invalid_path, rollback_path)
    except err:
        preserved = Path(rollback_path).read_text() == "sentinel"
    _check(preserved, "invalid migration leaves destination unchanged")
    _check(not Path(rollback_path + ".preflight").exists(), "preflight artifact cleaned after rollback")

    print("migration smoke ok: json-preflight legacy corrupt toml rollback reopen hash-preservation")
