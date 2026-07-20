from std.pathlib import Path
from fala.package import load_package_json, load_package_toml
from fala.native_package import validate_package_json_text, serialize_package_json, PackageManifestError
from fala.toml import parse_toml_json
from fala.sdk import (
    SdkUnavailableError,
    SdkError,
    load_manifest,
    input_values,
    declared_inputs,
    conduction,
    upstream_reactions,
    find_reaction,
    output,
    output_reactions,
    find_output_reaction,
    output_metadata,
    run_manifest_effector,
)
from fala.adapters import AdapterError, AdapterKind, AdapterSpec
from fala.errors import ValidationError


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("package SDK contracts smoke: " + message)


def _expect_package_error(text: String, needle: String) raises:
    var matched = False
    try:
        _ = validate_package_json_text(text, "<package-smoke>")
    except err:
        matched = String(err).find(needle) >= 0
    _check(matched, "package diagnostic contains '" + needle + "'")


def _expect_toml_error(text: String, needle: String) raises:
    var matched = False
    try:
        _ = parse_toml_json(text, "<toml-smoke>")
    except err:
        matched = String(err).find(needle) >= 0
    _check(matched, "TOML diagnostic contains '" + needle + "'")


def _write(path: String, text: String) raises:
    Path(path).write_text(text)


def main() raises:
    # Strict JSON package loading preserves impulse ontology and runtime data.
    var package_json = "{\"runtime\":{\"reaction_store\":{\"root\":\"reactions\",\"kind\":\"filesystem\"},\"backend\":{\"path\":\"state.sqlite\",\"kind\":\"sqlite\"}},\"capabilities\":[{\"id\":\"normalize\",\"accepts_impulse_types\":[\"input_text\"],\"emits_impulse_types\":[\"normalized_text\"]}],\"impulse_types\":[{\"id\":\"input_text\",\"media_types\":[\"text/plain\"]},{\"id\":\"normalized_text\",\"media_types\":[\"text/plain\"]}],\"impulse_relations\":[{\"id\":\"normalized_from\",\"source_impulse_types\":[\"input_text\"],\"target_impulse_types\":[\"normalized_text\"]}],\"correlation_paths\":[{\"id\":\"basic\",\"effectors\":[{\"id\":\"normalize\",\"capability\":\"normalize\",\"adapter\":{\"kind\":\"manual_homeostat\"}}]}],\"id\":\"fala_package\",\"version\":2}"
    var manifest = validate_package_json_text(package_json, "<package-smoke>")
    var package_json_path = "/tmp/fala-package-sdk-contracts.json"
    _write(package_json_path, package_json)
    var loaded_json = load_package_json(package_json_path)
    _check(loaded_json.id == "fala_package" and loaded_json.version == "2", "strict JSON file loading")
    _check(manifest.id == "fala_package" and manifest.version == "2", "package identity and version")
    _check(len(manifest.correlation_paths) == 1 and manifest.correlation_paths[0].effectors[0].adapter_kind == "manual_homeostat", "package path and adapter")
    _check(manifest.impulse_types_json.find("input_text") >= 0 and manifest.runtime_json.find("state.sqlite") >= 0, "ontology and runtime retention")
    var canonical = serialize_package_json(manifest)
    _check(canonical == serialize_package_json(manifest), "deterministic package serialization")

    # Package boundaries are strict: malformed, non-object, unknown, dangling, and
    # legacy/Python forms are rejected instead of being interpreted compatibly.
    _expect_package_error("{bad", "manifest.invalid at <package-smoke>")
    _expect_package_error("[]", "manifest.type at /: manifest must be a JSON object")
    _expect_package_error("{\"id\":\"pkg\",\"extra\":true,\"correlation_paths\":[]}", "manifest.unknown at /extra")
    _expect_package_error("{\"id\":\"pkg\",\"impulse_types\":[{\"id\":\"input\"}],\"capabilities\":[{\"id\":\"cap\",\"accepts_impulse_types\":[\"missing\"]}],\"correlation_paths\":[{\"id\":\"p\",\"effectors\":[{\"id\":\"e\",\"capability\":\"cap\",\"adapter\":{\"kind\":\"manual_homeostat\"}}]}]}", "manifest.dangling_reference")
    # Retired CPython kind is an unknown/unsupported adapter, not a special transport.
    _expect_package_error("{\"id\":\"pkg\",\"correlation_paths\":[{\"id\":\"p\",\"effectors\":[{\"id\":\"e\",\"adapter\":{\"kind\":\"python_function\",\"ref\":\"py.fn\"}}]}]}", "unsupported adapter kind")
    _expect_package_error("{\"id\":\"pkg\",\"correlation_paths\":{}}", "manifest.type at /correlation_paths")
    _expect_package_error("{\"package\":\"pkg\",\"correlation_paths\":[]}", "manifest.unknown at /package")
    _expect_package_error("{\"id\":\"pkg\",\"correlation_paths\":[{\"pipeline\":\"path\",\"effectors\":[{\"id\":\"e\",\"adapter\":{\"kind\":\"manual_homeostat\"}}]}]}", "manifest.unknown at /correlation_paths/0/pipeline")
    _expect_package_error("{\"id\":\"pkg\",\"association_kinds\":[{\"id\":\"stats\",\"value_schema\":{\"type\":\"date\"}}],\"correlation_paths\":[{\"id\":\"p\",\"effectors\":[{\"id\":\"e\",\"adapter\":{\"kind\":\"manual_homeostat\"}}]}]}", "manifest.value")

    # Authored TOML uses the same strict package validator; unsupported TOML and
    # YAML-like text fail closed rather than entering a compatibility path.
    var toml_text = """
id = "toml_pkg"
version = 2
[[correlation_paths]]
id = "path"
[[correlation_paths.effectors]]
id = "eff"
adapter = { kind = "manual_homeostat" }
"""
    var toml_manifest_path = "/tmp/fala-package-sdk-contracts.toml"
    _write(toml_manifest_path, toml_text)
    var toml_manifest = load_package_toml(toml_manifest_path)
    _check(toml_manifest.id == "toml_pkg" and toml_manifest.version == "2" and len(toml_manifest.correlation_paths) == 1, "TOML package loading")
    _expect_toml_error("a = 1\na = 2\n", "duplicate key")
    _expect_toml_error("a = 2026-01-01\n", "date/time values are unsupported")
    _expect_package_error("id: pkg\nversion: '1'\n", "manifest.invalid")

    # SDK envelope helpers canonicalize JSON and strip only native injected keys.
    var sdk_manifest = "{\"input\":{\"source\":\"hello\",\"conduction\":{\"ingest\":{\"chars\":5}},\"upstream_reactions\":[{\"kind\":\"draft\",\"path\":\"a\"},{\"kind\":\"draft\",\"path\":\"b\"},{\"kind\":\"final\",\"path\":\"c\"}]}}"
    _check(input_values(sdk_manifest).find("source") >= 0, "SDK input values")
    _check(declared_inputs(sdk_manifest) == "{\"source\":\"hello\"}", "SDK declared inputs")
    _check(conduction(sdk_manifest) == "{\"ingest\":{\"chars\":5}}", "SDK conduction")
    _check(upstream_reactions(sdk_manifest).find("\"path\":\"b\"") >= 0, "SDK upstream reaction objects")
    _check(find_reaction(sdk_manifest, "draft") == "{\"kind\":\"draft\",\"path\":\"b\"}", "SDK latest reaction selection")
    var envelope = output("{\"ok\":true}", "[{\"kind\":\"report\"},3,{\"kind\":\"manifest\"}]", "[{\"kind\":\"draft\",\"v\":1},{\"kind\":\"draft\",\"v\":2}]", "{\"telemetry\":{\"ms\":12}}")
    _check(output_reactions(envelope).find("\"v\":2") >= 0 and find_output_reaction(envelope, "draft") == "{\"kind\":\"draft\",\"v\":2}", "SDK output reaction envelope")
    _check(output_metadata(envelope) == "{\"telemetry\":{\"ms\":12}}", "SDK output metadata")

    # SDK malformed/type diagnostics are stable and execution remains an explicit
    # native boundary (no subprocess, UUID, clock, or Python fallback).
    var sdk_invalid_json = False
    try:
        _ = load_manifest("{bad")
    except err:
        sdk_invalid_json = String(err).find("sdk.invalid_json at /manifest") >= 0
    _check(sdk_invalid_json, "SDK malformed JSON diagnostic")
    var sdk_invalid_type = False
    try:
        _ = input_values("{\"input\":[]}")
    except err:
        sdk_invalid_type = String(err).find("sdk.invalid_type at /manifest/input") >= 0
    _check(sdk_invalid_type, "SDK input type diagnostic")
    var unavailable = run_manifest_effector()
    _check(unavailable.is_unavailable() and unavailable.code == "sdk.execution_unavailable", "SDK unavailable execution code")

    # Native typed diagnostics expose stable code/path fields.
    var package_error = PackageManifestError("bad field", "manifest.type", "/id")
    _check(package_error.code == "manifest.type" and package_error.path == "/id" and package_error.__str__().find("manifest.type") >= 0, "typed package error")
    var sdk_error = SdkError("sdk.invalid_type", "/manifest", "expected JSON object")
    _check(sdk_error.code == "sdk.invalid_type" and sdk_error.path == "/manifest", "typed SDK error")
    var validation_error = ValidationError("validation.invalid", "/id", "invalid id")
    _check(validation_error.describe() == "validation.invalid at /id: invalid id", "typed validation error serialization")
    var unknown_adapter = AdapterSpec(AdapterKind("python_function")).validate()
    _check(unknown_adapter.code == "invalid_adapter" and not unknown_adapter.is_ok(), "retired adapter kind is invalid")

    print("package SDK contracts smoke ok")
