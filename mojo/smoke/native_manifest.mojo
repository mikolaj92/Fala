from std.pathlib import Path
from fala.native_package import load_package_json, serialize_package_json


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("native manifest smoke: " + message)


def _write(path: String, text: String) raises:
    Path(path).write_text(text)


def _expect_error(path: String, needle: String) raises:
    var matched = False
    try:
        _ = load_package_json(path)
    except err:
        var text = String(err)
        matched = text.find(needle) >= 0
    _check(matched, "diagnostic contains '" + needle + "'")


def _manifest(adapter: String) raises -> String:
    return "{\"id\":\"pkg\",\"version\":\"1\",\"correlation_paths\":[{\"id\":\"path\",\"effectors\":[{\"id\":\"eff\",\"adapter\":" + adapter + "}]}]}"


def main() raises:
    var valid = "/tmp/fala-native-manifest-smoke.json"

    # Supported adapter kinds are exercised; unknown kinds and fala_runtime are rejections.
    _write(valid, _manifest("{\"kind\":\"subprocess\",\"command\":[\"echo\",\"ok\"]}"))
    var subprocess_manifest = load_package_json(valid)
    _check(subprocess_manifest.correlation_paths[0].effectors[0].adapter_kind == "subprocess", "subprocess adapter")
    _check(serialize_package_json(subprocess_manifest).find("\"runtime\":null") >= 0, "absent runtime remains null")
    _write(valid, _manifest("{\"kind\":\"native_function\",\"ref\":\"native.fn\"}"))
    var native_manifest = load_package_json(valid)
    _check(native_manifest.correlation_paths[0].effectors[0].adapter_kind == "native_function", "native_function adapter")
    _write(valid, _manifest("{\"kind\":\"manual_homeostat\"}"))
    var manual_manifest = load_package_json(valid)
    _check(manual_manifest.correlation_paths[0].effectors[0].adapter_kind == "manual_homeostat", "manual_homeostat adapter")
    _write(valid, _manifest("{\"kind\":\"fala_runtime\",\"runtime_ref\":\"runtime.main\"}"))
    _expect_error(valid, "manifest.unsupported")
    _expect_error(valid, "fala_runtime is not part of Fala")
    _write(valid, _manifest("{\"kind\":\"python_function\",\"ref\":\"py.fn\"}"))
    _expect_error(valid, "manifest.unsupported")
    _expect_error(valid, "unsupported adapter kind")

    # Config is required to be an object and is retained as JSON text.
    _write(valid, "{\"id\":\"pkg\",\"version\":\"1\",\"correlation_paths\":[{\"id\":\"path\",\"effectors\":[{\"id\":\"eff\",\"config\":{\"limit\":2},\"adapter\":{\"kind\":\"manual_homeostat\"}}]}]}")
    var configured = load_package_json(valid)
    _check(configured.correlation_paths[0].effectors[0].config_json == "{\"limit\":2}", "config object retention")
    _write(valid, "{\"id\":\"pkg\",\"version\":\"1\",\"correlation_paths\":[{\"id\":\"path\",\"effectors\":[{\"id\":\"eff\",\"config\":[],\"adapter\":{\"kind\":\"manual_homeostat\"}}]}]}")
    _expect_error(valid, "manifest.type at /correlation_paths/0/effectors/0/config: expected object")
    _write(valid, "{\"id\":\"pkg\",\"version\":\"1\",\"correlation_paths\":[{\"id\":\"path\",\"effectors\":[{\"id\":\"eff\",\"retry_policy\":\"none\",\"adapter\":{\"kind\":\"manual_homeostat\"}}]}]}")
    var no_retry = load_package_json(valid)
    _check(no_retry.correlation_paths[0].effectors[0].retry_policy == "none" and serialize_package_json(no_retry).find("\"retry_policy\":\"none\"") >= 0, "retry policy round trip")
    _write(valid, "{\"id\":\"pkg\",\"version\":\"1\",\"correlation_paths\":[{\"id\":\"path\",\"effectors\":[{\"id\":\"eff\",\"retry_policy\":\"sometimes\",\"adapter\":{\"kind\":\"manual_homeostat\"}}]}]}")
    _expect_error(valid, "expected automatic or none")

    # Root, path, effector, adapter, and array fields are strict.
    _write(valid, "[]")
    _expect_error(valid, "manifest.type at /: manifest must be a JSON object")
    _write(valid, "{\"id\":\"pkg\",\"version\":\"1\",\"correlation_paths\":{}}")
    _expect_error(valid, "manifest.type at /correlation_paths: expected array")
    _write(valid, "{\"id\":\"pkg\",\"version\":\"1\",\"correlation_paths\":[[]]}")
    _expect_error(valid, "manifest.type at /correlation_paths/0: expected correlation path object")
    _write(valid, "{\"id\":\"pkg\",\"version\":\"1\",\"correlation_paths\":[{\"id\":\"path\",\"effectors\":{}}]}")
    _expect_error(valid, "manifest.value at /correlation_paths/0/effectors: must be nonempty array")
    _write(valid, "{\"id\":\"pkg\",\"version\":\"1\",\"correlation_paths\":[{\"id\":\"path\",\"effectors\":[{\"id\":\"eff\",\"adapter\":[]}]}]}")
    _expect_error(valid, "manifest.type at /correlation_paths/0/effectors/0/adapter: expected adapter object")
    _write(valid, "{\"id\":4,\"version\":\"1\",\"correlation_paths\":[]}")
    _expect_error(valid, "manifest.type at /id: expected string")

    # Unknown fields and duplicate IDs are deterministic errors.
    _write(valid, "{\"id\":\"pkg\",\"version\":\"1\",\"extra\":true,\"correlation_paths\":[{\"id\":\"path\",\"effectors\":[{\"id\":\"eff\",\"adapter\":{\"kind\":\"manual_homeostat\"}}]}]}")
    _expect_error(valid, "manifest.unknown at /extra: unknown field")
    _write(valid, "{\"id\":\"pkg\",\"version\":\"1\",\"correlation_paths\":[{\"id\":\"path\",\"effectors\":[{\"id\":\"eff\",\"adapter\":{\"kind\":\"manual_homeostat\"}},{\"id\":\"eff\",\"adapter\":{\"kind\":\"manual_homeostat\"}}]}]}")
    _expect_error(valid, "manifest.duplicate at /correlation_paths/0/effectors/1/id: duplicate effector id")
    _write(valid, "{\"id\":\"pkg\",\"version\":\"1\",\"correlation_paths\":[{\"id\":\"path\",\"effectors\":[{\"id\":\"eff\",\"adapter\":{\"kind\":\"manual_homeostat\"}}]},{\"id\":\"path\",\"effectors\":[{\"id\":\"other\",\"adapter\":{\"kind\":\"manual_homeostat\"}}]}]}")
    _expect_error(valid, "manifest.duplicate at /correlation_paths/1/id: duplicate correlation path id")
    _write(valid, "{\"id\":\"pkg\",\"version\":\"1\",\"correlation_paths\":[{\"id\":\"path\",\"effectors\":[{\"id\":\"eff\",\"adapter\":{\"kind\":\"manual_homeostat\",\"unknown\":true}}]}]}")
    _expect_error(valid, "manifest.unknown at /correlation_paths/0/effectors/0/adapter/unknown: unknown field")
    # RFC 6901 escaping and omitted-version default are part of the native contract.
    _write(valid, "{\"id\":\"pkg\",\"correlation_paths\":[{\"id\":\"path\",\"effectors\":[{\"id\":\"eff\",\"adapter\":{\"kind\":\"manual_homeostat\"}}]}]}")
    var default_version = load_package_json(valid)
    _check(default_version.version == "2", "omitted version defaults to 2")
    _write(valid, "{\"id\":\"pkg\",\"a/b~c\":true,\"correlation_paths\":[{\"id\":\"path\",\"effectors\":[{\"id\":\"eff\",\"adapter\":{\"kind\":\"manual_homeostat\"}}]}]}")
    _expect_error(valid, "manifest.unknown at /a~1b~0c: unknown field")

    # Runtime reaction stores are strict filesystem configurations.
    _write(valid, "{\"id\":\"pkg\",\"runtime\":{\"backend\":{\"kind\":\"sqlite\",\"path\":\"state.sqlite\"},\"reaction_store\":{\"kind\":\"memory\",\"root\":\"reactions\"}},\"correlation_paths\":[{\"id\":\"path\",\"effectors\":[{\"id\":\"eff\",\"adapter\":{\"kind\":\"manual_homeostat\"}}]}]}")
    _expect_error(valid, "manifest.unsupported at /runtime/reaction_store/kind: unsupported reaction store kind")
    _write(valid, "{\"id\":\"pkg\",\"runtime\":{\"backend\":{\"kind\":\"sqlite\",\"path\":\"state.sqlite\"},\"reaction_store\":{\"kind\":\"filesystem\"}},\"correlation_paths\":[{\"id\":\"path\",\"effectors\":[{\"id\":\"eff\",\"adapter\":{\"kind\":\"manual_homeostat\"}}]}]}")
    _expect_error(valid, "manifest.missing at /runtime/reaction_store/root: required field is missing")

    # Ontology references and disabled feedback cycles cannot dangle.
    _write(valid, "{\"id\":\"pkg\",\"impulse_types\":[{\"id\":\"input\"}],\"impulse_relations\":[{\"id\":\"derived\",\"source_impulse_types\":[\"missing\"],\"target_impulse_types\":[\"input\"]}],\"correlation_paths\":[{\"id\":\"path\",\"effectors\":[{\"id\":\"eff\",\"adapter\":{\"kind\":\"manual_homeostat\"}}]}]}")
    _expect_error(valid, "manifest.dangling_reference at /impulse_relations/0/source_impulse_types/0: unknown impulse type 'missing'")
    _write(valid, "{\"id\":\"pkg\",\"impulse_types\":[{\"id\":\"input\"}],\"capabilities\":[{\"id\":\"cap\",\"accepts_impulse_types\":[\"missing\"]}],\"correlation_paths\":[{\"id\":\"path\",\"effectors\":[{\"id\":\"eff\",\"capability\":\"cap\",\"adapter\":{\"kind\":\"manual_homeostat\"}}]}]}")
    _expect_error(valid, "manifest.dangling_reference at /capabilities/0/accepts_impulse_types/0: unknown impulse type 'missing'")
    # Conduction cycles are allowed by default in the cybernetic model.
    _write(valid, "{\"id\":\"pkg\",\"correlation_paths\":[{\"id\":\"path\",\"effectors\":[{\"id\":\"root\",\"conduction\":[\"leaf\"],\"adapter\":{\"kind\":\"manual_homeostat\"}},{\"id\":\"leaf\",\"conduction\":[\"root\"],\"adapter\":{\"kind\":\"manual_homeostat\"}}]}]}")
    var cycle_manifest = load_package_json(valid)
    _check(len(cycle_manifest.correlation_paths[0].effectors) == 2, "feedback cycles allowed in manifest")

    # An explicit zero timeout is invalid; omission retains the zero default.
    _write(valid, "{\"id\":\"pkg\",\"correlation_paths\":[{\"id\":\"path\",\"effectors\":[{\"id\":\"eff\",\"timeout_seconds\":0,\"adapter\":{\"kind\":\"manual_homeostat\"}}]}]}")
    _expect_error(valid, "must be greater than 0")
    _write(valid, "{\"id\":\"pkg\",\"correlation_paths\":[{\"id\":\"path\",\"effectors\":[{\"id\":\"eff\",\"adapter\":{\"kind\":\"manual_homeostat\"}}]}]}")
    var omitted_timeout = load_package_json(valid)
    _check(omitted_timeout.correlation_paths[0].effectors[0].timeout_seconds == 0.0, "omitted effector timeout succeeds")

    _write(valid, "{\"id\":\"pkg\",\"correlation_paths\":[{\"id\":\"path\",\"effectors\":[{\"id\":\"eff\",\"adapter\":{\"kind\":\"subprocess\",\"command\":[\"echo\",\"ok\"],\"cwd\":\"work\"}}]}]}")
    var relative_cwd = load_package_json(valid)
    _check(relative_cwd.correlation_paths[0].effectors[0].adapter_cwd == "/tmp/work", "relative adapter cwd resolves against manifest parent")


    # Complete ontology/runtime round-trip uses EmberJson canonical ordering and retains nested values.
    var ontology_fixture = "{\"runtime\":{\"reaction_store\":{\"root\":\"reactions\",\"kind\":\"filesystem\"},\"backend\":{\"path\":\"state.sqlite\",\"kind\":\"sqlite\"}},\"tags\":[\"z\\\\tag\",\"unicode \\u03c0\"],\"correlation_paths\":[{\"effectors\":[{\"config\":{\"nested\":[null,{\"escaped\":\"line\\\\break\\nquote\\\"\"}]},\"adapter\":{\"kind\":\"native_function\",\"ref\":\"native.fn\"},\"tags\":[\"eff-tag\"],\"description\":\"Effector \\u03bb\",\"title\":\"Eff\",\"capability\":\"cap\",\"conduction\":[],\"timeout_seconds\":2.5,\"id\":\"eff\"}],\"tags\":[\"path-tag\"],\"description\":\"Path \\n description\",\"title\":\"Path\",\"accumulate_upstream_reactions\":true,\"id\":\"path\"}],\"version\":3,\"id\":\"pkg\",\"description\":\"Root \\t description\",\"title\":\"Package\",\"impulse_types\":[{\"metadata_schema\":{\"type\":\"object\",\"properties\":{\"note\":{\"type\":\"string\"}}},\"value_schema\":{\"type\":[\"string\",\"null\"],\"const\":null},\"media_types\":[\"text/plain\"],\"tags\":[\"input-tag\"],\"description\":\"Input \\u03b4\",\"title\":\"Input\",\"id\":\"input\"}],\"impulse_relations\":[{\"target_impulse_types\":[\"input\"],\"source_impulse_types\":[\"input\"],\"tags\":[],\"id\":\"derived\"}],\"association_kinds\":[{\"metadata_schema\":{\"type\":\"object\"},\"value_schema\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},\"id\":\"link\"}],\"reaction_kinds\":[{\"media_types\":[\"application/json\"],\"metadata_schema\":{\"type\":\"object\"},\"value_schema\":{\"type\":\"object\",\"properties\":{\"ok\":{\"type\":\"boolean\"}}},\"id\":\"react\"}],\"capabilities\":[{\"output_schema\":{\"type\":\"object\",\"properties\":{\"ok\":{\"type\":\"boolean\"}}},\"config_schema\":{\"type\":\"object\",\"properties\":{\"limit\":{\"type\":\"number\"}}},\"emits_association_kinds\":[\"link\"],\"emits_reaction_kinds\":[\"react\"],\"emits_impulse_types\":[\"input\"],\"accepts_reaction_kinds\":[\"react\"],\"accepts_impulse_types\":[\"input\"],\"description\":\"Capability\",\"title\":\"Cap\",\"id\":\"cap\"}]}"
    _write(valid, ontology_fixture)
    var complete_manifest = load_package_json(valid)
    var canonical = serialize_package_json(complete_manifest)
    var canonical_again = serialize_package_json(complete_manifest)
    _check(canonical == canonical_again, "canonical manifest serialization is deterministic")
    _check(canonical.find("\"nested\"") >= 0 and canonical.find("[null") >= 0, "nested null survives canonicalization")
    _check(canonical.find("line\\\\break\\nquote") >= 0, "escaped strings survive canonicalization")
    _check(canonical.find("\"impulse_types\"") >= 0 and canonical.find("\"runtime\"") >= 0, "ontology and runtime fields serialize")
    _write(valid, canonical)
    var reloaded_manifest = load_package_json(valid)
    _check(serialize_package_json(reloaded_manifest) == canonical, "manifest canonical round-trip")

    # EmberJson duplicate members and malformed JSON/YAML receive guidance.
    _write(valid, "{\"id\":\"pkg\",\"id\":\"again\",\"version\":\"1\",\"correlation_paths\":[]}")
    _expect_error(valid, "manifest.invalid")
    _write(valid, "{bad")
    _expect_error(valid, "invalid JSON manifest; YAML is unsupported")
    _write(valid, "id: pkg\nversion: '1'\n")
    _expect_error(valid, "YAML is unsupported")
    print("native manifest smoke ok")
