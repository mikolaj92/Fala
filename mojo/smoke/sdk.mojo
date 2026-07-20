from fala.sdk import (
    SdkUnavailableError,
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
    serialize_result,
    run_manifest_effector,
)


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("sdk smoke: " + message)


def _expect_error(call_kind: String, text: String, code: String, path: String) raises:
    var matched = False
    try:
        if call_kind == "manifest":
            _ = load_manifest(text)
        elif call_kind == "input":
            _ = input_values(text)
        elif call_kind == "output":
            _ = output(text)
        elif call_kind == "associations":
            _ = output("{}", text)
    except err:
        var diagnostic = String(err)
        matched = diagnostic.find(code + " at " + path + ":") >= 0
    _check(matched, "typed error " + code + " at " + path)


def main() raises:
    var manifest = "{\"input\":{\"source\":\"hello\",\"conduction\":{\"up\":{\"n\":1}},\"upstream_reactions\":[{\"kind\":\"draft\",\"v\":1},{\"kind\":\"draft\",\"v\":2},{\"kind\":\"final\"}]},\"config\":{\"limit\":2}}"
    _check(load_manifest(manifest) == "{\"config\":{\"limit\":2},\"input\":{\"conduction\":{\"up\":{\"n\":1}},\"source\":\"hello\",\"upstream_reactions\":[{\"kind\":\"draft\",\"v\":1},{\"kind\":\"draft\",\"v\":2},{\"kind\":\"final\"}]}}", "manifest canonicalization")
    _check(input_values(manifest).find("source") >= 0, "input object")
    _check(declared_inputs(manifest) == "{\"source\":\"hello\"}", "injected keys filtered")
    _check(conduction(manifest) == "{\"up\":{\"n\":1}}", "conduction object")
    _check(upstream_reactions(manifest).find("\"v\":2") >= 0, "reaction objects filtered")
    _check(find_reaction(manifest, "draft") == "{\"kind\":\"draft\",\"v\":2}", "latest reaction wins")
    _expect_error("input", "[]", "sdk.invalid_type", "/manifest")
    _expect_error("manifest", "{bad", "sdk.invalid_json", "/manifest")
    _expect_error("input", "{\"input\":[]}", "sdk.invalid_type", "/manifest/input")
    _expect_error("output", "[]", "sdk.invalid_type", "/values")
    _expect_error("associations", "{\"bad\":true}", "sdk.invalid_type", "/associations")
    var result = output("{\"ok\":true}", "[{\"kind\":\"a\"},3,{\"kind\":\"b\"}]", "[{\"kind\":\"draft\",\"v\":1},{\"kind\":\"draft\",\"v\":2}]", "{\"telemetry\":{\"ms\":12}}")
    _check(result == "{\"associations\":[{\"kind\":\"a\"},{\"kind\":\"b\"}],\"metadata\":{\"telemetry\":{\"ms\":12}},\"reactions\":[{\"kind\":\"draft\",\"v\":1},{\"kind\":\"draft\",\"v\":2}],\"values\":{\"ok\":true}}", "exact output envelope")
    _check(output_reactions(result).find("\"v\":2") >= 0, "output reactions")
    _check(find_output_reaction(result, "draft") == "{\"kind\":\"draft\",\"v\":2}", "latest output reaction")
    _check(output_metadata(result) == "{\"telemetry\":{\"ms\":12}}", "output metadata")
    _check(serialize_result("{\"z\":1,\"a\":{\"z\":2,\"a\":3}}") == "{\n    \"a\": {\n        \"a\": 3,\n        \"z\": 2\n    },\n    \"z\": 1\n}", "pretty sorted serialization")

    var unavailable = run_manifest_effector()
    _check(unavailable.is_unavailable() and unavailable.code == "sdk.execution_unavailable", "typed unavailable execution")
    print("sdk smoke ok")
