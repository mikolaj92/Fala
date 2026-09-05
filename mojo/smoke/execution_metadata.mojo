from fala.native_package import validate_package_json_text, serialize_package_json
from fala.execution_metadata import validate_usage_json, provenance_json, aggregate_usage


def expect(value: Bool, message: String) raises:
    if not value: raise Error(message)


def main() raises:
    var package = "{\"id\":\"p\",\"capabilities\":[{\"id\":\"publish\",\"secret_handles\":[\"TOKEN\"]}],\"correlation_paths\":[{\"id\":\"x\",\"effectors\":[{\"id\":\"e\",\"capability\":\"publish\",\"adapter\":{\"kind\":\"subprocess\",\"command\":[\"true\"],\"inherit_env\":[\"TOKEN\"],\"env\":{\"AUTH\":\"${env:TOKEN}\"}}}]}]}"
    var manifest = validate_package_json_text(package)
    var public = serialize_package_json(manifest)
    expect(public.find("secret_handles") >= 0 and public.find("canary-secret-value") < 0, "manifest stores handles only")
    var rejected = False
    try: _ = validate_package_json_text("{\"id\":\"p\",\"capabilities\":[{\"id\":\"publish\",\"secret_handles\":[\"TOKEN\"]}],\"correlation_paths\":[{\"id\":\"x\",\"effectors\":[{\"id\":\"e\",\"capability\":\"publish\",\"adapter\":{\"kind\":\"subprocess\",\"command\":[\"true\"],\"inherit_env\":[\"OTHER\"]}}]}]}")
    except err: rejected = String(err).find("manifest.secret_scope") >= 0
    expect(rejected, "undeclared secret request fails closed")
    var usage = validate_usage_json("{\"duration_seconds\":1.5,\"input_tokens\":10,\"output_tokens\":2,\"cost\":0.25,\"unit\":\"USD\"}")
    var provenance = provenance_json("graph-sha", "path-sha", "publish", "subprocess:/usr/bin/tool", "1", "run:path:e", 2, "start", "finish", usage, model_id="model", tool_id="tool")
    expect(provenance.find("graph-sha") >= 0 and provenance.find("\"attempt\":2") >= 0 and provenance.find("\"usage\"") >= 0, "full terminal provenance")
    var invalid = False
    try: _ = validate_usage_json("{\"cost\":-1,\"unit\":\"USD\"}")
    except err: invalid = String(err).find("usage.invalid") >= 0
    expect(invalid, "invalid usage fails closed")
    var total = aggregate_usage("[{\"duration_seconds\":1,\"cost\":0.2,\"unit\":\"USD\"},{\"duration_seconds\":2,\"cost\":0.3,\"unit\":\"USD\",\"input_tokens\":4}]")
    expect(total.find("\"duration_seconds\":3") >= 0 and total.find("\"cost\":0.5") >= 0 and total.find("\"input_tokens\":4") >= 0, "run usage aggregation")
    print("execution metadata smoke ok")
