from fala.context_policy import resolve_context
from fala.native_package import validate_package_json_text, serialize_package_json
from fala.adapters import AdapterSpec, EffectorRequest, adapter_manifest_json


def expect(value: Bool, message: String) raises:
    if not value: raise Error(message)


def main() raises:
    var manifest = validate_package_json_text("{\"id\":\"p\",\"correlation_paths\":[{\"id\":\"x\",\"effectors\":[{\"id\":\"author\",\"adapter\":{\"kind\":\"manual_homeostat\"},\"context_policy\":\"resume\",\"context_invalidation_digest\":\"prompt-v1\"},{\"id\":\"review\",\"adapter\":{\"kind\":\"manual_homeostat\"},\"conduction\":[\"author\"],\"context_policy\":\"inherit\",\"context_source\":\"author\"},{\"id\":\"independent\",\"adapter\":{\"kind\":\"manual_homeostat\"},\"context_policy\":\"fresh\"}]}]}")
    expect(serialize_package_json(manifest).find("context_source") >= 0, "package context policies")
    var invalid = False
    try: _ = validate_package_json_text("{\"id\":\"p\",\"correlation_paths\":[{\"id\":\"x\",\"effectors\":[{\"id\":\"e\",\"adapter\":{\"kind\":\"manual_homeostat\"},\"context_policy\":\"inherit\"}]}]}")
    except err: invalid = String(err).find("context_source") >= 0
    expect(invalid, "inherit requires explicit source")
    var one = resolve_context("resume", "run", "process", "impulse", "v1")
    var retry = resolve_context("resume", "run", "process", "impulse", "v1")
    var changed = resolve_context("resume", "run", "process", "impulse", "v2")
    expect(one.key == retry.key and one.key != changed.key, "retry stable and invalidation safe")
    var other = resolve_context("fresh", "run", "other-process", "impulse", "v1")
    expect(one.key != other.key, "independent roles do not share context")
    var inherited = resolve_context("inherit", "run", "review", "impulse", "v1", "run:x:author", "succeeded", "{\"package_digest\":\"graph\"}")
    expect(inherited.source_process_id == "run:x:author", "verified explicit inheritance")
    var no_provenance = False
    try: _ = resolve_context("inherit", "run", "review", "", "v1", "author", "succeeded", "{}")
    except err: no_provenance = String(err).find("provenance") >= 0
    expect(no_provenance, "inherit requires source provenance")
    var command = List[String](); command.append("tool")
    var request = EffectorRequest("process", AdapterSpec.subprocess(command^), attempt=2, max_attempts=2, run_id="run", context_json=one.to_json())
    var wire = adapter_manifest_json(request)
    expect(wire.find("\"context\":{") >= 0 and wire.find("ctx:sha256") >= 0 and wire.find("session") < 0, "vendor-neutral wire contract")
    print("context policy smoke ok")
