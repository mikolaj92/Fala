"""NativeFunctionRegistry smoke (thin callables; no subprocess required)."""

from fala.adapters import (
    NativeFunctionRegistry,
    AdapterSpec,
    EffectorRequest,
    execute_native_function,
)


def _double(input_json: String, config_json: String) raises -> String:
    return "{\"doubled\":true}"


def main() raises:
    var registry = NativeFunctionRegistry()
    registry.register("demo.double", _double)

    var request = EffectorRequest(
        "p1",
        AdapterSpec.native_function("demo.double"),
        "",
        "{\"n\":2}",
        "{}",
        "",
    )
    var result = execute_native_function(request, registry)
    if not result.success:
        raise Error("native_function should succeed: " + result.error.message)
    if result.output_json.find("doubled") < 0:
        raise Error("output missing doubled flag")

    var missing = EffectorRequest(
        "p2",
        AdapterSpec.native_function("missing.fn"),
        "",
        "{}",
        "{}",
        "",
    )
    var bad = execute_native_function(missing, registry)
    if bad.success:
        raise Error("missing ref must fail")
    if bad.error.code != "native_function_not_registered":
        raise Error("expected not_registered code, got " + bad.error.code)

    print("native_function registry smoke ok")
