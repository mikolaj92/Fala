from emberjson import Value
from fala.output_variants import finite_output_variants


def main() raises:
    var schema = Value(parse_string='{"type":"object","required":["route"],"properties":{"route":{"enum":["ready","wait"]}}}')
    var variants = finite_output_variants(schema)
    if not variants.proven or len(variants.values_json) != 2 or variants.field_path != "route": raise Error("required enum must enumerate variants")
    var optional = finite_output_variants(Value(parse_string='{"type":"object","properties":{"route":{"enum":["ready","wait"]}}}'))
    if optional.proven: raise Error("optional field must not prove coverage")
    var dynamic = finite_output_variants(Value(parse_string='{}'))
    if dynamic.proven: raise Error("unconstrained output must not prove coverage")
    print("output variants smoke ok")
