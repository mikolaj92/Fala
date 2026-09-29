from emberjson import Value
from fala.output_variants import finite_output_variants


def main() raises:
    var schema = Value(parse_string='{"type":"object","required":["route"],"properties":{"route":{"enum":["ready","wait"]}}}')
    var variants = finite_output_variants(schema)
    if not variants.proven or len(variants.values_json) != 2 or variants.field_path != "route": raise Error("required enum must enumerate variants")
    if variants.values_json[0] != "\"ready\"" and variants.values_json[0] != "\"wait\"": raise Error("enum values must be the declared scalars")
    var optional = finite_output_variants(Value(parse_string='{"type":"object","properties":{"route":{"enum":["ready","wait"]}}}'))
    if optional.proven: raise Error("optional field must not prove coverage")
    var dynamic = finite_output_variants(Value(parse_string='{}'))
    if dynamic.proven: raise Error("unconstrained output must not prove coverage")
    var constant = finite_output_variants(Value(parse_string='{"type":"object","required":["ok"],"properties":{"ok":{"const":true}}}'))
    if not constant.proven or constant.field_path != "ok" or len(constant.values_json) != 1 or constant.values_json[0] != "true": raise Error("required const must prove the scalar")
    var unconstrained = finite_output_variants(Value(parse_string='{"type":"object","required":["route"],"properties":{"route":{"type":"string"}}}'))
    if unconstrained.proven: raise Error("unconstrained required string must not prove coverage")
    print("output variants smoke ok")
