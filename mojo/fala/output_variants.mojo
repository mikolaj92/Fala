"""Conservative finite discriminants shared by graph and fixture coverage.

Only required scalar const/enum properties prove a finite partition. Optional
fields and unconstrained outputs are explicitly unproven, never green coverage.
"""
from emberjson import Value, to_string
from fala.json_schema import validate_schema
from fala.json import canonical_json_text


@fieldwise_init
struct OutputVariants(Copyable, Movable):
    var proven: Bool
    var field_path: String
    var values_json: List[String]
    var reason: String


def finite_output_variants(schema: Value) raises -> OutputVariants:
    validate_schema(schema)
    var result = OutputVariants(proven=False, field_path="", values_json=List[String](), reason="no required finite scalar discriminant")
    if "type" not in schema.object() or not schema.object()["type"].is_string() or schema.object()["type"].string() != "object": return result^
    if "required" not in schema.object() or "properties" not in schema.object(): return result^
    for name in schema.object()["required"].array():
        var key = name.string()
        if key not in schema.object()["properties"].object(): continue
        var property = schema.object()["properties"].object()[key].copy()
        var values = List[String]()
        if "const" in property.object():
            var candidate = property.object()["const"].copy()
            if candidate.is_object() or candidate.is_array(): continue
            values.append(canonical_json_text(to_string(candidate)))
        elif "enum" in property.object():
            var scalar = True
            for candidate in property.object()["enum"].array():
                if candidate.is_object() or candidate.is_array(): scalar = False
                var encoded = canonical_json_text(to_string(candidate.copy()))
                if encoded not in values: values.append(encoded)
            if not scalar: continue
        if len(values) == 0: continue
        # Dot-separated runtime paths cannot address a literal dotted key.
        if key.find(".") >= 0 or key == "": continue
        result.proven = True
        result.field_path = key
        result.values_json = values^
        result.reason = "required finite scalar discriminant"
        return result^
    return result^
