"""Safe provider-neutral provenance, usage validation, and aggregation."""

from emberjson import Object, Value, to_string
from fala.json import canonical_json_text, quote_json_string as quote


def _number(value: Value, path: String) raises -> Float64:
    var result = 0.0
    if value.is_float(): result = value.float()
    elif value.is_int(): result = Float64(value.int())
    elif value.is_uint(): result = Float64(value.uint())
    else: raise Error("usage.invalid at " + path + ": expected number")
    if result < 0.0: raise Error("usage.invalid at " + path + ": must not be negative")
    return result


def validate_usage_json(text: String) raises -> String:
    var usage = Value(parse_string=text)
    if not usage.is_object(): raise Error("usage.invalid at /usage: expected object")
    for pair in usage.object().items():
        if pair.key != "duration_seconds" and pair.key != "input_tokens" and pair.key != "output_tokens" and pair.key != "cost" and pair.key != "unit": raise Error("usage.invalid at /usage/" + pair.key + ": unknown field")
        if pair.key == "unit":
            if not pair.value.is_string() or pair.value.string() == "": raise Error("usage.invalid at /usage/unit: expected nonempty string")
        else: _ = _number(pair.value.copy(), "/usage/" + pair.key)
    if "cost" in usage.object() and "unit" not in usage.object(): raise Error("usage.invalid at /usage/unit: required with cost")
    return canonical_json_text(to_string(usage))


def provenance_json(package_digest: String, path_digest: String, capability: String, adapter_identity: String, adapter_version: String, execution_id: String, attempt: Int, started_at: String, finished_at: String, usage_json: String = "{}", model_id: String = "", tool_id: String = "") raises -> String:
    if package_digest == "" or path_digest == "" or capability == "" or adapter_identity == "" or execution_id == "" or attempt < 1 or started_at == "" or finished_at == "": raise Error("provenance.invalid: required identity fields are missing")
    var usage = validate_usage_json(usage_json)
    var result = "{\"adapter\":{\"identity\":" + quote(adapter_identity) + ",\"version\":" + ("null" if adapter_version == "" else quote(adapter_version)) + "},\"attempt\":" + String(attempt) + ",\"capability\":" + quote(capability) + ",\"execution_id\":" + quote(execution_id) + ",\"finished_at\":" + quote(finished_at) + ",\"package_digest\":" + quote(package_digest) + ",\"path_digest\":" + quote(path_digest) + ",\"started_at\":" + quote(started_at) + ",\"usage\":" + usage
    if model_id != "": result += ",\"model_id\":" + quote(model_id)
    if tool_id != "": result += ",\"tool_id\":" + quote(tool_id)
    result += "}"
    return canonical_json_text(result)


def aggregate_usage(items_json: String) raises -> String:
    var items = Value(parse_string=items_json)
    if not items.is_array(): raise Error("usage.invalid at /: expected array")
    var duration = 0.0; var input_tokens = 0.0; var output_tokens = 0.0; var cost = 0.0; var unit = String("")
    for index in range(len(items.array())):
        var usage = Value(parse_string=validate_usage_json(to_string(items.array()[index].copy())))
        for pair in usage.object().items():
            if pair.key == "duration_seconds": duration += _number(pair.value.copy(), "/" + String(index) + "/duration_seconds")
            elif pair.key == "input_tokens": input_tokens += _number(pair.value.copy(), "/" + String(index) + "/input_tokens")
            elif pair.key == "output_tokens": output_tokens += _number(pair.value.copy(), "/" + String(index) + "/output_tokens")
            elif pair.key == "cost": cost += _number(pair.value.copy(), "/" + String(index) + "/cost")
            elif pair.key == "unit":
                if unit != "" and unit != pair.value.string(): raise Error("usage.invalid: cannot aggregate different cost units")
                unit = pair.value.string()
    return canonical_json_text("{\"cost\":" + String(cost) + ",\"duration_seconds\":" + String(duration) + ",\"input_tokens\":" + String(input_tokens) + ",\"output_tokens\":" + String(output_tokens) + ",\"unit\":" + ("null" if unit == "" else quote(unit)) + "}")
