"""Shared domain payload corpus: journal and conduction must agree."""
from emberjson import Value
from fala.journal import validate_json_schema_value, NativeJournal
from fala.effector_protocol import result_message
from fala.correlation_advance import _validate_projected_schema, _project_output
from fala.correlation_persistence import _project_output as persisted_projection, _validate_output_schema
from fala.native_package import _json_schema, validate_package_json_text, serialize_correlation_path_json


def rejected(payload: String, schema: String, conduction: Bool = False) -> Bool:
    try:
        if conduction:
            _validate_projected_schema(Value(parse_string=payload), Value(parse_string=schema), "/test")
        else:
            validate_json_schema_value(Value(parse_string=payload), Value(parse_string=schema), "/test")
    except:
        return True
    return False


def expect(value: Bool, message: String) raises:
    if not value: raise Error(message)


def main() raises:
    var schema = '{"type":"object","required":["route"],"properties":{"route":{"type":"string","enum":["ready","wait"]}},"oneOf":[{"properties":{"route":{"const":"ready"},"artifact":{"type":"string"}},"required":["artifact"]},{"properties":{"route":{"const":"wait"},"reason":{"type":"string"}},"required":["reason"]}]}'
    for conduction in [False, True]:
        for payload in ['{"route":"ready","artifact":"local.txt"}', '{"route":"wait","reason":"missing input"}']:
            expect(not rejected(payload, schema, conduction), "valid variant rejected")
        for payload in ['{"route":"unknown"}', '{"route":"ready"}', '{"route":"ready","artifact":3}', '{"route":"ready","reason":"wrong variant"}']:
            expect(rejected(payload, schema, conduction), "invalid variant accepted: " + payload)
        expect(rejected('{"child":{"n":"wrong"}}', '{"properties":{"child":{"properties":{"n":{"type":"integer"}}}}}', conduction), "nested mismatch accepted")
        expect(rejected('1', '{"oneOf":[{"type":"integer"},{"type":"number"}]}', conduction), "oneOf ambiguity accepted")
        expect(not rejected('1', '{"anyOf":[{"type":"integer"},{"type":"number"}]}', conduction), "anyOf overlap rejected")
        expect(rejected('1', '{"allOf":[{"type":"integer"},{"minimum":2}]}', conduction), "allOf ignored")
        expect(rejected('[]', '{"minItems":1}', conduction), "minItems ignored")
        expect(rejected('[1,2]', '{"maxItems":1}', conduction), "maxItems ignored")
        expect(rejected('"wrong"', '{"pattern":"^ok$"}', conduction), "unsupported pattern silently accepted")
        expect(rejected('1', '{"anyOf":[{}, {"type":42}]}', conduction), "malformed branch hidden by matching branch")
    for declaration in ['{"pattern":"^ok$"}', '{"anyOf":[{}, {"type":42}]}']:
        var loader_rejected = False
        try: _json_schema(Value(parse_string=declaration), "/schema")
        except: loader_rejected = True
        expect(loader_rejected, "loader accepted unsupported declaration")
        var persistence_rejected = False
        try: _validate_output_schema(declaration, "/schema")
        except: persistence_rejected = True
        expect(persistence_rejected, "persistence accepted unsupported declaration")
    var output = Value(parse_string='{"route":"ready","artifact":"local.txt"}')
    var projected = _project_output(output, schema)
    _validate_projected_schema(projected, Value(parse_string=schema), "/projection")
    var persisted = persisted_projection(output, schema)
    _validate_projected_schema(persisted, Value(parse_string=schema), "/persistence-projection")
    var manifest = validate_package_json_text('{"id":"contracts","correlation_paths":[{"id":"ship","effectors":[{"id":"decide","output_schema":' + schema + ',"adapter":{"kind":"subprocess","command":["/must/not/run"]}}]}]}')
    expect(serialize_correlation_path_json(manifest.correlation_paths[0]).find('"output_schema"') >= 0, "package lost output declaration")
    var journal = NativeJournal.open(":memory:")
    journal.initialize()
    _ = journal.create_run("contract", "created", "{}", "2026-01-01T00:00:00Z")
    _ = journal.schedule_process("contract", "source", "native_function", "2026-01-01T00:00:00Z", output_schema_json=schema)
    _ = journal.claim_process("contract", "source", "test", "2026-01-01T00:00:01Z", "2099-01-01T00:00:00Z")
    var message = result_message("source", "parent", "source", "msg:request", '{"route":"ready","artifact":"local.txt"}')
    var fep_projected = _project_output(Value(parse_string=message), schema)
    _validate_projected_schema(fep_projected, Value(parse_string=schema), "/fep-projection")
    var fep_persisted = persisted_projection(Value(parse_string=message), schema)
    _validate_projected_schema(fep_persisted, Value(parse_string=schema), "/fep-persisted")
    var with_telemetry = message.replace('"kind":', '"adapter":{"returncode":0},"kind":')
    _ = journal.complete_process("contract", "source", "test", "2026-01-01T00:00:02Z", with_telemetry)
    expect(journal.get_process("contract", "source").status == "succeeded", "Fala payload must satisfy domain schema")
    journal.close()
    print("output contracts smoke ok")
