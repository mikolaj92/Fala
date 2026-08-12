"""Native, durable correlation advancement over journal process projections.

Readiness is computed with the pure correlation graph helpers, while SQLite is
used only for the atomic pending-to-ready promotion.  Dead upstreams are
reported, not executed or silently converted into reactions.
"""

from std.collections import List

from fala.adapters import adapter_spec_from_json
from fala.reactions import filter_reactions_json

from fala.correlation import (
    CorrelationEffectorSpec,
    CorrelationPathSpec,
    CorrelationBlocked,
    CorrelationWaitDiagnostic,
    CorrelationConductionValue,
    CorrelationExecutionState,
    CorrelationInstantiationPlan,
    CorrelationProcessPlan,
    CorrelationAdvancePlan,
    Readiness,
    diagnose_correlation_wait,
    advance_correlation_states,
    project_conduction,
    validate_correlation_input_json,
)
from emberjson import Array, Object, Value, to_string
from fala.json import canonical_json_text, json_values_equal
from fala.journal import NativeJournal, ProcessRow, CorrelationChildTransition

from fala.sqlite import SQLiteError


@fieldwise_init
struct CorrelationAdvanceError(Copyable, Movable):
    """Stable, typed diagnostics for an advancement boundary failure."""

    var code: String
    var path: String
    var message: String

    def __str__(self) -> String:
        return self.code + " at " + self.path + ": " + self.message


@fieldwise_init
struct CorrelationReactionMarker(Copyable, Movable):
    """Explicit marker for the native reaction boundary.

    Advancement never fabricates reaction effects.  Consumers can persist or
    replay this marker when a native reaction API is introduced.
    """

    var code: String
    var process_id: String
    var replay_safe: Bool
    var message: String

@fieldwise_init
struct CorrelationAdvanceResult(Copyable, Movable):
    """Durable rows plus deterministic advancement projections."""

    var rows: List[ProcessRow]
    var readied: List[CorrelationProcessPlan]
    var conduction: List[CorrelationConductionValue]
    var blocked: List[CorrelationBlocked]
    var cancelled: List[CorrelationBlocked]
    var readiness: Readiness
    var wait_diagnostic: CorrelationWaitDiagnostic
    var reaction: CorrelationReactionMarker
    var replayed: Bool


def _contains(values: List[String], wanted: String) -> Bool:
    for value in values:
        if value == wanted:
            return True
    return False


def _find_row(rows: List[ProcessRow], process_id: String) -> Int:
    for index in range(len(rows)):
        if rows[index].id == process_id:
            return index
    return -1


def _find_plan(plan: CorrelationInstantiationPlan, process_id: String) -> Int:
    for index in range(len(plan.processes)):
        if plan.processes[index].id == process_id:
            return index
    return -1


def validate_correlation_advance_plan(plan: CorrelationInstantiationPlan) -> CorrelationAdvanceError:
    """Validate plan identity and conduction references without touching SQLite."""
    if plan.run_id == "":
        return CorrelationAdvanceError("correlation.advance.missing_plan", "/run_id", "run id must not be empty")
    if plan.correlation_path_id == "":
        return CorrelationAdvanceError("correlation.advance.missing_plan", "/correlation_path_id", "correlation path id must not be empty")
    if len(plan.processes) == 0:
        return CorrelationAdvanceError("correlation.advance.missing_plan", "/processes", "correlation plan has no processes")
    var ids = List[String]()
    var effectors = List[String]()
    for index in range(len(plan.processes)):
        var item = plan.processes[index].copy()
        var path = "/processes/" + String(index)
        if item.run_id != plan.run_id:
            return CorrelationAdvanceError("correlation.advance.missing_plan", path + "/run_id", "process run differs from plan")
        if item.id == "" or item.effector_id == "":
            return CorrelationAdvanceError("correlation.advance.missing_plan", path, "process and effector ids must not be empty")
        if not item.id.startswith(plan.correlation_path_id + ":"):
            return CorrelationAdvanceError("correlation.advance.missing_plan", path + "/id", "process is outside correlation path")
        if _contains(ids, item.id) or _contains(effectors, item.effector_id):
            return CorrelationAdvanceError("correlation.advance.missing_plan", path, "duplicate process or effector id")
        ids.append(item.id)
        effectors.append(item.effector_id)
    for index in range(len(plan.processes)):
        var item = plan.processes[index].copy()
        for upstream_index in range(len(item.conduction)):
            var upstream = item.conduction[upstream_index]
            if upstream == "":
                return CorrelationAdvanceError("correlation.advance.unknown_upstream", "/processes/" + String(index) + "/conduction", "conduction upstream must not be empty")
            if upstream == item.effector_id:
                return CorrelationAdvanceError("correlation.advance.missing_plan", "/processes/" + String(index) + "/conduction", "effector cannot depend on itself")
            for prior_index in range(upstream_index):
                if item.conduction[prior_index] == upstream:
                    return CorrelationAdvanceError("correlation.advance.missing_plan", "/processes/" + String(index) + "/conduction", "duplicate conduction reference: " + upstream)
            if not _contains(effectors, upstream):
                return CorrelationAdvanceError("correlation.advance.unknown_upstream", "/processes/" + String(index) + "/conduction", "unknown upstream " + upstream)
    return CorrelationAdvanceError("", "", "")


def _validate(plan: CorrelationInstantiationPlan) raises:
    var diagnostic = validate_correlation_advance_plan(plan)
    if diagnostic.code != "":
        raise Error(diagnostic.__str__())
    for item in plan.processes:
        var input = Value(parse_string=item.input_json)
        if not input.is_object():
            raise Error("correlation.advance.invalid_input at /processes/" + item.id + "/input_json: expected JSON object")
        var config = Value(parse_string=item.config_json)
        if not config.is_object():
            raise Error("correlation.advance.invalid_config at /processes/" + item.id + "/config_json: expected JSON object")
        var output_schema = Value(parse_string=item.output_schema_json)
        if not output_schema.is_object():
            raise Error("correlation.advance.invalid_output_schema at /processes/" + item.id + "/output_schema_json: expected JSON object")
        var authored = Object(capacity=len(input.object()))
        for pair in input.object().items():
            if pair.key != "conduction" and pair.key != "upstream_reactions" and pair.key != "regulation": authored[pair.key] = pair.value.copy()
        validate_correlation_input_json(canonical_json_text(to_string(Value(authored^))))

def _canonical_object(text: String) raises -> String:
    var parsed = Value(parse_string=text)
    if not parsed.is_object():
        raise Error("expected JSON object")
    return canonical_json_text(to_string(parsed^))

def _same_durable_row(row: ProcessRow, item: CorrelationProcessPlan) raises -> Bool:
    if row.run_id != item.run_id or row.id != item.id or row.process_type != "correlation.effector" or row.priority != item.priority or row.max_attempts != item.max_attempts:
        return False
    var expected_input = Value(parse_string=item.input_json)
    var persisted_input = Value(parse_string=row.input_json)
    if not expected_input.is_object() or not persisted_input.is_object():
        return False
    var authored = Object(capacity=len(persisted_input.object()))
    for pair in persisted_input.object().items():
        if pair.key != "conduction" and pair.key != "upstream_reactions" and pair.key != "regulation": authored[pair.key] = pair.value.copy()
    var expected_authored = Object(capacity=len(expected_input.object()))
    for pair in expected_input.object().items():
        if pair.key != "conduction" and pair.key != "upstream_reactions" and pair.key != "regulation": expected_authored[pair.key] = pair.value.copy()
    if canonical_json_text(to_string(authored^)) != canonical_json_text(to_string(expected_authored^)):
        return False
    var metadata = Value(parse_string=row.metadata)
    var expected_metadata = Value(parse_string=item.metadata_json)
    if not metadata.is_object() or not expected_metadata.is_object():
        return False
    # The row stores plan metadata alongside injected persistence fields.  Compare
    # every plan-owned marker field so replay cannot reuse a row for a different
    # path/spec declaration, sequence, regulation, accumulation policy, or
    # accepted reaction set.  Wait diagnostics are advancement state, not identity.
    var persisted_marker = Object(capacity=len(metadata.object()))
    for pair in metadata.object().items():
        if pair.key == "__adapter_binding":
            try:
                var binding = adapter_spec_from_json(to_string(pair.value.copy()))
                _ = binding
            except err:
                raise Error("correlation.advance.invalid_metadata at /processes/" + item.id + "/metadata/__adapter_binding: invalid adapter binding")
        elif pair.key != "__correlation_config" and pair.key != "__correlation_output_schema" and pair.key != "__correlation_conduction" and pair.key != "__correlation_timeout_seconds" and pair.key != "__correlation_wait_diagnostic":
            persisted_marker[pair.key] = pair.value.copy()
    var expected_marker = Object(capacity=len(expected_metadata.object()))
    for pair in expected_metadata.object().items():
        if pair.key != "__correlation_config" and pair.key != "__correlation_output_schema" and pair.key != "__correlation_conduction" and pair.key != "__correlation_timeout_seconds" and pair.key != "__correlation_wait_diagnostic":
            expected_marker[pair.key] = pair.value.copy()
    if canonical_json_text(to_string(persisted_marker^)) != canonical_json_text(to_string(expected_marker^)):
        return False
    # Keep the typed declaration sequence bound to the durable marker as well;
    # otherwise a caller could mutate the plan field while retaining stale JSON.
    if "seq" not in expected_metadata.object():
        return False
    var expected_seq = expected_metadata.object()["seq"].copy()
    if expected_seq.is_int():
        if Int(expected_seq.int()) != item.declaration_seq:
            return False
    elif expected_seq.is_uint():
        if Int(expected_seq.uint()) != item.declaration_seq:
            return False
    else:
        return False
    if "effector_id" not in expected_metadata.object() or not expected_metadata.object()["effector_id"].is_string() or expected_metadata.object()["effector_id"].string() != item.effector_id:
        return False
    var config = ""; var schema = ""; var conduction = ""; var timeout = ""
    for pair in metadata.object().items():
        if pair.key == "__correlation_config": config = canonical_json_text(to_string(pair.value.copy()))
        elif pair.key == "__correlation_output_schema": schema = canonical_json_text(to_string(pair.value.copy()))
        elif pair.key == "__correlation_conduction": conduction = canonical_json_text(to_string(pair.value.copy()))
        elif pair.key == "__correlation_timeout_seconds": timeout = canonical_json_text(to_string(pair.value.copy()))
    var expected_config = _canonical_object(item.config_json)
    var expected_schema = _canonical_object(item.output_schema_json)
    var expected_conduction = Array(capacity=len(item.conduction))
    for upstream in item.conduction: expected_conduction.append(Value(upstream))
    var expected_conduction_json = canonical_json_text(to_string(Value(expected_conduction^)))
    var expected_timeout = canonical_json_text(to_string(Value(item.timeout_seconds)))
    return config == expected_config and schema == expected_schema and conduction == expected_conduction_json and timeout == expected_timeout

def _states(plan: CorrelationInstantiationPlan, rows: List[ProcessRow]) raises -> List[CorrelationExecutionState]:
    var states = List[CorrelationExecutionState]()
    for index in range(len(plan.processes)):
        var item = plan.processes[index].copy()
        var row_index = _find_row(rows, item.id)
        if row_index < 0:
            raise Error("correlation.advance.missing_plan at /processes/" + item.id + ": durable process row is missing")
        var row = rows[row_index].copy()
        var output = row.output_json
        if row.status == "succeeded":
            try:
                var output_value = Value(parse_string=output)
                if not output_value.is_object():
                    raise Error("root output must be an object")
                output = canonical_json_text(output)
                _validate_projected_schema(_project_output(output_value, item.output_schema_json), Value(parse_string=item.output_schema_json), "/processes/" + item.id + "/output_json")
            except err:
                raise Error("correlation.advance.invalid_output at /processes/" + item.id + "/output_json: succeeded output is invalid")
        elif row.status == "failed" or row.status == "cancelled" or row.status == "timed_out":
            output = row.error_json
        states.append(CorrelationExecutionState(process_id=row.id, effector_id=item.effector_id, status=row.status, attempt=row.attempt, max_attempts=row.max_attempts, output_json=output, input_json=row.input_json, reactions_json="{}", conduction=item.conduction.copy()))
    return states^


def _readiness(rows: List[ProcessRow], plan: CorrelationInstantiationPlan) -> Readiness:
    var ready = List[String]()
    var blocked = List[String]()
    for item in plan.processes:
        var index = _find_row(rows, item.id)
        if index < 0:
            continue
        if rows[index].status == "ready":
            ready.append(item.effector_id)
        elif rows[index].status == "pending":
            blocked.append(item.effector_id)
    return Readiness(ready=ready^, blocked=blocked^)


def _ordered_rows(plan: CorrelationInstantiationPlan, rows: List[ProcessRow]) -> List[ProcessRow]:
    var result = List[ProcessRow]()
    for item in plan.processes:
        var index = _find_row(rows, item.id)
        if index >= 0:
            result.append(rows[index].copy())
    return result^




def _project_output(output: Value, output_schema_json: String) raises -> Value:
    """Strip adapter execution envelope and apply top-level schema projection."""
    if not output.is_object():
        raise Error("expected JSON object")
    var source = Object(capacity=len(output.object()))
    for pair in output.object().items():
        if pair.key != "adapter": source[pair.key] = pair.value.copy()
    var schema = Value(parse_string=output_schema_json)
    if schema.is_object() and "properties" in schema.object():
        var properties = schema.object()["properties"].copy()
        if properties.is_object() and len(properties.object()) > 0:
            var projected = Object(capacity=len(properties.object()))
            for pair in properties.object().items():
                if pair.key in source: projected[pair.key] = source[pair.key].copy()
            return Value(projected^)
    if "values" in source and source["values"].is_object():
        return source["values"].copy()
    return Value(source^)
def _schema_number(value: Value) -> Float64:
    if value.is_float(): return value.float()
    if value.is_int(): return Float64(value.int())
    if value.is_uint(): return Float64(value.uint())
    return 0.0

def _schema_kind_matches(value: Value, kind: String) -> Bool:
    if kind == "object": return value.is_object()
    if kind == "array": return value.is_array()
    if kind == "string": return value.is_string()
    if kind == "boolean": return value.is_bool()
    if kind == "number": return value.is_int() or value.is_uint() or value.is_float()
    if kind == "integer":
        if value.is_int() or value.is_uint(): return True
        if value.is_float():
            var numeric = value.float()
            if numeric != numeric or numeric < -9223372036854775808.0 or numeric >= 9223372036854775808.0:
                return False
            return Float64(Int(numeric)) == numeric
        return False
    if kind == "null": return value.is_null()
    return False

def _schema_type_matches(value: Value, schema: Value) raises -> Bool:
    if not schema.is_object() or "type" not in schema.object(): return True
    var type_value = schema.object()["type"].copy()
    if type_value.is_string(): return _schema_kind_matches(value, type_value.string())
    if type_value.is_array():
        var matched = False
        for member in type_value.array():
            if not member.is_string(): return False
            if _schema_kind_matches(value, member.string()): matched = True
        return matched
    return False



def _schema_codepoint_length(value: String) -> Int:
    var count = 0
    for _ in value.codepoint_slices():
        count += 1
    return count
def _validate_projected_schema(value: Value, schema: Value, path: String) raises:
    """Validate the small JSON-Schema subset used by native conduction."""
    if not schema.is_object(): return
    var schema_object = schema.object().copy()
    if "const" in schema_object:
        var expected_const = schema_object["const"].copy()
        if not json_values_equal(value, expected_const^):
            raise Error("correlation.advance.invalid_output at " + path + ": output does not match schema const")
    if "enum" in schema_object:
        var values = schema_object["enum"].copy()
        if values.is_array():
            var enum_match = False
            for candidate in values.array():
                if json_values_equal(value, candidate):
                    enum_match = True
                    break
            if not enum_match:
                raise Error("correlation.advance.invalid_output at " + path + ": output does not match schema enum")
    if not _schema_type_matches(value, schema):
        raise Error("correlation.advance.invalid_output at " + path + ": output does not match schema type")
    var value_is_number = value.is_int() or value.is_uint() or value.is_float()
    if value_is_number:
        var actual_number = _schema_number(value)
        if "minimum" in schema_object:
            var minimum = schema_object["minimum"].copy()
            if minimum.is_int() or minimum.is_uint() or minimum.is_float():
                if actual_number < _schema_number(minimum):
                    raise Error("correlation.advance.invalid_output at " + path + ": output is below schema minimum")
        if "maximum" in schema_object:
            var maximum = schema_object["maximum"].copy()
            if maximum.is_int() or maximum.is_uint() or maximum.is_float():
                if actual_number > _schema_number(maximum):
                    raise Error("correlation.advance.invalid_output at " + path + ": output exceeds schema maximum")
    if value.is_string():
        var string_length = Float64(_schema_codepoint_length(String(value.string())))
        if "minLength" in schema_object:
            var min_length = schema_object["minLength"].copy()
            if (min_length.is_int() or min_length.is_uint()) and string_length < _schema_number(min_length):
                raise Error("correlation.advance.invalid_output at " + path + ": output is shorter than schema minLength")
        if "maxLength" in schema_object:
            var max_length = schema_object["maxLength"].copy()
            if (max_length.is_int() or max_length.is_uint()) and string_length > _schema_number(max_length):
                raise Error("correlation.advance.invalid_output at " + path + ": output exceeds schema maxLength")
    if value.is_array():
        var item_count = Float64(len(value.array()))
        if "minItems" in schema_object:
            var min_items = schema_object["minItems"].copy()
            if (min_items.is_int() or min_items.is_uint()) and item_count < _schema_number(min_items):
                raise Error("correlation.advance.invalid_output at " + path + ": output has fewer items than schema minItems")
        if "maxItems" in schema_object:
            var max_items = schema_object["maxItems"].copy()
            if (max_items.is_int() or max_items.is_uint()) and item_count > _schema_number(max_items):
                raise Error("correlation.advance.invalid_output at " + path + ": output has more items than schema maxItems")
    if value.is_object() and "required" in schema_object:
        var required = schema_object["required"].copy()
        if required.is_array():
            for key in required.array():
                if key.is_string() and key.string() not in value.object():
                    raise Error("correlation.advance.invalid_output at " + path + ": missing required output field " + key.string())
    if value.is_object() and "additionalProperties" in schema_object:
        var additional = schema_object["additionalProperties"].copy()
        if additional.is_bool() and not additional.bool():
            var properties = Object(capacity=0)
            if "properties" in schema_object and schema_object["properties"].is_object():
                properties = schema_object["properties"].object().copy()
            for pair in value.object().items():
                if pair.key not in properties:
                    raise Error("correlation.advance.invalid_output at " + path + ": output contains additional property " + pair.key)
    if value.is_object() and "properties" in schema_object:
        var properties = schema_object["properties"].copy()
        if properties.is_object():
            for pair in properties.object().items():
                if pair.key in value.object():
                    _validate_projected_schema(value.object()[pair.key], pair.value, path + "/" + pair.key)
    if value.is_array() and "items" in schema_object:
        var item_schema = schema_object["items"].copy()
        for index in range(len(value.array())):
            _validate_projected_schema(value.array()[index], item_schema, path + "/" + String(index))
def _reaction_list(output: Value, allowed: List[String]) raises -> String:
    """Validate and filter reaction objects through the native JSON boundary."""
    if not output.is_object():
        raise Error("correlation.advance.invalid_reactions: expected output object")
    if "reactions" not in output.object():
        return "[]"
    var reactions = output.object()["reactions"].copy()
    if not reactions.is_array():
        raise Error("correlation.advance.invalid_reactions: reactions must be an array")
    for reaction in reactions.array():
        if not reaction.is_object():
            raise Error("correlation.advance.invalid_reactions: reaction must be an object")
        if "kind" not in reaction.object() or not reaction.object()["kind"].is_string() or reaction.object()["kind"].string() == "":
            raise Error("correlation.advance.invalid_reactions: reaction kind must be a non-empty string")
        if "uri" in reaction.object() and not reaction.object()["uri"].is_string():
            raise Error("correlation.advance.invalid_reactions: reaction uri must be a string")
        if "id" in reaction.object() and not reaction.object()["id"].is_string():
            raise Error("correlation.advance.invalid_reactions: reaction id must be a string")
    var output_copy = output.copy()
    return filter_reactions_json(canonical_json_text(to_string(output_copy^)), allowed)
def _ancestor_effectors(plan: CorrelationInstantiationPlan, effector_id: String) raises -> List[String]:
    var order = List[String]()
    var seen = List[String]()
    var cursor = List[String]()
    var finished = List[Bool]()
    cursor.append(effector_id)
    finished.append(False)
    while len(cursor) > 0:
        var current = cursor.pop()
        var done = finished.pop()
        if done:
            if current != effector_id:
                order.append(current)
            continue
        if _contains(seen, current): continue
        var index = -1
        for candidate_index in range(len(plan.processes)):
            if plan.processes[candidate_index].effector_id == current:
                index = candidate_index
                break
        if index < 0: continue
        seen.append(current)
        cursor.append(current)
        finished.append(True)
        var conduction = plan.processes[index].conduction.copy()
        for offset in range(len(conduction)):
            var upstream = conduction[len(conduction) - 1 - offset]
            if not _contains(seen, upstream) and upstream != effector_id:
                cursor.append(upstream)
                finished.append(False)
    return order^


def _merged_input(item: CorrelationProcessPlan, plan: CorrelationInstantiationPlan, states: List[CorrelationExecutionState]) raises -> String:
    """Merge terminal upstream payloads under conduction, preserving authored keys.

    Succeeded upstreams are schema-projected. Failed / cancelled / timed_out
    upstreams conduct their error payload as an object without success schema.
    """
    var authored = Value(parse_string=item.input_json)
    if not authored.is_object():
        raise Error("correlation.advance.invalid_input at /processes/" + item.id + "/input_json: expected JSON object")
    var source = authored.object().copy()
    var merged = Object(capacity=len(source) + 1)
    for pair in source.items():
        var authored_value = pair.value.copy()
        merged[pair.key] = authored_value^
    var downstream_index = -1
    for index in range(len(states)):
        if states[index].process_id == item.id:
            downstream_index = index
            break
    if downstream_index < 0:
        raise Error("correlation.advance.missing_plan at /processes/" + item.id + ": execution projection is missing")
    var values = project_conduction(states, states[downstream_index])
    var conduction = Object(capacity=len(values))
    for value in values:
        var output = Value(parse_string=value.output_json)
        var source_plan_index = -1
        var upstream_status = ""
        for candidate_index in range(len(states)):
            if states[candidate_index].effector_id == value.upstream_effector_id:
                upstream_status = states[candidate_index].status
                break
        for candidate_index in range(len(plan.processes)):
            if plan.processes[candidate_index].effector_id == value.upstream_effector_id:
                source_plan_index = candidate_index
                break
        if source_plan_index < 0:
            raise Error("correlation.advance.unknown_upstream: " + value.upstream_effector_id)
        if upstream_status == "succeeded":
            var projected = _project_output(output, plan.processes[source_plan_index].output_schema_json)
            _validate_projected_schema(projected, Value(parse_string=plan.processes[source_plan_index].output_schema_json), "/processes/" + item.id + "/conduction/" + value.upstream_effector_id)
            if not projected.is_object():
                raise Error("correlation.advance.invalid_output at /processes/" + item.id + "/conduction/" + value.upstream_effector_id + ": expected JSON object")
            conduction[value.upstream_effector_id] = projected^
        else:
            if not output.is_object():
                var wrapped = Object(capacity=1)
                wrapped["error"] = output.copy()
                conduction[value.upstream_effector_id] = Value(wrapped^)
            else:
                conduction[value.upstream_effector_id] = output^
    merged["conduction"] = Value(conduction^)
    var metadata = Value(parse_string=item.metadata_json)
    var marker_regulation = Object(capacity=0)
    var has_marker_regulation = False
    if metadata.is_object() and "regulation" in metadata.object():
        var regulation = metadata.object()["regulation"].copy()
        if regulation.is_object() and len(regulation.object()) > 0:
            marker_regulation = regulation.object().copy()
            has_marker_regulation = True
    var propagated_regulation = Object(capacity=4)
    for value in values:
        var upstream_index = -1
        for candidate_index in range(len(states)):
            if states[candidate_index].effector_id == value.upstream_effector_id:
                upstream_index = candidate_index
                break
        if upstream_index < 0: continue
        var upstream_input = Value(parse_string=states[upstream_index].input_json)
        if upstream_input.is_object() and "regulation" in upstream_input.object():
            var upstream_regulation = upstream_input.object()["regulation"].copy()
            if upstream_regulation.is_object():
                for pair in upstream_regulation.object().items(): propagated_regulation[pair.key] = pair.value.copy()
    if has_marker_regulation:
        for pair in marker_regulation.items(): propagated_regulation[pair.key] = pair.value.copy()
    if len(propagated_regulation) > 0: merged["regulation"] = Value(propagated_regulation^)
    if metadata.is_object() and "accumulate_upstream_reactions" in metadata.object() and metadata.object()["accumulate_upstream_reactions"].bool():
        var allowed = List[String]()
        if "accepted_reaction_kinds" in metadata.object():
            var accepted = metadata.object()["accepted_reaction_kinds"].copy()
            if accepted.is_array():
                for kind in accepted.array():
                    if kind.is_string(): allowed.append(kind.string())
        var accumulated = Array(capacity=len(values))
        var ancestors = _ancestor_effectors(plan, item.effector_id)
        for ancestor_id in ancestors:
            var ancestor_index = -1
            for candidate_index in range(len(states)):
                if states[candidate_index].effector_id == ancestor_id:
                    ancestor_index = candidate_index
                    break
            if ancestor_index < 0: continue
            var selected = _reaction_list(Value(parse_string=states[ancestor_index].output_json), allowed)
            var parsed = Value(parse_string=selected)
            if parsed.is_array():
                for reaction in parsed.array(): accumulated.append(reaction.copy())
        merged["upstream_reactions"] = Value(accumulated^)
    return canonical_json_text(to_string(merged^))

def _reaction_marker(unavailable: Bool = False) -> CorrelationReactionMarker:
    if unavailable:
        return CorrelationReactionMarker(code="correlation.reaction.unavailable", process_id="", replay_safe=True, message="native reaction API is unavailable; no reaction effect was emitted")
    return CorrelationReactionMarker(code="", process_id="", replay_safe=True, message="no reaction effect requested")

def _reaction_requested(plan: CorrelationInstantiationPlan) raises -> Bool:
    for item in plan.processes:
        var metadata = Value(parse_string=item.metadata_json)
        if metadata.is_object() and "accumulate_upstream_reactions" in metadata.object():
            if metadata.object()["accumulate_upstream_reactions"].is_bool() and metadata.object()["accumulate_upstream_reactions"].bool():
                return True
    return False

def _wait_marker_json(diagnostic: CorrelationWaitDiagnostic) raises -> String:
    var ids = Array(capacity=len(diagnostic.blocked_process_ids))
    for item in diagnostic.blocked_process_ids: ids.append(Value(item))
    var root = Object(capacity=4)
    root["blocked_process_ids"] = Value(ids^)
    root["deadlocked"] = Value(diagnostic.deadlocked)
    root["reason"] = Value(diagnostic.reason)
    root["code"] = Value(diagnostic.code)
    return canonical_json_text(to_string(Value(root^)))

def _metadata_with_wait_marker(metadata: String, marker: String) raises -> String:
    var parsed = Value(parse_string=metadata)
    if not parsed.is_object(): raise Error("correlation.advance.invalid_metadata: expected JSON object")
    var root = parsed.object().copy()
    root["__correlation_wait_diagnostic"] = Value(parse_string=marker)
    return canonical_json_text(to_string(root^))

def _metadata_without_wait_marker(metadata: String) raises -> String:
    var parsed = Value(parse_string=metadata)
    if not parsed.is_object(): raise Error("correlation.advance.invalid_metadata: expected JSON object")
    var root = Object(capacity=len(parsed.object()))
    for pair in parsed.object().items():
        if pair.key != "__correlation_wait_diagnostic": root[pair.key] = pair.value.copy()
    return canonical_json_text(to_string(root^))

def _persist_wait_diagnostic(mut journal: NativeJournal, plan: CorrelationInstantiationPlan, diagnostic: CorrelationWaitDiagnostic) raises:
    """Persist one canonical wait marker, clearing stale markers atomically."""
    var marker = ""
    var holder = ""
    if diagnostic.code != "" and len(diagnostic.blocked_process_ids) > 0:
        try:
            marker = _wait_marker_json(diagnostic)
        except err:
            raise Error(String(SQLiteError(code=1, message="correlation advance: invalid wait diagnostic")))
        holder = diagnostic.blocked_process_ids[0]
    var rows = journal.list_processes(plan.run_id)
    journal.db.begin()
    try:
        for row in rows:
            var parsed = Value(parse_string=row.metadata)
            if not parsed.is_object(): raise Error("correlation.advance.invalid_metadata: expected JSON object")
            var has_marker = "__correlation_wait_diagnostic" in parsed.object()
            var should_hold = marker != "" and row.id == holder and row.status == "pending"
            if should_hold:
                var existing = ""
                if has_marker: existing = canonical_json_text(to_string(parsed.object()["__correlation_wait_diagnostic"].copy()))
                if existing == canonical_json_text(marker): continue
                var updated = _metadata_with_wait_marker(row.metadata, marker)
                var stmt = journal.db.query("UPDATE processes SET metadata=?,updated_at=? WHERE run_id=? AND id=?")
                stmt.bind_text(1, updated); stmt.bind_text(2, "correlation.advance"); stmt.bind_text(3, plan.run_id); stmt.bind_text(4, row.id); _ = stmt.step()
            elif has_marker:
                var updated = _metadata_without_wait_marker(row.metadata)
                var stmt = journal.db.query("UPDATE processes SET metadata=?,updated_at=? WHERE run_id=? AND id=?")
                stmt.bind_text(1, updated); stmt.bind_text(2, "correlation.advance"); stmt.bind_text(3, plan.run_id); stmt.bind_text(4, row.id); _ = stmt.step()
        journal.db.commit()
    except err:
        journal.db.rollback()
        raise Error(String(SQLiteError(code=1, message="correlation advance: wait diagnostic transaction failed")))

def _wait_diagnostic(computed: CorrelationAdvancePlan) -> CorrelationWaitDiagnostic:
    if computed.wait_diagnostic.code != "": return computed.wait_diagnostic.copy()
    if len(computed.blocked) == 0: return CorrelationWaitDiagnostic(List[String](), False, "", "")
    var ids = List[String]()
    for item in computed.blocked: ids.append(item.process_id)
    return CorrelationWaitDiagnostic(ids^, False, "wait_graph_unavailable", "wait_graph_unavailable")

def advance_correlation(mut journal: NativeJournal, plan: CorrelationInstantiationPlan) raises -> CorrelationAdvanceResult:
    """Reconcile durable correlation rows to a deterministic fixed point."""
    _validate(plan)
    var initial = journal.list_processes(plan.run_id)
    var replayed = plan.replayed
    for item in plan.processes:
        var durable_index = _find_row(initial, item.id)
        if durable_index < 0:
            raise Error("correlation.advance.missing_plan at /processes/" + item.id + ": durable process row is missing")
        if not _same_durable_row(initial[durable_index], item):
            raise Error("correlation.advance.duplicate_conflict at /processes/" + item.id + ": durable row conflicts with plan")
    var all_promoted = List[CorrelationProcessPlan]()
    var last_conduction = List[CorrelationConductionValue]()
    var last_blocked = List[CorrelationBlocked]()
    var last_diagnostic = CorrelationWaitDiagnostic(List[String](), False, "", "")
    var changed = True
    var rounds = 0
    while changed and rounds <= len(plan.processes):
        changed = False
        rounds += 1
        var rows = journal.list_processes(plan.run_id)
        var states = _states(plan, rows)
        var computed = advance_correlation_states(_path_for_plan(plan), states)
        last_conduction = computed.conduction.copy()
        last_blocked = computed.blocked.copy()
        last_diagnostic = _wait_diagnostic(computed)
        var children = List[CorrelationChildTransition]()
        var child_plans = List[CorrelationProcessPlan]()
        for item in computed.readied:
            var row_index = _find_row(rows, item.id)
            if row_index >= 0 and rows[row_index].status == "pending":
                var plan_index = _find_plan(plan, item.id)
                if plan_index < 0:
                    raise Error("correlation.advance.missing_plan at /processes/" + item.id + ": canonical process plan is missing")
                var canonical_item = plan.processes[plan_index].copy()
                var merged_input = _merged_input(canonical_item, plan, states)
                children.append(CorrelationChildTransition(process_id=item.id, target_status="ready", input_json=merged_input, error_json="{}"))
                child_plans.append(canonical_item^)
        if len(children) > 0:
            _ = journal.apply_correlation_children(plan.run_id, children)
            for item in child_plans: all_promoted.append(item.copy())
            changed = True
        _persist_wait_diagnostic(journal, plan, last_diagnostic)
    if changed:
        raise Error("correlation.advance.nonconvergent at /processes: fixed-point iteration exceeded process count")
    var refreshed = journal.list_processes(plan.run_id)
    var no_op = len(all_promoted) == 0
    return CorrelationAdvanceResult(rows=_ordered_rows(plan, refreshed), readied=all_promoted^, conduction=last_conduction^, blocked=last_blocked^, cancelled=List[CorrelationBlocked](), readiness=_readiness(refreshed, plan), wait_diagnostic=last_diagnostic^, reaction=_reaction_marker(_reaction_requested(plan)), replayed=replayed or no_op)


def _path_for_plan(plan: CorrelationInstantiationPlan) raises -> CorrelationPathSpec:
    var effectors = List[CorrelationEffectorSpec]()
    var accumulate_upstream_reactions = False
    for item in plan.processes:
        var metadata = Value(parse_string=item.metadata_json)
        if metadata.is_object():
            if "accumulate_upstream_reactions" in metadata.object() and metadata.object()["accumulate_upstream_reactions"].is_bool():
                accumulate_upstream_reactions = accumulate_upstream_reactions or metadata.object()["accumulate_upstream_reactions"].bool()
        effectors.append(CorrelationEffectorSpec.create(item.effector_id, "", item.conduction.copy()))
    return CorrelationPathSpec(plan.correlation_path_id, effectors^, accumulate_upstream_reactions)
