"""Durable correlation-plan scheduling and readiness reconciliation.

The journal schedule primitive supplies idempotent durable insertion for each
process.  This module then uses a local transaction to initialize dependents
and move them to pending.  If scheduling is interrupted, replay safely fills
the remaining rows before applying the pending projection.
NativeJournal currently exposes no pending-status transition, so that local
SQL update remains intentionally scoped to this integration module.
"""

from std.collections import List
from emberjson import Array, Object, Value, to_string

from fala.correlation import CorrelationInstantiationPlan, CorrelationProcessPlan
from fala.correlation import Readiness
from fala.correlation_advance import _ancestor_effectors, _reaction_list, _validate_projected_schema
from fala.json import canonical_json_text
from fala.journal import NativeJournal, ProcessRow
from fala.adapters import adapter_spec_from_json
from fala.sqlite import SQLiteError


struct CorrelationPersistenceError(Copyable, Movable):
    """Stable diagnostics for invalid plans and replay conflicts."""

    var code: String
    var path: String
    var message: String

    def __init__(out self, code: String, path: String, message: String):
        self.code = code
        self.path = path
        self.message = message

    def __str__(self) -> String:
        return self.code + " at " + self.path + ": " + self.message


@fieldwise_init
struct CorrelationPersistenceResult(Copyable, Movable):
    """Rows in declaration order and the durable readiness projection."""

    var rows: List[ProcessRow]
    var readiness: Readiness
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


def validate_correlation_persistence_plan(plan: CorrelationInstantiationPlan) -> CorrelationPersistenceError:
    """Return stable diagnostics without requiring exception interop."""
    if plan.run_id == "":
        return CorrelationPersistenceError("correlation.persistence.invalid_plan", "/run_id", "run id must not be empty")
    if plan.correlation_path_id == "":
        return CorrelationPersistenceError("correlation.persistence.invalid_plan", "/correlation_path_id", "path id must not be empty")
    var ids = List[String]()
    var effectors = List[String]()
    for index in range(len(plan.processes)):
        var item = plan.processes[index].copy()
        var path = "/processes/" + String(index)
        if item.run_id != plan.run_id:
            return CorrelationPersistenceError("correlation.persistence.unknown_run", path + "/run_id", "process run differs from plan")
        if item.id == "" or item.effector_id == "":
            return CorrelationPersistenceError("correlation.persistence.invalid_plan", path, "process and effector ids must not be empty")
        if not item.id.startswith(plan.correlation_path_id + ":"):
            return CorrelationPersistenceError("correlation.persistence.unknown_path", path + "/id", "process is outside correlation path")
        if _contains(ids, item.id) or _contains(effectors, item.effector_id):
            return CorrelationPersistenceError("correlation.persistence.duplicate_conflict", path, "duplicate process or effector id")
        if item.max_attempts < 1:
            return CorrelationPersistenceError("correlation.persistence.invalid_plan", path + "/max_attempts", "must be positive")
        ids.append(item.id)
        effectors.append(item.effector_id)
    for index in range(len(plan.processes)):
        var item = plan.processes[index].copy()
        for upstream_index in range(len(item.conduction)):
            var upstream = item.conduction[upstream_index]
            if upstream == "":
                return CorrelationPersistenceError("correlation.persistence.unknown_upstream", "/processes/" + String(index) + "/conduction", "conduction upstream must not be empty")
            if upstream == item.effector_id:
                return CorrelationPersistenceError("correlation.persistence.invalid_plan", "/processes/" + String(index) + "/conduction", "effector cannot depend on itself")
            for prior_index in range(upstream_index):
                if item.conduction[prior_index] == upstream:
                    return CorrelationPersistenceError("correlation.persistence.duplicate_conflict", "/processes/" + String(index) + "/conduction", "duplicate conduction reference: " + upstream)
            if not _contains(effectors, upstream):
                return CorrelationPersistenceError("correlation.persistence.unknown_upstream", "/processes/" + String(index) + "/conduction", "unknown upstream " + upstream)
    return CorrelationPersistenceError("", "", "")


def _validate_plan(plan: CorrelationInstantiationPlan) raises:
    var diagnostic = validate_correlation_persistence_plan(plan)
    if diagnostic.code != "":
        raise Error(diagnostic.__str__())
    for item in plan.processes:
        _ = _authored_json(item.input_json, "/processes/" + item.id + "/input_json", True)
        _ = _metadata_for_item(item)

def _canonical_object(text: String, path: String) raises -> String:
    var parsed: Value
    try:
        parsed = Value(parse_string=text)
    except err:
        raise Error("correlation.persistence.invalid_json at " + path + ": malformed JSON")
    if not parsed.is_object():
        raise Error("correlation.persistence.invalid_json at " + path + ": expected JSON object")
    return canonical_json_text(to_string(parsed^))

def _authored_json(text: String, path: String, reject_injected: Bool = False) raises -> String:
    var parsed: Value
    try:
        parsed = Value(parse_string=text)
    except err:
        raise Error("correlation.persistence.invalid_input at " + path + ": malformed JSON")
    if not parsed.is_object():
        raise Error("correlation.persistence.invalid_input at " + path + ": expected JSON object")
    var source = parsed.object().copy()
    var authored = Object(capacity=len(source))
    for pair in source.items():
        if reject_injected and (pair.key == "adapter" or pair.key == "config" or pair.key == "conduction" or pair.key == "upstream_reactions" or pair.key == "regulation"):
            raise Error("correlation.persistence.invalid_input at " + path + "/" + pair.key + ": reserved key")
        authored[pair.key] = pair.value.copy()
    return canonical_json_text(to_string(authored^))
def _validate_output_schema(text: String, path: String) raises:
    """Validate the structural JSON Schema subset accepted by native scheduling."""
    var parsed: Value
    try:
        parsed = Value(parse_string=text)
    except err:
        raise Error("correlation.persistence.invalid_output_schema at " + path + ": malformed JSON")
    if not parsed.is_object():
        raise Error("correlation.persistence.invalid_output_schema at " + path + ": expected JSON object")
    var schema = parsed.object().copy()
    var allowed = List[String]()
    for keyword in ["$schema", "$id", "$ref", "$defs", "definitions", "$anchor", "$dynamicRef", "$comment", "title", "description", "type", "enum", "const", "multipleOf", "maximum", "exclusiveMaximum", "minimum", "exclusiveMinimum", "maxLength", "minLength", "pattern", "format", "contentEncoding", "contentMediaType", "maxItems", "minItems", "uniqueItems", "maxContains", "minContains", "maxProperties", "minProperties", "required", "dependentRequired", "dependentSchemas", "properties", "patternProperties", "additionalProperties", "propertyNames", "unevaluatedProperties", "items", "prefixItems", "contains", "additionalItems", "unevaluatedItems", "allOf", "anyOf", "oneOf", "not"]:
        allowed.append(keyword)
    for pair in schema.items():
        if not _contains(allowed, pair.key):
            raise Error("correlation.persistence.invalid_output_schema at " + path + ": unknown JSON Schema keyword " + pair.key)
    if "type" in schema:
        var type_value = schema["type"].copy()
        if type_value.is_string():
            var kind = type_value.string()
            if kind != "null" and kind != "boolean" and kind != "object" and kind != "array" and kind != "number" and kind != "integer" and kind != "string":
                raise Error("correlation.persistence.invalid_output_schema at " + path + "/type: unsupported JSON Schema type")
        elif type_value.is_array():
            if len(type_value.array()) == 0:
                raise Error("correlation.persistence.invalid_output_schema at " + path + "/type: type array must not be empty")
            var seen_types = List[String]()
            for item in type_value.array():
                if not item.is_string():
                    raise Error("correlation.persistence.invalid_output_schema at " + path + "/type: expected string or array of strings")
                var kind = item.string()
                if _contains(seen_types, kind):
                    raise Error("correlation.persistence.invalid_output_schema at " + path + "/type: duplicate JSON Schema type")
                seen_types.append(kind)
                if kind != "null" and kind != "boolean" and kind != "object" and kind != "array" and kind != "number" and kind != "integer" and kind != "string":
                    raise Error("correlation.persistence.invalid_output_schema at " + path + "/type: unsupported JSON Schema type")
        else:
            raise Error("correlation.persistence.invalid_output_schema at " + path + "/type: expected string or array of strings")
    if "properties" in schema:
        var properties = schema["properties"].copy()
        if not properties.is_object():
            raise Error("correlation.persistence.invalid_output_schema at " + path + "/properties: expected object")
        for pair in properties.object().items():
            _validate_output_schema(to_string(pair.value.copy()), path + "/properties/" + pair.key)
    if "required" in schema:
        var required = schema["required"].copy()
        if not required.is_array():
            raise Error("correlation.persistence.invalid_output_schema at " + path + "/required: expected array of strings")
        for item in required.array():
            if not item.is_string():
                raise Error("correlation.persistence.invalid_output_schema at " + path + "/required: expected array of strings")
    if "items" in schema:
        var items = schema["items"].copy()
        if not items.is_object():
            raise Error("correlation.persistence.invalid_output_schema at " + path + "/items: expected schema object")
        _validate_output_schema(to_string(items^), path + "/items")

def _metadata_for_item(item: CorrelationProcessPlan) raises -> String:
    var parsed = Value(parse_string=item.metadata_json)
    if not parsed.is_object():
        raise Error("correlation.persistence.invalid_metadata at /processes/" + item.id + "/metadata: expected JSON object")
    var source = parsed.object().copy()
    var metadata = Object(capacity=len(source) + 4)
    for pair in source.items():
        if pair.key == "__correlation_config" or pair.key == "__correlation_output_schema" or pair.key == "__correlation_conduction" or pair.key == "__correlation_timeout_seconds" or pair.key == "__adapter_binding" or pair.key == "__correlation_wait_diagnostic":
            raise Error("correlation.persistence.invalid_metadata at /processes/" + item.id + "/metadata/" + pair.key + ": reserved key")
        metadata[pair.key] = pair.value.copy()
    var config = Value(parse_string=item.config_json)
    if not config.is_object():
        raise Error("correlation.persistence.invalid_config at /processes/" + item.id + "/config_json: expected JSON object")
    _validate_output_schema(item.output_schema_json, "/processes/" + item.id + "/output_schema_json")
    var output_schema = Value(parse_string=item.output_schema_json)
    if not output_schema.is_object():
        raise Error("correlation.persistence.invalid_output_schema at /processes/" + item.id + "/output_schema_json: expected JSON object")
    var conduction = Array(capacity=len(item.conduction))
    for upstream in item.conduction:
        conduction.append(Value(upstream))
    metadata["__correlation_config"] = config^
    metadata["__correlation_output_schema"] = output_schema^
    metadata["__correlation_conduction"] = Value(conduction^)
    metadata["__correlation_timeout_seconds"] = Value(item.timeout_seconds)
    return canonical_json_text(to_string(metadata^))

def _same_authored_input(row: ProcessRow, item: CorrelationProcessPlan) raises -> Bool:
    var expected = _authored_json(item.input_json, "/processes/" + item.id + "/input_json", True)
    var persisted = Value(parse_string=row.input_json)
    if not persisted.is_object():
        raise Error("correlation.persistence.invalid_input at /processes/" + item.id + "/input_json: expected JSON object")
    var source = persisted.object().copy()
    var authored = Object(capacity=len(source))
    for pair in source.items():
        if pair.key != "conduction" and pair.key != "upstream_reactions" and pair.key != "regulation": authored[pair.key] = pair.value.copy()
    return expected == canonical_json_text(to_string(authored^))

def _same_metadata(row: ProcessRow, item: CorrelationProcessPlan) raises -> Bool:
    var expected = _canonical_object(item.metadata_json, "/processes/" + item.id + "/metadata")
    var persisted = Value(parse_string=row.metadata)
    if not persisted.is_object():
        raise Error("correlation.persistence.invalid_metadata at /processes/" + item.id + "/metadata: expected JSON object")
    var source = persisted.object().copy()
    var metadata = Object(capacity=len(source))
    var config = ""
    var output_schema = ""
    var conduction = ""
    var timeout = ""
    for pair in source.items():
        if pair.key == "__adapter_binding":
            try:
                var binding = adapter_spec_from_json(to_string(pair.value.copy()))
                _ = binding
            except err:
                raise Error("correlation.persistence.invalid_metadata at /processes/" + item.id + "/metadata/__adapter_binding: invalid adapter binding")
        elif pair.key == "__correlation_wait_diagnostic":
            pass
        elif pair.key == "__correlation_config": config = canonical_json_text(to_string(pair.value.copy()))
        elif pair.key == "__correlation_output_schema": output_schema = canonical_json_text(to_string(pair.value.copy()))
        elif pair.key == "__correlation_conduction": conduction = canonical_json_text(to_string(pair.value.copy()))
        elif pair.key == "__correlation_timeout_seconds": timeout = canonical_json_text(to_string(pair.value.copy()))
        else: metadata[pair.key] = pair.value.copy()
    var expected_config = _canonical_object(item.config_json, "/processes/" + item.id + "/config_json")
    var expected_output_schema = _canonical_object(item.output_schema_json, "/processes/" + item.id + "/output_schema_json")
    var expected_metadata = expected == canonical_json_text(to_string(metadata^))
    var expected_conduction = Array(capacity=len(item.conduction))
    for upstream in item.conduction:
        expected_conduction.append(Value(upstream))
    var expected_conduction_json = canonical_json_text(to_string(Value(expected_conduction^)))
    var expected_timeout_json = canonical_json_text(to_string(Value(item.timeout_seconds)))
    return expected_metadata and config == expected_config and output_schema == expected_output_schema and conduction == expected_conduction_json and timeout == expected_timeout_json

def _same_row(row: ProcessRow, item: CorrelationProcessPlan) raises -> Bool:
    var expected_output_schema = _canonical_object(item.output_schema_json, "/processes/" + item.id + "/output_schema_json")
    return (
        row.run_id == item.run_id
        and row.id == item.id
        and row.process_type == "correlation.effector"
        and row.priority == item.priority
        and row.max_attempts == item.max_attempts
        and row.output_schema_json == expected_output_schema
        and _same_authored_input(row, item)
        and _same_metadata(row, item)
    )

def _project_output(output: Value, output_schema_json: String) raises -> Value:
    """Strip adapter envelopes and project domain output for durable conduction."""
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

def _initial_input(item: CorrelationProcessPlan) raises -> String:
    """Persist authored input plus the path marker's root regulation policy."""
    var authored = Value(parse_string=item.input_json)
    if not authored.is_object():
        raise Error("correlation.persistence.invalid_input at /processes/" + item.id + "/input_json: expected JSON object")
    var merged = Object(capacity=len(authored.object()) + 1)
    for pair in authored.object().items(): merged[pair.key] = pair.value.copy()
    var metadata = Value(parse_string=item.metadata_json)
    if metadata.is_object() and "regulation" in metadata.object():
        var regulation = metadata.object()["regulation"].copy()
        if regulation.is_object() and len(regulation.object()) > 0: merged["regulation"] = regulation^
    return canonical_json_text(to_string(merged^))

def _validated_upstream_output(row: ProcessRow, upstream_item: CorrelationProcessPlan, path: String) raises -> Value:
    """Validate a succeeded durable output before it can feed conduction."""
    var schema_path = "/processes/" + row.id + "/output_schema_json"
    _validate_output_schema(row.output_schema_json, schema_path)
    var durable_schema = _canonical_object(row.output_schema_json, schema_path)
    var expected_schema = _canonical_object(upstream_item.output_schema_json, schema_path)
    if durable_schema != expected_schema:
        raise Error("correlation.persistence.invalid_output_schema at " + schema_path + ": durable output schema differs from plan")
    var output: Value
    try:
        output = Value(parse_string=row.output_json)
    except err:
        raise Error("correlation.persistence.invalid_output at " + path + ": malformed JSON")
    if not output.is_object():
        raise Error("correlation.persistence.invalid_output at " + path + ": expected JSON object")
    var projected: Value
    try:
        projected = _project_output(output, expected_schema)
        _validate_projected_schema(projected, Value(parse_string=expected_schema), path)
    except err:
        raise Error("correlation.persistence.invalid_output at " + path + ": invalid projected output")
    if not projected.is_object():
        raise Error("correlation.persistence.invalid_output at " + path + ": expected JSON object")
    return projected^


def _merged_input(item: CorrelationProcessPlan, plan: CorrelationInstantiationPlan, rows: List[ProcessRow]) raises -> String:
    """Preserve authored keys and inject projected durable upstream outputs."""
    var authored = Value(parse_string=item.input_json)
    if not authored.is_object():
        raise Error("correlation.persistence.invalid_input at /processes/" + item.id + "/input_json: expected JSON object")
    var source = authored.object().copy()
    var merged = Object(capacity=len(source) + 3)
    for pair in source.items(): merged[pair.key] = pair.value.copy()
    var conduction = Object(capacity=len(item.conduction))
    var upstream_ids = List[String]()
    for upstream in item.conduction:
        var upstream_id = ""
        var upstream_plan_index = -1
        for candidate_index in range(len(plan.processes)):
            if plan.processes[candidate_index].effector_id == upstream:
                upstream_id = plan.processes[candidate_index].id
                upstream_plan_index = candidate_index
                break
        if upstream_id == "" or upstream_plan_index < 0:
            raise Error("correlation.persistence.unknown_upstream at /processes/" + item.id + "/conduction: unknown upstream " + upstream)
        var upstream_index = _find_row(rows, upstream_id)
        if upstream_index < 0:
            raise Error("correlation.persistence.unknown_path at /processes/" + upstream_id + ": durable process row is missing")
        var projected = _validated_upstream_output(rows[upstream_index], plan.processes[upstream_plan_index], "/processes/" + item.id + "/conduction/" + upstream)
        conduction[upstream] = projected^
        upstream_ids.append(upstream_id)
    merged["conduction"] = Value(conduction^)
    var propagated_regulation = Object(capacity=4)
    for upstream_id in upstream_ids:
        var upstream_index = _find_row(rows, upstream_id)
        if upstream_index < 0: continue
        var upstream_input = Value(parse_string=rows[upstream_index].input_json)
        if upstream_input.is_object() and "regulation" in upstream_input.object():
            var regulation = upstream_input.object()["regulation"].copy()
            if regulation.is_object():
                for pair in regulation.object().items(): propagated_regulation[pair.key] = pair.value.copy()
    var metadata = Value(parse_string=item.metadata_json)
    if metadata.is_object() and "regulation" in metadata.object():
        var marker_regulation = metadata.object()["regulation"].copy()
        if marker_regulation.is_object():
            for pair in marker_regulation.object().items(): propagated_regulation[pair.key] = pair.value.copy()
    if len(propagated_regulation) > 0: merged["regulation"] = Value(propagated_regulation^)
    if metadata.is_object() and "accumulate_upstream_reactions" in metadata.object() and metadata.object()["accumulate_upstream_reactions"].is_bool() and metadata.object()["accumulate_upstream_reactions"].bool():
        var allowed = List[String]()
        if "accepted_reaction_kinds" in metadata.object():
            var accepted = metadata.object()["accepted_reaction_kinds"].copy()
            if accepted.is_array():
                for kind in accepted.array():
                    if kind.is_string(): allowed.append(kind.string())
        var accumulated = Array(capacity=len(upstream_ids))
        var ancestors = _ancestor_effectors(plan, item.effector_id)
        for ancestor_id in ancestors:
            var ancestor_id_row = ""
            for candidate in plan.processes:
                if candidate.effector_id == ancestor_id:
                    ancestor_id_row = candidate.id
                    break
            if ancestor_id_row == "": continue
            var ancestor_index = _find_row(rows, ancestor_id_row)
            if ancestor_index < 0: continue
            var selected = _reaction_list(Value(parse_string=rows[ancestor_index].output_json), allowed)
            var parsed = Value(parse_string=selected)
            if parsed.is_array():
                for reaction in parsed.array(): accumulated.append(reaction.copy())
        merged["upstream_reactions"] = Value(accumulated^)
    return canonical_json_text(to_string(merged^))

def _persist_new_rows(mut journal: NativeJournal, plan: CorrelationInstantiationPlan, created_at: String, created: List[String]) raises:
    """Schedule rows through NativeJournal, then atomically initialize dependents."""
    for item in plan.processes:
        if not _contains(created, item.id):
            continue
        var authored = _initial_input(item)
        var metadata = _metadata_for_item(item)
        var schema = _canonical_object(item.output_schema_json, "/processes/" + item.id + "/output_schema_json")
        var scheduled = journal.schedule_process(
            plan.run_id,
            item.id,
            "correlation.effector",
            created_at,
            input_json=authored,
            metadata=metadata,
            priority=item.priority,
            max_attempts=item.max_attempts,
            output_schema_json=schema,
        )
        _ = scheduled
    journal.db.begin()
    try:
        for item in plan.processes:
            if len(item.conduction) == 0 or not _contains(created, item.id):
                continue
            var pending = journal.db.query("UPDATE processes SET status='pending' WHERE run_id=? AND id=? AND status='ready'")
            pending.bind_text(1, plan.run_id)
            pending.bind_text(2, item.id)
            _ = pending.step()
        journal.db.commit()
    except err:
        journal.db.rollback()
        raise SQLiteError(code=1, message="correlation persistence: pending initialization failed")


def _readiness(mut journal: NativeJournal, plan: CorrelationInstantiationPlan) raises -> Readiness:
    var rows = journal.list_processes(plan.run_id)
    var ready = List[String]()
    var blocked = List[String]()
    for item in plan.processes:
        var row_index = _find_row(rows, item.id)
        if row_index < 0:
            raise Error("correlation.persistence.unknown_path at /processes/" + item.id + ": durable process row is missing")
        var row = rows[row_index].copy()
        if row.status == "ready":
            ready.append(item.effector_id)
        elif row.status == "pending":
            blocked.append(item.effector_id)
    return Readiness(ready=ready^, blocked=blocked^)

def _readiness_from_rows(plan: CorrelationInstantiationPlan, rows: List[ProcessRow]) raises -> Readiness:
    var ready = List[String]()
    var blocked = List[String]()
    for item in plan.processes:
        var row_index = _find_row(rows, item.id)
        if row_index < 0:
            raise Error("correlation.persistence.unknown_path at /processes/" + item.id + ": durable process row is missing")
        if rows[row_index].status == "ready":
            ready.append(item.effector_id)
        elif rows[row_index].status == "pending":
            blocked.append(item.effector_id)
    return Readiness(ready=ready^, blocked=blocked^)


def refresh_correlation_readiness(mut journal: NativeJournal, plan: CorrelationInstantiationPlan) raises -> CorrelationPersistenceResult:
    """Promote pending dependents only after durable conduction is present."""
    _validate_plan(plan)
    var rows = journal.list_processes(plan.run_id)
    var merged_inputs = List[String]()
    var candidates = List[String]()
    for item in plan.processes:
        var row_index = _find_row(rows, item.id)
        if row_index < 0:
            raise Error("correlation.persistence.unknown_path at /processes/" + item.id + ": durable process row is missing")
        if rows[row_index].status != "pending":
            continue
        var all_succeeded = True
        for upstream in item.conduction:
            var upstream_id = ""
            for candidate in plan.processes:
                if candidate.effector_id == upstream:
                    upstream_id = candidate.id
                    break
            if upstream_id == "":
                raise Error("correlation.persistence.unknown_path at /processes/" + item.id + "/conduction: unknown upstream " + upstream)
            var upstream_index = _find_row(rows, upstream_id)
            if upstream_index < 0:
                raise Error("correlation.persistence.unknown_path at /processes/" + upstream_id + ": durable process row is missing")
            if rows[upstream_index].status != "succeeded":
                all_succeeded = False
                break
        if all_succeeded:
            candidates.append(item.id)
            merged_inputs.append(_merged_input(item, plan, rows))
    journal.db.begin()
    try:
        for index in range(len(candidates)):
            var stmt = journal.db.query("UPDATE processes SET input_json=?,status='ready' WHERE run_id=? AND id=? AND status='pending'")
            stmt.bind_text(1, merged_inputs[index])
            stmt.bind_text(2, plan.run_id)
            stmt.bind_text(3, candidates[index])
            _ = stmt.step()
        journal.db.commit()
    except err:
        journal.db.rollback()
        raise SQLiteError(code=1, message="correlation persistence: readiness reconciliation failed")
    rows = journal.list_processes(plan.run_id)
    var readiness = _readiness_from_rows(plan, rows)
    return CorrelationPersistenceResult(rows=_ordered_rows(plan, rows), readiness=readiness^, replayed=plan.replayed)


def _ordered_rows(plan: CorrelationInstantiationPlan, rows: List[ProcessRow]) -> List[ProcessRow]:
    var result = List[ProcessRow]()
    for item in plan.processes:
        var index = _find_row(rows, item.id)
        if index >= 0:
            result.append(rows[index].copy())
    return result^
def persist_correlation_plan(mut journal: NativeJournal, plan: CorrelationInstantiationPlan, created_at: String) raises -> CorrelationPersistenceResult:
    """Schedule a plan exactly once, then persist initial and derived readiness."""
    _validate_plan(plan)
    var existing = journal.list_processes(plan.run_id)
    var created = List[String]()
    var all_existing = True
    for item in plan.processes:
        var index = _find_row(existing, item.id)
        if index >= 0:
            if not _same_row(existing[index], item):
                raise Error("correlation.persistence.duplicate_conflict at /processes/" + item.id + ": existing durable row conflicts with plan")
            continue
        all_existing = False
        created.append(item.id)
    if len(created) > 0:
        _persist_new_rows(journal, plan, created_at, created)
    var updated = plan.copy()
    updated.replayed = all_existing
    return refresh_correlation_readiness(journal, updated)
