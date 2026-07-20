"""Pure correlation-path graph utilities for the native Fala runtime."""

from std.collections import List
from std.collections import Dict
from emberjson import Object, Value, to_string
from fala.json import canonical_json_text


struct EffectorNode(Copyable, Movable):
    """One effector and its immediate upstream conduction ids."""

    var id: String
    var conduction: List[String]

    def __init__(out self, id: String, conduction: List[String]):
        self.id = id
        self.conduction = conduction.copy()

    @staticmethod
    def root(id: String) -> EffectorNode:
        return EffectorNode(id, List[String]())


struct ConductionEdge(Copyable, Movable):
    """A directed edge from upstream to downstream effector."""

    var upstream: String
    var downstream: String

    def __init__(out self, upstream: String, downstream: String):
        self.upstream = upstream
        self.downstream = downstream

    def __eq__(self, other: Self) -> Bool:
        return self.upstream == other.upstream and self.downstream == other.downstream


struct Readiness(Copyable, Movable):
    """Effectors ready now and effectors blocked by dependencies."""

    var ready: List[String]
    var blocked: List[String]

    def __init__(out self, ready: List[String], blocked: List[String]):
        self.ready = ready.copy()
        self.blocked = blocked.copy()


struct CorrelationGraph(Copyable, Movable):
    """Validated graph and its feedback-cycle policy."""

    var nodes: List[EffectorNode]
    var edges: List[ConductionEdge]
    var allow_feedback_cycles: Bool

    def __init__(
        out self,
        nodes: List[EffectorNode],
        allow_feedback_cycles: Bool = False,
    ) raises:
        validate_graph(nodes, allow_feedback_cycles)
        self.nodes = nodes.copy()
        self.edges = conduction_edges(nodes)
        self.allow_feedback_cycles = allow_feedback_cycles

    def topological_order(self) raises -> List[String]:
        return _topological_order(self.nodes, self.allow_feedback_cycles)

    def readiness(self, completed: List[String], failed: List[String]) raises -> Readiness:
        return readiness(self, completed, failed)


def _contains(values: List[String], wanted: String) -> Bool:
    for value in values:
        if value == wanted:
            return True
    return False


def _insert_sorted(mut values: List[String], value: String):
    var position = 0
    while position < len(values) and values[position] < value:
        position += 1
    values.append(value)
    var index = len(values) - 1
    while index > position:
        values[index] = values[index - 1]
        index -= 1
    values[position] = value


def effector_ids(nodes: List[EffectorNode]) -> List[String]:
    """Return unique effector ids in deterministic lexical order."""
    var ids = List[String]()
    for node in nodes:
        if not _contains(ids, node.id):
            _insert_sorted(ids, node.id)
    return ids^


def conduction_edges(nodes: List[EffectorNode]) -> List[ConductionEdge]:
    """Expand conduction declarations into upstream-to-downstream edges."""
    var edges = List[ConductionEdge]()
    for node in nodes:
        for upstream in node.conduction:
            edges.append(ConductionEdge(upstream, node.id))
    return edges^


def validate_graph(
    nodes: List[EffectorNode],
    allow_feedback_cycles: Bool = False,
) raises:
    """Reject duplicate ids, bad refs, self refs, duplicate refs, and cycles."""
    var ids = List[String]()
    for node in nodes:
        if node.id == "":
            raise Error("correlation graph effector id must not be empty")
        if _contains(ids, node.id):
            raise Error("correlation graph has duplicate effector id: " + node.id)
        ids.append(node.id)

    for node in nodes:
        var seen_upstreams = List[String]()
        for upstream in node.conduction:
            if upstream == "":
                raise Error("correlation graph conduction reference must not be empty")
            if not _contains(ids, upstream):
                raise Error("correlation graph effector " + node.id + " references unknown effector: " + upstream)
            if upstream == node.id:
                raise Error("correlation graph effector cannot depend on itself: " + node.id)
            if _contains(seen_upstreams, upstream):
                raise Error("correlation graph effector " + node.id + " has duplicate conduction reference: " + upstream)
            seen_upstreams.append(upstream)

    if not allow_feedback_cycles:
        var order = _topological_order(nodes, False)
        if len(order) != len(ids):
            raise Error("correlation graph contains a cycle")


def _topological_order(
    nodes: List[EffectorNode],
    allow_feedback_cycles: Bool,
) raises -> List[String]:
    var ids = effector_ids(nodes)^
    var remaining = ids^
    var order = List[String]()
    while len(remaining) > 0:
        var selected = ""
        for candidate in remaining:
            var node_index = 0
            while node_index < len(nodes) and nodes[node_index].id != candidate:
                node_index += 1
            var ready = True
            if node_index < len(nodes):
                for upstream in nodes[node_index].conduction:
                    if not _contains(order, upstream):
                        ready = False
                        break
            if ready:
                selected = candidate
                break
        if selected == "":
            if not allow_feedback_cycles:
                raise Error("correlation graph contains a cycle")
            for candidate in remaining:
                order.append(candidate)
            remaining.clear()
            break
        order.append(selected)
        var index = 0
        while index < len(remaining) and remaining[index] != selected:
            index += 1
        if index < len(remaining):
            _ = remaining.pop(index)
    return order^


def topological_order(graph: CorrelationGraph) raises -> List[String]:
    """Return deterministic upstream-before-downstream order."""
    return _topological_order(graph.nodes, graph.allow_feedback_cycles)


def readiness(
    graph: CorrelationGraph,
    completed: List[String],
    failed: List[String],
) raises -> Readiness:
    """Calculate ready and blocked nodes from completed and failed sets."""
    var ids = effector_ids(graph.nodes)^
    for id in completed:
        if not _contains(ids, id):
            raise Error("completed set references unknown effector: " + id)
    for id in failed:
        if not _contains(ids, id):
            raise Error("failed set references unknown effector: " + id)
        if _contains(completed, id):
            raise Error("effector cannot be both completed and failed: " + id)

    var ready = List[String]()
    var blocked = List[String]()
    for id in ids:
        if _contains(completed, id) or _contains(failed, id):
            continue
        var node_index = 0
        while node_index < len(graph.nodes) and graph.nodes[node_index].id != id:
            node_index += 1
        var can_run = True
        if node_index < len(graph.nodes):
            for upstream in graph.nodes[node_index].conduction:
                if not _contains(completed, upstream):
                    can_run = False
                    break
        if can_run:
            ready.append(id)
        else:
            blocked.append(id)
    return Readiness(ready^, blocked^)
@fieldwise_init
struct CorrelationInputField(Copyable, Movable):
    """One authored input field; injected keys are rejected at the boundary."""
    var key: String
    var value_json: String


@fieldwise_init
struct CorrelationEffectorSpec(Copyable, Movable):
    """Native, persistence-neutral effector declaration used by execution plans."""
    var id: String
    var conduction: List[String]
    var capability: String
    var timeout_seconds: Float64
    var config_json: String
    var output_schema_json: String
    # Optional native propagation metadata; kept as JSON to avoid inventing a reaction schema.
    var regulation_json: String
    var accepted_reaction_kinds: List[String]

    @staticmethod
    def create(id: String, capability: String = "", var conduction: List[String] = List[String](), timeout_seconds: Float64 = 0.0, config_json: String = "{}", output_schema_json: String = "{}", regulation_json: String = "{}", var accepted_reaction_kinds: List[String] = List[String]()) raises -> CorrelationEffectorSpec:
        if id == "": raise Error("correlation effector id must not be empty")
        if timeout_seconds < 0.0: raise Error("correlation effector timeout must not be negative")
        var seen = List[String]()
        for upstream in conduction:
            if upstream == id: raise Error("correlation effector cannot depend on itself: " + id)
            if _contains(seen, upstream): raise Error("duplicate conduction reference: " + upstream)
            seen.append(upstream)
        return CorrelationEffectorSpec(id=id, conduction=conduction^, capability=capability, timeout_seconds=timeout_seconds, config_json=config_json, output_schema_json=output_schema_json, regulation_json=regulation_json, accepted_reaction_kinds=accepted_reaction_kinds^)


struct CorrelationPathSpec(Copyable, Movable):
    """Typed path declaration for callers that do not depend on models.mojo."""
    var id: String
    var effectors: List[CorrelationEffectorSpec]
    var allow_feedback_cycles: Bool
    var accumulate_upstream_reactions: Bool

    def __init__(out self, id: String, var effectors: List[CorrelationEffectorSpec], allow_feedback_cycles: Bool = False, accumulate_upstream_reactions: Bool = False) raises:
        if id == "": raise Error("correlation path id must not be empty")
        if len(effectors) == 0: raise Error("correlation path effectors must be nonempty")
        var nodes = List[EffectorNode]()
        for effector in effectors:
            nodes.append(EffectorNode(effector.id, effector.conduction.copy()))
        validate_graph(nodes, allow_feedback_cycles)
        self.id = id
        self.effectors = effectors.copy()
        self.allow_feedback_cycles = allow_feedback_cycles
        self.accumulate_upstream_reactions = accumulate_upstream_reactions


@fieldwise_init
struct CorrelationProcessPlan(Copyable, Movable):
    """Deterministic process row plan; persistence adapters can store it atomically."""
    var id: String
    var run_id: String
    var effector_id: String
    var declaration_seq: Int
    var status: String
    var priority: Int
    var max_attempts: Int
    var timeout_seconds: Float64
    var input_json: String
    var config_json: String
    var output_schema_json: String
    var conduction: List[String]
    var metadata_json: String
    var idempotency_key: String


@fieldwise_init
struct CorrelationInstantiationPlan(Copyable, Movable):
    """One deterministic process per effector, safe to replay by idempotency key."""
    var correlation_path_id: String
    var run_id: String
    var processes: List[CorrelationProcessPlan]
    var replayed: Bool


@fieldwise_init
struct CorrelationExecutionState(Copyable, Movable):
    """Small process projection consumed by pure readiness/advancement helpers."""
    var process_id: String
    var effector_id: String
    var status: String
    var attempt: Int
    var max_attempts: Int
    var output_json: String
    var input_json: String
    var reactions_json: String
    var conduction: List[String]


@fieldwise_init
struct CorrelationConductionValue(Copyable, Movable):
    """Projected domain output from one succeeded upstream effector."""
    var upstream_effector_id: String
    var output_json: String


@fieldwise_init
struct CorrelationBlocked(Copyable, Movable):
    """Diagnostic for an unresolved or permanently dead dependency."""
    var process_id: String
    var effector_id: String
    var unmet: List[String]
    var dead_upstreams: List[String]
    var reason: String


struct CorrelationWaitDiagnostic(Copyable, Movable):
    """Deterministic diagnosis persisted alongside an unresolved wait."""
    var blocked_process_ids: List[String]
    var deadlocked: Bool
    var reason: String
    var code: String

    def __init__(out self, blocked_process_ids: List[String], deadlocked: Bool, reason: String = "feedback_cycle_wait", code: String = "feedback_cycle_wait"):
        self.blocked_process_ids = blocked_process_ids.copy()
        self.deadlocked = deadlocked
        self.reason = reason
        self.code = code


@fieldwise_init
struct CorrelationAdvancePlan(Copyable, Movable):
    """Pure advancement result; callers persist ready/cancel transitions."""
    var readied: List[CorrelationProcessPlan]
    var conduction: List[CorrelationConductionValue]
    var blocked: List[CorrelationBlocked]
    var cancelled: List[CorrelationBlocked]
    var wait_diagnostic: CorrelationWaitDiagnostic
    var replayed: Bool


def _has_json_key(text: String, key: String) -> Bool:
    var needle = '"' + key + '"'
    var width = needle.byte_length()
    if width > text.byte_length(): return False
    for index in range(text.byte_length() - width + 1):
        if String(text[byte=index:index + width]) != needle: continue
        var cursor = index + width
        while cursor < text.byte_length():
            var ch = String(text[byte=cursor:cursor + 1])
            if ch != " " and ch != "\n" and ch != "\r" and ch != "\t": break
            cursor += 1
        if cursor < text.byte_length() and String(text[byte=cursor:cursor + 1]) == ":": return True
    return False


def validate_correlation_inputs(fields: List[CorrelationInputField]) raises:
    """Reject placeholders/reserved keys before process scheduling."""
    var seen = List[String]()
    for field in fields:
        if field.key == "": raise Error("correlation input key must not be empty")
        if _contains(seen, field.key): raise Error("correlation input key is duplicated: " + field.key)
        seen.append(field.key)
        if field.key == "adapter" or field.key == "config" or field.key == "conduction" or field.key == "upstream_reactions" or field.key == "regulation" or field.key == "__correlation_config" or field.key == "__correlation_output_schema" or field.key == "__correlation_conduction" or field.key == "__correlation_timeout_seconds":
            raise Error("correlation input uses reserved key: " + field.key)
def validate_correlation_input_json(input_json: String) raises:
    """Reject reserved injected/envelope keys in a raw authored JSON object."""
    var parsed = Value(parse_string=input_json)
    if not parsed.is_object(): raise Error("correlation input must be a JSON object")
    var fields = List[CorrelationInputField]()
    for pair in parsed.object().items():
        fields.append(CorrelationInputField(key=pair.key, value_json=to_string(pair.value.copy())))
    validate_correlation_inputs(fields^)

def _authored_json(fields: List[CorrelationInputField]) raises -> String:
    validate_correlation_inputs(fields)
    var object = Object(capacity=len(fields))
    for field in fields:
        var parsed = Value(parse_string=field.value_json)
        object[field.key] = parsed^
    return canonical_json_text(to_string(Value(object^)))


def _effector_input(global_fields: List[CorrelationInputField], per_effector: Dict[String, List[CorrelationInputField]], effector_id: String) raises -> String:
    # Validate each authored scope first: global/per-effector duplicate keys are
    # errors, while a per-effector key may intentionally override a global key.
    validate_correlation_inputs(global_fields)
    var fields = List[CorrelationInputField]()
    for field in global_fields:
        fields.append(field.copy())
    if effector_id in per_effector:
        validate_correlation_inputs(per_effector[effector_id])
        for field in per_effector[effector_id]:
            var replaced = False
            for index in range(len(fields)):
                if fields[index].key == field.key:
                    fields[index] = field.copy()
                    replaced = True
                    break
            if not replaced:
                fields.append(field.copy())
    return _authored_json(fields^)


def _merge_config(base_json: String, override_json: String, path: String) raises -> String:
    var base = Value(parse_string=base_json)
    var override = Value(parse_string=override_json)
    if not base.is_object() or not override.is_object(): raise Error("correlation config must be a JSON object at " + path)
    var merged = Object(capacity=len(base.object()) + len(override.object()))
    for pair in base.object().items(): merged[pair.key] = pair.value.copy()
    for pair in override.object().items(): merged[pair.key] = pair.value.copy()
    return canonical_json_text(to_string(Value(merged^)))


def instantiate_correlation_path_plan(
    path: CorrelationPathSpec,
    run_id: String,
    correlation_path_id: String = "",
    input_fields: List[CorrelationInputField] = List[CorrelationInputField](),
    max_attempts: Int = 1,
    priority: Int = 0,
    existing_process_ids: List[String] = List[String](),
    per_effector_inputs: Dict[String, List[CorrelationInputField]] = Dict[String, List[CorrelationInputField]](),
    per_effector_configs: Dict[String, String] = Dict[String, String](),
    timeout_by_effector: Dict[String, Float64] = Dict[String, Float64](),
    max_attempts_by_effector: Dict[String, Int] = Dict[String, Int](),
    regulation_by_effector: Dict[String, String] = Dict[String, String](),
) raises -> CorrelationInstantiationPlan:
    """Build deterministic, duplicate-safe process plans without journal coupling."""
    if run_id == "": raise Error("run_id must not be empty")
    if max_attempts < 1: raise Error("max_attempts must be at least one")
    validate_correlation_inputs(input_fields)
    var path_id = correlation_path_id
    if path_id == "": path_id = run_id + ":" + path.id
    var known = List[String]()
    for effector in path.effectors: known.append(effector.id)
    for pair in per_effector_inputs.items():
        if not _contains(known, pair.key): raise Error("per_effector_inputs reference unknown effector: " + pair.key)
    for pair in per_effector_configs.items():
        if not _contains(known, pair.key): raise Error("per_effector_configs reference unknown effector: " + pair.key)
        var parsed = Value(parse_string=pair.value)
        if not parsed.is_object(): raise Error("correlation config must be a JSON object")
    for pair in timeout_by_effector.items():
        if not _contains(known, pair.key): raise Error("timeout_by_effector reference unknown effector: " + pair.key)
        if pair.value < 0.0: raise Error("effector timeout must not be negative")
    for pair in max_attempts_by_effector.items():
        if not _contains(known, pair.key): raise Error("max_attempts_by_effector reference unknown effector: " + pair.key)
        if pair.value < 1: raise Error("effector max_attempts must be at least one")
    for pair in regulation_by_effector.items():
        if not _contains(known, pair.key): raise Error("regulation_by_effector reference unknown effector: " + pair.key)
        var override_regulation = Value(parse_string=pair.value)
        if not override_regulation.is_object(): raise Error("correlation regulation must be a JSON object")
    var plans = List[CorrelationProcessPlan]()
    var all_existing = True
    for index in range(len(path.effectors)):
        var effector = path.effectors[index].copy()
        var process_id = path_id + ":" + effector.id
        var status = "pending"
        if len(effector.conduction) == 0: status = "ready"
        if not _contains(existing_process_ids, process_id): all_existing = False
        var regulation = canonical_json_text(effector.regulation_json)
        if effector.id in regulation_by_effector: regulation = canonical_json_text(regulation_by_effector[effector.id])
        var regulation_value = Value(parse_string=regulation)
        if not regulation_value.is_object(): raise Error("correlation regulation must be a JSON object")
        var marker = '{"correlation_path_id":"' + path_id + '","correlation_path_spec_id":"' + path.id + '","allow_feedback_cycles":' + ("true" if path.allow_feedback_cycles else "false") + ',"effector_id":"' + effector.id + '","seq":' + String(index) + ',"accumulate_upstream_reactions":' + ("true" if path.accumulate_upstream_reactions else "false") + ',"regulation":' + regulation + ',"accepted_reaction_kinds":['
        for reaction_index in range(len(effector.accepted_reaction_kinds)):
            if reaction_index > 0: marker += ','
            marker += '"' + effector.accepted_reaction_kinds[reaction_index] + '"'
        marker += ']}'
        var timeout = effector.timeout_seconds
        if effector.id in timeout_by_effector: timeout = timeout_by_effector[effector.id]
        if timeout < 0.0: raise Error("effector timeout must not be negative")
        var attempts = max_attempts
        if effector.id in max_attempts_by_effector: attempts = max_attempts_by_effector[effector.id]
        if regulation_value.is_object() and "max_attempts" in regulation_value.object():
            var max_value = regulation_value.object()["max_attempts"].copy()
            if max_value.is_int(): attempts = Int(max_value.int())
            elif max_value.is_uint(): attempts = Int(max_value.uint())
            else: raise Error("correlation regulation max_attempts must be an integer")
            if attempts < 1: raise Error("correlation regulation max_attempts must be at least one")
        var config = canonical_json_text(effector.config_json)
        var config_value = Value(parse_string=config)
        if not config_value.is_object(): raise Error("correlation config must be a JSON object at /effectors/" + effector.id + "/config")
        config = canonical_json_text(to_string(config_value^))
        if effector.id in per_effector_configs: config = _merge_config(config, per_effector_configs[effector.id], "/per_effector_configs/" + effector.id)
        var authored = _effector_input(input_fields, per_effector_inputs, effector.id)
        plans.append(CorrelationProcessPlan(id=process_id, run_id=run_id, effector_id=effector.id, declaration_seq=index, status=status, priority=priority, max_attempts=attempts, timeout_seconds=timeout, input_json=authored, config_json=config, output_schema_json=canonical_json_text(effector.output_schema_json), conduction=effector.conduction.copy(), metadata_json=marker, idempotency_key="process.schedule:" + path_id + ":" + effector.id))
    return CorrelationInstantiationPlan(correlation_path_id=path_id, run_id=run_id, processes=plans^, replayed=all_existing)


def instantiate_correlation_path(
    path: CorrelationPathSpec,
    run_id: String,
    existing_process_ids: List[String] = List[String](),
    correlation_path_id: String = "",
    input_fields: List[CorrelationInputField] = List[CorrelationInputField](),
    max_attempts: Int = 1,
    priority: Int = 0,
    per_effector_inputs: Dict[String, List[CorrelationInputField]] = Dict[String, List[CorrelationInputField]](),
    per_effector_configs: Dict[String, String] = Dict[String, String](),
    timeout_by_effector: Dict[String, Float64] = Dict[String, Float64](),
    max_attempts_by_effector: Dict[String, Int] = Dict[String, Int](),
    regulation_by_effector: Dict[String, String] = Dict[String, String](),
) raises -> CorrelationInstantiationPlan:
    """Convenience alias exposing deterministic plan overrides."""
    return instantiate_correlation_path_plan(path, run_id, correlation_path_id, input_fields, max_attempts, priority, existing_process_ids, per_effector_inputs, per_effector_configs, timeout_by_effector, max_attempts_by_effector, regulation_by_effector)


def project_conduction(states: List[CorrelationExecutionState], downstream: CorrelationExecutionState) raises -> List[CorrelationConductionValue]:
    """Project direct upstream outputs in declaration order, exactly once."""
    var projected = List[CorrelationConductionValue]()
    for upstream_id in downstream.conduction:
        var found = False
        for state in states:
            if state.effector_id == upstream_id:
                found = True
                if state.status == "succeeded": projected.append(CorrelationConductionValue(upstream_effector_id=upstream_id, output_json=state.output_json))
                break
        if not found: raise Error("unknown conduction upstream: " + upstream_id)
    return projected^


def _feedback_cycle_member(states: List[CorrelationExecutionState], start: String, blocked_effectors: List[String]) -> Bool:
    var cursor = List[String]()
    var visited = List[String]()
    cursor.append(start)
    while len(cursor) > 0:
        var current = cursor.pop()
        if _contains(visited, current): continue
        visited.append(current)
        var state_index = 0
        while state_index < len(states) and states[state_index].effector_id != current:
            state_index += 1
        if state_index >= len(states): continue
        for upstream in states[state_index].conduction:
            if upstream == start: return True
            if _contains(blocked_effectors, upstream) and not _contains(visited, upstream):
                cursor.append(upstream)
    return False


def advance_correlation_states(path: CorrelationPathSpec, states: List[CorrelationExecutionState]) raises -> CorrelationAdvancePlan:
    """Compute root/chain/diamond readiness and dead-upstream cancellation diagnostics."""
    var readied = List[CorrelationProcessPlan]()
    var projected = List[CorrelationConductionValue]()
    var blocked = List[CorrelationBlocked]()
    var cancelled = List[CorrelationBlocked]()
    for state in states:
        if state.status != "pending": continue
        var unmet = List[String]()
        var dead = List[String]()
        var values = List[CorrelationConductionValue]()
        for upstream_id in state.conduction:
            var found = False
            for upstream in states:
                if upstream.effector_id == upstream_id:
                    found = True
                    if upstream.status == "succeeded": values.append(CorrelationConductionValue(upstream_effector_id=upstream_id, output_json=upstream.output_json))
                    elif upstream.status == "cancelled" or upstream.status == "timed_out" or (upstream.status == "failed" and upstream.attempt >= upstream.max_attempts): dead.append(upstream_id)
                    else: unmet.append(upstream_id)
                    break
            if not found: dead.append(upstream_id)
        if len(dead) > 0:
            cancelled.append(CorrelationBlocked(process_id=state.process_id, effector_id=state.effector_id, unmet=unmet^, dead_upstreams=dead^, reason="dead_upstream"))
        elif len(unmet) > 0:
            blocked.append(CorrelationBlocked(process_id=state.process_id, effector_id=state.effector_id, unmet=unmet^, dead_upstreams=List[String](), reason="unmet_dependencies"))
        else:
            for item in values: projected.append(item.copy())
            readied.append(CorrelationProcessPlan(id=state.process_id, run_id="", effector_id=state.effector_id, declaration_seq=0, status="ready", priority=0, max_attempts=state.max_attempts, timeout_seconds=0.0, input_json=state.input_json, config_json="{}", output_schema_json="{}", conduction=state.conduction.copy(), metadata_json="{}", idempotency_key="process.ready:" + state.process_id))
    if path.allow_feedback_cycles and len(blocked) > 0:
        # Only pending members of an actual dependency cycle are deadlocked.
        # Running/retry/waiting upstreams remain ordinary unresolved waits.
        var blocked_effectors = List[String]()
        for item in blocked: blocked_effectors.append(item.effector_id)
        for index in range(len(blocked)):
            if _feedback_cycle_member(states, blocked[index].effector_id, blocked_effectors):
                var item = blocked[index].copy()
                item.reason = "feedback_cycle_wait"
                blocked[index] = item.copy()
    var wait_diagnostic = diagnose_correlation_wait(path, states, blocked, readied)
    return CorrelationAdvancePlan(readied=readied^, conduction=projected^, blocked=blocked^, cancelled=cancelled^, wait_diagnostic=wait_diagnostic^, replayed=False)
def diagnose_correlation_wait(path: CorrelationPathSpec, states: List[CorrelationExecutionState], blocked: List[CorrelationBlocked], readied: List[CorrelationProcessPlan]) -> CorrelationWaitDiagnostic:
    """Return a stable persisted diagnosis for an actual feedback-cycle wait."""
    var ids = List[String]()
    if path.allow_feedback_cycles and len(blocked) > 0:
        var blocked_effectors = List[String]()
        for item in blocked: blocked_effectors.append(item.effector_id)
        for item in blocked:
            if item.reason == "feedback_cycle_wait" and _feedback_cycle_member(states, item.effector_id, blocked_effectors):
                ids.append(item.process_id)
    return CorrelationWaitDiagnostic(blocked_process_ids=ids^, deadlocked=len(ids) > 0, reason=("feedback_cycle_wait" if len(ids) > 0 else ""), code=("feedback_cycle_wait" if len(ids) > 0 else ""))



def replay_safe_advance(previous: CorrelationAdvancePlan, current: CorrelationAdvancePlan) -> CorrelationAdvancePlan:
    var same = len(previous.readied) == len(current.readied)
    if same:
        for index in range(len(previous.readied)):
            if previous.readied[index].id != current.readied[index].id: same = False
    if same:
        return CorrelationAdvancePlan(readied=List[CorrelationProcessPlan](), conduction=List[CorrelationConductionValue](), blocked=current.blocked.copy(), cancelled=current.cancelled.copy(), wait_diagnostic=current.wait_diagnostic.copy(), replayed=True)
    return current.copy()
