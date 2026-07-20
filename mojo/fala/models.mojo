"""Native Fala package/domain contracts.

This module is deliberately independent of Python and std.python.  JSON-shaped
payloads are represented as String until the native JsonValue API is stable;
`to_json` hooks provide an explicit serialization boundary for later cutover.
Factories validate the same structural invariants as the Python source models.
"""

from std.collections import Dict
from std.collections import List
from .json import canonical_json_text
def _canonical(value: String) raises -> String:
    return canonical_json_text(value)


def _json_escape(value: String) -> String:
    var result = String()
    for ch in value.codepoint_slices():
        if ch == "\\": result += "\\\\"
        elif ch == "\"": result += "\\\""
        elif ch == "\n": result += "\\n"
        elif ch == "\r": result += "\\r"
        elif ch == "\t": result += "\\t"
        else: result += ch
    return result^


def _json_quote(value: String) -> String:
    return "\"" + _json_escape(value) + "\""
def _json_optional(value: String) -> String:
    if value == "": return "null"
    return _json_quote(value)


def _json_strings(values: List[String]) -> String:
    var result = "["
    var first = True
    for value in values:
        if not first: result += ","
        result += _json_quote(value)
        first = False
    return result + "]"


def _json_map(values: Dict[String, String]) -> String:
    var keys = List[String]()
    for pair in values.items(): keys.append(pair.key.copy())
    var i = 1
    while i < len(keys):
        var key = keys[i].copy()
        var j = i
        while j > 0 and keys[j - 1] > key:
            keys[j] = keys[j - 1].copy()
            j -= 1
        keys[j] = key^
        i += 1
    var result = "{"
    var first = True
    for key in keys:
        if not first: result += ","
        result += _json_quote(key) + ":" + _json_quote(values[key])
        first = False
    return result + "}"

def _json_optional_map(values: Dict[String, String]) -> String:
    var keys = List[String]()
    for pair in values.items(): keys.append(pair.key.copy())
    var i = 1
    while i < len(keys):
        var key = keys[i].copy()
        var j = i
        while j > 0 and keys[j - 1] > key:
            keys[j] = keys[j - 1].copy()
            j -= 1
        keys[j] = key^
        i += 1
    var result = "{"
    var first = True
    for key in keys:
        if not first: result += ","
        var value = ""
        for pair in values.items():
            if pair.key == key: value = pair.value.copy()
        if value == "": result += _json_quote(key) + ":null"
        else: result += _json_quote(key) + ":" + _json_quote(value)
        first = False
    return result + "}"


def _json_list_map(values: Dict[String, List[String]]) -> String:
    var keys = List[String]()
    for pair in values.items(): keys.append(pair.key.copy())
    var i = 1
    while i < len(keys):
        var key = keys[i].copy()
        var j = i
        while j > 0 and keys[j - 1] > key:
            keys[j] = keys[j - 1].copy()
            j -= 1
        keys[j] = key^
        i += 1
    var result = "{"
    var first = True
    for key in keys:
        if not first: result += ","
        var values_for_key = List[String]()
        for pair in values.items():
            if pair.key == key: values_for_key = pair.value.copy()
        result += _json_quote(key) + ":" + _json_strings(values_for_key)
        first = False
    return result + "}"


def _json_fragment(value: String) raises -> String:
    if value == "": return "{}"
    return canonical_json_text(value)


def _check_runtime_id(value: String, label: String = "RuntimeId") raises:
    if value.byte_length() < 1 or value.byte_length() > 128:
        raise Error(label + " must contain 1..128 ASCII characters")
    var first = True
    for ch in value.codepoint_slices():
        if first:
            if ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz":
                raise Error(label + " must start with an ASCII letter")
            first = False
        elif ch not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-":
            raise Error(label + " contains an invalid character")


def _check_nonempty(value: String, label: String) raises:
    if value == "":
        raise Error(label + " must not be empty")


def _unique(values: List[String], label: String) raises:
    var seen = Dict[String, Bool]()
    for value in values:
        if value in seen:
            raise Error("duplicate " + label + " '" + value + "'")
        seen[value] = True


def _known(values: List[String], known: Dict[String, Bool], label: String) raises:
    for value in values:
        if value not in known:
            raise Error(label + " references unknown id '" + value + "'")


def _validate_acyclic(ids: List[String], graph: Dict[String, List[String]]) raises:
    # Iterative depth-first traversal avoids the old three-edge bound while
    # keeping cycle reports deterministic (ids and dependencies are authored
    # in stable order).  A gray node is on the active path and therefore
    # closes a cycle; black nodes have already been completely checked.
    var state = Dict[String, Int]()
    for node in ids:
        state[node] = 0
    var stack = List[String]()
    var offsets = List[Int]()
    for root in ids:
        if state[root] != 0: continue
        stack.append(root)
        offsets.append(0)
        state[root] = 1
        while len(stack) > 0:
            var top = len(stack) - 1
            var current = stack[top]
            var dependencies = graph[current]
            if offsets[top] >= len(dependencies):
                state[current] = 2
                _ = stack.pop()
                _ = offsets.pop()
                continue
            var dependency = dependencies[offsets[top]]
            offsets[top] += 1
            if state[dependency] == 1:
                raise Error("correlation path contains a conduction cycle at '" + dependency + "'")
            if state[dependency] == 0:
                stack.append(dependency)
                offsets.append(0)
                state[dependency] = 1


struct RuntimeId(Copyable, Movable):
    var value: String

    def __init__(out self, value: String):
        self.value = value

    @staticmethod
    def create(value: String) raises -> RuntimeId:
        _check_runtime_id(value)
        return RuntimeId(value)

    def to_json(self) -> String:
        return "\"" + self.value + "\""

    def __str__(self) -> String:
        return self.value


@fieldwise_init
struct ReactionRef(Copyable, Movable):
    var id: String
    var kind: String
    var uri: String
    var metadata: Dict[String, String]

    @staticmethod
    def create(id: String, kind: String, uri: String, metadata: Dict[String, String] = Dict[String, String]()) raises -> ReactionRef:
        _check_runtime_id(id, "reaction id")
        _check_nonempty(kind, "reaction kind")
        _check_nonempty(uri, "reaction uri")
        return ReactionRef(id=id, kind=kind, uri=uri, metadata=metadata^)

    def to_json(self) raises -> String:
        return _canonical("{\"id\":" + _json_quote(self.id) + ",\"kind\":" + _json_quote(self.kind) + ",\"uri\":" + _json_quote(self.uri) + ",\"metadata\":" + _json_map(self.metadata) + "}")


@fieldwise_init
struct ImpulseTypeSpec(Copyable, Movable):
    var id: String
    var title: String
    var description: String
    var tags: List[String]
    var media_types: List[String]
    var value_schema: String
    var metadata_schema: String

    @staticmethod
    def create(id: String, title: String = "", description: String = "", var tags: List[String] = List[String](), var media_types: List[String] = List[String](), value_schema: String = "", metadata_schema: String = "") raises -> ImpulseTypeSpec:
        _check_runtime_id(id, "impulse type id")
        return ImpulseTypeSpec(id=id, title=title, description=description, tags=tags^, media_types=media_types^, value_schema=value_schema, metadata_schema=metadata_schema)

    def to_json(self) raises -> String:
        return _canonical("{\"id\":" + _json_quote(self.id) + ",\"title\":" + _json_optional(self.title) + ",\"description\":" + _json_optional(self.description) + ",\"tags\":" + _json_strings(self.tags) + ",\"media_types\":" + _json_strings(self.media_types) + ",\"value_schema\":" + _json_fragment(self.value_schema) + ",\"metadata_schema\":" + _json_fragment(self.metadata_schema) + "}")


@fieldwise_init
struct ImpulseRelationSpec(Copyable, Movable):
    var id: String
    var title: String
    var description: String
    var tags: List[String]
    var source_impulse_types: List[String]
    var target_impulse_types: List[String]

    @staticmethod
    def create(id: String, title: String = "", description: String = "", tags: List[String] = List[String](), source_impulse_types: List[String] = List[String](), target_impulse_types: List[String] = List[String]()) raises -> ImpulseRelationSpec:
        _check_runtime_id(id, "impulse relation id")
        _unique(source_impulse_types, "source impulse type")
        _unique(target_impulse_types, "target impulse type")
        return ImpulseRelationSpec(id=id, title=title, description=description, tags=tags^, source_impulse_types=source_impulse_types^, target_impulse_types=target_impulse_types^)

    def to_json(self) raises -> String:
        return _canonical("{\"id\":" + _json_quote(self.id) + ",\"title\":" + _json_optional(self.title) + ",\"description\":" + _json_optional(self.description) + ",\"tags\":" + _json_strings(self.tags) + ",\"source_impulse_types\":" + _json_strings(self.source_impulse_types) + ",\"target_impulse_types\":" + _json_strings(self.target_impulse_types) + "}")


@fieldwise_init
struct AssociationKindSpec(Copyable, Movable):
    var id: String
    var title: String
    var description: String
    var tags: List[String]
    var value_schema: String
    var metadata_schema: String

    @staticmethod
    def create(id: String, title: String = "", description: String = "", tags: List[String] = List[String](), value_schema: String = "", metadata_schema: String = "") raises -> AssociationKindSpec:
        _check_runtime_id(id, "association kind id")
        return AssociationKindSpec(id=id, title=title, description=description, tags=tags^, value_schema=value_schema, metadata_schema=metadata_schema)

    def to_json(self) raises -> String:
        return _canonical("{\"id\":" + _json_quote(self.id) + ",\"title\":" + _json_optional(self.title) + ",\"description\":" + _json_optional(self.description) + ",\"tags\":" + _json_strings(self.tags) + ",\"value_schema\":" + _json_fragment(self.value_schema) + ",\"metadata_schema\":" + _json_fragment(self.metadata_schema) + "}")


@fieldwise_init
struct ReactionKindSpec(Copyable, Movable):
    var id: String
    var title: String
    var description: String
    var tags: List[String]
    var media_types: List[String]
    var value_schema: String
    var metadata_schema: String

    @staticmethod
    def create(id: String, title: String = "", description: String = "", tags: List[String] = List[String](), media_types: List[String] = List[String](), value_schema: String = "", metadata_schema: String = "") raises -> ReactionKindSpec:
        _check_runtime_id(id, "reaction kind id")
        return ReactionKindSpec(id=id, title=title, description=description, tags=tags^, media_types=media_types^, value_schema=value_schema, metadata_schema=metadata_schema)

    def to_json(self) raises -> String:
        return _canonical("{\"id\":" + _json_quote(self.id) + ",\"title\":" + _json_optional(self.title) + ",\"description\":" + _json_optional(self.description) + ",\"tags\":" + _json_strings(self.tags) + ",\"media_types\":" + _json_strings(self.media_types) + ",\"value_schema\":" + _json_fragment(self.value_schema) + ",\"metadata_schema\":" + _json_fragment(self.metadata_schema) + "}")


@fieldwise_init
struct CapabilitySpec(Copyable, Movable):
    var id: String
    var title: String
    var description: String
    var tags: List[String]
    var accepts_impulse_types: List[String]
    var accepts_reaction_kinds: List[String]
    var emits_impulse_types: List[String]
    var emits_reaction_kinds: List[String]
    var emits_association_kinds: List[String]
    var config_schema: String
    var output_schema: String

    @staticmethod
    def create(id: String, title: String = "", description: String = "", var tags: List[String] = List[String](), var accepts_impulse_types: List[String] = List[String](), var accepts_reaction_kinds: List[String] = List[String](), var emits_impulse_types: List[String] = List[String](), var emits_reaction_kinds: List[String] = List[String](), var emits_association_kinds: List[String] = List[String](), config_schema: String = "", output_schema: String = "") raises -> CapabilitySpec:
        _check_runtime_id(id, "capability id")
        return CapabilitySpec(id=id, title=title, description=description, tags=tags^, accepts_impulse_types=accepts_impulse_types^, accepts_reaction_kinds=accepts_reaction_kinds^, emits_impulse_types=emits_impulse_types^, emits_reaction_kinds=emits_reaction_kinds^, emits_association_kinds=emits_association_kinds^, config_schema=config_schema, output_schema=output_schema)

    def to_json(self) raises -> String:
        return _canonical("{\"id\":" + _json_quote(self.id) + ",\"title\":" + _json_optional(self.title) + ",\"description\":" + _json_optional(self.description) + ",\"tags\":" + _json_strings(self.tags) + ",\"accepts_impulse_types\":" + _json_strings(self.accepts_impulse_types) + ",\"accepts_reaction_kinds\":" + _json_strings(self.accepts_reaction_kinds) + ",\"emits_impulse_types\":" + _json_strings(self.emits_impulse_types) + ",\"emits_reaction_kinds\":" + _json_strings(self.emits_reaction_kinds) + ",\"emits_association_kinds\":" + _json_strings(self.emits_association_kinds) + ",\"config_schema\":" + _json_fragment(self.config_schema) + ",\"output_schema\":" + _json_fragment(self.output_schema) + "}")


@fieldwise_init
struct EffectorAdapterSpec(Copyable, Movable):
    var kind: String
    var command: List[String]
    var `ref`: String
    var runtime_ref: String
    var cwd: String
    var env: Dict[String, String]
    var inherit_env: List[String]
    var timeout_seconds: Float64

    @staticmethod
    def create(kind: String, var command: List[String] = List[String](), `ref`: String = "", runtime_ref: String = "", cwd: String = "", var env: Dict[String, String] = Dict[String, String](), var inherit_env: List[String] = List[String](), timeout_seconds: Float64 = -1.0) raises -> EffectorAdapterSpec:
        if kind != "subprocess" and kind != "native_function" and kind != "python_function" and kind != "manual_homeostat" and kind != "fala_runtime":
            raise Error("unknown effector adapter kind '" + kind + "'")
        if timeout_seconds == 0.0: raise Error("adapter timeout_seconds must be greater than 0 when provided")
        if timeout_seconds < -1.0: raise Error("adapter timeout_seconds must not be negative")
        var normalized_timeout = timeout_seconds if timeout_seconds > 0.0 else 0.0
        if kind == "subprocess":
            if len(command) == 0: raise Error("subprocess adapter requires non-empty command")
            if `ref` != "" or runtime_ref != "": raise Error("subprocess adapter cannot define ref/runtime_ref")
        elif kind == "native_function" or kind == "python_function":
            _check_nonempty(`ref`, kind + " adapter ref")
            if len(command) != 0 or runtime_ref != "" or cwd != "" or len(env) != 0 or len(inherit_env) != 0: raise Error(kind + " adapter has invalid boundary fields")
        elif kind == "manual_homeostat":
            if len(command) != 0 or `ref` != "" or runtime_ref != "" or cwd != "" or len(env) != 0 or len(inherit_env) != 0 or normalized_timeout != 0.0: raise Error("manual_homeostat adapter has invalid boundary fields")
        else:
            _check_nonempty(runtime_ref, "fala_runtime adapter runtime_ref")
            if len(command) != 0 or `ref` != "" or cwd != "" or len(env) != 0 or len(inherit_env) != 0: raise Error("fala_runtime adapter has invalid boundary fields")
        if kind != "subprocess" and len(inherit_env) != 0: raise Error("only subprocess adapters may inherit environment")
        return EffectorAdapterSpec(kind=kind, command=command^, `ref`=`ref`, runtime_ref=runtime_ref, cwd=cwd, env=env^, inherit_env=inherit_env^, timeout_seconds=normalized_timeout)

    def to_json(self) raises -> String:
        var result = "{\"kind\":" + _json_quote(self.kind)
        if len(self.command) != 0: result += ",\"command\":" + _json_strings(self.command)
        if self.`ref` != "": result += ",\"ref\":" + _json_quote(self.`ref`)
        if self.runtime_ref != "": result += ",\"runtime_ref\":" + _json_quote(self.runtime_ref)
        if self.cwd != "": result += ",\"cwd\":" + _json_quote(self.cwd)
        if len(self.env) != 0: result += ",\"env\":" + _json_map(self.env)
        if len(self.inherit_env) != 0: result += ",\"inherit_env\":" + _json_strings(self.inherit_env)
        if self.timeout_seconds > 0.0: result += ",\"timeout_seconds\":" + String(self.timeout_seconds)
        return _canonical(result + "}")



@fieldwise_init
struct EffectorSpec(Copyable, Movable):
    var id: String
    var title: String
    var description: String
    var tags: List[String]
    var capability: String
    var adapter: EffectorAdapterSpec
    var conduction: List[String]
    var timeout_seconds: Float64
    var config: String

    @staticmethod
    def create(id: String, capability: String, var adapter: EffectorAdapterSpec, title: String = "", description: String = "", var tags: List[String] = List[String](), var conduction: List[String] = List[String](), timeout_seconds: Float64 = -1.0, config: String = "") raises -> EffectorSpec:
        _check_runtime_id(id, "effector id")
        _check_runtime_id(capability, "effector capability")
        _unique(conduction, "conduction")
        if id in conduction: raise Error("effector cannot depend on itself")
        if timeout_seconds == 0.0: raise Error("effector timeout_seconds must be greater than 0 when provided")
        if timeout_seconds < -1.0: raise Error("effector timeout_seconds must not be negative")
        var normalized_timeout = timeout_seconds if timeout_seconds > 0.0 else 0.0
        return EffectorSpec(id=id, title=title, description=description, tags=tags^, capability=capability, adapter=adapter^, conduction=conduction^, timeout_seconds=normalized_timeout, config=config)

    def to_json(self) raises -> String:
        var result = "{\"id\":" + _json_quote(self.id) + ",\"title\":" + _json_optional(self.title) + ",\"description\":" + _json_optional(self.description) + ",\"tags\":" + _json_strings(self.tags) + ",\"capability\":" + _json_quote(self.capability) + ",\"adapter\":" + self.adapter.to_json() + ",\"conduction\":" + _json_strings(self.conduction)
        if self.timeout_seconds > 0.0: result += ",\"timeout_seconds\":" + String(self.timeout_seconds)
        result += ",\"config\":" + _json_fragment(self.config) + "}"
        return _canonical(result)


@fieldwise_init
struct CorrelationWaitDiagnostic(Copyable, Movable):
    """Deterministic persisted diagnosis for an unresolved feedback wait."""
    var blocked_process_ids: List[String]
    var deadlocked: Bool
    var reason: String
    var code: String

    @staticmethod
    def create(blocked_process_ids: List[String], deadlocked: Bool, reason: String = "feedback_cycle_wait", code: String = "feedback_cycle_wait") raises -> CorrelationWaitDiagnostic:
        if reason == "": raise Error("wait diagnostic reason must not be empty")
        if code == "": raise Error("wait diagnostic code must not be empty")
        _unique(blocked_process_ids, "blocked process")
        return CorrelationWaitDiagnostic(blocked_process_ids=blocked_process_ids^, deadlocked=deadlocked, reason=reason, code=code)

    def to_json(self) raises -> String:
        return _canonical("{\"blocked_process_ids\":" + _json_strings(self.blocked_process_ids) + ",\"deadlocked\":" + ("true" if self.deadlocked else "false") + ",\"reason\":" + _json_quote(self.reason) + ",\"code\":" + _json_quote(self.code) + "}")
@fieldwise_init
struct WaitDiagnosticIssue(Copyable, Movable):
    var process_id: String
    var status: String
    var reason: String
    var blocked_by: List[String]
    var dependency_statuses: Dict[String, String]
    var data: String

    def to_json(self) -> String:
        var result = "{\"process_id\":" + _json_quote(self.process_id)
        result += ",\"status\":" + (_json_optional(self.status))
        result += ",\"reason\":" + _json_quote(self.reason)
        result += ",\"blocked_by\":" + _json_strings(self.blocked_by)
        result += ",\"dependency_statuses\":" + _json_optional_map(self.dependency_statuses)
        result += ",\"data\":" + (self.data if self.data != "" else "{}") + "}"
        return result

@fieldwise_init
struct WaitGraphDiagnostic(Copyable, Movable):
    var run_id: String
    var impulse_id: String
    var deadlocked: Bool
    var deadlocks: List[List[String]]
    var wait_edges: Dict[String, List[String]]
    var blocked: List[WaitDiagnosticIssue]
    var open_homeostats: List[String]
    var pending: List[String]
    var ready: List[String]
    var running: List[String]
    var waiting: List[String]
    var retry_wait: List[String]
    var succeeded: List[String]
    var failed: List[String]
    var cancel_requested: List[String]
    var cancelled: List[String]
    var timed_out: List[String]
    # Compatibility fields for bounded driver callers predating graph diagnosis.
    var blocked_process_ids: List[String]
    var reason: String
    var code: String

    def to_json(self) -> String:
        var result = "{\"run_id\":" + _json_quote(self.run_id)
        result += ",\"impulse_id\":" + _json_optional(self.impulse_id)
        result += ",\"deadlocked\":" + ("true" if self.deadlocked else "false")
        result += ",\"deadlocks\":["
        var first = True
        for cycle in self.deadlocks:
            if not first: result += ","
            first = False
            result += _json_strings(cycle)
        result += "],\"wait_edges\":" + _json_list_map(self.wait_edges)
        result += ",\"blocked\":["; first = True
        for issue in self.blocked:
            if not first: result += ","
            first = False
            result += issue.to_json()
        result += "],\"open_homeostats\":" + _json_strings(self.open_homeostats)
        result += ",\"pending\":" + _json_strings(self.pending)
        result += ",\"ready\":" + _json_strings(self.ready)
        result += ",\"running\":" + _json_strings(self.running)
        result += ",\"waiting\":" + _json_strings(self.waiting)
        result += ",\"retry_wait\":" + _json_strings(self.retry_wait)
        result += ",\"succeeded\":" + _json_strings(self.succeeded)
        result += ",\"failed\":" + _json_strings(self.failed)
        result += ",\"cancel_requested\":" + _json_strings(self.cancel_requested)
        result += ",\"cancelled\":" + _json_strings(self.cancelled)
        result += ",\"timed_out\":" + _json_strings(self.timed_out) + "}"
        return result


@fieldwise_init
struct CorrelationPathSpec(Copyable, Movable):
    var id: String
    var title: String
    var description: String
    var tags: List[String]
    var effectors: List[EffectorSpec]
    var allow_feedback_cycles: Bool
    var accumulate_upstream_reactions: Bool

    @staticmethod
    def create(id: String, var effectors: List[EffectorSpec], title: String = "", description: String = "", var tags: List[String] = List[String](), allow_feedback_cycles: Bool = False, accumulate_upstream_reactions: Bool = False) raises -> CorrelationPathSpec:
        _check_runtime_id(id, "correlation path id")
        if len(effectors) == 0: raise Error("correlation path effectors must be nonempty")
        var ids = List[String]()
        var graph = Dict[String, List[String]]()
        for effector in effectors:
            if effector.id in graph: raise Error("duplicate effector id '" + effector.id + "'")
            ids.append(effector.id)
            graph[effector.id] = effector.conduction.copy()
        for effector in effectors:
            for dependency in effector.conduction:
                if dependency not in graph: raise Error("effector conduction references unknown id '" + dependency + "'")
        if not allow_feedback_cycles: _validate_acyclic(ids, graph)
        return CorrelationPathSpec(id=id, title=title, description=description, tags=tags^, effectors=effectors^, allow_feedback_cycles=allow_feedback_cycles, accumulate_upstream_reactions=accumulate_upstream_reactions)

    def to_json(self) raises -> String:
        var effectors_json = "["
        var first = True
        for effector in self.effectors:
            if not first: effectors_json += ","
            effectors_json += effector.to_json()
            first = False
        var result = "{\"id\":" + _json_quote(self.id) + ",\"title\":" + _json_optional(self.title) + ",\"description\":" + _json_optional(self.description) + ",\"tags\":" + _json_strings(self.tags) + ",\"effectors\":" + effectors_json + "]"
        if self.allow_feedback_cycles: result += ",\"allow_feedback_cycles\":true"
        if self.accumulate_upstream_reactions: result += ",\"accumulate_upstream_reactions\":true"
        return canonical_json_text(result + "}")


@fieldwise_init
struct RuntimeBackendConfig(Copyable, Movable):
    var kind: String
    var path: String

    @staticmethod
    def create(path: String, kind: String = "sqlite") raises -> RuntimeBackendConfig:
        if kind != "sqlite": raise Error("runtime backend kind must be sqlite")
        _check_nonempty(path, "runtime backend path")
        return RuntimeBackendConfig(kind=kind, path=path)

    def to_json(self) raises -> String:
        return canonical_json_text("{\"kind\":" + _json_quote(self.kind) + ",\"path\":" + _json_quote(self.path) + "}")


@fieldwise_init
struct ReactionStoreConfig(Copyable, Movable):
    var kind: String
    var root: String

    @staticmethod
    def create(root: String, kind: String = "filesystem") raises -> ReactionStoreConfig:
        if kind != "filesystem": raise Error("reaction store kind must be filesystem")
        _check_nonempty(root, "reaction store root")
        return ReactionStoreConfig(kind=kind, root=root)

    def to_json(self) raises -> String:
        return canonical_json_text("{\"kind\":" + _json_quote(self.kind) + ",\"root\":" + _json_quote(self.root) + "}")


@fieldwise_init
struct RuntimeConfigSpec(Copyable, Movable):
    var backend: RuntimeBackendConfig
    var reaction_store: ReactionStoreConfig

    @staticmethod
    def create(backend: RuntimeBackendConfig, reaction_store: ReactionStoreConfig) -> RuntimeConfigSpec:
        return RuntimeConfigSpec(backend=backend^, reaction_store=reaction_store^)

    def to_json(self) raises -> String:
        return canonical_json_text("{\"backend\":" + self.backend.to_json() + ",\"reaction_store\":" + self.reaction_store.to_json() + "}")


@fieldwise_init
struct FalaPackageSpec(Copyable, Movable):
    var id: String
    var title: String
    var description: String
    var tags: List[String]
    var version: String
    var impulse_types: List[ImpulseTypeSpec]
    var impulse_relations: List[ImpulseRelationSpec]
    var association_kinds: List[AssociationKindSpec]
    var reaction_kinds: List[ReactionKindSpec]
    var capabilities: List[CapabilitySpec]
    var correlation_paths: List[CorrelationPathSpec]
    var has_runtime: Bool
    var runtime: RuntimeConfigSpec

    @staticmethod
    def create(id: String, var correlation_paths: List[CorrelationPathSpec], title: String = "", description: String = "", var tags: List[String] = List[String](), version: String = "2", var impulse_types: List[ImpulseTypeSpec] = List[ImpulseTypeSpec](), var impulse_relations: List[ImpulseRelationSpec] = List[ImpulseRelationSpec](), var association_kinds: List[AssociationKindSpec] = List[AssociationKindSpec](), var reaction_kinds: List[ReactionKindSpec] = List[ReactionKindSpec](), var capabilities: List[CapabilitySpec] = List[CapabilitySpec](), has_runtime: Bool = False, var runtime: RuntimeConfigSpec = RuntimeConfigSpec(backend=RuntimeBackendConfig(kind="sqlite", path=""), reaction_store=ReactionStoreConfig(kind="filesystem", root=""))) raises -> FalaPackageSpec:
        _check_runtime_id(id, "fala package id")
        if len(correlation_paths) == 0: raise Error("fala package correlation_paths must be nonempty")
        var impulse_ids = Dict[String, Bool]()
        for item in impulse_types:
            if item.id in impulse_ids: raise Error("duplicate impulse type id '" + item.id + "'")
            impulse_ids[item.id] = True
        var relation_ids = Dict[String, Bool]()
        for item in impulse_relations:
            if item.id in relation_ids: raise Error("duplicate impulse relation id '" + item.id + "'")
            relation_ids[item.id] = True
            _known(item.source_impulse_types, impulse_ids, "impulse relation source")
            _known(item.target_impulse_types, impulse_ids, "impulse relation target")
        var association_ids = Dict[String, Bool]()
        for item in association_kinds:
            if item.id in association_ids: raise Error("duplicate association kind id '" + item.id + "'")
            association_ids[item.id] = True
        var reaction_ids = Dict[String, Bool]()
        for item in reaction_kinds:
            if item.id in reaction_ids: raise Error("duplicate reaction kind id '" + item.id + "'")
            reaction_ids[item.id] = True
        var capability_ids = Dict[String, Bool]()
        for item in capabilities:
            if item.id in capability_ids: raise Error("duplicate capability id '" + item.id + "'")
            capability_ids[item.id] = True
            _known(item.accepts_impulse_types, impulse_ids, "capability accepts impulse")
            _known(item.emits_impulse_types, impulse_ids, "capability emits impulse")
            _known(item.accepts_reaction_kinds, reaction_ids, "capability accepts reaction")
            _known(item.emits_reaction_kinds, reaction_ids, "capability emits reaction")
            _known(item.emits_association_kinds, association_ids, "capability emits association")
        var path_ids = Dict[String, Bool]()
        for path in correlation_paths:
            if path.id in path_ids: raise Error("duplicate correlation path id '" + path.id + "'")
            path_ids[path.id] = True
            for effector in path.effectors:
                if effector.capability not in capability_ids: raise Error("correlation path references unknown capability '" + effector.capability + "'")
        return FalaPackageSpec(id=id, title=title, description=description, tags=tags^, version=version, impulse_types=impulse_types^, impulse_relations=impulse_relations^, association_kinds=association_kinds^, reaction_kinds=reaction_kinds^, capabilities=capabilities^, correlation_paths=correlation_paths^, has_runtime=has_runtime, runtime=runtime^)

    def to_json(self) raises -> String:
        var impulse_types_json = "["; var relation_json = "["; var association_json = "["; var reaction_json = "["; var capability_json = "["; var paths_json = "["
        var first = True
        for item in self.impulse_types:
            if not first: impulse_types_json += ","
            impulse_types_json += item.to_json(); first = False
        impulse_types_json += "]"; first = True
        for item in self.impulse_relations:
            if not first: relation_json += ","
            relation_json += item.to_json(); first = False
        relation_json += "]"; first = True
        for item in self.association_kinds:
            if not first: association_json += ","
            association_json += item.to_json(); first = False
        association_json += "]"; first = True
        for item in self.reaction_kinds:
            if not first: reaction_json += ","
            reaction_json += item.to_json(); first = False
        reaction_json += "]"; first = True
        for item in self.capabilities:
            if not first: capability_json += ","
            capability_json += item.to_json(); first = False
        capability_json += "]"; first = True
        for item in self.correlation_paths:
            if not first: paths_json += ","
            paths_json += item.to_json(); first = False
        paths_json += "]"
        var runtime_json = "null"
        if self.has_runtime: runtime_json = self.runtime.to_json()
        return _canonical("{\"id\":" + _json_quote(self.id) + ",\"title\":" + _json_quote(self.title) + ",\"description\":" + _json_quote(self.description) + ",\"tags\":" + _json_strings(self.tags) + ",\"version\":" + _json_quote(self.version) + ",\"impulse_types\":" + impulse_types_json + ",\"impulse_relations\":" + relation_json + ",\"association_kinds\":" + association_json + ",\"reaction_kinds\":" + reaction_json + ",\"capabilities\":" + capability_json + ",\"correlation_paths\":" + paths_json + ",\"runtime\":" + runtime_json + "}")
def validate_runtime_id(value: String) raises:
    _check_runtime_id(value)
