"""Pure native runtime-pool and delegation-policy helpers.

This boundary only validates JSON-shaped domain records and chooses an explicit
RuntimeRef.  It never resolves adapters or performs transport I/O.
"""

from std.collections import List
from emberjson import Value, Object, to_string
from fala.json import canonical_json_text
from fala.domain import RuntimeRef, RuntimePool, DelegationPolicy, RuntimeBudget
from fala.errors import ValidationError


@fieldwise_init
struct RuntimePolicyError(Copyable, Movable):
    var code: String
    var path: String
    var message: String

    def __str__(self) -> String:
        return self.code + " at " + self.path + ": " + self.message

    def is_ok(self) -> Bool:
        return self.code == ""


@fieldwise_init
struct RuntimeSelection(Copyable, Movable):
    var runtime: RuntimeRef
    var index: Int
    var next_index: Int
    var policy: String


@fieldwise_init
struct DelegationEnvelope(Copyable, Movable):
    var delivery_id: String
    var target_run_id: String
    var pool_id: String
    var budget: RuntimeBudget
    var metadata: String


def _fail(code: String, path: String, message: String) -> RuntimePolicyError:
    return RuntimePolicyError(code, path, message)


def _ok() -> RuntimePolicyError:
    return RuntimePolicyError("", "", "")


def _error(error: RuntimePolicyError) raises:
    if not error.is_ok():
        raise Error(error.message + " [" + error.code + "] at " + error.path)


def _parse(text: String, path: String) raises -> Value:
    try:
        return Value(parse_string=text)
    except err:
        raise Error("invalid_json at " + path + ": malformed JSON")


def _string(value: Value, path: String) raises -> String:
    if not value.is_string():
        raise Error("invalid_type at " + path + ": expected string")
    return value.string()


def _integer(value: Value, path: String) raises -> Int:
    # Pydantic's integer fields accept numeric strings, but not fractional values.
    if value.is_string():
        var parsed = _parse(value.string(), path)
        return _integer(parsed^, path)
    if value.is_int(): return Int(value.int())
    if value.is_uint(): return Int(value.uint())
    raise Error("invalid_type at " + path + ": expected integer")


def _strict_strings(value: Value, path: String) raises -> List[String]:
    if not value.is_array():
        raise Error("invalid_type at " + path + ": expected array of strings")
    var result = List[String]()
    var i = 0
    for item in value.array():
        var text = _string(item.copy(), path + "/" + String(i))
        if text == "": raise Error("invalid_value at " + path + ": empty string")
        for prior in result:
            if prior == text: raise Error("duplicate_value at " + path + ": duplicate " + text)
        result.append(text^)
        i += 1
    return result^


def parse_runtime_refs_json(json_text: String) raises -> List[RuntimeRef]:
    """Decode a strict array of `{id, uri?, metadata?}` runtime references."""
    var root = _parse(json_text, "/runtimes")
    if not root.is_array(): raise Error("invalid_type at /runtimes: expected array")
    var result = List[RuntimeRef]()
    var i = 0
    for item in root.array():
        var path = "/runtimes/" + String(i)
        if not item.is_object(): raise Error("invalid_type at " + path + ": expected object")
        var object = item.object().copy()
        for pair in object.items():
            if pair.key != "id" and pair.key != "uri" and pair.key != "metadata":
                raise Error("unknown_field at " + path + "/" + pair.key + ": unknown field")
        if "id" not in object: raise Error("missing_field at " + path + "/id: required field is missing")
        var id = _string(object["id"].copy(), path + "/id")
        var id_error = RuntimePolicyError("", "", "")
        if id == "": id_error = _fail("invalid_runtime_id", path + "/id", "runtime id must not be empty")
        _error(id_error)
        var uri = String("")
        if "uri" in object: uri = _string(object["uri"].copy(), path + "/uri")
        var metadata = String("{}")
        if "metadata" in object:
            var metadata_value = object["metadata"].copy()
            if not metadata_value.is_object(): raise Error("invalid_type at " + path + "/metadata: expected object")
            metadata = canonical_json_text(to_string(metadata_value^))
        for prior in result:
            if prior.id == id: raise Error("duplicate_value at " + path + "/id: duplicate runtime id")
        result.append(RuntimeRef(id, uri, metadata)^)
        i += 1
    return result^


def _types(text: String, path: String) raises -> List[String]:
    var root = _parse(text, path)
    return _strict_strings(root^, path)


def validate_runtime_pool(pool: RuntimePool) -> RuntimePolicyError:
    if pool.id == "": return _fail("invalid_pool", "/id", "pool id must not be empty")
    try:
        var refs = parse_runtime_refs_json(pool.runtimes)
        _ = refs
        var types = _types(pool.impulse_types, "/impulse_types")
        _ = types
        var metadata = _parse(pool.metadata, "/metadata")
        if not metadata.is_object(): return _fail("invalid_type", "/metadata", "expected object")
    except err:
        return _fail("invalid_pool", "/", String(err))
    return _ok()


def validate_delegation_policy(policy: DelegationPolicy) -> RuntimePolicyError:
    if policy.id == "": return _fail("invalid_policy", "/id", "policy id must not be empty")
    if policy.pool_id == "": return _fail("invalid_policy", "/pool_id", "pool id must not be empty")
    if not policy.budget.is_valid(): return _fail("invalid_budget", "/budget", "budget values must be non-negative")
    try:
        var types = _types(policy.impulse_types, "/impulse_types")
        _ = types
        var metadata = _parse(policy.metadata, "/metadata")
        if not metadata.is_object(): return _fail("invalid_type", "/metadata", "expected object")
    except err:
        return _fail("invalid_policy", "/", String(err))
    return _ok()


def _contains(values: List[String], value: String) -> Bool:
    for item in values:
        if item == value: return True
    return False


def _allowed(pool: List[String], policy: List[String], impulse_type: String) -> Bool:
    return (_len(pool) == 0 or _contains(pool, impulse_type)) and (_len(policy) == 0 or _contains(policy, impulse_type))


def _len(values: List[String]) -> Int:
    return len(values)


def _load(runtime: RuntimeRef) raises -> Float64:
    var metadata = _parse(runtime.metadata, "/runtimes/metadata")
    if not metadata.is_object(): raise Error("invalid_type at /runtimes/metadata: expected object")
    var key = String("")
    if "load" in metadata.object():
        # Explicit load metadata always takes precedence over fallback values.
        key = "load"
    elif "pending_processes" in metadata.object():
        key = "pending_processes"
    else:
        return 0.0
    var value = metadata.object()[key].copy()
    return _coerce_number(value^, "/runtimes/metadata/" + key)


def _coerce_number(value: Value, path: String) raises -> Float64:
    if value.is_float(): return value.float()
    if value.is_int(): return Float64(value.int())
    if value.is_uint(): return Float64(value.uint())
    if value.is_string():
        var parsed = _parse(value.string(), path)
        return _coerce_number(parsed^, path)
    raise Error("invalid_type at " + path + ": expected number")




def select_runtime(pool: RuntimePool, impulse_type: String, policy: String = "", manual_runtime_id: String = "", round_robin_index: Int = -1) raises -> RuntimeSelection:
    """Select deterministically; no adapter or transport inference is performed."""
    _error(validate_runtime_pool(pool))
    if impulse_type == "": raise Error("invalid_impulse_type at /impulse_type: must not be empty")
    var pool_types = _types(pool.impulse_types, "/impulse_types")
    var policy_types = List[String]()
    if policy != "":
        # The strategy argument is not an impulse allow-list.
        policy_types = List[String]()
    var metadata = _parse(pool.metadata, "/metadata")
    var chosen_policy = policy.copy()
    if chosen_policy == "" and metadata.is_object() and "policy" in metadata.object():
        chosen_policy = _string(metadata.object()["policy"].copy(), "/metadata/policy")
    if chosen_policy == "": chosen_policy = "first"
    if chosen_policy != "first" and chosen_policy != "manual" and chosen_policy != "least_busy" and chosen_policy != "round_robin":
        raise Error("unknown_policy at /metadata/policy: unsupported selection policy")
    var refs = parse_runtime_refs_json(pool.runtimes)
    if len(refs) == 0: raise Error("no_runtime_targets at /runtimes: pool has no runtimes")
    if not _allowed(pool_types, policy_types, impulse_type):
        raise Error("impulse_type_not_accepted at /impulse_type: pool does not accept " + impulse_type)
    var selected = 0
    if chosen_policy == "manual":
        if manual_runtime_id == "":
            selected = 0
        else:
            selected = -1
            for i in range(len(refs)):
                if refs[i].id == manual_runtime_id: selected = i
            if selected < 0: raise Error("unknown_runtime at /manual_runtime_id: target is not in pool")
    elif chosen_policy == "least_busy":
        var best = _load(refs[0])
        for i in range(1, len(refs)):
            var load = _load(refs[i])
            if load < best:
                best = load; selected = i
    elif chosen_policy == "round_robin":
        var index = round_robin_index
        # -1 is the omitted-argument sentinel; other negative indexes use Python-style modulo.
        if index == -1:
            if metadata.is_object() and "round_robin_index" in metadata.object():
                index = _integer(metadata.object()["round_robin_index"].copy(), "/metadata/round_robin_index")
            else:
                index = 0
        selected = index % len(refs)
        if selected < 0: selected += len(refs)
    var next_index = (selected + 1) % len(refs)
    return RuntimeSelection(refs[selected].copy(), selected, next_index, chosen_policy)

def resolve_delegation_policy(pool: RuntimePool, policy: DelegationPolicy, impulse_type: String, request_budget: RuntimeBudget = RuntimeBudget(), manual_runtime_id: String = "", round_robin_index: Int = -1) raises -> RuntimeSelection:
    """Validate policy, enforce both impulse allow-lists, and choose a target."""
    _error(validate_delegation_policy(policy))
    if policy.pool_id != pool.id:
        raise Error("pool_mismatch at /pool_id: policy references a different pool")
    var allowed = _types(policy.impulse_types, "/impulse_types")
    if len(allowed) > 0 and not _contains(allowed, impulse_type):
        raise Error("impulse_type_not_accepted at /impulse_type: policy does not accept " + impulse_type)
    var selected = select_runtime(pool, impulse_type, policy="", manual_runtime_id=manual_runtime_id, round_robin_index=round_robin_index)
    var merged = merge_runtime_budgets(policy.budget, request_budget)
    if not merged.allows(impulse_count=1, runtime_hops=1):
        raise Error("budget_exhausted at /budget: delegation request exceeds budget")
    return selected^ 


@fieldwise_init
struct _BudgetLimit(Copyable, Movable):
    var value: Int
    var limited: Bool

def _merge_limit(a: Int, alimited: Bool, b: Int, blimited: Bool) -> _BudgetLimit:
    if alimited and blimited:
        return _BudgetLimit(a if a < b else b, True)
    if alimited: return _BudgetLimit(a, True)
    if blimited: return _BudgetLimit(b, True)
    return _BudgetLimit(0, False)


def merge_runtime_budgets(policy: RuntimeBudget, request: RuntimeBudget) -> RuntimeBudget:
    var a = _merge_limit(policy.runtime_hops, policy.runtime_hops_limited, request.runtime_hops, request.runtime_hops_limited)
    var b = _merge_limit(policy.spawned_runs, policy.spawned_runs_limited, request.spawned_runs, request.spawned_runs_limited)
    var c = _merge_limit(policy.impulse_count, policy.impulse_count_limited, request.impulse_count, request.impulse_count_limited)
    var d = _merge_limit(policy.wall_time_seconds, policy.wall_time_seconds_limited, request.wall_time_seconds, request.wall_time_seconds_limited)
    var e = _merge_limit(policy.attempts, policy.attempts_limited, request.attempts, request.attempts_limited)
    var f = _merge_limit(policy.reaction_bytes, policy.reaction_bytes_limited, request.reaction_bytes, request.reaction_bytes_limited)
    return RuntimeBudget(a.value, b.value, c.value, d.value, e.value, f.value, a.limited, b.limited, c.limited, d.limited, e.limited, f.limited)


def budget_allows_request(budget: RuntimeBudget, runtime_hops: Int = 0, spawned_runs: Int = 0, impulse_count: Int = 0, wall_time_seconds: Int = 0, attempts: Int = 0, reaction_bytes: Int = 0) -> Bool:
    return budget.allows(runtime_hops, spawned_runs, impulse_count, wall_time_seconds, attempts, reaction_bytes)

def parse_runtime_budget_json(json_text: String) raises -> RuntimeBudget:
    var root = _parse(json_text, "/budget")
    if not root.is_object(): raise Error("invalid_type at /budget: expected object")
    for pair in root.object().items():
        if pair.key != "runtime_hops" and pair.key != "spawned_runs" and pair.key != "impulse_count" and pair.key != "wall_time_seconds" and pair.key != "attempts" and pair.key != "reaction_bytes":
            raise Error("unknown_field at /budget/" + pair.key + ": unknown field")
    var values = List[Int](length=6, fill=0); var limited = List[Bool](length=6, fill=False)
    var names = ["runtime_hops", "spawned_runs", "impulse_count", "wall_time_seconds", "attempts", "reaction_bytes"]
    for i in range(6):
        if names[i] in root.object():
            var value = root.object()[names[i]].copy()
            if value.is_null(): continue
            var number = _integer(value^, "/budget/" + names[i])
            if number < 0: raise Error("invalid_value at /budget/" + names[i] + ": must be non-negative")
            values[i] = number; limited[i] = True
    return RuntimeBudget(values[0], values[1], values[2], values[3], values[4], values[5], limited[0], limited[1], limited[2], limited[3], limited[4], limited[5])


def create_delegation_envelope(delivery_id: String, target_run_id: String, pool_id: String, budget: RuntimeBudget, metadata_json: String = "{}") raises -> String:
    if delivery_id == "": raise Error("missing_field at /delivery_id: required field is missing")
    if target_run_id == "": raise Error("missing_field at /target_run_id: required field is missing")
    if pool_id == "": raise Error("missing_field at /pool_id: required field is missing")
    if not budget.is_valid(): raise Error("invalid_budget at /budget: budget values must be non-negative")
    var metadata = _parse(metadata_json, "/metadata")
    if not metadata.is_object(): raise Error("invalid_type at /metadata: expected object")
    return canonical_json_text("{\"delivery_id\":" + _quote(delivery_id) + ",\"target_run_id\":" + _quote(target_run_id) + ",\"pool_id\":" + _quote(pool_id) + ",\"budget\":" + budget.to_json() + ",\"metadata\":" + metadata_json + "}")


def extract_delegation_envelope(json_text: String) raises -> DelegationEnvelope:
    var root = _parse(json_text, "/")
    if not root.is_object(): raise Error("invalid_type at /: expected object")
    for pair in root.object().items():
        if pair.key != "delivery_id" and pair.key != "target_run_id" and pair.key != "pool_id" and pair.key != "budget" and pair.key != "metadata":
            raise Error("unknown_field at /" + pair.key + ": unknown field")
    for key in ["delivery_id", "target_run_id", "pool_id", "budget", "metadata"]:
        if key not in root.object(): raise Error("missing_field at /" + key + ": required field is missing")
    var delivery = _string(root.object()["delivery_id"].copy(), "/delivery_id")
    var target = _string(root.object()["target_run_id"].copy(), "/target_run_id")
    var pool = _string(root.object()["pool_id"].copy(), "/pool_id")
    if delivery == "" or target == "" or pool == "": raise Error("invalid_value at /: envelope identifiers must not be empty")
    var budget = parse_runtime_budget_json(to_string(root.object()["budget"].copy()))
    var metadata_value = root.object()["metadata"].copy()
    if not metadata_value.is_object(): raise Error("invalid_type at /metadata: expected object")
    return DelegationEnvelope(delivery, target, pool, budget^, canonical_json_text(to_string(metadata_value^)))


def _quote(value: String) -> String:
    var escaped = value.replace("\\", "\\\\").replace("\"", "\\\"")
    return "\"" + escaped + "\""
