"""Runtime budget helpers for bridge envelopes (not fleet/pool selection).

Fleet RuntimePool selection was removed from Fala product surface.
"""

from std.collections import List
from emberjson import Value, to_string
from fala.json import canonical_json_text
from fala.domain import RuntimeBudget


def _parse(text: String, path: String) raises -> Value:
    try:
        return Value(parse_string=text)
    except err:
        raise Error("invalid_json at " + path + ": malformed JSON")


def _integer(value: Value, path: String) raises -> Int:
    if value.is_string():
        var parsed = _parse(value.string(), path)
        return _integer(parsed^, path)
    if value.is_int():
        return Int(value.int())
    if value.is_uint():
        return Int(value.uint())
    raise Error("invalid_type at " + path + ": expected integer")


@fieldwise_init
struct _BudgetLimit(Copyable, Movable):
    var value: Int
    var limited: Bool


def _merge_limit(a: Int, alimited: Bool, b: Int, blimited: Bool) -> _BudgetLimit:
    if alimited and blimited:
        return _BudgetLimit(a if a < b else b, True)
    if alimited:
        return _BudgetLimit(a, True)
    if blimited:
        return _BudgetLimit(b, True)
    return _BudgetLimit(0, False)


def merge_runtime_budgets(policy: RuntimeBudget, request: RuntimeBudget) -> RuntimeBudget:
    var a = _merge_limit(
        policy.runtime_hops,
        policy.runtime_hops_limited,
        request.runtime_hops,
        request.runtime_hops_limited,
    )
    var b = _merge_limit(
        policy.spawned_runs,
        policy.spawned_runs_limited,
        request.spawned_runs,
        request.spawned_runs_limited,
    )
    var c = _merge_limit(
        policy.impulse_count,
        policy.impulse_count_limited,
        request.impulse_count,
        request.impulse_count_limited,
    )
    var d = _merge_limit(
        policy.wall_time_seconds,
        policy.wall_time_seconds_limited,
        request.wall_time_seconds,
        request.wall_time_seconds_limited,
    )
    var e = _merge_limit(
        policy.attempts,
        policy.attempts_limited,
        request.attempts,
        request.attempts_limited,
    )
    var f = _merge_limit(
        policy.reaction_bytes,
        policy.reaction_bytes_limited,
        request.reaction_bytes,
        request.reaction_bytes_limited,
    )
    return RuntimeBudget(
        a.value,
        b.value,
        c.value,
        d.value,
        e.value,
        f.value,
        a.limited,
        b.limited,
        c.limited,
        d.limited,
        e.limited,
        f.limited,
    )


def budget_allows_request(
    budget: RuntimeBudget,
    runtime_hops: Int = 0,
    spawned_runs: Int = 0,
    impulse_count: Int = 0,
    wall_time_seconds: Int = 0,
    attempts: Int = 0,
    reaction_bytes: Int = 0,
) -> Bool:
    return budget.allows(
        runtime_hops,
        spawned_runs,
        impulse_count,
        wall_time_seconds,
        attempts,
        reaction_bytes,
    )


def parse_runtime_budget_json(json_text: String) raises -> RuntimeBudget:
    var root = _parse(json_text, "/budget")
    if not root.is_object():
        raise Error("invalid_type at /budget: expected object")
    for pair in root.object().items():
        if (
            pair.key != "runtime_hops"
            and pair.key != "spawned_runs"
            and pair.key != "impulse_count"
            and pair.key != "wall_time_seconds"
            and pair.key != "attempts"
            and pair.key != "reaction_bytes"
        ):
            raise Error("unknown_field at /budget/" + pair.key + ": unknown field")
    var values = List[Int](length=6, fill=0)
    var limited = List[Bool](length=6, fill=False)
    var names = [
        "runtime_hops",
        "spawned_runs",
        "impulse_count",
        "wall_time_seconds",
        "attempts",
        "reaction_bytes",
    ]
    for i in range(6):
        if names[i] in root.object():
            var value = root.object()[names[i]].copy()
            if value.is_null():
                continue
            var number = _integer(value^, "/budget/" + names[i])
            if number < 0:
                raise Error(
                    "invalid_value at /budget/" + names[i] + ": must be non-negative"
                )
            values[i] = number
            limited[i] = True
    return RuntimeBudget(
        values[0],
        values[1],
        values[2],
        values[3],
        values[4],
        values[5],
        limited[0],
        limited[1],
        limited[2],
        limited[3],
        limited[4],
        limited[5],
    )
