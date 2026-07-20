from fala.correlation import (
    CorrelationEffectorSpec,
    CorrelationExecutionState,
    CorrelationInputField,
    CorrelationPathSpec,
    advance_correlation_states,
    instantiate_correlation_path,
    replay_safe_advance,
    validate_correlation_input_json,
)
from fala.correlation_advance import advance_correlation
from fala.correlation_persistence import persist_correlation_plan
from fala.journal import NativeJournal
from std.collections import Dict, List
from fala.reaction_effects import accumulate_reaction_effects


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error(message)
def _list_one(value: String) -> List[String]:
    var values = List[String]()
    values.append(value)
    return values^


def _list_two(first: String, second: String) -> List[String]:
    var values = List[String]()
    values.append(first)
    values.append(second)
    return values^
def _list_four(a: String, b: String, c: String, d: String) -> List[String]:
    var values = List[String]()
    values.append(a)
    values.append(b)
    values.append(c)
    values.append(d)
    return values^


def main() raises:
    var root = CorrelationEffectorSpec.create("root", "source")
    var left = CorrelationEffectorSpec.create("left", "branch", _list_one("root"))
    var right = CorrelationEffectorSpec.create("right", "branch", _list_one("root"))
    var sink = CorrelationEffectorSpec.create("sink", "join", _list_two("left", "right"))
    var effectors = List[CorrelationEffectorSpec]()
    effectors.append(root.copy())
    var path = CorrelationPathSpec("diamond", effectors^)
    path.effectors.append(left.copy())
    path.effectors.append(right.copy())
    path.effectors.append(sink.copy())
    var plan = instantiate_correlation_path(path, "run-1")
    _check(len(plan.processes) == 4, "diamond process count")
    _check(plan.processes[0].status == "ready", "root readiness")
    _check(plan.processes[1].status == "pending", "dependent pending")
    var existing = _list_four("run-1:diamond:root", "run-1:diamond:left", "run-1:diamond:right", "run-1:diamond:sink")
    var replay = instantiate_correlation_path(path, "run-1", existing)
    _check(replay.replayed, "duplicate-safe instantiation")
    var authored = List[CorrelationInputField]()
    authored.append(CorrelationInputField(key="shared", value_json="1"))
    var left_authored = List[CorrelationInputField]()
    left_authored.append(CorrelationInputField(key="shared", value_json="2"))
    left_authored.append(CorrelationInputField(key="local", value_json="\"left\""))
    var per_inputs = Dict[String, List[CorrelationInputField]]()
    per_inputs["left"] = left_authored^
    var per_configs = Dict[String, String]()
    per_configs["left"] = "{\"override\":true}"
    var per_timeouts = Dict[String, Float64]()
    per_timeouts["left"] = 2.5
    var per_attempts = Dict[String, Int]()
    per_attempts["right"] = 3
    var inherited = instantiate_correlation_path(path, "run-2", input_fields=authored^, per_effector_inputs=per_inputs^, per_effector_configs=per_configs^, timeout_by_effector=per_timeouts^, max_attempts_by_effector=per_attempts^)
    _check(inherited.processes[0].input_json == "{\"shared\":1}", "global input inheritance")
    _check(inherited.processes[1].input_json == "{\"local\":\"left\",\"shared\":2}", "per-effector input override")
    _check(inherited.processes[1].config_json == "{\"override\":true}", "per-effector config override")
    _check(inherited.processes[1].timeout_seconds == 2.5 and inherited.processes[2].timeout_seconds == 0.0, "deterministic timeout")
    _check(inherited.processes[1].max_attempts == 1 and inherited.processes[2].max_attempts == 3, "deterministic max attempts")
    var unknown_inputs = Dict[String, List[CorrelationInputField]]()
    unknown_inputs["missing"] = List[CorrelationInputField]()
    try:
        _ = instantiate_correlation_path(path, "run-unknown", per_effector_inputs=unknown_inputs^)
        raise Error("unknown effector input accepted")
    except err:
        _ = err

    var states = List[CorrelationExecutionState]()
    states.append(CorrelationExecutionState("run-1:diamond:root", "root", "succeeded", 1, 1, "{\"value\":1}", "{}", "[]", List[String]()))
    states.append(CorrelationExecutionState("run-1:diamond:left", "left", "succeeded", 1, 1, "{\"left\":2}", "{}", "[]", _list_one("root")))
    states.append(CorrelationExecutionState("run-1:diamond:right", "right", "pending", 0, 1, "{}", "{}", "[]", _list_one("root")))
    states.append(CorrelationExecutionState("run-1:diamond:sink", "sink", "pending", 0, 1, "{}", "{}", "[]", _list_two("left", "right")))
    var advance = advance_correlation_states(path, states)
    _check(len(advance.readied) == 1 and advance.readied[0].effector_id == "right", "diamond readiness")
    _check(len(advance.blocked) == 1 and advance.blocked[0].effector_id == "sink", "join blocked")
    var replayed = replay_safe_advance(advance, advance)
    _check(replayed.replayed and len(replayed.readied) == 0, "idempotent replay")

    states[2].status = "cancelled"
    var dead = advance_correlation_states(path, states)
    _check(len(dead.cancelled) == 1 and dead.cancelled[0].reason == "dead_upstream", "dead upstream")
    var cycle_a = CorrelationEffectorSpec.create("a", "cycle", _list_one("b"))
    var cycle_b = CorrelationEffectorSpec.create("b", "cycle", _list_one("a"))
    var cycle_effectors = List[CorrelationEffectorSpec](); cycle_effectors.append(cycle_a^); cycle_effectors.append(cycle_b^)
    var cycle_path = CorrelationPathSpec("cycle", cycle_effectors^, True)
    var cycle_states = List[CorrelationExecutionState](); cycle_states.append(CorrelationExecutionState("cycle:a", "a", "pending", 0, 1, "{}", "{}", "[]", _list_one("b"))); cycle_states.append(CorrelationExecutionState("cycle:b", "b", "pending", 0, 1, "{}", "{}", "[]", _list_one("a")))
    var cycle_wait = advance_correlation_states(cycle_path, cycle_states)
    _check(len(cycle_wait.blocked) == 2 and cycle_wait.blocked[0].reason == "feedback_cycle_wait", "feedback cycle diagnosis")
    _check(cycle_wait.wait_diagnostic.deadlocked, "feedback cycle deadlock")
    _check(len(cycle_wait.wait_diagnostic.blocked_process_ids) == 2 and cycle_wait.wait_diagnostic.blocked_process_ids[0] == "cycle:a" and cycle_wait.wait_diagnostic.blocked_process_ids[1] == "cycle:b", "deterministic blocked process ids")
    var cycle_replay = replay_safe_advance(cycle_wait, cycle_wait)
    _check(cycle_replay.replayed and cycle_replay.wait_diagnostic.deadlocked and cycle_replay.wait_diagnostic.blocked_process_ids[0] == "cycle:a", "cycle diagnosis replay")
    # Durable advancement preserves the plan marker and promotes only proven dependents.
    var durable_journal = NativeJournal(":memory:\0")
    durable_journal.initialize()
    _ = durable_journal.create_run("run-durable", "created", "{}", "2026-01-01T00:00:00Z")
    var durable_plan = instantiate_correlation_path(path, "run-durable")
    var persisted = persist_correlation_plan(durable_journal, durable_plan, "2026-01-01T00:00:00Z")
    _check(len(persisted.rows) == 4 and persisted.rows[0].metadata.find("__correlation_conduction") >= 0, "durable correlation metadata")
    var durable_root = durable_journal.claim_process("run-durable", "run-durable:diamond:root", "smoke-worker", "2026-01-01T00:00:01Z", "2026-01-01T00:01:00Z")
    _ = durable_journal.complete_process(durable_root.run_id, durable_root.id, "smoke-worker", "2026-01-01T00:00:02Z", "{\"value\":1}")
    var durable_advance = advance_correlation(durable_journal, durable_plan)
    _check(len(durable_advance.readied) == 2 and durable_advance.reaction.code == "", "durable correlation readiness without fake effects")
    var durable_left = durable_journal.get_process("run-durable", "run-durable:diamond:left")
    _check(durable_left.status == "ready" and durable_left.input_json.find("\"conduction\"") >= 0, "durable conduction input")
    try:
        validate_correlation_input_json("{\"conduction\":{}}")
        raise Error("reserved input accepted")
    except err:
        _ = err
    var no_reactions = List[String](); no_reactions.append("{\"value\":1}")
    var no_effects = accumulate_reaction_effects(no_reactions^)
    _check(no_effects.status == "unavailable" and no_effects.effects_json == "[]", "missing reaction envelope is typed empty")
    print("correlation execution smoke ok")
