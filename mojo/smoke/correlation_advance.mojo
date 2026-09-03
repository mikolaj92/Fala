from std.os import remove
from std.collections import List

from fala.correlation import CorrelationEffectorSpec, CorrelationInstantiationPlan, CorrelationPathSpec, CorrelationExecutionState, instantiate_correlation_path, advance_correlation_states
from fala.correlation_advance import advance_correlation
from fala.correlation_persistence import persist_correlation_plan
from fala.journal import NativeJournal
from fala.native_driver import drive_correlation_once, drive_correlation_until_idle, diagnose_waits, transition_homeostat_terminal, reopen_homeostat
from fala.adapters import AdapterSpec, NativeFunctionRegistry


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error(message)


def _one(value: String) -> List[String]:
    var values = List[String]()
    values.append(value)
    return values^
def _retry_once(input_json: String, config_json: String) raises -> String:
    return "{\"value\":1,\"reactions\":[{\"kind\":\"accepted\"}]}"
def _plan(run_id: String, max_attempts: Int = 1, union_schema: Bool = False) raises -> CorrelationInstantiationPlan:
    var root_schema = "{\"properties\":{\"value\":{}}}"
    if union_schema:
        root_schema = "{\"type\":[\"object\",\"null\"],\"properties\":{\"value\":{}}}"
    var root = CorrelationEffectorSpec.create("root", "source", output_schema_json=root_schema)
    var leaf = CorrelationEffectorSpec.create("leaf", "sink", _one("root"), accepted_reaction_kinds=_one("accepted"))
    var effectors = List[CorrelationEffectorSpec]()
    effectors.append(root^)
    effectors.append(leaf^)
    return instantiate_correlation_path(CorrelationPathSpec("chain", effectors^, accumulate_upstream_reactions=True), run_id, max_attempts=max_attempts)



def main() raises:
    var journal = NativeJournal(":memory:\0")
    journal.initialize()
    _ = journal.create_run("advance-ok", "created", "{}", "2026-01-01T00:00:00Z")
    var plan = _plan("advance-ok", union_schema=True)
    _ = persist_correlation_plan(journal, plan, "2026-01-01T00:00:00Z")
    var root = journal.claim_process("advance-ok", "advance-ok:chain:root", "smoke", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    _ = journal.complete_process(root.run_id, root.id, "smoke", "2026-01-01T00:00:02Z", "{\"adapter\":{\"returncode\":0},\"value\":1,\"noise\":2,\"reactions\":[{\"kind\":\"accepted\"},{\"kind\":\"rejected\"}]}")
    var advanced = advance_correlation(journal, plan)
    _check(advanced.rows[0].status == "succeeded" and advanced.rows[1].status == "ready", "type-array OR schema accepts projected object during conduction advancement")
    _check(advanced.rows[1].status == "ready", "dependent becomes ready")
    _check(advanced.rows[1].input_json == "{\"conduction\":{\"root\":{\"value\":1}},\"upstream_reactions\":[{\"kind\":\"accepted\"}]}", "conduction input persisted")
    var replay = advance_correlation(journal, plan)
    _check(replay.rows[0].status == "succeeded" and replay.rows[1].status == "ready", "replay does not regress terminal states")

    var regulation_journal = NativeJournal(":memory:\0")
    regulation_journal.initialize()
    _ = regulation_journal.create_run("advance-regulation", "created", "{}", "2026-01-01T00:00:00Z")
    var regulated_effectors = List[CorrelationEffectorSpec]()
    var regulated_root = CorrelationEffectorSpec.create("root", "source", regulation_json="{\"signal\":\"upstream\",\"shared\":1}")
    var regulated_leaf = CorrelationEffectorSpec.create("leaf", "sink", _one("root"), regulation_json="{\"signal\":\"marker\",\"marker\":true}")
    regulated_effectors.append(regulated_root^)
    regulated_effectors.append(regulated_leaf^)
    var regulation_plan = instantiate_correlation_path(CorrelationPathSpec("regulation", regulated_effectors^), "advance-regulation")
    _ = persist_correlation_plan(regulation_journal, regulation_plan, "2026-01-01T00:00:00Z")
    var regulation_root = regulation_journal.claim_process("advance-regulation", "advance-regulation:regulation:root", "smoke", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    _ = regulation_journal.complete_process(regulation_root.run_id, regulation_root.id, "smoke", "2026-01-01T00:00:02Z", "{\"value\":1}")
    var regulated = advance_correlation(regulation_journal, regulation_plan)
    _check(regulated.rows[1].input_json == "{\"conduction\":{\"root\":{\"value\":1}},\"regulation\":{\"marker\":true,\"shared\":1,\"signal\":\"marker\"}}", "regulation marker wins deterministically")

    var neutral_journal = NativeJournal(":memory:\0")
    neutral_journal.initialize()
    _ = neutral_journal.create_run("advance-neutral", "created", "{}", "2026-01-01T00:00:00Z")
    var neutral_effectors = List[CorrelationEffectorSpec]()
    var neutral_root = CorrelationEffectorSpec.create("root", "source")
    var neutral_leaf = CorrelationEffectorSpec.create("leaf", "sink", _one("root"))
    neutral_effectors.append(neutral_root^)
    neutral_effectors.append(neutral_leaf^)
    var neutral_plan = instantiate_correlation_path(CorrelationPathSpec("neutral", neutral_effectors^), "advance-neutral")
    _ = persist_correlation_plan(neutral_journal, neutral_plan, "2026-01-01T00:00:00Z")
    var neutral_row = neutral_journal.claim_process("advance-neutral", "advance-neutral:neutral:root", "smoke", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    _ = neutral_journal.complete_process(neutral_row.run_id, neutral_row.id, "smoke", "2026-01-01T00:00:02Z", "{\"value\":1}")
    var neutral = advance_correlation(neutral_journal, neutral_plan)
    _check(neutral.reaction.code == "" and neutral.reaction.replay_safe, "reaction marker stays neutral when accumulation is off")
 

    var transitive_journal = NativeJournal(":memory:\0")
    transitive_journal.initialize()
    var transitive = List[CorrelationEffectorSpec]()
    var source = CorrelationEffectorSpec.create("source", "source")
    var middle = CorrelationEffectorSpec.create("middle", "middle", _one("source"))
    var terminal = CorrelationEffectorSpec.create("terminal", "terminal", _one("middle"), accepted_reaction_kinds=_one("accepted"))
    transitive.append(source^)
    transitive.append(middle^)
    transitive.append(terminal^)
    var transitive_plan = instantiate_correlation_path(CorrelationPathSpec("transitive", transitive^, accumulate_upstream_reactions=True), "advance-transitive")
    _ = transitive_journal.create_run("advance-transitive", "created", "{}", "2026-01-01T00:00:00Z")
    _ = persist_correlation_plan(transitive_journal, transitive_plan, "2026-01-01T00:00:00Z")
    var source_row = transitive_journal.claim_process("advance-transitive", "advance-transitive:transitive:source", "smoke", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    _ = transitive_journal.complete_process(source_row.run_id, source_row.id, "smoke", "2026-01-01T00:00:02Z", "{\"value\":1,\"reactions\":[{\"kind\":\"accepted\"}]}")
    var middle_ready = advance_correlation(transitive_journal, transitive_plan)
    _check(middle_ready.rows[1].status == "ready", "transitive middle becomes ready")
    var middle_row = transitive_journal.claim_process("advance-transitive", "advance-transitive:transitive:middle", "smoke", "2026-01-01T00:00:03Z", "2026-01-01T00:10:00Z")
    _ = transitive_journal.complete_process(middle_row.run_id, middle_row.id, "smoke", "2026-01-01T00:00:04Z", "{\"value\":2,\"reactions\":[{\"kind\":\"accepted\"},{\"kind\":\"rejected\"}]}")
    var terminal_ready = advance_correlation(transitive_journal, transitive_plan)
    _check(terminal_ready.rows[2].input_json == "{\"conduction\":{\"middle\":{\"reactions\":[{\"kind\":\"accepted\"},{\"kind\":\"rejected\"}],\"value\":2}},\"upstream_reactions\":[{\"kind\":\"accepted\"},{\"kind\":\"accepted\"}]}", "transitive reactions accumulate in post-order")

    var diamond_journal = NativeJournal(":memory:\0")
    diamond_journal.initialize()
    var diamond = List[CorrelationEffectorSpec]()
    var diamond_source = CorrelationEffectorSpec.create("source", "source")
    var left = CorrelationEffectorSpec.create("left", "left", _one("source"))
    var right = CorrelationEffectorSpec.create("right", "right", _one("source"))
    var terminal_conduction = List[String]()
    terminal_conduction.append("left")
    terminal_conduction.append("right")
    var terminal_accepted = _one("accepted")
    var diamond_terminal = CorrelationEffectorSpec.create("terminal", "terminal", terminal_conduction^, accepted_reaction_kinds=terminal_accepted^)
    diamond.append(diamond_source^)
    diamond.append(left^)
    diamond.append(right^)
    diamond.append(diamond_terminal^)
    var diamond_plan = instantiate_correlation_path(CorrelationPathSpec("diamond", diamond^, accumulate_upstream_reactions=True), "advance-diamond")
    _ = diamond_journal.create_run("advance-diamond", "created", "{}", "2026-01-01T00:00:00Z")
    _ = persist_correlation_plan(diamond_journal, diamond_plan, "2026-01-01T00:00:00Z")
    var diamond_source_row = diamond_journal.claim_process("advance-diamond", "advance-diamond:diamond:source", "smoke", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    _ = diamond_journal.complete_process(diamond_source_row.run_id, diamond_source_row.id, "smoke", "2026-01-01T00:00:02Z", "{\"value\":1,\"reactions\":[{\"kind\":\"accepted\"}]}")
    var diamond_middle = advance_correlation(diamond_journal, diamond_plan)
    _check(diamond_middle.rows[1].status == "ready" and diamond_middle.rows[2].status == "ready", "diamond branches become ready")
    var left_row = diamond_journal.claim_process("advance-diamond", "advance-diamond:diamond:left", "smoke", "2026-01-01T00:00:03Z", "2026-01-01T00:10:00Z")
    _ = diamond_journal.complete_process(left_row.run_id, left_row.id, "smoke", "2026-01-01T00:00:04Z", "{\"value\":2,\"reactions\":[{\"kind\":\"accepted\"},{\"kind\":\"ignored\"}]}")
    var right_row = diamond_journal.claim_process("advance-diamond", "advance-diamond:diamond:right", "smoke", "2026-01-01T00:00:05Z", "2026-01-01T00:10:00Z")
    _ = diamond_journal.complete_process(right_row.run_id, right_row.id, "smoke", "2026-01-01T00:00:06Z", "{\"value\":3,\"reactions\":[{\"kind\":\"accepted\"}]}")
    var diamond_terminal_ready = advance_correlation(diamond_journal, diamond_plan)
    _check(diamond_terminal_ready.rows[3].status == "ready", "diamond terminal becomes ready")
    _check(diamond_terminal_ready.rows[3].input_json == "{\"conduction\":{\"left\":{\"reactions\":[{\"kind\":\"accepted\"},{\"kind\":\"ignored\"}],\"value\":2},\"right\":{\"reactions\":[{\"kind\":\"accepted\"}],\"value\":3}},\"upstream_reactions\":[{\"kind\":\"accepted\"},{\"kind\":\"accepted\"},{\"kind\":\"accepted\"}]}" , "diamond post-order reaction accumulation")

    var blocked_journal = NativeJournal(":memory:\0")
    blocked_journal.initialize()
    _ = blocked_journal.create_run("advance-fail", "created", "{}", "2026-01-01T00:00:00Z")
    var failed_plan = _plan("advance-fail")
    _ = persist_correlation_plan(blocked_journal, failed_plan, "2026-01-01T00:00:00Z")
    var failed_root = blocked_journal.claim_process("advance-fail", "advance-fail:chain:root", "smoke", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    _ = blocked_journal.fail_process(failed_root.run_id, failed_root.id, "smoke", "2026-01-01T00:00:02Z", "{\"code\":\"boom\"}")
    var dead = advance_correlation(blocked_journal, failed_plan)
    _check(len(dead.readied) == 1 and len(dead.cancelled) == 0, "failed upstream conducts to dependency")
    _check(dead.rows[1].status == "ready", "dead upstream conducts rather than canceling")
    _check(dead.rows[1].input_json.find("\"code\":\"boom\"") >= 0, "failed upstream error is conducted")

    # A failed root readies only the next terminal-peer layer; deeper nodes wait.
    var cascade_journal = NativeJournal(":memory:\0")
    cascade_journal.initialize()
    _ = cascade_journal.create_run("advance-cascade", "created", "{}", "2026-01-01T00:00:00Z")
    var cascade_effectors = List[CorrelationEffectorSpec]()
    cascade_effectors.append(CorrelationEffectorSpec.create("root", "source").copy())
    cascade_effectors.append(CorrelationEffectorSpec.create("middle", "middle", _one("root")).copy())
    cascade_effectors.append(CorrelationEffectorSpec.create("leaf", "leaf", _one("middle")).copy())
    var cascade_plan = instantiate_correlation_path(CorrelationPathSpec("cascade", cascade_effectors^), "advance-cascade")
    _ = persist_correlation_plan(cascade_journal, cascade_plan, "2026-01-01T00:00:00Z")
    var cascade_root = cascade_journal.claim_process("advance-cascade", "advance-cascade:cascade:root", "smoke", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    _ = cascade_journal.fail_process(cascade_root.run_id, cascade_root.id, "smoke", "2026-01-01T00:00:02Z", "{\"code\":\"boom\"}")
    var cascaded = advance_correlation(cascade_journal, cascade_plan)
    _check(len(cascaded.readied) == 1 and cascaded.rows[1].status == "ready" and cascaded.rows[2].status == "pending", "failed root conducts to middle, leaf stays pending")
    _check(cascaded.rows[1].input_json.find("\"code\":\"boom\"") >= 0, "cascade middle receives failed root payload")
    var cycle_path = "/tmp/fala-native-feedback-cycle.sqlite"
    try:
        remove(cycle_path)
    except err:
        pass
    var cycle_journal = NativeJournal.open(cycle_path)
    cycle_journal.initialize()
    _ = cycle_journal.create_run("advance-cycle", "created", "{}", "2026-01-01T00:00:00Z")
    var cycle_effectors = List[CorrelationEffectorSpec]()
    var cycle_a = CorrelationEffectorSpec.create("a", "cycle", _one("b"))
    var cycle_b = CorrelationEffectorSpec.create("b", "cycle", _one("a"))
    cycle_effectors.append(cycle_a^)
    cycle_effectors.append(cycle_b^)
    var cycle_plan = instantiate_correlation_path(CorrelationPathSpec("cycle", cycle_effectors^), "advance-cycle")
    _ = persist_correlation_plan(cycle_journal, cycle_plan, "2026-01-01T00:00:00Z")
    var cycle = advance_correlation(cycle_journal, cycle_plan)
    _check(cycle.wait_diagnostic.deadlocked and cycle.wait_diagnostic.code == "feedback_cycle_wait", "allowed cycle terminates with typed diagnosis")
    _check(len(cycle.wait_diagnostic.blocked_process_ids) == 2 and cycle.wait_diagnostic.blocked_process_ids[0] == "advance-cycle:cycle:a" and cycle.wait_diagnostic.blocked_process_ids[1] == "advance-cycle:cycle:b", "allowed cycle blocked ids are deterministic")
    var cycle_before_restart = diagnose_waits(cycle_journal, "advance-cycle")
    _check(cycle_before_restart.code == "feedback_cycle_wait" and cycle_before_restart.reason == "feedback_cycle_wait" and cycle_before_restart.deadlocked and len(cycle_before_restart.blocked_process_ids) == 2, "cycle diagnosis preserves all fields before reopen")
    cycle_journal.close()
    var reopened_cycle_journal = NativeJournal.open(cycle_path)
    reopened_cycle_journal.initialize()
    var cycle_after_restart = diagnose_waits(reopened_cycle_journal, "advance-cycle")
    _check(cycle_after_restart.code == "feedback_cycle_wait" and cycle_after_restart.reason == "feedback_cycle_wait" and cycle_after_restart.deadlocked and len(cycle_after_restart.blocked_process_ids) == 2 and cycle_after_restart.blocked_process_ids[0] == "advance-cycle:cycle:a" and cycle_after_restart.blocked_process_ids[1] == "advance-cycle:cycle:b", "cycle diagnosis survives close and reopen")
    reopened_cycle_journal.close()
    remove(cycle_path)

    var wait_path = "/tmp/fala-native-correlation-wait.sqlite"
    try:
        remove(wait_path)
    except err:
        pass
    var wait_journal = NativeJournal.open(wait_path)
    wait_journal.initialize()
    _ = wait_journal.create_run("advance-wait", "created", "{}", "2026-01-01T00:00:00Z")
    var wait_plan = _plan("advance-wait")
    _ = persist_correlation_plan(wait_journal, wait_plan, "2026-01-01T00:00:00Z")
    var wait = advance_correlation(wait_journal, wait_plan)
    _check(wait.wait_diagnostic.code == "wait_graph_unavailable" and len(wait.wait_diagnostic.blocked_process_ids) == 1 and wait.wait_diagnostic.blocked_process_ids[0] == "advance-wait:chain:leaf", "unresolved wait reports typed diagnostic")
    _check("__correlation_wait_diagnostic" in wait_journal.get_process("advance-wait", "advance-wait:chain:leaf").metadata, "unresolved wait diagnosis is durable")
    var before_restart = diagnose_waits(wait_journal, "advance-wait")
    _check(before_restart.code == "wait_graph_unavailable" and before_restart.reason == "wait_graph_unavailable" and not before_restart.deadlocked and len(before_restart.blocked_process_ids) == 1, "unresolved wait diagnosis preserves reason")
    wait_journal.close()
    var reopened_wait_journal = NativeJournal.open("/tmp/fala-native-correlation-wait.sqlite")
    reopened_wait_journal.initialize()
    var after_restart = diagnose_waits(reopened_wait_journal, "advance-wait")
    _check(after_restart.code == "wait_graph_unavailable" and after_restart.reason == "wait_graph_unavailable" and not after_restart.deadlocked and len(after_restart.blocked_process_ids) == 1 and after_restart.blocked_process_ids[0] == "advance-wait:chain:leaf", "unresolved wait diagnosis survives reopen")
    reopened_wait_journal.close()

    var invalid_journal = NativeJournal(":memory:\0")
    invalid_journal.initialize()
    _ = invalid_journal.create_run("advance-invalid", "created", "{}", "2026-01-01T00:00:00Z")
    var invalid_plan = _plan("advance-invalid")
    _ = persist_correlation_plan(invalid_journal, invalid_plan, "2026-01-01T00:00:00Z")
    var invalid_root = invalid_journal.claim_process("advance-invalid", "advance-invalid:chain:root", "smoke", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    _ = invalid_journal.complete_process(invalid_root.run_id, invalid_root.id, "smoke", "2026-01-01T00:00:02Z", "[]")
    var invalid_rejected = False
    try:
        _ = advance_correlation(invalid_journal, invalid_plan)
    except err:
        invalid_rejected = String(err).find("correlation.advance.invalid_output") >= 0
    _check(invalid_rejected, "invalid terminal output is rejected")
    var malformed_reaction_journal = NativeJournal(":memory:\0")
    malformed_reaction_journal.initialize()
    _ = malformed_reaction_journal.create_run("advance-malformed-reaction", "created", "{}", "2026-01-01T00:00:00Z")
    var malformed_reaction_plan = _plan("advance-malformed-reaction")
    _ = persist_correlation_plan(malformed_reaction_journal, malformed_reaction_plan, "2026-01-01T00:00:00Z")
    var malformed_root = malformed_reaction_journal.claim_process("advance-malformed-reaction", "advance-malformed-reaction:chain:root", "smoke", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    _ = malformed_reaction_journal.complete_process(malformed_root.run_id, malformed_root.id, "smoke", "2026-01-01T00:00:02Z", "{\"value\":1,\"reactions\":[1]}")
    var malformed_reaction_rejected = False
    try:
        _ = advance_correlation(malformed_reaction_journal, malformed_reaction_plan)
    except err:
        malformed_reaction_rejected = String(err).find("correlation.advance.invalid_reactions") >= 0
    _check(malformed_reaction_rejected, "malformed terminal reactions are rejected")
    # A succeeded row whose effector output violates its declared schema must
    # fail closed during advancement; the dependent remains pending.
    var schema_mismatch_journal = NativeJournal(":memory:\0")
    schema_mismatch_journal.initialize()
    _ = schema_mismatch_journal.create_run("advance-schema-mismatch", "created", "{}", "2026-01-01T00:00:00Z")
    var schema_mismatch_plan = _plan("advance-schema-mismatch")
    schema_mismatch_plan.processes[0].output_schema_json = "{\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"string\"}},\"required\":[\"value\"]}"
    _ = persist_correlation_plan(schema_mismatch_journal, schema_mismatch_plan, "2026-01-01T00:00:00Z")
    var schema_mismatch_row = schema_mismatch_journal.claim_process("advance-schema-mismatch", "advance-schema-mismatch:chain:root", "smoke", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    _ = schema_mismatch_row
    var force_mismatch = schema_mismatch_journal.db.query("UPDATE processes SET status='succeeded',output_json=?,finished_at=?,updated_at=? WHERE run_id=? AND id=?")
    force_mismatch.bind_text(1, "{\"value\":1}"); force_mismatch.bind_text(2, "2026-01-01T00:00:02Z"); force_mismatch.bind_text(3, "2026-01-01T00:00:02Z"); force_mismatch.bind_text(4, "advance-schema-mismatch"); force_mismatch.bind_text(5, "advance-schema-mismatch:chain:root"); _ = force_mismatch.step(); force_mismatch.close()
    var schema_mismatch_rejected = False
    try:
        _ = advance_correlation(schema_mismatch_journal, schema_mismatch_plan)
    except err:
        schema_mismatch_rejected = String(err).find("correlation.advance.invalid_output") >= 0
    _check(schema_mismatch_rejected, "schema-mismatched effector output is rejected with typed diagnostic")
    _check(schema_mismatch_journal.get_process("advance-schema-mismatch", "advance-schema-mismatch:chain:root").status == "succeeded", "invalid output is not rewritten")
    _check(schema_mismatch_journal.get_process("advance-schema-mismatch", "advance-schema-mismatch:chain:leaf").status == "pending", "schema mismatch does not silently ready dependent")
    var cycle_inflight_effectors = List[CorrelationEffectorSpec]()
    cycle_inflight_effectors.append(CorrelationEffectorSpec.create("a", "cycle", _one("b")))
    cycle_inflight_effectors.append(CorrelationEffectorSpec.create("b", "cycle", _one("a")))
    var cycle_inflight_states = List[CorrelationExecutionState]()
    cycle_inflight_states.append(CorrelationExecutionState("advance-cycle:a", "a", "running", 1, 1, "{}", "{}", "[]", _one("b")))
    cycle_inflight_states.append(CorrelationExecutionState("advance-cycle:b", "b", "pending", 0, 1, "{}", "{}", "[]", _one("a")))
    var inflight = advance_correlation_states(CorrelationPathSpec("cycle", cycle_inflight_effectors^), cycle_inflight_states)
    _check(not inflight.wait_diagnostic.deadlocked and inflight.wait_diagnostic.code == "", "in-flight feedback dependency is not deadlocked")
    var inflight_chain_effectors = List[CorrelationEffectorSpec]()
    inflight_chain_effectors.append(CorrelationEffectorSpec.create("root", "source").copy())
    inflight_chain_effectors.append(CorrelationEffectorSpec.create("leaf", "sink", _one("root")).copy())
    var inflight_chain_path = CorrelationPathSpec("inflight-chain", inflight_chain_effectors^)
    var root_running_states = List[CorrelationExecutionState]()
    root_running_states.append(CorrelationExecutionState("inflight:root", "root", "running", 1, 2, "{}", "{}", "[]", List[String]()))
    root_running_states.append(CorrelationExecutionState("inflight:leaf", "leaf", "pending", 0, 1, "{}", "{}", "[]", _one("root")))
    var root_running = advance_correlation_states(inflight_chain_path, root_running_states)
    _check(len(root_running.blocked) == 1 and not root_running.wait_diagnostic.deadlocked and root_running.wait_diagnostic.code == "", "running upstream is an ordinary wait")
    root_running_states[0].status = "retry_wait"
    var retry_wait = advance_correlation_states(inflight_chain_path, root_running_states)
    _check(len(retry_wait.blocked) == 1 and not retry_wait.wait_diagnostic.deadlocked, "retry-wait upstream is an ordinary wait")
    root_running_states[0].status = "waiting"
    var waiting_upstream = advance_correlation_states(inflight_chain_path, root_running_states)
    _check(len(waiting_upstream.blocked) == 1 and not waiting_upstream.wait_diagnostic.deadlocked, "waiting upstream is an ordinary wait")
    var mixed_cycle_effectors = List[CorrelationEffectorSpec]()
    mixed_cycle_effectors.append(CorrelationEffectorSpec.create("a", "cycle", _one("b")).copy())
    mixed_cycle_effectors.append(CorrelationEffectorSpec.create("b", "cycle", _one("a")).copy())
    var mixed_cycle_path = CorrelationPathSpec("mixed-cycle", mixed_cycle_effectors^)
    var mixed_cycle_states = List[CorrelationExecutionState]()
    mixed_cycle_states.append(CorrelationExecutionState("mixed:a", "a", "running", 1, 1, "{}", "{}", "[]", _one("b")))
    mixed_cycle_states.append(CorrelationExecutionState("mixed:b", "b", "pending", 0, 1, "{}", "{}", "[]", _one("a")))
    var mixed_cycle = advance_correlation_states(mixed_cycle_path, mixed_cycle_states)
    _check(len(mixed_cycle.blocked) == 1 and mixed_cycle.blocked[0].reason == "unmet_dependencies" and not mixed_cycle.wait_diagnostic.deadlocked, "in-flight cycle is not deadlocked")

    # Homeostat terminal transitions advance only when the caller supplies the explicit plan.
    var homeostat_journal = NativeJournal(":memory:\0")
    homeostat_journal.initialize()
    _ = homeostat_journal.create_run("advance-homeostat", "created", "{}", "2026-01-01T00:00:00Z")
    var homeostat_plan = _plan("advance-homeostat")
    _ = persist_correlation_plan(homeostat_journal, homeostat_plan, "2026-01-01T00:00:00Z")
    var homeostat_root = homeostat_journal.claim_process("advance-homeostat", "advance-homeostat:chain:root", "homeostat-worker", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    _ = homeostat_journal.park_homeostat_process("advance-homeostat", "homeostat-open", homeostat_root.id, "homeostat-worker", "2026-01-01T00:00:02Z", "{\"prompt\":\"approve\"}", "{}", "homeostat-open-once")
    var homeostat_terminal = transition_homeostat_terminal(
        homeostat_journal, homeostat_plan, "advance-homeostat", "homeostat-open", homeostat_root.id,
        "completed", "succeeded", "homeostat-worker", "2026-01-01T00:00:03Z", "{\"value\":1}", "{}", "homeostat-complete-once",
    )
    _check(homeostat_terminal.status == "succeeded" and homeostat_journal.get_process("advance-homeostat", "advance-homeostat:chain:leaf").status == "ready", "plan-aware homeostat terminal advances downstream correlation")

    var reopen_plan = _plan("advance-reopen", 2)
    var reopen_journal = NativeJournal(":memory:\0")
    reopen_journal.initialize()
    _ = reopen_journal.create_run("advance-reopen", "created", "{}", "2026-01-01T00:00:00Z")
    _ = persist_correlation_plan(reopen_journal, reopen_plan, "2026-01-01T00:00:00Z")
    var reopen_root = reopen_journal.claim_process("advance-reopen", "advance-reopen:chain:root", "homeostat-worker", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    _ = reopen_journal.park_homeostat_process("advance-reopen", "homeostat-open", reopen_root.id, "homeostat-worker", "2026-01-01T00:00:02Z", "{\"prompt\":\"approve\"}", "{}", "homeostat-open-once")
    _ = reopen_journal.transition_homeostat_process("advance-reopen", "homeostat-open", reopen_root.id, "completed", "succeeded", "homeostat-worker", "2026-01-01T00:00:03Z", "{\"value\":1}", "{}", "homeostat-complete-once")
    var before_reopen = reopen_journal.get_process("advance-reopen", "advance-reopen:chain:leaf")
    _check(before_reopen.status == "pending", "unplanned homeostat terminal does not advance correlation")
    _ = reopen_homeostat(reopen_journal, "advance-reopen", "homeostat-open", reopen_root.id, "homeostat-worker", "2026-01-01T00:00:04Z")
    _check(reopen_journal.get_process("advance-reopen", "advance-reopen:chain:leaf").status == "pending", "homeostat reopen does not advance correlation")
    # The native correlation driver retries a failed effector at the transition
    # timestamp, then advances and drives its dependent from durable state.
    var driver_journal = NativeJournal(":memory:\0")
    driver_journal.initialize()
    _ = driver_journal.create_run("advance-driver", "created", "{}", "2026-01-01T00:00:00Z")
    var driver_plan = _plan("advance-driver", 2)
    _ = persist_correlation_plan(driver_journal, driver_plan, "2026-01-01T00:00:00Z")
    var driver_registry = NativeFunctionRegistry()
    var driver_adapter = AdapterSpec.native_function("native.retry-once")
    var driver_root = driver_journal.get_process("advance-driver", "advance-driver:chain:root")
    var first_attempt = drive_correlation_once(driver_journal, driver_root, driver_adapter, "driver-worker", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z", driver_registry, driver_plan)
    _check(first_attempt.failed and first_attempt.error.code == "native_function_not_registered", "correlation driver first attempt fails")
    var retry_row = driver_journal.get_process("advance-driver", "advance-driver:chain:root")
    _check(retry_row.status == "retry_wait" and retry_row.attempt == 1 and retry_row.available_at == "2026-01-01T00:00:01Z", "correlation driver retry is immediately available")
    driver_registry.register("native.retry-once", _retry_once)
    var second_attempt = drive_correlation_once(driver_journal, retry_row, driver_adapter, "driver-worker", "2026-01-01T00:00:02Z", "2026-01-01T00:10:00Z", driver_registry, driver_plan)
    _check(second_attempt.completed, "correlation driver retry succeeds")
    var advanced_driver = driver_journal.get_process("advance-driver", "advance-driver:chain:leaf")
    _check(advanced_driver.status == "ready" and advanced_driver.input_json == "{\"conduction\":{\"root\":{\"value\":1}},\"upstream_reactions\":[{\"kind\":\"accepted\"}]}", "correlation driver advances dependent status=" + advanced_driver.status + " input=" + advanced_driver.input_json)
    var driver_processes = driver_journal.list_processes("advance-driver")
    var driver_adapters = List[AdapterSpec]()
    driver_adapters.append(driver_adapter.copy())
    driver_adapters.append(driver_adapter.copy())
    var driven = drive_correlation_until_idle(driver_journal, driver_processes, driver_adapters, "driver-worker", "2026-01-01T00:00:03Z", "2026-01-01T00:10:00Z", 4, driver_registry, driver_plan)
    _check(driven.ticks == 1 and driver_journal.get_process("advance-driver", "advance-driver:chain:leaf").status == "succeeded", "correlation driver drains downstream")
    # Fala, not a consumer dispatcher, selects one closed-set result branch.
    var conditional_journal = NativeJournal(":memory:\0")
    conditional_journal.initialize()
    _ = conditional_journal.create_run("advance-conditional", "created", "{}", "2026-01-01T00:00:00Z")
    var conditional_effectors = List[CorrelationEffectorSpec]()
    conditional_effectors.append(CorrelationEffectorSpec.create("review", "source", output_schema_json="{\"type\":\"object\",\"properties\":{\"decision\":{\"type\":\"object\",\"properties\":{\"verdict\":{\"type\":\"string\"}},\"required\":[\"verdict\"]}},\"required\":[\"decision\"]}").copy())
    conditional_effectors.append(CorrelationEffectorSpec.create("merge", "merge", _one("review"), when_json="{\"upstream\":\"review\",\"path\":\"decision.verdict\",\"equals\":\"approve\"}").copy())
    conditional_effectors.append(CorrelationEffectorSpec.create("repair", "repair", _one("review"), when_json="{\"upstream\":\"review\",\"path\":\"decision.verdict\",\"equals\":\"request_changes\"}").copy())
    var conditional_plan = instantiate_correlation_path(CorrelationPathSpec("conditional", conditional_effectors^), "advance-conditional")
    _ = persist_correlation_plan(conditional_journal, conditional_plan, "2026-01-01T00:00:00Z")
    var review_row = conditional_journal.claim_process("advance-conditional", "advance-conditional:conditional:review", "smoke", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    _ = conditional_journal.complete_process(review_row.run_id, review_row.id, "smoke", "2026-01-01T00:00:02Z", "{\"decision\":{\"verdict\":\"request_changes\"}}")
    var conditional_result = advance_correlation(conditional_journal, conditional_plan)
    _check(conditional_result.rows[1].status == "skipped" and conditional_result.rows[1].output_json.find("condition_not_met") >= 0, "nonmatching branch is durably skipped without adapter execution")
    _check(conditional_result.rows[2].status == "ready", "matching branch becomes ready")
    var conditional_replay = advance_correlation(conditional_journal, conditional_plan)
    _check(conditional_replay.rows[1].status == "skipped" and conditional_replay.rows[2].status == "ready", "conditional selection replays without changing branch")

    var missing_journal = NativeJournal(":memory:\0")
    missing_journal.initialize()
    _ = missing_journal.create_run("advance-condition-missing", "created", "{}", "2026-01-01T00:00:00Z")
    var missing_effectors = List[CorrelationEffectorSpec]()
    missing_effectors.append(CorrelationEffectorSpec.create("review", "source").copy())
    missing_effectors.append(CorrelationEffectorSpec.create("merge", "merge", _one("review"), when_json="{\"upstream\":\"review\",\"path\":\"decision.verdict\",\"equals\":\"approve\"}").copy())
    var missing_plan = instantiate_correlation_path(CorrelationPathSpec("conditional", missing_effectors^), "advance-condition-missing")
    _ = persist_correlation_plan(missing_journal, missing_plan, "2026-01-01T00:00:00Z")
    var missing_source = missing_journal.claim_process("advance-condition-missing", "advance-condition-missing:conditional:review", "smoke", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    _ = missing_journal.complete_process(missing_source.run_id, missing_source.id, "smoke", "2026-01-01T00:00:02Z", "{\"decision\":{}}")
    var missing_failed_closed = False
    try:
        _ = advance_correlation(missing_journal, missing_plan)
    except err:
        missing_failed_closed = String(err).find("condition_path_missing") >= 0
    _check(missing_failed_closed and missing_journal.get_process("advance-condition-missing", "advance-condition-missing:conditional:merge").status == "pending", "missing condition evidence fails closed without selecting a branch")

    # Skipped/failed upstream is a finished miss. A `when` child must skip,
    # not throw condition_source_not_succeeded and leave the row pending.
    var skipped_journal = NativeJournal(":memory:\0")
    skipped_journal.initialize()
    _ = skipped_journal.create_run("advance-skipped-when", "created", "{}", "2026-01-01T00:00:00Z")
    var skipped_effectors = List[CorrelationEffectorSpec]()
    skipped_effectors.append(CorrelationEffectorSpec.create("coding", "source", output_schema_json="{\"type\":\"object\",\"properties\":{\"route\":{\"type\":\"string\"}},\"required\":[\"route\"]}").copy())
    skipped_effectors.append(CorrelationEffectorSpec.create("relocalize", "relocalize", _one("coding"), when_json="{\"upstream\":\"coding\",\"path\":\"route\",\"equals\":\"implemented\"}").copy())
    skipped_effectors.append(CorrelationEffectorSpec.create("summarize", "summarize", _one("coding")).copy())
    var skipped_plan = instantiate_correlation_path(CorrelationPathSpec("delivery", skipped_effectors^), "advance-skipped-when")
    _ = persist_correlation_plan(skipped_journal, skipped_plan, "2026-01-01T00:00:00Z")
    var coding_row = skipped_journal.get_process("advance-skipped-when", "advance-skipped-when:delivery:coding")
    _ = skipped_journal.skip_process(coding_row.run_id, coding_row.id, "correlation", "2026-01-01T00:00:01Z", "{\"reason\":\"condition_not_met\"}", "process.skip:" + coding_row.id)
    var skipped_when_result = advance_correlation(skipped_journal, skipped_plan)
    _check(skipped_when_result.rows[0].status == "skipped", "skipped upstream stays skipped")
    _check(skipped_when_result.rows[1].status == "skipped" and skipped_when_result.rows[1].output_json.find("condition_not_met") >= 0, "when on skipped upstream skips the child instead of throwing")
    _check(skipped_when_result.rows[2].status == "ready", "unconditional sibling of a skipped upstream still becomes ready")
    var skipped_when_replay = advance_correlation(skipped_journal, skipped_plan)
    _check(skipped_when_replay.rows[1].status == "skipped" and skipped_when_replay.rows[2].status == "ready", "skipped-when selection replays without throwing")

    print("correlation advancement smoke ok")
