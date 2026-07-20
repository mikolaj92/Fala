from std.collections import List

from fala.correlation import CorrelationEffectorSpec, CorrelationInputField, CorrelationPathSpec, instantiate_correlation_path_plan
from fala.correlation_persistence import persist_correlation_plan, refresh_correlation_readiness
from fala.journal import NativeJournal


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error(message)


def _one(value: String) -> List[String]:
    var result = List[String]()
    result.append(value)
    return result^


def main() raises:
    var root = CorrelationEffectorSpec.create("root", "source", output_schema_json="{\"type\":\"object\",\"properties\":{\"value\":{\"type\":\"number\"}}}")
    var leaf = CorrelationEffectorSpec.create("leaf", "sink", _one("root"))
    var effectors = List[CorrelationEffectorSpec]()
    effectors.append(root^)
    effectors.append(leaf^)
    var path = CorrelationPathSpec("chain", effectors^)
    var fields = List[CorrelationInputField]()
    fields.append(CorrelationInputField(key="authored", value_json="{\"x\":1}"))
    var plan = instantiate_correlation_path_plan(path, "run-persist", input_fields=fields^)

    var journal = NativeJournal(":memory:\0")
    journal.initialize()
    _ = journal.create_run("run-persist", "created", "{}", "2026-01-01T00:00:00Z")

    var first = persist_correlation_plan(journal, plan, "2026-01-01T00:00:00Z")
    _check(len(first.rows) == 2, "first durable row count")
    _check(first.rows[0].id == "run-persist:chain:root", "declaration order root")
    _check(first.rows[0].status == "ready", "root ready")
    _check(first.rows[1].status == "pending", "dependent pending")
    _check(len(journal.list_processes("run-persist")) == 2, "first persisted count")
    _check(first.rows[0].output_schema_json != "{}", "root output schema persisted")
    var stored_root = journal.get_process("run-persist", "run-persist:chain:root")
    _check(stored_root.output_schema_json == first.rows[0].output_schema_json, "get_process output schema")

    var replay = persist_correlation_plan(journal, plan, "2026-01-01T00:00:00Z")
    _check(replay.replayed, "replay marker")
    _check(len(replay.rows) == 2, "replay row count")
    _check(len(journal.list_processes("run-persist")) == 2, "replay did not duplicate")
    _check(replay.rows[0].output_schema_json == first.rows[0].output_schema_json, "replay output schema")

    var claimed = journal.claim_process("run-persist", "run-persist:chain:root", "smoke", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    _ = journal.complete_process(claimed.run_id, claimed.id, "smoke", "2026-01-01T00:00:02Z", "{\"adapter\":{\"returncode\":0},\"value\":1,\"noise\":9}")
    var advanced = refresh_correlation_readiness(journal, plan)
    _check(advanced.rows[1].input_json == "{\"authored\":{\"x\":1},\"conduction\":{\"root\":{\"value\":1}}}", "authored and conduction input")
    _check(len(advanced.readiness.ready) == 1 and advanced.readiness.ready[0] == "leaf", "readiness projection")
    var corrupt_output = journal.db.query("UPDATE processes SET output_json=? WHERE run_id=? AND id=?")
    corrupt_output.bind_text(1, "[]")
    corrupt_output.bind_text(2, "run-persist")
    corrupt_output.bind_text(3, "run-persist:chain:root")
    _ = corrupt_output.step()
    var reset_pending = journal.db.query("UPDATE processes SET status='pending' WHERE run_id=? AND id=?")
    reset_pending.bind_text(1, "run-persist")
    reset_pending.bind_text(2, "run-persist:chain:leaf")
    _ = reset_pending.step()
    var corrupt_rejected = False
    try:
        var corrupt_result = refresh_correlation_readiness(journal, plan)
        _ = corrupt_result
    except err:
        corrupt_rejected = String(err).find("correlation.persistence.invalid_output") >= 0
    _check(corrupt_rejected, "invalid succeeded output rejected")
    _check(journal.get_process("run-persist", "run-persist:chain:leaf").status == "pending", "invalid output leaves dependent pending")
    var invalid = List[CorrelationEffectorSpec]()
    invalid.append(CorrelationEffectorSpec.create("bad", "source", output_schema_json="{\"type\":\"bogus\"}")^)
    var invalid_plan = instantiate_correlation_path_plan(CorrelationPathSpec("invalid", invalid^), "run-persist")
    var invalid_rejected = False
    try:
        var invalid_result = persist_correlation_plan(journal, invalid_plan, "2026-01-01T00:00:00Z")
        _ = invalid_result
    except err:
        invalid_rejected = String(err).find("invalid_output_schema") >= 0
    _check(invalid_rejected, "invalid output schema rejected")

    var invalid_union = List[CorrelationEffectorSpec]()
    invalid_union.append(CorrelationEffectorSpec.create("bad-union", "source", output_schema_json="{\"type\":[\"bogus\"]}")^)
    var invalid_union_plan = instantiate_correlation_path_plan(CorrelationPathSpec("invalid-union", invalid_union^), "run-persist")
    var invalid_union_rejected = False
    try:
        var invalid_union_result = persist_correlation_plan(journal, invalid_union_plan, "2026-01-01T00:00:00Z")
        _ = invalid_union_result
    except err:
        invalid_union_rejected = String(err).find("invalid_output_schema") >= 0
    _check(invalid_union_rejected, "invalid output schema union rejected")

    var empty_union = List[CorrelationEffectorSpec]()
    empty_union.append(CorrelationEffectorSpec.create("empty-union", "source", output_schema_json="{\"type\":[]}")^)
    var empty_union_plan = instantiate_correlation_path_plan(CorrelationPathSpec("empty-union", empty_union^), "run-persist")
    var empty_union_rejected = False
    try:
        var empty_union_result = persist_correlation_plan(journal, empty_union_plan, "2026-01-01T00:00:00Z")
        _ = empty_union_result
    except err:
        empty_union_rejected = String(err).find("invalid_output_schema") >= 0
    _check(empty_union_rejected, "empty output schema union rejected")

    var duplicate_union = List[CorrelationEffectorSpec]()
    duplicate_union.append(CorrelationEffectorSpec.create("duplicate-union", "source", output_schema_json="{\"type\":[\"string\",\"string\"]}")^)
    var duplicate_union_plan = instantiate_correlation_path_plan(CorrelationPathSpec("duplicate-union", duplicate_union^), "run-persist")
    var duplicate_union_rejected = False
    try:
        var duplicate_union_result = persist_correlation_plan(journal, duplicate_union_plan, "2026-01-01T00:00:00Z")
        _ = duplicate_union_result
    except err:
        duplicate_union_rejected = String(err).find("invalid_output_schema") >= 0
    _check(duplicate_union_rejected, "duplicate output schema union rejected")

    var union_journal = NativeJournal(":memory:\0")
    union_journal.initialize()
    _ = union_journal.create_run("run-union", "created", "{}", "2026-01-01T00:00:00Z")
    var union_effectors = List[CorrelationEffectorSpec]()
    union_effectors.append(CorrelationEffectorSpec.create("root", "source", output_schema_json="{\"type\":[\"string\",\"null\"]}")^)
    var union_plan = instantiate_correlation_path_plan(CorrelationPathSpec("union", union_effectors^), "run-union")
    _ = persist_correlation_plan(union_journal, union_plan, "2026-01-01T00:00:00Z")
    var union_row = union_journal.claim_process("run-union", "run-union:union:root", "smoke", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    var union_rejected = False
    try:
        _ = union_journal.complete_process(union_row.run_id, union_row.id, "smoke", "2026-01-01T00:00:02Z", "1")
    except err:
        union_rejected = String(err).find("output does not match output_schema_json") >= 0
    _check(union_rejected, "union rejects number")
    _ = union_journal.complete_process(union_row.run_id, union_row.id, "smoke", "2026-01-01T00:00:03Z", "null")
    _check(union_journal.get_process("run-union", union_row.id).status == "succeeded", "union accepts null")

    var integer_journal = NativeJournal(":memory:\0")
    integer_journal.initialize()
    _ = integer_journal.create_run("run-integer", "created", "{}", "2026-01-01T00:00:00Z")
    var integer_effectors = List[CorrelationEffectorSpec]()
    integer_effectors.append(CorrelationEffectorSpec.create("root", "source", output_schema_json="{\"type\":\"integer\"}")^)
    var integer_plan = instantiate_correlation_path_plan(CorrelationPathSpec("integer", integer_effectors^), "run-integer")
    _ = persist_correlation_plan(integer_journal, integer_plan, "2026-01-01T00:00:00Z")
    var integer_row = integer_journal.claim_process("run-integer", "run-integer:integer:root", "smoke", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    var fractional_rejected = False
    try:
        _ = integer_journal.complete_process(integer_row.run_id, integer_row.id, "smoke", "2026-01-01T00:00:02Z", "1.5")
    except err:
        fractional_rejected = String(err).find("output does not match output_schema_json") >= 0
    _check(fractional_rejected, "integer rejects fractional number")
    _ = integer_journal.complete_process(integer_row.run_id, integer_row.id, "smoke", "2026-01-01T00:00:03Z", "1.0")
    _check(integer_journal.get_process("run-integer", integer_row.id).status == "succeeded", "integer accepts integral float")

    var const_journal = NativeJournal(":memory:\0")
    const_journal.initialize()
    _ = const_journal.create_run("run-const", "created", "{}", "2026-01-01T00:00:00Z")
    var const_effectors = List[CorrelationEffectorSpec]()
    const_effectors.append(CorrelationEffectorSpec.create("root", "source", output_schema_json="{\"const\":1}")^)
    var const_plan = instantiate_correlation_path_plan(CorrelationPathSpec("const", const_effectors^), "run-const")
    _ = persist_correlation_plan(const_journal, const_plan, "2026-01-01T00:00:00Z")
    var const_row = const_journal.claim_process("run-const", "run-const:const:root", "smoke", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    _ = const_journal.complete_process(const_row.run_id, const_row.id, "smoke", "2026-01-01T00:00:02Z", "1.0")
    _check(const_journal.get_process("run-const", const_row.id).status == "succeeded", "const accepts numerically equivalent integral float")

    var enum_journal = NativeJournal(":memory:\0")
    enum_journal.initialize()
    _ = enum_journal.create_run("run-enum", "created", "{}", "2026-01-01T00:00:00Z")
    var enum_effectors = List[CorrelationEffectorSpec]()
    enum_effectors.append(CorrelationEffectorSpec.create("root", "source", output_schema_json="{\"enum\":[1,2]}")^)
    var enum_plan = instantiate_correlation_path_plan(CorrelationPathSpec("enum", enum_effectors^), "run-enum")
    _ = persist_correlation_plan(enum_journal, enum_plan, "2026-01-01T00:00:00Z")
    var enum_row = enum_journal.claim_process("run-enum", "run-enum:enum:root", "smoke", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    _ = enum_journal.complete_process(enum_row.run_id, enum_row.id, "smoke", "2026-01-01T00:00:02Z", "2.0")
    _check(enum_journal.get_process("run-enum", enum_row.id).status == "succeeded", "enum accepts numerically equivalent integral float")

    var fractional_journal = NativeJournal(":memory:\0")
    fractional_journal.initialize()
    _ = fractional_journal.create_run("run-fractional", "created", "{}", "2026-01-01T00:00:00Z")
    var fractional_effectors = List[CorrelationEffectorSpec]()
    fractional_effectors.append(CorrelationEffectorSpec.create("root", "source", output_schema_json="{\"const\":1}")^)
    var fractional_plan = instantiate_correlation_path_plan(CorrelationPathSpec("fractional", fractional_effectors^), "run-fractional")
    _ = persist_correlation_plan(fractional_journal, fractional_plan, "2026-01-01T00:00:00Z")
    var fractional_row = fractional_journal.claim_process("run-fractional", "run-fractional:fractional:root", "smoke", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    var const_fractional_rejected = False
    try:
        _ = fractional_journal.complete_process(fractional_row.run_id, fractional_row.id, "smoke", "2026-01-01T00:00:02Z", "1.5")
    except err:
        const_fractional_rejected = String(err).find("output does not match output_schema_json") >= 0
    _check(const_fractional_rejected, "const rejects fractional numeric mismatch")

    var nested_journal = NativeJournal(":memory:\0")
    nested_journal.initialize()
    _ = nested_journal.create_run("run-nested", "created", "{}", "2026-01-01T00:00:00Z")
    var nested_effectors = List[CorrelationEffectorSpec]()
    nested_effectors.append(CorrelationEffectorSpec.create("root", "source", output_schema_json="{\"const\":{\"n\":1,\"a\":[2]}}")^)
    var nested_plan = instantiate_correlation_path_plan(CorrelationPathSpec("nested", nested_effectors^), "run-nested")
    _ = persist_correlation_plan(nested_journal, nested_plan, "2026-01-01T00:00:00Z")
    var nested_row = nested_journal.claim_process("run-nested", "run-nested:nested:root", "smoke", "2026-01-01T00:00:01Z", "2026-01-01T00:10:00Z")
    _ = nested_journal.complete_process(nested_row.run_id, nested_row.id, "smoke", "2026-01-01T00:00:02Z", "{\"a\":[2.0],\"n\":1.0}")
    _check(nested_journal.get_process("run-nested", nested_row.id).status == "succeeded", "nested const compares object keys and numeric values")

    print("correlation persistence smoke ok")
