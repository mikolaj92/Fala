"""Compatibility classification and atomic active-run graph migration."""

from emberjson import Value, to_string
from std.pathlib import Path
from fala.graph_tools import graph_diff, graph_fingerprint
from fala.json import canonical_json_text, quote_json_string as quote
from fala.sqlite import Connection
from fala.schema import initialize_native_schema


def classify_graph_change(before_path: String, after_path: String) raises -> String:
    var diff = graph_diff(before_path, after_path)
    var value = Value(parse_string=diff)
    var classification = String("compatible_additive")
    if value.object()["equal"].bool() and graph_fingerprint(before_path) == graph_fingerprint(after_path): classification = "identical"
    for change in value.object()["changes"].array():
        var kind = change.object()["kind"].string()
        if kind == "node_removed" or kind == "path_removed" or kind == "terminal_removed" or kind == "edges_changed" or kind == "condition_changed" or kind == "capability_changed" or kind == "adapter_changed": classification = "forbidden_for_active_run"
        elif classification != "forbidden_for_active_run" and (kind == "terminal_added" or kind == "terminal_changed" or kind == "retry_changed" or kind == "timeout_changed" or kind == "runtime_policy_changed"): classification = "requires_explicit_migration"
    return canonical_json_text("{\"classification\":" + quote(classification) + ",\"diff\":" + diff + ",\"new_fingerprint\":" + quote(graph_fingerprint(after_path)) + ",\"old_fingerprint\":" + quote(graph_fingerprint(before_path)) + "}")


def assert_resume_compatible(durable_fingerprint: String, current_path: String, before_path: String = "") raises -> String:
    var current = graph_fingerprint(current_path)
    if current == durable_fingerprint: return "identical"
    if before_path == "": raise Error("graph.resume_mismatch: durable fingerprint differs; restart or explicit migration required")
    var report = Value(parse_string=classify_graph_change(before_path, current_path))
    var classification = report.object()["classification"].string()
    if classification != "compatible_additive": raise Error("graph.resume_mismatch: " + classification)
    return classification


def migrate_active_run(db_path: String, run_id: String, old_graph_path: String, new_graph_path: String, migration_path: String, at: String) raises -> String:
    var migration = Value(parse_string=Path(migration_path).read_text())
    if not migration.is_object(): raise Error("graph.migration at /: expected object")
    for key in ["old_fingerprint", "new_fingerprint", "process_map", "terminal_map", "historical_graph_ref"]:
        if key not in migration.object(): raise Error("graph.migration at /" + key + ": required field missing")
    var old_digest = graph_fingerprint(old_graph_path); var new_digest = graph_fingerprint(new_graph_path)
    if not migration.object()["old_fingerprint"].is_string() or migration.object()["old_fingerprint"].string() != old_digest: raise Error("graph.migration at /old_fingerprint: mismatch")
    if not migration.object()["new_fingerprint"].is_string() or migration.object()["new_fingerprint"].string() != new_digest: raise Error("graph.migration at /new_fingerprint: mismatch")
    if not migration.object()["process_map"].is_object() or not migration.object()["terminal_map"].is_object() or not migration.object()["historical_graph_ref"].is_string(): raise Error("graph.migration: invalid mapping/reference")
    var db = Connection(db_path); initialize_native_schema(db)
    var run = db.query("SELECT status,package_digest,metadata FROM runs WHERE id=?"); run.bind_text(1, run_id)
    if not run.step(): run.close(); db.close(); raise Error("graph.migration: run not found")
    var status = run.column_text(0); var durable = run.column_text(1); var metadata_text = run.column_text(2); run.close()
    if status == "completed" or status == "failed" or status == "cancelled" or status == "timed_out": db.close(); raise Error("graph.migration: run is terminal")
    if durable != old_digest and durable != new_digest: db.close(); raise Error("graph.migration: durable fingerprint mismatch")
    var replay_key = "graph.migrate:" + old_digest + ":" + new_digest
    if durable == new_digest:
        var replay = db.query("SELECT count(*) FROM runtime_commands WHERE run_id=? AND idempotency_key=?"); replay.bind_text(1, run_id); replay.bind_text(2, replay_key); _ = replay.step(); var replay_count = replay.column_int(0); replay.close(); db.close()
        if replay_count == 1: return "replayed"
        raise Error("graph.migration: new fingerprint lacks migration audit")
    var count_stmt = db.query("SELECT count(*) FROM processes WHERE run_id=?"); count_stmt.bind_text(1, run_id); _ = count_stmt.step(); var process_count = count_stmt.column_int(0); count_stmt.close()
    if len(migration.object()["process_map"].object()) != process_count: db.close(); raise Error("graph.migration at /process_map: partial mapping")
    for pair in migration.object()["process_map"].object().items():
        if not pair.value.is_string() or pair.value.string() == "": db.close(); raise Error("graph.migration at /process_map/" + pair.key + ": target ID required")
        var known = db.query("SELECT count(*) FROM processes WHERE run_id=? AND id=?"); known.bind_text(1, run_id); known.bind_text(2, pair.key); _ = known.step(); var exists = known.column_int(0); known.close()
        if exists != 1: db.close(); raise Error("graph.migration at /process_map/" + pair.key + ": unknown process ID")
    var metadata = Value(parse_string=metadata_text)
    if not metadata.is_object():
        db.close(); raise Error("graph.migration: run metadata invalid")
    var metadata_object = metadata.object().copy(); metadata_object["historical_graph_ref"] = migration.object()["historical_graph_ref"].copy(); metadata_object["previous_graph_fingerprint"] = Value(old_digest)
    var payload = canonical_json_text("{\"historical_graph_ref\":" + quote(migration.object()["historical_graph_ref"].string()) + ",\"new_fingerprint\":" + quote(new_digest) + ",\"old_fingerprint\":" + quote(old_digest) + ",\"process_map\":" + to_string(migration.object()["process_map"].copy()) + ",\"terminal_map\":" + to_string(migration.object()["terminal_map"].copy()) + "}")
    var key = replay_key
    db.begin_immediate()
    try:
        var prior = db.query("SELECT payload FROM runtime_commands WHERE run_id=? AND idempotency_key=?"); prior.bind_text(1, run_id); prior.bind_text(2, key)
        if prior.step():
            if prior.column_text(0) != payload: raise Error("graph.migration: idempotency conflict")
            prior.close(); db.commit(); db.close(); return "replayed"
        prior.close()
        # Two-phase temporary IDs avoid swaps/collisions.
        for pair in migration.object()["process_map"].object().items():
            var update = db.query("UPDATE processes SET id=? WHERE run_id=? AND id=?"); update.bind_text(1, "__fala_migration__" + pair.key); update.bind_text(2, run_id); update.bind_text(3, pair.key); _ = update.step(); update.close()
        for pair in migration.object()["process_map"].object().items():
            var update = db.query("UPDATE processes SET id=? WHERE run_id=? AND id=?"); update.bind_text(1, pair.value.string()); update.bind_text(2, run_id); update.bind_text(3, "__fala_migration__" + pair.key); _ = update.step(); update.close()
        var update_run = db.query("UPDATE runs SET package_digest=?,correlation_path_digest=?,metadata=?,updated_at=? WHERE id=?"); update_run.bind_text(1,new_digest); update_run.bind_text(2,new_digest); update_run.bind_text(3,canonical_json_text(to_string(Value(metadata_object^)))); update_run.bind_text(4,at); update_run.bind_text(5,run_id); _ = update_run.step(); update_run.close()
        var command = db.query("INSERT INTO runtime_commands (run_id,id,command_type,idempotency_key,actor,payload,created_at) VALUES (?,?,?,?,?,?,?)"); command.bind_text(1,run_id); command.bind_text(2,key); command.bind_text(3,"graph.migrate"); command.bind_text(4,key); command.bind_text(5,"migration"); command.bind_text(6,payload); command.bind_text(7,at); _ = command.step(); command.close()
        var sequence = db.query("SELECT coalesce(max(sequence),0)+1 FROM runtime_events WHERE run_id=?"); sequence.bind_text(1,run_id); _ = sequence.step(); var next_sequence = sequence.column_int(0); sequence.close()
        var event = db.query("INSERT INTO runtime_events (run_id,sequence,id,event_type,schema_version,command_id,actor,payload,created_at) VALUES (?,?,?,?,?,?,?,?,?)"); event.bind_text(1,run_id); event.bind_int(2,next_sequence); event.bind_text(3,key+":event"); event.bind_text(4,"graph.migrated"); event.bind_int(5,1); event.bind_text(6,key); event.bind_text(7,"migration"); event.bind_text(8,payload); event.bind_text(9,at); _ = event.step(); event.close()
        db.commit()
    except err:
        db.rollback(); db.close(); raise err^
    db.close(); return "migrated"
