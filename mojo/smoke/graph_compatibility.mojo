from std.os import remove
from std.pathlib import Path
from fala.graph_compatibility import classify_graph_change, assert_resume_compatible, migrate_active_run
from fala.graph_tools import graph_fingerprint
from fala.journal import NativeJournal


def expect(value: Bool, message: String) raises:
    if not value: raise Error(message)


def main() raises:
    var old = "/tmp/fala-compat-old.json"; var additive = "/tmp/fala-compat-add.json"; var breaking = "/tmp/fala-compat-break.json"; var migration = "/tmp/fala-compat-migration.json"; var db_path = "/tmp/fala-compat-219-test.sqlite"
    Path(old).write_text("{\"id\":\"p\",\"correlation_paths\":[{\"id\":\"x\",\"effectors\":[{\"id\":\"gate\",\"adapter\":{\"kind\":\"manual_homeostat\"}}],\"terminals\":[{\"id\":\"done\",\"source_effector\":\"gate\",\"status\":\"succeeded\",\"output_schema\":{\"type\":\"object\"}}]}]}")
    Path(additive).write_text("{\"id\":\"p\",\"description\":\"metadata\",\"correlation_paths\":[{\"id\":\"x\",\"effectors\":[{\"id\":\"gate\",\"adapter\":{\"kind\":\"manual_homeostat\"}}],\"terminals\":[{\"id\":\"done\",\"source_effector\":\"gate\",\"status\":\"succeeded\",\"output_schema\":{\"type\":\"object\"}}]}]}")
    Path(breaking).write_text(Path(old).read_text().replace("\"gate\"", "\"quality\""))
    expect(classify_graph_change(old, additive).find("compatible_additive") >= 0, "description is compatible additive")
    expect(classify_graph_change(old, breaking).find("forbidden_for_active_run") >= 0, "gate removal forbidden")
    expect(assert_resume_compatible(graph_fingerprint(old), old) == "identical", "identical resume")
    var blocked = False
    try: _ = assert_resume_compatible(graph_fingerprint(old), breaking, old)
    except err: blocked = String(err).find("resume_mismatch") >= 0
    expect(blocked, "active resume fails closed")
    for candidate in [db_path + "-wal", db_path + "-shm", db_path]:
        try: remove(candidate)
        except: pass
    var old_fingerprint = graph_fingerprint(old).copy()
    var new_fingerprint = graph_fingerprint(breaking).copy()
    var journal = NativeJournal(db_path); journal.initialize()
    _ = journal.create_run("active", "waiting", "{}", "2026-01-01", package_id="p", package_version="2", package_digest=old_fingerprint, correlation_path_id="x", correlation_path_digest=old_fingerprint, runtime_version="r", backend_version="b")
    _ = journal.schedule_process("active", "old-gate", "correlation", "2026-01-01"); journal.close()
    Path(migration).write_text("{\"old_fingerprint\":\"" + old_fingerprint + "\",\"new_fingerprint\":\"" + new_fingerprint + "\",\"process_map\":{\"old-gate\":\"new-quality\"},\"terminal_map\":{\"done\":\"done\"},\"historical_graph_ref\":\"file://old-expanded.json\"}")
    expect(migrate_active_run(db_path, "active", old, breaking, migration, "2026-01-02") == "migrated", "atomic migration")
    expect(migrate_active_run(db_path, "active", old, breaking, migration, "2026-01-03") == "replayed", "idempotent migration")
    var reopened = NativeJournal(db_path); reopened.initialize()
    expect(reopened.get_process("active", "new-quality").id == "new-quality" and reopened.get_run_record("active").metadata.find("historical_graph_ref") >= 0, "state mapped and historical graph retained")
    var events = reopened.list_events("active"); var migrated = 0
    for event in events:
        if event.event_type == "graph.migrated": migrated += 1
    expect(migrated == 1, "single migration audit event")
    reopened.close(); print("graph compatibility smoke ok")
