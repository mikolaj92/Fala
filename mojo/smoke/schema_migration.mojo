from fala.sqlite import Connection
from fala.schema import migrate_schema, schema_status, SCHEMA_VERSION
from std.os import remove


def _cleanup(path: String):
    try:
        remove(path)
    except err:
        pass
    try:
        remove(path + "-wal")
    except err:
        pass
    try:
        remove(path + "-shm")
    except err:
        pass


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("schema migration smoke: " + message)


def _fresh(mut db: Connection, runtime_sql: String, row_sql: String = "") raises:
    db.execute("DROP TABLE IF EXISTS schema_migrations; DROP TABLE IF EXISTS runtime_events; DROP TABLE IF EXISTS processes")
    db.execute(runtime_sql)
    if row_sql != "": db.execute(row_sql)

def _assert_process_output_schema(mut db: Connection) raises:
    var columns = db.query("SELECT COUNT(*) FROM pragma_table_info('processes') WHERE name='output_schema_json'")
    _check(columns.step() and columns.column_int(0) == 1, "legacy processes output_schema_json column")
    columns.close()
    var value = db.query("SELECT output_schema_json FROM processes WHERE id='legacy-process'")
    _check(value.step() and value.column_text(0) == "{}", "legacy processes output schema default")
    value.close()

def _assert_migrated(mut db: Connection, label: String, expected_payload: String) raises:
    migrate_schema(db)
    var status = schema_status(db)
    _check(status.user_version == SCHEMA_VERSION, label + ": user_version")
    _check(status.migration_version == SCHEMA_VERSION, label + ": migration version")
    _check(status.runtime_events_has_process_id and status.runtime_events_has_schema_version, label + ": event columns")
    var row = db.query("SELECT payload, process_id, schema_version, actor, correlation_id FROM runtime_events WHERE id='legacy-event'")
    _check(row.step(), label + ": row exists")
    _check(row.column_text(0) == expected_payload, label + ": payload preserved")
    _check(row.column_null(1) and row.column_int(2) == 1 and row.column_null(3) and row.column_null(4), label + ": NULL/default preservation")
    row.close()
    migrate_schema(db)
    var count = db.query("SELECT COUNT(*) FROM runtime_events WHERE id='legacy-event'")
    _check(count.step() and count.column_int(0) == 1, label + ": idempotent reopen")
    count.close()

def _assert_version_rejected(mut db: Connection, label: String, expected_version: Int) raises:
    var failed = False
    try:
        migrate_schema(db)
    except:
        failed = True
    _check(failed, label + ": rejected")
    var columns = db.query("SELECT COUNT(*) FROM pragma_table_info('runtime_events') WHERE name='process_id'")
    _check(columns.step() and columns.column_int(0) == 0, label + ": rollback preserved columns")
    columns.close()
    var metadata = db.query("SELECT version FROM schema_migrations WHERE id='runtime_backend'")
    _check(metadata.step() and metadata.column_int(0) == expected_version, label + ": metadata preserved")
    metadata.close()

def main() raises:
    var minimal_sql = "CREATE TABLE runtime_events (run_id TEXT NOT NULL, sequence INTEGER NOT NULL, id TEXT PRIMARY KEY, event_type TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL)"
    var process_sql = "CREATE TABLE runtime_events (run_id TEXT NOT NULL, sequence INTEGER NOT NULL, id TEXT PRIMARY KEY, event_type TEXT NOT NULL, process_id TEXT, payload TEXT NOT NULL, created_at TEXT NOT NULL)"
    var schema_sql = "CREATE TABLE runtime_events (run_id TEXT NOT NULL, sequence INTEGER NOT NULL, id TEXT PRIMARY KEY, event_type TEXT NOT NULL, schema_version INTEGER NOT NULL DEFAULT 1, payload TEXT NOT NULL, created_at TEXT NOT NULL)"
    var row = "INSERT INTO runtime_events (run_id, sequence, id, event_type, payload, created_at) VALUES ('legacy', 1, 'legacy-event', 'run.created', '{\"kept\":true}', '2026-01-01T00:00:00Z')"
    _cleanup("/tmp/fala-native-schema-migration-minimal.sqlite")
    _cleanup("/tmp/fala-native-schema-migration-process.sqlite")
    _cleanup("/tmp/fala-native-schema-migration-schema.sqlite")
    _cleanup("/tmp/fala-native-schema-migration-process-columns.sqlite")
    _cleanup("/tmp/fala-native-schema-migration-future.sqlite")
    _cleanup("/tmp/fala-native-schema-migration-mismatch.sqlite")
    _cleanup("/tmp/fala-native-schema-migration-nonpositive.sqlite")
    _cleanup("/tmp/fala-native-schema-migration-rollback.sqlite")
    var minimal = Connection("/tmp/fala-native-schema-migration-minimal.sqlite")
    _fresh(minimal, minimal_sql, row)
    _assert_migrated(minimal, "minimal", "{\"kept\":true}")
    minimal.close()
    var process = Connection("/tmp/fala-native-schema-migration-process.sqlite")
    _fresh(process, process_sql, row)
    _assert_migrated(process, "process-only", "{\"kept\":true}")
    process.close()
    var schema = Connection("/tmp/fala-native-schema-migration-schema.sqlite")
    _fresh(schema, schema_sql, row)
    _assert_migrated(schema, "schema-only", "{\"kept\":true}")
    schema.close()
    var legacy_process_sql = "CREATE TABLE processes (run_id TEXT NOT NULL, id TEXT PRIMARY KEY, process_type TEXT NOT NULL, impulse_id TEXT, status TEXT NOT NULL, priority INTEGER NOT NULL, attempt INTEGER NOT NULL, max_attempts INTEGER NOT NULL, available_at TEXT NOT NULL, lease_owner TEXT, lease_expires_at TEXT, input_json TEXT NOT NULL, output_json TEXT NOT NULL, error_json TEXT NOT NULL, metadata TEXT NOT NULL, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, started_at TEXT, finished_at TEXT)"
    var legacy_process_row = "INSERT INTO processes (run_id,id,process_type,status,priority,attempt,max_attempts,available_at,input_json,output_json,error_json,metadata,created_at,updated_at) VALUES ('legacy','legacy-process','manual','ready',0,0,1,'now','{}','{}','{}','{}','now','now')"
    var legacy_process = Connection("/tmp/fala-native-schema-migration-process-columns.sqlite")
    _fresh(legacy_process, legacy_process_sql, legacy_process_row)
    migrate_schema(legacy_process)
    _assert_process_output_schema(legacy_process)
    legacy_process.close()
    var future = Connection("/tmp/fala-native-schema-migration-future.sqlite")
    _fresh(future, minimal_sql, row)
    future.execute("CREATE TABLE schema_migrations (id TEXT PRIMARY KEY, version INTEGER NOT NULL, name TEXT NOT NULL, applied_at TEXT NOT NULL)")
    future.execute("INSERT INTO schema_migrations VALUES ('runtime_backend', 7, 'runtime_backend', '2026-01-01T00:00:00Z'); PRAGMA user_version=7")
    _assert_version_rejected(future, "future version", 7)
    future.close()
    var mismatch = Connection("/tmp/fala-native-schema-migration-mismatch.sqlite")
    _fresh(mismatch, minimal_sql, row)
    mismatch.execute("CREATE TABLE schema_migrations (id TEXT PRIMARY KEY, version INTEGER NOT NULL, name TEXT NOT NULL, applied_at TEXT NOT NULL)")
    mismatch.execute("INSERT INTO schema_migrations VALUES ('runtime_backend', 5, 'runtime_backend', '2026-01-01T00:00:00Z'); PRAGMA user_version=4")
    _assert_version_rejected(mismatch, "mismatched version", 5)
    mismatch.close()
    var nonpositive = Connection("/tmp/fala-native-schema-migration-nonpositive.sqlite")
    _fresh(nonpositive, minimal_sql, row)
    nonpositive.execute("CREATE TABLE schema_migrations (id TEXT PRIMARY KEY, version INTEGER NOT NULL, name TEXT NOT NULL, applied_at TEXT NOT NULL)")
    nonpositive.execute("INSERT INTO schema_migrations VALUES ('runtime_backend', 0, 'runtime_backend', '2026-01-01T00:00:00Z')")
    _assert_version_rejected(nonpositive, "nonpositive version", 0)
    nonpositive.close()
    var broken = Connection("/tmp/fala-native-schema-migration-rollback.sqlite")
    _fresh(broken, minimal_sql, row)
    broken.execute("CREATE TABLE schema_migrations (id TEXT PRIMARY KEY, version INTEGER NOT NULL)")
    var failed = False
    try:
        migrate_schema(broken)
    except:
        failed = True
    _check(failed, "malformed metadata rejected")
    var unchanged = broken.query("SELECT COUNT(*) FROM pragma_table_info('runtime_events') WHERE name='process_id'")
    _check(unchanged.step() and unchanged.column_int(0) == 0, "failed migration rolled back")
    unchanged.close()
    broken.close()
    var reopen = Connection("/tmp/fala-native-schema-migration-minimal.sqlite")
    var persisted = reopen.query("SELECT payload FROM runtime_events WHERE id='legacy-event'")
    _check(persisted.step() and persisted.column_text(0) == "{\"kept\":true}", "reopen preserved data")
    persisted.close()
    var pragma = reopen.query("PRAGMA user_version")
    _check(pragma.step() and pragma.column_int(0) == 6, "PRAGMA user_version=6")
    pragma.close()
    reopen.close()
    print("schema migration matrix ok")
