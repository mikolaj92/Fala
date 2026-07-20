"""SQLite schema-v6 definition for the native Fala runtime.

The schema is deliberately exposed as SQL text.  A native SQLite adapter can
execute :func:`initialize_schema`'s result as one script; keeping this module
adapter-free means it also works with small embedders that only accept SQL
strings.
"""

from fala.sqlite import Connection, SQLiteError
from std.collections import List

# Keep this in lock-step with src/fala/runtime_backend.py.
comptime SCHEMA_VERSION: Int = 6

# The Python backend currently creates sixteen named tables.  The migration
# table is included in this list (and in the script) so callers can inspect the
# complete schema rather than only runtime domain tables.
comptime _TABLE_NAMES: List[String] = [
    "runs",
    "schema_migrations",
    "impulses",
    "impulse_types",
    "impulse_relations",
    "runtime_commands",
    "runtime_events",
    "associations",
    "reactions",
    "processes",
    "homeostats",
    "projections",
    "bridge_outbox",
    "bridge_inbox",
    "runtime_pools",
    "delegation_policies",
]

comptime SCHEMA_SQL: String = """
PRAGMA busy_timeout = 30000;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    title TEXT,
    package_id TEXT,
    package_version TEXT,
    package_digest TEXT,
    correlation_path_id TEXT,
    correlation_path_digest TEXT,
    runtime_version TEXT,
    backend_version TEXT,
    schema_version INTEGER NOT NULL,
    metadata TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT
);

CREATE TABLE IF NOT EXISTS schema_migrations (
    id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    name TEXT NOT NULL,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS impulses (
    run_id TEXT NOT NULL,
    id TEXT NOT NULL,
    impulse_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    metadata TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, id)
);

CREATE TABLE IF NOT EXISTS impulse_types (
    run_id TEXT NOT NULL,
    id TEXT NOT NULL,
    title TEXT,
    description TEXT,
    media_types TEXT NOT NULL,
    value_schema_json TEXT NOT NULL,
    metadata TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, id)
);

CREATE TABLE IF NOT EXISTS impulse_relations (
    run_id TEXT NOT NULL,
    id TEXT NOT NULL,
    relation_type TEXT NOT NULL,
    source_impulse_id TEXT NOT NULL,
    target_impulse_id TEXT NOT NULL,
    metadata TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, id),
    FOREIGN KEY (run_id, source_impulse_id)
        REFERENCES impulses (run_id, id),
    FOREIGN KEY (run_id, target_impulse_id)
        REFERENCES impulses (run_id, id)
);

CREATE TABLE IF NOT EXISTS runtime_commands (
    run_id TEXT NOT NULL,
    id TEXT NOT NULL,
    command_type TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    actor TEXT,
    correlation_id TEXT,
    causation_id TEXT,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, id),
    UNIQUE (run_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS runtime_events (
    run_id TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    impulse_id TEXT,
    process_id TEXT,
    command_id TEXT,
    actor TEXT,
    correlation_id TEXT,
    causation_id TEXT,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, sequence),
    UNIQUE (run_id, id),
    FOREIGN KEY (run_id, command_id)
        REFERENCES runtime_commands (run_id, id)
);

CREATE TABLE IF NOT EXISTS associations (
    run_id TEXT NOT NULL,
    id TEXT NOT NULL,
    kind TEXT NOT NULL,
    impulse_id TEXT,
    values_json TEXT NOT NULL,
    metadata TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, id)
);

CREATE TABLE IF NOT EXISTS reactions (
    run_id TEXT NOT NULL,
    id TEXT NOT NULL,
    kind TEXT NOT NULL,
    uri TEXT NOT NULL,
    impulse_id TEXT,
    media_type TEXT,
    size_bytes INTEGER,
    content_hash TEXT,
    metadata TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (run_id, id),
    FOREIGN KEY (run_id, impulse_id)
        REFERENCES impulses (run_id, id)
);

CREATE TABLE IF NOT EXISTS processes (
    run_id TEXT NOT NULL,
    id TEXT NOT NULL,
    process_type TEXT NOT NULL,
    impulse_id TEXT,
    status TEXT NOT NULL,
    priority INTEGER NOT NULL,
    attempt INTEGER NOT NULL,
    max_attempts INTEGER NOT NULL,
    available_at TEXT NOT NULL,
    lease_owner TEXT,
    lease_expires_at TEXT,
    input_json TEXT NOT NULL,
    output_json TEXT NOT NULL,
    error_json TEXT NOT NULL,
    metadata TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    output_schema_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY (run_id, id),
    FOREIGN KEY (run_id, impulse_id)
        REFERENCES impulses (run_id, id)
);

CREATE TABLE IF NOT EXISTS homeostats (
    run_id TEXT NOT NULL,
    id TEXT NOT NULL,
    kind TEXT NOT NULL,
    impulse_id TEXT,
    status TEXT NOT NULL,
    values_json TEXT NOT NULL,
    metadata TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, id)
);


CREATE TABLE IF NOT EXISTS projections (
    run_id TEXT NOT NULL,
    name TEXT NOT NULL,
    id TEXT NOT NULL,
    version INTEGER NOT NULL,
    data TEXT NOT NULL,
    source_event_sequence INTEGER NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, name)
);

CREATE TABLE IF NOT EXISTS bridge_outbox (
    run_id TEXT NOT NULL,
    id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    source_ref TEXT NOT NULL,
    target_ref TEXT NOT NULL,
    impulse_json TEXT NOT NULL,
    event_ref TEXT,
    pool_id TEXT,
    budget TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    metadata TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, id),
    UNIQUE (run_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS bridge_inbox (
    run_id TEXT NOT NULL,
    id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    source_ref TEXT NOT NULL,
    target_ref TEXT NOT NULL,
    impulse_json TEXT NOT NULL,
    event_ref TEXT,
    pool_id TEXT,
    budget TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    metadata TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (run_id, id),
    UNIQUE (run_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS runtime_pools (
    id TEXT PRIMARY KEY,
    runtimes_json TEXT NOT NULL,
    impulse_types TEXT NOT NULL,
    metadata TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS delegation_policies (
    id TEXT PRIMARY KEY,
    pool_id TEXT NOT NULL,
    impulse_types TEXT NOT NULL,
    budget TEXT NOT NULL,
    metadata TEXT NOT NULL,
    FOREIGN KEY (pool_id)
        REFERENCES runtime_pools (id)
);

CREATE INDEX IF NOT EXISTS idx_runtime_events_impulse
    ON runtime_events (run_id, impulse_id, sequence);
CREATE INDEX IF NOT EXISTS idx_runtime_events_process
    ON runtime_events (run_id, process_id, sequence);
CREATE INDEX IF NOT EXISTS idx_runs_status
    ON runs (status, updated_at);
CREATE INDEX IF NOT EXISTS idx_delegation_policies_pool
    ON delegation_policies (pool_id, id);
CREATE INDEX IF NOT EXISTS idx_impulse_relations_source
    ON impulse_relations (run_id, source_impulse_id, relation_type);
CREATE INDEX IF NOT EXISTS idx_impulse_relations_target
    ON impulse_relations (run_id, target_impulse_id, relation_type);
CREATE INDEX IF NOT EXISTS idx_associations_impulse
    ON associations (run_id, impulse_id, created_at);
CREATE INDEX IF NOT EXISTS idx_reactions_impulse
    ON reactions (run_id, impulse_id, kind, created_at);
CREATE INDEX IF NOT EXISTS idx_processes_ready
    ON processes (status, available_at, priority, created_at);
CREATE INDEX IF NOT EXISTS idx_processes_run_status
    ON processes (run_id, status, updated_at);
CREATE INDEX IF NOT EXISTS idx_processes_impulse
    ON processes (run_id, impulse_id, status);
CREATE INDEX IF NOT EXISTS idx_homeostats_status
    ON homeostats (run_id, status, updated_at);
CREATE INDEX IF NOT EXISTS idx_bridge_outbox_status
    ON bridge_outbox (run_id, status, updated_at);
CREATE INDEX IF NOT EXISTS idx_bridge_inbox_status
    ON bridge_inbox (run_id, status, updated_at);

CREATE TRIGGER IF NOT EXISTS runtime_events_no_update
BEFORE UPDATE ON runtime_events
BEGIN
    SELECT RAISE(ABORT, 'runtime_events is append-only');
END;

CREATE TRIGGER IF NOT EXISTS runtime_events_no_delete
BEFORE DELETE ON runtime_events
BEGIN
    SELECT RAISE(ABORT, 'runtime_events is append-only');
END;

CREATE TRIGGER IF NOT EXISTS runtime_commands_no_update
BEFORE UPDATE ON runtime_commands
BEGIN
    SELECT RAISE(ABORT, 'runtime_commands is append-only');
END;

CREATE TRIGGER IF NOT EXISTS runtime_commands_no_delete
BEFORE DELETE ON runtime_commands
BEGIN
    SELECT RAISE(ABORT, 'runtime_commands is append-only');
END;
INSERT OR IGNORE INTO schema_migrations (id, version, name, applied_at)
VALUES ('runtime_backend', 6, 'runtime_backend', datetime('now'));
UPDATE schema_migrations SET version=6, name='runtime_backend'
WHERE id='runtime_backend';
UPDATE schema_migrations SET applied_at=datetime('now')
WHERE id='runtime_backend' AND (applied_at IS NULL OR applied_at='');
PRAGMA user_version = 6;
"""

"""Return the schema's named tables in creation order."""
def table_names() -> List[String]:
    return [
        "runs",
        "schema_migrations",
        "impulses",
        "impulse_types",
        "impulse_relations",
        "runtime_commands",
        "runtime_events",
        "associations",
        "reactions",
        "processes",
        "homeostats",
        "projections",
        "bridge_outbox",
        "bridge_inbox",
        "runtime_pools",
        "delegation_policies",
    ]

# Minimal adapter contract used by initialize_schema. Native SQLite wrappers
# can conform by exposing execute(String) raises; no Python interop is needed.
trait SQLConnection:
    def execute(mut self, sql: String) raises:
        ...

# Execute the complete schema script on any conforming SQLite connection.
def initialize_schema[T: SQLConnection](mut connection: T) raises:
    var sql = SCHEMA_SQL
    connection.execute(sql)

def _index_names() -> List[String]:
    return [
        "idx_runtime_events_impulse", "idx_runtime_events_process", "idx_runs_status",
        "idx_delegation_policies_pool", "idx_impulse_relations_source", "idx_impulse_relations_target",
        "idx_associations_impulse", "idx_reactions_impulse", "idx_processes_ready",
    ]

def _trigger_names() -> List[String]:
    return [
        "runtime_events_no_update", "runtime_events_no_delete",
        "runtime_commands_no_update", "runtime_commands_no_delete",
    ]

@fieldwise_init
struct SchemaStatus(Movable):
    """Observable schema state without hiding missing or stale migrations."""
    var current_version: Int
    var latest_version: Int
    var user_version: Int
    var migration_version: Int
    var applied_at: String
    var missing_tables: List[String]
    var missing_indices: List[String]
    var missing_triggers: List[String]
    var runtime_events_has_process_id: Bool
    var runtime_events_has_schema_version: Bool

    def is_current(self) -> Bool:
        return (
            len(self.missing_tables) == 0
            and len(self.missing_indices) == 0
            and len(self.missing_triggers) == 0
            and self.current_version == self.latest_version
            and self.user_version == self.latest_version
            and self.applied_at != ""
            and self.runtime_events_has_process_id
            and self.runtime_events_has_schema_version
        )

def _object_exists(mut connection: Connection, object_type: String, name: String) raises SQLiteError -> Bool:
    var stmt = connection.query("SELECT 1 FROM sqlite_master WHERE type=? AND name=?")
    stmt.bind_text(1, object_type)
    stmt.bind_text(2, name)
    var result = stmt.step()
    stmt.close()
    return result

def _table_exists(mut connection: Connection, name: String) raises SQLiteError -> Bool:
    return _object_exists(connection, "table", name)

def _has_runtime_event_column(mut connection: Connection, column: String) raises SQLiteError -> Bool:
    if not _table_exists(connection, "runtime_events"):
        return False
    var stmt = connection.query("PRAGMA table_info(runtime_events)")
    var found = False
    while stmt.step():
        if stmt.column_text(1) == column:
            found = True
            break
    stmt.close()
    return found
def _has_table_column(mut connection: Connection, table: String, column: String) raises SQLiteError -> Bool:
    if not _table_exists(connection, table):
        return False
    var stmt = connection.query("PRAGMA table_info(" + table + ")")
    var found = False
    while stmt.step():
        if stmt.column_text(1) == column:
            found = True
            break
    stmt.close()
    return found

def _validate_legacy_metadata(mut connection: Connection) raises SQLiteError:
    # An existing metadata table is part of the durable contract.  SQLite's
    # CREATE TABLE IF NOT EXISTS cannot repair a partial legacy definition;
    # fail before touching event columns so the surrounding transaction can
    # roll back without silently discarding migration history.
    if not _table_exists(connection, "schema_migrations"):
        return
    var required = ["id", "version", "name", "applied_at"]
    for column in required:
        if not _has_table_column(connection, "schema_migrations", column):
            raise SQLiteError(code=1, message="schema migration metadata is missing column: " + column)

def _validate_legacy_runtime_events(mut connection: Connection) raises SQLiteError:
    if not _table_exists(connection, "runtime_events"):
        return
    # These columns existed in every reference runtime_events shape.  Optional
    # v6 columns are added below; a table missing a key column is not safely
    # migratable because indexes and row identity cannot be preserved.
    var required = ["run_id", "sequence", "id", "event_type", "payload", "created_at"]
    for column in required:
        if not _has_table_column(connection, "runtime_events", column):
            raise SQLiteError(code=1, message="legacy runtime_events is missing column: " + column)

def _validate_schema_versions(mut connection: Connection) raises SQLiteError:
    var metadata_present = False
    var metadata_version = 0
    if _table_exists(connection, "schema_migrations"):
        var metadata = connection.query("SELECT version FROM schema_migrations WHERE id='runtime_backend'")
        if metadata.step():
            metadata_present = True
            metadata_version = metadata.column_int(0)
        metadata.close()
    var user_version = _user_version(connection)
    if metadata_present and metadata_version <= 0:
        raise SQLiteError(code=1, message="schema migration version must be positive")
    if metadata_present and metadata_version > SCHEMA_VERSION:
        raise SQLiteError(code=1, message="schema migration version is newer than this runtime")
    if user_version < 0 or user_version > SCHEMA_VERSION:
        raise SQLiteError(code=1, message="schema user_version is unsupported")
    if metadata_present and user_version != metadata_version:
        raise SQLiteError(code=1, message="schema migration version does not match PRAGMA user_version")
    if not metadata_present and user_version != 0:
        raise SQLiteError(code=1, message="schema user_version has no migration metadata")

def _migration_version(mut connection: Connection) raises SQLiteError -> Int:
    if not _table_exists(connection, "schema_migrations"):
        return 0
    var stmt = connection.query("SELECT version FROM schema_migrations WHERE id='runtime_backend'")
    var result = 0
    if stmt.step():
        result = stmt.column_int(0)
    stmt.close()
    return result

def _migration_applied_at(mut connection: Connection) raises SQLiteError -> String:
    if not _table_exists(connection, "schema_migrations"):
        return String("")
    var stmt = connection.query("SELECT applied_at FROM schema_migrations WHERE id='runtime_backend'")
    var result = String("")
    if stmt.step() and not stmt.column_null(0):
        result = stmt.column_text(0)
    stmt.close()
    return result

def _user_version(mut connection: Connection) raises SQLiteError -> Int:
    var stmt = connection.query("PRAGMA user_version")
    if not stmt.step():
        stmt.close()
        raise SQLiteError(code=1, message="schema: PRAGMA user_version returned no row")
    var result = stmt.column_int(0)
    stmt.close()
    return result

def schema_status(mut connection: Connection) raises SQLiteError -> SchemaStatus:
    var missing = List[String]()
    if not _table_exists(connection, "runs"): missing.append("runs")
    if not _table_exists(connection, "schema_migrations"): missing.append("schema_migrations")
    if not _table_exists(connection, "impulses"): missing.append("impulses")
    if not _table_exists(connection, "impulse_types"): missing.append("impulse_types")
    if not _table_exists(connection, "impulse_relations"): missing.append("impulse_relations")
    if not _table_exists(connection, "runtime_commands"): missing.append("runtime_commands")
    if not _table_exists(connection, "runtime_events"): missing.append("runtime_events")
    if not _table_exists(connection, "associations"): missing.append("associations")
    if not _table_exists(connection, "reactions"): missing.append("reactions")
    if not _table_exists(connection, "processes"): missing.append("processes")
    if not _table_exists(connection, "homeostats"): missing.append("homeostats")
    if not _table_exists(connection, "projections"): missing.append("projections")
    if not _table_exists(connection, "bridge_outbox"): missing.append("bridge_outbox")
    if not _table_exists(connection, "bridge_inbox"): missing.append("bridge_inbox")
    if not _table_exists(connection, "runtime_pools"): missing.append("runtime_pools")
    if not _table_exists(connection, "delegation_policies"): missing.append("delegation_policies")
    var missing_indices = List[String]()
    if not _object_exists(connection, "index", "idx_runtime_events_impulse"): missing_indices.append("idx_runtime_events_impulse")
    if not _object_exists(connection, "index", "idx_runtime_events_process"): missing_indices.append("idx_runtime_events_process")
    if not _object_exists(connection, "index", "idx_runs_status"): missing_indices.append("idx_runs_status")
    if not _object_exists(connection, "index", "idx_delegation_policies_pool"): missing_indices.append("idx_delegation_policies_pool")
    if not _object_exists(connection, "index", "idx_impulse_relations_source"): missing_indices.append("idx_impulse_relations_source")
    if not _object_exists(connection, "index", "idx_impulse_relations_target"): missing_indices.append("idx_impulse_relations_target")
    if not _object_exists(connection, "index", "idx_associations_impulse"): missing_indices.append("idx_associations_impulse")
    if not _object_exists(connection, "index", "idx_reactions_impulse"): missing_indices.append("idx_reactions_impulse")
    if not _object_exists(connection, "index", "idx_processes_ready"): missing_indices.append("idx_processes_ready")
    if not _object_exists(connection, "index", "idx_processes_run_status"): missing_indices.append("idx_processes_run_status")
    if not _object_exists(connection, "index", "idx_processes_impulse"): missing_indices.append("idx_processes_impulse")
    if not _object_exists(connection, "index", "idx_homeostats_status"): missing_indices.append("idx_homeostats_status")
    if not _object_exists(connection, "index", "idx_bridge_outbox_status"): missing_indices.append("idx_bridge_outbox_status")
    if not _object_exists(connection, "index", "idx_bridge_inbox_status"): missing_indices.append("idx_bridge_inbox_status")
    var missing_triggers = List[String]()
    if not _object_exists(connection, "trigger", "runtime_events_no_update"): missing_triggers.append("runtime_events_no_update")
    if not _object_exists(connection, "trigger", "runtime_events_no_delete"): missing_triggers.append("runtime_events_no_delete")
    if not _object_exists(connection, "trigger", "runtime_commands_no_update"): missing_triggers.append("runtime_commands_no_update")
    if not _object_exists(connection, "trigger", "runtime_commands_no_delete"): missing_triggers.append("runtime_commands_no_delete")
    var migration = _migration_version(connection)
    return SchemaStatus(
        current_version=migration,
        latest_version=SCHEMA_VERSION,
        user_version=_user_version(connection),
        migration_version=migration,
        applied_at=_migration_applied_at(connection),
        missing_tables=missing^,
        missing_indices=missing_indices^,
        missing_triggers=missing_triggers^,
        runtime_events_has_process_id=_has_runtime_event_column(connection, "process_id"),
        runtime_events_has_schema_version=_has_runtime_event_column(connection, "schema_version"),
    )

def _require_current_schema(mut connection: Connection) raises SQLiteError:
    var status = schema_status(connection)
    if not status.is_current():
        raise SQLiteError(code=1, message="schema initialization incomplete")

def _prepare_schema_connection(mut connection: Connection) raises SQLiteError:
    connection.execute("PRAGMA busy_timeout = 30000; PRAGMA journal_mode = WAL; PRAGMA foreign_keys = ON;")


def _migrate_schema_in_transaction(mut connection: Connection) raises:
    _validate_legacy_metadata(connection)
    _validate_legacy_runtime_events(connection)
    _validate_schema_versions(connection)
    connection.execute(
        "CREATE TABLE IF NOT EXISTS schema_migrations (id TEXT PRIMARY KEY, version INTEGER NOT NULL, name TEXT NOT NULL, applied_at TEXT NOT NULL)"
    )
    # CREATE TABLE IF NOT EXISTS cannot repair an existing legacy table.
    if _table_exists(connection, "runtime_events"):
        if not _has_runtime_event_column(connection, "process_id"):
            connection.execute("ALTER TABLE runtime_events ADD COLUMN process_id TEXT")
        if not _has_runtime_event_column(connection, "schema_version"):
            connection.execute(
                "ALTER TABLE runtime_events ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1"
            )
        if not _has_runtime_event_column(connection, "impulse_id"):
            connection.execute("ALTER TABLE runtime_events ADD COLUMN impulse_id TEXT")
        if not _has_runtime_event_column(connection, "command_id"):
            connection.execute("ALTER TABLE runtime_events ADD COLUMN command_id TEXT")
        if not _has_runtime_event_column(connection, "actor"):
            connection.execute("ALTER TABLE runtime_events ADD COLUMN actor TEXT")
        if not _has_runtime_event_column(connection, "correlation_id"):
            connection.execute("ALTER TABLE runtime_events ADD COLUMN correlation_id TEXT")
        if not _has_runtime_event_column(connection, "causation_id"):
            connection.execute("ALTER TABLE runtime_events ADD COLUMN causation_id TEXT")
    if _table_exists(connection, "homeostats"):
        if not _has_table_column(connection, "homeostats", "attempt"):
            connection.execute("ALTER TABLE homeostats ADD COLUMN attempt INTEGER NOT NULL DEFAULT 0")
        if not _has_table_column(connection, "homeostats", "max_attempts"):
            connection.execute("ALTER TABLE homeostats ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 1")
    if _table_exists(connection, "processes") and not _has_table_column(connection, "processes", "output_schema_json"):
        connection.execute("ALTER TABLE processes ADD COLUMN output_schema_json TEXT NOT NULL DEFAULT '{}'")
    # SCHEMA_SQL is idempotent and supplies all tables, indexes, triggers, and
    # migration metadata. Keep this as the sole complete-schema execution path.
    connection.execute(SCHEMA_SQL)

def migrate_schema(mut connection: Connection) raises:
    """Atomically ensure the complete schema and repair legacy event columns."""
    _prepare_schema_connection(connection)
    connection.begin_immediate()
    try:
        _migrate_schema_in_transaction(connection)
        connection.commit()
    except err:
        connection.rollback()
        raise err^

# Connection-specific initialization uses the same atomic migration path.
def _initialize_connection_schema(mut connection: Connection) raises:
    _migrate_schema_in_transaction(connection)
    _require_current_schema(connection)

def initialize_native_schema(mut connection: Connection) raises SQLiteError:
    """Initialize the complete schema atomically with native storage errors."""
    _prepare_schema_connection(connection)
    connection.begin_immediate()
    try:
        _initialize_connection_schema(connection)
        connection.commit()
    except err:
        connection.rollback()
        raise SQLiteError(code=1, message="schema initialization failed: " + String(err))
