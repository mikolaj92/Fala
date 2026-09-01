"""Fail-closed schema-v6 contract for host journal ensure.

Current table/index/trigger/FK truth comes from an in-memory SCHEMA_SQL
apply. Closed legacy PK rebuilds stay here so Python does not copy schema.
"""
from std.collections import List
from fala.sqlite import Connection, Statement, SQLiteError
from fala.schema import (
    SCHEMA_VERSION,
    initialize_native_schema,
    table_names,
)

@fieldwise_init
struct ColumnShape(Copyable, Movable):
    var name: String
    var col_type: String
    var not_null: Int
    var default_value: String
    var default_is_null: Bool
    var pk: Int


def _upper(value: String) -> String:
    var out = String()
    for ch in value.codepoint_slices():
        out += String(ch).upper()
    return out


def _normalize_sql(value: String) -> String:
    var out = String()
    var space = False
    for ch in value.codepoint_slices():
        var ws = ch == " " or ch == "\n" or ch == "\r" or ch == "\t"
        if ws:
            if not space and out.byte_length() > 0:
                out += " "
                space = True
            continue
        space = False
        out += String(ch).lower()
    if out.byte_length() > 0 and out[byte = out.byte_length() - 1] == " ":
        var trimmed = String()
        for i in range(out.byte_length() - 1):
            trimmed += out[byte=i]
        return trimmed
    return out


def _quote_ident(value: String) -> String:
    return "'" + value + "'"


def _error(message: String) raises:
    raise Error(message)


def _text(mut stmt: Statement, index: Int) raises -> String:
    if stmt.column_null(index):
        return String("")
    return stmt.column_text(index)


def _table_exists(mut db: Connection, name: String) raises -> Bool:
    var stmt = db.query("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?")
    stmt.bind_text(1, name)
    var found = stmt.step()
    stmt.close()
    return found


def _has_user_table(mut db: Connection) raises -> Bool:
    var stmt = db.query(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' LIMIT 1"
    )
    var found = stmt.step()
    stmt.close()
    return found


def _shape(mut db: Connection, table: String) raises -> List[ColumnShape]:
    var result = List[ColumnShape]()
    var stmt = db.query("PRAGMA table_info(" + table + ")")
    while stmt.step():
        var default_is_null = stmt.column_null(4)
        result.append(ColumnShape(
            name=_text(stmt, 1),
            col_type=_upper(_text(stmt, 2)),
            not_null=stmt.column_int(3),
            default_value=String("") if default_is_null else _text(stmt, 4),
            default_is_null=default_is_null,
            pk=stmt.column_int(5),
        )^)
    stmt.close()
    return result^


def _col_eq(left: ColumnShape, right: ColumnShape) -> Bool:
    if left.name != right.name or left.col_type != right.col_type:
        return False
    if left.not_null != right.not_null or left.pk != right.pk:
        return False
    if left.default_is_null != right.default_is_null:
        return False
    return left.default_is_null or left.default_value == right.default_value


def _shapes_match(left: List[ColumnShape], right: List[ColumnShape]) -> Bool:
    if len(left) != len(right):
        return False
    var used = List[Bool]()
    for _ in range(len(right)):
        used.append(False)
    for i in range(len(left)):
        var found = False
        for j in range(len(right)):
            if used[j]:
                continue
            if _col_eq(left[i], right[j]):
                used[j] = True
                found = True
                break
        if not found:
            return False
    return True


def _without_names(shape: List[ColumnShape], names: List[String]) -> List[ColumnShape]:
    var result = List[ColumnShape]()
    for col in shape:
        var drop = False
        for name in names:
            if col.name == name:
                drop = True
        if not drop:
            result.append(col.copy())
    return result^


def _legacy_event_shapes() -> List[List[ColumnShape]]:
    var minimal = List[ColumnShape]()
    minimal.append(ColumnShape("run_id", "TEXT", 1, "", True, 0)^)
    minimal.append(ColumnShape("sequence", "INTEGER", 1, "", True, 0)^)
    minimal.append(ColumnShape("id", "TEXT", 0, "", True, 1)^)
    minimal.append(ColumnShape("event_type", "TEXT", 1, "", True, 0)^)
    minimal.append(ColumnShape("payload", "TEXT", 1, "", True, 0)^)
    minimal.append(ColumnShape("created_at", "TEXT", 1, "", True, 0)^)
    var process_only = List[ColumnShape]()
    process_only.append(ColumnShape("run_id", "TEXT", 1, "", True, 0)^)
    process_only.append(ColumnShape("sequence", "INTEGER", 1, "", True, 0)^)
    process_only.append(ColumnShape("id", "TEXT", 0, "", True, 1)^)
    process_only.append(ColumnShape("event_type", "TEXT", 1, "", True, 0)^)
    process_only.append(ColumnShape("process_id", "TEXT", 0, "", True, 0)^)
    process_only.append(ColumnShape("payload", "TEXT", 1, "", True, 0)^)
    process_only.append(ColumnShape("created_at", "TEXT", 1, "", True, 0)^)
    var schema_only = List[ColumnShape]()
    schema_only.append(ColumnShape("run_id", "TEXT", 1, "", True, 0)^)
    schema_only.append(ColumnShape("sequence", "INTEGER", 1, "", True, 0)^)
    schema_only.append(ColumnShape("id", "TEXT", 0, "", True, 1)^)
    schema_only.append(ColumnShape("event_type", "TEXT", 1, "", True, 0)^)
    schema_only.append(ColumnShape("schema_version", "INTEGER", 1, "1", False, 0)^)
    schema_only.append(ColumnShape("payload", "TEXT", 1, "", True, 0)^)
    schema_only.append(ColumnShape("created_at", "TEXT", 1, "", True, 0)^)
    var all_shapes = List[List[ColumnShape]]()
    all_shapes.append(minimal^)
    all_shapes.append(process_only^)
    all_shapes.append(schema_only^)
    return all_shapes^


def _legacy_process_shape(current: List[ColumnShape]) -> List[ColumnShape]:
    var result = List[ColumnShape]()
    for col in current:
        if col.name == "output_schema_json":
            continue
        var not_null = 0 if col.name == "id" else col.not_null
        var pk = 1 if col.name == "id" else 0
        result.append(ColumnShape(
            col.name.copy(), col.col_type.copy(), not_null,
            col.default_value.copy(), col.default_is_null, pk,
        )^)
    return result^


def _shapes_eq_ordered(left: List[ColumnShape], right: List[ColumnShape]) -> Bool:
    if len(left) != len(right):
        return False
    for i in range(len(left)):
        if not _col_eq(left[i], right[i]):
            return False
    return True


def _is_legacy(table: String, actual: List[ColumnShape], current_processes: List[ColumnShape]) raises -> Bool:
    if table == "runtime_events":
        for candidate in _legacy_event_shapes():
            if _shapes_eq_ordered(actual, candidate):
                return True
        return False
    if table == "processes":
        return _shapes_eq_ordered(actual, _legacy_process_shape(current_processes))
    return False


def _foreign_keys(mut db: Connection, table: String) raises -> List[String]:
    var result = List[String]()
    var stmt = db.query("PRAGMA foreign_key_list(" + table + ")")
    while stmt.step():
        result.append(
            _text(stmt, 2) + "|" + _text(stmt, 3) + "|" + _text(stmt, 4) + "|"
            + _text(stmt, 5) + "|" + _text(stmt, 6) + "|" + _text(stmt, 7)
        )
    stmt.close()
    return result^


def _index_columns(mut db: Connection, name: String) raises -> String:
    var cols = String()
    var stmt = db.query("PRAGMA index_info(" + name + ")")
    var first = True
    while stmt.step():
        if not first:
            cols += ","
        first = False
        cols += _text(stmt, 2)
    stmt.close()
    return cols


def _unique_sets(mut db: Connection, table: String) raises -> List[String]:
    var result = List[String]()
    var stmt = db.query("PRAGMA index_list(" + table + ")")
    var names = List[String]()
    while stmt.step():
        if _text(stmt, 3) == "u":
            names.append(_text(stmt, 1))
    stmt.close()
    for name in names:
        result.append(_index_columns(db, name))
    return result^


def _list_eq(left: List[String], right: List[String]) -> Bool:
    if len(left) != len(right):
        return False
    for i in range(len(left)):
        if left[i] != right[i]:
            return False
    return True


def _set_eq(left: List[String], right: List[String]) -> Bool:
    if len(left) != len(right):
        return False
    var used = List[Bool]()
    for _ in range(len(right)):
        used.append(False)
    for i in range(len(left)):
        var found = False
        for j in range(len(right)):
            if used[j]:
                continue
            if left[i] == right[j]:
                used[j] = True
                found = True
                break
        if not found:
            return False
    return True


def _object_sql(mut db: Connection, object_type: String, name: String) raises -> String:
    var stmt = db.query("SELECT sql FROM sqlite_master WHERE type=? AND name=?")
    stmt.bind_text(1, object_type)
    stmt.bind_text(2, name)
    var sql = String("")
    var found = stmt.step()
    if found:
        sql = _text(stmt, 0)
    stmt.close()
    return sql


def _named_indexes(mut db: Connection) raises -> List[String]:
    var result = List[String]()
    var stmt = db.query(
        "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    )
    while stmt.step():
        result.append(_text(stmt, 0))
    stmt.close()
    return result^


def _named_triggers(mut db: Connection) raises -> List[String]:
    var result = List[String]()
    var stmt = db.query("SELECT name FROM sqlite_master WHERE type='trigger' ORDER BY name")
    while stmt.step():
        result.append(_text(stmt, 0))
    stmt.close()
    return result^


def _validate_schema(mut db: Connection, mut reference: Connection, before_migration: Bool) raises:
    var processes_current = _shape(reference, "processes")
    for table in table_names():
        if not _table_exists(db, table):
            if before_migration:
                continue
            _error("fala journal: schema-v6 table " + _quote_ident(table) + " is missing")
        var actual = _shape(db, table)
        var expected = _shape(reference, table)
        var allowed = List[List[ColumnShape]]()
        allowed.append(expected.copy())
        if before_migration and table == "processes":
            var drop_schema = List[String]()
            drop_schema.append("output_schema_json")
            allowed.append(_without_names(expected.copy(), drop_schema))
        if before_migration and table == "homeostats":
            var drop_max = List[String]()
            drop_max.append("max_attempts")
            var drop_both = List[String]()
            drop_both.append("attempt")
            drop_both.append("max_attempts")
            allowed.append(_without_names(expected.copy(), drop_max))
            allowed.append(_without_names(expected.copy(), drop_both))
        var matched = False
        for candidate in allowed:
            if _shapes_match(actual, candidate):
                matched = True
        var legacy = before_migration and _is_legacy(table, actual, processes_current)
        if not legacy and not matched:
            _error("fala journal: incompatible " + table + " table; refusing schema-v6 write")
        if not legacy:
            if not _list_eq(_foreign_keys(db, table), _foreign_keys(reference, table)):
                _error("fala journal: incompatible " + table + " foreign keys; refusing schema-v6 write")
            if not _set_eq(_unique_sets(db, table), _unique_sets(reference, table)):
                _error("fala journal: incompatible " + table + " unique constraints; refusing schema-v6 write")
    for name in _named_indexes(reference):
        var sql = _object_sql(db, "index", name)
        if sql == "":
            if before_migration:
                continue
            _error("fala journal: schema-v6 index " + _quote_ident(name) + " is missing")
        if _index_columns(db, name) != _index_columns(reference, name):
            _error("fala journal: incompatible index " + name + "; refusing schema-v6 write")
    for name in _named_triggers(reference):
        var sql = _object_sql(db, "trigger", name)
        if sql == "":
            if before_migration:
                continue
            _error("fala journal: schema-v6 trigger " + _quote_ident(name) + " is missing")
        if _normalize_sql(sql) != _normalize_sql(_object_sql(reference, "trigger", name)):
            _error("fala journal: incompatible trigger " + name + "; refusing schema-v6 write")


def _column_names(shape: List[ColumnShape]) -> List[String]:
    var names = List[String]()
    for col in shape:
        names.append(col.name.copy())
    return names^


def _contains_name(names: List[String], wanted: String) -> Bool:
    for name in names:
        if name == wanted:
            return True
    return False


def _create_canonical_table(
    mut db: Connection, mut reference: Connection, table: String
) raises:
    """Create one table from the schema-v6 reference, never copied DDL."""
    var sql = _object_sql(reference, "table", table)
    if sql == "":
        _error("fala journal: canonical table DDL is missing for " + table)
    db.execute(sql)


def _rebuild_legacy_tables(mut db: Connection, mut reference: Connection) raises:
    var processes_current = _shape(reference, "processes")
    var rebuild_events = _table_exists(db, "runtime_events") and _is_legacy(
        "runtime_events", _shape(db, "runtime_events"), processes_current
    )
    var rebuild_processes = _table_exists(db, "processes") and _is_legacy(
        "processes", _shape(db, "processes"), processes_current
    )
    if not rebuild_events and not rebuild_processes:
        return
    db.execute("PRAGMA foreign_keys=OFF")
    db.begin_immediate()
    try:
        if rebuild_events:
            var columns = _column_names(_shape(db, "runtime_events"))
            db.execute("ALTER TABLE runtime_events RENAME TO _fala_legacy_runtime_events")
            _create_canonical_table(db, reference, "runtime_events")
            var targets = List[String]()
            targets.append("run_id")
            targets.append("sequence")
            targets.append("id")
            targets.append("event_type")
            targets.append("schema_version")
            targets.append("impulse_id")
            targets.append("process_id")
            targets.append("command_id")
            targets.append("actor")
            targets.append("correlation_id")
            targets.append("causation_id")
            targets.append("payload")
            targets.append("created_at")
            var insert = String("INSERT INTO runtime_events (")
            var select = String(" SELECT ")
            var first = True
            for name in targets:
                if not first:
                    insert += ","
                    select += ","
                first = False
                insert += name
                if _contains_name(columns, name):
                    select += name
                elif name == "schema_version":
                    select += "1"
                else:
                    select += "NULL"
            db.execute(insert + ")" + select + " FROM _fala_legacy_runtime_events")
            db.execute("DROP TABLE _fala_legacy_runtime_events")
        if rebuild_processes:
            var columns = _column_names(_shape(db, "processes"))
            db.execute("ALTER TABLE processes RENAME TO _fala_legacy_processes")
            _create_canonical_table(db, reference, "processes")
            var insert = String("INSERT INTO processes (")
            var select = String(" SELECT ")
            var first = True
            for name in columns:
                if not first:
                    insert += ","
                    select += ","
                first = False
                insert += name
                select += name
            insert += ",output_schema_json)"
            select += ", '{}' FROM _fala_legacy_processes"
            db.execute(insert + select)
            db.execute("DROP TABLE _fala_legacy_processes")
        db.commit()
    except err:
        db.rollback()
        raise err^


def _require_v6(mut db: Connection) raises:
    var version = db.query("PRAGMA user_version")
    if not version.step() or version.column_int(0) != SCHEMA_VERSION:
        version.close()
        _error("fala journal: schema-v6 initialization incomplete")
    version.close()
    var row = db.query("SELECT version FROM schema_migrations WHERE id='runtime_backend'")
    if not row.step() or row.column_int(0) != SCHEMA_VERSION:
        row.close()
        _error("fala journal: schema-v6 initialization incomplete")
    row.close()


def ensure_host_journal(path: String) raises:
    """Validate, rebuild closed legacy PK tables, then initialize schema v6."""
    var reference = Connection(":memory:")
    initialize_native_schema(reference)
    var db = Connection(path)
    try:
        if _has_user_table(db):
            _validate_schema(db, reference, True)
            _rebuild_legacy_tables(db, reference)
        initialize_native_schema(db)
        _validate_schema(db, reference, False)
        _require_v6(db)
        db.close()
        reference.close()
    except err:
        try:
            db.close()
        except close_err:
            pass
        try:
            reference.close()
        except close_err:
            pass
        raise err^
