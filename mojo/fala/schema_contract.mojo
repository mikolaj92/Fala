"""Fail-closed current-schema contract for host journal ensure.

Current table/index/trigger/FK truth comes from an in-memory SCHEMA_SQL
apply. Empty journals are initialized. Existing journals must already be
current; there is no rebuild path.
"""
from std.collections import List
from fala.sqlite import Connection, Statement
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


def _validate_schema(mut db: Connection, mut reference: Connection) raises:
    for table in table_names():
        if not _table_exists(db, table):
            _error("fala journal: schema-v6 table " + _quote_ident(table) + " is missing")
        var actual = _shape(db, table)
        var expected = _shape(reference, table)
        if not _shapes_match(actual, expected):
            _error("fala journal: incompatible " + table + " table; refusing schema-v6 write")
        if not _list_eq(_foreign_keys(db, table), _foreign_keys(reference, table)):
            _error("fala journal: incompatible " + table + " foreign keys; refusing schema-v6 write")
        if not _set_eq(_unique_sets(db, table), _unique_sets(reference, table)):
            _error("fala journal: incompatible " + table + " unique constraints; refusing schema-v6 write")
    for name in _named_indexes(reference):
        var sql = _object_sql(db, "index", name)
        if sql == "":
            _error("fala journal: schema-v6 index " + _quote_ident(name) + " is missing")
        if _index_columns(db, name) != _index_columns(reference, name):
            _error("fala journal: incompatible index " + name + "; refusing schema-v6 write")
    for name in _named_triggers(reference):
        var sql = _object_sql(db, "trigger", name)
        if sql == "":
            _error("fala journal: schema-v6 trigger " + _quote_ident(name) + " is missing")
        if _normalize_sql(sql) != _normalize_sql(_object_sql(reference, "trigger", name)):
            _error("fala journal: incompatible trigger " + name + "; refusing schema-v6 write")


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
    """Initialize an empty journal, or accept a current schema-v6 journal."""
    var reference = Connection(":memory:")
    initialize_native_schema(reference)
    var db = Connection(path)
    try:
        if _has_user_table(db):
            _validate_schema(db, reference)
        else:
            initialize_native_schema(db)
            _validate_schema(db, reference)
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
