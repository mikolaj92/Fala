"""Read-only native CLI inspection, listing, event validation, and trace helpers."""
from std.collections import List
from fala.journal import NativeJournal
from fala.sqlite import Connection, Statement, SQLiteError
from fala.json import parse_json, quote_json_string as _quote
from fala.native_driver import diagnose_waits, observe_run_boundary
from emberjson import Value, to_string
from fala.native_cli_parse import (
    _flag, _validate, _bool_option, _limit, _after_sequence,
)

def _text(mut stmt: Statement, index: Int) raises -> String:
    if stmt.column_null(index): return String("")
    return stmt.column_text(index)
def _nullable_text_json(mut stmt: Statement, index: Int) raises -> String:
    if stmt.column_null(index): return "null"
    return _quote(stmt.column_text(index))

def _nullable_int_json(mut stmt: Statement, index: Int) raises -> String:
    if stmt.column_null(index): return "null"
    return String(stmt.column_int(index))



def _event_schema_max(command: String) raises -> Int:
    var raw = _flag(command, "--max-schema-version", "1")
    try:
        var parsed = parse_json(raw)
        var value = -1
        if parsed.value.is_int(): value = Int(parsed.value.int())
        elif parsed.value.is_uint(): value = Int(parsed.value.uint())
        else: raise Error("not integer")
        if value < 1: raise Error("non-positive")
        return value
    except err:
        raise Error(String(SQLiteError(code=2, message="argument_error: --max-schema-version must be greater than zero")))


def _events_validate_schema(path: String, command: String) raises -> String:
    var run_id = _flag(command, "--run-id")
    if run_id == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
    var max_schema_version = _event_schema_max(command)
    var connection = Connection(path)
    initialize_native_schema(connection)
    var stmt = connection.query("SELECT id,sequence,event_type,schema_version FROM runtime_events WHERE run_id=? ORDER BY sequence ASC")
    stmt.bind_text(1, run_id)
    var event_count = 0
    var versions = "{"
    var version_counts = List[Int]()
    var version_values = List[Int]()
    var unsupported = "["
    var unsupported_first = True
    while stmt.step():
        var version = stmt.column_int(3)
        var version_index = -1
        for index in range(len(version_values)):
            if version_values[index] == version: version_index = index
        if version_index < 0:
            version_values.append(version)
            version_counts.append(1)
        else:
            version_counts[version_index] += 1
        if version > max_schema_version:
            if not unsupported_first: unsupported += ","
            unsupported_first = False
            unsupported += "{\"id\":" + _quote(_text(stmt, 0)) + ",\"sequence\":" + String(stmt.column_int(1)) + ",\"event_type\":" + _quote(_text(stmt, 2)) + ",\"schema_version\":" + String(version) + "}"
        event_count += 1
    stmt.close()
    connection.close()
    # Event schema versions are emitted in numeric order like the reference.
    for outer in range(len(version_values)):
        for inner in range(outer + 1, len(version_values)):
            if version_values[inner] < version_values[outer]:
                var swap_version = version_values[outer]
                version_values[outer] = version_values[inner]
                version_values[inner] = swap_version
                var swap_count = version_counts[outer]
                version_counts[outer] = version_counts[inner]
                version_counts[inner] = swap_count
    for index in range(len(version_values)):
        if index > 0: versions += ","
        versions += _quote(String(version_values[index])) + ":" + String(version_counts[index])
    versions += "}"
    unsupported += "]"
    var ok = "true"
    if unsupported != "[]": ok = "false"
    return "{\"ok\":" + ok + ",\"event_count\":" + String(event_count) + ",\"max_schema_version\":" + String(max_schema_version) + ",\"schema_versions\":" + versions + ",\"unsupported_events\":" + unsupported + "}"




def _runs(path: String, status: String, run_id: String = "", limit: Int = -1, jsonl: Bool = False) raises -> String:
    var connection = Connection(path)
    initialize_native_schema(connection)
    var sql = "SELECT id,status,title,package_id,package_version,package_digest,correlation_path_id,correlation_path_digest,runtime_version,backend_version,schema_version,metadata,created_at,updated_at,started_at,finished_at FROM runs"
    var has_where = False
    if status != "": sql += " WHERE status=?"; has_where = True
    if run_id != "":
        if has_where: sql += " AND id=?"
        else: sql += " WHERE id=?"; has_where = True
    sql += " ORDER BY created_at ASC,id ASC"
    if limit >= 0: sql += " LIMIT ?"
    var stmt = connection.query(sql)
    var bind = 1
    if status != "": stmt.bind_text(bind, status); bind += 1
    if run_id != "": stmt.bind_text(bind, run_id); bind += 1
    if limit >= 0: stmt.bind_int(bind, limit)
    var items = "["
    var lines = ""
    var first = True
    var line_first = True
    var row_count = 0
    while stmt.step():
        var row_json = "{\"id\":" + _quote(_text(stmt,0)) + ",\"status\":" + _quote(_text(stmt,1)) + ",\"title\":" + _nullable_text_json(stmt,2) + ",\"package_id\":" + _nullable_text_json(stmt,3) + ",\"package_version\":" + _nullable_text_json(stmt,4) + ",\"package_digest\":" + _nullable_text_json(stmt,5) + ",\"correlation_path_id\":" + _nullable_text_json(stmt,6) + ",\"correlation_path_digest\":" + _nullable_text_json(stmt,7) + ",\"runtime_version\":" + _nullable_text_json(stmt,8) + ",\"backend_version\":" + _nullable_text_json(stmt,9) + ",\"schema_version\":" + String(stmt.column_int(10)) + ",\"metadata\":" + _text(stmt,11) + ",\"created_at\":" + _quote(_text(stmt,12)) + ",\"updated_at\":" + _quote(_text(stmt,13)) + ",\"started_at\":" + _nullable_text_json(stmt,14) + ",\"finished_at\":" + _nullable_text_json(stmt,15) + "}"
        row_count += 1
        if not first: items += ","
        first = False
        items += row_json
        if jsonl:
            if not line_first: lines += "\n"
            line_first = False
            lines += row_json
    items += "]"
    stmt.close()
    connection.close()
    if jsonl: return lines
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"runs\",\"count\":" + String(row_count) + ",\"items\":" + items + "}"

def _command_rows(path: String, command: String) raises -> String:
    var run_id = _flag(command, "--run-id")
    if run_id == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
    var command_type = _flag(command, "--command-type")
    var actor = _flag(command, "--actor")
    var limit = _limit(command)
    var jsonl = _bool_option(command, "--jsonl")
    var connection = Connection(path)
    initialize_native_schema(connection)
    var sql = "SELECT run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at FROM runtime_commands WHERE run_id=?"
    if command_type != "": sql += " AND command_type=?"
    if actor != "": sql += " AND actor=?"
    sql += " ORDER BY created_at ASC,id ASC"
    if limit >= 0: sql += " LIMIT ?"
    var stmt = connection.query(sql)
    var bind = 1
    stmt.bind_text(bind, run_id); bind += 1
    if command_type != "": stmt.bind_text(bind, command_type); bind += 1
    if actor != "": stmt.bind_text(bind, actor); bind += 1
    if limit >= 0: stmt.bind_int(bind, limit)
    var items = "["
    var lines = ""
    var first = True
    var line_first = True
    var row_count = 0
    while stmt.step():
        var row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"id\":" + _quote(_text(stmt,1)) + ",\"command_type\":" + _quote(_text(stmt,2)) + ",\"idempotency_key\":" + _quote(_text(stmt,3)) + ",\"actor\":" + _nullable_text_json(stmt,4) + ",\"correlation_id\":" + _nullable_text_json(stmt,5) + ",\"causation_id\":" + _nullable_text_json(stmt,6) + ",\"payload\":" + _text(stmt,7) + ",\"created_at\":" + _quote(_text(stmt,8)) + "}"
        if not first: items += ","
        first = False
        items += row_json
        if jsonl:
            if not line_first: lines += "\n"
            line_first = False
            lines += row_json
        row_count += 1
    items += "]"
    stmt.close()
    connection.close()
    if jsonl: return lines
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":\"commands\",\"count\":" + String(row_count) + ",\"items\":" + items + "}"



def _run_inspect(path: String, run_id: String) raises -> String:
    if run_id == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
    var connection = Connection(path)
    initialize_native_schema(connection)
    var stmt = connection.query("SELECT id,status,title,package_id,package_version,package_digest,correlation_path_id,correlation_path_digest,runtime_version,backend_version,schema_version,metadata,created_at,updated_at,started_at,finished_at FROM runs WHERE id=?")
    stmt.bind_text(1, run_id)
    if not stmt.step():
        stmt.close()
        connection.close()
        return "{\"ok\":false,\"runtime\":\"mojo\",\"run\":null}"
    var result = "{\"ok\":true,\"runtime\":\"mojo\",\"run\":{"
    result += "\"id\":" + _quote(_text(stmt, 0))
    result += ",\"status\":" + _quote(_text(stmt, 1))
    result += ",\"title\":" + _nullable_text_json(stmt, 2)
    result += ",\"package_id\":" + _nullable_text_json(stmt, 3)
    result += ",\"package_version\":" + _nullable_text_json(stmt, 4)
    result += ",\"package_digest\":" + _nullable_text_json(stmt, 5)
    result += ",\"correlation_path_id\":" + _nullable_text_json(stmt, 6)
    result += ",\"correlation_path_digest\":" + _nullable_text_json(stmt, 7)
    result += ",\"runtime_version\":" + _nullable_text_json(stmt, 8)
    result += ",\"backend_version\":" + _nullable_text_json(stmt, 9)
    result += ",\"schema_version\":" + String(stmt.column_int(10))
    result += ",\"metadata\":" + _text(stmt, 11)
    result += ",\"created_at\":" + _quote(_text(stmt, 12))
    result += ",\"updated_at\":" + _quote(_text(stmt, 13))
    result += ",\"started_at\":" + _nullable_text_json(stmt, 14)
    result += ",\"finished_at\":" + _nullable_text_json(stmt, 15)
    result += "}}"
    stmt.close()
    connection.close()
    return result

def _run_observe(path: String, run_id: String) raises -> String:
    if run_id == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
    var journal = NativeJournal.open(path)
    journal.initialize()
    var boundary = observe_run_boundary(journal, run_id)
    journal.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"boundary\":{\"run_id\":" + _quote(boundary.run_id) + ",\"status\":" + _quote(boundary.status) + ",\"derived_status\":" + _quote(boundary.derived_status) + ",\"process_status_counts\":" + boundary.process_status_counts + ",\"event_watermark\":" + String(boundary.event_watermark) + "}}"

def _diagnose_waits(path: String, run_id: String, impulse_id: String = "") raises -> String:
    if run_id == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
    var journal = NativeJournal.open(path)
    journal.initialize()
    var diagnostic = diagnose_wait_graph(journal, run_id, impulse_id)
    var result = "{\"ok\":true,\"runtime\":\"mojo\",\"wait_diagnostics\":" + diagnostic.to_json() + "}"
    journal.close()
    return result

def _impulse_inspect(path: String, run_id: String, impulse_id: String) raises -> String:
    if run_id == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
    if impulse_id == "": raise Error(String(SQLiteError(code=2, message="argument_error: --impulse-id is required")))
    var connection = Connection(path)
    initialize_native_schema(connection)
    var stmt = connection.query("SELECT id,run_id,impulse_type,payload,metadata,created_at,updated_at FROM impulses WHERE run_id=? AND id=?")
    stmt.bind_text(1, run_id)
    stmt.bind_text(2, impulse_id)
    if not stmt.step():
        stmt.close()
        connection.close()
        return "{\"ok\":false,\"runtime\":\"mojo\",\"impulse\":null}"
    var result = "{\"ok\":true,\"runtime\":\"mojo\",\"impulse\":{"
    result += "\"id\":" + _quote(_text(stmt, 0))
    result += ",\"run_id\":" + _quote(_text(stmt, 1))
    result += ",\"impulse_type\":" + _quote(_text(stmt, 2))
    result += ",\"payload\":" + _text(stmt, 3)
    result += ",\"metadata\":" + _text(stmt, 4)
    result += ",\"created_at\":" + _quote(_text(stmt, 5))
    result += ",\"updated_at\":" + _quote(_text(stmt, 6))
    result += "}}"
    stmt.close()
    connection.close()
    return result

def _command_inspect(path: String, run_id: String, command_id: String) raises -> String:
    if run_id == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
    if command_id == "": raise Error(String(SQLiteError(code=2, message="argument_error: --command-id is required")))
    var connection = Connection(path)
    initialize_native_schema(connection)
    var stmt = connection.query("SELECT run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at FROM runtime_commands WHERE run_id=? AND id=?")
    stmt.bind_text(1, run_id)
    stmt.bind_text(2, command_id)
    if not stmt.step():
        stmt.close()
        connection.close()
        return "{\"ok\":false,\"runtime\":\"mojo\",\"command\":null}"
    var result = "{\"ok\":true,\"runtime\":\"mojo\",\"command\":{"
    result += "\"run_id\":" + _quote(_text(stmt, 0))
    result += ",\"id\":" + _quote(_text(stmt, 1))
    result += ",\"command_type\":" + _quote(_text(stmt, 2))
    result += ",\"idempotency_key\":" + _quote(_text(stmt, 3))
    result += ",\"actor\":" + _nullable_text_json(stmt, 4)
    result += ",\"correlation_id\":" + _nullable_text_json(stmt, 5)
    result += ",\"causation_id\":" + _nullable_text_json(stmt, 6)
    result += ",\"payload\":" + _text(stmt, 7)
    result += ",\"created_at\":" + _quote(_text(stmt, 8)) + "}}"
    stmt.close()
    connection.close()
    return result

def _process_inspect(path: String, run_id: String, process_id: String) raises -> String:
    if run_id == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
    if process_id == "": raise Error(String(SQLiteError(code=2, message="argument_error: --process-id is required")))
    var connection = Connection(path)
    initialize_native_schema(connection)
    var stmt = connection.query("SELECT run_id,id,process_type,impulse_id,status,priority,attempt,max_attempts,available_at,lease_owner,lease_expires_at,input_json,output_json,error_json,metadata,created_at,updated_at,started_at,finished_at,output_schema_json FROM processes WHERE run_id=? AND id=?")
    stmt.bind_text(1, run_id)
    stmt.bind_text(2, process_id)
    if not stmt.step():
        stmt.close()
        connection.close()
        return "{\"ok\":false,\"runtime\":\"mojo\",\"process\":null}"
    var result = "{\"ok\":true,\"runtime\":\"mojo\",\"process\":{"
    result += "\"run_id\":" + _quote(_text(stmt, 0))
    result += ",\"id\":" + _quote(_text(stmt, 1))
    result += ",\"process_type\":" + _quote(_text(stmt, 2))
    result += ",\"impulse_id\":" + _nullable_text_json(stmt, 3)
    result += ",\"status\":" + _quote(_text(stmt, 4))
    result += ",\"priority\":" + String(stmt.column_int(5))
    result += ",\"attempt\":" + String(stmt.column_int(6))
    result += ",\"max_attempts\":" + String(stmt.column_int(7))
    result += ",\"available_at\":" + _quote(_text(stmt, 8))
    result += ",\"lease_owner\":" + _nullable_text_json(stmt, 9)
    result += ",\"lease_expires_at\":" + _nullable_text_json(stmt, 10)
    result += ",\"input\":" + _text(stmt, 11)
    result += ",\"output\":" + _text(stmt, 12)
    result += ",\"error\":" + _text(stmt, 13)
    result += ",\"metadata\":" + _text(stmt, 14)
    result += ",\"created_at\":" + _quote(_text(stmt, 15))
    result += ",\"updated_at\":" + _quote(_text(stmt, 16))
    result += ",\"started_at\":" + _nullable_text_json(stmt, 17)
    result += ",\"finished_at\":" + _nullable_text_json(stmt, 18)
    result += ",\"output_schema\":" + _text(stmt, 19)
    result += "}}"
    stmt.close()
    connection.close()
    return result
def _domain_inspect(path: String, resource: String, table: String, run_id: String, row_id: String, id_name: String) raises -> String:
    if row_id == "": raise Error(String(SQLiteError(code=2, message="argument_error: " + id_name + " is required")))
    if run_id == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
    var connection = Connection(path)
    initialize_native_schema(connection)
    var sql = ""
    if table == "impulse_types": sql = "SELECT id,run_id,title,description,media_types,value_schema_json,metadata,created_at,updated_at FROM impulse_types WHERE run_id=? AND id=?"
    elif table == "impulse_relations": sql = "SELECT id,run_id,relation_type,source_impulse_id,target_impulse_id,metadata,created_at FROM impulse_relations WHERE run_id=? AND id=?"
    elif table == "associations": sql = "SELECT id,run_id,kind,impulse_id,values_json,metadata,created_at FROM associations WHERE run_id=? AND id=?"
    elif table == "reactions": sql = "SELECT id,run_id,kind,uri,impulse_id,media_type,size_bytes,content_hash,metadata,created_at FROM reactions WHERE run_id=? AND id=?"
    else:
        connection.close()
        raise Error(String(SQLiteError(code=2, message="argument_error: unsupported inspect table")))
    var stmt = connection.query(sql)
    stmt.bind_text(1, run_id)
    stmt.bind_text(2, row_id)
    if not stmt.step():
        stmt.close()
        connection.close()
        return "{\"ok\":false,\"runtime\":\"mojo\",\"" + resource + "\":null}"
    var row = ""
    if table == "impulse_types": row = "{\"id\":" + _quote(_text(stmt,0)) + ",\"run_id\":" + _quote(_text(stmt,1)) + ",\"title\":" + _nullable_text_json(stmt,2) + ",\"description\":" + _nullable_text_json(stmt,3) + ",\"media_types\":" + _text(stmt,4) + ",\"value_schema\":" + _text(stmt,5) + ",\"metadata\":" + _text(stmt,6) + ",\"created_at\":" + _quote(_text(stmt,7)) + ",\"updated_at\":" + _quote(_text(stmt,8)) + "}"
    elif table == "impulse_relations": row = "{\"id\":" + _quote(_text(stmt,0)) + ",\"run_id\":" + _quote(_text(stmt,1)) + ",\"relation_type\":" + _quote(_text(stmt,2)) + ",\"source_impulse_id\":" + _quote(_text(stmt,3)) + ",\"target_impulse_id\":" + _quote(_text(stmt,4)) + ",\"metadata\":" + _text(stmt,5) + ",\"created_at\":" + _quote(_text(stmt,6)) + "}"
    elif table == "associations": row = "{\"id\":" + _quote(_text(stmt,0)) + ",\"run_id\":" + _quote(_text(stmt,1)) + ",\"kind\":" + _quote(_text(stmt,2)) + ",\"impulse_id\":" + _nullable_text_json(stmt,3) + ",\"values\":" + _text(stmt,4) + ",\"metadata\":" + _text(stmt,5) + ",\"created_at\":" + _quote(_text(stmt,6)) + "}"
    else: row = "{\"id\":" + _quote(_text(stmt,0)) + ",\"run_id\":" + _quote(_text(stmt,1)) + ",\"kind\":" + _quote(_text(stmt,2)) + ",\"uri\":" + _quote(_text(stmt,3)) + ",\"impulse_id\":" + _nullable_text_json(stmt,4) + ",\"media_type\":" + _nullable_text_json(stmt,5) + ",\"size_bytes\":" + _nullable_int_json(stmt,6) + ",\"content_hash\":" + _nullable_text_json(stmt,7) + ",\"metadata\":" + _text(stmt,8) + ",\"created_at\":" + _quote(_text(stmt,9)) + "}"
    stmt.close()
    connection.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"" + resource + "\":" + row + "}"

def _table(path: String, resource: String, table: String, run_id: String) raises -> String:
    var connection = Connection(path)
    initialize_native_schema(connection)
    var sql = "SELECT COUNT(*) FROM " + table
    if run_id != "": sql += " WHERE run_id=?"
    var stmt = connection.query(sql)
    if run_id != "": stmt.bind_text(1, run_id)
    var count = 0
    while stmt.step(): count = stmt.column_int(0)
    stmt.close()
    connection.close()
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":" + _quote(resource) + ",\"count\":" + String(count) + "}"
def _rows(path: String, resource: String, table: String, command: String) raises -> String:
    var run_id = _flag(command, "--run-id")
    var impulse_id = _flag(command, "--impulse-id")
    var impulse_type = _flag(command, "--impulse-type")
    var status = _flag(command, "--status")
    var event_type = _flag(command, "--event-type")
    var process_id = _flag(command, "--process-id")
    var command_id = _flag(command, "--command-id")
    var command_type = _flag(command, "--command-type")
    var actor = _flag(command, "--actor")
    var relation_type = _flag(command, "--relation-type")
    var kind = _flag(command, "--kind")
    var after_sequence = _after_sequence(command)
    var limit = _limit(command)
    var jsonl = _bool_option(command, "--jsonl")
    var connection = Connection(path)
    initialize_native_schema(connection)
    var sql = ""
    if table == "impulses": sql = "SELECT run_id,id,impulse_type,payload,metadata,created_at,updated_at FROM impulses"
    elif table == "impulse_types": sql = "SELECT run_id,id,title,description,media_types,value_schema_json,metadata,created_at,updated_at FROM impulse_types"
    elif table == "impulse_relations": sql = "SELECT run_id,id,relation_type,source_impulse_id,target_impulse_id,metadata,created_at FROM impulse_relations"
    elif table == "associations": sql = "SELECT run_id,id,kind,impulse_id,values_json,metadata,created_at FROM associations"
    elif table == "reactions": sql = "SELECT run_id,id,kind,uri,impulse_id,media_type,size_bytes,content_hash,metadata,created_at FROM reactions"
    elif table == "homeostats": sql = "SELECT run_id,id,kind,impulse_id,status,values_json,metadata,attempt,max_attempts,created_at,updated_at FROM homeostats"
    elif table == "projections": sql = "SELECT p.id,p.run_id,p.name,p.version,p.data,p.source_event_sequence,p.updated_at,CASE WHEN EXISTS (SELECT 1 FROM runtime_events e WHERE e.run_id=p.run_id AND e.sequence>p.source_event_sequence) THEN 1 ELSE 0 END FROM projections p"
    elif table == "bridge_outbox" or table == "bridge_inbox": sql = "SELECT run_id,id,idempotency_key,status,attempts,source_ref,target_ref,impulse_json,event_ref,pool_id,budget,metadata,created_at,updated_at FROM " + table
    elif table == "runtime_commands": sql = "SELECT run_id,id,command_type,idempotency_key,actor,correlation_id,causation_id,payload,created_at FROM runtime_commands"
    elif table == "runtime_events": sql = "SELECT run_id,sequence,id,event_type,schema_version,impulse_id,process_id,command_id,actor,correlation_id,causation_id,payload,created_at FROM runtime_events"
    elif table == "processes": sql = "SELECT run_id,id,process_type,impulse_id,status,priority,attempt,max_attempts,available_at,lease_owner,lease_expires_at,input_json,output_json,error_json,metadata,created_at,updated_at,started_at,finished_at,output_schema_json FROM processes"
    else: raise Error(String(SQLiteError(code=2, message="argument_error: unsupported row table")))
    var where = False
    if run_id != "": sql += " WHERE run_id=?"; where = True
    if impulse_id != "":
        if table == "impulse_relations":
            if where: sql += " AND (source_impulse_id=? OR target_impulse_id=?)"
            else: sql += " WHERE (source_impulse_id=? OR target_impulse_id=?)"; where = True
        elif table == "impulses" or table == "associations" or table == "reactions" or table == "homeostats" or table == "processes":
            if where: sql += " AND impulse_id=?"
            else: sql += " WHERE impulse_id=?"; where = True
    if impulse_type != "" and table == "impulses":
        if where: sql += " AND impulse_type=?"
        else: sql += " WHERE impulse_type=?"; where = True
    if status != "" and (table == "homeostats" or table == "processes" or table == "bridge_outbox" or table == "bridge_inbox"):
        if where: sql += " AND status=?"
        else: sql += " WHERE status=?"; where = True
    if event_type != "" and table == "runtime_events":
        if where: sql += " AND event_type=?"
        else: sql += " WHERE event_type=?"; where = True
    if after_sequence >= 0 and table == "runtime_events":
        if where: sql += " AND sequence>?"
        else: sql += " WHERE sequence>?"; where = True
    if process_id != "" and table == "runtime_events":
        if where: sql += " AND process_id=?"
        else: sql += " WHERE process_id=?"; where = True
    if command_id != "" and table == "runtime_events":
        if where: sql += " AND command_id=?"
        else: sql += " WHERE command_id=?"; where = True
    if command_type != "" and table == "runtime_commands":
        if where: sql += " AND command_type=?"
        else: sql += " WHERE command_type=?"; where = True
    if actor != "" and (table == "runtime_commands" or table == "runtime_events"):
        if where: sql += " AND actor=?"
        else: sql += " WHERE actor=?"; where = True
    if relation_type != "" and table == "impulse_relations":
        if where: sql += " AND relation_type=?"
        else: sql += " WHERE relation_type=?"; where = True
    if kind != "" and (table == "associations" or table == "reactions" or table == "homeostats"):
        if where: sql += " AND kind=?"
        else: sql += " WHERE kind=?"; where = True
    if table == "runtime_events": sql += " ORDER BY sequence ASC"
    elif table == "impulse_types": sql += " ORDER BY id ASC"
    elif table == "projections": sql += " ORDER BY name ASC"
    elif table == "processes": sql += " ORDER BY priority DESC,available_at ASC,created_at ASC,id ASC"
    elif table == "homeostats": sql += " ORDER BY updated_at ASC,id ASC"
    else: sql += " ORDER BY created_at ASC,id ASC"
    if limit >= 0: sql += " LIMIT ?"
    var stmt = connection.query(sql)
    var bind = 1
    if run_id != "": stmt.bind_text(bind, run_id); bind += 1
    if impulse_id != "" and (table == "impulse_relations" or table == "impulses" or table == "associations" or table == "reactions" or table == "homeostats" or table == "processes"):
        stmt.bind_text(bind, impulse_id); bind += 1
        if table == "impulse_relations": stmt.bind_text(bind, impulse_id); bind += 1
    if impulse_type != "" and table == "impulses": stmt.bind_text(bind, impulse_type); bind += 1
    if status != "" and (table == "homeostats" or table == "processes" or table == "bridge_outbox" or table == "bridge_inbox"): stmt.bind_text(bind, status); bind += 1
    if event_type != "" and table == "runtime_events": stmt.bind_text(bind, event_type); bind += 1
    if after_sequence >= 0 and table == "runtime_events": stmt.bind_int(bind, after_sequence); bind += 1
    if process_id != "" and table == "runtime_events": stmt.bind_text(bind, process_id); bind += 1
    if command_id != "" and table == "runtime_events": stmt.bind_text(bind, command_id); bind += 1
    if command_type != "" and table == "runtime_commands": stmt.bind_text(bind, command_type); bind += 1
    if actor != "" and (table == "runtime_commands" or table == "runtime_events"): stmt.bind_text(bind, actor); bind += 1
    if relation_type != "" and table == "impulse_relations": stmt.bind_text(bind, relation_type); bind += 1
    if kind != "" and (table == "associations" or table == "reactions" or table == "homeostats"): stmt.bind_text(bind, kind); bind += 1
    if limit >= 0: stmt.bind_int(bind, limit)
    var items = "["
    var jsonl_items = ""
    var first = True
    var jsonl_first = True
    var row_count = 0
    while stmt.step():
        var row_json = ""
        if table == "impulses": row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"id\":" + _quote(_text(stmt,1)) + ",\"impulse_type\":" + _quote(_text(stmt,2)) + ",\"payload\":" + _text(stmt,3) + ",\"metadata\":" + _text(stmt,4) + ",\"created_at\":" + _quote(_text(stmt,5)) + ",\"updated_at\":" + _quote(_text(stmt,6)) + "}"
        elif table == "impulse_types": row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"id\":" + _quote(_text(stmt,1)) + ",\"title\":" + _nullable_text_json(stmt,2) + ",\"description\":" + _nullable_text_json(stmt,3) + ",\"media_types\":" + _text(stmt,4) + ",\"value_schema\":" + _text(stmt,5) + ",\"metadata\":" + _text(stmt,6) + ",\"created_at\":" + _quote(_text(stmt,7)) + ",\"updated_at\":" + _quote(_text(stmt,8)) + "}"
        elif table == "impulse_relations": row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"id\":" + _quote(_text(stmt,1)) + ",\"relation_type\":" + _quote(_text(stmt,2)) + ",\"source_impulse_id\":" + _quote(_text(stmt,3)) + ",\"target_impulse_id\":" + _quote(_text(stmt,4)) + ",\"metadata\":" + _text(stmt,5) + ",\"created_at\":" + _quote(_text(stmt,6)) + "}"
        elif table == "associations": row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"id\":" + _quote(_text(stmt,1)) + ",\"kind\":" + _quote(_text(stmt,2)) + ",\"impulse_id\":" + _nullable_text_json(stmt,3) + ",\"values\":" + _text(stmt,4) + ",\"metadata\":" + _text(stmt,5) + ",\"created_at\":" + _quote(_text(stmt,6)) + "}"
        elif table == "reactions": row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"id\":" + _quote(_text(stmt,1)) + ",\"kind\":" + _quote(_text(stmt,2)) + ",\"uri\":" + _quote(_text(stmt,3)) + ",\"impulse_id\":" + _nullable_text_json(stmt,4) + ",\"media_type\":" + _nullable_text_json(stmt,5) + ",\"size_bytes\":" + _nullable_int_json(stmt,6) + ",\"content_hash\":" + _nullable_text_json(stmt,7) + ",\"metadata\":" + _text(stmt,8) + ",\"created_at\":" + _quote(_text(stmt,9)) + "}"
        elif table == "homeostats": row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"id\":" + _quote(_text(stmt,1)) + ",\"kind\":" + _quote(_text(stmt,2)) + ",\"impulse_id\":" + _nullable_text_json(stmt,3) + ",\"status\":" + _quote(_text(stmt,4)) + ",\"values\":" + _text(stmt,5) + ",\"metadata\":" + _text(stmt,6) + ",\"attempt\":" + String(stmt.column_int(7)) + ",\"max_attempts\":" + String(stmt.column_int(8)) + ",\"created_at\":" + _quote(_text(stmt,9)) + ",\"updated_at\":" + _quote(_text(stmt,10)) + "}"
        elif table == "projections": row_json = "{\"id\":" + _quote(_text(stmt,0)) + ",\"run_id\":" + _quote(_text(stmt,1)) + ",\"name\":" + _quote(_text(stmt,2)) + ",\"version\":" + String(stmt.column_int(3)) + ",\"data\":" + _text(stmt,4) + ",\"source_event_sequence\":" + String(stmt.column_int(5)) + ",\"updated_at\":" + _quote(_text(stmt,6)) + ",\"stale\":" + ("true" if stmt.column_int(7) != 0 else "false") + "}"
        elif table == "runtime_commands": row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"id\":" + _quote(_text(stmt,1)) + ",\"command_type\":" + _quote(_text(stmt,2)) + ",\"idempotency_key\":" + _quote(_text(stmt,3)) + ",\"actor\":" + _nullable_text_json(stmt,4) + ",\"correlation_id\":" + _nullable_text_json(stmt,5) + ",\"causation_id\":" + _nullable_text_json(stmt,6) + ",\"payload\":" + _text(stmt,7) + ",\"created_at\":" + _quote(_text(stmt,8)) + "}"
        elif table == "runtime_events": row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"sequence\":" + String(stmt.column_int(1)) + ",\"id\":" + _quote(_text(stmt,2)) + ",\"event_type\":" + _quote(_text(stmt,3)) + ",\"schema_version\":" + String(stmt.column_int(4)) + ",\"impulse_id\":" + _nullable_text_json(stmt,5) + ",\"process_id\":" + _nullable_text_json(stmt,6) + ",\"command_id\":" + _nullable_text_json(stmt,7) + ",\"actor\":" + _nullable_text_json(stmt,8) + ",\"correlation_id\":" + _nullable_text_json(stmt,9) + ",\"causation_id\":" + _nullable_text_json(stmt,10) + ",\"payload\":" + _text(stmt,11) + ",\"created_at\":" + _quote(_text(stmt,12)) + "}"
        elif table == "processes": row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"id\":" + _quote(_text(stmt,1)) + ",\"process_type\":" + _quote(_text(stmt,2)) + ",\"impulse_id\":" + _nullable_text_json(stmt,3) + ",\"status\":" + _quote(_text(stmt,4)) + ",\"priority\":" + String(stmt.column_int(5)) + ",\"attempt\":" + String(stmt.column_int(6)) + ",\"max_attempts\":" + String(stmt.column_int(7)) + ",\"available_at\":" + _quote(_text(stmt,8)) + ",\"lease_owner\":" + _nullable_text_json(stmt,9) + ",\"lease_expires_at\":" + _nullable_text_json(stmt,10) + ",\"input\":" + _text(stmt,11) + ",\"output\":" + _text(stmt,12) + ",\"error\":" + _text(stmt,13) + ",\"metadata\":" + _text(stmt,14) + ",\"created_at\":" + _quote(_text(stmt,15)) + ",\"updated_at\":" + _quote(_text(stmt,16)) + ",\"started_at\":" + _nullable_text_json(stmt,17) + ",\"finished_at\":" + _nullable_text_json(stmt,18) + ",\"output_schema\":" + _text(stmt,19) + "}"
        elif table == "bridge_outbox" or table == "bridge_inbox": row_json = "{\"run_id\":" + _quote(_text(stmt,0)) + ",\"id\":" + _quote(_text(stmt,1)) + ",\"idempotency_key\":" + _quote(_text(stmt,2)) + ",\"status\":" + _quote(_text(stmt,3)) + ",\"attempts\":" + String(stmt.column_int(4)) + ",\"source_ref\":" + _quote(_text(stmt,5)) + ",\"target_ref\":" + _quote(_text(stmt,6)) + ",\"impulse\":" + _text(stmt,7) + ",\"event_ref\":" + _nullable_text_json(stmt,8) + ",\"pool_id\":" + _nullable_text_json(stmt,9) + ",\"budget\":" + _text(stmt,10) + ",\"metadata\":" + _text(stmt,11) + ",\"created_at\":" + _quote(_text(stmt,12)) + ",\"updated_at\":" + _quote(_text(stmt,13)) + "}"
        if not first: items += ","
        first = False
        items += row_json
        if jsonl:
            if not jsonl_first: jsonl_items += "\n"
            jsonl_first = False
            jsonl_items += row_json
        row_count += 1
    items += "]"
    stmt.close()
    connection.close()
    if jsonl: return jsonl_items
    return "{\"ok\":true,\"runtime\":\"mojo\",\"resource\":" + _quote(resource) + ",\"count\":" + String(row_count) + ",\"items\":" + items + "}"
def _trace_parse(text: String) raises -> Value:
    try:
        var parsed = parse_json(text)
        return parsed.value.copy()
    except err:
        raise Error(String(SQLiteError(code=1, message="trace: invalid persisted JSON")))

def _trace_items(path: String, resource: String, table: String, command: String) raises -> String:
    try:
        var envelope = _trace_parse(_rows(path, resource, table, command))
        if not envelope.is_object(): raise Error(String(SQLiteError(code=1, message="trace rows envelope is invalid")))
        var items_text = _trace_value(envelope, "items")
        var items = _trace_parse(items_text)
        if not items.is_array(): raise Error(String(SQLiteError(code=1, message="trace rows items are invalid")))
        return items_text
    except err:
        raise Error(String(SQLiteError(code=1, message="trace: unable to read " + resource)))

def _trace_value(value: Value, key: String) raises -> String:
    try:
        if value.is_object() and key in value.object():
            var child = value.object()[key].copy()
            return to_string(child^)
        return "null"
    except err:
        raise Error(String(SQLiteError(code=1, message="trace: invalid row value")))

def _trace_run(path: String, run_id: String) raises -> String:
    try:
        var envelope = _trace_parse(_run_inspect(path, run_id))
        if not envelope.is_object(): raise Error(String(SQLiteError(code=1, message="run envelope is invalid")))
        var run_text = _trace_value(envelope, "run")
        var run = _trace_parse(run_text)
        return run_text
    except err:
        raise Error(String(SQLiteError(code=1, message="trace: unable to read run")))

def _trace_timeline(events_json: String) raises -> String:
    try:
        var events = _trace_parse(events_json)
        if not events.is_array(): raise Error(String(SQLiteError(code=1, message="events must be an array")))
        var timeline = "["
        var first = True
        for event in events.array():
            if not first: timeline += ","
            first = False
            timeline += "{\"sequence\":" + _trace_value(event, "sequence")
            timeline += ",\"type\":" + _trace_value(event, "event_type")
            timeline += ",\"impulse_id\":" + _trace_value(event, "impulse_id")
            timeline += ",\"process_id\":" + _trace_value(event, "process_id")
            timeline += ",\"actor\":" + _trace_value(event, "actor")
            timeline += ",\"created_at\":" + _trace_value(event, "created_at") + "}"
        return timeline + "]"
    except err:
        raise Error(String(SQLiteError(code=1, message="trace: unable to build timeline")))
def _trace_count(items_json: String) raises -> Int:
    try:
        var items = _trace_parse(items_json)
        if not items.is_array(): raise Error(String(SQLiteError(code=1, message="trace items must be an array")))
        return len(items.array())
    except err:
        raise Error(String(SQLiteError(code=1, message="trace: unable to count rows")))


def _trace(path: String, command: String) raises -> String:
    var run_id = _flag(command, "--run-id", "")
    if run_id == "": raise Error(String(SQLiteError(code=2, message="argument_error: --run-id is required")))
    var run = _trace_run(path, run_id)
    var events = _trace_items(path, "events", "runtime_events", command)
    var impulses = _trace_items(path, "impulses", "impulses", command)
    var relations = _trace_items(path, "impulse-relations", "impulse_relations", command)
    var associations = _trace_items(path, "associations", "associations", command)
    var reactions = _trace_items(path, "reactions", "reactions", command)
    var processes = _trace_items(path, "processes", "processes", command)
    var homeostats = _trace_items(path, "homeostats", "homeostats", command)
    var projections = _trace_items(path, "projections", "projections", command)
    var timeline = _trace_timeline(events)
    var trace = "{\"run_id\":" + _quote(run_id) + ",\"run\":" + run + ",\"counts\":{"
    trace += "\"reactions\":" + String(_trace_count(reactions))
    trace += ",\"impulse_relations\":" + String(_trace_count(relations))
    trace += ",\"impulses\":" + String(_trace_count(impulses))
    trace += ",\"events\":" + String(_trace_count(events))
    trace += ",\"homeostats\":" + String(_trace_count(homeostats))
    trace += ",\"associations\":" + String(_trace_count(associations))
    trace += ",\"processes\":" + String(_trace_count(processes))
    trace += ",\"projections\":" + String(_trace_count(projections)) + "}"
    trace += ",\"timeline\":" + timeline + ",\"events\":" + events
    trace += ",\"impulses\":" + impulses + ",\"impulse_relations\":" + relations
    trace += ",\"associations\":" + associations + ",\"reactions\":" + reactions
    trace += ",\"processes\":" + processes + ",\"homeostats\":" + homeostats
    trace += ",\"projections\":" + projections + "}"
    return "{\"ok\":true,\"runtime\":\"mojo\",\"trace\":" + trace + "}"
