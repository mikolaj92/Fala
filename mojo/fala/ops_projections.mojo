"""Optional projection rebuild ops (not Essential Fala).

Lightweight put/get/list remain on NativeDomainStore. Rebuild bodies live here.
Uses store._require_run, _domain_command_start, _append_domain_event_in_tx, get_projection.
"""

from std.collections import List
from fala.sqlite import SQLiteError
from fala.domain import Projection
from fala.journal import EventInput, EventRow, CommandSubmission
from fala.domain_store import NativeDomainStore

# local quote for summary JSON keys
def _quote(value: String) -> String:
    var result = "\""
    for i in range(value.byte_length()):
        var ch = value[byte=i]
        if ch == "\"": result += "\\\""
        elif ch == "\\": result += "\\\\"
        elif ch == "\n": result += "\\n"
        elif ch == "\r": result += "\\r"
        elif ch == "\t": result += "\\t"
        elif ch < " ": result += "\\u0000"
        else: result += String(ch)
    result += "\""
    return result

@fieldwise_init
struct ProjectionRebuildResult(Copyable, Movable):
    var projections: List[Projection]
    var submission: CommandSubmission

def rebuild_projections_with_command(
    mut store: NativeDomainStore, run_id: String, names: List[String], command_id: String,
    command_type: String, idempotency_key: String, created_at: String,
    updated_at: String = "", events: List[EventInput] = List[EventInput](),
    actor: String = "", correlation_id: String = "", causation_id: String = "",
) raises SQLiteError -> ProjectionRebuildResult:
    if command_type != "projection.rebuild":
        raise SQLiteError(code=1, message="rebuild_projections_with_command requires command_type 'projection.rebuild'")
    store._require_run(run_id)
    var requested = List[String]()
    if len(names) == 0: requested.append("run_summary")
    else:
        for name in names:
            if name == "": raise SQLiteError(code=1, message="domain store: projection name must not be empty")
            if name != "run_summary": raise SQLiteError(code=1, message="Unknown projection rebuild name: " + name)
            var duplicate = False
            for prior in requested:
                if prior == name: duplicate = True
            if not duplicate: requested.append(name)
    var payload = "{\"names\":["
    var first = True
    for name in requested:
        if not first: payload += ","
        first = False
        payload += _quote(name)
    payload += "]}"
    var start = store._domain_command_start(run_id, command_id, command_type, idempotency_key, payload, created_at, actor, correlation_id, causation_id)
    var command = start.command.copy()
    var result = List[Projection]()
    var stored_events = List[EventRow]()
    if start.replayed:
        for name in requested: result.append(store.get_projection(run_id, name)^)
        return ProjectionRebuildResult(projections=result^, submission=CommandSubmission(command=command^, events=stored_events^, replayed=True))
    try:
        for item in events: stored_events.append(store._append_domain_event_in_tx(command, item)^)
        var latest = store.db.query("SELECT COALESCE(MAX(sequence),0) FROM runtime_events WHERE run_id=?")
        latest.bind_text(1, run_id)
        if not latest.step(): raise SQLiteError(code=1, message="domain store: unable to read event watermark")
        var watermark = latest.column_int(0); latest.close()
        var effective_at = updated_at
        if effective_at == "": effective_at = created_at
        if effective_at == "":
            raise SQLiteError(code=2, message="domain store: projection rebuild timestamp must not be empty")
        for name in requested:
            var data = _run_summary_data(store, run_id, watermark)
            var insert = store.db.query("INSERT INTO projections (run_id,name,id,version,data,source_event_sequence,updated_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(run_id,name) DO UPDATE SET data=excluded.data,source_event_sequence=excluded.source_event_sequence,updated_at=excluded.updated_at")
            insert.bind_text(1, run_id); insert.bind_text(2, name); insert.bind_text(3, name + ":" + run_id); insert.bind_int(4, 1); insert.bind_text(5, data); insert.bind_int(6, watermark); insert.bind_text(7, effective_at); _ = insert.step(); insert.close()
            result.append(store.get_projection(run_id, name)^)
        store.db.commit()
        return ProjectionRebuildResult(projections=result^, submission=CommandSubmission(command=command^, events=stored_events^, replayed=False))
    except err:
        store.db.rollback(); raise err^

def _count_rows(mut store: NativeDomainStore, table: String, run_id: String) raises SQLiteError -> Int:
    var stmt = store.db.query("SELECT COUNT(*) FROM " + table + " WHERE run_id=?")
    stmt.bind_text(1, run_id)
    if not stmt.step():
        stmt.close()
        return 0
    var result = stmt.column_int(0)
    stmt.close()
    return result

def _group_json(mut store: NativeDomainStore, table: String, column: String, run_id: String) raises SQLiteError -> String:
    var stmt = store.db.query("SELECT " + column + ",COUNT(*) FROM " + table + " WHERE run_id=? GROUP BY " + column + " ORDER BY " + column + " ASC")
    stmt.bind_text(1, run_id)
    var result = "{"
    var first = True
    while stmt.step():
        if not first: result += ","
        first = False
        result += _quote(store._text(stmt, 0)) + ":" + String(stmt.column_int(1))
    stmt.close()
    return result + "}"

def _run_summary_data(mut store: NativeDomainStore, run_id: String, source_sequence: Int) raises SQLiteError -> String:
    var event_types = _group_json(store, "runtime_events", "event_type", run_id)
    var impulse_types = _group_json(store, "impulses", "impulse_type", run_id)
    var homeostat_status = _group_json(store, "homeostats", "status", run_id)
    var process_status = _group_json(store, "processes", "status", run_id)
    var reaction_bytes_stmt = store.db.query("SELECT COALESCE(SUM(size_bytes),0) FROM reactions WHERE run_id=?")
    reaction_bytes_stmt.bind_text(1, run_id); _ = reaction_bytes_stmt.step()
    var reaction_bytes = reaction_bytes_stmt.column_int(0); reaction_bytes_stmt.close()
    var attempts_stmt = store.db.query("SELECT COALESCE(SUM(attempt),0) FROM processes WHERE run_id=?")
    attempts_stmt.bind_text(1, run_id); _ = attempts_stmt.step()
    var process_attempts = attempts_stmt.column_int(0); attempts_stmt.close()
    var input_stmt = store.db.query("SELECT COALESCE(SUM(LENGTH(input_json)),0) FROM processes WHERE run_id=?")
    input_stmt.bind_text(1, run_id); _ = input_stmt.step()
    var input_bytes = input_stmt.column_int(0); input_stmt.close()
    var output_stmt = store.db.query("SELECT COALESCE(SUM(LENGTH(output_json)),0) FROM processes WHERE run_id=?")
    output_stmt.bind_text(1, run_id); _ = output_stmt.step()
    var output_bytes = output_stmt.column_int(0); output_stmt.close()
    var bridge_commands = store.db.query("SELECT COUNT(*) FROM runtime_commands WHERE run_id=? AND command_type LIKE 'bridge.%'")
    bridge_commands.bind_text(1, run_id); _ = bridge_commands.step()
    var bridge_command_count = bridge_commands.column_int(0); bridge_commands.close()
    var bridge_delivery_count = _count_rows(store, "bridge_outbox", run_id) + _count_rows(store, "bridge_inbox", run_id)
    var spawned_stmt = store.db.query("SELECT COUNT(DISTINCT json_extract(target_ref,'$.run_id')) FROM bridge_outbox WHERE run_id=?")
    spawned_stmt.bind_text(1, run_id); _ = spawned_stmt.step()
    var spawned = spawned_stmt.column_int(0); spawned_stmt.close()
    var subprocess_stmt = store.db.query("SELECT COUNT(*) FROM processes WHERE run_id=? AND process_type='subprocess'")
    subprocess_stmt.bind_text(1, run_id); _ = subprocess_stmt.step()
    var subprocesses = subprocess_stmt.column_int(0); subprocess_stmt.close()
    var event_count = _count_rows(store, "runtime_events", run_id)
    return "{\"association_count\":" + String(_count_rows(store, "associations", run_id)) + ",\"event_count\":" + String(event_count) + ",\"event_type_counts\":" + event_types + ",\"homeostat_count\":" + String(_count_rows(store, "homeostats", run_id)) + ",\"homeostat_status_counts\":" + homeostat_status + ",\"impulse_count\":" + String(_count_rows(store, "impulses", run_id)) + ",\"impulse_type_counts\":" + impulse_types + ",\"process_count\":" + String(_count_rows(store, "processes", run_id)) + ",\"process_status_counts\":" + process_status + ",\"reaction_count\":" + String(_count_rows(store, "reactions", run_id)) + ",\"resource_accounting\":{\"bridge_command_count\":" + String(bridge_command_count) + ",\"bridge_delivery_count\":" + String(bridge_delivery_count) + ",\"process_attempts\":" + String(process_attempts) + ",\"process_input_bytes\":" + String(input_bytes) + ",\"process_output_bytes\":" + String(output_bytes) + ",\"reaction_bytes\":" + String(reaction_bytes) + ",\"spawned_run_count\":" + String(spawned) + ",\"subprocess_count\":" + String(subprocesses) + "},\"run_id\":" + _quote(run_id) + ",\"source_event_sequence\":" + String(source_sequence) + "}"

def rebuild_projection(mut store: NativeDomainStore, run_id: String, name: String, updated_at: String = "") raises SQLiteError -> Projection:
    var names = List[String](); names.append(name)
    var rebuilt = rebuild_projections(store, run_id, names, updated_at)
    var result = rebuilt[0].copy()
    return result^

def rebuild_projections(mut store: NativeDomainStore, run_id: String, names: List[String], updated_at: String = "") raises SQLiteError -> List[Projection]:
    store._require_run(run_id)
    var requested = names.copy()
    if len(requested) == 0: requested.append("run_summary")
    var result = List[Projection]()
    var effective_updated_at = updated_at
    if effective_updated_at == "":
        raise SQLiteError(code=2, message="domain store: projection rebuild timestamp must not be empty")
    store.db.begin()
    try:
        var latest = store.db.query("SELECT COALESCE(MAX(sequence),0) FROM runtime_events WHERE run_id=?"); latest.bind_text(1, run_id)
        if not latest.step(): raise SQLiteError(code=1, message="domain store: unable to read event watermark")
        var watermark = latest.column_int(0); latest.close()
        for name in requested:
            if name == "": raise SQLiteError(code=1, message="domain store: projection name must not be empty")
            var data = ""
            if name == "run_summary": data = _run_summary_data(store, run_id, watermark)
            else: data = "{\"event_count\":" + String(watermark) + ",\"source_event_sequence\":" + String(watermark) + "}"
            var stmt = store.db.query("INSERT INTO projections (run_id,name,id,version,data,source_event_sequence,updated_at) VALUES (?,?,?,?,?,?,?) ON CONFLICT(run_id,name) DO UPDATE SET data=excluded.data,source_event_sequence=excluded.source_event_sequence,updated_at=excluded.updated_at")
            stmt.bind_text(1, run_id); stmt.bind_text(2, name); stmt.bind_text(3, name + ":" + run_id); stmt.bind_int(4, 1); stmt.bind_text(5, data); stmt.bind_int(6, watermark); stmt.bind_text(7, effective_updated_at); _ = stmt.step()
        store.db.commit()
    except err:
        store.db.rollback(); raise SQLiteError(code=1, message="domain store: projection rebuild failed")
    for name in requested: result.append(store.get_projection(run_id, name)^)
    return result^

