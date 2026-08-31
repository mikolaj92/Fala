"""Host lifecycle writes through the native journal.

Python keeps the public function names. Durable INSERT/UPDATE lives here.
"""
from emberjson import Object, to_string
from fala.journal import NativeJournal
from fala.json import parse_json, quote_json_string
from fala.schema_contract import ensure_host_journal
from fala.status import ProcessStatus, RunStatus, can_transition_process, can_transition_run

def _error(message: String) raises:
    raise Error(message)


def _quote_ident(value: String) -> String:
    return "'" + value + "'"


def _known_run_status(value: String) -> Bool:
    return RunStatus(value).is_known()


def _known_process_status(value: String) -> Bool:
    return ProcessStatus(value).is_known()


def _terminal_run(value: String) -> Bool:
    return RunStatus(value).is_terminal()


def _terminal_process(value: String) -> Bool:
    return ProcessStatus(value).is_terminal()


def _require_transition_run(current: String, target: String) raises:
    if current == target:
        return
    if not can_transition_run(RunStatus(current), RunStatus(target)):
        _error("fala journal: unsafe run transition " + _quote_ident(current) + " -> " + _quote_ident(target))


def _require_transition_process(current: String, target: String) raises:
    if current == target:
        return
    if not can_transition_process(ProcessStatus(current), ProcessStatus(target)):
        _error("fala journal: unsafe process transition " + _quote_ident(current) + " -> " + _quote_ident(target))


def _open(path: String) raises -> NativeJournal:
    ensure_host_journal(path)
    var journal = NativeJournal.open(path)
    journal.initialize()
    return journal^


def upsert_run_metadata(
    path: String,
    run_id: String,
    status: String,
    metadata_json: String,
    now: String,
    title: String,
    title_present: Bool,
) raises:
    if run_id == "":
        _error("fala journal: run_id must not be blank")
    if not _known_run_status(status):
        _error("fala journal: invalid status " + _quote_ident(status))
    var journal = _open(path)
    try:
        journal.db.begin_immediate()
        var stmt = journal.db.query("SELECT status,title,metadata FROM runs WHERE id=?")
        stmt.bind_text(1, run_id)
        if not stmt.step():
            var insert = journal.db.query(
                "INSERT INTO runs (id,status,title,schema_version,metadata,created_at,updated_at,finished_at) VALUES (?,?,?,?,?,?,?,?)"
            )
            insert.bind_text(1, run_id)
            insert.bind_text(2, status)
            if title_present and title != "":
                insert.bind_text(3, title)
            else:
                insert.bind_text(3, run_id)
            insert.bind_int(4, 6)
            insert.bind_text(5, metadata_json)
            insert.bind_text(6, now)
            insert.bind_text(7, now)
            if _terminal_run(status):
                insert.bind_text(8, now)
            else:
                insert.bind_null(8)
            _ = insert.step()
            journal.db.commit()
            journal.close()
            return
        var current = journal._text(stmt, 0)
        var stored_title = journal._text(stmt, 1)
        var stored_metadata = journal._text(stmt, 2)
        var effective_title = title if title_present else stored_title
        if _terminal_run(current):
            if status == current and effective_title == stored_title and metadata_json == stored_metadata:
                journal.db.commit()
                journal.close()
                return
            _error("fala journal: terminal run " + _quote_ident(run_id) + " cannot be overwritten or reopened")
        _require_transition_run(current, status)
        var update = journal.db.query(
            "UPDATE runs SET status=?,title=COALESCE(?,title),metadata=?,updated_at=?,finished_at=? WHERE id=?"
        )
        update.bind_text(1, status)
        if title_present:
            update.bind_text(2, title)
        else:
            update.bind_null(2)
        update.bind_text(3, metadata_json)
        update.bind_text(4, now)
        if _terminal_run(status):
            update.bind_text(5, now)
        else:
            update.bind_null(5)
        update.bind_text(6, run_id)
        _ = update.step()
        journal.db.commit()
        journal.close()
    except err:
        try:
            journal.db.rollback()
        except roll_err:
            pass
        try:
            journal.close()
        except close_err:
            pass
        raise err^


def _merge_metadata(stored_json: String, updates_json: String) raises -> String:
    var merged = Object()
    try:
        var stored = parse_json(stored_json)
        if stored.value.is_object():
            for pair in stored.value.object().items():
                merged[pair.key] = pair.value.copy()
    except parse_err:
        merged = Object()
    var updates = parse_json(updates_json)
    if updates.value.is_object():
        for pair in updates.value.object().items():
            merged[pair.key] = pair.value.copy()
    return to_string(merged^)


def transition_run(
    path: String,
    run_id: String,
    status: String,
    updates_json: String,
    now: String,
) raises:
    if run_id == "":
        _error("fala journal: run_id must not be blank")
    if not _known_run_status(status):
        _error("fala journal: invalid status " + _quote_ident(status))
    var journal = _open(path)
    try:
        journal.db.begin_immediate()
        var stmt = journal.db.query("SELECT status,metadata FROM runs WHERE id=?")
        stmt.bind_text(1, run_id)
        if not stmt.step():
            _error("fala journal: run " + _quote_ident(run_id) + " not found")
        var current = journal._text(stmt, 0)
        var stored_metadata = journal._text(stmt, 1)
        _require_transition_run(current, status)
        var encoded = _merge_metadata(stored_metadata, updates_json)
        if current == status and encoded == stored_metadata:
            journal.db.commit()
            journal.close()
            return
        if _terminal_run(current):
            _error("fala journal: terminal run " + _quote_ident(run_id) + " cannot be overwritten")
        var update = journal.db.query(
            "UPDATE runs SET status=?,metadata=?,updated_at=?,finished_at=? WHERE id=?"
        )
        update.bind_text(1, status)
        update.bind_text(2, encoded)
        update.bind_text(3, now)
        if _terminal_run(status):
            update.bind_text(4, now)
        else:
            update.bind_null(4)
        update.bind_text(5, run_id)
        _ = update.step()
        journal.db.commit()
        journal.close()
    except err:
        try:
            journal.db.rollback()
        except roll_err:
            pass
        try:
            journal.close()
        except close_err:
            pass
        raise err^


def upsert_process(
    path: String,
    run_id: String,
    process_id: String,
    status: String,
    process_type: String,
    attempt: Int,
    input_json: String,
    output_json: String,
    error_json: String,
    metadata_json: String,
    now: String,
) raises:
    if run_id == "":
        _error("fala journal: run_id must not be blank")
    if process_id == "":
        _error("fala journal: process_id must not be blank")
    if process_type == "":
        _error("fala journal: process_type must not be blank")
    if attempt < 1:
        _error("fala journal: attempt must be a positive integer")
    if not _known_process_status(status):
        _error("fala journal: invalid status " + _quote_ident(status))
    var journal = _open(path)
    try:
        journal.db.begin_immediate()
        var run_stmt = journal.db.query("SELECT 1 FROM runs WHERE id=?")
        run_stmt.bind_text(1, run_id)
        if not run_stmt.step():
            _error("fala journal: run " + _quote_ident(run_id) + " not found")
        var stmt = journal.db.query(
            "SELECT status,process_type,attempt,input_json,output_json,error_json,metadata,lease_owner,lease_expires_at FROM processes WHERE run_id=? AND id=?"
        )
        stmt.bind_text(1, run_id)
        stmt.bind_text(2, process_id)
        if stmt.step():
            var current = journal._text(stmt, 0)
            var stored_type = journal._text(stmt, 1)
            var stored_attempt = stmt.column_int(2)
            var stored_input = journal._text(stmt, 3)
            var stored_output = journal._text(stmt, 4)
            var stored_error = journal._text(stmt, 5)
            var stored_metadata = journal._text(stmt, 6)
            var lease_owner = journal._text(stmt, 7)
            var lease_expires = journal._text(stmt, 8)
            var same = (
                status == current
                and process_type == stored_type
                and attempt == stored_attempt
                and input_json == stored_input
                and output_json == stored_output
                and error_json == stored_error
                and metadata_json == stored_metadata
            )
            if _terminal_process(current):
                if same:
                    if lease_owner != "" or lease_expires != "":
                        var clear = journal.db.query(
                            "UPDATE processes SET lease_owner=NULL,lease_expires_at=NULL WHERE run_id=? AND id=?"
                        )
                        clear.bind_text(1, run_id)
                        clear.bind_text(2, process_id)
                        _ = clear.step()
                    journal.db.commit()
                    journal.close()
                    return
                _error("fala journal: terminal process " + _quote_ident(process_id) + " cannot be overwritten or reopened")
            _require_transition_process(current, status)
            if status == "waiting" and (lease_owner != "" or lease_expires != ""):
                _error("fala journal: leased process " + _quote_ident(process_id) + " cannot be upserted as waiting")
            var update = journal.db.query(
                "UPDATE processes SET process_type=?,status=?,attempt=?,input_json=?,output_json=?,error_json=?,metadata=?,lease_owner=NULL,lease_expires_at=NULL,updated_at=?,finished_at=? WHERE run_id=? AND id=?"
            )
            update.bind_text(1, process_type)
            update.bind_text(2, status)
            update.bind_int(3, attempt)
            update.bind_text(4, input_json)
            update.bind_text(5, output_json)
            update.bind_text(6, error_json)
            update.bind_text(7, metadata_json)
            update.bind_text(8, now)
            if _terminal_process(status):
                update.bind_text(9, now)
            else:
                update.bind_null(9)
            update.bind_text(10, run_id)
            update.bind_text(11, process_id)
            _ = update.step()
            journal.db.commit()
            journal.close()
            return
        var insert = journal.db.query(
            "INSERT INTO processes (run_id,id,process_type,status,priority,attempt,max_attempts,available_at,input_json,output_json,error_json,metadata,created_at,updated_at,started_at,finished_at,output_schema_json) VALUES (?,?,?,?,0,?,1,?,?,?,?,?,?,?,?,?,'{}')"
        )
        insert.bind_text(1, run_id)
        insert.bind_text(2, process_id)
        insert.bind_text(3, process_type)
        insert.bind_text(4, status)
        insert.bind_int(5, attempt)
        insert.bind_text(6, now)
        insert.bind_text(7, input_json)
        insert.bind_text(8, output_json)
        insert.bind_text(9, error_json)
        insert.bind_text(10, metadata_json)
        insert.bind_text(11, now)
        insert.bind_text(12, now)
        insert.bind_text(13, now)
        if _terminal_process(status):
            insert.bind_text(14, now)
        else:
            insert.bind_null(14)
        _ = insert.step()
        journal.db.commit()
        journal.close()
    except err:
        try:
            journal.db.rollback()
        except roll_err:
            pass
        try:
            journal.close()
        except close_err:
            pass
        raise err^


def complete_waiting_process(
    path: String,
    run_id: String,
    process_id: String,
    process_status: String,
    run_status: String,
    blocker_id: String,
    has_blocker: Bool,
    blocker_status: String,
    output_json: String,
    now: String,
) raises -> String:
    if run_id == "":
        _error("fala journal: run_id must not be blank")
    if process_id == "":
        _error("fala journal: process_id must not be blank")
    if not _terminal_process(process_status):
        _error("fala journal: invalid process_status " + _quote_ident(process_status))
    if not _terminal_run(run_status):
        _error("fala journal: invalid run_status " + _quote_ident(run_status))
    if blocker_status != "completed" and blocker_status != "cancelled" and blocker_status != "expired":
        _error("fala journal: invalid blocker_status " + _quote_ident(blocker_status))
    var expected_process = "succeeded"
    if blocker_status == "cancelled":
        expected_process = "cancelled"
    elif blocker_status == "expired":
        expected_process = "timed_out"
    if expected_process != process_status:
        _error(
            "fala journal: blocker/process terminal pairing "
            + _quote_ident(blocker_status) + "/" + _quote_ident(process_status) + " is invalid"
        )
    var expected_run = "completed"
    if process_status == "cancelled":
        expected_run = "cancelled"
    elif process_status == "timed_out":
        expected_run = "timed_out"
    elif process_status != "succeeded":
        expected_run = "failed"
    if expected_run != run_status:
        _error(
            "fala journal: process/run terminal pairing "
            + _quote_ident(process_status) + "/" + _quote_ident(run_status) + " is invalid"
        )
    var journal = _open(path)
    var changed = True
    try:
        journal.db.begin_immediate()
        var process = journal.db.query(
            "SELECT status,output_json,lease_owner,lease_expires_at FROM processes WHERE run_id=? AND id=?"
        )
        process.bind_text(1, run_id)
        process.bind_text(2, process_id)
        if not process.step():
            _error("fala journal: process " + _quote_ident(process_id) + " not found")
        var pstatus = journal._text(process, 0)
        var poutput = journal._text(process, 1)
        var lease_owner = journal._text(process, 2)
        var lease_expires = journal._text(process, 3)
        var run = journal.db.query("SELECT status FROM runs WHERE id=?")
        run.bind_text(1, run_id)
        if not run.step():
            _error("fala journal: run " + _quote_ident(run_id) + " not found")
        var rstatus = journal._text(run, 0)
        var bstatus = String("")
        var bvalues = String("")
        var blocker_found = False
        if has_blocker:
            var blocker = journal.db.query(
                "SELECT status,values_json FROM homeostats WHERE run_id=? AND id=?"
            )
            blocker.bind_text(1, run_id)
            blocker.bind_text(2, blocker_id)
            if not blocker.step():
                _error("fala journal: blocker " + _quote_ident(blocker_id) + " not found")
            blocker_found = True
            bstatus = journal._text(blocker, 0)
            bvalues = journal._text(blocker, 1)
        var exact = pstatus == process_status and poutput == output_json and rstatus == run_status
        if has_blocker:
            exact = exact and bstatus == blocker_status and bvalues == output_json
        if pstatus != "waiting":
            if exact:
                changed = lease_owner != "" or lease_expires != ""
                if changed:
                    var clear = journal.db.query(
                        "UPDATE processes SET lease_owner=NULL,lease_expires_at=NULL WHERE run_id=? AND id=?"
                    )
                    clear.bind_text(1, run_id)
                    clear.bind_text(2, process_id)
                    _ = clear.step()
                journal.db.commit()
                journal.close()
                return _lifecycle_json(run_id, process_id, changed, process_status, run_status)
            _error("fala journal: completion conflicts with durable lifecycle state")
        if lease_owner != "" or lease_expires != "":
            _error("fala journal: waiting process must be wholly unleased before completion")
        if _terminal_run(rstatus) and rstatus != run_status:
            _error("fala journal: completion conflicts with terminal run")
        if has_blocker and blocker_found and bstatus != "open" and not (bstatus == blocker_status and bvalues == output_json):
            _error("fala journal: completion conflicts with terminal blocker")
        var upd = journal.db.query(
            "UPDATE processes SET status=?,output_json=?,lease_owner=NULL,lease_expires_at=NULL,updated_at=?,finished_at=? WHERE run_id=? AND id=? AND status='waiting' AND lease_owner IS NULL AND lease_expires_at IS NULL"
        )
        upd.bind_text(1, process_status)
        upd.bind_text(2, output_json)
        upd.bind_text(3, now)
        upd.bind_text(4, now)
        upd.bind_text(5, run_id)
        upd.bind_text(6, process_id)
        _ = upd.step()
        if journal.db.changes() != 1:
            _error("fala journal: process completion lost compare-and-set")
        if has_blocker and bstatus == "open":
            var hb = journal.db.query(
                "UPDATE homeostats SET status=?,values_json=?,updated_at=? WHERE run_id=? AND id=? AND status='open'"
            )
            hb.bind_text(1, blocker_status)
            hb.bind_text(2, output_json)
            hb.bind_text(3, now)
            hb.bind_text(4, run_id)
            hb.bind_text(5, blocker_id)
            _ = hb.step()
            if journal.db.changes() != 1:
                _error("fala journal: blocker completion lost compare-and-set")
        var ru = journal.db.query(
            "UPDATE runs SET status=?,updated_at=?,finished_at=? WHERE id=? AND status NOT IN ('completed','failed','cancelled','timed_out')"
        )
        ru.bind_text(1, run_status)
        ru.bind_text(2, now)
        ru.bind_text(3, now)
        ru.bind_text(4, run_id)
        _ = ru.step()
        if journal.db.changes() != 1 and rstatus != run_status:
            _error("fala journal: run completion lost compare-and-set")
        journal.db.commit()
        journal.close()
        return _lifecycle_json(run_id, process_id, True, process_status, run_status)
    except err:
        try:
            journal.db.rollback()
        except roll_err:
            pass
        try:
            journal.close()
        except close_err:
            pass
        raise err^


def record_process_start(
    path: String,
    run_id: String,
    process_id: String,
    process_type: String,
    input_json: String,
    metadata_json: String,
    now: String,
) raises:
    if run_id == "" or process_id == "" or process_type == "":
        _error("fala.record_in_process: run_id, process_id, and process_type must not be blank")
    var journal = _open(path)
    try:
        journal.db.begin_immediate()
        var run = journal.db.query("SELECT 1 FROM runs WHERE id=?")
        run.bind_text(1, run_id)
        if not run.step():
            _error("fala.record_in_process: unknown run: " + run_id)
        var insert = journal.db.query(
            "INSERT INTO processes (run_id,id,process_type,status,priority,attempt,max_attempts,available_at,input_json,output_json,error_json,metadata,created_at,updated_at,started_at,output_schema_json) VALUES (?,?,?,'running',0,1,1,?,?,'{}','{}',?,?,?,?,'{}')"
        )
        insert.bind_text(1, run_id)
        insert.bind_text(2, process_id)
        insert.bind_text(3, process_type)
        insert.bind_text(4, now)
        insert.bind_text(5, input_json)
        insert.bind_text(6, metadata_json)
        insert.bind_text(7, now)
        insert.bind_text(8, now)
        insert.bind_text(9, now)
        _ = insert.step()
        journal.db.commit()
        journal.close()
    except err:
        try:
            journal.db.rollback()
        except roll_err:
            pass
        try:
            journal.close()
        except close_err:
            pass
        raise err^


def record_process_finish(
    path: String,
    run_id: String,
    process_id: String,
    status: String,
    output_json: String,
    error_json: String,
    now: String,
) raises:
    var journal = NativeJournal.open(path)
    try:
        journal.db.begin_immediate()
        var upd = journal.db.query(
            "UPDATE processes SET status=?,output_json=?,error_json=?,updated_at=?,finished_at=? WHERE run_id=? AND id=? AND status='running'"
        )
        upd.bind_text(1, status)
        upd.bind_text(2, output_json)
        upd.bind_text(3, error_json)
        upd.bind_text(4, now)
        upd.bind_text(5, now)
        upd.bind_text(6, run_id)
        upd.bind_text(7, process_id)
        _ = upd.step()
        if journal.db.changes() != 1:
            _error("fala.record_in_process: active process row disappeared")
        journal.db.commit()
        journal.close()
    except err:
        try:
            journal.db.rollback()
        except roll_err:
            pass
        try:
            journal.close()
        except close_err:
            pass
        raise err^


def _lifecycle_json(
    run_id: String,
    process_id: String,
    changed: Bool,
    process_status: String,
    run_status: String,
) -> String:
    var flag = "true" if changed else "false"
    return (
        "{\"run_id\":" + quote_json_string(run_id)
        + ",\"process_id\":" + quote_json_string(process_id)
        + ",\"changed\":" + flag
        + ",\"process_status\":" + quote_json_string(process_status)
        + ",\"run_status\":" + quote_json_string(run_status) + "}"
    )
