from std.os import remove
from std.pathlib import Path
from std.ffi import c_int, external_call
from fala import AdapterSpec, NativeFunctionRegistry
from fala.journal import NativeJournal, ProcessRow
from fala.native_process_host import start
from fala.durable_subprocess import wait_durable_subprocess
from fala.native_driver import drive_once


def expect(value: Bool, message: String) raises:
    if not value: raise Error(message)


def _cleanup(paths: List[String]) raises:
    for path in paths:
        try: remove(path)
        except: pass


def main() raises:
    var db_path = "/tmp/fala-cancel.sqlite"; var pid_path = "/tmp/fala-cancel-grandchild.pid"
    _cleanup([db_path, db_path + "-wal", db_path + "-shm", pid_path])
    var journal = NativeJournal(db_path); journal.initialize()
    _ = journal.create_run("cancel-run", "active", "{}", "2026-01-01T00:00:00Z")
    _ = journal.schedule_process("cancel-run", "child", "correlation", "2026-01-01T00:00:00Z")
    _ = journal.claim_process("cancel-run", "child", "worker", "2026-01-01T00:00:01Z", "2099-01-01T00:00:00Z")
    var argv = List[String](); argv.append("/bin/sh"); argv.append("-c"); argv.append("sleep 30 & echo $! > " + pid_path + "; wait")
    var process = start(argv, terminate_grace_ms=20)
    _ = journal.request_cancel_process("cancel-run", "child", "operator", "2026-01-01T00:00:02Z", idempotency_key="operator-cancel")
    var status = wait_durable_subprocess(journal, "cancel-run", "child", process, "worker", "2026-01-01T00:00:03Z")
    expect(status == "cancelled" and journal.get_process("cancel-run", "child").status == "cancelled", "durable request reaches terminal")
    _ = journal.request_cancel_process("cancel-run", "child", "operator", "2026-01-01T00:00:02Z", idempotency_key="operator-cancel")
    var events = journal.list_events("cancel-run"); var terminal = 0; var request = 0; var signal = 0
    for event in events:
        if event.event_type == "process.cancelled": terminal += 1
        if event.event_type == "process.cancel_requested": request += 1
        if event.event_type == "process.cancel.signal": signal += 1
    expect(terminal == 1 and request == 1 and signal == 1, "idempotent cancel and one terminal causal trace")
    if Path(pid_path).is_file():
        var pid = Int(Path(pid_path).read_text())
        expect(external_call["kill", c_int](c_int(pid), c_int(0)) != 0, "grandchild process group is gone")
    journal.close()

    var drive_db = "/tmp/fala-cancel-drive.sqlite"
    var drive_pid = "/tmp/fala-cancel-drive.pid"
    var canceler_path = "/tmp/fala-cancel-drive.py"
    _cleanup([drive_db, drive_db + "-wal", drive_db + "-shm", drive_pid, canceler_path])
    var drive_journal = NativeJournal(drive_db)
    drive_journal.initialize()
    _ = drive_journal.create_run("drive-cancel", "active", "{}", "2026-01-01T00:00:00Z")
    var drive_row = drive_journal.schedule_process(
        "drive-cancel", "child", "correlation", "2026-01-01T00:00:00Z"
    )
    Path(canceler_path).write_text(
        "import sqlite3,sys,time\n"
        + "db,run_id,process_id=sys.argv[1:4]\n"
        + "con=sqlite3.connect(db,timeout=30)\n"
        + "con.execute('PRAGMA busy_timeout=30000')\n"
        + "deadline=time.time()+8\n"
        + "while time.time()<deadline:\n"
        + "    row=con.execute('select status from processes where run_id=? and id=?',(run_id,process_id)).fetchone()\n"
        + "    if row and row[0]=='running':\n"
        + "        time.sleep(1.2)\n"
        + "        con.execute(\"update processes set status='cancel_requested',updated_at='2026-01-01T00:00:03Z' where run_id=? and id=? and status='running'\",(run_id,process_id))\n"
        + "        con.commit()\n"
        + "        break\n"
        + "    time.sleep(0.02)\n"
    )
    var canceler_argv = List[String]()
    canceler_argv.append("/usr/bin/env")
    canceler_argv.append("python3")
    canceler_argv.append(canceler_path)
    canceler_argv.append(drive_db)
    canceler_argv.append("drive-cancel")
    canceler_argv.append("child")
    var canceler = start(canceler_argv, terminate_grace_ms=20)
    var command = List[String]()
    command.append("/bin/sh")
    command.append("-c")
    command.append("sleep 8 & echo $! > " + drive_pid + "; wait")
    var adapter = AdapterSpec.subprocess(command)
    var driven = drive_once(
        drive_journal,
        drive_row,
        adapter,
        "worker",
        "2026-01-01T00:00:01Z",
        "2099-01-01T00:00:00Z",
        NativeFunctionRegistry(),
        realtime_timestamps=True,
    )
    _ = canceler.wait_result()
    var driven_row = drive_journal.get_process("drive-cancel", "child")
    expect(
        driven_row.status == "cancelled" and not driven.completed
        and driven_row.started_at < driven_row.finished_at,
        "drive_once live-cancel reaches a timed cancelled terminal, got " + driven_row.status
        + " started_at=" + driven_row.started_at + " finished_at=" + driven_row.finished_at,
    )
    var drive_events = drive_journal.list_events("drive-cancel")
    var drive_signal = 0
    var drive_terminal = 0
    var drive_signal_at = ""
    var drive_terminal_at = ""
    for event in drive_events:
        if event.event_type == "process.cancel.signal":
            drive_signal += 1
            drive_signal_at = event.created_at
        if event.event_type == "process.cancelled":
            drive_terminal += 1
            drive_terminal_at = event.created_at
    expect(drive_signal == 1 and drive_terminal == 1, "drive_once emits one cancel signal and one cancelled terminal")
    expect(
        driven_row.started_at < drive_signal_at and drive_signal_at <= drive_terminal_at
        and drive_terminal_at == driven_row.finished_at,
        "drive_once cancellation events use actual lifecycle timestamps",
    )
    if Path(drive_pid).is_file():
        var child_pid = Int(Path(drive_pid).read_text())
        expect(external_call["kill", c_int](c_int(child_pid), c_int(0)) != 0, "drive_once grandchild process group is gone")
    drive_journal.close()
    print("durable subprocess cancellation smoke ok")
