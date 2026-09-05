from std.os import remove
from std.pathlib import Path
from std.ffi import c_int, external_call
from fala.journal import NativeJournal, ProcessRow
from fala.native_process_host import start
from fala.durable_subprocess import wait_durable_subprocess


def expect(value: Bool, message: String) raises:
    if not value: raise Error(message)


def main() raises:
    var db_path = "/tmp/fala-cancel.sqlite"; var pid_path = "/tmp/fala-cancel-grandchild.pid"
    for path in [db_path, db_path + "-wal", db_path + "-shm", pid_path]:
        try: remove(path)
        except: pass
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
    journal.close(); print("durable subprocess cancellation smoke ok")
