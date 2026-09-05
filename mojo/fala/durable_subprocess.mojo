"""Durable-cancellation bridge for one already-running subprocess handle."""

from std.ffi import c_int, external_call
from fala.journal import NativeJournal
from fala.native_process_host import ProcessHost, PROCESS_RUNNING, PROCESS_STATUS_CANCELLED


def _sleep_ms(milliseconds: Int):
    if milliseconds > 0: _ = external_call["usleep", c_int](c_int(milliseconds * 1000))


def wait_durable_subprocess(mut journal: NativeJournal, run_id: String, process_id: String, mut process: ProcessHost, actor: String, at: String, poll_ms: Int = 10) raises -> String:
    """Poll durable state, cancel process group, and write one terminal row."""
    var cancel_seen = False
    while process.status() == PROCESS_RUNNING:
        var row = journal.get_process(run_id, process_id)
        if row.status == "cancel_requested":
            cancel_seen = True
            _ = journal.append_event(run_id, "process.cancel.signal:" + process_id, "process.cancel.signal", "{\"signal\":\"SIGTERM\"}", at, process_id=process_id, actor=actor)
            _ = process.cancel_result()
            if process.signal() == 9: _ = journal.append_event(run_id, "process.cancel.escalation:" + process_id, "process.cancel.escalation", "{\"signal\":\"SIGKILL\"}", at, process_id=process_id, actor=actor)
            break
        _ = process.poll_result()
        if process.status() == PROCESS_RUNNING: _sleep_ms(poll_ms)
    if process.status() == PROCESS_RUNNING: _ = process.wait_result()
    if cancel_seen or process.was_cancelled() or process.status() == PROCESS_STATUS_CANCELLED:
        var current = journal.get_process(run_id, process_id)
        if current.status == "cancelled": return "cancelled"
        _ = journal.cancel_process(run_id, process_id, actor, at, "{\"code\":\"operator_cancelled\"}")
        return "cancelled"
    return "completed"
