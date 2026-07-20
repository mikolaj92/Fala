from fala import NativeJournal, finalize_run


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("finalization smoke: " + message)


def main() raises:
    var journal = NativeJournal.open(":memory:\0")
    journal.initialize()

    _ = journal.create_run("completed-run", "active", "{}", "t0")
    _ = journal.schedule_process("completed-run", "done", "native", "t0")
    _ = journal.claim_process("completed-run", "done", "worker", "t1", "t2")
    _ = journal.complete_process("completed-run", "done", "worker", "t2", "{\"ok\":true}")
    var completed = finalize_run(journal, "completed-run", "idle", 0, "t3")
    _check(completed.status == "completed" and completed.completed_count == 1 and completed.incomplete_count == 0, "completion")
    var transition = NativeJournal.open(":memory:\0")
    transition.initialize()
    _ = transition.create_run("transition-run", "created", "{}", "t0")
    var transitioned = transition.transition_run_status("transition-run", "active", "t1", "transition-key", reason="started")
    _check(not transitioned.replayed and transitioned.run.status == "active", "run transition")
    var transition_replay = transition.transition_run_status("transition-run", "active", "t1", "transition-key", reason="started")
    _check(transition_replay.replayed and transition_replay.run.status == "active", "run transition replay")
    var transition_conflict = False
    try:
        _ = transition.transition_run_status("transition-run", "active", "t1", "transition-key", reason="changed")
    except err:
        transition_conflict = True
    _check(transition_conflict, "run transition conflict")
    var transition_commands = transition.db.query("SELECT count(*) FROM runtime_commands WHERE run_id=? AND idempotency_key=?")
    transition_commands.bind_text(1, "transition-run"); transition_commands.bind_text(2, "transition-key")
    _check(transition_commands.step() and transition_commands.column_int(0) == 1, "run transition command cardinality")
    var transition_events = transition.list_events("transition-run", event_type="run.active")
    _check(len(transition_events) == 1, "run transition event cardinality")
    transition.close()
 
    var replay = finalize_run(journal, "completed-run", "max_ticks", 1, "t4")
    _check(replay.already_terminal and replay.status == "completed", "terminal replay")

    _ = journal.create_run("failed-run", "active", "{}", "t0")
    _ = journal.schedule_process("failed-run", "bad", "native", "t0")
    _ = journal.claim_process("failed-run", "bad", "worker", "t1", "t2")
    _ = journal.fail_process("failed-run", "bad", "worker", "t2", "{\"error\":true}")
    var failed = finalize_run(journal, "failed-run", "idle", 0, "t3")
    _check(failed.status == "failed" and failed.failed_count == 1, "failure")

    _ = journal.create_run("timeout-run", "active", "{}", "t0")
    _ = journal.schedule_process("timeout-run", "pending", "native", "t0")
    var timed_out = finalize_run(journal, "timeout-run", "max_ticks", 1, "t1")
    _check(timed_out.status == "timed_out" and timed_out.incomplete_count == 1, "timeout")
    var timeout_events = journal.list_events("timeout-run", event_type="run.timed_out")
    _check(len(timeout_events) == 1 and timeout_events[0].payload.find("max_ticks") >= 0, "durable timeout reason")


    _ = journal.create_run("retry-run", "active", "{}", "t0")
    _ = journal.schedule_process("retry-run", "retrying", "native", "t0", "{}", "{}", "", 0, 2, "t0")
    _ = journal.claim_process("retry-run", "retrying", "worker", "t1", "t2")
    _ = journal.retry_process("retry-run", "retrying", "worker", "t2", "t2", "{\"error\":true}")
    var retry_final = finalize_run(journal, "retry-run", "failed", 0, "t3")
    _check(retry_final.status == "waiting" and retry_final.incomplete_count == 1, "retry-wait remains resumable")
    var invalid_reason = False
    try:
        _ = finalize_run(journal, "retry-run", "unknown", 0, "t4")
    except err:
        invalid_reason = True
    _check(invalid_reason, "invalid finalization reason rejected")
    _ = journal.create_run("waiting-run", "active", "{}", "t0")
    _ = journal.schedule_process("waiting-run", "waiting", "native", "t0")
    _ = journal.claim_process("waiting-run", "waiting", "worker", "t0", "t2")
    _ = journal.wait_process("waiting-run", "waiting", "worker", "t1", "{\"status\":\"waiting\"}", "wait-key")
    var waiting = finalize_run(journal, "waiting-run", "idle", 0, "t1")
    _check(waiting.status == "waiting" and waiting.waiting_count == 1 and waiting.incomplete_count == 1, "waiting")
    _ = journal.create_run("empty-run", "active", "{}", "t0")
    var empty = finalize_run(journal, "empty-run", "idle", 0, "t1")
    _check(empty.status == "waiting" and empty.total_count == 0, "empty run remains resumable")

    print("finalization smoke ok")
