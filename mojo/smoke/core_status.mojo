"""Core pure status smoke — no SQLite."""

from fala.status import ProcessStatus, RunStatus, can_transition_process, can_transition_run


def main() raises:
    if not can_transition_process(ProcessStatus.ready(), ProcessStatus.running()):
        raise Error("ready -> running must be allowed")
    if can_transition_process(ProcessStatus.succeeded(), ProcessStatus.ready()):
        raise Error("terminal -> ready must be rejected")
    if not can_transition_run(RunStatus.created(), RunStatus.active()):
        raise Error("created -> active must be allowed")
    if can_transition_run(RunStatus.completed(), RunStatus.active()):
        raise Error("completed -> active must be rejected")
    print("core status smoke ok")
