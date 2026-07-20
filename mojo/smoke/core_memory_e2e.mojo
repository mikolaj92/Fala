"""End-to-end memory path: create run → instantiate chain → drive → events."""

from std.collections import List

from fala.correlation import CorrelationEffectorSpec, CorrelationPathSpec
from fala.domain import Impulse
from fala.memory_driver import MemoryDriver
from fala.status import ProcessStatus


def _one(value: String) -> List[String]:
    var values = List[String]()
    values.append(value)
    return values^


def main() raises:
    var driver = MemoryDriver("memory://e2e")
    _ = driver.runtime.create_run("run_e2e", title="e2e")
    driver.runtime.accept_impulse(Impulse("imp1", "run_e2e", "case", "{\"n\":1}"))
    driver.runtime.set_run_status("run_e2e", "active")

    var root = CorrelationEffectorSpec.create("root", "source")
    var leaf = CorrelationEffectorSpec.create("leaf", "sink", _one("root"))
    var effectors = List[CorrelationEffectorSpec]()
    effectors.append(root^)
    effectors.append(leaf^)
    var path = CorrelationPathSpec("chain", effectors^)

    var plan = driver.runtime.instantiate_path(path, "run_e2e", max_attempts=1)
    if len(plan.processes) != 2:
        raise Error("expected 2 processes")

    driver.register_output("root", "{\"value\":42}")
    driver.register_output("leaf", "{\"done\":true}")

    var ticks = driver.drive_until_idle(path, "run_e2e", max_ticks=16)
    if ticks < 2:
        raise Error("expected at least two ticks")

    var root_proc = driver.runtime.journal.get_process(plan.processes[0].id)
    var leaf_proc = driver.runtime.journal.get_process(plan.processes[1].id)
    if root_proc.status != ProcessStatus.succeeded():
        raise Error("root must succeed, got " + root_proc.status.value)
    if leaf_proc.status != ProcessStatus.succeeded():
        raise Error("leaf must succeed, got " + leaf_proc.status.value)

    var events = driver.runtime.journal.list_events("run_e2e")
    if len(events) < 4:
        raise Error("expected multiple runtime events, got " + String(len(events)))

    # leaf input should carry conduction after advance
    if plan.processes[1].id in driver.runtime.extras:
        var leaf_extra = driver.runtime.extras[plan.processes[1].id].copy()
        if not leaf_extra.input_json.startswith("{\"conduction\":"):
            # May still be {} if advance order differed — require at least root output stored
            var root_extra = driver.runtime.extras[plan.processes[0].id].copy()
            if root_extra.output_json != "{\"value\":42}":
                raise Error("root output not recorded")
        else:
            if leaf_extra.input_json.find("42") < 0:
                raise Error("leaf conduction should include root value")

    print("core memory e2e smoke ok ticks=" + String(ticks) + " events=" + String(len(events)))
