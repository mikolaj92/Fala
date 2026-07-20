"""MemoryDriver + NativeFunctionRegistry e2e."""

from std.collections import List
from fala.adapters import NativeFunctionRegistry
from fala.correlation import CorrelationEffectorSpec, CorrelationPathSpec
from fala.memory_driver import MemoryDriver
from fala.status import ProcessStatus


def _root_fn(input_json: String, config_json: String) raises -> String:
    return "{\"value\":7}"


def _leaf_fn(input_json: String, config_json: String) raises -> String:
    return "{\"done\":true}"


def _one(value: String) -> List[String]:
    var values = List[String]()
    values.append(value)
    return values^


def main() raises:
    var registry = NativeFunctionRegistry()
    registry.register("pkg.root", _root_fn)
    registry.register("pkg.leaf", _leaf_fn)

    var driver = MemoryDriver("memory://registry-e2e")
    driver.bind_registry(registry)
    driver.register_ref("root", "pkg.root")
    driver.register_ref("leaf", "pkg.leaf")

    _ = driver.runtime.create_run("run_reg")
    driver.runtime.set_run_status("run_reg", "active")

    var root = CorrelationEffectorSpec.create("root", "source")
    var leaf = CorrelationEffectorSpec.create("leaf", "sink", _one("root"))
    var effectors = List[CorrelationEffectorSpec]()
    effectors.append(root^)
    effectors.append(leaf^)
    var path = CorrelationPathSpec("chain", effectors^)
    var plan = driver.runtime.instantiate_path(path, "run_reg")

    _ = driver.drive_until_idle(path, "run_reg", max_ticks=16)

    var root_p = driver.runtime.journal.get_process(plan.processes[0].id)
    var leaf_p = driver.runtime.journal.get_process(plan.processes[1].id)
    if root_p.status != ProcessStatus.succeeded():
        raise Error("root failed: " + root_p.status.value)
    if leaf_p.status != ProcessStatus.succeeded():
        raise Error("leaf failed: " + leaf_p.status.value)

    var root_out = driver.runtime.extras[plan.processes[0].id].output_json
    if root_out.find("7") < 0:
        raise Error("registry root output missing value 7")

    print("core driver registry smoke ok")
