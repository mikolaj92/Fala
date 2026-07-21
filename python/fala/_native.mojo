"""Python extension: thin in-process Fala *host* surface (memory path).

Not a full runtime re-export. One JSON helper runs create_run → impulse →
instantiate path → drive_until_idle (same shape as core memory e2e).

SQLite / CLI / ops packs stay outside this binding.
"""

from std.collections import List
from std.os import abort
from std.python import PythonObject
from std.python.bindings import PythonModuleBuilder

from emberjson import Value, to_string
from fala.correlation import CorrelationEffectorSpec, CorrelationPathSpec
from fala.domain import Impulse
from fala.json import parse_json
from fala.memory_driver import MemoryDriver


def _obj_string(obj: Value, key: String, default: String = "") raises -> String:
    if not obj.is_object() or key not in obj.object():
        return default
    var item = obj.object()[key].copy()
    if item.is_string():
        return item.string()
    if item.is_null():
        return default
    return to_string(item)


def _obj_int(obj: Value, key: String, default: Int = 0) raises -> Int:
    if not obj.is_object() or key not in obj.object():
        return default
    var item = obj.object()[key].copy()
    if item.is_int():
        return Int(item.int())
    if item.is_uint():
        return Int(item.uint())
    return default


def _path_from_json(path_val: Value) raises -> CorrelationPathSpec:
    var path_id = _obj_string(path_val, "id", "path")
    var effectors = List[CorrelationEffectorSpec]()
    if path_val.is_object() and "effectors" in path_val.object():
        var arr = path_val.object()["effectors"].copy()
        if arr.is_array():
            for item in arr.array():
                if not item.is_object():
                    continue
                var eid = _obj_string(item, "id")
                var cap = _obj_string(item, "capability", eid)
                var cond = List[String]()
                if "conduction" in item.object() and item.object()["conduction"].is_array():
                    for c in item.object()["conduction"].array():
                        if c.is_string():
                            cond.append(c.string())
                effectors.append(CorrelationEffectorSpec.create(eid, cap, cond^))
    return CorrelationPathSpec(path_id, effectors^)


def host_drive_json(request: PythonObject) raises -> PythonObject:
    """Memory-only host: create_run + accept_impulse + instantiate + drive."""
    var text = String(py=request)
    var parsed = parse_json(text)
    var root = parsed.value.copy()
    if not root.is_object():
        raise Error("fala.host_drive_json: root must be object")

    var stream = _obj_string(root, "stream_id", "memory://host")
    var run_id = _obj_string(root, "run_id", "run")
    var title = _obj_string(root, "title", "")
    var max_ticks = _obj_int(root, "max_ticks", 16)

    var driver = MemoryDriver(stream)
    _ = driver.runtime.create_run(run_id, title=title)
    driver.runtime.set_run_status(run_id, "active")

    if "impulse" in root.object():
        var imp = root.object()["impulse"].copy()
        var iid = _obj_string(imp, "id", "imp1")
        var itype = _obj_string(imp, "type", "case")
        var payload = "{}"
        if "payload" in imp.object():
            var p = imp.object()["payload"].copy()
            if p.is_string():
                payload = p.string()
            else:
                payload = to_string(p)
        driver.runtime.accept_impulse(Impulse(iid, run_id, itype, payload))

    if "path" not in root.object():
        raise Error("fala.host_drive_json: path required")
    var path = _path_from_json(root.object()["path"].copy())
    var plan = driver.runtime.instantiate_path(path, run_id, max_attempts=1)

    if "outputs" in root.object() and root.object()["outputs"].is_object():
        for entry in root.object()["outputs"].object().items():
            var out_v = entry.value.copy()
            var out_s = String("")
            if out_v.is_string():
                out_s = out_v.string()
            else:
                out_s = to_string(out_v)
            driver.register_output(entry.key, out_s)

    var ticks = driver.drive_until_idle(path, run_id, max_ticks=max_ticks)
    var events = driver.runtime.journal.list_events(run_id)

    var statuses = String("[")
    var i = 0
    while i < len(plan.processes):
        var proc = driver.runtime.journal.get_process(plan.processes[i].id)
        if i > 0:
            statuses += ","
        statuses += (
            "{\"id\":\""
            + plan.processes[i].id
            + "\",\"status\":\""
            + proc.status.value
            + "\"}"
        )
        i += 1
    statuses += "]"

    var out = (
        "{\"ok\":true,\"ticks\":"
        + String(ticks)
        + ",\"run_id\":\""
        + run_id
        + "\",\"process_count\":"
        + String(len(plan.processes))
        + ",\"event_count\":"
        + String(len(events))
        + ",\"processes\":"
        + statuses
        + "}"
    )
    return PythonObject(out)


@export
def PyInit__native() abi("C") -> PythonObject:
    try:
        var m = PythonModuleBuilder("_native")
        m.def_function[host_drive_json]("host_drive_json")
        return m.finalize()
    except e:
        abort(String("fala._native init failed: ", e))
