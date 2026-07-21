"""Python extension: thin in-process Fala *host* surface (memory path).

Not a full runtime re-export. One JSON helper runs create_run → impulse →
instantiate path → drive_until_idle (same shape as core memory e2e).

SQLite / CLI / ops packs stay outside this binding.
"""

from std.collections import Dict, List
from std.os import abort
from std.python import PythonObject
from std.python.bindings import PythonModuleBuilder

from emberjson import Value, to_string
from fala.adapters import AdapterSpec, NativeFunctionRegistry
from fala.correlation import (
    CorrelationEffectorSpec,
    CorrelationInputField,
    CorrelationPathSpec,
    instantiate_correlation_path,
)
from fala.correlation_runtime import run_correlation_path
from fala.domain import Impulse
from fala.journal import NativeJournal
from fala.json import parse_json
from fala.memory_driver import MemoryDriver
from fala.native_driver import AdapterBinding
from fala.package import load_package_json, load_package_toml


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




def open_sqlite_journal(path: PythonObject) raises -> PythonObject:
    """Open (create) a durable SQLite journal at path; close after probe."""
    var p = String(py=path)
    if p == "":
        raise Error("fala.open_sqlite_journal: path required")
    var journal = NativeJournal.open(p)
    journal.close()
    var out = "{\"ok\":true,\"kind\":\"sqlite\",\"path\":\"" + p + "\"}"
    return PythonObject(out)


def host_run_package_json(request: PythonObject) raises -> PythonObject:
    """Durable package run: SQLite journal + TOML package + path drive.

    Request JSON::
      {
        "db_path": "/abs/path.sqlite",
        "package_path": "/abs/fala-package.toml",
        "path_id": "basic_enrichment",
        "run_id": "run1",
        "inputs": {"source": "\"hello\""},   // values already JSON-encoded strings
        "max_ticks": 32,
        "worker_id": "python-host",
        "created_at": "2026-01-01T00:00:00Z",
        "now": "2026-01-01T00:00:01Z",
        "lease_expires_at": "2026-01-01T01:00:00Z"
      }

    Supports package effectors with adapter kind ``native_function`` (no-op
    registry empty → fails if executed) or ``subprocess`` (process host).
    """
    var text = String(py=request)
    var parsed = parse_json(text)
    var root = parsed.value.copy()
    if not root.is_object():
        raise Error("fala.host_run_package_json: root must be object")

    var db_path = _obj_string(root, "db_path")
    var package_path = _obj_string(root, "package_path")
    var path_id = _obj_string(root, "path_id")
    var run_id = _obj_string(root, "run_id", "run")
    if db_path == "" or package_path == "" or path_id == "":
        raise Error("fala.host_run_package_json: db_path, package_path, path_id required")

    var max_ticks = _obj_int(root, "max_ticks", 32)
    var worker_id = _obj_string(root, "worker_id", "python-host")
    var created_at = _obj_string(root, "created_at", "2026-01-01T00:00:00Z")
    var now = _obj_string(root, "now", "2026-01-01T00:00:01Z")
    var lease = _obj_string(root, "lease_expires_at", "2026-01-01T01:00:00Z")

    var manifest = load_package_toml(package_path)
    # JSON packages (Temida materializes YAML → JSON for Mojo host)
    if package_path.find(".json") >= 0:
        manifest = load_package_json(package_path)
    var package_path_spec = manifest.correlation_paths[0].copy()
    var found = False
    for pth in manifest.correlation_paths:
        if pth.id == path_id:
            package_path_spec = pth.copy()
            found = True
            break
    if not found:
        raise Error("fala.host_run_package_json: path_id not in package: " + path_id)

    var effectors = List[CorrelationEffectorSpec]()
    for item in package_path_spec.effectors:
        var spec = CorrelationEffectorSpec.create(
            item.id,
            item.capability,
            item.conduction.copy(),
            item.timeout_seconds,
            item.config_json,
            "{}",
            "{}",
            List[String](),
        )
        effectors.append(spec^)
    var path = CorrelationPathSpec(
        package_path_spec.id,
        effectors^,
        package_path_spec.allow_feedback_cycles,
        package_path_spec.accumulate_upstream_reactions,
    )

    var inputs = List[CorrelationInputField]()
    if "inputs" in root.object() and root.object()["inputs"].is_object():
        for entry in root.object()["inputs"].object().items():
            var v = entry.value.copy()
            # Host encodes values as JSON text (Python side); pass through strings.
            var vjson = String("")
            if v.is_string():
                vjson = v.string()
            else:
                vjson = to_string(v)
            inputs.append(CorrelationInputField(key=entry.key, value_json=vjson))

    # Optional per-effector authored inputs: { "parse": {"document_path": "..."} }
    var per_inputs = Dict[String, List[CorrelationInputField]]()
    if "effector_inputs" in root.object() and root.object()["effector_inputs"].is_object():
        for entry in root.object()["effector_inputs"].object().items():
            var fields = List[CorrelationInputField]()
            var body = entry.value.copy()
            if body.is_object():
                for field in body.object().items():
                    var fv = field.value.copy()
                    var fjson = String("")
                    if fv.is_string():
                        fjson = fv.string()
                    else:
                        fjson = to_string(fv)
                    fields.append(CorrelationInputField(key=field.key, value_json=fjson))
            per_inputs[entry.key] = fields^

    var per_configs = Dict[String, String]()
    if "effector_configs" in root.object() and root.object()["effector_configs"].is_object():
        for entry in root.object()["effector_configs"].object().items():
            var cv = entry.value.copy()
            if cv.is_string():
                per_configs[entry.key] = cv.string()
            else:
                per_configs[entry.key] = to_string(cv)

    var plan = instantiate_correlation_path(
        path,
        run_id,
        input_fields=inputs^,
        per_effector_inputs=per_inputs^,
        per_effector_configs=per_configs^,
    )

    # Map package effector id -> adapter from package
    var kind_by_id = Dict[String, String]()
    var cmd_by_id = Dict[String, List[String]]()
    var ref_by_id = Dict[String, String]()
    for item in package_path_spec.effectors:
        kind_by_id[item.id] = item.adapter_kind
        cmd_by_id[item.id] = item.adapter_command.copy()
        ref_by_id[item.id] = item.adapter_ref

    # Optional command rewrites: { "ping": ["/usr/bin/python", "-m", "mod"] }
    var rewrite = Dict[String, List[String]]()
    if "command_overrides" in root.object() and root.object()["command_overrides"].is_object():
        for entry in root.object()["command_overrides"].object().items():
            var arr = entry.value.copy()
            var cmd = List[String]()
            if arr.is_array():
                for item in arr.array():
                    if item.is_string():
                        cmd.append(item.string())
            rewrite[entry.key] = cmd^

    var bindings = List[AdapterBinding]()
    for index in range(len(plan.processes)):
        var proc = plan.processes[index].copy()
        var eid = proc.effector_id
        var kind = kind_by_id[eid]
        var adapter = AdapterSpec.manual_homeostat()
        if kind == "subprocess":
            var command = cmd_by_id[eid].copy()
            if eid in rewrite:
                command = rewrite[eid].copy()
            adapter = AdapterSpec.subprocess(command^)
            for pe in package_path_spec.effectors:
                if pe.id == eid:
                    adapter.timeout_seconds = pe.timeout_seconds
                    adapter.env = pe.adapter_env.copy()
                    adapter.inherit_env = pe.adapter_inherit_env.copy()
                    adapter.cwd = pe.adapter_cwd
                    break
        elif kind == "native_function":
            adapter = AdapterSpec.native_function(ref_by_id[eid])
        elif kind == "manual_homeostat":
            adapter = AdapterSpec.manual_homeostat()
        else:
            raise Error("fala.host_run_package_json: unsupported adapter kind: " + kind)
        bindings.append(AdapterBinding(process_id=proc.id, adapter=adapter^, run_id=run_id))

    var registry = NativeFunctionRegistry()
    # Optional: host may not register native functions; native_function effectors then fail closed.

    var journal = NativeJournal.open(db_path)
    journal.initialize()
    var result = run_correlation_path(
        journal,
        run_id,
        plan,
        bindings,
        registry,
        created_at,
        worker_id,
        now,
        lease,
        max_ticks,
    )

    var statuses = String("[")
    var procs = journal.list_processes(run_id)
    var i = 0
    while i < len(procs):
        if i > 0:
            statuses += ","
        statuses += (
            "{\"id\":\""
            + procs[i].id
            + "\",\"status\":\""
            + procs[i].status
            + "\"}"
        )
        i += 1
    statuses += "]"
    journal.close()

    var out = (
        "{\"ok\":true,\"run_id\":\""
        + run_id
        + "\",\"run_status\":\""
        + result.run_status
        + "\",\"replayed\":"
        + ("true" if result.replayed else "false")
        + ",\"ticks\":"
        + String(result.drive_result.ticks)
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
        m.def_function[open_sqlite_journal]("open_sqlite_journal")
        m.def_function[host_run_package_json]("host_run_package_json")
        return m.finalize()
    except e:
        abort(String("fala._native init failed: ", e))
