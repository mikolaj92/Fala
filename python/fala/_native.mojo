"""Python extension: thin in-process Fala *host* surface.

Not a full runtime re-export. JSON helpers cover:
- memory path: create_run → impulse → instantiate path → drive_until_idle
- durable path: open_sqlite probe, package drive, terminal-run deletion

Heavy multi-organ CLI / bridge / projections stay on the Mojo CLI surface.
"""

from std.collections import Dict, List
from std.os import abort
from std.pathlib import Path
from std.python import PythonObject
from std.python.bindings import PythonModuleBuilder

from emberjson import Value, to_string
from fala.adapters import (
    AdapterSpec,
    NativeFunctionRegistry,
    materialize_host_environment_into_adapter,
)
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
from fala.package import PackageManifest, load_package_json, load_package_toml
from fala.domain_store import NativeDomainStore
from fala.ops_maintenance import RunDeleteCounts, delete_terminal_run
from fala.native_driver import AdapterBinding
from fala.native_package import serialize_correlation_path_json
from fala.reactions import content_address_json, sha256_raw_bytes

# Durable host identity constants for package-driven runs.
# Keep aligned with published package version (pyproject / releases).
comptime FALA_RUNTIME_VERSION: String = "0.7.18"
comptime FALA_BACKEND_VERSION: String = "native-sqlite"



def _load_package(path: String) raises -> PackageManifest:
    if path.endswith(".json"):
        return load_package_json(path)
    return load_package_toml(path)

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
    var acc_reactions = False
    if path_val.is_object():
        if "accumulate_upstream_reactions" in path_val.object() and path_val.object()["accumulate_upstream_reactions"].is_bool():
            acc_reactions = path_val.object()["accumulate_upstream_reactions"].bool()
    return CorrelationPathSpec(path_id, effectors^, acc_reactions)


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

    var package_bytes = Path(package_path).read_bytes()
    var package_digest = sha256_raw_bytes(package_bytes)
    var manifest = _load_package(package_path)
    var package_id = manifest.id
    var package_version = manifest.version
    if package_id == "" or package_version == "":
        raise Error("fala.host_run_package_json: package id/version required")
    var package_path_spec = manifest.correlation_paths[0].copy()
    var found = False
    for pth in manifest.correlation_paths:
        if pth.id == path_id:
            package_path_spec = pth.copy()
            found = True
            break
    if not found:
        raise Error("fala.host_run_package_json: path_id not in package: " + path_id)
    var correlation_path_digest = content_address_json(
        serialize_correlation_path_json(package_path_spec)
    )
    var effectors = List[CorrelationEffectorSpec]()
    for item in package_path_spec.effectors:
        var spec = CorrelationEffectorSpec.create(
            item.id,
            item.capability,
            item.conduction.copy(),
            item.timeout_seconds,
            item.config_json,
            "{}",
            "{\"retry_policy\":\"" + item.retry_policy + "\"}",
            List[String](),
        )
        effectors.append(spec^)
    var path = CorrelationPathSpec(
        package_path_spec.id,
        effectors^,
        package_path_spec.accumulate_upstream_reactions,
    )

    var inputs = List[CorrelationInputField]()
    if "inputs" in root.object() and root.object()["inputs"].is_object():
        for entry in root.object()["inputs"].object().items():
            # to_string emits canonical JSON text for any Value (incl. strings).
            inputs.append(
                CorrelationInputField(key=entry.key, value_json=to_string(entry.value.copy()))
            )

    # Optional per-effector authored inputs: { "parse": {"document_path": "..."} }
    var per_inputs = Dict[String, List[CorrelationInputField]]()
    if "effector_inputs" in root.object() and root.object()["effector_inputs"].is_object():
        for entry in root.object()["effector_inputs"].object().items():
            var fields = List[CorrelationInputField]()
            var body = entry.value.copy()
            if body.is_object():
                for field in body.object().items():
                    fields.append(
                        CorrelationInputField(
                            key=field.key, value_json=to_string(field.value.copy())
                        )
                    )
            per_inputs[entry.key] = fields^

    var per_configs = Dict[String, String]()
    if "effector_configs" in root.object() and root.object()["effector_configs"].is_object():
        for entry in root.object()["effector_configs"].object().items():
            per_configs[entry.key] = to_string(entry.value.copy())

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
            # Bake host process env into adapter.env (inherit_env → literals).
            if "host_environment" in root.object() and root.object()["host_environment"].is_object():
                var host_env = Dict[String, String]()
                for entry in root.object()["host_environment"].object().items():
                    if entry.value.is_string():
                        host_env[entry.key] = entry.value.string()
                    else:
                        host_env[entry.key] = to_string(entry.value.copy())
                materialize_host_environment_into_adapter(adapter, host_env)
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
        package_id=package_id,
        package_version=package_version,
        package_digest=package_digest,
        correlation_path_id=path_id,
        correlation_path_digest=correlation_path_digest,
        runtime_version=FALA_RUNTIME_VERSION,
        backend_version=FALA_BACKEND_VERSION,
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
        + ",\"package_id\":"
        + _quote_json(package_id)
        + ",\"package_version\":"
        + _quote_json(package_version)
        + ",\"package_digest\":"
        + _quote_json(package_digest)
        + ",\"correlation_path_id\":"
        + _quote_json(path_id)
        + ",\"correlation_path_digest\":"
        + _quote_json(correlation_path_digest)
        + ",\"runtime_version\":"
        + _quote_json(FALA_RUNTIME_VERSION)
        + ",\"backend_version\":"
        + _quote_json(FALA_BACKEND_VERSION)
        + ",\"processes\":"
        + statuses
        + "}"
    )
    return PythonObject(out)


def _quote_json(value: String) -> String:
    var result = "\""
    for i in range(value.byte_length()):
        var ch = value[byte=i]
        if ch == "\"":
            result += "\\\""
        elif ch == "\\":
            result += "\\\\"
        elif ch == "\n":
            result += "\\n"
        elif ch == "\r":
            result += "\\r"
        elif ch == "\t":
            result += "\\t"
        else:
            result += String(ch)
    result += "\""
    return result


def _delete_counts_json(run_id: String, counts: RunDeleteCounts) -> String:
    return (
        "{\"ok\":true"
        + ",\"run_id\":"
        + _quote_json(run_id)
        + ",\"bridge_inbox\":"
        + String(counts.bridge_inbox)
        + ",\"bridge_outbox\":"
        + String(counts.bridge_outbox)
        + ",\"projections\":"
        + String(counts.projections)
        + ",\"homeostats\":"
        + String(counts.homeostats)
        + ",\"processes\":"
        + String(counts.processes)
        + ",\"reactions\":"
        + String(counts.reactions)
        + ",\"associations\":"
        + String(counts.associations)
        + ",\"impulse_relations\":"
        + String(counts.impulse_relations)
        + ",\"impulse_types\":"
        + String(counts.impulse_types)
        + ",\"impulses\":"
        + String(counts.impulses)
        + ",\"runtime_events\":"
        + String(counts.runtime_events)
        + ",\"runtime_commands\":"
        + String(counts.runtime_commands)
        + ",\"runs\":"
        + String(counts.runs)
        + ",\"total\":"
        + String(counts.total())
        + "}"
    )


def delete_terminal_run_json(request: PythonObject) raises -> PythonObject:
    """Delete one terminal durable run via NativeDomainStore transaction.

    Request JSON::
      {"db_path": "/abs/path.sqlite", "run_id": "run-1"}

    Rejects blank run IDs, unknown runs, and non-terminal statuses. Status
    check and deletion share one BEGIN IMMEDIATE transaction.
    """
    var text = String(py=request)
    var parsed = parse_json(text)
    var root = parsed.value.copy()
    if not root.is_object():
        raise Error("fala.delete_terminal_run_json: root must be object")

    var db_path = _obj_string(root, "db_path")
    var run_id = _obj_string(root, "run_id")
    if db_path == "":
        raise Error("fala.delete_terminal_run_json: db_path required")
    if run_id == "":
        raise Error("domain store: run_id must not be empty")

    var store = NativeDomainStore.open(db_path)
    var out = String("")
    try:
        store.initialize()
        var counts = delete_terminal_run(store, run_id)
        out = _delete_counts_json(run_id, counts)
        store.close()
    except err:
        try:
            store.close()
        except close_err:
            pass
        # Surface storage diagnostics as Error for the Python host.
        raise Error(String(err))
    return PythonObject(out)


@export
def PyInit__native() abi("C") -> PythonObject:
    try:
        var m = PythonModuleBuilder("_native")
        m.def_function[host_drive_json]("host_drive_json")
        m.def_function[open_sqlite_journal]("open_sqlite_journal")
        m.def_function[host_run_package_json]("host_run_package_json")
        m.def_function[delete_terminal_run_json]("delete_terminal_run_json")
        return m.finalize()
    except e:
        abort(String("fala._native init failed: ", e))
