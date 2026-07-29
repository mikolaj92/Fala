from std.os import remove
from fala import NativeJournal, AdapterKind, AdapterError, AdapterSpec, AdapterBinding, EffectorRequest, NativeFunctionRegistry, EffectorResult, persist_adapter_binding, load_adapter_bindings, adapter_spec_json, adapter_spec_from_json, adapter_result_json, execute_native_function, drive_once, drive_bound_queue, drive_all_runs


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("adapter persistence smoke: " + message)


def _wildcard_select(input_json: String, config_json: String) raises -> String:
    return "{\"result\":\"wildcard\"}"


def _exact_select(input_json: String, config_json: String) raises -> String:
    return "{\"result\":\"exact\"}"


def _invalid_output(input_json: String, config_json: String) raises -> String:
    return "not-json"


def _array_output(input_json: String, config_json: String) raises -> String:
    return "[]"


def _echo(input_json: String, config_json: String) raises -> String:
    return "{\"ok\":true}"


def _raising_output(input_json: String, config_json: String) raises -> String:
    raise Error("callable boom")

def _cleanup(path: String):
    try:
        remove(path)
    except err:
        pass


def main() raises:
    var path = "/tmp/fala-adapter-persistence-smoke.sqlite"
    _cleanup(path)
    var journal = NativeJournal.open(path)
    journal.initialize()
    _ = journal.create_run("run-persist", "created", "{}", "2026-01-01T00:00:00Z")
    _ = journal.schedule_process("run-persist", "p-native", "native", "impulse", "{}", "{}", "", 1, 1, "2026-01-01T00:00:00Z")
    var adapter = AdapterSpec(AdapterKind.native_function())
    adapter.`ref` = "echo"
    var binding = AdapterBinding("p-native", adapter, "run-persist")
    persist_adapter_binding(journal, binding, "2026-01-01T00:00:01Z")
    var encoded = adapter_spec_json(adapter)
    _check(adapter_spec_json(adapter_spec_from_json(encoded)) == encoded, "codec round trip")
    var unknown_field_rejected = False
    try:
        _ = adapter_spec_from_json("{\"kind\":\"native_function\",\"ref\":\"echo\",\"runtime_ref\":\"runtime.main\"}")
    except err:
        unknown_field_rejected = String(err).find("unknown key: runtime_ref") >= 0
    _check(unknown_field_rejected, "runtime_ref is rejected as unknown adapter binding key")
    var loaded = load_adapter_bindings(journal, "run-persist")
    _check(len(loaded) == 1 and loaded[0].process_id == "p-native" and loaded[0].adapter.`ref` == "echo", "load before reopen")
    journal.close()

    var reopened = NativeJournal.open(path)
    reopened.initialize()
    var reloaded = load_adapter_bindings(reopened, "run-persist")
    _check(len(reloaded) == 1 and reloaded[0].adapter.`ref` == "echo", "load after reopen")
    var replay_binding = AdapterBinding("p-native", adapter, "run-persist")
    persist_adapter_binding(reopened, replay_binding, "2026-01-01T00:00:01Z")
    var conflict = False
    try:
        var alternate = AdapterSpec.native_function("different")
        persist_adapter_binding(reopened, AdapterBinding("p-native", alternate, "run-persist"), "2026-01-01T00:00:01Z")
    except err:
        conflict = True
    _check(conflict, "conflicting binding rejected")
    var registry = NativeFunctionRegistry()
    # Registry lookup canonicalizes object input, prefers exact matches, and falls back to wildcard.
    var exact_registry = NativeFunctionRegistry()
    exact_registry.register_for_input("select", "*", _wildcard_select)
    exact_registry.register_for_input("select", "{\"value\":1}", _exact_select)
    var exact_result = exact_registry.execute("select", "{ \"value\" : 1 }", "{}")
    _check(exact_result.registered and exact_result.output_json.find("exact") >= 0, "exact canonical registry lookup")
    var wildcard_result = exact_registry.execute("select", "{\"other\":true}", "{}")
    _check(wildcard_result.registered and wildcard_result.output_json.find("wildcard") >= 0, "wildcard registry lookup")
    var malformed_registration = False
    try:
        exact_registry.register("malformed", _invalid_output)
    except err:
        malformed_registration = True
    _check(not malformed_registration, "callable registration accepts deferred output validation")
    var array_registration = False
    try:
        exact_registry.register("array", _array_output)
    except err:
        array_registration = True
    _check(not array_registration, "callable registration accepts deferred output validation")
    exact_registry.register("raising", _raising_output)
    var raised_result = exact_registry.execute("raising", "{}", "{}")
    _check(raised_result.registered and raised_result.error_message.find("callable boom") >= 0, "callable exceptions are captured")
    var invalid_result = exact_registry.execute("malformed", "{}", "{}")
    _check(invalid_result.registered and invalid_result.output_json == "not-json", "invalid output reaches execution validation")
    var array_result = exact_registry.execute("array", "{}", "{}")
    _check(array_result.registered and array_result.output_json == "[]", "array output reaches execution validation")
    var malformed_request = EffectorRequest("p-malformed", AdapterSpec.native_function("malformed"), input_json="{}", config_json="{}")
    var malformed_boundary = execute_native_function(malformed_request, exact_registry)
    _check(not malformed_boundary.success and malformed_boundary.error.code == "invalid_adapter", "malformed callable output is rejected at native boundary")
    var array_request = EffectorRequest("p-array", AdapterSpec.native_function("array"), input_json="{}", config_json="{}")
    var array_boundary = execute_native_function(array_request, exact_registry)
    _check(not array_boundary.success and array_boundary.error.code == "invalid_adapter", "array callable output is rejected at native boundary")
    _check(not AdapterSpec(AdapterKind("unknown")).validate().is_ok(), "unknown adapter kind rejected")
    var empty_command = List[String]()
    _check(not AdapterSpec.subprocess(empty_command).validate().is_ok(), "empty subprocess command rejected")
    var bad_argv0 = AdapterSpec.subprocess([""])
    _check(not bad_argv0.validate().is_ok(), "empty argv0 rejected")
    var bad_argv_control = AdapterSpec.subprocess(["/bin/true", "bad\narg"])
    _check(not bad_argv_control.validate().is_ok(), "newline argv rejected")
    var bad_cwd = AdapterSpec.subprocess(["/bin/true"])
    bad_cwd.cwd = "bad\0cwd"
    _check(not bad_cwd.validate().is_ok(), "NUL cwd rejected")
    var bad_env_name = AdapterSpec.subprocess(["/bin/true"])
    bad_env_name.env["BAD-NAME"] = "value"
    _check(not bad_env_name.validate().is_ok(), "invalid env name rejected")
    var bad_env_value = AdapterSpec.subprocess(["/bin/true"])
    bad_env_value.env["VALID_NAME"] = "value\n"
    _check(not bad_env_value.validate().is_ok(), "newline env value rejected")
    var invalid_manual = AdapterSpec.manual_homeostat()
    invalid_manual.timeout_seconds = 1.0
    _check(not invalid_manual.validate().is_ok(), "manual homeostat timeout rejected")
    var invalid_timeout = AdapterSpec.native_function("timeout")
    invalid_timeout.timeout_seconds = -1.0
    var timeout_error = invalid_timeout.validate()
    _check(timeout_error.code == "invalid_adapter" and timeout_error.message == "timeout_seconds must not be negative", "negative timeout diagnostic")
    var invalid_native = AdapterSpec.native_function("native.invalid")
    invalid_native.cwd = "/tmp"
    _check(not invalid_native.validate().is_ok(), "native function cwd rejected")
    registry.register("echo", _echo)
    var driven = drive_bound_queue(reopened, List[AdapterBinding](), "worker", "2026-01-01T00:00:02Z", "2026-01-01T00:01:00Z", 2, registry, "run-persist")
    _check(driven.ticks == 1 and driven.completed, "persisted binding drives queue")
    var process = reopened.get_process("run-persist", "p-native")
    _check(process.status == "succeeded", "durable process completion")
    _ = reopened.create_run("run-unsupported", "created", "{}", "2026-01-01T00:00:02Z")
    _ = reopened.schedule_process("run-unsupported", "p-unsupported", "native", "impulse", "{}", "{}", "", 1, 1, "2026-01-01T00:00:02Z")
    var unsupported_bindings = List[AdapterBinding]()
    # Unknown adapter kinds fail preflight and are durably failed (no special Python transport).
    unsupported_bindings.append(AdapterBinding("p-unsupported", AdapterSpec(AdapterKind("python_function")), "run-unsupported"))
    var unsupported_drive = drive_all_runs(reopened, unsupported_bindings, "worker-unsupported", "2026-01-01T00:00:02Z", "2026-01-01T00:01:00Z", registry)
    var unsupported_process = reopened.get_process("run-unsupported", "p-unsupported")
    _check(
        not unsupported_drive.idle
            and unsupported_drive.failed
            and unsupported_drive.ticks == 1
            and unsupported_drive.unsupported_mappings == 1
            and unsupported_drive.error.code == "invalid_adapter"
            and unsupported_process.status == "failed"
            and unsupported_process.attempt == 1
            and unsupported_process.output_json == "{}"
            and unsupported_process.error_json.find("unknown adapter kind") >= 0,
        "unsupported binding is durably failed without output"
    )
    _ = reopened.schedule_process("run-unsupported", "p-timeout", "native", "impulse", "{}", "{}", "", 1, 1, "2026-01-01T00:00:02Z")
    var timeout_adapter = AdapterSpec.native_function("timeout.ref")
    timeout_adapter.timeout_seconds = 1.0
    var timeout_result = drive_once(reopened, reopened.get_process("run-unsupported", "p-timeout"), timeout_adapter, "worker-timeout", "2026-01-01T00:00:02Z", "2026-01-01T00:01:00Z", registry)
    var timeout_process = reopened.get_process("run-unsupported", "p-timeout")
    _check(
        timeout_result.ticks == 1
            and timeout_result.failed
            and timeout_result.error.code == "adapter_timeout_unavailable"
            and timeout_result.error.message == "positive adapter timeout_seconds cannot be enforced in this Mojo build"
            and timeout_process.status == "failed"
            and timeout_process.attempt == 1
            and timeout_process.output_json == "{}"
            and timeout_process.error_json.find("adapter_timeout_unavailable") >= 0,
        "positive timeout durably fails without output"
    )

    # Rows without explicit bindings are skipped; malformed binding metadata fails closed.
    _ = reopened.schedule_process("run-persist", "p-unbound", "native", "impulse", "{}", "{}", "", 0, 1, "2026-01-01T00:00:03Z")
    _check(len(load_adapter_bindings(reopened, "run-persist")) == 1, "unbound row does not erase explicit mapping")
    var malformed = reopened.db.query("UPDATE processes SET metadata=? WHERE run_id=? AND id=?")
    malformed.bind_text(1, "{\"__adapter_binding\":{\"kind\":\"native_function\"}}")
    malformed.bind_text(2, "run-persist")
    malformed.bind_text(3, "p-unbound")
    _ = malformed.step()
    malformed.close()
    var malformed_rejected = False
    try:
        _ = load_adapter_bindings(reopened, "run-persist")
    except err:
        malformed_rejected = True
    _check(malformed_rejected, "malformed mapping fails closed")
    reopened.close()
    _cleanup(path)
    print("adapter persistence smoke ok")
