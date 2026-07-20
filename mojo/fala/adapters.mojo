"""Standalone native adapter boundaries for Fala (no std.python)."""

from std.collections import Dict
from std.utils.numerics import isfinite
from emberjson import Value
from std.collections import List
from std.pathlib import Path, cwd
from std.os import makedirs, remove
from .json import canonical_json_text
from .native_process_host import ProcessHost, start as start_native_process
from .reactions import sha256_bytes

struct AdapterKind(Copyable, Movable):
    var value: String

    def __init__(out self, value: String):
        self.value = value

    @staticmethod
    def subprocess() -> AdapterKind: return AdapterKind("subprocess")
    @staticmethod
    def manual_homeostat() -> AdapterKind: return AdapterKind("manual_homeostat")
    @staticmethod
    def native_function() -> AdapterKind: return AdapterKind("native_function")

    def is_known(self) -> Bool:
        return (
            self.value == "subprocess" or self.value == "manual_homeostat"
            or self.value == "native_function"
        )
    def __eq__(self, other: Self) -> Bool: return self.value == other.value
    def __ne__(self, other: Self) -> Bool: return self.value != other.value
    def __str__(self) -> String: return self.value


struct AdapterError(Copyable, Movable):
    var code: String
    var message: String

    def __init__(out self, code: String = "", message: String = ""):
        self.code = code
        self.message = message

    @staticmethod
    def none() -> AdapterError: return AdapterError()
    @staticmethod
    def invalid(message: String) -> AdapterError:
        return AdapterError("invalid_adapter", message)
    @staticmethod
    def subprocess_transport_unavailable() -> AdapterError:
        return AdapterError("subprocess_transport_unavailable", "direct argv subprocess transport is not available in this Mojo build; provide a native process host")
    @staticmethod
    def subprocess_startup(message: String) -> AdapterError:
        return AdapterError("adapter_startup_failed", "subprocess adapter failed to start: " + message)
    @staticmethod
    def subprocess_timeout() -> AdapterError:
        return AdapterError("adapter_timeout", "subprocess adapter timed out")
    @staticmethod
    def subprocess_failed(message: String) -> AdapterError:
        return AdapterError("adapter_failed", "subprocess adapter failed" + ((": " + message) if message != "" else ""))
    @staticmethod
    def subprocess_missing_output() -> AdapterError:
        return AdapterError("adapter_missing_output", "subprocess adapter did not write output/result.json")
    @staticmethod
    def subprocess_invalid_result(message: String) -> AdapterError:
        return AdapterError("adapter_invalid_result", "subprocess adapter result is invalid: " + message)
    @staticmethod
    def timeout_unavailable() -> AdapterError:
        return AdapterError("adapter_timeout_unavailable", "positive adapter timeout_seconds cannot be enforced in this Mojo build")
    @staticmethod
    def native_function_unavailable(value: String) -> AdapterError:
        return AdapterError("native_function_unavailable", "native_function ref is not callable in this native host: " + value)
    @staticmethod
    def native_function_failed(value: String, detail: String) -> AdapterError:
        var message = "native_function callable failed: " + value
        if detail != "": message += ": " + detail
        return AdapterError("native_function_failed", message)
    @staticmethod
    def native_function_not_registered(value: String) -> AdapterError:
        return AdapterError("native_function_not_registered", "native_function ref is not registered: " + value)
    @staticmethod
    def unsupported_fala_runtime() -> AdapterError:
        return AdapterError("unsupported_fala_runtime", "fala_runtime is not part of Fala; use subprocess with a separate journal")
    def is_ok(self) -> Bool: return self.code == ""

comptime NativeFunction = def(String, String) thin raises -> String

struct NativeFunctionInvocation(Copyable, Movable):
    """Result returned by the typed registry after one invocation."""
    var registered: Bool
    var output_json: String
    var error_message: String

    def __init__(out self, registered: Bool = False, output_json: String = "", error_message: String = ""):
        self.registered = registered
        self.output_json = output_json
        self.error_message = error_message

@fieldwise_init
struct NativeFunctionRegistry(Copyable, Movable):
    """Typed native-function registry.

    Registration binds a stable ref and optional canonical object input to a
    concrete, non-capturing Mojo function pointer. The registry owns both ref
    membership and invocation; durable adapter metadata stores only the stable
    ref because function pointers are process-local.
    """
    var refs: List[String]
    var inputs: List[String]
    var callables: List[NativeFunction]

    def __init__(out self):
        self.refs = List[String]()
        self.inputs = List[String]()
        self.callables = List[NativeFunction]()

    def register(mut self, `ref`: String, callable: NativeFunction) raises:
        self.register_for_input(`ref`, "*", callable)
    def register_callable(mut self, `ref`: String, callable: NativeFunction) raises:
        self.register(`ref`, callable)

    def register_for_input(mut self, `ref`: String, input_json: String, callable: NativeFunction) raises:
        if `ref` == "": raise Error("native_function registry ref must not be empty")
        if input_json == "": raise Error("native_function registry input_json must not be empty")
        var normalized_input = input_json
        if input_json != "*":
            var input_error = _validate_json_text(input_json, "native_function input_json")
            if not input_error.is_ok(): raise Error(input_error.message)
            normalized_input = canonical_json_text(input_json)
        var index = 0
        while index < len(self.refs):
            if self.refs[index] == `ref` and self.inputs[index] == normalized_input:
                self.callables[index] = callable
                return
            index += 1
        self.refs.append(`ref`)
        self.inputs.append(normalized_input)
        self.callables.append(callable)

    def execute(self, `ref`: String, input_json: String, config_json: String) -> NativeFunctionInvocation:
        var input_error = _validate_json_text(input_json, "native_function input_json")
        if not input_error.is_ok(): return NativeFunctionInvocation(error_message=input_error.message)
        var config_error = _validate_json_text(config_json, "native_function config_json")
        if not config_error.is_ok(): return NativeFunctionInvocation(error_message=config_error.message)
        try:
            var normalized_input = canonical_json_text(input_json)
            var normalized_config = canonical_json_text(config_json)
            var exact = -1
            var fallback = -1
            var index = 0
            while index < len(self.refs):
                if self.refs[index] == `ref`:
                    if self.inputs[index] == normalized_input: exact = index
                    elif self.inputs[index] == "*": fallback = index
                index += 1
            var selected = exact if exact >= 0 else fallback
            if selected < 0: return NativeFunctionInvocation()
            try:
                var output = self.callables[selected](normalized_input, normalized_config)
                return NativeFunctionInvocation(registered=True, output_json=output)
            except err:
                return NativeFunctionInvocation(registered=True, error_message=String(err))
        except err:
            return NativeFunctionInvocation(error_message=String(err))

def _validate_json_text(value: String, field: String) -> AdapterError:
    try:
        var parsed = Value(parse_string=value)
        if not parsed.is_object(): return AdapterError.invalid(field + " must be a JSON object")
    except err:
        return AdapterError.invalid(field + " must contain valid JSON")
    return AdapterError.none()

def _base_environment_keys() -> List[String]:
    var keys = List[String]()
    keys.append("PATH"); keys.append("HOME"); keys.append("TMPDIR")
    keys.append("LANG"); keys.append("LC_ALL"); keys.append("TZ")
    return keys^

def _lookup_environment(inherited: Dict[String, String], key: String) -> String:
    for pair in inherited.items():
        if pair.key == key: return pair.value.copy()
    return ""

def _has_environment(inherited: Dict[String, String], key: String) -> Bool:
    for pair in inherited.items():
        if pair.key == key: return True
    return False

def _redaction_values(environment: Dict[String, String]) -> List[String]:
    var secrets = List[String]()
    for pair in environment.items():
        if pair.value != "": secrets.append(pair.value.copy())
    var i = 1
    while i < len(secrets):
        var current = secrets[i].copy()
        var j = i
        while j > 0 and secrets[j - 1].byte_length() < current.byte_length():
            secrets[j] = secrets[j - 1].copy()
            j -= 1
        secrets[j] = current^
        i += 1
    return secrets^

def _redacted_env_value(value: String) -> String:
    if value.startswith("${env:") and value.endswith("}"): return value.copy()
    return "<redacted>"


def resolve_environment(spec: AdapterSpec, inherited: Dict[String, String] = Dict[String, String]()) raises -> Dict[String, String]:
    """Resolve minimal base and explicitly allowlisted environment values."""
    var resolved = Dict[String, String]()
    for key in _base_environment_keys():
        if _has_environment(inherited, key): resolved[key] = _lookup_environment(inherited, key)
    for pair in spec.env.items():
        var value = pair.value.copy()
        if value.startswith("${env:") and value.endswith("}"):
            var key = String(value[byte=6:value.byte_length() - 1])
            value = _lookup_environment(inherited, key)
        resolved[pair.key] = value^
    for key in spec.inherit_env:
        if not _has_environment(inherited, key): raise Error("inherited environment key is not allowlisted: " + key)
        resolved[key] = _lookup_environment(inherited, key)
    return resolved^

def redact_environment(value: String, environment: Dict[String, String]) -> String:
    """Redact secrets longest-first with the reference marker."""
    var redacted = value.copy()
    for secret in _redaction_values(environment):
        var index = 0
        var replaced = String()
        while index < redacted.byte_length():
            if index + secret.byte_length() <= redacted.byte_length() and redacted[byte=index:index + secret.byte_length()] == secret:
                replaced += "<redacted>"
                index += secret.byte_length()
            else:
                replaced += redacted[byte=index]
                index += 1
        redacted = replaced^
    return redacted



@fieldwise_init
struct AdapterSpec(Copyable, Movable):
    """Adapter options; timeout_seconds 0 means omitted, otherwise must be > 0."""
    var kind: AdapterKind
    var command: List[String]
    var `ref`: String
    var runtime_ref: String
    var cwd: String
    var env: Dict[String, String]
    var inherit_env: List[String]
    var timeout_seconds: Float64
    def __init__(out self, kind: AdapterKind):
        self.kind = kind.copy()
        self.command = List[String]()
        self.`ref` = ""
        self.runtime_ref = ""
        self.cwd = ""
        self.env = Dict[String, String]()
        self.inherit_env = List[String]()
        self.timeout_seconds = 0.0

    @staticmethod
    def subprocess(command: List[String]) -> AdapterSpec:
        var value = AdapterSpec(AdapterKind.subprocess())
        value.command = command.copy()
        return value^
    @staticmethod
    def manual_homeostat() -> AdapterSpec:
        return AdapterSpec(AdapterKind.manual_homeostat())
    @staticmethod
    def native_function(value: String) -> AdapterSpec:
        var spec = AdapterSpec(AdapterKind.native_function())
        spec.`ref` = value
        return spec^

    def validate(self) -> AdapterError:
        if self.kind.value == "fala_runtime":
            return AdapterError.unsupported_fala_runtime()
        if not self.kind.is_known(): return AdapterError.invalid("unknown adapter kind: " + self.kind.value)
        if self.timeout_seconds < 0.0: return AdapterError.invalid("timeout_seconds must not be negative")
        if not isfinite(self.timeout_seconds): return AdapterError.invalid("timeout_seconds must be finite")
        if self.kind == AdapterKind.subprocess():
            if len(self.command) == 0: return AdapterError.invalid("subprocess requires non-empty argv command")
            if self.`ref` != "" or self.runtime_ref != "": return AdapterError.invalid("subprocess cannot define ref or runtime_ref")
            return AdapterError.none()
        if self.kind == AdapterKind.manual_homeostat():
            if len(self.command) != 0 or self.`ref` != "" or self.runtime_ref != "": return AdapterError.invalid("manual_homeostat does not accept command, ref, or runtime_ref")
            if self.cwd != "" or len(self.env) != 0 or len(self.inherit_env) != 0: return AdapterError.invalid("manual_homeostat does not accept cwd or environment")
            if self.timeout_seconds != 0.0: return AdapterError.invalid("manual_homeostat cannot define timeout_seconds")
            return AdapterError.none()
        if self.kind == AdapterKind.native_function():
            if self.`ref` == "": return AdapterError.invalid("function adapter requires ref")
            if len(self.command) != 0 or self.runtime_ref != "": return AdapterError.invalid("function adapter cannot define command or runtime_ref")
            if self.cwd != "" or len(self.env) != 0 or len(self.inherit_env) != 0: return AdapterError.invalid("function adapter does not accept cwd or environment")
            return AdapterError.none()
        return AdapterError.invalid("unknown adapter kind")


struct EffectorRequest(Copyable, Movable):
    var process_id: String
    var adapter: AdapterSpec
    var impulse_id: String
    var input_json: String
    var config_json: String
    var work_dir: String

    def __init__(out self, process_id: String, adapter: AdapterSpec, impulse_id: String = "", input_json: String = "{}", config_json: String = "{}", work_dir: String = ""):
        self.process_id = process_id
        self.adapter = adapter.copy()
        self.impulse_id = impulse_id
        self.input_json = input_json
        self.config_json = config_json
        self.work_dir = work_dir

@fieldwise_init
struct EffectorResult(Copyable, Movable):
    var success: Bool
    var output_json: String
    var stdout: String
    var stderr: String
    var returncode: Int
    var waiting: Bool
    var homeostat_id: String
    var metadata_json: String
    var error: AdapterError
    @staticmethod
    def failure(error: AdapterError) -> EffectorResult:
        return EffectorResult(success=False, output_json="{}", stdout="", stderr="", returncode=-1, waiting=False, homeostat_id="", metadata_json="{}", error=error.copy())


struct SubprocessBoundary(Movable):
    var argv: List[String]
    var input_dir: String
    var output_dir: String
    var manifest_path: String
    var output_path: String
    var environment: Dict[String, String]

    def __init__(out self, argv: List[String], root: String):
        self.argv = argv.copy()
        self.input_dir = root + "/input"
        self.output_dir = root + "/output"
        self.manifest_path = self.input_dir + "/manifest.json"
        self.output_path = self.output_dir + "/result.json"
        self.environment = Dict[String, String]()
        self.environment["FALA_EFFECTOR_INPUT_DIR"] = self.input_dir
        self.environment["FALA_EFFECTOR_OUTPUT_DIR"] = self.output_dir
        self.environment["FALA_EFFECTOR_MANIFEST"] = self.manifest_path


def _json_escape(value: String) -> String:
    var result = String()
    for ch in value.codepoint_slices():
        if ch == "\\": result += "\\\\"
        elif ch == "\"": result += "\\\""
        elif ch == "\n": result += "\\n"
        elif ch == "\r": result += "\\r"
        elif ch == "\t": result += "\\t"
        else: result += ch
    return result^

def _json_quoted(value: String) -> String:
    return "\"" + _json_escape(value) + "\""

def _json_string_list(values: List[String]) -> String:
    var result = "["
    var first = True
    for value in values:
        if not first: result += ","
        result += _json_quoted(value)
        first = False
    return result + "]"

def _json_string_map(values: Dict[String, String]) -> String:
    var result = "{"
    var first = True
    for pair in values.items():
        if not first: result += ","
        result += _json_quoted(pair.key) + ":" + _json_quoted(pair.value)
        first = False
    return result + "}"

def _adapter_metadata_json(adapter: AdapterSpec) -> String:
    var result = "{\"kind\":" + _json_quoted(adapter.kind.value)
    if adapter.`ref` != "": result += ",\"ref\":" + _json_quoted(adapter.`ref`)
    if adapter.runtime_ref != "": result += ",\"runtime_ref\":" + _json_quoted(adapter.runtime_ref)
    if len(adapter.command) != 0: result += ",\"command\":" + _json_string_list(adapter.command)
    if adapter.cwd != "": result += ",\"cwd\":" + _json_quoted(adapter.cwd)
    if len(adapter.env) != 0:
        result += ",\"env\":{"
        var env_first = True
        for pair in adapter.env.items():
            if not env_first: result += ","
            result += _json_quoted(pair.key) + ":" + _json_quoted(_redacted_env_value(pair.value))
            env_first = False
        result += "}"
    if len(adapter.inherit_env) != 0: result += ",\"inherit_env\":" + _json_string_list(adapter.inherit_env)
    if adapter.timeout_seconds > 0.0: result += ",\"timeout_seconds\":" + String(adapter.timeout_seconds)
    return result + "}"
def adapter_spec_json(adapter: AdapterSpec) raises -> String:
    """Canonical JSON representation used by durable adapter bindings."""
    return canonical_json_text(_adapter_metadata_json(adapter))


def adapter_spec_from_json(text: String) raises -> AdapterSpec:
    """Decode and validate one explicit adapter binding; never infer kind."""
    var parsed = Value(parse_string=text)
    if not parsed.is_object(): raise Error("adapter binding must be a JSON object")
    var root = parsed.object().copy()
    for pair in root.items():
        if pair.key != "kind" and pair.key != "ref" and pair.key != "runtime_ref" and pair.key != "cwd" and pair.key != "timeout_seconds" and pair.key != "command" and pair.key != "inherit_env" and pair.key != "env":
            raise Error("adapter binding contains unknown key: " + pair.key)
    if "kind" not in root or not root["kind"].is_string():
        raise Error("adapter binding requires string kind")
    var adapter = AdapterSpec(AdapterKind(root["kind"].string()))
    if "ref" in root:
        if not root["ref"].is_string(): raise Error("adapter binding ref must be a string")
        adapter.`ref` = root["ref"].string()
    if "runtime_ref" in root:
        if not root["runtime_ref"].is_string(): raise Error("adapter binding runtime_ref must be a string")
        adapter.runtime_ref = root["runtime_ref"].string()
    if "cwd" in root:
        if not root["cwd"].is_string(): raise Error("adapter binding cwd must be a string")
        adapter.cwd = root["cwd"].string()
    if "timeout_seconds" in root:
        if root["timeout_seconds"].is_float(): adapter.timeout_seconds = root["timeout_seconds"].float()
        elif root["timeout_seconds"].is_int(): adapter.timeout_seconds = Float64(root["timeout_seconds"].int())
        elif root["timeout_seconds"].is_uint(): adapter.timeout_seconds = Float64(root["timeout_seconds"].uint())
        else: raise Error("adapter binding timeout_seconds must be a number")
        if adapter.timeout_seconds == 0.0: raise Error("adapter binding timeout_seconds must be greater than 0 when provided")
    if "command" in root:
        if not root["command"].is_array(): raise Error("adapter binding command must be an array")
        for item in root["command"].array():
            if not item.is_string(): raise Error("adapter binding command entries must be strings")
            adapter.command.append(item.string())
    if "inherit_env" in root:
        if not root["inherit_env"].is_array(): raise Error("adapter binding inherit_env must be an array")
        for item in root["inherit_env"].array():
            if not item.is_string(): raise Error("adapter binding inherit_env entries must be strings")
            adapter.inherit_env.append(item.string())
    if "env" in root:
        if not root["env"].is_object(): raise Error("adapter binding env must be an object")
        for pair in root["env"].object().items():
            if not pair.value.is_string(): raise Error("adapter binding env values must be strings")
            adapter.env[pair.key] = pair.value.string()
    var validation = adapter.validate()
    if not validation.is_ok(): raise Error(validation.message)
    return adapter^


def adapter_manifest_json(request: EffectorRequest) raises -> String:
    var adapter_error = request.adapter.validate()
    if not adapter_error.is_ok(): raise Error(adapter_error.message)
    var input_error = _validate_json_text(request.input_json, "request.input_json")
    if not input_error.is_ok(): raise Error(input_error.message)
    var config_error = _validate_json_text(request.config_json, "request.config_json")
    if not config_error.is_ok(): raise Error(config_error.message)
    return "{\"process_id\":" + _json_quoted(request.process_id) + ",\"impulse_id\":" + _json_quoted(request.impulse_id) + ",\"input\":" + request.input_json + ",\"config\":" + request.config_json + ",\"adapter\":" + _adapter_metadata_json(request.adapter) + "}"
def adapter_result_json(result: EffectorResult) raises -> String:
    var output_error = _validate_json_text(result.output_json, "result.output_json")
    if not output_error.is_ok(): raise Error(output_error.message)
    var metadata_error = _validate_json_text(result.metadata_json, "result.metadata_json")
    if not metadata_error.is_ok(): raise Error(metadata_error.message)
    return "{\"success\":" + ("true" if result.success else "false") + ",\"waiting\":" + ("true" if result.waiting else "false") + ",\"output\":" + result.output_json + ",\"stdout\":" + _json_quoted(result.stdout) + ",\"stderr\":" + _json_quoted(result.stderr) + ",\"returncode\":" + String(result.returncode) + ",\"homeostat_id\":" + _json_quoted(result.homeostat_id) + ",\"metadata\":" + result.metadata_json + ",\"error_code\":" + _json_quoted(result.error.code) + ",\"error_message\":" + _json_quoted(result.error.message) + "}"
def _environment_entries(environment: Dict[String, String]) -> List[String]:
    var entries = List[String]()
    for pair in environment.items():
        entries.append(pair.key + "=" + pair.value)
    return entries^

def _read_text_or_empty(path: String) -> String:
    try:
        return Path(path).read_text()
    except err:
        return ""

def execute_subprocess(request: EffectorRequest, inherited_env: Dict[String, String] = Dict[String, String]()) -> EffectorResult:
    """Execute one direct-argv effector through the Darwin process host."""
    var validation = request.adapter.validate()
    if not validation.is_ok(): return EffectorResult.failure(validation)
    var input_error = _validate_json_text(request.input_json, "request.input_json")
    if not input_error.is_ok(): return EffectorResult.failure(input_error)
    var config_error = _validate_json_text(request.config_json, "request.config_json")
    if not config_error.is_ok(): return EffectorResult.failure(config_error)
    if request.adapter.kind != AdapterKind.subprocess():
        return EffectorResult.failure(AdapterError.invalid("execute_subprocess requires subprocess adapter"))

    var root = request.work_dir
    if root != "":
        try:
            var boundary_root = Path(root)
            if not root.startswith("/"):
                boundary_root = cwd() / boundary_root
            root = boundary_root.__fspath__()
        except err:
            return EffectorResult.failure(AdapterError.subprocess_startup(String(err)))
    else:
        try:
            root = (cwd() / Path(".fala-effector-" + sha256_bytes(request.process_id + ":" + request.impulse_id))).__fspath__()
        except err:
            return EffectorResult.failure(AdapterError.subprocess_startup(String(err)))
    var boundary = SubprocessBoundary(request.adapter.command, root)
    try:
        makedirs(Path(boundary.input_dir), exist_ok=True)
        makedirs(Path(boundary.output_dir), exist_ok=True)
        Path(boundary.manifest_path).write_text(adapter_manifest_json(request))
    except err:
        return EffectorResult.failure(AdapterError.subprocess_startup(String(err)))
    try:
        for stale in [boundary.output_path, boundary.output_dir + "/stdout.txt", boundary.output_dir + "/stderr.txt"]:
            if Path(stale).exists(): remove(Path(stale))
    except err:
        return EffectorResult.failure(AdapterError.subprocess_startup(String(err)))

    var environment: Dict[String, String]
    try:
        environment = resolve_environment(request.adapter, inherited_env)
    except err:
        return EffectorResult.failure(AdapterError.subprocess_startup(String(err)))
    for pair in boundary.environment.items():
        environment[pair.key] = pair.value.copy()
    var env_entries = _environment_entries(environment)
    var stdout_path = boundary.output_dir + "/stdout.txt"
    var stderr_path = boundary.output_dir + "/stderr.txt"
    var timeout_ms = -1
    if request.adapter.timeout_seconds > 0.0:
        timeout_ms = Int(request.adapter.timeout_seconds * 1000.0)
        if timeout_ms <= 0: timeout_ms = 1
    var process: ProcessHost
    try:
        process = start_native_process(
            request.adapter.command,
            env_entries,
            request.adapter.cwd,
            "",
            stdout_path,
            stderr_path,
            timeout_ms,
            100,
        )
    except err:
        return EffectorResult.failure(AdapterError.subprocess_startup(String(err)))
    var wait_status = 0
    try:
        wait_status = process.wait_result()
    except err:
        var stdout = redact_environment(_read_text_or_empty(stdout_path), environment)
        var stderr = redact_environment(_read_text_or_empty(stderr_path), environment)
        return EffectorResult(success=False, output_json="{}", stdout=stdout, stderr=stderr, returncode=-1, waiting=False, homeostat_id="", metadata_json="{}", error=AdapterError.subprocess_failed(String(err)))
    var timed_out = False
    try: timed_out = process.was_timed_out()
    except: timed_out = wait_status == 3
    var exit_code = -1
    try: exit_code = process.exit_code()
    except: pass
    var signal = 0
    try: signal = process.signal()
    except: pass
    var pid = -1
    try: pid = process.pid()
    except: pass
    var stdout = redact_environment(_read_text_or_empty(stdout_path), environment)
    var stderr = redact_environment(_read_text_or_empty(stderr_path), environment)
    if timed_out or wait_status == 3:
        return EffectorResult(success=False, output_json="{}", stdout=stdout, stderr=stderr, returncode=-1, waiting=False, homeostat_id="", metadata_json="{}", error=AdapterError.subprocess_timeout())
    if wait_status != 0 or signal != 0 or exit_code != 0:
        var detail = stderr
        if detail == "": detail = "exit " + String(exit_code)
        return EffectorResult(success=False, output_json="{}", stdout=stdout, stderr=stderr, returncode=exit_code, waiting=False, homeostat_id="", metadata_json="{}", error=AdapterError.subprocess_failed(redact_environment(detail, environment)))
    if not Path(boundary.output_path).exists():
        return EffectorResult(success=False, output_json="{}", stdout=stdout, stderr=stderr, returncode=exit_code, waiting=False, homeostat_id="", metadata_json="{}", error=AdapterError.subprocess_missing_output())
    var output_text = _read_text_or_empty(boundary.output_path)
    if output_text == "": return EffectorResult(success=False, output_json="{}", stdout=stdout, stderr=stderr, returncode=exit_code, waiting=False, homeostat_id="", metadata_json="{}", error=AdapterError.subprocess_invalid_result("result.json is empty or unreadable"))
    try:
        var parsed = Value(parse_string=output_text)
        if not parsed.is_object(): return EffectorResult(success=False, output_json="{}", stdout=stdout, stderr=stderr, returncode=exit_code, waiting=False, homeostat_id="", metadata_json="{}", error=AdapterError.subprocess_invalid_result("result.json must contain an object"))
        var output = redact_environment(canonical_json_text(output_text), environment)
        var metadata = "{\"pid\":" + String(pid) + ",\"signal\":" + String(signal) + "}"
        var success_result = EffectorResult(success=True, output_json=output, stdout=stdout, stderr=stderr, returncode=exit_code, waiting=False, homeostat_id="", metadata_json=metadata, error=AdapterError.none())
        return success_result^
    except err:
        return EffectorResult(success=False, output_json="{}", stdout=stdout, stderr=stderr, returncode=exit_code, waiting=False, homeostat_id="", metadata_json="{}", error=AdapterError.subprocess_invalid_result(String(err)))

def execute_manual_homeostat(request: EffectorRequest) -> EffectorResult:
    var validation = request.adapter.validate()
    if not validation.is_ok(): return EffectorResult.failure(validation)
    var input_error = _validate_json_text(request.input_json, "request.input_json")
    if not input_error.is_ok(): return EffectorResult.failure(input_error)
    var config_error = _validate_json_text(request.config_json, "request.config_json")
    if not config_error.is_ok(): return EffectorResult.failure(config_error)
    if request.adapter.kind != AdapterKind.manual_homeostat(): return EffectorResult.failure(AdapterError.invalid("execute_manual_homeostat requires manual_homeostat adapter"))
    return EffectorResult(success=True, output_json="{\"status\":\"waiting\"}", stdout="", stderr="", returncode=0, waiting=True, homeostat_id="homeostat:" + request.process_id, metadata_json="{\"status\":\"open\"}", error=AdapterError())

def execute_native_function(request: EffectorRequest, registry: NativeFunctionRegistry) -> EffectorResult:
    var validation = request.adapter.validate()
    if not validation.is_ok(): return EffectorResult.failure(validation)
    var input_error = _validate_json_text(request.input_json, "request.input_json")
    if not input_error.is_ok(): return EffectorResult.failure(input_error)
    var config_error = _validate_json_text(request.config_json, "request.config_json")
    if not config_error.is_ok(): return EffectorResult.failure(config_error)
    if request.adapter.kind != AdapterKind.native_function(): return EffectorResult.failure(AdapterError.invalid("execute_native_function requires native_function adapter"))
    var invocation = registry.execute(request.adapter.`ref`, request.input_json, request.config_json)
    if not invocation.registered:
        if invocation.error_message != "": return EffectorResult.failure(AdapterError.invalid(invocation.error_message))
        return EffectorResult.failure(AdapterError.native_function_not_registered(request.adapter.`ref`))
    if invocation.error_message != "": return EffectorResult.failure(AdapterError.native_function_failed(request.adapter.`ref`, invocation.error_message))
    var output_error = _validate_json_text(invocation.output_json, "native_function output_json")
    if not output_error.is_ok(): return EffectorResult.failure(output_error)
    var output = ""
    try:
        output = canonical_json_text(invocation.output_json)
    except err:
        return EffectorResult.failure(AdapterError.native_function_failed(request.adapter.`ref`, String(err)))
    return EffectorResult(success=True, output_json=output, stdout="", stderr="", returncode=0, waiting=False, homeostat_id="", metadata_json="{\"registry_ref\":" + _json_quoted(request.adapter.`ref`) + "}", error=AdapterError())
