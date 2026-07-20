from std.collections import List
from std.ffi import CStringSlice, c_int, external_call
from std.memory import UnsafePointer
from std.os import remove
from std.pathlib import Path
from fala import AdapterSpec, EffectorRequest, NativeFunctionRegistry, execute_subprocess
from fala.journal import NativeJournal
from fala.native_driver import drive_once


def _check(condition: Bool, message: String) raises:
    if not condition: raise Error("native subprocess smoke: " + message)


def _fresh_root() -> String:
    return "/tmp/fala-native-subprocess-smoke"


def _rmdir(path: String) raises:
    var text = path + "\0"
    var c_path = CStringSlice(text)
    _ = external_call["rmdir", c_int](c_path.unsafe_ptr())

def _cleanup(path: String) raises:
    try: remove(path + "/input/manifest.json")
    except: pass
    try: remove(path + "/output/result.json")
    except: pass
    try: remove(path + "/output/stdout.txt")
    except: pass
    try: remove(path + "/output/stderr.txt")
    except: pass
    _rmdir(path + "/input")
    _rmdir(path + "/output")
    _rmdir(path)


def main() raises:
    var root = ""
    try:
        root = _fresh_root()
        var command = List[String]()
        command.append("/tmp/fala-native-subprocess-fixture")
        command.append("success")
        var adapter = AdapterSpec.subprocess(command)
        adapter.env["SECRET"] = "top-secret"
        var request = EffectorRequest("subprocess-smoke", adapter, "impulse", "{\"value\":1}", "{}", root)
        var result = execute_subprocess(request)
        _check(result.success and result.error.is_ok(), "successful result")
        _check(result.output_json.find("\"ok\":true") >= 0, "result JSON")
        _check(result.stdout.find("<redacted>") >= 0 and result.stdout.find("top-secret") < 0, "stdout redaction")
        _check(result.stderr.find("<redacted>") >= 0 and result.stderr.find("top-secret") < 0, "stderr redaction")
        _check(Path(root + "/input/manifest.json").exists(), "manifest file")
        _check(Path(root + "/output/result.json").exists(), "result file")

        var no_output = AdapterSpec.subprocess(command)
        no_output.command[1] = "no-output"
        no_output.env["SECRET"] = "top-secret"
        var stale = execute_subprocess(EffectorRequest("subprocess-smoke", no_output, "impulse", "{}", "{}", root))
        _check(not stale.success and stale.error.code == "adapter_missing_output", "stale result removed")
        var nonzero = AdapterSpec.subprocess(command)
        nonzero.command[1] = "nonzero"
        nonzero.env["SECRET"] = "top-secret"
        var failed = execute_subprocess(EffectorRequest("subprocess-nonzero", nonzero, "impulse", "{}", "{}", root))
        _check(not failed.success and failed.error.code == "adapter_failed" and failed.returncode == 7, "nonzero exit")


        var timeout = AdapterSpec.subprocess(command)
        timeout.command[1] = "sleep"
        timeout.env["SECRET"] = "top-secret"
        timeout.timeout_seconds = 0.02
        var durable = NativeJournal(":memory:\0")
        durable.initialize()
        _ = durable.create_run("subprocess-durable", "active", "{}", "2026-01-01T00:00:00Z")
        var retry_command = List[String]()
        retry_command.append("/tmp/fala-native-subprocess-fixture")
        retry_command.append("nonzero")
        var retry_adapter = AdapterSpec.subprocess(retry_command)
        retry_adapter.env["SECRET"] = "durable-secret"
        var retry_row = durable.schedule_process(
            "subprocess-durable", "retry", "native", "2026-01-01T00:00:00Z", "{}", "{}", "", 1, 2,
            "2026-01-01T00:00:00Z",
        )
        var retry_first = drive_once(
            durable, retry_row, retry_adapter, "subprocess-worker",
            "2026-01-01T00:00:01Z", "2026-01-01T00:01:00Z", NativeFunctionRegistry(),
        )
        var retry_waiting = durable.get_process("subprocess-durable", "retry")
        _check(
            retry_first.failed
                and retry_first.error.code == "adapter_failed"
                and retry_waiting.status == "retry_wait"
                and retry_waiting.attempt == 1,
            "durable subprocess failure retries",
        )
        var retry_second = drive_once(
            durable, retry_waiting, retry_adapter, "subprocess-worker",
            "2026-01-01T00:00:02Z", "2026-01-01T00:01:00Z", NativeFunctionRegistry(),
        )
        var retry_failed = durable.get_process("subprocess-durable", "retry")
        _check(
            retry_second.failed
                and retry_failed.status == "failed"
                and retry_failed.attempt == 2
                and len(durable.list_commands("subprocess-durable", "process.retry")) == 1
                and len(durable.list_commands("subprocess-durable", "process.fail")) == 1,
            "durable subprocess terminal failure persists",
        )
        var timeout_command = List[String]()
        timeout_command.append("/tmp/fala-native-subprocess-fixture")
        timeout_command.append("sleep")
        var timeout_adapter = AdapterSpec.subprocess(timeout_command)
        timeout_adapter.timeout_seconds = 0.02
        timeout_adapter.env["SECRET"] = "durable-secret"
        var timeout_row = durable.schedule_process(
            "subprocess-durable", "timeout", "native", "2026-01-01T00:00:03Z", "{}", "{}", "", 1, 2,
            "2026-01-01T00:00:03Z",
        )
        var timeout_first = drive_once(
            durable, timeout_row, timeout_adapter, "subprocess-worker",
            "2026-01-01T00:00:04Z", "2026-01-01T00:01:00Z", NativeFunctionRegistry(),
        )
        var timeout_waiting = durable.get_process("subprocess-durable", "timeout")
        _check(
            timeout_first.timed_out
                and timeout_first.error.code == "adapter_timeout"
                and timeout_waiting.status == "retry_wait"
                and timeout_waiting.attempt == 1,
            "durable subprocess timeout retries",
        )
        var timeout_second = drive_once(
            durable, timeout_waiting, timeout_adapter, "subprocess-worker",
            "2026-01-01T00:00:05Z", "2026-01-01T00:01:00Z", NativeFunctionRegistry(),
        )
        var timeout_failed = durable.get_process("subprocess-durable", "timeout")
        _check(
            timeout_second.timed_out
                and timeout_failed.status == "timed_out"
                and timeout_failed.attempt == 2
                and len(durable.list_commands("subprocess-durable", "process.timeout")) == 1,
            "durable subprocess timeout terminal persistence",
        )
        var timed = execute_subprocess(EffectorRequest("subprocess-timeout", timeout, "impulse", "{}", "{}", root))
        _check(not timed.success and timed.error.code == "adapter_timeout", "timeout")
        print("native subprocess smoke ok: manifest result redaction stale-output nonzero timeout")
    except err:
        if root != "": _cleanup(root)
        raise Error(String(err))
    if root != "": _cleanup(root)
