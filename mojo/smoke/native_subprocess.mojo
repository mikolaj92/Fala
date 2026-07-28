from std.collections import List
from std.ffi import CStringSlice, c_int, external_call
from std.memory import UnsafePointer
from std.os import remove
from std.pathlib import Path, cwd
from fala import AdapterSpec, EffectorRequest, NativeFunctionRegistry, execute_subprocess
from fala.reactions import sha256_bytes
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
    var attempt_one_root = ""
    var attempt_two_root = ""
    try:
        root = _fresh_root()
        var command = List[String]()
        command.append("/tmp/fala-native-subprocess-fixture")
        command.append("success")
        var adapter = AdapterSpec.subprocess(command)
        adapter.env["SECRET"] = "top-secret"
        var request = EffectorRequest("subprocess-smoke", adapter, "impulse", "{\"value\":1}", "{}", root, run_id="run-smoke")
        var result = execute_subprocess(request)
        _check(result.success and result.error.is_ok(), "successful result")
        _check(result.output_json.find("\"ok\":true") >= 0, "result JSON")
        _check(result.stdout.find("<redacted>") >= 0 and result.stdout.find("top-secret") < 0, "stdout redaction")
        _check(result.stderr.find("<redacted>") >= 0 and result.stderr.find("top-secret") < 0, "stderr redaction")

        # Nonempty short secrets are still fully redacted from operator streams.
        var short_command = List[String]()
        short_command.append("/bin/sh")
        short_command.append("-c")
        short_command.append(
            "printf \"short=$SHORT_SECRET\\n\" >&1; printf \"short=$SHORT_SECRET\\n\" >&2; "
            + "printf '{\"ok\":true}\\n' > \"$FALA_EFFECTOR_OUTPUT_DIR/result.json\""
        )
        var short_adapter = AdapterSpec.subprocess(short_command)
        short_adapter.env["SHORT_SECRET"] = "shrt"
        var short_result = execute_subprocess(
            EffectorRequest("subprocess-short-secret", short_adapter, "impulse", "{}", "{}")
        )
        _check(
            short_result.success
                and short_result.stdout.find("shrt") < 0
                and short_result.stdout.find("<redacted>") >= 0,
            "short stdout redaction",
        )
        _check(
            short_result.stderr.find("shrt") < 0
                and short_result.stderr.find("<redacted>") >= 0,
            "short stderr redaction",
        )
        _check(Path(root + "/input/manifest.json").exists(), "manifest file")
        var manifest = Path(root + "/input/manifest.json").read_text()
        _check(manifest.find("\"protocol_version\":1") >= 0, "manifest protocol version")
        _check(manifest.find("\"execution_id\":\"run-smoke:subprocess-smoke\"") >= 0, "manifest execution id")
        _check(manifest.find("\"attempt\":1") >= 0 and manifest.find("\"max_attempts\":1") >= 0, "manifest attempt context")
        _check(Path(root + "/output/result.json").exists(), "result file")

        # Retries preserve logical identity but never share physical files.
        attempt_one_root = (
            cwd()
            / Path(
                ".fala-effector-"
                + sha256_bytes("run-smoke:subprocess-boundary:impulse:1")
            )
        ).__fspath__()
        attempt_two_root = (
            cwd()
            / Path(
                ".fala-effector-"
                + sha256_bytes("run-smoke:subprocess-boundary:impulse:2")
            )
        ).__fspath__()
        _cleanup(attempt_one_root)
        _cleanup(attempt_two_root)
        var attempt_one = execute_subprocess(
            EffectorRequest(
                "subprocess-boundary", adapter, "impulse", "{}", "{}",
                attempt=1, max_attempts=2, run_id="run-smoke",
            )
        )
        var attempt_two = execute_subprocess(
            EffectorRequest(
                "subprocess-boundary", adapter, "impulse", "{}", "{}",
                attempt=2, max_attempts=2, run_id="run-smoke",
            )
        )
        _check(attempt_one.success and attempt_two.success, "attempt boundary execution")
        _check(attempt_one_root != attempt_two_root, "distinct attempt roots")
        var attempt_one_manifest = Path(attempt_one_root + "/input/manifest.json").read_text()
        var attempt_two_manifest = Path(attempt_two_root + "/input/manifest.json").read_text()
        _check(
            attempt_one_manifest.find("\"execution_id\":\"run-smoke:subprocess-boundary\"") >= 0
                and attempt_two_manifest.find("\"execution_id\":\"run-smoke:subprocess-boundary\"") >= 0,
            "stable execution identity across attempt roots",
        )
        _check(
            attempt_one_manifest.find("\"attempt\":1") >= 0
                and attempt_two_manifest.find("\"attempt\":2") >= 0,
            "physical attempt manifests",
        )

        # Multi-byte UTF-8 on stdout/stderr must not abort the host during env
        # redaction (StringSlice codepoint-boundary assert). #121.
        var unicode_command = List[String]()
        unicode_command.append("/bin/sh")
        unicode_command.append("-c")
        # secret + Polish text: redaction walks codepoints, not raw bytes.
        unicode_command.append(
            "printf 'sekret=top-secret żółć héllo 世界\\n' >&1; "
            + "printf 'err top-secret ąę\\n' >&2; "
            + "printf '{\"ok\":true}\\n' > \"$FALA_EFFECTOR_OUTPUT_DIR/result.json\""
        )
        var unicode_adapter = AdapterSpec.subprocess(unicode_command)
        unicode_adapter.env["SECRET"] = "top-secret"
        var unicode_result = execute_subprocess(
            EffectorRequest("subprocess-unicode", unicode_adapter, "impulse", "{}", "{}", root)
        )
        _check(unicode_result.success and unicode_result.error.is_ok(), "unicode stream success")
        _check(
            unicode_result.stdout.find("<redacted>") >= 0
                and unicode_result.stdout.find("top-secret") < 0
                and unicode_result.stdout.find("żółć") >= 0,
            "unicode stdout redaction",
        )
        _check(
            unicode_result.stderr.find("<redacted>") >= 0
                and unicode_result.stderr.find("top-secret") < 0,
            "unicode stderr redaction",
        )

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

        # Failed subprocess with multi-byte stderr (execute path).
        var fail_unicode = List[String]()
        fail_unicode.append("/bin/sh")
        fail_unicode.append("-c")
        fail_unicode.append(
            "printf 'błąd krytyczny: żółć ąę\\n' >&2; exit 9"
        )
        var fail_adapter = AdapterSpec.subprocess(fail_unicode)
        var fail_result = execute_subprocess(
            EffectorRequest("subprocess-fail-unicode", fail_adapter, "impulse", "{}", "{}", root)
        )
        _check(
            not fail_result.success
                and fail_result.error.code == "adapter_failed"
                and fail_result.returncode == 9
                and fail_result.stderr.find("żółć") >= 0,
            "unicode stderr failure",
        )


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

        # drive_once path: multi-byte error messages must JSON-quote without abort.
        var fail_row = durable.schedule_process(
            "subprocess-durable", "fail-unicode", "native", "2026-01-01T00:00:10Z", "{}", "{}", "", 1, 1,
            "2026-01-01T00:00:10Z",
        )
        var fail_drive = drive_once(
            durable, fail_row, fail_adapter, "subprocess-worker",
            "2026-01-01T00:00:11Z", "2026-01-01T00:01:00Z", NativeFunctionRegistry(),
        )
        var fail_done = durable.get_process("subprocess-durable", "fail-unicode")
        _check(
            fail_drive.failed and fail_done.status == "failed",
            "unicode failure durable terminal",
        )

        print("native subprocess smoke ok: manifest result redaction stale-output nonzero timeout unicode")
    except err:
        if root != "": _cleanup(root)
        if attempt_one_root != "": _cleanup(attempt_one_root)
        if attempt_two_root != "": _cleanup(attempt_two_root)
        raise Error(String(err))
    if root != "": _cleanup(root)
    if attempt_one_root != "": _cleanup(attempt_one_root)
    if attempt_two_root != "": _cleanup(attempt_two_root)
