from std.collections import List
from std.ffi import CStringSlice, c_int, external_call
from std.memory import UnsafePointer, MutUnsafePointer
from std.memory import alloc
from std.os import remove
from std.pathlib import Path
from fala import (
    PROCESS_OK, PROCESS_EXITED, PROCESS_TIMED_OUT, PROCESS_STATUS_TIMED_OUT,
    start_native_process,
)


def _check(condition: Bool, message: String) raises:
    if not condition:
        raise Error("native process host smoke: " + message)


def _fresh_root() raises -> String:
    var template = "/tmp/fala-native-process-host-XXXXXX\0"
    var c_template = CStringSlice(template)
    var root_ptr = external_call["mkdtemp", UnsafePointer[UInt8, MutUntrackedOrigin]](c_template.unsafe_ptr())
    if Int(root_ptr) == 0:
        raise Error("native process host smoke: unable to create temporary root")
    return String(unsafe_from_utf8_ptr=root_ptr)


def _remove_tree(path: Path) raises:
    if not path.exists():
        return
    if path.is_dir():
        for entry in path.listdir():
            _remove_tree(path / entry.name())
        var text = path.__fspath__() + "\0"
        var c_path = CStringSlice(text)
        var result = external_call["rmdir", c_int](c_path.unsafe_ptr())
        _check(result == 0, "temporary directory cleanup failed")
    else:
        remove(path.__fspath__())


def _argv(first: String, second: String = "", third: String = "") -> List[String]:
    var result = List[String]()
    result.append(first)
    if second != "": result.append(second)
    if third != "": result.append(third)
    return result^

def _implicit_raii(environment: List[String]) raises:
    var process = start_native_process(_argv("/usr/bin/true"), environment)
    _check(process.wait_result() == PROCESS_OK, "implicit RAII wait failed")


def main() raises:
    var root = ""
    try:
        root = _fresh_root()
        var environment = List[String]()
        environment.append("PATH=/usr/bin:/bin")
        environment.append("FALA_SMOKE=ok")

        _implicit_raii(environment)
        var after_raii = start_native_process(_argv("/usr/bin/true"), environment)
        _check(after_raii.wait_result() == PROCESS_OK, "post-RAII process failed")
        after_raii.destroy()
        var success_path = root + "/success.out"
        var success = start_native_process(
            _argv("/usr/bin/printf", "hello %s\\n", "world"),
            environment,
            root,
            "",
            success_path,
            "",
        )
        _check(success.wait_result() == PROCESS_OK, "printf wait failed")
        _check(success.status() == PROCESS_EXITED, "printf did not exit")
        _check(success.exit_code() == 0, "printf exit code")
        _check(Path(success_path).read_text() == "hello world\n", "stdout capture")
        success.destroy()

        var env_path = root + "/environment.out"
        var env_process = start_native_process(
            _argv("/usr/bin/env"), environment, root, "", env_path, "")
        _check(env_process.wait_result() == PROCESS_OK, "env wait failed")
        var env_text = Path(env_path).read_text()
        _check(env_text.find("FALA_SMOKE=ok") >= 0, "supplied environment missing")
        env_process.destroy()

        var nonzero = start_native_process(
            _argv("/usr/bin/false"), environment, root)
        _check(nonzero.wait_result() == PROCESS_OK, "false wait failed")
        _check(nonzero.status() == PROCESS_EXITED and nonzero.exit_code() == 1, "nonzero exit")
        nonzero.destroy()

        var timeout = start_native_process(
            _argv("/bin/sleep", "1"), environment, root, "", "", "", 20, 10)
        _check(timeout.wait_result() == PROCESS_TIMED_OUT, "timeout result")
        _check(timeout.status() == PROCESS_STATUS_TIMED_OUT and timeout.was_timed_out(), "timeout state")
        timeout.destroy()

        var missing_failed = False
        try:
            var missing = start_native_process(
                _argv("fala-process-host-missing"), environment, root)
            missing.destroy()
        except err:
            missing_failed = True
            _check(String(err).find("resolve executable") >= 0, "missing executable diagnostic")
        _check(missing_failed, "missing executable was accepted")
        print("native process host smoke ok: dylib success nonzero timeout missing cwd env stdout")
    except err:
        if root != "":
            _remove_tree(Path(root))
        raise Error(String(err))
    if root != "":
        _remove_tree(Path(root))
