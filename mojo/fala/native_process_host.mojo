"""Mojo wrapper for the native direct-argv process host ABI.

The process host is loaded as a sibling native library. The wrapper owns the
library for at least as long as each process handle and never invokes a shell.
"""

from std.collections import List
from std.ffi import CStringSlice, OwnedDLHandle, c_int, c_long_long, external_call
from std.memory import MutUnsafePointer, UnsafePointer, alloc
from std.pathlib import Path, cwd
from std.sys import CompilationTarget
from std.sys import argv

comptime CStr = MutUnsafePointer[Int8, MutUntrackedOrigin]
comptime HostPtr = MutUnsafePointer[UInt8, MutUntrackedOrigin]
comptime HostOut = MutUnsafePointer[HostPtr, MutUntrackedOrigin]

comptime StartFn = def(CStr, c_int, CStr, c_int, CStr, CStr, CStr, CStr, c_long_long, c_long_long, HostOut) thin abi("C") -> c_int
comptime DestroyFn = def(HostPtr) thin abi("C")
comptime WaitFn = def(HostPtr) thin abi("C") -> c_int
comptime StatusFn = def(HostPtr) thin abi("C") -> c_int
comptime IntGetterFn = def(HostPtr) thin abi("C") -> c_int
comptime ErrorFn = def(HostPtr) thin abi("C") -> CStr

comptime PROCESS_OK: Int = 0
comptime PROCESS_INVALID_ARGUMENT: Int = 1
comptime PROCESS_SYSTEM_ERROR: Int = 2
comptime PROCESS_TIMED_OUT: Int = 3
comptime PROCESS_CANCELLED: Int = 4

comptime PROCESS_RUNNING: Int = 0
comptime PROCESS_EXITED: Int = 1
comptime PROCESS_SIGNALED: Int = 2
comptime PROCESS_STATUS_TIMED_OUT: Int = 3
comptime PROCESS_STATUS_CANCELLED: Int = 4
comptime PROCESS_STATUS_ERROR: Int = 5


@fieldwise_init
struct ProcessHostError(Copyable, Movable):
    var code: Int
    var message: String

    def __str__(self) -> String:
        return "native process error (" + String(self.code) + "): " + self.message


def _as_cstr(value: String) -> CStr:
    return CStr(unsafe_from_address=Int(value.as_bytes().unsafe_ptr()))


def _blob(values: List[String], label: String) raises -> String:
    var result = String()
    for value in values:
        if value.find("\0") >= 0:
            raise Error(label + " entries must not contain NUL")
        result += value + "\0"
    return result


def _library_path() raises -> String:
    """Find the packaged host beside bin/fala, then the source build."""
    if not CompilationTarget.is_macos():
        raise Error("fala native process host requires Darwin")
    var size = alloc[UInt32](1)
    size[] = UInt32(1)
    var probe = alloc[UInt8](1)
    var result = external_call["_NSGetExecutablePath", c_int](probe, size)
    probe.free()
    if result == 0:
        size.free()
        raise Error("fala process host: unable to determine executable path")
    var buffer = alloc[UInt8](Int(size[]))
    result = external_call["_NSGetExecutablePath", c_int](buffer, size)
    if result != 0:
        size.free()
        buffer.free()
        raise Error("fala process host: unable to determine executable path")
    var raw = String(unsafe_from_utf8_ptr=buffer)
    size.free()
    buffer.free()
    var raw_text = raw + "\0"
    var canonical_buffer = alloc[UInt8](4096)
    var canonical = external_call["realpath", UnsafePointer[UInt8, MutUntrackedOrigin]](raw_text.as_bytes().unsafe_ptr(), canonical_buffer)
    var executable = raw
    if Int(canonical) != 0:
        executable = String(unsafe_from_utf8_ptr=canonical)
    canonical_buffer.free()
    var slash = -1
    var index = 0
    while index < executable.byte_length():
        if executable[byte=index] == "/": slash = index
        index += 1
    if slash <= 0: raise Error("fala process host: executable path has no directory")
    var directory = String(executable[byte=0:slash])
    var packaged = directory + "/../native/libfala_process_host.dylib"
    if Path(packaged).exists(): return packaged
    var source = (cwd() / Path("../../mojo/fala/native/libfala_process_host.dylib")).__fspath__()
    if Path(source).exists(): return source
    return packaged
def native_process_host_available() -> Bool:
    """Return whether the host loads and exposes the complete ABI."""
    try:
        var library = OwnedDLHandle(_library_path())
        _ = library.get_function[StartFn]("fala_process_start_blob")
        _ = library.get_function[DestroyFn]("fala_process_destroy")
        _ = library.get_function[WaitFn]("fala_process_wait")
        _ = library.get_function[WaitFn]("fala_process_cancel")
        _ = library.get_function[StatusFn]("fala_process_get_status")
        _ = library.get_function[IntGetterFn]("fala_process_get_pid")
        _ = library.get_function[IntGetterFn]("fala_process_get_exit_code")
        _ = library.get_function[IntGetterFn]("fala_process_get_term_signal")
        _ = library.get_function[IntGetterFn]("fala_process_was_timed_out")
        _ = library.get_function[IntGetterFn]("fala_process_was_cancelled")
        _ = library.get_function[IntGetterFn]("fala_process_get_error_code")
        _ = library.get_function[ErrorFn]("fala_process_get_error_message")
        return True
    except:
        return False

struct ProcessHost(Movable):
    """Move-only owner for a native process and its loaded host library."""

    var _library: OwnedDLHandle
    var _start: StartFn
    var _destroy: DestroyFn
    var _wait: WaitFn
    var _cancel: WaitFn
    var _status: StatusFn
    var _pid: IntGetterFn
    var _exit_code: IntGetterFn
    var _signal: IntGetterFn
    var _timed_out: IntGetterFn
    var _cancelled: IntGetterFn
    var _error_code: IntGetterFn
    var _error_message: ErrorFn
    var _process: Int

    def __init__(out self, argv: List[String], env: List[String], cwd_path: String, stdin_path: String, stdout_path: String, stderr_path: String, timeout_ms: Int, terminate_grace_ms: Int) raises:
        self._process = 0
        var library_path = _library_path()
        self._library = OwnedDLHandle(library_path)
        self._start = self._library.get_function[StartFn]("fala_process_start_blob")
        self._destroy = self._library.get_function[DestroyFn]("fala_process_destroy")
        self._wait = self._library.get_function[WaitFn]("fala_process_wait")
        self._cancel = self._library.get_function[WaitFn]("fala_process_cancel")
        self._status = self._library.get_function[StatusFn]("fala_process_get_status")
        self._pid = self._library.get_function[IntGetterFn]("fala_process_get_pid")
        self._exit_code = self._library.get_function[IntGetterFn]("fala_process_get_exit_code")
        self._signal = self._library.get_function[IntGetterFn]("fala_process_get_term_signal")
        self._timed_out = self._library.get_function[IntGetterFn]("fala_process_was_timed_out")
        self._cancelled = self._library.get_function[IntGetterFn]("fala_process_was_cancelled")
        self._error_code = self._library.get_function[IntGetterFn]("fala_process_get_error_code")
        self._error_message = self._library.get_function[ErrorFn]("fala_process_get_error_message")

        var argv_blob = _blob(argv, "argv")
        var env_blob = _blob(env, "environment")
        var argv_c = CStr(unsafe_from_address=Int(argv_blob.as_bytes().unsafe_ptr()))
        var env_c = CStr(unsafe_from_address=Int(env_blob.as_bytes().unsafe_ptr()))
        var cwd_blob = cwd_path + "\0"
        var stdin_blob = stdin_path + "\0"
        var stdout_blob = stdout_path + "\0"
        var stderr_blob = stderr_path + "\0"
        var cwd_c = CStr(unsafe_from_address=Int(cwd_blob.as_bytes().unsafe_ptr()))
        var stdin_c = CStr(unsafe_from_address=Int(stdin_blob.as_bytes().unsafe_ptr()))
        var stdout_c = CStr(unsafe_from_address=Int(stdout_blob.as_bytes().unsafe_ptr()))
        var stderr_c = CStr(unsafe_from_address=Int(stderr_blob.as_bytes().unsafe_ptr()))
        var out = alloc[HostPtr](1)
        var result = self._start(
            argv_c, c_int(len(argv)),
            env_c, c_int(len(env)),
            cwd_c,
            stdin_c,
            stdout_c,
            stderr_c,
            c_long_long(timeout_ms), c_long_long(terminate_grace_ms), HostOut(to=out[]))
        _ = argv_blob
        _ = env_blob
        self._process = Int(out[])
        out.free()
        _ = argv_c
        _ = env_c
        _ = cwd_blob
        _ = stdin_blob
        _ = stdout_blob
        _ = stderr_blob
        if Int(result) != PROCESS_OK:
            var code = Int(result)
            var message = "native process operation failed"
            if self._process != 0:
                var process_ptr = HostPtr(unsafe_from_address=self._process)
                message = String(unsafe_from_utf8_ptr=self._error_message(process_ptr))
                self._destroy(process_ptr)
                self._process = 0
            raise Error(ProcessHostError(code=code, message=message).__str__())
        if self._process == 0:
            raise Error("native process start returned a null handle")

    def __moveinit__(mut self, var other: Self):
        self._library = other._library^
        self._start = other._start
        self._destroy = other._destroy
        self._wait = other._wait
        self._cancel = other._cancel
        self._status = other._status
        self._pid = other._pid
        self._exit_code = other._exit_code
        self._signal = other._signal
        self._timed_out = other._timed_out
        self._cancelled = other._cancelled
        self._error_code = other._error_code
        self._error_message = other._error_message
        self._process = other._process
        other._process = 0

    def __del__(deinit self):
        self.destroy()
    def _require(self) raises -> HostPtr:
        if self._process == 0:
            raise Error("native process handle is closed")
        return HostPtr(unsafe_from_address=self._process)

    def wait_result(mut self) raises -> Int:
        return Int(self._wait(self._require()))

    def wait(mut self) raises:
        var result = self.wait_result()
        if result != PROCESS_OK:
            raise Error(self.error_message())

    def cancel_result(mut self) raises -> Int:
        return Int(self._cancel(self._require()))

    def cancel(mut self) raises:
        var result = self.cancel_result()
        if result != PROCESS_OK and result != PROCESS_CANCELLED:
            raise Error(self.error_message())

    def destroy(mut self):
        if self._process != 0:
            self._destroy(HostPtr(unsafe_from_address=self._process))
            self._process = 0

    def status(self) raises -> Int:
        return Int(self._status(self._require()))

    def pid(self) raises -> Int:
        return Int(self._pid(self._require()))

    def exit_code(self) raises -> Int:
        return Int(self._exit_code(self._require()))

    def signal(self) raises -> Int:
        return Int(self._signal(self._require()))

    def was_timed_out(self) raises -> Bool:
        return self._timed_out(self._require()) != 0

    def was_cancelled(self) raises -> Bool:
        return self._cancelled(self._require()) != 0

    def error_code(self) raises -> Int:
        return Int(self._error_code(self._require()))

    def error_message(self) raises -> String:
        return String(unsafe_from_utf8_ptr=self._error_message(self._require()))


def start(
    argv: List[String],
    env: List[String] = List[String](),
    cwd: String = "",
    stdin_path: String = "",
    stdout_path: String = "",
    stderr_path: String = "",
    timeout_ms: Int = -1,
    terminate_grace_ms: Int = 100,
) raises -> ProcessHost:
    """Start a direct-argv process with explicit environment entries."""
    if len(argv) == 0:
        raise Error("native process argv must not be empty")
    return ProcessHost(argv, env, cwd, stdin_path, stdout_path, stderr_path, timeout_ms, terminate_grace_ms)


def main():
    pass
