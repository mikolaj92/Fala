"""Mojo wrapper for the native direct-argv process host ABI.

The process host is loaded as a sibling native library. The wrapper owns the
library for at least as long as each process handle and never invokes a shell.
"""

from std.collections import List
from std.ffi import CStringSlice, OwnedDLHandle, c_int, c_long_long, external_call
from std.memory import MutUnsafePointer, UnsafePointer, alloc
from std.pathlib import Path
from std.os import getenv
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
comptime HostGetEnvFn = def(CStr) thin abi("C") -> CStr

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
def _is_environment_name(value: String) -> Bool:
    var n = value.byte_length()
    if n == 0: return False
    var first = value[byte=0]
    if not ((first >= "A" and first <= "Z") or (first >= "a" and first <= "z") or first == "_"):
        return False
    for index in range(1, n):
        var character = value[byte=index]
        if ((character >= "A" and character <= "Z") or (character >= "a" and character <= "z")
                or (character >= "0" and character <= "9") or character == "_"):
            continue
        return False
    return True


def _validate_start_inputs(argv: List[String], env: List[String], cwd_path: String) raises:
    if len(argv) == 0: raise Error("native process argv must not be empty")
    if argv[0] == "": raise Error("native process argv[0] must not be empty")
    for value in argv:
        if value.find("\0") >= 0 or value.find("\n") >= 0 or value.find("\r") >= 0:
            raise Error("native process argv entries must not contain NUL or newline")
    if cwd_path.find("\0") >= 0 or cwd_path.find("\n") >= 0 or cwd_path.find("\r") >= 0:
        raise Error("native process cwd must not contain NUL or newline")
    for entry in env:
        var separator = entry.find("=")
        if separator <= 0:
            raise Error("native process environment entries must be NAME=value")
        var name = String(entry[byte=0:separator])
        if not _is_environment_name(name):
            raise Error("native process environment name is invalid: " + name)
        if entry.find("\0") >= 0 or entry.find("\n") >= 0 or entry.find("\r") >= 0:
            raise Error("native process environment entries must not contain NUL or newline")



def _host_library_name() -> String:
    """Shared library name for this POSIX target (Darwin dylib / Linux so)."""
    if CompilationTarget.is_macos():
        return "libfala_process_host.dylib"
    return "libfala_process_host.so"


def _directory_of(path: String) raises -> String:
    var slash = -1
    var index = 0
    while index < path.byte_length():
        if path[byte=index] == "/": slash = index
        index += 1
    if slash <= 0:
        raise Error("fala process host: path has no directory")
    return String(path[byte=0:slash])


def _realpath_string(path: String) raises -> String:
    var raw_text = path + "\0"
    var canonical_buffer = alloc[UInt8](4096)
    var canonical = external_call["realpath", UnsafePointer[UInt8, MutUntrackedOrigin]](
        raw_text.as_bytes().unsafe_ptr(), canonical_buffer
    )
    if Int(canonical) == 0:
        canonical_buffer.free()
        return path
    var resolved = String(unsafe_from_utf8_ptr=canonical)
    canonical_buffer.free()
    return resolved


def _executable_path() raises -> String:
    """Resolve the current process executable (Darwin or Linux)."""
    if CompilationTarget.is_macos():
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
        return _realpath_string(raw)
    # Linux / other POSIX: /proc/self/exe
    var link = "/proc/self/exe\0"
    var buffer = alloc[UInt8](4096)
    var n = external_call["readlink", c_int](
        CStr(unsafe_from_address=Int(link.as_bytes().unsafe_ptr())),
        buffer,
        c_int(4095),
    )
    if n < 0:
        buffer.free()
        raise Error("fala process host: unable to read /proc/self/exe (POSIX host requires Linux or Darwin)")
    buffer[Int(n)] = 0
    var raw = String(unsafe_from_utf8_ptr=buffer)
    buffer.free()
    return _realpath_string(raw)


def _library_path() raises -> String:
    """Resolve only an explicit or executable-relative process-host library."""
    if not CompilationTarget.is_macos() and not CompilationTarget.is_linux():
        raise Error("fala native process host requires Darwin or Linux")
    var configured = getenv("FALA_PROCESS_HOST_LIBRARY")
    if configured != "":
        if not configured.startswith("/"):
            raise Error("FALA_PROCESS_HOST_LIBRARY must be an absolute path")
        if not Path(configured).is_file():
            raise Error("FALA_PROCESS_HOST_LIBRARY must name an existing regular file")
        return configured
    var executable = _executable_path()
    var packaged = _directory_of(executable) + "/../native/" + _host_library_name()
    if not Path(packaged).is_file():
        raise Error(
            "fala process host library is missing beside the executable; "
            "set FALA_PROCESS_HOST_LIBRARY to an absolute packaged library path"
        )
    return packaged
def native_process_host_available() -> Bool:
    """Return whether the host loads and exposes the complete ABI."""
    try:
        var library = OwnedDLHandle(_library_path())
        _ = library.get_function[c_int]("fala_process_start_blob")
        _ = library.get_function[NoneType]("fala_process_destroy")
        _ = library.get_function[c_int]("fala_process_wait")
        _ = library.get_function[c_int]("fala_process_cancel")
        _ = library.get_function[c_int]("fala_process_get_status")
        _ = library.get_function[c_int]("fala_process_get_pid")
        _ = library.get_function[c_int]("fala_process_get_exit_code")
        _ = library.get_function[c_int]("fala_process_get_term_signal")
        _ = library.get_function[c_int]("fala_process_was_timed_out")
        _ = library.get_function[c_int]("fala_process_was_cancelled")
        _ = library.get_function[c_int]("fala_process_get_error_code")
        _ = library.get_function[CStr]("fala_process_get_error_message")
        _ = library.get_function[CStr]("fala_host_getenv")
        return True
    except:
        return False


struct HostEnvValue(Copyable, Movable):
    """Host getenv result: *present* distinguishes unset from empty string."""

    var present: Int
    var value: String

    def __init__(out self, present: Int = 0, value: String = ""):
        self.present = present
        self.value = value


def host_getenv(name: String) -> HostEnvValue:
    """Look up *name* in the host process environment via the process-host ABI.

    ``present == 1`` when set (value may be empty); ``present == 0`` when unset
    or when the host library cannot be loaded.
    """
    try:
        var library = OwnedDLHandle(_library_path())
        var get_env = library.get_function[CStr]("fala_host_getenv")
        var key = name + "\0"
        var ptr = get_env(CStr(unsafe_from_address=Int(key.as_bytes().unsafe_ptr())))
        if Int(ptr) == 0:
            return HostEnvValue(0, "")
        return HostEnvValue(1, String(unsafe_from_utf8_ptr=ptr))
    except:
        return HostEnvValue(0, "")

struct ProcessHost(Movable):
    """Move-only owner for a native process and its loaded host library."""

    var _library: OwnedDLHandle
    var _process: Int

    def __init__(out self, argv: List[String], env: List[String], cwd_path: String, stdin_path: String, stdout_path: String, stderr_path: String, timeout_ms: Int, terminate_grace_ms: Int) raises:
        _validate_start_inputs(argv, env, cwd_path)
        self._process = 0
        var library_path = _library_path()
        self._library = OwnedDLHandle(library_path)

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
        var result = self._library.get_function[c_int]("fala_process_start_blob")(
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
                message = String(unsafe_from_utf8_ptr=self._library.get_function[CStr]("fala_process_get_error_message")(process_ptr))
                self._library.get_function[NoneType]("fala_process_destroy")(process_ptr)
                self._process = 0
            raise Error(ProcessHostError(code=code, message=message).__str__())
        if self._process == 0:
            raise Error("native process start returned a null handle")

    def __moveinit__(mut self, var other: Self):
        self._library = other._library^
        self._process = other._process
        other._process = 0

    def __del__(deinit self):
        self.destroy()
    def _require(self) raises -> HostPtr:
        if self._process == 0:
            raise Error("native process handle is closed")
        return HostPtr(unsafe_from_address=self._process)

    def wait_result(mut self) raises -> Int:
        return Int(self._library.get_function[c_int]("fala_process_wait")(self._require()))

    def wait(mut self) raises:
        var result = self.wait_result()
        if result != PROCESS_OK:
            raise Error(self.error_message())

    def cancel_result(mut self) raises -> Int:
        return Int(self._library.get_function[c_int]("fala_process_cancel")(self._require()))

    def cancel(mut self) raises:
        var result = self.cancel_result()
        if result != PROCESS_OK and result != PROCESS_CANCELLED:
            raise Error(self.error_message())

    def destroy(mut self):
        if self._process != 0:
            try:
                self._library.get_function[NoneType]("fala_process_destroy")(HostPtr(unsafe_from_address=self._process))
            except:
                pass
            self._process = 0

    def status(self) raises -> Int:
        return Int(self._library.get_function[c_int]("fala_process_get_status")(self._require()))

    def pid(self) raises -> Int:
        return Int(self._library.get_function[c_int]("fala_process_get_pid")(self._require()))

    def exit_code(self) raises -> Int:
        return Int(self._library.get_function[c_int]("fala_process_get_exit_code")(self._require()))

    def signal(self) raises -> Int:
        return Int(self._library.get_function[c_int]("fala_process_get_term_signal")(self._require()))

    def was_timed_out(self) raises -> Bool:
        return self._library.get_function[c_int]("fala_process_was_timed_out")(self._require()) != 0

    def was_cancelled(self) raises -> Bool:
        return self._library.get_function[c_int]("fala_process_was_cancelled")(self._require()) != 0

    def error_code(self) raises -> Int:
        return Int(self._library.get_function[c_int]("fala_process_get_error_code")(self._require()))

    def error_message(self) raises -> String:
        return String(unsafe_from_utf8_ptr=self._library.get_function[CStr]("fala_process_get_error_message")(self._require()))


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
