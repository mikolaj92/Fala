"""Helpers for POSIX APIs that require mutable C strings."""

from std.collections import List
from std.ffi import c_size_t, external_call
from std.origin import MutUntrackedOrigin


def string_from_c_string(address: Int) -> String:
    """Copy a null-terminated C string; Mojo 1.2 removed String(unsafe_from_utf8_ptr=)."""
    if address == 0:
        return String()
    var bytes = Pointer[Byte, MutUntrackedOrigin](unsafe_from_address=address)
    var length = external_call["strlen", c_size_t](bytes.unsafe_bitcast[Int8]())
    return String(from_utf8_lossy=Span(unsafe_ptr=bytes, length=Int(length)))


def mutable_c_string(value: String) -> List[UInt8]:
    """Copy UTF-8 text into NUL-terminated, writable byte storage."""
    var buffer = List[UInt8]()
    for index in range(value.byte_length()):
        buffer.append(value.as_bytes()[index])
    buffer.append(UInt8(0))
    return buffer^
