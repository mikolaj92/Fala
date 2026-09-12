"""Helpers for POSIX APIs that require mutable C strings."""

from std.collections import List


def mutable_c_string(value: String) -> List[UInt8]:
    """Copy UTF-8 text into NUL-terminated, writable byte storage."""
    var buffer = List[UInt8]()
    for index in range(value.byte_length()):
        buffer.append(value.as_bytes()[index])
    buffer.append(UInt8(0))
    return buffer^
