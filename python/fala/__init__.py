"""Fala — Mojo correlator + thin optional Python *host* binding.

Product truth is ``mojo/fala``. This package only exposes a memory-path host
helper (``host_drive`` / ``open_memory``). CLI, SQLite multi-organ, and ops
remain subprocess/CLI. No dual engine.
"""

from __future__ import annotations

from fala.host import MemoryHost, host_drive, host_drive_json, open_memory

__all__ = [
    "MemoryHost",
    "host_drive",
    "host_drive_json",
    "open_memory",
    "__version__",
]
__version__ = "0.5.0"
