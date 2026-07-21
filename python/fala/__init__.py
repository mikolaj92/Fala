"""Fala — exclusive Mojo correlator + thin optional Python host binding.

Product truth is ``mojo/fala``. This package exposes:

- **host**: ``host_drive`` / ``open_memory`` (memory path), ``open_sqlite`` (durable probe)
- **sdk**: pure-Python effector helpers (``FALA_EFFECTOR_*``) for subprocess organs

There is **no** CPython ``RuntimeBackendService`` engine. Orchestration is Mojo.
"""

from __future__ import annotations

from fala import sdk as sdk
from fala.host import (
    MemoryHost,
    host_drive,
    host_drive_json,
    host_run_package,
    open_memory,
    open_sqlite,
)

__all__ = [
    "MemoryHost",
    "host_drive",
    "host_drive_json",
    "host_run_package",
    "open_memory",
    "open_sqlite",
    "sdk",
    "__version__",
]
__version__ = "0.7.2"
