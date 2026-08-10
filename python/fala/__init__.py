"""Fala — exclusive Mojo correlator + thin optional Python host binding.

Product truth is ``mojo/fala``. This package exposes:

- **host**: ``host_drive`` / ``open_memory`` (memory path), ``open_sqlite`` /
  ``host_run_package`` / ``delete_terminal_run`` / ``maintain_journal`` /
  ``recover_incomplete`` (durable)
- **inspection**: read-only schema-v6 process lease visibility
- **sdk**: pure-Python effector helpers (``FALA_EFFECTOR_*``) for subprocess organs

There is **no** CPython ``RuntimeBackendService`` engine. Orchestration is Mojo.
"""

from __future__ import annotations

from fala import sdk as sdk
from fala.inspection import inspect_leases
from fala.host import (
    IncompleteRecoveryResult,
    JournalMaintenanceResult,
    MemoryHost,
    delete_terminal_run,
    maintain_journal,
    recover_incomplete,
    host_drive,
    host_drive_json,
    host_run_package,
    open_memory,
    open_sqlite,
    record_in_process,
)

__all__ = [
    "IncompleteRecoveryResult",
    "JournalMaintenanceResult",
    "MemoryHost",
    "__version__",
    "delete_terminal_run",
    "maintain_journal",
    "recover_incomplete",
    "host_drive",
    "host_drive_json",
    "host_run_package",
    "inspect_leases",
    "open_memory",
    "open_sqlite",
    "record_in_process",
    "sdk",
]
__version__ = "0.7.24"
