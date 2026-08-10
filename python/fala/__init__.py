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
from fala.host import (
    IncompleteRecoveryResult,
    JournalMaintenanceResult,
    MemoryHost,
    delete_terminal_run,
    host_drive,
    host_drive_json,
    host_run_package,
    maintain_journal,
    open_memory,
    open_sqlite,
    record_in_process,
    recover_incomplete,
)
from fala.inspection import inspect_leases
from fala.journal import (
    LifecycleResult,
    complete_waiting_process,
    ensure_journal,
    finalize_run,
    park_process,
    transition_run,
    upsert_process,
    upsert_run_metadata,
)

__all__ = [
    "IncompleteRecoveryResult",
    "JournalMaintenanceResult",
    "LifecycleResult",
    "MemoryHost",
    "__version__",
    "complete_waiting_process",
    "delete_terminal_run",
    "ensure_journal",
    "finalize_run",
    "host_drive",
    "host_drive_json",
    "host_run_package",
    "inspect_leases",
    "maintain_journal",
    "open_memory",
    "open_sqlite",
    "park_process",
    "record_in_process",
    "recover_incomplete",
    "sdk",
    "transition_run",
    "upsert_process",
    "upsert_run_metadata",
]
__version__ = "0.7.25"
