"""Pure (no I/O) process/run transition and claim eligibility helpers.

Shared by Correlator (SQLite) and future InMemoryJournal so policy stays one
source of truth. Correlation-path ready/cancel pure helpers live in
``fala.correlation_paths`` and are re-exported here for a single import surface.

See docs/EVENT_STREAM_CORE.md §PR2.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from fala.runtime_backend import (
    Process,
    ProcessStatus,
    RunStatus,
    RuntimeCommand,
)

TERMINAL_RUN_STATUSES: frozenset[RunStatus] = frozenset(
    {
        RunStatus.completed,
        RunStatus.failed,
        RunStatus.cancelled,
        RunStatus.timed_out,
    }
)

TERMINAL_PROCESS_STATUSES: frozenset[ProcessStatus] = frozenset(
    {
        ProcessStatus.succeeded,
        ProcessStatus.failed,
        ProcessStatus.cancelled,
        ProcessStatus.timed_out,
    }
)

RUN_STATUS_TRANSITIONS: dict[RunStatus, frozenset[RunStatus]] = {
    RunStatus.created: frozenset(
        {
            RunStatus.active,
            RunStatus.waiting,
            RunStatus.completed,
            RunStatus.failed,
            RunStatus.cancel_requested,
            RunStatus.cancelled,
            RunStatus.timed_out,
        }
    ),
    RunStatus.active: frozenset(
        {
            RunStatus.waiting,
            RunStatus.completed,
            RunStatus.failed,
            RunStatus.cancel_requested,
            RunStatus.cancelled,
            RunStatus.timed_out,
        }
    ),
    RunStatus.waiting: frozenset(
        {
            RunStatus.active,
            RunStatus.completed,
            RunStatus.failed,
            RunStatus.cancel_requested,
            RunStatus.cancelled,
            RunStatus.timed_out,
        }
    ),
    RunStatus.cancel_requested: frozenset(
        {
            RunStatus.cancelled,
            RunStatus.failed,
            RunStatus.timed_out,
        }
    ),
}

PROCESS_TRANSITION_COMMANDS: dict[ProcessStatus, str] = {
    ProcessStatus.ready: "process.ready",
    ProcessStatus.succeeded: "process.complete",
    ProcessStatus.failed: "process.fail",
    ProcessStatus.retry_wait: "process.retry",
    ProcessStatus.waiting: "process.wait",
    ProcessStatus.cancelled: "process.cancel",
    ProcessStatus.timed_out: "process.timeout",
}


def validate_run_status_transition(current: RunStatus, target: RunStatus) -> None:
    """Raise ValueError if ``current -> target`` is not allowed."""
    if current in TERMINAL_RUN_STATUSES:
        raise ValueError(f"Run status {current.value!r} is terminal")
    allowed = RUN_STATUS_TRANSITIONS.get(current, frozenset())
    if target not in allowed:
        raise ValueError(
            f"Invalid run status transition: {current.value!r} -> {target.value!r}"
        )


def process_transition_command_type(status: ProcessStatus) -> str:
    """Return the required command_type for a process status transition."""
    command_type = PROCESS_TRANSITION_COMMANDS.get(status)
    if command_type is None:
        raise ValueError(f"Unsupported process transition status: {status.value}")
    return command_type


def validate_process_transition_command(
    *,
    run_id: str,
    status: ProcessStatus,
    command: RuntimeCommand,
) -> None:
    """Validate command_type and run_id for a process transition."""
    expected = process_transition_command_type(status)
    if command.run_id != run_id:
        raise ValueError("process transition command run_id must match run_id")
    if command.command_type != expected:
        raise ValueError(
            f"transition to {status.value!r} requires command_type {expected!r}"
        )


def validate_process_can_finish(
    process: Process,
    *,
    actor: str | None,
) -> None:
    """Validate running/waiting process may complete or fail under lease rules."""
    if process.status not in {
        ProcessStatus.running,
        ProcessStatus.waiting,
    }:
        raise ValueError(
            f"Process {process.id!r} is not running or waiting: {process.status.value}"
        )
    if process.lease_owner is not None and actor != process.lease_owner:
        raise ValueError(
            f"Process {process.id!r} lease is held by {process.lease_owner!r}; "
            f"actor {actor!r} cannot finish it"
        )


def validate_process_can_retry(
    process: Process,
    *,
    actor: str | None,
) -> None:
    """Validate process may enter retry_wait."""
    if process.status not in {
        ProcessStatus.running,
        ProcessStatus.failed,
    }:
        raise ValueError(
            f"Process {process.id!r} cannot be retried from status: "
            f"{process.status.value}"
        )
    if process.lease_owner is not None and actor != process.lease_owner:
        raise ValueError(
            f"Process {process.id!r} lease is held by {process.lease_owner!r}; "
            f"actor {actor!r} cannot retry it"
        )
    if process.attempt >= process.max_attempts:
        raise ValueError(f"Process {process.id!r} exhausted retry attempts")


def validate_process_can_wait(process: Process) -> None:
    if process.status != ProcessStatus.running:
        raise ValueError(
            f"Process {process.id!r} cannot wait from status: {process.status.value}"
        )


def validate_process_can_ready(process: Process) -> None:
    if process.status != ProcessStatus.pending:
        raise ValueError(
            f"Process {process.id!r} cannot become ready from status: "
            f"{process.status.value}"
        )


def process_is_claimable(process: Process, *, now: datetime) -> bool:
    """Mirror Correlator claim SQL eligibility (attempt < max_attempts)."""
    if process.attempt >= process.max_attempts:
        return False
    if process.status == ProcessStatus.ready:
        return True
    if process.status == ProcessStatus.retry_wait and process.available_at <= now:
        return True
    if (
        process.status == ProcessStatus.running
        and process.lease_expires_at is not None
        and process.lease_expires_at <= now
    ):
        return True
    return False


def process_lease_expired_no_retries(process: Process, *, now: datetime) -> bool:
    """True when a running process must be force-failed on claim (no attempts left)."""
    return (
        process.status == ProcessStatus.running
        and process.lease_expires_at is not None
        and process.lease_expires_at <= now
        and process.attempt >= process.max_attempts
    )


def lease_expired_error(process: Process) -> dict[str, Any]:
    return {
        "type": "lease_expired",
        "message": "process lease expired with no attempts left",
        "lease_owner": process.lease_owner,
    }


def apply_lease_expired_fail(process: Process, *, now: datetime) -> Process:
    """Return process snapshot after lease-expired terminal fail (no I/O)."""
    return process.model_copy(
        update={
            "status": ProcessStatus.failed,
            "lease_owner": None,
            "lease_expires_at": None,
            "error": lease_expired_error(process),
            "updated_at": now,
            "finished_at": now,
        }
    )


def apply_process_claim(
    process: Process,
    *,
    worker_id: str,
    lease_seconds: float,
    now: datetime,
) -> Process:
    """Return process snapshot after a successful claim (no I/O)."""
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be greater than zero")
    expires_at = now + timedelta(seconds=lease_seconds)
    return process.model_copy(
        update={
            "status": ProcessStatus.running,
            "attempt": process.attempt + 1,
            "lease_owner": worker_id,
            "lease_expires_at": expires_at,
            "updated_at": now,
            "started_at": process.started_at or now,
        }
    )


def claim_sort_key(process: Process) -> tuple[int, datetime, datetime, str]:
    """Ordering for claim selection: priority DESC, available_at, created_at, id."""
    return (-process.priority, process.available_at, process.created_at, process.id)


def select_next_claimable(
    processes: list[Process],
    *,
    now: datetime,
    run_id: str | None = None,
) -> Process | None:
    """Pure selection of the next claimable process (no mutation)."""
    candidates = [
        p
        for p in processes
        if (run_id is None or p.run_id == run_id) and process_is_claimable(p, now=now)
    ]
    if not candidates:
        return None
    candidates.sort(key=claim_sort_key)
    return candidates[0]


__all__ = [
    "TERMINAL_PROCESS_STATUSES",
    "TERMINAL_RUN_STATUSES",
    "PROCESS_TRANSITION_COMMANDS",
    "RUN_STATUS_TRANSITIONS",
    "apply_lease_expired_fail",
    "apply_process_claim",
    "claim_sort_key",
    "lease_expired_error",
    "process_is_claimable",
    "process_lease_expired_no_retries",
    "process_transition_command_type",
    "select_next_claimable",
    "validate_process_can_finish",
    "validate_process_can_ready",
    "validate_process_can_retry",
    "validate_process_can_wait",
    "validate_process_transition_command",
    "validate_run_status_transition",
]
