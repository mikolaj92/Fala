"""Embedded run-until-idle driver for the Impulse runtime.

The claim/execute/complete loop behind ``fala runtime
run-until-idle``, exposed as a library API so embedded consumers can drive a
run in-process without shelling out to the CLI. After each successful effector
completion the driver advances any correlation_path the effector belongs to (see
``fala.correlation_paths``), readying dependent effectors with their conduction injected.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable
from urllib.parse import unquote, urlparse

from fala.adapters import EffectorRunRequest, create_effector_adapter
from fala.errors import FalaDeadlockDetected
from fala.correlation_paths import CorrelationPathInstance, instantiate_correlation_path
from fala.models import EffectorAdapterSpec, CorrelationPathSpec
from fala.runtime_backend import (
    ProcessStatus,
    RunStatus,
    Homeostat,
    Process,
    WaitGraphDiagnostic,
    Run,
    RuntimeBackend,
    RuntimeBackendService,
    Correlator,
)


def backend_runtime_uri(backend: RuntimeBackend) -> str:
    """Resolve a durable runtime URI for bridge source/target refs."""
    uri = getattr(backend, "runtime_uri", None)
    if isinstance(uri, str) and uri:
        return uri
    if isinstance(backend, Correlator):
        return f"sqlite://{backend.path.expanduser().resolve()}"
    journal = getattr(backend, "journal", None)
    if journal is not None:
        juri = getattr(journal, "runtime_uri", None)
        if isinstance(juri, str) and juri:
            return juri
    return "unknown://local"


@dataclass(frozen=True)
class RunUntilIdleResult:
    ok: bool
    ticks: int
    stopped_reason: str
    completed: list[Process]
    failed: list[Process]
    waiting: list[Process]
    deadlocked: bool = False
    deadlocks: list[list[str]] = field(default_factory=list)
    wait_diagnostic: WaitGraphDiagnostic | None = None


@dataclass(frozen=True)
class RunCorrelationPathResult:
    run: Run
    correlation_path: CorrelationPathInstance
    outcome: RunUntilIdleResult
    status: RunStatus
    processes: list[Process]


async def run_until_idle(
    service: RuntimeBackendService,
    *,
    worker_id: str,
    run_id: str | None = None,
    lease_seconds: float = 300.0,
    max_ticks: int = 100,
    work_dir: str | Path | None = None,
    advance_correlation_paths: bool = True,
    should_stop: Callable[[], bool] | None = None,
    all_runs: bool = False,
) -> RunUntilIdleResult:
    if max_ticks < 1:
        raise ValueError("max_ticks must be greater than zero")
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be greater than zero")
    completed: list[Process] = []
    failed: list[Process] = []
    waiting: list[Process] = []
    ticks = 0
    stopped = False
    work_root = Path(work_dir).expanduser() if work_dir else None
    if work_root is not None:
        work_root.mkdir(parents=True, exist_ok=True)

    while ticks < max_ticks:
        if should_stop is not None and should_stop():
            stopped = True
            break
        process = await service.claim_next_ready_process(
            worker_id=worker_id,
            run_id=run_id,
            lease_seconds=lease_seconds,
            all_runs=all_runs,
        )
        if process is None:
            break
        ticks += 1
        try:
            adapter, effector_input, config = process_effector_request_parts(process)
            effector_work_dir = work_root / process.id if work_root is not None else None
            if effector_work_dir is not None:
                effector_work_dir.mkdir(parents=True, exist_ok=True)
            request = EffectorRunRequest(
                process_id=process.id,
                impulse_id=process.impulse_id,
                adapter=adapter,
                input=effector_input,
                config=config,
                work_dir=effector_work_dir,
            )
            result = await create_effector_adapter(adapter.kind).run(request)
            if result.waiting:
                if result.homeostat_id is not None:
                    await service.save_homeostat(
                        Homeostat(
                            id=result.homeostat_id,
                            run_id=process.run_id,
                            impulse_id=process.impulse_id,
                            kind=adapter.kind,
                            values=result.output,
                            metadata=result.metadata,
                            # Inherit the (possibly regulation-overridden) damping budget
                            # from the process. This makes homeostat defense elastic:
                            # the same max_attempts granted to the effector applies to
                            # re-open attempts on the homeostat.
                            max_attempts=process.max_attempts,
                        ),
                        idempotency_key=f"homeostat.open:{result.homeostat_id}",
                        actor=worker_id,
                    )
                stored, _ = await service.wait_process(
                    run_id=process.run_id,
                    process_id=process.id,
                    output=result.output,
                    idempotency_key=f"process.wait:{process.id}:{process.attempt}",
                    actor=worker_id,
                )
                waiting.append(stored)
                continue

            stored, _ = await service.complete_process(
                run_id=process.run_id,
                process_id=process.id,
                output={
                    **result.output,
                    "adapter": {
                        "returncode": result.returncode,
                        "stdout": result.stdout,
                        "stderr": result.stderr,
                    },
                },
                idempotency_key=f"process.complete:{process.id}:{process.attempt}",
                actor=worker_id,
            )
            completed.append(stored)
        except Exception as exc:
            error = {"type": type(exc).__name__, "message": str(exc)}
            if process.attempt < process.max_attempts:
                stored, _ = await service.retry_process(
                    run_id=process.run_id,
                    process_id=process.id,
                    error=error,
                    idempotency_key=f"process.retry:{process.id}:{process.attempt}",
                    actor=worker_id,
                )
            else:
                stored, _ = await service.fail_process(
                    run_id=process.run_id,
                    process_id=process.id,
                    error=error,
                    idempotency_key=f"process.fail:{process.id}:{process.attempt}",
                    actor=worker_id,
                )
            failed.append(stored)
            continue
        # Readies and dead-upstream cancellations are performed atomically inside
        # transition_process for succeeded/failed. External advance remains useful
        # for unblocking after external events (homeostats, child runs).

    # Deadlock detection (LUKA6): run a wait-graph diagnosis at the end of the
    # idle loop. If a deadlock is present, record it so callers and higher layers
    # (e.g. run_correlation_path status) can react instead of silently parking.
    deadlocked = False
    deadlocks: list[list[str]] = []
    wait_diagnostic: WaitGraphDiagnostic | None = None
    if run_id:
        try:
            diag = await service.diagnose_waits(run_id=run_id)
            deadlocked = diag.deadlocked
            deadlocks = diag.deadlocks
            wait_diagnostic = diag
            if deadlocked:
                stopped = True
        except Exception:
            # Diagnosis must not break the driver; surface via result only.
            pass
    if deadlocked:
        raise FalaDeadlockDetected(
            f"deadlock detected during run_until_idle for run {run_id}",
            details={"run_id": run_id, "deadlocks": deadlocks},
        )
    return RunUntilIdleResult(
        ok=ticks < max_ticks,
        ticks=ticks,
        stopped_reason="stopped" if stopped else "max_ticks" if ticks >= max_ticks else "idle",
        completed=completed,
        failed=failed,
        waiting=waiting,
        deadlocked=deadlocked,
        deadlocks=deadlocks,
        wait_diagnostic=wait_diagnostic,
    )
def process_error_text(process: Process) -> str:
    """Render a failed process's structured ``error`` into one human line.

    ``run_until_idle`` records an effector failure as ``{"type": ..., "message":
    ...}`` (see the exception handler above). This turns that envelope back into
    a string -- ``"Type: message"`` when both are present, the message or a JSON
    dump as fallbacks, and a fixed sentinel when there is no error at all -- so a
    caller surfacing the failure never has to know the error dict's shape.
    """
    error = process.error if isinstance(process.error, dict) else {}
    message = error.get("message")
    error_type = error.get("type")
    if isinstance(message, str) and message:
        if isinstance(error_type, str) and error_type:
            return f"{error_type}: {message}"
        return message
    if error:
        return json.dumps(error, ensure_ascii=False, sort_keys=True)
    return "unknown effector failure"


_PROCESS_FAILURE_STATUSES = {
    ProcessStatus.failed,
    ProcessStatus.cancelled,
    ProcessStatus.timed_out,
}

# Mirrors runtime_backend._TERMINAL_RUN_STATUSES: statuses from which the run
# state machine allows no further transition. Kept local so run_correlation_path can tell a
# finished run from a resumable one without reaching into a private symbol.
_TERMINAL_RUN_STATUSES = {
    RunStatus.completed,
    RunStatus.failed,
    RunStatus.cancelled,
    RunStatus.timed_out,
}


def _run_correlation_path_status(
    processes: list[Process], *, stopped_reason: str, max_ticks: int
) -> tuple[RunStatus, str | None]:
    """Derive the run status from the correlation_path's own effector processes.

    Owning this mapping is what keeps the run's state machine inside Fala. The
    authoritative signal is the post-drive status of every effector process, not the
    per-drive tallies in :class:`RunUntilIdleResult`: a correlation_path can drain on the
    exact tick the budget runs out, so ``stopped_reason`` alone cannot tell
    completion from a timeout. A failed, cancelled, or timed-out effector fails the
    run; every effector succeeding completes it; a spent tick budget with effectors
    still incomplete times the run out. Otherwise the correlation_path drained to idle with
    no failure and budget to spare -- it is parked on a waiting or homeostat-blocked effector, so
    the run is *suspended* (``waiting``), not terminal: marking it ``failed``
    here would be irreversible and would strand a resumable run.
    """
    failures = [p for p in processes if p.status in _PROCESS_FAILURE_STATUSES]
    if failures:
        return (RunStatus.failed, f"{len(failures)} effector(s) did not succeed")
    if processes and all(p.status == ProcessStatus.succeeded for p in processes):
        return (RunStatus.completed, None)
    incomplete = [p for p in processes if p.status != ProcessStatus.succeeded]
    if stopped_reason == "max_ticks":
        return (
            RunStatus.timed_out,
            f"correlation_path did not finish within {max_ticks} ticks; "
            f"{len(incomplete)} effector(s) still incomplete",
        )
    return (
        RunStatus.waiting,
        f"correlation_path parked with {len(incomplete)} effector(s) awaiting external progress",
    )


async def run_correlation_path(
    service: RuntimeBackendService,
    *,
    run: Run,
    correlation_path: CorrelationPathSpec,
    worker_id: str,
    correlation_path_id: str | None = None,
    effector_inputs: dict[str, dict[str, Any]] | None = None,
    effector_configs: dict[str, dict[str, Any]] | None = None,
    capability_output_schemas: dict[str, dict[str, Any]] | None = None,
    accepted_reaction_kinds_by_effector: dict[str, list[str]] | None = None,
    max_attempts_by_effector: dict[str, int] | None = None,
    regulation_by_effector: dict[str, dict[str, Any]] | None = None,
    work_dir: str | Path | None = None,
    max_ticks: int = 100,
    lease_seconds: float = 300.0,
    actor: str | None = None,
) -> RunCorrelationPathResult:
    """Create a run, instantiate one correlation_path on it, drive it to idle, finalize status.

    The single-call run lifecycle for the common embedded case: the caller
    supplies the run record and the correlation_path to execute, and Fala owns the whole
    run -- creating it, scheduling the correlation_path's effectors, running the
    claim/execute/advance loop until no work remains, and recording the terminal
    run status derived from the outcome (see :func:`_run_correlation_path_status`).
    Per-effector, per-run values reach the effectors through ``effector_inputs`` and
    ``effector_configs`` exactly as with :func:`instantiate_correlation_path`.

    Returns the finalized run, the instantiated correlation_path (whose resolved
    ``correlation_path_id`` addresses its effector processes), the raw
    :class:`RunUntilIdleResult`, and the terminal status that was set.
    """
    stored_run, _ = await service.create_run(
        run, idempotency_key="run.create", actor=actor
    )
    instance = await instantiate_correlation_path(
        service,
        run_id=run.id,
        correlation_path=correlation_path,
        correlation_path_id=correlation_path_id,
        effector_inputs=effector_inputs,
        accepted_reaction_kinds_by_effector=accepted_reaction_kinds_by_effector,
        effector_configs=effector_configs,
        actor=actor,
        capability_output_schemas=capability_output_schemas,
        regulation_by_effector=regulation_by_effector,
        max_attempts_by_effector=max_attempts_by_effector,
    )
    if stored_run.status in _TERMINAL_RUN_STATUSES:
        # Re-invoked on a run that already reached a terminal state (create_run
        # replayed rather than created it). Driving it again would re-run any
        # leftover-ready effector and then try an illegal terminal-to-terminal
        # transition; return the finished run untouched instead.
        return RunCorrelationPathResult(
            run=stored_run,
            correlation_path=instance,
            outcome=RunUntilIdleResult(
                ok=True,
                ticks=0,
                stopped_reason="already_terminal",
                completed=[],
                failed=[],
                waiting=[],
            ),
            status=stored_run.status,
            processes=await service.list_processes(run_id=run.id),
        )
    outcome = await run_until_idle(
        service,
        worker_id=worker_id,
        run_id=run.id,
        lease_seconds=lease_seconds,
        max_ticks=max_ticks,
        work_dir=work_dir,
    )
    processes = await service.list_processes(run_id=run.id)
    status, reason = _run_correlation_path_status(
        processes, stopped_reason=outcome.stopped_reason, max_ticks=max_ticks
    )
    finalized, _ = await service.set_run_status(
        run_id=run.id,
        status=status,
        idempotency_key=f"run.{status.value}",
        reason=reason,
        actor=actor,
    )
    return RunCorrelationPathResult(
        run=finalized,
        correlation_path=instance,
        outcome=outcome,
        status=status,
        processes=processes,
    )


def process_effector_request_parts(
    process: Process,
) -> tuple[EffectorAdapterSpec, dict[str, Any], dict[str, Any]]:
    raw_input = dict(process.input)
    raw_adapter = raw_input.pop("adapter", None)
    if not isinstance(raw_adapter, dict):
        raise ValueError(f"Process {process.id!r} input requires adapter object")
    raw_config = raw_input.pop("config", {})
    config = raw_config if isinstance(raw_config, dict) else {}
    return EffectorAdapterSpec.model_validate(raw_adapter), raw_input, dict(config)


def sqlite_db_path(target: str) -> str:
    parsed = urlparse(target)
    if parsed.scheme in {"sqlite", "sqlite3"}:
        if parsed.netloc and parsed.netloc != "localhost":
            raise ValueError("SQLite URL host must be empty or localhost")
        if parsed.netloc == "localhost":
            path = parsed.path
        elif target.startswith(f"{parsed.scheme}:////"):
            path = parsed.path
        else:
            path = parsed.path.lstrip("/")
        if not path:
            raise ValueError("SQLite URL must include a database path")
        return unquote(path)
    if parsed.scheme:
        raise ValueError(f"Unsupported Impulse runtime DB URL scheme: {parsed.scheme!r}")
    return target


__all__ = [
    "RunCorrelationPathResult",
    "RunUntilIdleResult",
    "backend_runtime_uri",
    "process_error_text",
    "process_effector_request_parts",
    "run_correlation_path",
    "run_until_idle",
    "sqlite_db_path",
]
