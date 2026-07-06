"""Embedded run-until-idle driver for the Carrier runtime.

The claim/execute/complete loop behind ``fala carrier-runtime
run-until-idle``, exposed as a library API so embedded consumers can drive a
run in-process without shelling out to the CLI. After each successful step
completion the driver advances any flow the step belongs to (see
``fala.flows``), readying dependent steps with their needs injected.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable
from urllib.parse import unquote, urlparse

from fala.adapters import StepRunRequest, StepRunResult, create_step_adapter
from fala.errors import FalaConfigurationError
from fala.flows import FlowInstance, advance_flow_for_process, instantiate_flow
from fala.models import CarrierAdapterSpec, CarrierFlowSpec
from fala.runtime_backend import (
    BridgeDelivery,
    Carrier,
    CarrierProcessStatus,
    CarrierRunStatus,
    EventRef,
    Gate,
    Process,
    Run,
    RunRef,
    RuntimeBackend,
    RuntimeBackendService,
    RuntimeBudget,
    RuntimePool,
    RuntimeRef,
    SQLiteRuntimeBackend,
)


@dataclass(frozen=True)
class RunUntilIdleResult:
    ok: bool
    ticks: int
    stopped_reason: str
    completed: list[Process]
    failed: list[Process]
    waiting: list[Process]


@dataclass(frozen=True)
class RunFlowResult:
    run: Run
    flow: FlowInstance
    outcome: RunUntilIdleResult
    status: CarrierRunStatus


async def run_until_idle(
    service: RuntimeBackendService,
    *,
    worker_id: str,
    run_id: str | None = None,
    lease_seconds: float = 300.0,
    max_ticks: int = 100,
    work_dir: str | Path | None = None,
    advance_flows: bool = True,
    should_stop: Callable[[], bool] | None = None,
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
        )
        if process is None:
            break
        ticks += 1
        try:
            adapter, step_input, config = process_step_request_parts(process)
            step_work_dir = work_root / process.id if work_root is not None else None
            if step_work_dir is not None:
                step_work_dir.mkdir(parents=True, exist_ok=True)
            request = StepRunRequest(
                run_id=process.run_id,
                process_id=process.id,
                carrier_id=process.carrier_id,
                adapter=adapter,
                input=step_input,
                config=config,
                work_dir=step_work_dir,
            )
            if adapter.kind == "fala_runtime":
                result = await enqueue_fala_runtime_process(
                    service=service,
                    process=process,
                    request=request,
                    actor=worker_id,
                )
            else:
                result = await create_step_adapter(adapter.kind).run(request)
            if result.waiting:
                if result.gate_id is not None:
                    await service.save_gate(
                        Gate(
                            id=result.gate_id,
                            run_id=process.run_id,
                            carrier_id=process.carrier_id,
                            kind=adapter.kind,
                            values=result.output,
                            metadata=result.metadata,
                        ),
                        idempotency_key=f"{process.run_id}:gate.open:{result.gate_id}",
                        actor=worker_id,
                    )
                stored, _ = await service.wait_process(
                    run_id=process.run_id,
                    process_id=process.id,
                    output=result.output,
                    idempotency_key=f"{process.run_id}:process.wait:{process.id}:{process.attempt}",
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
                idempotency_key=f"{process.run_id}:process.complete:{process.id}:{process.attempt}",
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
                    idempotency_key=f"{process.run_id}:process.retry:{process.id}:{process.attempt}",
                    actor=worker_id,
                )
            else:
                stored, _ = await service.fail_process(
                    run_id=process.run_id,
                    process_id=process.id,
                    error=error,
                    idempotency_key=f"{process.run_id}:process.fail:{process.id}:{process.attempt}",
                    actor=worker_id,
                )
            failed.append(stored)
            continue
        if advance_flows:
            await advance_flow_for_process(service, process=stored, actor=worker_id)

    return RunUntilIdleResult(
        ok=ticks < max_ticks,
        ticks=ticks,
        stopped_reason="stopped" if stopped else "max_ticks" if ticks >= max_ticks else "idle",
        completed=completed,
        failed=failed,
        waiting=waiting,
    )


_PROCESS_FAILURE_STATUSES = {
    CarrierProcessStatus.failed,
    CarrierProcessStatus.cancelled,
    CarrierProcessStatus.timed_out,
}

# Mirrors runtime_backend._TERMINAL_RUN_STATUSES: statuses from which the run
# state machine allows no further transition. Kept local so run_flow can tell a
# finished run from a resumable one without reaching into a private symbol.
_TERMINAL_RUN_STATUSES = {
    CarrierRunStatus.completed,
    CarrierRunStatus.failed,
    CarrierRunStatus.cancelled,
    CarrierRunStatus.timed_out,
}


def _run_flow_status(
    processes: list[Process], *, stopped_reason: str, max_ticks: int
) -> tuple[CarrierRunStatus, str | None]:
    """Derive the run status from the flow's own step processes.

    Owning this mapping is what keeps the run's state machine inside Fala. The
    authoritative signal is the post-drive status of every step process, not the
    per-drive tallies in :class:`RunUntilIdleResult`: a flow can drain on the
    exact tick the budget runs out, so ``stopped_reason`` alone cannot tell
    completion from a timeout. A failed, cancelled, or timed-out step fails the
    run; every step succeeding completes it; a spent tick budget with steps
    still incomplete times the run out. Otherwise the flow drained to idle with
    no failure and budget to spare -- it is parked on a waiting/gated step, so
    the run is *suspended* (``waiting``), not terminal: marking it ``failed``
    here would be irreversible and would strand a resumable run.
    """
    failures = [p for p in processes if p.status in _PROCESS_FAILURE_STATUSES]
    if failures:
        return (CarrierRunStatus.failed, f"{len(failures)} step(s) did not succeed")
    if processes and all(p.status == CarrierProcessStatus.succeeded for p in processes):
        return (CarrierRunStatus.completed, None)
    incomplete = [p for p in processes if p.status != CarrierProcessStatus.succeeded]
    if stopped_reason == "max_ticks":
        return (
            CarrierRunStatus.timed_out,
            f"flow did not finish within {max_ticks} ticks; "
            f"{len(incomplete)} step(s) still incomplete",
        )
    return (
        CarrierRunStatus.waiting,
        f"flow parked with {len(incomplete)} step(s) awaiting external progress",
    )


async def run_flow(
    service: RuntimeBackendService,
    *,
    run: Run,
    flow: CarrierFlowSpec,
    worker_id: str,
    flow_id: str | None = None,
    step_inputs: dict[str, dict[str, Any]] | None = None,
    step_configs: dict[str, dict[str, Any]] | None = None,
    work_dir: str | Path | None = None,
    max_ticks: int = 100,
    lease_seconds: float = 300.0,
    actor: str | None = None,
) -> RunFlowResult:
    """Create a run, instantiate one flow on it, drive it to idle, finalize status.

    The single-call run lifecycle for the common embedded case: the caller
    supplies the run record and the flow to execute, and Fala owns the whole
    run -- creating it, scheduling the flow's steps, running the
    claim/execute/advance loop until no work remains, and recording the terminal
    run status derived from the outcome (see :func:`_run_flow_terminal_status`).
    Per-step, per-run values reach the steps through ``step_inputs`` and
    ``step_configs`` exactly as with :func:`instantiate_flow`.

    Returns the finalized run, the instantiated flow (whose resolved
    ``flow_id`` addresses its step processes), the raw
    :class:`RunUntilIdleResult`, and the terminal status that was set.
    """
    stored_run, _ = await service.create_run(
        run, idempotency_key=f"{run.id}:run.create", actor=actor
    )
    instance = await instantiate_flow(
        service,
        run_id=run.id,
        flow=flow,
        flow_id=flow_id,
        step_inputs=step_inputs,
        step_configs=step_configs,
        actor=actor,
    )
    if stored_run.status in _TERMINAL_RUN_STATUSES:
        # Re-invoked on a run that already reached a terminal state (create_run
        # replayed rather than created it). Driving it again would re-run any
        # leftover-ready step and then try an illegal terminal-to-terminal
        # transition; return the finished run untouched instead.
        return RunFlowResult(
            run=stored_run,
            flow=instance,
            outcome=RunUntilIdleResult(
                ok=True,
                ticks=0,
                stopped_reason="already_terminal",
                completed=[],
                failed=[],
                waiting=[],
            ),
            status=stored_run.status,
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
    status, reason = _run_flow_status(
        processes, stopped_reason=outcome.stopped_reason, max_ticks=max_ticks
    )
    finalized, _ = await service.set_run_status(
        run_id=run.id,
        status=status,
        idempotency_key=f"{run.id}:run.{status.value}",
        reason=reason,
        actor=actor,
    )
    return RunFlowResult(run=finalized, flow=instance, outcome=outcome, status=status)


async def enqueue_fala_runtime_process(
    *,
    service: RuntimeBackendService,
    process: Process,
    request: StepRunRequest,
    actor: str,
) -> StepRunResult:
    if request.adapter.runtime_ref is None:
        raise ValueError("fala_runtime adapter requires runtime_ref")
    if process.carrier_id is None:
        raise ValueError("fala_runtime process requires carrier_id")

    backend = service.backend
    if not isinstance(backend, SQLiteRuntimeBackend):
        raise FalaConfigurationError(
            "fala_runtime steps require a SQLite-backed runtime service"
        )

    carrier = await backend.get_carrier(
        run_id=process.run_id,
        carrier_id=process.carrier_id,
    )
    if carrier is None:
        raise ValueError(f"Unknown carrier for fala_runtime process: {process.carrier_id!r}")

    events = await backend.list_events(
        run_id=process.run_id,
        carrier_id=process.carrier_id,
    )
    source_runtime = RuntimeRef(
        id=str(request.config.get("source_runtime_id") or "local"),
        uri=f"sqlite://{backend.path.expanduser().resolve()}",
    )
    target_runtime, pool_id, budget = await resolve_fala_runtime_target(
        backend=backend,
        carrier=carrier,
        request=request,
    )
    target_run_id = str(request.config.get("target_run_id") or process.run_id)
    delivery_id = str(
        request.config.get("delivery_id")
        or f"bridge:{process.run_id}:{process.id}"
    )
    delivery = BridgeDelivery(
        id=delivery_id,
        run_id=process.run_id,
        idempotency_key=f"{process.run_id}:bridge.enqueue:{process.id}:{process.attempt}",
        source=RunRef(runtime=source_runtime, run_id=process.run_id),
        target=RunRef(runtime=target_runtime, run_id=target_run_id),
        carrier=carrier,
        event_ref=EventRef(
            runtime=source_runtime,
            run_id=process.run_id,
            event_id=events[-1].id if events else None,
            sequence=events[-1].sequence if events else None,
        ),
        pool_id=pool_id,
        budget=budget,
        metadata={
            "process_id": process.id,
            "process_type": process.process_type,
        },
    )
    outbox, submission = await service.enqueue_bridge_delivery(
        delivery,
        actor=actor,
    )
    return StepRunResult(
        waiting=True,
        output={
            "status": "submitted",
            "runtime_ref": request.adapter.runtime_ref,
            "target_run_id": target_run_id,
            "delivery_id": outbox.id,
            "command_id": submission.command.id,
            "replayed": submission.replayed,
        },
    )


async def resolve_fala_runtime_target(
    *,
    backend: RuntimeBackend,
    carrier: Carrier,
    request: StepRunRequest,
) -> tuple[RuntimeRef, str | None, RuntimeBudget]:
    assert request.adapter.runtime_ref is not None
    configured_budget = request.config.get("budget")
    pool = await backend.get_runtime_pool(pool_id=request.adapter.runtime_ref)
    if pool is None:
        return (
            RuntimeRef(
                id=str(
                    request.config.get("target_runtime_id")
                    or runtime_ref_id(request.adapter.runtime_ref)
                ),
                uri=request.adapter.runtime_ref,
            ),
            request.config.get("pool_id"),
            RuntimeBudget.model_validate(configured_budget or {}),
        )

    if pool.carrier_types and carrier.carrier_type not in pool.carrier_types:
        raise ValueError(
            f"Runtime pool {pool.id!r} does not accept carrier type {carrier.carrier_type!r}"
        )
    if not pool.runtimes:
        raise ValueError(f"Runtime pool {pool.id!r} has no runtimes")

    policies = await backend.list_delegation_policies(pool_id=pool.id)
    delegation_policy = next(
        (
            item
            for item in policies
            if not item.carrier_types or carrier.carrier_type in item.carrier_types
        ),
        None,
    )
    budget = RuntimeBudget.model_validate(
        configured_budget
        or (
            delegation_policy.budget.model_dump(mode="json")
            if delegation_policy is not None
            else {}
        )
    )
    pool_policy = str(request.config.get("pool_policy") or pool.metadata.get("policy") or "manual")
    return await select_runtime_from_pool(backend, pool=pool, policy=pool_policy), pool.id, budget


async def select_runtime_from_pool(
    backend: RuntimeBackend,
    *,
    pool: RuntimePool,
    policy: str,
) -> RuntimeRef:
    if policy in {"manual", "first"}:
        return pool.runtimes[0]
    if policy == "least_busy":
        return min(pool.runtimes, key=_runtime_declared_load)
    if policy == "round_robin":
        index = _int_metadata(pool.metadata.get("round_robin_index"))
        selected = pool.runtimes[index % len(pool.runtimes)]
        metadata = {
            **pool.metadata,
            "round_robin_index": (index + 1) % len(pool.runtimes),
        }
        await backend.put_runtime_pool(pool.model_copy(update={"metadata": metadata}))
        return selected
    raise ValueError(f"Unknown runtime pool policy: {policy!r}")


def _runtime_declared_load(runtime: RuntimeRef) -> float:
    value = runtime.metadata.get("load", runtime.metadata.get("pending_processes", 0))
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Runtime {runtime.id!r} has invalid load metadata") from exc


def _int_metadata(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError) as exc:
        raise ValueError("runtime pool round_robin_index metadata must be an integer") from exc


def runtime_ref_id(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme in {"sqlite", "sqlite3"}:
        path = Path(sqlite_db_path(value))
        return path.stem or "sqlite"
    return value


def process_step_request_parts(
    process: Process,
) -> tuple[CarrierAdapterSpec, dict[str, Any], dict[str, Any]]:
    raw_input = dict(process.input)
    raw_adapter = raw_input.pop("adapter", None)
    if not isinstance(raw_adapter, dict):
        raise ValueError(f"Process {process.id!r} input requires adapter object")
    raw_config = raw_input.pop("config", {})
    config = raw_config if isinstance(raw_config, dict) else {}
    return CarrierAdapterSpec.model_validate(raw_adapter), raw_input, dict(config)


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
        raise ValueError(f"Unsupported Carrier runtime DB URL scheme: {parsed.scheme!r}")
    return target


__all__ = [
    "RunUntilIdleResult",
    "enqueue_fala_runtime_process",
    "process_step_request_parts",
    "resolve_fala_runtime_target",
    "run_until_idle",
    "runtime_ref_id",
    "select_runtime_from_pool",
    "sqlite_db_path",
]
