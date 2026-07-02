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
from typing import Any
from urllib.parse import unquote, urlparse

from fala.adapters import StepRunRequest, StepRunResult, create_step_adapter
from fala.errors import FalaConfigurationError
from fala.flows import advance_flow_for_process
from fala.models import CarrierAdapterSpec
from fala.runtime_backend import (
    BridgeDelivery,
    Carrier,
    EventRef,
    Gate,
    Process,
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


async def run_until_idle(
    service: RuntimeBackendService,
    *,
    worker_id: str,
    run_id: str | None = None,
    lease_seconds: float = 300.0,
    max_ticks: int = 100,
    work_dir: str | Path | None = None,
    advance_flows: bool = True,
) -> RunUntilIdleResult:
    if max_ticks < 1:
        raise ValueError("max_ticks must be greater than zero")
    if lease_seconds <= 0:
        raise ValueError("lease_seconds must be greater than zero")
    completed: list[Process] = []
    failed: list[Process] = []
    waiting: list[Process] = []
    ticks = 0
    work_root = Path(work_dir).expanduser() if work_dir else None
    if work_root is not None:
        work_root.mkdir(parents=True, exist_ok=True)

    while ticks < max_ticks:
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
        stopped_reason="max_ticks" if ticks >= max_ticks else "idle",
        completed=completed,
        failed=failed,
        waiting=waiting,
    )


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
