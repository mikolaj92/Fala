"""Flow orchestration over the Carrier runtime process store.

Instantiates a ``CarrierFlowSpec`` as one process per step and advances the
dependency graph: a step with no needs starts ``ready``; a step with needs
starts ``pending`` (invisible to claim) and is readied only once every need
has succeeded, with each need's output injected into the dependent step's
input under ``"needs"`` (readable via ``fala.sdk.needs``). A step whose needs
can no longer succeed is never readied and never auto-cancelled — pending is
unclaimable, so blocked steps fail closed by inaction and are reported in
``FlowAdvance.blocked``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from fala.models import CarrierFlowSpec
from fala.runtime_backend import (
    CarrierProcessStatus,
    Process,
    RuntimeBackendService,
)

_RESERVED_STEP_INPUT_KEYS = ("adapter", "config", "needs")
_DEAD_NEED_STATUSES = {
    CarrierProcessStatus.cancelled,
    CarrierProcessStatus.timed_out,
}


@dataclass(frozen=True)
class FlowInstance:
    flow_id: str
    run_id: str
    processes: list[Process]


@dataclass(frozen=True)
class FlowBlockedStep:
    process_id: str
    step_id: str
    unmet: list[str]
    dead: list[str]


@dataclass(frozen=True)
class FlowAdvance:
    readied: list[Process]
    blocked: list[FlowBlockedStep]


async def instantiate_flow(
    service: RuntimeBackendService,
    *,
    run_id: str,
    flow: CarrierFlowSpec,
    flow_id: str | None = None,
    carrier_id: str | None = None,
    step_inputs: Mapping[str, Mapping[str, Any]] | None = None,
    step_configs: Mapping[str, Mapping[str, Any]] | None = None,
    max_attempts: int = 1,
    priority: int = 0,
    actor: str | None = None,
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> FlowInstance:
    resolved_flow_id = flow_id or f"{run_id}:{flow.id}"
    known_steps = {step.id for step in flow.steps}
    inputs = {key: dict(value) for key, value in (step_inputs or {}).items()}
    configs = {key: dict(value) for key, value in (step_configs or {}).items()}
    unknown_inputs = sorted(set(inputs) - known_steps)
    if unknown_inputs:
        raise ValueError(
            f"step_inputs reference unknown flow steps: {unknown_inputs}"
        )
    unknown_configs = sorted(set(configs) - known_steps)
    if unknown_configs:
        raise ValueError(
            f"step_configs reference unknown flow steps: {unknown_configs}"
        )
    for step_id, values in inputs.items():
        reserved = sorted(set(values) & set(_RESERVED_STEP_INPUT_KEYS))
        if reserved:
            raise ValueError(
                f"step_inputs[{step_id!r}] uses reserved keys: {reserved}"
            )

    processes: list[Process] = []
    for step in flow.steps:
        adapter = step.adapter
        if (
            step.timeout_seconds is not None
            and adapter.timeout_seconds is None
            and adapter.kind != "manual_gate"
        ):
            adapter = adapter.model_copy(
                update={"timeout_seconds": step.timeout_seconds}
            )
        process = Process(
            id=f"{resolved_flow_id}:{step.id}",
            run_id=run_id,
            process_type=step.capability,
            carrier_id=carrier_id,
            status=(
                CarrierProcessStatus.pending
                if step.needs
                else CarrierProcessStatus.ready
            ),
            priority=priority,
            max_attempts=max_attempts,
            input={
                **inputs.get(step.id, {}),
                "adapter": adapter.model_dump(mode="json"),
                "config": {**step.config, **configs.get(step.id, {})},
            },
            metadata={
                "flow": {
                    "flow_id": resolved_flow_id,
                    "flow_spec_id": flow.id,
                    "step_id": step.id,
                    "needs": list(step.needs),
                }
            },
        )
        stored, _ = await service.schedule_process(
            process,
            idempotency_key=(
                f"{run_id}:process.schedule:{resolved_flow_id}:{step.id}"
            ),
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )
        processes.append(stored)
    return FlowInstance(
        flow_id=resolved_flow_id,
        run_id=run_id,
        processes=processes,
    )


async def advance_flow(
    service: RuntimeBackendService,
    *,
    run_id: str,
    flow_id: str,
    actor: str | None = None,
) -> FlowAdvance:
    processes = await service.list_processes(run_id=run_id)
    members: dict[str, Process] = {}
    for process in processes:
        marker = _flow_marker(process)
        if marker is None or marker.get("flow_id") != flow_id:
            continue
        step_id = marker.get("step_id")
        if isinstance(step_id, str) and step_id:
            members[step_id] = process

    readied: list[Process] = []
    blocked: list[FlowBlockedStep] = []
    for step_id, process in members.items():
        if process.status != CarrierProcessStatus.pending:
            continue
        marker = _flow_marker(process) or {}
        needs = [str(item) for item in marker.get("needs") or []]
        unmet: list[str] = []
        dead: list[str] = []
        met: dict[str, Process] = {}
        for need_id in needs:
            need = members.get(need_id)
            if need is None:
                raise ValueError(
                    f"Flow {flow_id!r} step {step_id!r} needs unknown step: "
                    f"{need_id!r}"
                )
            if need.status == CarrierProcessStatus.succeeded:
                met[need_id] = need
            elif need.status in _DEAD_NEED_STATUSES:
                dead.append(need_id)
            elif (
                need.status == CarrierProcessStatus.failed
                and need.attempt >= need.max_attempts
            ):
                dead.append(need_id)
            else:
                unmet.append(need_id)
        if not unmet and not dead:
            new_input = {
                **process.input,
                "needs": {
                    need_id: dict(item.output) for need_id, item in met.items()
                },
            }
            stored, _ = await service.ready_process(
                run_id=run_id,
                process_id=process.id,
                input=new_input,
                idempotency_key=f"{run_id}:process.ready:{process.id}",
                actor=actor,
            )
            readied.append(stored)
        else:
            blocked.append(
                FlowBlockedStep(
                    process_id=process.id,
                    step_id=step_id,
                    unmet=unmet,
                    dead=dead,
                )
            )
    return FlowAdvance(readied=readied, blocked=blocked)


async def advance_flow_for_process(
    service: RuntimeBackendService,
    *,
    process: Process,
    actor: str | None = None,
) -> FlowAdvance | None:
    marker = _flow_marker(process)
    if marker is None:
        return None
    flow_id = marker.get("flow_id")
    if not isinstance(flow_id, str) or not flow_id:
        return None
    return await advance_flow(
        service,
        run_id=process.run_id,
        flow_id=flow_id,
        actor=actor,
    )


def _flow_marker(process: Process) -> dict[str, Any] | None:
    marker = process.metadata.get("flow")
    if isinstance(marker, dict):
        return marker
    return None


__all__ = [
    "FlowAdvance",
    "FlowBlockedStep",
    "FlowInstance",
    "advance_flow",
    "advance_flow_for_process",
    "instantiate_flow",
]
