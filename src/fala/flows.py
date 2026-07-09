"""Flow orchestration over the Carrier runtime process store.

Instantiates a ``CarrierFlowSpec`` as one process per step and advances the
dependency graph: a step with no needs starts ``ready``; a step with needs
starts ``pending`` (invisible to claim) and is readied only once every need
has succeeded, with each need's output injected into the dependent step's
input under ``"needs"`` (readable via ``fala.sdk.needs``). A step whose needs
can no longer succeed is never readied and never auto-cancelled — pending is
unclaimable, so blocked steps fail closed by inaction and are reported in
``FlowAdvance.blocked``. Subprocess adapter commands may name the driving
interpreter portably via :data:`PYTHON_COMMAND_PLACEHOLDER`.
"""

from __future__ import annotations

import sys
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from fala.models import CarrierFlowSpec
from fala.runtime_backend import (
    CarrierProcessStatus,
    Process,
    RuntimeBackendService,
)
from fala.sdk import INJECTED_INPUT_KEYS

_RESERVED_STEP_INPUT_KEYS = ("adapter", "config", *sorted(INJECTED_INPUT_KEYS))

PYTHON_COMMAND_PLACEHOLDER = "${python}"
"""Portable interpreter token for subprocess adapter commands.

Resolved by :func:`instantiate_flow` to the driving interpreter
(``sys.executable``), so a flow spec never hard-codes a host's Python path.
Bare ``python``/``python3`` tokens are left untouched -- only this explicit
placeholder is resolved.
"""
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
    """Schedule one process per flow step, binding adapters to this host.

    Per-step binding includes inheriting ``timeout_seconds`` and resolving
    :data:`PYTHON_COMMAND_PLACEHOLDER` tokens in subprocess adapter commands
    to the driving interpreter (``sys.executable``).
    """
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
        if (
            adapter.kind == "subprocess"
            and adapter.command
            and PYTHON_COMMAND_PLACEHOLDER in adapter.command
        ):
            adapter = adapter.model_copy(
                update={
                    "command": [
                        sys.executable if token == PYTHON_COMMAND_PLACEHOLDER else token
                        for token in adapter.command
                    ]
                }
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
                    "accumulate_upstream_artifacts": (
                        flow.accumulate_upstream_artifacts
                    ),
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
    members = flow_step_processes(processes, flow_id)

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
            if marker.get("accumulate_upstream_artifacts"):
                # Every direct need has succeeded, so by induction the whole
                # transitive ancestor closure has too -- each ancestor's output is
                # real. Flatten their ``artifacts`` in topological order (deepest
                # ancestor first) so a later producer of the same artifact wins for
                # consumers that scan latest-first.
                new_input["upstream_artifacts"] = [
                    artifact
                    for ancestor_id in _ancestor_steps_topo(step_id, members)
                    for artifact in (members[ancestor_id].output.get("artifacts") or [])
                ]
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


def _ancestor_steps_topo(step_id: str, members: dict[str, Process]) -> list[str]:
    """Transitive-need step ids of ``step_id`` in topological order.

    Post-order DFS over the ``needs`` graph: each ancestor is emitted after its own
    needs, so the result is a valid topological order (deepest ancestor first) with
    every ancestor appearing exactly once. ``step_id`` itself is never emitted, and
    the ``seen`` set makes the walk terminate even if feedback cycles are allowed.
    """
    order: list[str] = []
    seen: set[str] = {step_id}

    def visit(current: str) -> None:
        marker = _flow_marker(members[current]) or {}
        for need_id in (str(item) for item in marker.get("needs") or []):
            if need_id in members and need_id not in seen:
                seen.add(need_id)
                visit(need_id)
                order.append(need_id)

    visit(step_id)
    return order


def flow_step_processes(
    processes: Iterable[Process], flow_id: str
) -> dict[str, Process]:
    """The processes of flow ``flow_id``, keyed by step id.

    Reads each process's ``metadata['flow']`` marker -- the same channel
    :func:`advance_flow` uses to assemble a flow's members -- so a host-side
    caller never reconstructs the ``{flow_id}:{step_id}`` process-id format to
    find a flow's steps. A process without a matching marker is skipped; if two
    carry the same step id (never in a normal flow) the last wins.
    """
    members: dict[str, Process] = {}
    for process in processes:
        marker = _flow_marker(process)
        if marker is None or marker.get("flow_id") != flow_id:
            continue
        step_id = marker.get("step_id")
        if isinstance(step_id, str) and step_id:
            members[step_id] = process
    return members


def find_flow_step_process(
    processes: Iterable[Process], *, flow_id: str, step_id: str
) -> Process | None:
    """The process for ``step_id`` within flow ``flow_id``, or ``None``.

    A convenience over :func:`flow_step_processes` for the common single-step
    lookup a host-side reader needs (e.g. locating a flow's report step in a
    finished run's process list).
    """
    return flow_step_processes(processes, flow_id).get(step_id)


def _flow_marker(process: Process) -> dict[str, Any] | None:
    marker = process.metadata.get("flow")
    if isinstance(marker, dict):
        return marker
    return None


__all__ = [
    "FlowAdvance",
    "FlowBlockedStep",
    "FlowInstance",
    "PYTHON_COMMAND_PLACEHOLDER",
    "advance_flow",
    "advance_flow_for_process",
    "find_flow_step_process",
    "flow_step_processes",
    "instantiate_flow",
]
