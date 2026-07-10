"""CorrelationPath orchestration over the Impulse runtime process store.

Instantiates a ``CorrelationPathSpec`` as one process per effector and advances the
dependency graph: an effector with no conduction starts ``ready``; an effector with conduction
starts ``pending`` (invisible to claim) and is readied only once every upstream
has succeeded, with each upstream's output injected into the dependent effector's
input under ``"conduction"`` (readable via ``fala.sdk.conduction``). An effector whose conduction
can no longer succeed is never readied and never auto-cancelled — pending is
unclaimable, so blocked effectors fail closed by inaction and are reported in
``CorrelationPathAdvance.blocked``. Subprocess adapter commands may name the driving
interpreter portably via :data:`PYTHON_COMMAND_PLACEHOLDER`.
"""

from __future__ import annotations

import sys
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from fala.models import CorrelationPathSpec
from fala.runtime_backend import (
    ProcessStatus,
    Process,
    RuntimeBackendService,
)
from fala.sdk import INJECTED_INPUT_KEYS

_RESERVED_EFFECTOR_INPUT_KEYS = ("adapter", "config", *sorted(INJECTED_INPUT_KEYS))

PYTHON_COMMAND_PLACEHOLDER = "${python}"
"""Portable interpreter token for subprocess adapter commands.

Resolved by :func:`instantiate_correlation_path` to the driving interpreter
(``sys.executable``), so a correlation_path spec never hard-codes a host's Python path.
Bare ``python``/``python3`` tokens are left untouched -- only this explicit
placeholder is resolved.
"""
_DEAD_UPSTREAM_STATUSES = {
    ProcessStatus.cancelled,
    ProcessStatus.timed_out,
}


@dataclass(frozen=True)
class CorrelationPathInstance:
    correlation_path_id: str
    run_id: str
    processes: list[Process]


@dataclass(frozen=True)
class CorrelationPathBlockedEffector:
    process_id: str
    effector_id: str
    unmet: list[str]
    dead: list[str]


@dataclass(frozen=True)
class CorrelationPathAdvance:
    readied: list[Process]
    blocked: list[CorrelationPathBlockedEffector]


async def instantiate_correlation_path(
    service: RuntimeBackendService,
    *,
    run_id: str,
    correlation_path: CorrelationPathSpec,
    correlation_path_id: str | None = None,
    impulse_id: str | None = None,
    effector_inputs: Mapping[str, Mapping[str, Any]] | None = None,
    effector_configs: Mapping[str, Mapping[str, Any]] | None = None,
    max_attempts: int = 1,
    priority: int = 0,
    actor: str | None = None,
    correlation_id: str | None = None,
    causation_id: str | None = None,
) -> CorrelationPathInstance:
    """Schedule one process per correlation_path effector, binding adapters to this host.

    Per-effector binding includes inheriting ``timeout_seconds`` and resolving
    :data:`PYTHON_COMMAND_PLACEHOLDER` tokens in subprocess adapter commands
    to the driving interpreter (``sys.executable``).
    """
    resolved_correlation_path_id = correlation_path_id or f"{run_id}:{correlation_path.id}"
    known_effectors = {effector.id for effector in correlation_path.effectors}
    inputs = {key: dict(value) for key, value in (effector_inputs or {}).items()}
    configs = {key: dict(value) for key, value in (effector_configs or {}).items()}
    unknown_inputs = sorted(set(inputs) - known_effectors)
    if unknown_inputs:
        raise ValueError(
            f"effector_inputs reference unknown correlation_path effectors: {unknown_inputs}"
        )
    unknown_configs = sorted(set(configs) - known_effectors)
    if unknown_configs:
        raise ValueError(
            f"effector_configs reference unknown correlation_path effectors: {unknown_configs}"
        )
    for effector_id, values in inputs.items():
        reserved = sorted(set(values) & set(_RESERVED_EFFECTOR_INPUT_KEYS))
        if reserved:
            raise ValueError(
                f"effector_inputs[{effector_id!r}] uses reserved keys: {reserved}"
            )

    processes: list[Process] = []
    for effector in correlation_path.effectors:
        adapter = effector.adapter
        if (
            effector.timeout_seconds is not None
            and adapter.timeout_seconds is None
            and adapter.kind != "manual_homeostat"
        ):
            adapter = adapter.model_copy(
                update={"timeout_seconds": effector.timeout_seconds}
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
            id=f"{resolved_correlation_path_id}:{effector.id}",
            run_id=run_id,
            process_type=effector.capability,
            impulse_id=impulse_id,
            status=(
                ProcessStatus.pending
                if effector.conduction
                else ProcessStatus.ready
            ),
            priority=priority,
            max_attempts=max_attempts,
            input={
                **inputs.get(effector.id, {}),
                "adapter": adapter.model_dump(mode="json"),
                "config": {**effector.config, **configs.get(effector.id, {})},
            },
            metadata={
                "correlation_path": {
                    "correlation_path_id": resolved_correlation_path_id,
                    "correlation_path_spec_id": correlation_path.id,
                    "effector_id": effector.id,
                    "conduction": list(effector.conduction),
                    "accumulate_upstream_reactions": (
                        correlation_path.accumulate_upstream_reactions
                    ),
                }
            },
        )
        stored, _ = await service.schedule_process(
            process,
            idempotency_key=(
                f"process.schedule:{resolved_correlation_path_id}:{effector.id}"
            ),
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )
        processes.append(stored)
    return CorrelationPathInstance(
        correlation_path_id=resolved_correlation_path_id,
        run_id=run_id,
        processes=processes,
    )


async def advance_correlation_path(
    service: RuntimeBackendService,
    *,
    run_id: str,
    correlation_path_id: str,
    actor: str | None = None,
) -> CorrelationPathAdvance:
    processes = await service.list_processes(run_id=run_id)
    members = correlation_path_effector_processes(processes, correlation_path_id)

    readied: list[Process] = []
    blocked: list[CorrelationPathBlockedEffector] = []
    for effector_id, process in members.items():
        if process.status != ProcessStatus.pending:
            continue
        marker = _correlation_path_marker(process) or {}
        conduction = [str(item) for item in marker.get("conduction") or []]
        unmet: list[str] = []
        dead: list[str] = []
        met: dict[str, Process] = {}
        for upstream_id in conduction:
            upstream = members.get(upstream_id)
            if upstream is None:
                raise ValueError(
                    f"CorrelationPath {correlation_path_id!r} effector {effector_id!r} conduction references unknown effector: "
                    f"{upstream_id!r}"
                )
            if upstream.status == ProcessStatus.succeeded:
                met[upstream_id] = upstream
            elif upstream.status in _DEAD_UPSTREAM_STATUSES:
                dead.append(upstream_id)
            elif (
                upstream.status == ProcessStatus.failed
                and upstream.attempt >= upstream.max_attempts
            ):
                dead.append(upstream_id)
            else:
                unmet.append(upstream_id)
        if not unmet and not dead:
            new_input = {
                **process.input,
                "conduction": {
                    upstream_id: dict(item.output) for upstream_id, item in met.items()
                },
            }
            if marker.get("accumulate_upstream_reactions"):
                # Every direct upstream has succeeded, so by induction the whole
                # transitive ancestor closure has too -- each ancestor's output is
                # real. Flatten their ``reactions`` in topological order (deepest
                # ancestor first) so a later producer of the same reaction wins for
                # consumers that scan latest-first.
                new_input["upstream_reactions"] = [
                    reaction
                    for ancestor_id in _ancestor_effectors_topo(effector_id, members)
                    for reaction in (members[ancestor_id].output.get("reactions") or [])
                ]
            stored, _ = await service.ready_process(
                run_id=run_id,
                process_id=process.id,
                input=new_input,
                idempotency_key=f"process.ready:{process.id}",
                actor=actor,
            )
            readied.append(stored)
        else:
            blocked.append(
                CorrelationPathBlockedEffector(
                    process_id=process.id,
                    effector_id=effector_id,
                    unmet=unmet,
                    dead=dead,
                )
            )
    return CorrelationPathAdvance(readied=readied, blocked=blocked)


async def advance_correlation_path_for_process(
    service: RuntimeBackendService,
    *,
    process: Process,
    actor: str | None = None,
) -> CorrelationPathAdvance | None:
    marker = _correlation_path_marker(process)
    if marker is None:
        return None
    correlation_path_id = marker.get("correlation_path_id")
    if not isinstance(correlation_path_id, str) or not correlation_path_id:
        return None
    return await advance_correlation_path(
        service,
        run_id=process.run_id,
        correlation_path_id=correlation_path_id,
        actor=actor,
    )


def _ancestor_effectors_topo(effector_id: str, members: dict[str, Process]) -> list[str]:
    """Transitive-upstream effector ids of ``effector_id`` in topological order.

    Post-order DFS over the ``conduction`` graph: each ancestor is emitted after its own
    conduction, so the result is a valid topological order (deepest ancestor first) with
    every ancestor appearing exactly once. ``effector_id`` itself is never emitted, and
    the ``seen`` set makes the walk terminate even if feedback cycles are allowed.
    """
    order: list[str] = []
    seen: set[str] = {effector_id}

    def visit(current: str) -> None:
        marker = _correlation_path_marker(members[current]) or {}
        for upstream_id in (str(item) for item in marker.get("conduction") or []):
            if upstream_id in members and upstream_id not in seen:
                seen.add(upstream_id)
                visit(upstream_id)
                order.append(upstream_id)

    visit(effector_id)
    return order


def correlation_path_effector_processes(
    processes: Iterable[Process], correlation_path_id: str
) -> dict[str, Process]:
    """The processes of correlation_path ``correlation_path_id``, keyed by effector id.

    Reads each process's ``metadata['correlation_path']`` marker -- the same channel
    :func:`advance_correlation_path` uses to assemble a correlation_path's members -- so a host-side
    caller never reconstructs the ``{correlation_path_id}:{effector_id}`` process-id format to
    find a correlation_path's effectors. A process without a matching marker is skipped; if two
    carry the same effector id (never in a normal correlation_path) the last wins.
    """
    members: dict[str, Process] = {}
    for process in processes:
        marker = _correlation_path_marker(process)
        if marker is None or marker.get("correlation_path_id") != correlation_path_id:
            continue
        effector_id = marker.get("effector_id")
        if isinstance(effector_id, str) and effector_id:
            members[effector_id] = process
    return members


def find_correlation_path_effector_process(
    processes: Iterable[Process], *, correlation_path_id: str, effector_id: str
) -> Process | None:
    """The process for ``effector_id`` within correlation_path ``correlation_path_id``, or ``None``.

    A convenience over :func:`correlation_path_effector_processes` for the common single-effector
    lookup a host-side reader performs (e.g. locating a correlation_path's report effector in a
    finished run's process list).
    """
    return correlation_path_effector_processes(processes, correlation_path_id).get(effector_id)


def _correlation_path_marker(process: Process) -> dict[str, Any] | None:
    marker = process.metadata.get("correlation_path")
    if isinstance(marker, dict):
        return marker
    return None


__all__ = [
    "CorrelationPathAdvance",
    "CorrelationPathBlockedEffector",
    "CorrelationPathInstance",
    "PYTHON_COMMAND_PLACEHOLDER",
    "advance_correlation_path",
    "advance_correlation_path_for_process",
    "find_correlation_path_effector_process",
    "correlation_path_effector_processes",
    "instantiate_correlation_path",
]
