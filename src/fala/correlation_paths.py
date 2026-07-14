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
    capability_output_schemas: Mapping[str, dict[str, Any]] | None = None,
    accepted_reaction_kinds_by_effector: Mapping[str, list[str]] | None = None,
    auto_advance: bool = True,
    regulation_by_effector: Mapping[str, dict[str, Any]] | None = None,
    max_attempts_by_effector: Mapping[str, int] | None = None,
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
    for idx, effector in enumerate(correlation_path.effectors):
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
            # Precedence (schedule time):
            # 1. regulation["max_attempts"] for this effector (dynamic damping)
            # 2. max_attempts_by_effector override
            # 3. global max_attempts default
            max_attempts=(
                int(reg["max_attempts"])
                if isinstance((reg := (regulation_by_effector or {}).get(effector.id, {})), dict)
                   and "max_attempts" in reg
                else (max_attempts_by_effector or {}).get(effector.id, max_attempts)
            ),
            input={
                **inputs.get(effector.id, {}),
                "adapter": adapter.model_dump(mode="json"),
                "config": {**effector.config, **configs.get(effector.id, {})},
                # Inject regulation into input for root effectors (ready at birth)
                # so the same "regulation" key is visible as for downstreams.
                **(
                    {"regulation": dict(reg)}
                    if isinstance((reg := (regulation_by_effector or {}).get(effector.id, {})), dict) and reg
                    else {}
                ),
            },
            output_schema=(
                capability_output_schemas.get(effector.capability, {})
                if capability_output_schemas
                else {}
            ),
            metadata={
                "correlation_path": {
                    "correlation_path_id": resolved_correlation_path_id,
                    "correlation_path_spec_id": correlation_path.id,
                    "effector_id": effector.id,
                    "seq": idx,  # declaration order in CorrelationPathSpec.effectors; absolute tor sequence
                    "conduction": list(effector.conduction),
                    "accumulate_upstream_reactions": (
                        correlation_path.accumulate_upstream_reactions
                    ),
                    **({"auto_advance": False} if not auto_advance else {}),
                    **({"regulation": dict((regulation_by_effector or {}).get(effector.id, {}))} if (regulation_by_effector or {}).get(effector.id) else {}),
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
    readies = compute_correlation_path_readies(processes, correlation_path_id)

    readied: list[Process] = []
    blocked: list[CorrelationPathBlockedEffector] = []
    # Recompute members for blocked reporting
    members = correlation_path_effector_processes(processes, correlation_path_id)
    for effector_id, process in members.items():
        if process.status != ProcessStatus.pending:
            continue
        marker = _correlation_path_marker(process) or {}
        conduction = [str(item) for item in marker.get("conduction") or []]
        unmet: list[str] = []
        dead: list[str] = []
        for upstream_id in conduction:
            upstream = members.get(upstream_id)
            if upstream is None:
                raise ValueError(
                    f"CorrelationPath {correlation_path_id!r} effector {effector_id!r} conduction references unknown effector: "
                    f"{upstream_id!r}"
                )
            if upstream.status == ProcessStatus.succeeded:
                pass
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
            pass
        else:
            blocked.append(
                CorrelationPathBlockedEffector(
                    process_id=process.id,
                    effector_id=effector_id,
                    unmet=unmet,
                    dead=dead,
                )
            )
    for process_id, new_input in readies:
        stored, _ = await service.ready_process(
            run_id=run_id,
            process_id=process_id,
            input=new_input,
            idempotency_key=f"process.ready:{process_id}",
            actor=actor,
        )
        readied.append(stored)
    return CorrelationPathAdvance(readied=readied, blocked=blocked)
def _filter_upstream_reactions(reactions: list[dict[str, Any]], allowed: list[str] | None) -> list[dict[str, Any]]:
    if not allowed:
        return list(reactions)
    allow = set(allowed)
    return [r for r in reactions if (r or {}).get("kind") in allow]


def _project_conduction_output(upstream: Process) -> dict[str, Any]:
    """Project only the domain-relevant part of an upstream's output for conduction.

    Strips execution noise (e.g. "adapter" envelope with returncode/stdout/stderr).
    If the upstream process carries an output_schema with properties, only those
    top-level keys are kept. Otherwise, prefers the conventional "values" sub-dict
    when present (as produced by sdk.output(values=...)).
    """
    out = dict(upstream.output or {})
    out.pop("adapter", None)
    schema = upstream.output_schema or {}
    if isinstance(schema, dict):
        props = schema.get("properties")
        if isinstance(props, dict) and props:
            return {k: out[k] for k in props if k in out}
    if "values" in out and isinstance(out["values"], dict):
        return dict(out["values"])
    return out

def compute_correlation_path_readies(
    processes: Iterable[Process], correlation_path_id: str
) -> list[tuple[str, dict[str, Any]]]:
    """Pure computation: which pending effectors in this correlation_path are now ready.

    Returns list of (process_id, new_input_dict) for those that can be transitioned to ready.
    Does not perform any I/O. Used by both public advance and internal atomic advance
    inside process completion transaction.
    """
    members = correlation_path_effector_processes(processes, correlation_path_id)
    readies: list[tuple[str, dict[str, Any]]] = []
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
                    upstream_id: _project_conduction_output(item)
                    for upstream_id, item in met.items()
                },
            }
            # Cybernetic regulation hook (Mazur gap closure):
            # If the correlation_path marker carries a "regulation" dict (populated
            # at schedule or via future projection/association feedback), it is
            # injected into the downstream input under the "regulation" key.
            # This is the primary entry point for quantitative damping, entropy
            # signals, or external variety measurements without changing conduction
            # topology.
            reg = marker.get("regulation")
            if isinstance(reg, dict) and reg:
                new_input["regulation"] = dict(reg)
            # Accumulate regulation from immediate upstreams (conduction carries control).
            # This turns per-effector static regulation into a propagating signal:
            # upstreams that received regulation (via marker or prior propagation) pass
            # it downstream so later effectors can react (damping, entropy, variety).
            upstream_regs: list[dict[str, Any]] = []
            for item in met.values():
                r = (item.input or {}).get("regulation") if isinstance(item.input, dict) else None
                if isinstance(r, dict) and r:
                    upstream_regs.append(r)
            if upstream_regs:
                merged: dict[str, Any] = {}
                for r in upstream_regs:
                    merged.update(r)
                if "regulation" in new_input and isinstance(new_input["regulation"], dict):
                    merged.update(new_input["regulation"])
                new_input["regulation"] = merged
            if marker.get("accumulate_upstream_reactions"):
                allowed = marker.get("accepted_reaction_kinds")
                raw = [
                    reaction
                    for ancestor_id in _ancestor_effectors_topo(effector_id, members)
                    for reaction in (members[ancestor_id].output.get("reactions") or [])
                ]
                new_input["upstream_reactions"] = _filter_upstream_reactions(raw, allowed)
            readies.append((process.id, new_input))
    return readies
    # (clean)

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


def compute_correlation_path_dead_cancellations(
    processes: Iterable[Process], correlation_path_id: str
) -> list[tuple[str, dict[str, Any]]]:
    """Pure: pending effectors that have at least one permanently dead upstream.

    Returns (process_id, error_dict) for each such pending effector.
    The error_dict will be used as the error payload on cancel.
    """
    members = correlation_path_effector_processes(processes, correlation_path_id)
    to_cancel: list[tuple[str, dict[str, Any]]] = []
    for effector_id, process in members.items():
        if process.status != ProcessStatus.pending:
            continue
        marker = _correlation_path_marker(process) or {}
        conduction = [str(item) for item in marker.get("conduction") or []]
        dead_up: list[str] = []
        for upstream_id in conduction:
            upstream = members.get(upstream_id)
            if upstream is None:
                # unknown -> treat as permanent for safety
                dead_up.append(upstream_id)
                continue
            if upstream.status in _DEAD_UPSTREAM_STATUSES:
                dead_up.append(upstream_id)
            elif (
                upstream.status == ProcessStatus.failed
                and upstream.attempt >= upstream.max_attempts
            ):
                dead_up.append(upstream_id)
        if dead_up:
            err = {
                "type": "DeadUpstream",
                "message": "upstream effector(s) permanently dead; downstream cannot proceed",
                "dead_upstreams": dead_up,
            }
            # Cybernetic regulation hook (symmetric to readies):
            # Preserve the per-effector regulation dict (if present) inside the
            # cancel error so downstream consumers or observers can react with
            # context (entropy, damping signals, etc.).
            reg = marker.get("regulation")
            if isinstance(reg, dict) and reg:
                err["regulation"] = dict(reg)
            to_cancel.append((process.id, err))
    return to_cancel


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


def _validate_correlation_path_marker(marker: dict[str, Any], *, process_id: str) -> None:
    """Validate the shape and basic invariants of a correlation_path marker embedded in Process.metadata.

    This is called at schedule time to reject bad metadata injection that would bypass
    CorrelationPathSpec validation. It does not require the full graph (unknown upstreams
    are still caught at advance time).
    """
    if not isinstance(marker.get("correlation_path_id"), str) or not marker["correlation_path_id"]:
        raise ValueError(f"Process {process_id!r} correlation_path marker missing correlation_path_id")
    if not isinstance(marker.get("effector_id"), str) or not marker["effector_id"]:
        raise ValueError(f"Process {process_id!r} correlation_path marker missing effector_id")
    conduction = marker.get("conduction")
    if not isinstance(conduction, list) or not all(isinstance(x, str) for x in conduction):
        raise ValueError(f"Process {process_id!r} correlation_path marker conduction must be list[str]")
    if len(conduction) != len(set(conduction)):
        raise ValueError(f"Process {process_id!r} correlation_path marker has duplicate conduction entries")
    effector_id = marker["effector_id"]
    if effector_id in conduction:
        raise ValueError(f"Process {process_id!r} correlation_path effector cannot depend on itself")
    known = marker.get("known_effectors")
    if known is not None:
        if not isinstance(known, list) or not all(isinstance(x, str) for x in known):
            raise ValueError(f"Process {process_id!r} correlation_path marker known_effectors must be list[str]")
        missing = [u for u in conduction if u not in known]
        if missing:
            raise ValueError(
                f"Process {process_id!r} correlation_path conduction refs unknown effectors in this path: {missing}"
            )
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
    # Collect with their seq for absolute tor order (declaration sequence).
    items: list[tuple[int, str, Process]] = []
    for process in processes:
        marker = _correlation_path_marker(process)
        if marker is None or marker.get("correlation_path_id") != correlation_path_id:
            continue
        effector_id = marker.get("effector_id")
        if isinstance(effector_id, str) and effector_id:
            seq = marker.get("seq")
            s = int(seq) if isinstance(seq, (int, str)) else 0
            items.append((s, effector_id, process))
    # Sort by seq (primary), then effector_id for stable tie-break.
    items.sort(key=lambda t: (t[0], t[1]))
    for _, effector_id, process in items:
        members[effector_id] = process
    return members


def find_correlation_path_effector_process(
    processes: Iterable[Process], *, correlation_path_id: str, effector_id: str
) -> Process | None:
    """The process for effector_id within correlation_path correlation_path_id, or None.

    A convenience over correlation_path_effector_processes for the common single-effector
    lookup a host-side reader performs (e.g. locating a correlation_path report effector in a
    finished run process list).
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
    "compute_correlation_path_readies",
    "_validate_correlation_path_marker",
    "compute_correlation_path_dead_cancellations",
    "instantiate_correlation_path",
]
