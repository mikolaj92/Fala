"""InMemoryJournal — single-process Journal sink under one asyncio.Lock.

Authoritative store is process-local maps. Multi-process claim is not supported;
use SqliteJournal for multi-worker fleets. See docs/EVENT_STREAM_CORE.md.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from fala.journal.types import (
    AppendResult,
    ClaimRequest,
    ClaimResult,
    CommandUnit,
    JournalBatch,
    StateFact,
)
from fala.runtime_backend import Process, ProcessStatus, RuntimeCommand, RuntimeEvent
from fala.runtime_pure import (
    apply_lease_expired_fail,
    apply_process_claim,
    lease_expired_error,
    process_lease_expired_no_retries,
    select_next_claimable,
)
from fala.reactions import content_address_json


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _entity_key(entity: str, key: dict[str, str]) -> str:
    if "id" in key and len(key) == 1:
        return key["id"]
    if entity in {"impulse_type"} and "run_id" in key and "id" in key:
        return f"{key['run_id']}:{key['id']}"
    parts = [f"{k}={key[k]}" for k in sorted(key)]
    return "|".join(parts)


def apply_facts(
    entities: dict[str, dict[str, dict[str, Any]]],
    facts: list[StateFact],
    *,
    processes: dict[str, Process] | None = None,
) -> None:
    """Apply StateFacts in order (full replace on upsert). Mutates ``entities``."""
    for fact in facts:
        bucket = entities.setdefault(fact.entity, {})
        ek = _entity_key(fact.entity, fact.key)
        if fact.op == "delete":
            bucket.pop(ek, None)
            if processes is not None and fact.entity == "process":
                processes.pop(ek, None)
            continue
        body = dict(fact.body)
        bucket[ek] = body
        if processes is not None and fact.entity == "process":
            processes[ek] = Process.model_validate(body)


class InMemoryJournal:
    """Journal Protocol implementation backed by in-process maps."""

    def __init__(self, *, stream_id: str = "memory://local") -> None:
        self._uri = stream_id if stream_id.startswith("memory://") else f"memory://{stream_id}"
        self._lock = asyncio.Lock()
        self._journal_seq = 0
        self._batches: list[JournalBatch] = []
        self._commands: dict[tuple[str, str], RuntimeCommand] = {}
        self._commands_by_id: dict[tuple[str, str], RuntimeCommand] = {}
        self._command_order: list[RuntimeCommand] = []
        self._events: dict[str, list[RuntimeEvent]] = {}
        self._event_seq: dict[str, int] = {}
        self._entities: dict[str, dict[str, dict[str, Any]]] = {}
        self._processes: dict[str, Process] = {}

    @property
    def runtime_uri(self) -> str:
        return self._uri

    async def append_batch(self, batch: JournalBatch) -> AppendResult:
        async with self._lock:
            return self._append_batch_unlocked(batch)

    def _append_batch_unlocked(self, batch: JournalBatch) -> AppendResult:
        if not batch.units:
            raise ValueError("JournalBatch.units must be non-empty")
        leading = batch.units[0].command
        lead_key = (leading.run_id, leading.idempotency_key)
        if lead_key in self._commands:
            stored = self._commands[lead_key]
            return AppendResult(
                batch=batch,
                replayed=True,
                units=[
                    CommandUnit(command=stored, events=[], facts=[])
                    for _ in batch.units
                ],
            )

        # Partial history guard: non-leading key must not exist alone.
        for unit in batch.units[1:]:
            nk = (unit.command.run_id, unit.command.idempotency_key)
            if nk in self._commands:
                raise ValueError(
                    "Corrupt partial history: non-leading idempotency key "
                    f"{unit.command.idempotency_key!r} exists without leading key"
                )

        assigned_units: list[CommandUnit] = []
        for unit in batch.units:
            cmd = unit.command
            ckey = (cmd.run_id, cmd.idempotency_key)
            if ckey in self._commands:
                raise ValueError(
                    f"Duplicate command idempotency key within batch: {cmd.idempotency_key!r}"
                )
            events = self._assign_events(cmd, unit.events)
            apply_facts(self._entities, unit.facts, processes=self._processes)
            self._commands[ckey] = cmd
            self._commands_by_id[(cmd.run_id, cmd.id)] = cmd
            self._command_order.append(cmd)
            assigned_units.append(
                CommandUnit(command=cmd, events=events, facts=list(unit.facts))
            )

        self._journal_seq += 1
        stored_batch = batch.model_copy(
            update={
                "journal_seq": self._journal_seq,
                "units": assigned_units,
            }
        )
        self._batches.append(stored_batch)
        return AppendResult(batch=stored_batch, replayed=False, units=assigned_units)

    def _assign_events(
        self, command: RuntimeCommand, events: list[RuntimeEvent]
    ) -> list[RuntimeEvent]:
        run_id = command.run_id
        seq = self._event_seq.get(run_id, 0)
        out: list[RuntimeEvent] = []
        for event in events:
            seq += 1
            stored = event.model_copy(
                update={
                    "run_id": run_id,
                    "command_id": command.id,
                    "sequence": seq,
                    "actor": event.actor if event.actor is not None else command.actor,
                    "correlation_id": (
                        event.correlation_id
                        if event.correlation_id is not None
                        else command.correlation_id
                    ),
                    "causation_id": (
                        event.causation_id
                        if event.causation_id is not None
                        else command.causation_id
                    ),
                }
            )
            self._events.setdefault(run_id, []).append(stored)
            out.append(stored)
        self._event_seq[run_id] = seq
        return out

    async def claim_next(self, request: ClaimRequest) -> ClaimResult:
        if request.lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than zero")
        if request.run_id is None and not request.all_runs:
            raise ValueError(
                "claim_next requires run_id, or all_runs=True to claim across every run"
            )
        async with self._lock:
            now = _now()
            units: list[CommandUnit] = []
            run_filter = None if request.all_runs else request.run_id
            batch_run_id = request.run_id or "mixed"

            # Collect lease reaps (mutate process map only after full batch build).
            reaped_snapshots: list[tuple[Process, Process, dict[str, Any]]] = []
            for process in list(self._processes.values()):
                if run_filter is not None and process.run_id != run_filter:
                    continue
                if not process_lease_expired_no_retries(process, now=now):
                    continue
                failed = apply_lease_expired_fail(process, now=now)
                err = lease_expired_error(process)
                reaped_snapshots.append((process, failed, err))

            for original, failed, err in reaped_snapshots:
                fail_cmd = RuntimeCommand(
                    run_id=failed.run_id,
                    command_type="process.fail",
                    idempotency_key=f"process.fail:{failed.id}:{failed.attempt}",
                    actor=request.worker_id,
                    payload={"process_id": failed.id},
                )
                fail_event = RuntimeEvent(
                    run_id=failed.run_id,
                    impulse_id=failed.impulse_id,
                    process_id=failed.id,
                    event_type="process.failed",
                    payload={
                        "process_id": failed.id,
                        "attempt": failed.attempt,
                        "input_digest": content_address_json(original.input),
                        "error_digest": content_address_json(err),
                    },
                )
                units.append(
                    CommandUnit(
                        command=fail_cmd,
                        events=[fail_event],
                        facts=[
                            StateFact(
                                entity="process",
                                op="upsert",
                                key={"id": failed.id},
                                body=failed.model_dump(mode="json"),
                            )
                        ],
                    )
                )
                # Apply reap to working set before claim selection.
                self._processes[failed.id] = failed

            candidates = list(self._processes.values())
            chosen = select_next_claimable(
                candidates, now=now, run_id=run_filter
            )
            claimed: Process | None = None
            if chosen is not None:
                claimed = apply_process_claim(
                    chosen,
                    worker_id=request.worker_id,
                    lease_seconds=request.lease_seconds,
                    now=now,
                )
                claim_cmd = RuntimeCommand(
                    run_id=claimed.run_id,
                    command_type="process.claim",
                    idempotency_key=f"process.claim:{claimed.id}:{claimed.attempt}",
                    actor=request.worker_id,
                    payload={
                        "process_id": claimed.id,
                        "worker_id": request.worker_id,
                        "attempt": claimed.attempt,
                        "lease_expires_at": (
                            claimed.lease_expires_at.isoformat()
                            if claimed.lease_expires_at
                            else None
                        ),
                    },
                )
                claim_event = RuntimeEvent(
                    run_id=claimed.run_id,
                    impulse_id=claimed.impulse_id,
                    process_id=claimed.id,
                    event_type="process.claimed",
                    payload={
                        "process_id": claimed.id,
                        "worker_id": request.worker_id,
                        "attempt": claimed.attempt,
                        "lease_expires_at": claim_cmd.payload.get("lease_expires_at"),
                    },
                )
                units.append(
                    CommandUnit(
                        command=claim_cmd,
                        events=[claim_event],
                        facts=[
                            StateFact(
                                entity="process",
                                op="upsert",
                                key={"id": claimed.id},
                                body=claimed.model_dump(mode="json"),
                            )
                        ],
                    )
                )
                batch_run_id = claimed.run_id
            elif reaped_snapshots:
                batch_run_id = reaped_snapshots[0][1].run_id

            if not units:
                return ClaimResult(process=None, batch=None, replayed=False)

            # One atomic multi-unit batch for reaps (+ optional claim).
            # Undo provisional process map updates; append_batch re-applies facts.
            for original, failed, _err in reaped_snapshots:
                self._processes[original.id] = original
            append = self._append_batch_unlocked(
                JournalBatch(run_id=batch_run_id, units=units)
            )
            process_out = claimed
            if claimed is not None:
                process_out = self._processes.get(claimed.id, claimed)
            return ClaimResult(
                process=process_out,
                batch=append.batch,
                replayed=append.replayed,
            )

    async def get_command_by_idempotency(
        self, *, run_id: str, idempotency_key: str
    ) -> RuntimeCommand | None:
        async with self._lock:
            return self._commands.get((run_id, idempotency_key))

    async def list_events(
        self,
        *,
        run_id: str,
        after_sequence: int | None = None,
        impulse_id: str | None = None,
        limit: int | None = None,
    ) -> list[RuntimeEvent]:
        async with self._lock:
            events = list(self._events.get(run_id, []))
            if after_sequence is not None:
                events = [e for e in events if (e.sequence or 0) > after_sequence]
            if impulse_id is not None:
                events = [e for e in events if e.impulse_id == impulse_id]
            if limit is not None:
                events = events[:limit]
            return events

    async def load(
        self,
        *,
        run_id: str | None = None,
        after_journal_seq: int | None = None,
        limit: int | None = None,
    ) -> list[JournalBatch]:
        async with self._lock:
            batches = list(self._batches)
            if after_journal_seq is not None:
                batches = [
                    b
                    for b in batches
                    if b.journal_seq is not None and b.journal_seq > after_journal_seq
                ]
            if run_id is not None:
                batches = [b for b in batches if b.run_id == run_id]
            if limit is not None:
                batches = batches[:limit]
            return batches

    def recover_entities(self) -> dict[str, dict[str, dict[str, Any]]]:
        """Rebuild entity maps from durable batches (pure recover helper)."""
        entities: dict[str, dict[str, dict[str, Any]]] = {}
        processes: dict[str, Process] = {}
        for batch in self._batches:
            for unit in batch.units:
                apply_facts(entities, unit.facts, processes=processes)
        return entities

    def get_process(self, process_id: str) -> Process | None:
        return self._processes.get(process_id)

    def seed_process(self, process: Process) -> None:
        """Test/admin helper: put a process into the claimable set without a batch."""
        self._processes[process.id] = process
        self._entities.setdefault("process", {})[process.id] = process.model_dump(
            mode="json"
        )

    def upsert_process(self, process: Process) -> None:
        """Replace process snapshot in the claimable/store maps (non-journaled)."""
        self.seed_process(process)

    def get_command(self, *, run_id: str, command_id: str) -> RuntimeCommand | None:
        return self._commands_by_id.get((run_id, command_id))

    def list_commands(
        self,
        *,
        run_id: str,
        command_type: str | None = None,
        actor: str | None = None,
        limit: int | None = None,
    ) -> list[RuntimeCommand]:
        items = [c for c in self._command_order if c.run_id == run_id]
        if command_type is not None:
            items = [c for c in items if c.command_type == command_type]
        if actor is not None:
            items = [c for c in items if c.actor == actor]
        items = sorted(items, key=lambda c: (c.created_at, c.id))
        if limit is not None:
            items = items[:limit]
        return items

    def put_entity_body(self, entity: str, key: dict[str, str], body: dict[str, Any]) -> None:
        """Non-journaled entity upsert into the authoritative entity maps."""
        ek = _entity_key(entity, key)
        self._entities.setdefault(entity, {})[ek] = body
        if entity == "process" and "id" in key:
            self._processes[key["id"]] = Process.model_validate(body)

    def delete_entity(self, entity: str, key: dict[str, str]) -> None:
        ek = _entity_key(entity, key)
        bucket = self._entities.get(entity)
        if bucket is not None:
            bucket.pop(ek, None)
        if entity == "process" and "id" in key:
            self._processes.pop(key["id"], None)

    def get_entity_body(self, entity: str, key: dict[str, str]) -> dict[str, Any] | None:
        return self._entities.get(entity, {}).get(_entity_key(entity, key))

    def list_entity_bodies(self, entity: str) -> list[tuple[str, dict[str, Any]]]:
        return list(self._entities.get(entity, {}).items())

    def clear_run_entities(self, run_id: str) -> dict[str, int]:
        """Best-effort delete of run-scoped state (InMemory admin path)."""
        counts: dict[str, int] = {}
        # Processes
        proc_ids = [pid for pid, p in self._processes.items() if p.run_id == run_id]
        for pid in proc_ids:
            self._processes.pop(pid, None)
        counts["processes"] = len(proc_ids)

        for entity, bucket in list(self._entities.items()):
            removed = 0
            for ek, body in list(bucket.items()):
                body_run = body.get("run_id")
                if body_run == run_id or (entity == "run" and body.get("id") == run_id):
                    bucket.pop(ek, None)
                    removed += 1
            counts[entity] = counts.get(entity, 0) + removed

        # Commands / events for run
        cmd_keys = [k for k in self._commands if k[0] == run_id]
        for k in cmd_keys:
            self._commands.pop(k, None)
        counts["runtime_commands"] = len(cmd_keys)
        id_keys = [k for k in self._commands_by_id if k[0] == run_id]
        for k in id_keys:
            self._commands_by_id.pop(k, None)
        self._command_order = [c for c in self._command_order if c.run_id != run_id]
        ev_count = len(self._events.pop(run_id, []))
        counts["runtime_events"] = ev_count
        self._event_seq.pop(run_id, None)
        return counts
