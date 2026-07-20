"""JournalBackedBackend — full RuntimeBackend adapter over a Journal sink.

SqliteJournal path: thin delegation to the embedded Correlator.
InMemoryJournal path: map-backed RuntimeBackend with command-path mutations
via journal.append_batch / claim_next.

See docs/EVENT_STREAM_CORE.md §PR5 and §JournalBackedBackend.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Sequence

from fala.journal.memory import InMemoryJournal
from fala.journal.sqlite import SqliteJournal
from fala.journal.types import (
    ClaimRequest,
    CommandUnit,
    JournalBatch,
    StateFact,
)
from fala.runtime_backend import (
    Association,
    BridgeDelivery,
    BridgeDeliveryStatus,
    CommandSubmission,
    DelegationPolicy,
    Homeostat,
    HomeostatStatus,
    Impulse,
    ImpulseRelation,
    ImpulseType,
    Process,
    ProcessStatus,
    Projection,
    Reaction,
    Run,
    RunStatus,
    RuntimeCommand,
    RuntimeEvent,
    RuntimePool,
    RuntimeBackend,
)
from fala.runtime_pure import (
    TERMINAL_PROCESS_STATUSES,
    TERMINAL_RUN_STATUSES,
    validate_process_can_finish,
    validate_process_can_ready,
    validate_process_can_retry,
    validate_process_can_wait,
    validate_process_transition_command,
    validate_run_status_transition,
)

_BUILT_IN_PROJECTIONS = ("run_summary",)
_HOMEOSTAT_TRANSITION_COMMANDS = {
    HomeostatStatus.completed: "homeostat.complete",
    HomeostatStatus.cancelled: "homeostat.cancel",
    HomeostatStatus.expired: "homeostat.expire",
}


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _fact(
    entity: str,
    body: dict[str, Any],
    *,
    key: dict[str, str] | None = None,
    op: str = "upsert",
) -> StateFact:
    if key is None:
        if "id" in body:
            key = {"id": str(body["id"])}
        elif entity == "projection" and "run_id" in body and "name" in body:
            key = {"run_id": str(body["run_id"]), "name": str(body["name"])}
        else:
            key = {"id": str(body.get("id", ""))}
    return StateFact(entity=entity, op=op, key=key, body=body)  # type: ignore[arg-type]


def _submission(result_units: list[CommandUnit], *, replayed: bool) -> CommandSubmission:
    unit = result_units[0]
    return CommandSubmission(
        command=unit.command,
        events=list(unit.events),
        replayed=replayed,
    )


class InMemoryRuntimeBackend:
    """Full RuntimeBackend over InMemoryJournal maps + append_batch / claim_next."""

    def __init__(self, journal: InMemoryJournal) -> None:
        self._journal = journal
        # Typed entity maps (authoritative for non-process put/get).
        # Process authority is journal._processes (shared with claim_next).
        self._runs: dict[str, Run] = {}
        self._impulses: dict[tuple[str, str], Impulse] = {}
        self._impulse_types: dict[tuple[str, str], ImpulseType] = {}
        self._impulse_relations: dict[tuple[str, str], ImpulseRelation] = {}
        self._associations: dict[tuple[str, str], Association] = {}
        self._reactions: dict[tuple[str, str], Reaction] = {}
        self._homeostats: dict[tuple[str, str], Homeostat] = {}
        self._projections: dict[tuple[str, str], Projection] = {}  # run_id, name
        self._pools: dict[str, RuntimePool] = {}
        self._policies: dict[str, DelegationPolicy] = {}
        self._outbox: dict[tuple[str, str], BridgeDelivery] = {}
        self._inbox: dict[tuple[str, str], BridgeDelivery] = {}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _require_run(self, run_id: str) -> Run:
        run = self._runs.get(run_id)
        if run is None:
            raise ValueError(f"Unknown run: {run_id!r}")
        return run

    def _sync_from_fact(self, fact: StateFact) -> None:
        if fact.op == "delete":
            self._delete_from_fact(fact)
            return
        body = fact.body
        entity = fact.entity
        if entity == "run":
            run = Run.model_validate(body)
            self._runs[run.id] = run
        elif entity == "impulse":
            impulse = Impulse.model_validate(body)
            self._impulses[(impulse.run_id, impulse.id)] = impulse
        elif entity == "impulse_type":
            item = ImpulseType.model_validate(body)
            self._impulse_types[(item.run_id, item.id)] = item
        elif entity == "impulse_relation":
            item = ImpulseRelation.model_validate(body)
            self._impulse_relations[(item.run_id, item.id)] = item
        elif entity == "association":
            item = Association.model_validate(body)
            self._associations[(item.run_id, item.id)] = item
        elif entity == "reaction":
            item = Reaction.model_validate(body)
            self._reactions[(item.run_id, item.id)] = item
        elif entity == "homeostat":
            item = Homeostat.model_validate(body)
            self._homeostats[(item.run_id, item.id)] = item
        elif entity == "projection":
            item = Projection.model_validate(body)
            self._projections[(item.run_id, item.name)] = item
        elif entity == "bridge_outbox":
            item = BridgeDelivery.model_validate(body)
            self._outbox[(item.run_id, item.id)] = item
        elif entity == "bridge_inbox":
            item = BridgeDelivery.model_validate(body)
            self._inbox[(item.run_id, item.id)] = item
        elif entity == "runtime_pool":
            item = RuntimePool.model_validate(body)
            self._pools[item.id] = item
        elif entity == "delegation_policy":
            item = DelegationPolicy.model_validate(body)
            self._policies[item.id] = item
        # process handled by journal.apply_facts

    def _delete_from_fact(self, fact: StateFact) -> None:
        entity = fact.entity
        key = fact.key
        if entity == "run" and "id" in key:
            self._runs.pop(key["id"], None)
        elif entity == "process" and "id" in key:
            pass  # journal handles
        elif entity == "impulse" and "id" in key:
            run_id = key.get("run_id")
            if run_id:
                self._impulses.pop((run_id, key["id"]), None)
            else:
                for k in list(self._impulses):
                    if k[1] == key["id"]:
                        self._impulses.pop(k, None)
        elif entity == "projection" and "name" in key:
            run_id = key.get("run_id")
            if run_id:
                self._projections.pop((run_id, key["name"]), None)

    def _apply_append_result(self, units: list[CommandUnit], *, replayed: bool) -> None:
        if replayed:
            return
        for unit in units:
            for fact in unit.facts:
                self._sync_from_fact(fact)

    async def _append(
        self,
        *,
        run_id: str,
        command: RuntimeCommand,
        events: Sequence[RuntimeEvent],
        facts: list[StateFact],
        extra_units: list[CommandUnit] | None = None,
    ) -> CommandSubmission:
        units = [
            CommandUnit(
                command=command,
                events=list(events),
                facts=list(facts),
            )
        ]
        if extra_units:
            units.extend(extra_units)
        result = await self._journal.append_batch(
            JournalBatch(run_id=run_id, units=units)
        )
        self._apply_append_result(result.units, replayed=result.replayed)
        return _submission(result.units, replayed=result.replayed)

    # ------------------------------------------------------------------
    # Runs
    # ------------------------------------------------------------------

    async def create_run(
        self,
        run: Run,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != run.id:
            raise ValueError("run.create command run_id must match run id")
        if command.command_type != "run.create":
            raise ValueError("create_run requires command_type 'run.create'")
        existing = await self._journal.get_command_by_idempotency(
            run_id=command.run_id, idempotency_key=command.idempotency_key
        )
        if existing is not None:
            return CommandSubmission(command=existing, events=[], replayed=True)
        if run.id in self._runs:
            raise ValueError(f"Run already exists: {run.id!r}")
        return await self._append(
            run_id=run.id,
            command=command,
            events=events,
            facts=[_fact("run", run.model_dump(mode="json"), key={"id": run.id})],
        )

    async def put_run(self, run: Run) -> None:
        self._runs[run.id] = run
        self._journal.put_entity_body("run", {"id": run.id}, run.model_dump(mode="json"))

    async def transition_run(
        self,
        *,
        run_id: str,
        status: RunStatus,
        command: RuntimeCommand,
        events: Sequence[RuntimeEvent] = (),
    ) -> tuple[Run, CommandSubmission]:
        if command.run_id != run_id:
            raise ValueError("run transition command run_id must match run_id")
        if command.command_type == "run.cancel":
            if status != RunStatus.cancel_requested:
                raise ValueError("run.cancel requires status 'cancel_requested'")
        elif command.command_type != "run.status.set":
            raise ValueError(
                "transition_run requires command_type 'run.status.set' or 'run.cancel'"
            )
        existing = await self._journal.get_command_by_idempotency(
            run_id=command.run_id, idempotency_key=command.idempotency_key
        )
        if existing is not None:
            stored_run_id = str(existing.payload.get("run_id", run_id))
            stored = self._runs.get(stored_run_id)
            if stored is None:
                raise ValueError(
                    f"Replayed run transition has no stored run: {stored_run_id!r}"
                )
            return stored, CommandSubmission(
                command=existing, events=[], replayed=True
            )
        run = self._require_run(run_id)
        if run.status != status:
            validate_run_status_transition(run.status, status)
        now = _now()
        started_at = run.started_at or (now if status == RunStatus.active else None)
        finished_at = now if status in TERMINAL_RUN_STATUSES else run.finished_at
        updated = run.model_copy(
            update={
                "status": status,
                "updated_at": now,
                "started_at": started_at,
                "finished_at": finished_at,
            }
        )
        submission = await self._append(
            run_id=run_id,
            command=command,
            events=events,
            facts=[
                _fact("run", updated.model_dump(mode="json"), key={"id": run_id})
            ],
        )
        return self._runs[run_id], submission

    async def get_run(self, *, run_id: str) -> Run | None:
        return self._runs.get(run_id)

    async def list_runs(
        self,
        *,
        status: RunStatus | None = None,
        limit: int | None = None,
    ) -> list[Run]:
        items = list(self._runs.values())
        if status is not None:
            items = [r for r in items if r.status == status]
        items = sorted(items, key=lambda r: (r.created_at, r.id))
        if limit is not None:
            items = items[:limit]
        return items

    async def delete_run(self, *, run_id: str) -> dict[str, int]:
        counts = self._journal.clear_run_entities(run_id)
        self._runs.pop(run_id, None)
        for mapping in (
            self._impulses,
            self._impulse_types,
            self._impulse_relations,
            self._associations,
            self._reactions,
            self._homeostats,
            self._projections,
            self._outbox,
            self._inbox,
        ):
            for key in [k for k in mapping if k[0] == run_id]:
                mapping.pop(key, None)
        counts["runs"] = 1 if "runs" not in counts else counts.get("runs", 0)
        return counts

    async def vacuum(self) -> dict[str, Any]:
        return {
            "store_type": "memory",
            "vacuumed": True,
            "bytes_before": 0,
            "bytes_after": 0,
            "bytes_reclaimed": 0,
        }

    # ------------------------------------------------------------------
    # Pools / policies
    # ------------------------------------------------------------------

    async def put_runtime_pool(self, pool: RuntimePool) -> None:
        self._pools[pool.id] = pool

    async def get_runtime_pool(self, *, pool_id: str) -> RuntimePool | None:
        return self._pools.get(pool_id)

    async def list_runtime_pools(self) -> list[RuntimePool]:
        return sorted(self._pools.values(), key=lambda p: p.id)

    async def put_delegation_policy(self, policy: DelegationPolicy) -> None:
        self._policies[policy.id] = policy

    async def get_delegation_policy(
        self, *, policy_id: str
    ) -> DelegationPolicy | None:
        return self._policies.get(policy_id)

    async def list_delegation_policies(
        self, *, pool_id: str | None = None
    ) -> list[DelegationPolicy]:
        items = list(self._policies.values())
        if pool_id is not None:
            items = [p for p in items if p.pool_id == pool_id]
        return sorted(items, key=lambda p: p.id)

    # ------------------------------------------------------------------
    # Impulse types
    # ------------------------------------------------------------------

    async def put_impulse_type(self, impulse_type: ImpulseType) -> None:
        self._require_run(impulse_type.run_id)
        self._impulse_types[(impulse_type.run_id, impulse_type.id)] = impulse_type

    async def register_impulse_type(
        self,
        impulse_type: ImpulseType,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != impulse_type.run_id:
            raise ValueError(
                "impulse_type.register command run_id must match impulse_type run_id"
            )
        if command.command_type != "impulse_type.register":
            raise ValueError(
                "register_impulse_type requires command_type 'impulse_type.register'"
            )
        existing = await self._journal.get_command_by_idempotency(
            run_id=command.run_id, idempotency_key=command.idempotency_key
        )
        if existing is not None:
            return CommandSubmission(command=existing, events=[], replayed=True)
        self._require_run(impulse_type.run_id)
        if (impulse_type.run_id, impulse_type.id) in self._impulse_types:
            raise ValueError(f"Impulse type already exists: {impulse_type.id!r}")
        return await self._append(
            run_id=impulse_type.run_id,
            command=command,
            events=events,
            facts=[
                _fact(
                    "impulse_type",
                    impulse_type.model_dump(mode="json"),
                    key={"run_id": impulse_type.run_id, "id": impulse_type.id},
                )
            ],
        )

    async def get_impulse_type(
        self, *, run_id: str, impulse_type_id: str
    ) -> ImpulseType | None:
        return self._impulse_types.get((run_id, impulse_type_id))

    async def list_impulse_types(self, *, run_id: str) -> list[ImpulseType]:
        items = [t for (rid, _), t in self._impulse_types.items() if rid == run_id]
        return sorted(items, key=lambda t: (t.created_at, t.id))

    # ------------------------------------------------------------------
    # Impulses
    # ------------------------------------------------------------------

    async def put_impulse(self, impulse: Impulse) -> None:
        self._require_run(impulse.run_id)
        self._impulses[(impulse.run_id, impulse.id)] = impulse

    async def accept_impulse(
        self,
        impulse: Impulse,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != impulse.run_id:
            raise ValueError("impulse.accept command run_id must match impulse run_id")
        if command.command_type != "impulse.accept":
            raise ValueError("accept_impulse requires command_type 'impulse.accept'")
        existing = await self._journal.get_command_by_idempotency(
            run_id=command.run_id, idempotency_key=command.idempotency_key
        )
        if existing is not None:
            return CommandSubmission(command=existing, events=[], replayed=True)
        self._require_run(impulse.run_id)
        if (impulse.run_id, impulse.id) in self._impulses:
            raise ValueError(f"Impulse already exists: {impulse.id!r}")
        return await self._append(
            run_id=impulse.run_id,
            command=command,
            events=events,
            facts=[
                _fact(
                    "impulse",
                    impulse.model_dump(mode="json"),
                    key={"id": impulse.id},
                )
            ],
        )

    async def get_impulse(self, *, run_id: str, impulse_id: str) -> Impulse | None:
        return self._impulses.get((run_id, impulse_id))

    async def list_impulses(
        self,
        *,
        run_id: str,
        impulse_type: str | None = None,
        limit: int | None = None,
    ) -> list[Impulse]:
        items = [i for (rid, _), i in self._impulses.items() if rid == run_id]
        if impulse_type is not None:
            items = [i for i in items if i.impulse_type == impulse_type]
        items = sorted(items, key=lambda i: (i.created_at, i.id))
        if limit is not None:
            items = items[:limit]
        return items

    # ------------------------------------------------------------------
    # Impulse relations
    # ------------------------------------------------------------------

    async def put_impulse_relation(self, relation: ImpulseRelation) -> None:
        self._require_run(relation.run_id)
        self._impulse_relations[(relation.run_id, relation.id)] = relation

    async def record_impulse_relation(
        self,
        relation: ImpulseRelation,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != relation.run_id:
            raise ValueError(
                "impulse_relation.record command run_id must match relation run_id"
            )
        if command.command_type != "impulse_relation.record":
            raise ValueError(
                "record_impulse_relation requires command_type 'impulse_relation.record'"
            )
        existing = await self._journal.get_command_by_idempotency(
            run_id=command.run_id, idempotency_key=command.idempotency_key
        )
        if existing is not None:
            return CommandSubmission(command=existing, events=[], replayed=True)
        self._require_run(relation.run_id)
        if (relation.run_id, relation.id) in self._impulse_relations:
            raise ValueError(f"Impulse relation already exists: {relation.id!r}")
        return await self._append(
            run_id=relation.run_id,
            command=command,
            events=events,
            facts=[
                _fact(
                    "impulse_relation",
                    relation.model_dump(mode="json"),
                    key={"id": relation.id},
                )
            ],
        )

    async def get_impulse_relation(
        self, *, run_id: str, relation_id: str
    ) -> ImpulseRelation | None:
        return self._impulse_relations.get((run_id, relation_id))

    async def list_impulse_relations(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
        relation_type: str | None = None,
    ) -> list[ImpulseRelation]:
        items = [r for (rid, _), r in self._impulse_relations.items() if rid == run_id]
        if impulse_id is not None:
            items = [
                r
                for r in items
                if r.source_impulse_id == impulse_id or r.target_impulse_id == impulse_id
            ]
        if relation_type is not None:
            items = [r for r in items if r.relation_type == relation_type]
        return sorted(items, key=lambda r: (r.created_at, r.id))

    # ------------------------------------------------------------------
    # Commands / events
    # ------------------------------------------------------------------

    async def submit_command(
        self, command: RuntimeCommand, *, events: Sequence[RuntimeEvent] = ()
    ) -> CommandSubmission:
        if command.command_type == "run.create":
            raise ValueError("run.create commands must use create_run")
        existing = await self._journal.get_command_by_idempotency(
            run_id=command.run_id, idempotency_key=command.idempotency_key
        )
        if existing is not None:
            return CommandSubmission(command=existing, events=[], replayed=True)
        self._require_run(command.run_id)
        return await self._append(
            run_id=command.run_id,
            command=command,
            events=events,
            facts=[],
        )

    async def get_command_by_idempotency(
        self, *, run_id: str, idempotency_key: str
    ) -> RuntimeCommand | None:
        return await self._journal.get_command_by_idempotency(
            run_id=run_id, idempotency_key=idempotency_key
        )

    async def get_command(
        self, *, run_id: str, command_id: str
    ) -> RuntimeCommand | None:
        return self._journal.get_command(run_id=run_id, command_id=command_id)

    async def list_commands(
        self,
        *,
        run_id: str,
        command_type: str | None = None,
        actor: str | None = None,
        limit: int | None = None,
    ) -> list[RuntimeCommand]:
        return self._journal.list_commands(
            run_id=run_id,
            command_type=command_type,
            actor=actor,
            limit=limit,
        )

    async def list_events(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
        after_sequence: int | None = None,
        limit: int | None = None,
    ) -> list[RuntimeEvent]:
        return await self._journal.list_events(
            run_id=run_id,
            impulse_id=impulse_id,
            after_sequence=after_sequence,
            limit=limit,
        )

    # ------------------------------------------------------------------
    # Associations
    # ------------------------------------------------------------------

    async def put_association(self, association: Association) -> None:
        self._require_run(association.run_id)
        self._associations[(association.run_id, association.id)] = association

    async def record_association(
        self,
        association: Association,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != association.run_id:
            raise ValueError(
                "association.record command run_id must match association run_id"
            )
        if command.command_type != "association.record":
            raise ValueError(
                "record_association requires command_type 'association.record'"
            )
        existing = await self._journal.get_command_by_idempotency(
            run_id=command.run_id, idempotency_key=command.idempotency_key
        )
        if existing is not None:
            return CommandSubmission(command=existing, events=[], replayed=True)
        self._require_run(association.run_id)
        if (association.run_id, association.id) in self._associations:
            raise ValueError(f"Association already exists: {association.id!r}")
        return await self._append(
            run_id=association.run_id,
            command=command,
            events=events,
            facts=[
                _fact(
                    "association",
                    association.model_dump(mode="json"),
                    key={"id": association.id},
                )
            ],
        )

    async def list_associations(
        self, *, run_id: str, impulse_id: str | None = None
    ) -> list[Association]:
        items = [a for (rid, _), a in self._associations.items() if rid == run_id]
        if impulse_id is not None:
            items = [a for a in items if a.impulse_id == impulse_id]
        return sorted(items, key=lambda a: (a.created_at, a.id))

    # ------------------------------------------------------------------
    # Reactions
    # ------------------------------------------------------------------

    async def put_reaction(self, reaction: Reaction) -> None:
        self._require_run(reaction.run_id)
        key = (reaction.run_id, reaction.id)
        # Parity with Correlator ON CONFLICT DO NOTHING
        if key not in self._reactions:
            self._reactions[key] = reaction

    async def record_reaction(
        self,
        reaction: Reaction,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != reaction.run_id:
            raise ValueError(
                "reaction.record command run_id must match reaction run_id"
            )
        if command.command_type != "reaction.record":
            raise ValueError("record_reaction requires command_type 'reaction.record'")
        existing = await self._journal.get_command_by_idempotency(
            run_id=command.run_id, idempotency_key=command.idempotency_key
        )
        if existing is not None:
            return CommandSubmission(command=existing, events=[], replayed=True)
        self._require_run(reaction.run_id)
        if (reaction.run_id, reaction.id) in self._reactions:
            raise ValueError(f"Reaction already exists: {reaction.id!r}")
        return await self._append(
            run_id=reaction.run_id,
            command=command,
            events=events,
            facts=[
                _fact(
                    "reaction",
                    reaction.model_dump(mode="json"),
                    key={"id": reaction.id},
                )
            ],
        )

    async def get_reaction(self, *, run_id: str, reaction_id: str) -> Reaction | None:
        return self._reactions.get((run_id, reaction_id))

    async def list_reactions(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
        kind: str | None = None,
    ) -> list[Reaction]:
        items = [r for (rid, _), r in self._reactions.items() if rid == run_id]
        if impulse_id is not None:
            items = [r for r in items if r.impulse_id == impulse_id]
        if kind is not None:
            items = [r for r in items if r.kind == kind]
        return sorted(items, key=lambda r: (r.created_at, r.id))

    # ------------------------------------------------------------------
    # Processes
    # ------------------------------------------------------------------

    async def put_process(self, process: Process) -> None:
        self._require_run(process.run_id)
        self._journal.upsert_process(process)

    async def schedule_process(
        self,
        process: Process,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != process.run_id:
            raise ValueError(
                "process.schedule command run_id must match process run_id"
            )
        if command.command_type != "process.schedule":
            raise ValueError("schedule_process requires command_type 'process.schedule'")
        existing = await self._journal.get_command_by_idempotency(
            run_id=command.run_id, idempotency_key=command.idempotency_key
        )
        if existing is not None:
            return CommandSubmission(command=existing, events=[], replayed=True)
        if process.status not in {ProcessStatus.pending, ProcessStatus.ready}:
            raise ValueError(
                "schedule_process requires process status 'pending' or 'ready'"
            )
        self._require_run(process.run_id)
        existing_proc = self._journal.get_process(process.id)
        if existing_proc is not None and existing_proc.run_id == process.run_id:
            raise ValueError(f"Process already exists: {process.id!r}")
        cp_marker = process.metadata.get("correlation_path")
        if isinstance(cp_marker, dict):
            from fala.correlation_paths import _validate_correlation_path_marker

            _validate_correlation_path_marker(cp_marker, process_id=process.id)
        return await self._append(
            run_id=process.run_id,
            command=command,
            events=events,
            facts=[
                _fact(
                    "process",
                    process.model_dump(mode="json"),
                    key={"id": process.id},
                )
            ],
        )

    async def get_process(self, *, run_id: str, process_id: str) -> Process | None:
        process = self._journal.get_process(process_id)
        if process is None or process.run_id != run_id:
            return None
        return process

    async def list_processes(
        self,
        *,
        run_id: str,
        status: ProcessStatus | None = None,
        impulse_id: str | None = None,
    ) -> list[Process]:
        items = [p for p in self._journal._processes.values() if p.run_id == run_id]
        if status is not None:
            items = [p for p in items if p.status == status]
        if impulse_id is not None:
            items = [p for p in items if p.impulse_id == impulse_id]
        return sorted(items, key=lambda p: (p.created_at, p.id))

    async def claim_next_ready_process(
        self,
        *,
        worker_id: str,
        run_id: str | None = None,
        lease_seconds: float = 300.0,
        all_runs: bool = False,
    ) -> Process | None:
        result = await self._journal.claim_next(
            ClaimRequest(
                worker_id=worker_id,
                run_id=run_id,
                lease_seconds=lease_seconds,
                all_runs=all_runs,
            )
        )
        return result.process

    async def complete_process(
        self,
        *,
        run_id: str,
        process_id: str,
        output: dict[str, Any] | None = None,
    ) -> Process:
        return await self._finish_process(
            run_id=run_id,
            process_id=process_id,
            status=ProcessStatus.succeeded,
            output=output or {},
            error={},
        )

    async def fail_process(
        self,
        *,
        run_id: str,
        process_id: str,
        error: dict[str, Any] | None = None,
    ) -> Process:
        return await self._finish_process(
            run_id=run_id,
            process_id=process_id,
            status=ProcessStatus.failed,
            output={},
            error=error or {},
        )

    async def cancel_process(
        self,
        *,
        run_id: str,
        process_id: str,
        error: dict[str, Any] | None = None,
    ) -> Process:
        return await self._stop_process(
            run_id=run_id,
            process_id=process_id,
            status=ProcessStatus.cancelled,
            error=error or {},
        )

    async def timeout_process(
        self,
        *,
        run_id: str,
        process_id: str,
        error: dict[str, Any] | None = None,
    ) -> Process:
        return await self._stop_process(
            run_id=run_id,
            process_id=process_id,
            status=ProcessStatus.timed_out,
            error=error or {},
        )

    async def retry_process(
        self,
        *,
        run_id: str,
        process_id: str,
        available_at: datetime | None = None,
        error: dict[str, Any] | None = None,
    ) -> Process:
        process = await self.get_process(run_id=run_id, process_id=process_id)
        if process is None:
            raise ValueError(f"Unknown process: {process_id!r}")
        if process.status not in {ProcessStatus.running, ProcessStatus.failed}:
            raise ValueError(
                f"Process {process_id!r} cannot be retried from status: "
                f"{process.status.value}"
            )
        if process.attempt >= process.max_attempts:
            raise ValueError(f"Process {process_id!r} exhausted retry attempts")
        now = _now()
        updated = process.model_copy(
            update={
                "status": ProcessStatus.retry_wait,
                "available_at": available_at or now,
                "lease_owner": None,
                "lease_expires_at": None,
                "error": error or process.error,
                "updated_at": now,
                "finished_at": None,
            }
        )
        self._journal.upsert_process(updated)
        return updated

    async def _finish_process(
        self,
        *,
        run_id: str,
        process_id: str,
        status: ProcessStatus,
        output: dict[str, Any],
        error: dict[str, Any],
    ) -> Process:
        process = await self.get_process(run_id=run_id, process_id=process_id)
        if process is None:
            raise ValueError(f"Unknown process: {process_id!r}")
        if process.status not in {ProcessStatus.running, ProcessStatus.waiting}:
            raise ValueError(
                f"Process {process_id!r} is not running or waiting: "
                f"{process.status.value}"
            )
        now = _now()
        updated = process.model_copy(
            update={
                "status": status,
                "lease_owner": None,
                "lease_expires_at": None,
                "output": output,
                "error": error,
                "updated_at": now,
                "finished_at": now,
            }
        )
        self._journal.upsert_process(updated)
        return updated

    async def _stop_process(
        self,
        *,
        run_id: str,
        process_id: str,
        status: ProcessStatus,
        error: dict[str, Any],
    ) -> Process:
        process = await self.get_process(run_id=run_id, process_id=process_id)
        if process is None:
            raise ValueError(f"Unknown process: {process_id!r}")
        if process.status in TERMINAL_PROCESS_STATUSES:
            raise ValueError(
                f"Process {process_id!r} status is terminal: {process.status.value}"
            )
        now = _now()
        updated = process.model_copy(
            update={
                "status": status,
                "lease_owner": None,
                "lease_expires_at": None,
                "error": error,
                "updated_at": now,
                "finished_at": now,
            }
        )
        self._journal.upsert_process(updated)
        return updated

    async def transition_process(
        self,
        *,
        run_id: str,
        process_id: str,
        status: ProcessStatus,
        command: RuntimeCommand,
        events: Sequence[RuntimeEvent] = (),
        output: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        available_at: datetime | None = None,
        input: dict[str, Any] | None = None,
    ) -> tuple[Process, CommandSubmission]:
        validate_process_transition_command(
            run_id=run_id, status=status, command=command
        )
        existing = await self._journal.get_command_by_idempotency(
            run_id=command.run_id, idempotency_key=command.idempotency_key
        )
        if existing is not None:
            stored_process_id = str(existing.payload.get("process_id", process_id))
            stored = await self.get_process(
                run_id=run_id, process_id=stored_process_id
            )
            if stored is None:
                raise ValueError(
                    "Replayed process transition has no stored process: "
                    f"{stored_process_id!r}"
                )
            return stored, CommandSubmission(
                command=existing, events=[], replayed=True
            )

        process = await self.get_process(run_id=run_id, process_id=process_id)
        if process is None:
            raise ValueError(f"Unknown process: {process_id!r}")
        now = _now()
        extra_units: list[CommandUnit] = []

        if status in {ProcessStatus.succeeded, ProcessStatus.failed}:
            validate_process_can_finish(process, actor=command.actor)
            stored_output = output or {} if status == ProcessStatus.succeeded else {}
            stored_error = error or {} if status == ProcessStatus.failed else {}
            updated = process.model_copy(
                update={
                    "status": status,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "output": stored_output,
                    "error": stored_error,
                    "updated_at": now,
                    "finished_at": now,
                }
            )
            # Correlation-path auto-advance (same atomic batch).
            extra_units = self._correlation_side_units(
                process=updated,
                command=command,
                now=now,
            )
        elif status == ProcessStatus.retry_wait:
            validate_process_can_retry(process, actor=command.actor)
            updated = process.model_copy(
                update={
                    "status": ProcessStatus.retry_wait,
                    "available_at": available_at or now,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "error": error or process.error,
                    "updated_at": now,
                    "finished_at": None,
                }
            )
        elif status == ProcessStatus.waiting:
            validate_process_can_wait(process)
            updated = process.model_copy(
                update={
                    "status": ProcessStatus.waiting,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "output": output if output is not None else process.output,
                    "updated_at": now,
                }
            )
        elif status == ProcessStatus.ready:
            validate_process_can_ready(process)
            ready_input = input if input is not None else process.input
            reg = (ready_input or {}).get("regulation") or {}
            ma_override = reg.get("max_attempts") if isinstance(reg, dict) else None
            updates: dict[str, Any] = {
                "status": ProcessStatus.ready,
                "input": ready_input,
                "updated_at": now,
            }
            if ma_override is not None:
                updates["max_attempts"] = int(ma_override)
            updated = process.model_copy(update=updates)
        else:
            if process.status in TERMINAL_PROCESS_STATUSES:
                raise ValueError(
                    f"Process {process_id!r} status is terminal: {process.status.value}"
                )
            updated = process.model_copy(
                update={
                    "status": status,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "error": error or {},
                    "updated_at": now,
                    "finished_at": now,
                }
            )

        facts = [
            _fact("process", updated.model_dump(mode="json"), key={"id": process_id})
        ]
        # Side-effect process facts live on extra units.
        submission = await self._append(
            run_id=run_id,
            command=command,
            events=events,
            facts=facts,
            extra_units=extra_units or None,
        )
        final = await self.get_process(run_id=run_id, process_id=process_id)
        assert final is not None
        return final, submission

    def _correlation_side_units(
        self,
        *,
        process: Process,
        command: RuntimeCommand,
        now: datetime,
    ) -> list[CommandUnit]:
        """Build auto-ready / dead-cancel units for correlation-path processes."""
        marker = process.metadata.get("correlation_path") or {}
        cp_id = marker.get("correlation_path_id")
        auto_advance = marker.get("auto_advance", True)
        if not (cp_id and isinstance(cp_id, str) and auto_advance):
            return []

        all_procs = [
            p
            for p in self._journal._processes.values()
            if p.run_id == process.run_id
        ]
        # Include the just-completed process snapshot for readiness calc.
        by_id = {p.id: p for p in all_procs}
        by_id[process.id] = process
        all_procs = list(by_id.values())

        units: list[CommandUnit] = []
        try:
            from fala.correlation_paths import compute_correlation_path_readies

            readies = compute_correlation_path_readies(all_procs, cp_id)
        except Exception:
            readies = []

        for dep_id, new_input in readies:
            ready_idem = f"process.ready:{dep_id}"
            # Skip if already present (best-effort; append_batch also guards).
            if (process.run_id, ready_idem) in self._journal._commands:
                continue
            dep = by_id.get(dep_id)
            if dep is None:
                continue
            reg = (new_input or {}).get("regulation") or {}
            ma_override = reg.get("max_attempts") if isinstance(reg, dict) else None
            updates: dict[str, Any] = {
                "status": ProcessStatus.ready,
                "input": new_input,
                "updated_at": now,
            }
            if ma_override is not None:
                updates["max_attempts"] = int(ma_override)
            ready_proc = dep.model_copy(update=updates)
            by_id[dep_id] = ready_proc
            ready_cmd = RuntimeCommand(
                run_id=process.run_id,
                command_type="process.ready",
                idempotency_key=ready_idem,
                actor=command.actor,
                correlation_id=command.correlation_id,
                causation_id=command.id,
                payload={"process_id": dep_id},
            )
            ready_evt = RuntimeEvent(
                run_id=process.run_id,
                impulse_id=process.impulse_id,
                process_id=dep_id,
                event_type="process.readied",
                payload={"process_id": dep_id},
            )
            units.append(
                CommandUnit(
                    command=ready_cmd,
                    events=[ready_evt],
                    facts=[
                        _fact(
                            "process",
                            ready_proc.model_dump(mode="json"),
                            key={"id": dep_id},
                        )
                    ],
                )
            )

        try:
            from fala.correlation_paths import (
                compute_correlation_path_dead_cancellations,
            )

            cancels = compute_correlation_path_dead_cancellations(
                list(by_id.values()), cp_id
            )
        except Exception:
            cancels = []

        for dep_id, err in cancels:
            cancel_idem = f"process.cancel:{dep_id}:dead"
            if (process.run_id, cancel_idem) in self._journal._commands:
                continue
            dep = by_id.get(dep_id)
            if dep is None:
                continue
            cancelled = dep.model_copy(
                update={
                    "status": ProcessStatus.cancelled,
                    "lease_owner": None,
                    "lease_expires_at": None,
                    "error": err,
                    "updated_at": now,
                    "finished_at": now,
                }
            )
            cancel_cmd = RuntimeCommand(
                run_id=process.run_id,
                command_type="process.cancel",
                idempotency_key=cancel_idem,
                actor=command.actor,
                correlation_id=command.correlation_id,
                causation_id=command.id,
                payload={"process_id": dep_id},
            )
            cancel_evt = RuntimeEvent(
                run_id=process.run_id,
                impulse_id=process.impulse_id,
                process_id=dep_id,
                event_type="process.cancelled",
                payload={"process_id": dep_id, "reason": "dead_upstream"},
            )
            units.append(
                CommandUnit(
                    command=cancel_cmd,
                    events=[cancel_evt],
                    facts=[
                        _fact(
                            "process",
                            cancelled.model_dump(mode="json"),
                            key={"id": dep_id},
                        )
                    ],
                )
            )
        return units

    # ------------------------------------------------------------------
    # Homeostats
    # ------------------------------------------------------------------

    async def put_homeostat(self, homeostat: Homeostat) -> None:
        self._require_run(homeostat.run_id)
        self._homeostats[(homeostat.run_id, homeostat.id)] = homeostat

    async def save_homeostat(
        self,
        homeostat: Homeostat,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != homeostat.run_id:
            raise ValueError("homeostat command run_id must match homeostat run_id")
        if command.command_type not in {"homeostat.save", "homeostat.open"}:
            raise ValueError(
                "save_homeostat requires command_type 'homeostat.save' or 'homeostat.open'"
            )
        existing = await self._journal.get_command_by_idempotency(
            run_id=command.run_id, idempotency_key=command.idempotency_key
        )
        if existing is not None:
            return CommandSubmission(command=existing, events=[], replayed=True)
        self._require_run(homeostat.run_id)
        if (homeostat.run_id, homeostat.id) in self._homeostats:
            raise ValueError(f"Homeostat already exists: {homeostat.id!r}")
        return await self._append(
            run_id=homeostat.run_id,
            command=command,
            events=events,
            facts=[
                _fact(
                    "homeostat",
                    homeostat.model_dump(mode="json"),
                    key={"id": homeostat.id},
                )
            ],
        )

    async def get_homeostat(
        self, *, run_id: str, homeostat_id: str
    ) -> Homeostat | None:
        return self._homeostats.get((run_id, homeostat_id))

    async def transition_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        status: HomeostatStatus,
        command: RuntimeCommand,
        events: Sequence[RuntimeEvent] = (),
        values: dict[str, Any] | None = None,
    ) -> tuple[Homeostat, CommandSubmission]:
        expected = _HOMEOSTAT_TRANSITION_COMMANDS.get(status)
        if expected is None:
            raise ValueError(f"Unsupported homeostat terminal status: {status.value}")
        if command.run_id != run_id:
            raise ValueError("homeostat transition command run_id must match run_id")
        if command.command_type != expected:
            raise ValueError(
                f"transition to {status.value!r} requires command_type {expected!r}"
            )
        existing = await self._journal.get_command_by_idempotency(
            run_id=command.run_id, idempotency_key=command.idempotency_key
        )
        if existing is not None:
            stored_id = str(existing.payload.get("homeostat_id", homeostat_id))
            stored = self._homeostats.get((run_id, stored_id))
            if stored is None:
                raise ValueError(
                    "Replayed homeostat transition has no stored homeostat: "
                    f"{stored_id!r}"
                )
            return stored, CommandSubmission(
                command=existing, events=[], replayed=True
            )
        homeostat = self._homeostats.get((run_id, homeostat_id))
        if homeostat is None:
            raise ValueError(f"Unknown homeostat: {homeostat_id!r}")
        if homeostat.status != HomeostatStatus.open:
            raise ValueError(
                f"Homeostat {homeostat_id!r} is not open: {homeostat.status.value}"
            )
        now = _now()
        updated = homeostat.model_copy(
            update={
                "status": status,
                "values": values if values is not None else {},
                "updated_at": now,
            }
        )
        submission = await self._append(
            run_id=run_id,
            command=command,
            events=events,
            facts=[
                _fact(
                    "homeostat",
                    updated.model_dump(mode="json"),
                    key={"id": homeostat_id},
                )
            ],
        )
        return self._homeostats[(run_id, homeostat_id)], submission

    async def complete_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        values: dict[str, Any] | None = None,
    ) -> Homeostat:
        return await self._finish_homeostat(
            run_id=run_id,
            homeostat_id=homeostat_id,
            status=HomeostatStatus.completed,
            values=values,
        )

    async def cancel_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        values: dict[str, Any] | None = None,
    ) -> Homeostat:
        return await self._finish_homeostat(
            run_id=run_id,
            homeostat_id=homeostat_id,
            status=HomeostatStatus.cancelled,
            values=values,
        )

    async def expire_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        values: dict[str, Any] | None = None,
    ) -> Homeostat:
        return await self._finish_homeostat(
            run_id=run_id,
            homeostat_id=homeostat_id,
            status=HomeostatStatus.expired,
            values=values,
        )

    async def _finish_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        status: HomeostatStatus,
        values: dict[str, Any] | None,
    ) -> Homeostat:
        homeostat = self._homeostats.get((run_id, homeostat_id))
        if homeostat is None:
            raise ValueError(f"Unknown homeostat: {homeostat_id!r}")
        now = _now()
        updated = homeostat.model_copy(
            update={
                "status": status,
                "values": values if values is not None else homeostat.values,
                "updated_at": now,
            }
        )
        self._homeostats[(run_id, homeostat_id)] = updated
        return updated

    async def list_homeostats(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
        status: HomeostatStatus | None = None,
    ) -> list[Homeostat]:
        items = [h for (rid, _), h in self._homeostats.items() if rid == run_id]
        if impulse_id is not None:
            items = [h for h in items if h.impulse_id == impulse_id]
        if status is not None:
            items = [h for h in items if h.status == status]
        return sorted(items, key=lambda h: (h.created_at, h.id))

    # ------------------------------------------------------------------
    # Projections
    # ------------------------------------------------------------------

    async def put_projection(self, projection: Projection) -> None:
        self._require_run(projection.run_id)
        self._projections[(projection.run_id, projection.name)] = projection

    async def save_projection(
        self,
        projection: Projection,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != projection.run_id:
            raise ValueError(
                "projection.save command run_id must match projection run_id"
            )
        if command.command_type != "projection.save":
            raise ValueError("save_projection requires command_type 'projection.save'")
        existing = await self._journal.get_command_by_idempotency(
            run_id=command.run_id, idempotency_key=command.idempotency_key
        )
        if existing is not None:
            return CommandSubmission(command=existing, events=[], replayed=True)
        self._require_run(projection.run_id)
        return await self._append(
            run_id=projection.run_id,
            command=command,
            events=events,
            facts=[
                _fact(
                    "projection",
                    projection.model_dump(mode="json"),
                    key={"run_id": projection.run_id, "name": projection.name},
                )
            ],
        )

    async def get_projection(self, *, run_id: str, name: str) -> Projection | None:
        return self._projections.get((run_id, name))

    async def list_projections(self, *, run_id: str) -> list[Projection]:
        items = [p for (rid, _), p in self._projections.items() if rid == run_id]
        return sorted(items, key=lambda p: p.name)

    def _build_run_summary(self, run_id: str) -> Projection:
        events = list(self._journal._events.get(run_id, []))
        event_type_counts = dict(
            sorted(Counter(e.event_type for e in events).items())
        )
        source_event_sequence = max((e.sequence or 0 for e in events), default=0)
        impulses = [i for (rid, _), i in self._impulses.items() if rid == run_id]
        reactions = [r for (rid, _), r in self._reactions.items() if rid == run_id]
        homeostats = [h for (rid, _), h in self._homeostats.items() if rid == run_id]
        associations = [
            a for (rid, _), a in self._associations.items() if rid == run_id
        ]
        processes = [
            p for p in self._journal._processes.values() if p.run_id == run_id
        ]
        outbox = [d for (rid, _), d in self._outbox.items() if rid == run_id]
        inbox = [d for (rid, _), d in self._inbox.items() if rid == run_id]
        commands = self._journal.list_commands(run_id=run_id)

        impulse_type_counts = dict(
            sorted(Counter(i.impulse_type for i in impulses).items())
        )
        homeostat_status_counts = dict(
            sorted(Counter(h.status.value for h in homeostats).items())
        )
        process_status_counts = dict(
            sorted(Counter(p.status.value for p in processes).items())
        )

        spawned_run_ids = {
            d.target.run_id for d in outbox if d.target and d.target.run_id
        }
        resource_accounting = {
            "reaction_bytes": sum(r.size_bytes or 0 for r in reactions),
            "bridge_command_count": sum(
                1 for c in commands if c.command_type.startswith("bridge.")
            ),
            "bridge_delivery_count": len(outbox) + len(inbox),
            "process_attempts": sum(p.attempt for p in processes),
            "process_input_bytes": sum(
                len(json.dumps(p.input, sort_keys=True, separators=(",", ":")))
                for p in processes
            ),
            "process_output_bytes": sum(
                len(json.dumps(p.output, sort_keys=True, separators=(",", ":")))
                for p in processes
            ),
            "spawned_run_count": len(spawned_run_ids),
            "subprocess_count": sum(
                1 for p in processes if p.process_type == "subprocess"
            ),
        }
        data = {
            "reaction_count": len(reactions),
            "impulse_count": len(impulses),
            "impulse_type_counts": impulse_type_counts,
            "event_count": sum(event_type_counts.values()),
            "event_type_counts": event_type_counts,
            "homeostat_count": len(homeostats),
            "homeostat_status_counts": homeostat_status_counts,
            "association_count": len(associations),
            "process_count": len(processes),
            "process_status_counts": process_status_counts,
            "resource_accounting": resource_accounting,
            "run_id": run_id,
            "source_event_sequence": source_event_sequence,
        }
        return Projection(
            id="projection_run_summary",
            run_id=run_id,
            name="run_summary",
            version=1,
            data=data,
            source_event_sequence=source_event_sequence,
        )

    async def rebuild_projections(
        self,
        *,
        run_id: str,
        names: Sequence[str] | None = None,
    ) -> list[Projection]:
        requested = (
            list(dict.fromkeys(names))
            if names is not None
            else list(_BUILT_IN_PROJECTIONS)
        )
        unsupported = sorted(set(requested) - set(_BUILT_IN_PROJECTIONS))
        if unsupported:
            raise ValueError(f"Unknown projection rebuild name: {unsupported[0]}")
        self._require_run(run_id)
        rebuilt: list[Projection] = []
        for _name in requested:
            projection = self._build_run_summary(run_id)
            self._projections[(run_id, projection.name)] = projection
            rebuilt.append(projection)
        return rebuilt

    async def rebuild_projections_with_command(
        self,
        *,
        run_id: str,
        names: Sequence[str] | None,
        command: RuntimeCommand,
        events: Sequence[RuntimeEvent] = (),
    ) -> tuple[list[Projection], CommandSubmission]:
        if command.run_id != run_id:
            raise ValueError("projection.rebuild command run_id must match run_id")
        if command.command_type != "projection.rebuild":
            raise ValueError(
                "rebuild_projections_with_command requires command_type "
                "'projection.rebuild'"
            )
        requested = (
            list(dict.fromkeys(names))
            if names is not None
            else list(_BUILT_IN_PROJECTIONS)
        )
        unsupported = sorted(set(requested) - set(_BUILT_IN_PROJECTIONS))
        if unsupported:
            raise ValueError(f"Unknown projection rebuild name: {unsupported[0]}")

        existing = await self._journal.get_command_by_idempotency(
            run_id=command.run_id, idempotency_key=command.idempotency_key
        )
        if existing is not None:
            projections: list[Projection] = []
            for name in requested:
                row = self._projections.get((run_id, name))
                if row is None:
                    raise ValueError(
                        "Replayed projection rebuild has no stored projection: "
                        f"{name!r}"
                    )
                projections.append(row)
            return projections, CommandSubmission(
                command=existing, events=[], replayed=True
            )

        self._require_run(run_id)
        rebuilt: list[Projection] = []
        facts: list[StateFact] = []
        for _name in requested:
            projection = self._build_run_summary(run_id)
            rebuilt.append(projection)
            facts.append(
                _fact(
                    "projection",
                    projection.model_dump(mode="json"),
                    key={"run_id": run_id, "name": projection.name},
                )
            )
        submission = await self._append(
            run_id=run_id,
            command=command,
            events=events,
            facts=facts,
        )
        return rebuilt, submission

    # ------------------------------------------------------------------
    # Bridge
    # ------------------------------------------------------------------

    async def put_outbox_delivery(self, delivery: BridgeDelivery) -> None:
        self._require_run(delivery.run_id)
        self._outbox[(delivery.run_id, delivery.id)] = delivery

    async def enqueue_outbox_delivery(
        self,
        delivery: BridgeDelivery,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.command_type != "bridge.outbox.enqueue":
            raise ValueError(
                "enqueue_outbox_delivery requires command_type 'bridge.outbox.enqueue'"
            )
        return await self._submit_bridge(
            table="outbox",
            delivery=delivery,
            command=command,
            events=events,
        )

    async def deliver_outbox_delivery(
        self,
        delivery: BridgeDelivery,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.command_type != "bridge.outbox.deliver":
            raise ValueError(
                "deliver_outbox_delivery requires command_type 'bridge.outbox.deliver'"
            )
        return await self._submit_bridge(
            table="outbox",
            delivery=delivery,
            command=command,
            events=events,
        )

    async def get_outbox_delivery(
        self, *, run_id: str, delivery_id: str
    ) -> BridgeDelivery | None:
        return self._outbox.get((run_id, delivery_id))

    async def list_outbox_deliveries(
        self,
        *,
        run_id: str,
        status: BridgeDeliveryStatus | None = None,
    ) -> list[BridgeDelivery]:
        items = [d for (rid, _), d in self._outbox.items() if rid == run_id]
        if status is not None:
            items = [d for d in items if d.status == status]
        return sorted(items, key=lambda d: (d.updated_at, d.id))

    async def put_inbox_delivery(self, delivery: BridgeDelivery) -> None:
        self._require_run(delivery.run_id)
        self._inbox[(delivery.run_id, delivery.id)] = delivery

    async def import_inbox_delivery(
        self,
        delivery: BridgeDelivery,
        impulse: Impulse,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.command_type != "bridge.inbox.import":
            raise ValueError(
                "import_inbox_delivery requires command_type 'bridge.inbox.import'"
            )
        return await self._submit_bridge(
            table="inbox",
            delivery=delivery,
            command=command,
            events=events,
            impulse=impulse,
        )

    async def get_inbox_delivery(
        self, *, run_id: str, delivery_id: str
    ) -> BridgeDelivery | None:
        return self._inbox.get((run_id, delivery_id))

    async def list_inbox_deliveries(
        self,
        *,
        run_id: str,
        status: BridgeDeliveryStatus | None = None,
    ) -> list[BridgeDelivery]:
        items = [d for (rid, _), d in self._inbox.items() if rid == run_id]
        if status is not None:
            items = [d for d in items if d.status == status]
        return sorted(items, key=lambda d: (d.updated_at, d.id))

    async def _submit_bridge(
        self,
        *,
        table: str,
        delivery: BridgeDelivery,
        command: RuntimeCommand,
        events: Sequence[RuntimeEvent],
        impulse: Impulse | None = None,
    ) -> CommandSubmission:
        if command.run_id != delivery.run_id:
            raise ValueError("bridge command run_id must match delivery run_id")
        existing = await self._journal.get_command_by_idempotency(
            run_id=command.run_id, idempotency_key=command.idempotency_key
        )
        if existing is not None:
            return CommandSubmission(command=existing, events=[], replayed=True)
        self._require_run(delivery.run_id)

        if (
            command.command_type == "bridge.outbox.enqueue"
            and delivery.budget.spawned_runs is not None
        ):
            spawned = {
                d.target.run_id
                for (rid, _), d in self._outbox.items()
                if rid == delivery.run_id and d.target and d.target.run_id
            }
            if (
                delivery.target.run_id not in spawned
                and len(spawned) >= delivery.budget.spawned_runs
            ):
                from fala.errors import FalaBudgetExceeded

                raise FalaBudgetExceeded(
                    "Bridge delivery exceeded spawned_runs budget",
                    details={
                        "run_id": delivery.run_id,
                        "delivery_id": delivery.id,
                        "spawned_runs": delivery.budget.spawned_runs,
                        "spawned_run_ids": sorted(spawned),
                    },
                )

        facts: list[StateFact] = []
        if impulse is not None:
            facts.append(
                _fact(
                    "impulse",
                    impulse.model_dump(mode="json"),
                    key={"id": impulse.id},
                )
            )
        entity = "bridge_outbox" if table == "outbox" else "bridge_inbox"
        facts.append(
            _fact(
                entity,
                delivery.model_dump(mode="json"),
                key={"id": delivery.id},
            )
        )
        return await self._append(
            run_id=delivery.run_id,
            command=command,
            events=events,
            facts=facts,
        )


class JournalBackedBackend:
    """Implements the full RuntimeBackend Protocol over a Journal sink.

    - SqliteJournal: delegates all RuntimeBackend methods to ``journal.correlator``.
    - InMemoryJournal: uses :class:`InMemoryRuntimeBackend` maps + journal batches.
    """

    def __init__(self, journal: InMemoryJournal | SqliteJournal) -> None:
        self.journal = journal
        if isinstance(journal, SqliteJournal):
            self._backend: RuntimeBackend = journal.correlator
        elif isinstance(journal, InMemoryJournal):
            self._backend = InMemoryRuntimeBackend(journal)
        else:
            raise TypeError(
                f"Unsupported journal type for JournalBackedBackend: {type(journal)!r}"
            )

    @property
    def runtime_uri(self) -> str:
        return self.journal.runtime_uri

    def __getattr__(self, name: str) -> Any:
        return getattr(self._backend, name)


__all__ = [
    "InMemoryRuntimeBackend",
    "JournalBackedBackend",
]
