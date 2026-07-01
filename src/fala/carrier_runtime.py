from __future__ import annotations

from datetime import datetime
from pathlib import Path

from fala.runtime_backend import (
    Artifact,
    CarrierWaitGraphDiagnostic,
    CarrierProcessStatus,
    CarrierRunStatus,
    Carrier,
    CarrierRelation,
    CarrierType,
    CommandSubmission,
    Gate,
    GateStatus,
    Observation,
    Process,
    Projection,
    Run,
    RuntimeBackend,
    RuntimeBackendService,
    RuntimeEvent,
    DelegationPolicy,
    RuntimePool,
)


class FalaRuntime:
    """Carrier-first embedded runtime facade.

    This module is intentionally independent from HTTP, CLI, and web UI modules.
    """

    def __init__(self, backend: RuntimeBackend) -> None:
        self.service = RuntimeBackendService(backend)
        self.backend = backend

    @classmethod
    def sqlite(cls, path: str | Path) -> "FalaRuntime":
        service = RuntimeBackendService.sqlite(path)
        runtime = cls.__new__(cls)
        runtime.service = service
        runtime.backend = service.backend
        return runtime

    async def create_run(
        self,
        run: Run,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Run, CommandSubmission]:
        return await self.service.create_run(
            run,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def set_run_status(
        self,
        *,
        run_id: str,
        status: CarrierRunStatus,
        idempotency_key: str,
        reason: str | None = None,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Run, CommandSubmission]:
        return await self.service.set_run_status(
            run_id=run_id,
            status=status,
            idempotency_key=idempotency_key,
            reason=reason,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def accept_carrier(
        self,
        carrier: Carrier,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Carrier, CommandSubmission]:
        return await self.service.accept_carrier(
            carrier,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def register_carrier_type(
        self,
        carrier_type: CarrierType,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[CarrierType, CommandSubmission]:
        return await self.service.register_carrier_type(
            carrier_type,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def record_carrier_relation(
        self,
        relation: CarrierRelation,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[CarrierRelation, CommandSubmission]:
        return await self.service.record_carrier_relation(
            relation,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def record_observation(
        self,
        observation: Observation,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Observation, CommandSubmission]:
        return await self.service.record_observation(
            observation,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def record_artifact(
        self,
        artifact: Artifact,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Artifact, CommandSubmission]:
        return await self.service.record_artifact(
            artifact,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def schedule_process(
        self,
        process: Process,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Process, CommandSubmission]:
        return await self.service.schedule_process(
            process,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def claim_next_ready_process(
        self,
        *,
        worker_id: str,
        run_id: str | None = None,
        lease_seconds: float = 300.0,
    ) -> Process | None:
        return await self.service.claim_next_ready_process(
            worker_id=worker_id,
            run_id=run_id,
            lease_seconds=lease_seconds,
        )

    async def complete_process(
        self,
        *,
        run_id: str,
        process_id: str,
        output: dict | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Process, CommandSubmission]:
        return await self.service.complete_process(
            run_id=run_id,
            process_id=process_id,
            output=output,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def fail_process(
        self,
        *,
        run_id: str,
        process_id: str,
        error: dict | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Process, CommandSubmission]:
        return await self.service.fail_process(
            run_id=run_id,
            process_id=process_id,
            error=error,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def retry_process(
        self,
        *,
        run_id: str,
        process_id: str,
        idempotency_key: str,
        available_at: datetime | None = None,
        error: dict | None = None,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Process, CommandSubmission]:
        return await self.service.retry_process(
            run_id=run_id,
            process_id=process_id,
            available_at=available_at,
            error=error,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def save_gate(
        self,
        gate: Gate,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Gate, CommandSubmission]:
        return await self.service.save_gate(
            gate,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def complete_gate(
        self,
        *,
        run_id: str,
        gate_id: str,
        values: dict | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Gate, CommandSubmission]:
        return await self.service.complete_gate(
            run_id=run_id,
            gate_id=gate_id,
            values=values,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def save_projection(
        self,
        projection: Projection,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Projection, CommandSubmission]:
        return await self.service.save_projection(
            projection,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def rebuild_projections(
        self,
        *,
        run_id: str,
        names: list[str] | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[list[Projection], CommandSubmission]:
        return await self.service.rebuild_projections(
            run_id=run_id,
            names=names,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def list_events(
        self,
        *,
        run_id: str,
        carrier_id: str | None = None,
        after_sequence: int | None = None,
        limit: int | None = None,
    ) -> list[RuntimeEvent]:
        return await self.backend.list_events(
            run_id=run_id,
            carrier_id=carrier_id,
            after_sequence=after_sequence,
            limit=limit,
        )

    async def list_runs(
        self,
        *,
        status: CarrierRunStatus | None = None,
        limit: int | None = None,
    ) -> list[Run]:
        return await self.service.list_runs(status=status, limit=limit)

    async def save_runtime_pool(self, pool: RuntimePool) -> RuntimePool:
        return await self.service.save_runtime_pool(pool)

    async def get_runtime_pool(self, *, pool_id: str) -> RuntimePool | None:
        return await self.service.get_runtime_pool(pool_id=pool_id)

    async def list_runtime_pools(self) -> list[RuntimePool]:
        return await self.service.list_runtime_pools()

    async def save_delegation_policy(
        self,
        policy: DelegationPolicy,
    ) -> DelegationPolicy:
        return await self.service.save_delegation_policy(policy)

    async def get_delegation_policy(
        self,
        *,
        policy_id: str,
    ) -> DelegationPolicy | None:
        return await self.service.get_delegation_policy(policy_id=policy_id)

    async def list_delegation_policies(
        self,
        *,
        pool_id: str | None = None,
    ) -> list[DelegationPolicy]:
        return await self.service.list_delegation_policies(pool_id=pool_id)

    async def list_carrier_types(self, *, run_id: str) -> list[CarrierType]:
        return await self.service.list_carrier_types(run_id=run_id)

    async def list_carrier_relations(
        self,
        *,
        run_id: str,
        carrier_id: str | None = None,
        relation_type: str | None = None,
    ) -> list[CarrierRelation]:
        return await self.service.list_carrier_relations(
            run_id=run_id,
            carrier_id=carrier_id,
            relation_type=relation_type,
        )

    async def list_artifacts(
        self,
        *,
        run_id: str,
        carrier_id: str | None = None,
        kind: str | None = None,
    ) -> list[Artifact]:
        return await self.service.list_artifacts(
            run_id=run_id,
            carrier_id=carrier_id,
            kind=kind,
        )

    async def list_processes(
        self,
        *,
        run_id: str,
        status: CarrierProcessStatus | None = None,
        carrier_id: str | None = None,
    ) -> list[Process]:
        return await self.service.list_processes(
            run_id=run_id,
            status=status,
            carrier_id=carrier_id,
        )

    async def list_gates(
        self,
        *,
        run_id: str,
        carrier_id: str | None = None,
        status: GateStatus | None = None,
    ) -> list[Gate]:
        return await self.service.list_gates(
            run_id=run_id,
            carrier_id=carrier_id,
            status=status,
        )

    async def diagnose_waits(
        self,
        *,
        run_id: str,
        carrier_id: str | None = None,
    ) -> CarrierWaitGraphDiagnostic:
        return await self.service.diagnose_waits(
            run_id=run_id,
            carrier_id=carrier_id,
        )


__all__ = ["FalaRuntime"]
