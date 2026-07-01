from __future__ import annotations

from pathlib import Path

from fala.runtime_backend import (
    Carrier,
    CommandSubmission,
    Gate,
    GateStatus,
    Observation,
    Projection,
    RuntimeBackend,
    RuntimeBackendService,
    RuntimeEvent,
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


__all__ = ["FalaRuntime"]
