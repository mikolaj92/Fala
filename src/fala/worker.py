from __future__ import annotations

import asyncio
from enum import Enum
from typing import Any

from pydantic import BaseModel, ConfigDict

from fala.adapters import AdapterRegistry
from fala.client import ProcessRuntimeClient
from fala.models import (
    AdapterKind,
    AdapterSpec,
    ProcessAction,
    ProcessEvent,
    ProcessOutput,
    ProcessSpec,
    ProcessStatus,
    ResourceSpec,
    RuntimeWorkerStatus,
)
from fala.scheduler import ClaimedProcess


class ProcessWorkerOutcome(str, Enum):
    idle = "idle"
    completed = "completed"
    retry_scheduled = "retry_scheduled"
    terminal_failed = "terminal_failed"


class ProcessWorkerResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claimed: ClaimedProcess | None
    outcome: ProcessWorkerOutcome = ProcessWorkerOutcome.idle
    completed: bool = False
    error: str | None = None
    error_kind: str | None = None


class AdapterProcessRuntimeWorker:
    """Runs claimed process adapters through the process-runtime API."""

    def __init__(
        self,
        *,
        client: ProcessRuntimeClient,
        pipeline_id: str,
        worker_id: str,
        adapter_kind: AdapterKind,
        capabilities: list[str] | None = None,
        resources: ResourceSpec | dict[str, Any] | None = None,
        adapters: AdapterRegistry | None = None,
        lease_seconds: float = 300.0,
        renew_interval_seconds: float | None = None,
        error_kind: str | None = "worker_error",
    ) -> None:
        self.client = client
        self.pipeline_id = pipeline_id
        self.worker_id = worker_id
        self.adapter_kind = adapter_kind
        self.capabilities = list(capabilities or [])
        self.resources = ResourceSpec.model_validate(resources or {})
        self.adapters = adapters or AdapterRegistry.default()
        self.lease_seconds = lease_seconds
        self.error_kind = error_kind
        self.renew_interval_seconds = (
            renew_interval_seconds
            if renew_interval_seconds is not None
            else max(1.0, min(60.0, lease_seconds / 2))
        )

    async def run_once(
        self,
        *,
        run_id: str,
        process_id: str | None = None,
    ) -> ProcessWorkerResult:
        await self._heartbeat(
            run_id=run_id,
            process_id=process_id,
            status=RuntimeWorkerStatus.idle,
        )
        claim = await self.client.claim_next(
            run_id=run_id,
            pipeline_id=self.pipeline_id,
            worker_id=self.worker_id,
            process_id=process_id,
            adapter_kind=self.adapter_kind,
            capabilities=self.capabilities,
            resources=self.resources,
            lease_seconds=self.lease_seconds,
        )
        if claim is None:
            return ProcessWorkerResult(claimed=None, outcome=ProcessWorkerOutcome.idle)

        await self._heartbeat(
            run_id=run_id,
            process_id=process_id,
            status=RuntimeWorkerStatus.working,
            current_document_id=claim.document_id,
            current_process_id=claim.process.id,
        )
        step = _process_spec_from_claim(claim)
        renew_task = self._start_renew_loop(claim)
        try:
            await self.client.append_event(
                run_id=claim.run_id,
                document_id=claim.document_id,
                event=ProcessEvent(
                    run_id=claim.run_id,
                    document_id=claim.document_id,
                    process_id=claim.process.id,
                    type="process.started",
                    status=ProcessStatus.running,
                    data={"worker_id": self.worker_id, "attempt": claim.attempt},
                ),
            )
            output = await self.adapters.run(
                step,
                claim.context,
                event_sink=lambda event: self.client.append_event(
                    run_id=claim.run_id,
                    document_id=claim.document_id,
                    event=event,
                ),
            )
        except BaseException as exc:
            error_kind = self.error_kind
            await self._stop_renew_loop(renew_task)
            await self._heartbeat(
                run_id=run_id,
                process_id=process_id,
                status=RuntimeWorkerStatus.error,
                current_document_id=claim.document_id,
                current_process_id=claim.process.id,
                metadata={"error": str(exc), "error_kind": error_kind},
            )
            status_response = await self.client.write_status(
                run_id=claim.run_id,
                document_id=claim.document_id,
                process_id=claim.process.id,
                status=ProcessStatus.failed,
                worker_id=self.worker_id,
                data={
                    "worker_id": self.worker_id,
                    "attempt": claim.attempt,
                    "error": str(exc),
                    "error_kind": error_kind,
                },
            )
            return ProcessWorkerResult(
                claimed=claim,
                outcome=_failure_outcome_from_response(status_response),
                error=str(exc),
                error_kind=error_kind,
            )
        await self._stop_renew_loop(renew_task)

        await self.client.write_output(
            run_id=claim.run_id,
            document_id=claim.document_id,
            process_id=claim.process.id,
            output=output,
            pipeline_id=self.pipeline_id,
            worker_id=self.worker_id,
        )
        await self._heartbeat(
            run_id=run_id,
            process_id=process_id,
            status=RuntimeWorkerStatus.idle,
        )
        return ProcessWorkerResult(
            claimed=claim,
            outcome=ProcessWorkerOutcome.completed,
            completed=True,
        )

    async def run_until_idle(
        self,
        *,
        run_id: str,
        process_id: str | None = None,
        max_steps: int = 1000,
    ) -> list[ProcessWorkerResult]:
        if max_steps < 1:
            raise ValueError("max_steps must be greater than zero")
        results: list[ProcessWorkerResult] = []
        for _ in range(max_steps):
            result = await self.run_once(run_id=run_id, process_id=process_id)
            if result.claimed is None:
                break
            results.append(result)
            if not result.completed:
                break
        return results

    def _start_renew_loop(self, claim: ClaimedProcess) -> asyncio.Task | None:
        if self.renew_interval_seconds <= 0:
            return None
        return asyncio.create_task(self._renew_loop(claim))

    async def _stop_renew_loop(self, task: asyncio.Task | None) -> None:
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            return

    async def _renew_loop(self, claim: ClaimedProcess) -> None:
        while True:
            await asyncio.sleep(self.renew_interval_seconds)
            try:
                await self._heartbeat(
                    run_id=claim.run_id,
                    process_id=claim.process.id,
                    status=RuntimeWorkerStatus.working,
                    current_document_id=claim.document_id,
                    current_process_id=claim.process.id,
                )
                renewed = await self.client.renew_claim(
                    run_id=claim.run_id,
                    document_id=claim.document_id,
                    process_id=claim.process.id,
                    pipeline_id=self.pipeline_id,
                    worker_id=self.worker_id,
                    lease_seconds=self.lease_seconds,
                )
            except Exception:
                return
            if renewed is None:
                return

    async def _heartbeat(
        self,
        *,
        run_id: str,
        process_id: str | None = None,
        status: RuntimeWorkerStatus,
        current_document_id: str | None = None,
        current_process_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        try:
            await self.client.worker_heartbeat(
                run_id=run_id,
                worker_id=self.worker_id,
                pipeline_id=self.pipeline_id,
                process_id=process_id,
                adapter_kind=self.adapter_kind,
                capabilities=self.capabilities,
                resources=self.resources,
                status=status,
                current_document_id=current_document_id,
                current_process_id=current_process_id,
                metadata=metadata,
            )
        except Exception:
            return


def _process_spec_from_claim(claim: ClaimedProcess) -> ProcessSpec:
    return ProcessSpec(
        id=claim.process.id,
        capability=claim.process.capability,
        needs=claim.process.needs,
        adapter=AdapterSpec.model_validate(claim.process.adapter),
        timeout_seconds=claim.process.timeout_seconds,
        priority=claim.process.priority,
        max_concurrency=claim.process.max_concurrency,
        resource_pool=claim.process.resource_pool,
        resources=claim.process.resources,
        config=claim.process.config or claim.context.config,
    )


def _normalize_output(value: ProcessOutput | dict[str, Any]) -> ProcessOutput:
    if isinstance(value, ProcessOutput):
        return value
    if isinstance(value, dict):
        return ProcessOutput.model_validate(value)
    raise TypeError(f"Unsupported process output type: {type(value).__name__}")


def _failure_outcome_from_response(response: dict[str, Any]) -> ProcessWorkerOutcome:
    action = response.get("action") if isinstance(response, dict) else None
    if isinstance(action, dict):
        action_value = action.get("action")
        if action_value == ProcessAction.retry.value:
            return ProcessWorkerOutcome.retry_scheduled
        if action_value == ProcessAction.fail.value:
            return ProcessWorkerOutcome.terminal_failed
    status = response.get("status") if isinstance(response, dict) else None
    status_value = status.value if hasattr(status, "value") else status
    if status_value == ProcessStatus.failed.value:
        return ProcessWorkerOutcome.terminal_failed
    return ProcessWorkerOutcome.terminal_failed
