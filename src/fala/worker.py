from __future__ import annotations

import asyncio
from typing import Any

from pydantic import BaseModel, ConfigDict

from fala.adapters import AdapterRegistry
from fala.client import ProcessRuntimeClient
from fala.models import (
    AdapterKind,
    AdapterSpec,
    ProcessEvent,
    ProcessExecutionContext,
    ProcessOutput,
    ProcessSpec,
    ProcessStatus,
)
from fala.scheduler import ClaimedProcess


class ProcessWorkerResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claimed: ClaimedProcess | None
    completed: bool = False
    error: str | None = None


class AdapterProcessRuntimeWorker:
    """Runs claimed process adapters through the process-runtime API."""

    def __init__(
        self,
        *,
        client: ProcessRuntimeClient,
        pipeline_id: str,
        worker_id: str,
        adapter_kind: AdapterKind,
        adapters: AdapterRegistry | None = None,
        lease_seconds: float = 300.0,
        renew_interval_seconds: float | None = None,
    ) -> None:
        self.client = client
        self.pipeline_id = pipeline_id
        self.worker_id = worker_id
        self.adapter_kind = adapter_kind
        self.adapters = adapters or AdapterRegistry.default()
        self.lease_seconds = lease_seconds
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
        claim = await self.client.claim_next(
            run_id=run_id,
            pipeline_id=self.pipeline_id,
            worker_id=self.worker_id,
            process_id=process_id,
            adapter_kind=self.adapter_kind,
            lease_seconds=self.lease_seconds,
        )
        if claim is None:
            return ProcessWorkerResult(claimed=None)

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
            await self._stop_renew_loop(renew_task)
            await self.client.write_status(
                run_id=claim.run_id,
                document_id=claim.document_id,
                process_id=claim.process.id,
                status=ProcessStatus.failed,
                worker_id=self.worker_id,
                data={"worker_id": self.worker_id, "attempt": claim.attempt, "error": str(exc)},
            )
            return ProcessWorkerResult(claimed=claim, error=str(exc))
        await self._stop_renew_loop(renew_task)

        await self.client.write_output(
            run_id=claim.run_id,
            document_id=claim.document_id,
            process_id=claim.process.id,
            output=output,
            pipeline_id=self.pipeline_id,
            worker_id=self.worker_id,
        )
        return ProcessWorkerResult(claimed=claim, completed=True)

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


def _process_spec_from_claim(claim: ClaimedProcess) -> ProcessSpec:
    return ProcessSpec(
        id=claim.process.id,
        needs=claim.process.needs,
        adapter=AdapterSpec.model_validate(claim.process.adapter),
        timeout_seconds=claim.process.timeout_seconds,
        config=claim.process.config or claim.context.config,
    )


def _normalize_output(value: ProcessOutput | dict[str, Any]) -> ProcessOutput:
    if isinstance(value, ProcessOutput):
        return value
    if isinstance(value, dict):
        return ProcessOutput.model_validate(value)
    raise TypeError(f"Unsupported process output type: {type(value).__name__}")
