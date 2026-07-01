from __future__ import annotations

import asyncio

from fala.adapters import AdapterRegistry
from fala.errors import FalaRuntimeError
from fala.lineage import output_with_lineage
from fala.models import (
    CombinedProjection,
    DocumentRunResult,
    PipelineSpec,
    ProcessEvent,
    ProcessExecutionContext,
    ProcessInput,
    ProcessOutput,
    ProcessSpec,
    ProcessStatus,
    new_id,
)
from fala.store import InMemoryStateStore, StateStore


class PipelineRunError(FalaRuntimeError):
    pass


class PipelineRunner:
    """Local harness for tests and operator smoke runs.

    Production orchestration should use the scheduler/claim API and run each
    process through a subprocess, HTTP service, or external queue worker.
    """

    def __init__(
        self,
        pipeline: PipelineSpec,
        *,
        store: StateStore | None = None,
        adapters: AdapterRegistry | None = None,
    ) -> None:
        self.pipeline = pipeline
        self.store = store or InMemoryStateStore()
        self.adapters = adapters or AdapterRegistry.default()

    async def run_document(
        self,
        *,
        document_id: str,
        run_id: str | None = None,
        values: dict | None = None,
        artifacts: list | None = None,
    ) -> DocumentRunResult:
        active_run_id = run_id or new_id("run")
        base_input = ProcessInput(values=values or {}, artifacts=artifacts or [])
        await self.store.put_document_input(
            run_id=active_run_id,
            document_id=document_id,
            input=base_input,
            pipeline_id=self.pipeline.id,
        )
        await self.store.append_event(
            ProcessEvent(
                run_id=active_run_id,
                document_id=document_id,
                process_id=None,
                type="document.initialized",
                data={
                    "pipeline_id": self.pipeline.id,
                    "value_keys": sorted(base_input.values),
                    "artifact_count": len(base_input.artifacts),
                },
            )
        )
        pending = {step.id: step for step in self.pipeline.steps}
        outputs: dict[str, ProcessOutput] = {}

        while pending:
            ready = [
                step
                for step in pending.values()
                if all(dep in outputs for dep in step.needs)
            ]
            if not ready:
                blocked = ", ".join(sorted(pending))
                raise PipelineRunError(f"Pipeline has no runnable process. Blocked: {blocked}")

            results = await asyncio.gather(
                *[
                    self._run_step(
                        step,
                        run_id=active_run_id,
                        document_id=document_id,
                        base_input=base_input,
                        outputs=outputs,
                    )
                    for step in ready
                ],
                return_exceptions=True,
            )

            first_error: BaseException | None = None
            for step, result in zip(ready, results, strict=True):
                if isinstance(result, BaseException):
                    first_error = first_error or result
                    continue
                outputs[step.id] = result
                del pending[step.id]
                await self._refresh_combines(
                    run_id=active_run_id,
                    document_id=document_id,
                    changed_process_id=step.id,
                )

            if first_error is not None:
                raise PipelineRunError(str(first_error)) from first_error

        projections: dict[str, CombinedProjection] = {}
        for combine in self.pipeline.combines:
            projection = await self.store.get_projection(
                run_id=active_run_id,
                document_id=document_id,
                projection_id=combine.id,
            )
            if projection is not None:
                projections[combine.id] = projection

        events = await self.store.list_events(run_id=active_run_id, document_id=document_id)
        return DocumentRunResult(
            run_id=active_run_id,
            document_id=document_id,
            outputs=outputs,
            projections=projections,
            events=events,
        )

    async def _run_step(
        self,
        step: ProcessSpec,
        *,
        run_id: str,
        document_id: str,
        base_input: ProcessInput,
        outputs: dict[str, ProcessOutput],
    ) -> ProcessOutput:
        attempt = 0
        last_error: BaseException | None = None
        while attempt < step.retry.max_attempts:
            attempt += 1
            await self._set_status_and_event(
                run_id=run_id,
                document_id=document_id,
                process_id=step.id,
                status=ProcessStatus.running,
                event_type="process.started",
                data={"attempt": attempt},
            )

            try:
                context = ProcessExecutionContext(
                    pipeline_id=self.pipeline.id,
                    run_id=run_id,
                    document_id=document_id,
                    process_id=step.id,
                    attempt=attempt,
                    input=self._build_input(step, base_input=base_input, outputs=outputs),
                    config=step.config,
                )
                output = await self._run_adapter(step, context)
                output = output_with_lineage(
                    output,
                    context=context,
                    step=step,
                    need_outputs={
                        dep: outputs[dep]
                        for dep in step.needs
                        if dep in outputs
                    },
                )
                await self.store.put_output(
                    run_id=run_id,
                    document_id=document_id,
                    process_id=step.id,
                    output=output,
                )
                await self._set_status_and_event(
                    run_id=run_id,
                    document_id=document_id,
                    process_id=step.id,
                    status=ProcessStatus.completed,
                    event_type="process.completed",
                    data={
                        "attempt": attempt,
                        "artifact_count": len(output.artifacts),
                        "value_keys": sorted(output.values),
                    },
                )
                return output
            except BaseException as exc:
                last_error = exc
                if attempt < step.retry.max_attempts:
                    await self.store.append_event(
                        ProcessEvent(
                            run_id=run_id,
                            document_id=document_id,
                            process_id=step.id,
                            type="process.retrying",
                            status=ProcessStatus.running,
                            data={"attempt": attempt, "error": str(exc)},
                        )
                    )
                    if step.retry.delay_seconds:
                        await asyncio.sleep(step.retry.delay_seconds)
                    continue

                await self._set_status_and_event(
                    run_id=run_id,
                    document_id=document_id,
                    process_id=step.id,
                    status=ProcessStatus.failed,
                    event_type="process.failed",
                    data={"attempt": attempt, "error": str(exc)},
                )
                raise

        raise PipelineRunError(str(last_error or f"Process {step.id!r} failed"))

    async def _run_adapter(
        self, step: ProcessSpec, context: ProcessExecutionContext
    ) -> ProcessOutput:
        coro = self.adapters.run(
            step,
            context,
            event_sink=self.store.append_event,
        )
        if step.timeout_seconds:
            return await asyncio.wait_for(coro, timeout=step.timeout_seconds)
        return await coro

    def _build_input(
        self,
        step: ProcessSpec,
        *,
        base_input: ProcessInput,
        outputs: dict[str, ProcessOutput],
    ) -> ProcessInput:
        artifacts = list(base_input.artifacts)
        needs: dict[str, dict] = {}
        for dep in step.needs:
            output = outputs[dep]
            artifacts.extend(output.artifacts)
            needs[dep] = output.values

        return ProcessInput(
            artifacts=artifacts,
            values={
                "initial": base_input.values,
                "needs": needs,
            },
        )

    async def _refresh_combines(
        self,
        *,
        run_id: str,
        document_id: str,
        changed_process_id: str,
    ) -> None:
        for combine in self.pipeline.combines:
            if changed_process_id not in combine.needs:
                continue

            latest = await self.store.get_outputs(
                run_id=run_id,
                document_id=document_id,
                process_ids=combine.needs,
            )
            complete = set(latest) == set(combine.needs)
            if not complete and not combine.emit_partial:
                continue

            projection = CombinedProjection(
                id=combine.id,
                run_id=run_id,
                document_id=document_id,
                complete=complete,
                latest=latest,
            )
            await self.store.put_projection(projection)
            await self.store.append_event(
                ProcessEvent(
                    run_id=run_id,
                    document_id=document_id,
                    process_id=None,
                    type="projection.updated",
                    data={
                        "projection_id": combine.id,
                        "complete": complete,
                        "process_ids": sorted(latest),
                    },
                )
            )

    async def _set_status_and_event(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        status: ProcessStatus,
        event_type: str,
        data: dict,
    ) -> None:
        await self.store.set_status(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            status=status,
            **_process_metadata_args(self.pipeline.id, _step_by_id(self.pipeline, process_id)),
        )
        await self.store.append_event(
            ProcessEvent(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                type=event_type,
                status=status,
                data=data,
            )
        )


def _step_by_id(pipeline: PipelineSpec, process_id: str) -> ProcessSpec:
    for step in pipeline.steps:
        if step.id == process_id:
            return step
    raise ValueError(f"Unknown process id: {process_id}")


def _process_metadata_args(pipeline_id: str, step: ProcessSpec) -> dict[str, str | None]:
    return {
        "pipeline_id": pipeline_id,
        "capability": step.capability,
        "adapter_kind": step.adapter.kind,
        "resource_pool": step.resource_pool,
    }
