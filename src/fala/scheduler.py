from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pydantic import BaseModel, ConfigDict, Field

from fala.models import (
    ArtifactRef,
    PipelineSpec,
    ProcessAction,
    ProcessClaim,
    ProcessEvent,
    ProcessExecutionContext,
    ProcessInput,
    ProcessOutput,
    ProcessSpec,
    ProcessStatus,
)
from fala.store import StateStore


class ScheduledProcess(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    needs: list[str] = Field(default_factory=list)
    adapter: dict
    timeout_seconds: float | None = None
    config: dict = Field(default_factory=dict)


class ScheduleResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_id: str
    run_id: str
    document_id: str
    queued: list[ScheduledProcess] = Field(default_factory=list)
    waiting: list[str] = Field(default_factory=list)
    running: list[str] = Field(default_factory=list)
    completed: list[str] = Field(default_factory=list)
    failed: list[str] = Field(default_factory=list)
    skipped: list[str] = Field(default_factory=list)
    cancelled: list[str] = Field(default_factory=list)


class ClaimedProcess(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_id: str
    run_id: str
    document_id: str
    process: ScheduledProcess
    worker_id: str | None = None
    attempt: int
    claim_expires_at: datetime
    context: ProcessExecutionContext


class ProcessControlResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_id: str
    run_id: str
    document_id: str
    process_id: str
    action: ProcessAction
    affected: list[str] = Field(default_factory=list)
    schedule: ScheduleResult


class PipelineScheduler:
    def __init__(self, pipeline: PipelineSpec, store: StateStore) -> None:
        self.pipeline = pipeline
        self.store = store

    async def initialize_document(
        self,
        *,
        run_id: str,
        document_id: str,
        values: dict | None = None,
        artifacts: list[ArtifactRef] | None = None,
    ) -> ScheduleResult:
        input_payload = ProcessInput(values=values or {}, artifacts=artifacts or [])
        existing_input = await self.store.get_document_input(
            run_id=run_id,
            document_id=document_id,
        )
        existing_pipeline_id = await self.store.get_document_pipeline_id(
            run_id=run_id,
            document_id=document_id,
        )
        if existing_pipeline_id is not None and existing_pipeline_id != self.pipeline.id:
            raise ValueError(
                f"Document {document_id!r} already initialized with pipeline "
                f"{existing_pipeline_id!r}"
            )
        active_input = existing_input or input_payload
        if existing_input is None:
            await self.store.put_document_input(
                run_id=run_id,
                document_id=document_id,
                input=input_payload,
                pipeline_id=self.pipeline.id,
            )
        elif existing_pipeline_id is None:
            await self.store.put_document_input(
                run_id=run_id,
                document_id=document_id,
                input=active_input,
                pipeline_id=self.pipeline.id,
            )
        statuses = await self.store.list_statuses(run_id=run_id, document_id=document_id)
        outputs = await self.store.list_outputs(run_id=run_id, document_id=document_id)
        if not statuses and not outputs:
            await self.store.append_event(
                ProcessEvent(
                    run_id=run_id,
                    document_id=document_id,
                    process_id=None,
                    type="document.initialized",
                    data={
                        "pipeline_id": self.pipeline.id,
                        "value_keys": sorted(active_input.values),
                        "artifact_count": len(active_input.artifacts),
                    },
                )
            )

        return await self.schedule_ready(run_id=run_id, document_id=document_id)

    async def schedule_ready(self, *, run_id: str, document_id: str) -> ScheduleResult:
        await self.recover_expired_claims(run_id=run_id, document_id=document_id)
        statuses = await self.store.list_statuses(run_id=run_id, document_id=document_id)
        outputs = await self.store.list_outputs(run_id=run_id, document_id=document_id)
        queued: list[ScheduledProcess] = []
        waiting: list[str] = []
        running: list[str] = []
        completed: list[str] = []
        failed: list[str] = []
        skipped: list[str] = []
        cancelled: list[str] = []

        for step in self.pipeline.steps:
            current_status = statuses.get(step.id)
            if step.id in outputs:
                if current_status == ProcessStatus.skipped:
                    skipped.append(step.id)
                    await self.store.clear_claim(
                        run_id=run_id,
                        document_id=document_id,
                        process_id=step.id,
                    )
                    continue
                completed.append(step.id)
                await self.store.clear_claim(
                    run_id=run_id,
                    document_id=document_id,
                    process_id=step.id,
                )
                if current_status != ProcessStatus.completed:
                    await self._set_status(
                        run_id=run_id,
                        document_id=document_id,
                        step=step,
                        status=ProcessStatus.completed,
                        event_type="process.completed",
                        data={"reason": "output_present"},
                    )
                continue

            if current_status == ProcessStatus.running:
                running.append(step.id)
                continue
            if current_status == ProcessStatus.failed:
                failed.append(step.id)
                continue
            if current_status == ProcessStatus.skipped:
                skipped.append(step.id)
                continue
            if current_status == ProcessStatus.cancelled:
                cancelled.append(step.id)
                continue

            if self._is_ready(step, outputs=set(outputs)):
                scheduled = self._scheduled_process(step)
                queued.append(scheduled)
                if current_status != ProcessStatus.queued:
                    await self._set_status(
                        run_id=run_id,
                        document_id=document_id,
                        step=step,
                        status=ProcessStatus.queued,
                        event_type="process.queued",
                        data={"needs": step.needs},
                    )
                continue

            waiting.append(step.id)
            if current_status != ProcessStatus.waiting:
                await self._set_status(
                    run_id=run_id,
                    document_id=document_id,
                    step=step,
                    status=ProcessStatus.waiting,
                    event_type="process.waiting",
                    data={"needs": step.needs, "missing": self._missing_needs(step, outputs=set(outputs))},
                )

        return ScheduleResult(
            pipeline_id=self.pipeline.id,
            run_id=run_id,
            document_id=document_id,
            queued=queued,
            waiting=waiting,
            running=running,
            completed=completed,
            failed=failed,
            skipped=skipped,
            cancelled=cancelled,
        )

    async def claim_next(
        self,
        *,
        run_id: str,
        document_ids: list[str],
        worker_id: str | None = None,
        process_id: str | None = None,
        adapter_kind: str | None = None,
        lease_seconds: float = 300.0,
    ) -> ClaimedProcess | None:
        for document_id in sorted(document_ids):
            await self.schedule_ready(run_id=run_id, document_id=document_id)
            statuses = await self.store.list_statuses(run_id=run_id, document_id=document_id)
            outputs = await self.store.list_outputs(run_id=run_id, document_id=document_id)
            for step in self.pipeline.steps:
                if process_id is not None and step.id != process_id:
                    continue
                if adapter_kind is not None and step.adapter.kind != adapter_kind:
                    continue
                if step.id in outputs:
                    continue
                if statuses.get(step.id) != ProcessStatus.queued:
                    continue

                previous_claim = await self.store.get_claim(
                    run_id=run_id,
                    document_id=document_id,
                    process_id=step.id,
                )
                attempt = (previous_claim.attempt if previous_claim else 0) + 1
                now = self._now()
                expires_at = now + timedelta(seconds=lease_seconds)
                claim = ProcessClaim(
                    run_id=run_id,
                    document_id=document_id,
                    process_id=step.id,
                    worker_id=worker_id,
                    attempt=attempt,
                    claimed_at=now,
                    expires_at=expires_at,
                )
                if not await self.store.try_claim_process(claim):
                    continue
                await self.store.append_event(
                    ProcessEvent(
                        run_id=run_id,
                        document_id=document_id,
                        process_id=step.id,
                        type="process.claimed",
                        status=ProcessStatus.running,
                        data={
                            "worker_id": worker_id,
                            "adapter_kind": step.adapter.kind,
                            "needs": step.needs,
                            "attempt": attempt,
                            "claim_expires_at": expires_at.isoformat(),
                        },
                    )
                )
                context = await self._build_execution_context(
                    step=step,
                    run_id=run_id,
                    document_id=document_id,
                    attempt=attempt,
                    outputs=outputs,
                )
                return ClaimedProcess(
                    pipeline_id=self.pipeline.id,
                    run_id=run_id,
                    document_id=document_id,
                    process=self._scheduled_process(step),
                    worker_id=worker_id,
                    attempt=attempt,
                    claim_expires_at=expires_at,
                    context=context,
                )
        return None

    async def recover_expired_claims(self, *, run_id: str, document_id: str) -> None:
        statuses = await self.store.list_statuses(run_id=run_id, document_id=document_id)
        outputs = await self.store.list_outputs(run_id=run_id, document_id=document_id)
        now = self._now()
        for step in self.pipeline.steps:
            if step.id in outputs:
                continue
            if statuses.get(step.id) != ProcessStatus.running:
                continue

            claim = await self.store.get_claim(
                run_id=run_id,
                document_id=document_id,
                process_id=step.id,
            )
            if claim is None or claim.expires_at > now:
                continue

            if claim.attempt < step.retry.max_attempts:
                await self._set_status(
                    run_id=run_id,
                    document_id=document_id,
                    step=step,
                    status=ProcessStatus.queued,
                    event_type="process.claim_expired",
                    data={
                        "worker_id": claim.worker_id,
                        "attempt": claim.attempt,
                        "max_attempts": step.retry.max_attempts,
                        "claim_expires_at": claim.expires_at.isoformat(),
                        "next_status": ProcessStatus.queued.value,
                    },
                )
                continue

            await self._set_status(
                run_id=run_id,
                document_id=document_id,
                step=step,
                status=ProcessStatus.failed,
                event_type="process.claim_expired",
                data={
                    "worker_id": claim.worker_id,
                    "attempt": claim.attempt,
                    "max_attempts": step.retry.max_attempts,
                    "claim_expires_at": claim.expires_at.isoformat(),
                    "next_status": ProcessStatus.failed.value,
                },
            )

    async def renew_claim(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        worker_id: str | None = None,
        lease_seconds: float = 300.0,
    ) -> ProcessClaim | None:
        statuses = await self.store.list_statuses(run_id=run_id, document_id=document_id)
        if statuses.get(process_id) != ProcessStatus.running:
            return None

        claim = await self.store.get_claim(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        if claim is None:
            return None
        if worker_id is not None and claim.worker_id not in {None, worker_id}:
            return None

        now = self._now()
        if claim.expires_at <= now:
            return None

        renewed = claim.model_copy(
            update={
                "worker_id": worker_id or claim.worker_id,
                "expires_at": now + timedelta(seconds=lease_seconds),
            }
        )
        await self.store.put_claim(renewed)
        await self.store.append_event(
            ProcessEvent(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                type="process.claim_renewed",
                status=ProcessStatus.running,
                data={
                    "worker_id": renewed.worker_id,
                    "attempt": renewed.attempt,
                    "claim_expires_at": renewed.expires_at.isoformat(),
                },
            )
        )
        return renewed

    async def retry_process(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        reason: str | None = None,
    ) -> ProcessControlResult:
        self._require_process(process_id)
        affected = self._process_and_descendants(process_id)
        for affected_process_id in affected:
            await self._clear_process_state(
                run_id=run_id,
                document_id=document_id,
                process_id=affected_process_id,
            )
        await self.store.clear_projections(run_id=run_id, document_id=document_id)
        await self.store.append_event(
            ProcessEvent(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                type="process.retry_requested",
                data={"reason": reason, "affected": affected},
            )
        )
        schedule = await self.schedule_ready(run_id=run_id, document_id=document_id)
        return ProcessControlResult(
            pipeline_id=self.pipeline.id,
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            action=ProcessAction.retry,
            affected=affected,
            schedule=schedule,
        )

    async def skip_process(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        reason: str | None = None,
    ) -> ProcessControlResult:
        self._require_process(process_id)
        descendants = self._descendants(process_id)
        for affected_process_id in descendants:
            await self._clear_process_state(
                run_id=run_id,
                document_id=document_id,
                process_id=affected_process_id,
            )
        await self.store.clear_projections(run_id=run_id, document_id=document_id)
        await self.store.clear_claim(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        await self.store.put_output(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            output=ProcessOutput(values={"status": "skipped", "reason": reason}),
        )
        await self.store.set_status(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            status=ProcessStatus.skipped,
        )
        await self.store.append_event(
            ProcessEvent(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                type="process.skip_requested",
                status=ProcessStatus.skipped,
                data={"reason": reason, "affected": descendants},
            )
        )
        schedule = await self.schedule_ready(run_id=run_id, document_id=document_id)
        return ProcessControlResult(
            pipeline_id=self.pipeline.id,
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            action=ProcessAction.skip,
            affected=[process_id, *descendants],
            schedule=schedule,
        )

    async def fail_process(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        reason: str | None = None,
    ) -> ProcessControlResult:
        self._require_process(process_id)
        return await self._terminal_without_output(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            status=ProcessStatus.failed,
            event_type="process.fail_requested",
            action=ProcessAction.fail,
            reason=reason,
        )

    async def record_process_failure(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        reason: str | None = None,
        data: dict | None = None,
    ) -> ProcessControlResult:
        self._require_process(process_id)
        step = self._step_by_id(process_id)
        claim = await self.store.get_claim(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        attempt = claim.attempt if claim is not None else step.retry.max_attempts
        descendants = self._descendants(process_id)
        for affected_process_id in descendants:
            await self._clear_process_state(
                run_id=run_id,
                document_id=document_id,
                process_id=affected_process_id,
            )
        await self.store.clear_output(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        await self.store.clear_projections(run_id=run_id, document_id=document_id)

        event_data = {
            **(data or {}),
            "reason": reason,
            "attempt": attempt,
            "max_attempts": step.retry.max_attempts,
            "affected": descendants,
        }
        if claim is not None:
            event_data["worker_id"] = claim.worker_id

        if claim is not None and attempt < step.retry.max_attempts:
            await self.store.set_status(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                status=ProcessStatus.queued,
            )
            await self.store.append_event(
                ProcessEvent(
                    run_id=run_id,
                    document_id=document_id,
                    process_id=process_id,
                    type="process.retry_scheduled",
                    status=ProcessStatus.queued,
                    data={**event_data, "next_status": ProcessStatus.queued.value},
                )
            )
            schedule = await self.schedule_ready(run_id=run_id, document_id=document_id)
            return ProcessControlResult(
                pipeline_id=self.pipeline.id,
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                action=ProcessAction.retry,
                affected=[process_id, *descendants],
                schedule=schedule,
            )

        await self.store.clear_claim(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        await self.store.set_status(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            status=ProcessStatus.failed,
        )
        await self.store.append_event(
            ProcessEvent(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                type="process.failed",
                status=ProcessStatus.failed,
                data={**event_data, "next_status": ProcessStatus.failed.value},
            )
        )
        schedule = await self.schedule_ready(run_id=run_id, document_id=document_id)
        return ProcessControlResult(
            pipeline_id=self.pipeline.id,
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            action=ProcessAction.fail,
            affected=[process_id, *descendants],
            schedule=schedule,
        )

    async def cancel_process(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        reason: str | None = None,
    ) -> ProcessControlResult:
        self._require_process(process_id)
        return await self._terminal_without_output(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            status=ProcessStatus.cancelled,
            event_type="process.cancel_requested",
            action=ProcessAction.cancel,
            reason=reason,
        )

    def _is_ready(self, step: ProcessSpec, *, outputs: set[str]) -> bool:
        return all(need in outputs for need in step.needs)

    def _missing_needs(self, step: ProcessSpec, *, outputs: set[str]) -> list[str]:
        return [need for need in step.needs if need not in outputs]

    def _scheduled_process(self, step: ProcessSpec) -> ScheduledProcess:
        return ScheduledProcess(
            id=step.id,
            needs=step.needs,
            adapter=step.adapter.model_dump(mode="json"),
            timeout_seconds=step.timeout_seconds,
            config=step.config,
        )

    async def _build_execution_context(
        self,
        *,
        step: ProcessSpec,
        run_id: str,
        document_id: str,
        attempt: int,
        outputs: dict[str, ProcessOutput],
    ) -> ProcessExecutionContext:
        base_input = await self.store.get_document_input(
            run_id=run_id,
            document_id=document_id,
        )
        if base_input is None:
            base_input = ProcessInput()

        artifacts = list(base_input.artifacts)
        needs: dict[str, dict] = {}
        for dep in step.needs:
            output = outputs[dep]
            artifacts.extend(output.artifacts)
            needs[dep] = output.values

        return ProcessExecutionContext(
            pipeline_id=self.pipeline.id,
            run_id=run_id,
            document_id=document_id,
            process_id=step.id,
            attempt=attempt,
            input=ProcessInput(
                artifacts=artifacts,
                values={
                    "initial": base_input.values,
                    "needs": needs,
                },
            ),
            config=step.config,
        )

    def _require_process(self, process_id: str) -> None:
        if process_id not in {step.id for step in self.pipeline.steps}:
            raise ValueError(f"Unknown process id: {process_id}")

    def _step_by_id(self, process_id: str) -> ProcessSpec:
        for step in self.pipeline.steps:
            if step.id == process_id:
                return step
        raise ValueError(f"Unknown process id: {process_id}")

    def _process_and_descendants(self, process_id: str) -> list[str]:
        return [process_id, *self._descendants(process_id)]

    def _descendants(self, process_id: str) -> list[str]:
        descendants: list[str] = []
        queue = [process_id]
        while queue:
            current = queue.pop(0)
            for step in self.pipeline.steps:
                if step.id == process_id or step.id in descendants:
                    continue
                if current in step.needs:
                    descendants.append(step.id)
                    queue.append(step.id)
        return descendants

    async def _clear_process_state(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> None:
        await self.store.clear_claim(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        await self.store.clear_output(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        await self.store.clear_status(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )

    async def _terminal_without_output(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        status: ProcessStatus,
        event_type: str,
        action: ProcessAction,
        reason: str | None,
    ) -> ProcessControlResult:
        descendants = self._descendants(process_id)
        for affected_process_id in descendants:
            await self._clear_process_state(
                run_id=run_id,
                document_id=document_id,
                process_id=affected_process_id,
            )
        await self.store.clear_claim(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        await self.store.clear_output(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        await self.store.set_status(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            status=status,
        )
        await self.store.clear_projections(run_id=run_id, document_id=document_id)
        await self.store.append_event(
            ProcessEvent(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                type=event_type,
                status=status,
                data={"reason": reason, "affected": descendants},
            )
        )
        schedule = await self.schedule_ready(run_id=run_id, document_id=document_id)
        return ProcessControlResult(
            pipeline_id=self.pipeline.id,
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            action=action,
            affected=[process_id, *descendants],
            schedule=schedule,
        )

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    async def _set_status(
        self,
        *,
        run_id: str,
        document_id: str,
        step: ProcessSpec,
        status: ProcessStatus,
        event_type: str,
        data: dict,
    ) -> None:
        await self.store.set_status(
            run_id=run_id,
            document_id=document_id,
            process_id=step.id,
            status=status,
        )
        await self.store.append_event(
            ProcessEvent(
                run_id=run_id,
                document_id=document_id,
                process_id=step.id,
                type=event_type,
                status=status,
                data=data,
            )
        )
