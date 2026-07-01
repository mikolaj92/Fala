from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from fala.models import (
    ArtifactRef,
    ChildDocumentWaitSpec,
    PipelineSpec,
    ProcessAction,
    ProcessClaim,
    ProcessConditionSpec,
    ProcessEvent,
    ProcessExecutionContext,
    ProcessInput,
    ProcessOutput,
    ProcessSpec,
    ProcessStatus,
    ResourceQuantity,
    ResourceSpec,
    RuntimeDocument,
)
from fala.store import StateStore


def process_condition_matches(
    condition: ProcessConditionSpec,
    *,
    document: Any | None = None,
    input: ProcessInput | None = None,
) -> bool:
    document_type = getattr(document, "document_type", None)
    media_type = getattr(document, "media_type", None)
    metadata = getattr(document, "metadata", None) or {}
    values = input.values if input is not None else {}
    if condition.document_types and document_type not in set(condition.document_types):
        return False
    if condition.media_types and not _matches_any_media_type(
        media_type,
        condition.media_types,
    ):
        return False
    if not _expected_items_match(condition.metadata, metadata):
        return False
    if not _expected_items_match(condition.values, values):
        return False
    return True


class ScheduledProcess(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    capability: str | None = None
    needs: list[str] = Field(default_factory=list)
    adapter: dict
    timeout_seconds: float | None = None
    priority: int = 0
    max_concurrency: int | None = None
    resource_pool: str = "default"
    resources: ResourceSpec = Field(default_factory=ResourceSpec)
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


class WaitDiagnosticIssue(BaseModel):
    model_config = ConfigDict(extra="forbid")

    process_id: str
    status: ProcessStatus | None = None
    reason: str
    missing_needs: list[str] = Field(default_factory=list)
    blocked_by: list[str] = Field(default_factory=list)
    dependency_statuses: dict[str, str | None] = Field(default_factory=dict)
    data: dict[str, Any] = Field(default_factory=dict)


class WaitGraphDiagnostic(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_id: str
    run_id: str
    document_id: str
    deadlocked: bool = False
    deadlocks: list[list[str]] = Field(default_factory=list)
    wait_edges: dict[str, list[str]] = Field(default_factory=dict)
    blocked: list[WaitDiagnosticIssue] = Field(default_factory=list)
    queued: list[str] = Field(default_factory=list)
    running: list[str] = Field(default_factory=list)
    completed: list[str] = Field(default_factory=list)
    failed: list[str] = Field(default_factory=list)
    skipped: list[str] = Field(default_factory=list)
    cancelled: list[str] = Field(default_factory=list)


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
        scheduled_at: datetime | None = None,
    ) -> ScheduleResult:
        input_payload = ProcessInput(
            values=values or {},
            artifacts=artifacts or [],
            scheduled_at=_normalize_datetime(scheduled_at),
        )
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
            initialized_data = {
                "pipeline_id": self.pipeline.id,
                "value_keys": sorted(active_input.values),
                "artifact_count": len(active_input.artifacts),
            }
            if active_input.scheduled_at is not None:
                initialized_data["scheduled_at"] = _format_datetime(
                    active_input.scheduled_at
                )
            await self.store.append_event(
                ProcessEvent(
                    run_id=run_id,
                    document_id=document_id,
                    process_id=None,
                    type="document.initialized",
                    data=initialized_data,
                )
            )

        return await self.schedule_ready(run_id=run_id, document_id=document_id)

    async def schedule_ready(self, *, run_id: str, document_id: str) -> ScheduleResult:
        await self.recover_expired_claims(run_id=run_id, document_id=document_id)
        statuses = await self.store.list_statuses(run_id=run_id, document_id=document_id)
        outputs = await self.store.list_outputs(run_id=run_id, document_id=document_id)
        document = await self.store.get_document(run_id=run_id, document_id=document_id)
        base_input = await self.store.get_document_input(
            run_id=run_id,
            document_id=document_id,
        )
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

            if not process_condition_matches(
                step.when,
                document=document,
                input=base_input,
            ):
                skipped.append(step.id)
                await self.store.clear_claim(
                    run_id=run_id,
                    document_id=document_id,
                    process_id=step.id,
                )
                if current_status != ProcessStatus.skipped:
                    await self._set_status(
                        run_id=run_id,
                        document_id=document_id,
                        step=step,
                        status=ProcessStatus.skipped,
                        event_type="process.skipped",
                        data={
                            "reason": "condition_not_matched",
                            "when": step.when.model_dump(mode="json"),
                        },
                    )
                    statuses[step.id] = ProcessStatus.skipped
                continue

            skipped_needs = [
                need
                for need in step.needs
                if statuses.get(need) == ProcessStatus.skipped and need not in outputs
            ]
            if skipped_needs:
                skipped.append(step.id)
                await self.store.clear_claim(
                    run_id=run_id,
                    document_id=document_id,
                    process_id=step.id,
                )
                if current_status != ProcessStatus.skipped:
                    await self._set_status(
                        run_id=run_id,
                        document_id=document_id,
                        step=step,
                        status=ProcessStatus.skipped,
                        event_type="process.skipped",
                        data={
                            "reason": "dependency_skipped",
                            "skipped_needs": skipped_needs,
                        },
                    )
                    statuses[step.id] = ProcessStatus.skipped
                continue

            if self._is_ready(step, outputs=set(outputs)):
                child_wait = await self._pending_child_wait(
                    step,
                    run_id=run_id,
                    document_id=document_id,
                )
                if child_wait is not None:
                    waiting.append(step.id)
                    await self._set_waiting_for_children(
                        run_id=run_id,
                        document_id=document_id,
                        step=step,
                        current_status=current_status,
                        data=child_wait,
                    )
                    continue

                not_before = await self._pending_not_before(
                    run_id=run_id,
                    document_id=document_id,
                    step=step,
                    current_status=current_status,
                    input=base_input,
                )
                if not_before is not None:
                    effective_not_before, scheduled_at, retry_after = not_before
                    waiting.append(step.id)
                    if scheduled_at is not None:
                        await self._set_waiting_for_not_before(
                            run_id=run_id,
                            document_id=document_id,
                            step=step,
                            current_status=current_status,
                            not_before=effective_not_before,
                            scheduled_at=scheduled_at,
                            retry_after=retry_after,
                        )
                        statuses[step.id] = ProcessStatus.waiting
                    continue

                if step.adapter.kind == "manual":
                    waiting.append(step.id)
                    if current_status != ProcessStatus.waiting:
                        await self._set_status(
                            run_id=run_id,
                            document_id=document_id,
                            step=step,
                            status=ProcessStatus.waiting,
                            event_type="process.manual_required",
                            data={
                                "needs": step.needs,
                                "priority": step.priority,
                                "resource_pool": step.resource_pool,
                                "resources": step.resources.model_dump(mode="json"),
                            },
                        )
                    continue

                scheduled = self._scheduled_process(step)
                queued.append(scheduled)
                if current_status != ProcessStatus.queued:
                    await self._set_status(
                        run_id=run_id,
                        document_id=document_id,
                        step=step,
                        status=ProcessStatus.queued,
                        event_type="process.queued",
                        data={
                            "needs": step.needs,
                            "priority": step.priority,
                            "max_concurrency": step.max_concurrency,
                            "resource_pool": step.resource_pool,
                            "resources": step.resources.model_dump(mode="json"),
                        },
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

    async def diagnose_waits(
        self,
        *,
        run_id: str,
        document_id: str,
    ) -> WaitGraphDiagnostic:
        schedule = await self.schedule_ready(run_id=run_id, document_id=document_id)
        statuses = await self.store.list_statuses(run_id=run_id, document_id=document_id)
        outputs = await self.store.list_outputs(run_id=run_id, document_id=document_id)
        output_ids = set(outputs)
        step_ids = {step.id for step in self.pipeline.steps}
        wait_edges: dict[str, list[str]] = {}
        blocked: list[WaitDiagnosticIssue] = []

        for step in self.pipeline.steps:
            if step.id in outputs:
                continue
            status = statuses.get(step.id)
            if status not in {None, ProcessStatus.waiting}:
                continue
            missing_needs = self._missing_needs(step, outputs=output_ids)
            if missing_needs:
                dependency_statuses = {
                    need: _status_value(statuses.get(need))
                    for need in missing_needs
                }
                terminal_needs = [
                    need
                    for need in missing_needs
                    if statuses.get(need)
                    in {
                        ProcessStatus.failed,
                        ProcessStatus.skipped,
                        ProcessStatus.cancelled,
                    }
                ]
                blocked_by = [need for need in missing_needs if need in step_ids]
                wait_edges[step.id] = blocked_by
                blocked.append(
                    WaitDiagnosticIssue(
                        process_id=step.id,
                        status=status,
                        reason=(
                            "terminal_dependencies"
                            if terminal_needs
                            else "missing_dependencies"
                        ),
                        missing_needs=missing_needs,
                        blocked_by=blocked_by,
                        dependency_statuses=dependency_statuses,
                        data=(
                            {"terminal_needs": terminal_needs}
                            if terminal_needs
                            else {}
                        ),
                    )
                )
                continue
            if status != ProcessStatus.waiting:
                continue

            events = await self.store.list_events(
                run_id=run_id,
                document_id=document_id,
                process_id=step.id,
                descending=True,
                limit=1,
            )
            event = events[0] if events else None
            blocked.append(
                WaitDiagnosticIssue(
                    process_id=step.id,
                    status=status,
                    reason=(
                        str(event.data.get("reason"))
                        if event is not None and event.data.get("reason")
                        else event.type
                        if event is not None
                        else "waiting"
                    ),
                    data=dict(event.data) if event is not None else {},
                )
            )

        deadlocks = _wait_cycles(wait_edges)
        deadlocked = bool(deadlocks) or any(
            issue.reason == "terminal_dependencies" for issue in blocked
        )
        return WaitGraphDiagnostic(
            pipeline_id=self.pipeline.id,
            run_id=run_id,
            document_id=document_id,
            deadlocked=deadlocked,
            deadlocks=deadlocks,
            wait_edges=wait_edges,
            blocked=blocked,
            queued=[process.id for process in schedule.queued],
            running=schedule.running,
            completed=schedule.completed,
            failed=schedule.failed,
            skipped=schedule.skipped,
            cancelled=schedule.cancelled,
        )

    async def claim_next(
        self,
        *,
        run_id: str,
        document_ids: list[str],
        worker_id: str | None = None,
        process_id: str | None = None,
        adapter_kind: str | None = None,
        capabilities: list[str] | None = None,
        resources: ResourceSpec | dict | None = None,
        resource_pools: dict[str, ResourceSpec | dict] | None = None,
        resource_pool_usage: dict[str, ResourceQuantity | dict] | None = None,
        lease_seconds: float = 300.0,
    ) -> ClaimedProcess | None:
        document_ids = await self._claimable_work_item_ids(
            run_id=run_id,
            document_ids=document_ids,
        )
        capability_set = set(capabilities or [])
        worker_resources = ResourceSpec.model_validate(resources or {})
        pool_limits = _normalize_resource_pools(resource_pools)
        pool_usage = _normalize_resource_pool_usage(resource_pool_usage)
        sorted_document_ids = sorted(document_ids)
        for document_id in sorted_document_ids:
            await self.schedule_ready(run_id=run_id, document_id=document_id)

        running_counts = await self._running_counts(
            run_id=run_id,
            document_ids=sorted_document_ids,
        )
        step_positions = {step.id: index for index, step in enumerate(self.pipeline.steps)}
        candidates: list[tuple[int, str, int, ProcessSpec, dict[str, ProcessOutput]]] = []
        for document_id in sorted_document_ids:
            statuses = await self.store.list_statuses(run_id=run_id, document_id=document_id)
            outputs = await self.store.list_outputs(run_id=run_id, document_id=document_id)
            for step in self.pipeline.steps:
                if process_id is not None and step.id != process_id:
                    continue
                if adapter_kind is not None and step.adapter.kind != adapter_kind:
                    continue
                if step.adapter.kind == "manual":
                    continue
                if capability_set and step.capability not in capability_set:
                    continue
                if not resources_compatible(step.resources, worker_resources):
                    continue
                if not resource_pool_allows(
                    step.resources,
                    pool_id=step.resource_pool,
                    pool_limits=pool_limits,
                    pool_usage=pool_usage,
                ):
                    continue
                if step.id in outputs:
                    continue
                if statuses.get(step.id) != ProcessStatus.queued:
                    continue
                if self._concurrency_saturated(step, running_counts=running_counts):
                    continue

                candidates.append(
                    (
                        -step.priority,
                        document_id,
                        step_positions[step.id],
                        step,
                        outputs,
                    )
                )

        for _priority, document_id, _step_index, step, outputs in sorted(candidates):
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
                        "capability": step.capability,
                        "worker_capabilities": sorted(capability_set),
                        "needs": step.needs,
                        "priority": step.priority,
                        "max_concurrency": step.max_concurrency,
                        "resource_pool": step.resource_pool,
                        "resources": step.resources.model_dump(mode="json"),
                        "worker_resources": worker_resources.model_dump(mode="json"),
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

    async def _claimable_work_item_ids(
        self,
        *,
        run_id: str,
        document_ids: list[str],
    ) -> list[str]:
        if self.pipeline.work_items.claim_strategy == "parallel":
            return document_ids

        ordered = [
            (
                await self._work_item_order_key(
                    run_id=run_id,
                    document_id=document_id,
                ),
                document_id,
            )
            for document_id in document_ids
        ]
        for _key, document_id in sorted(ordered):
            await self.schedule_ready(run_id=run_id, document_id=document_id)
            statuses = await self.store.list_statuses(run_id=run_id, document_id=document_id)
            if self._work_item_terminal(statuses=statuses):
                continue
            return [document_id]
        return []

    async def _work_item_order_key(
        self,
        *,
        run_id: str,
        document_id: str,
    ) -> tuple[int, str]:
        document_input = await self.store.get_document_input(
            run_id=run_id,
            document_id=document_id,
        )
        if document_input is None:
            return (10**9, document_id)
        raw_index = document_input.values.get(self.pipeline.work_items.order_by)
        try:
            return (int(raw_index), document_id)
        except (TypeError, ValueError):
            return (10**9, document_id)

    def _work_item_terminal(self, *, statuses: dict[str, ProcessStatus]) -> bool:
        step_statuses = [statuses.get(step.id) for step in self.pipeline.steps]
        if not step_statuses:
            return False
        has_failure = any(
            status in {ProcessStatus.failed, ProcessStatus.cancelled}
            for status in step_statuses
        )
        has_live = any(
            status in {ProcessStatus.running, ProcessStatus.queued}
            for status in step_statuses
        )
        has_unresolved = any(
            status in {None, ProcessStatus.waiting}
            for status in step_statuses
        )
        return not has_live and (not has_unresolved or has_failure)

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
                retry_after = self._retry_after(step)
                next_status = (
                    ProcessStatus.waiting
                    if retry_after is not None
                    else ProcessStatus.queued
                )
                event_data = {
                    "worker_id": claim.worker_id,
                    "attempt": claim.attempt,
                    "max_attempts": step.retry.max_attempts,
                    "claim_expires_at": claim.expires_at.isoformat(),
                    "next_status": next_status.value,
                }
                if retry_after is not None:
                    event_data["retry_after"] = retry_after.isoformat()
                    event_data["retry_delay_seconds"] = step.retry.delay_seconds
                await self._set_status(
                    run_id=run_id,
                    document_id=document_id,
                    step=step,
                    status=next_status,
                    event_type="process.claim_expired",
                    data=event_data,
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
            **_process_metadata_args(self.pipeline.id, self._step_by_id(process_id)),
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
        error_kind: str | None = None,
        data: dict | None = None,
    ) -> ProcessControlResult:
        if error_kind is None:
            data_error_kind = (data or {}).get("error_kind")
            if isinstance(data_error_kind, str) and data_error_kind:
                error_kind = data_error_kind
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
            "error_kind": error_kind,
            "attempt": attempt,
            "max_attempts": step.retry.max_attempts,
            "affected": descendants,
        }
        if claim is not None:
            event_data["worker_id"] = claim.worker_id

        retry_allowed = _failure_retry_allowed(step, error_kind)
        if claim is not None and attempt < step.retry.max_attempts and retry_allowed:
            retry_after = self._retry_after(step)
            next_status = (
                ProcessStatus.waiting
                if retry_after is not None
                else ProcessStatus.queued
            )
            retry_event_data = {
                **event_data,
                "next_status": next_status.value,
                "retry_allowed": True,
            }
            if retry_after is not None:
                retry_event_data["retry_after"] = retry_after.isoformat()
                retry_event_data["retry_delay_seconds"] = step.retry.delay_seconds
            await self.store.set_status(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                status=next_status,
                **_process_metadata_args(self.pipeline.id, step),
            )
            await self.store.append_event(
                ProcessEvent(
                    run_id=run_id,
                    document_id=document_id,
                    process_id=process_id,
                    type="process.retry_scheduled",
                    status=next_status,
                    data=retry_event_data,
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

        terminal_reason = _failure_terminal_reason(step, error_kind)
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
            **_process_metadata_args(self.pipeline.id, step),
        )
        await self.store.append_event(
            ProcessEvent(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                type="process.failed",
                status=ProcessStatus.failed,
                data={
                    **event_data,
                    "next_status": ProcessStatus.failed.value,
                    "retry_allowed": False,
                    "terminal_reason": terminal_reason,
                },
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

    async def _pending_child_wait(
        self,
        step: ProcessSpec,
        *,
        run_id: str,
        document_id: str,
    ) -> dict[str, Any] | None:
        wait = step.wait_for_children
        if wait is None:
            return None
        children = await self._matching_child_documents(
            wait,
            run_id=run_id,
            document_id=document_id,
        )
        required_statuses = {status.value for status in wait.required_statuses}
        waiting_children = [
            child
            for child in children
            if child.status.value not in required_statuses
        ]
        if len(children) >= wait.min_count and not waiting_children:
            return None
        status_counts = Counter(child.status.value for child in children)
        return {
            "reason": "waiting_for_children",
            "filters": {
                "from_processes": wait.from_processes,
                "document_types": wait.document_types,
                "relations": wait.relations,
                "required_statuses": sorted(required_statuses),
                "min_count": wait.min_count,
            },
            "matched_child_count": len(children),
            "ready_child_count": len(children) - len(waiting_children),
            "waiting_child_count": len(waiting_children),
            "missing_child_count": max(wait.min_count - len(children), 0),
            "status_counts": dict(sorted(status_counts.items())),
            "waiting_child_document_ids": [
                child.document_id for child in waiting_children
            ],
        }

    async def _matching_child_documents(
        self,
        wait: ChildDocumentWaitSpec,
        *,
        run_id: str,
        document_id: str,
    ) -> list[RuntimeDocument]:
        children = await self.store.list_document_records(
            run_id=run_id,
            parent_document_id=document_id,
        )
        from_processes = set(wait.from_processes)
        document_types = set(wait.document_types)
        relations = set(wait.relations)
        return [
            child
            for child in children
            if (not from_processes or child.parent_process_id in from_processes)
            and (not document_types or child.document_type in document_types)
            and (not relations or child.relation in relations)
        ]

    def _scheduled_process(self, step: ProcessSpec) -> ScheduledProcess:
        return ScheduledProcess(
            id=step.id,
            capability=step.capability,
            needs=step.needs,
            adapter=step.adapter.model_dump(mode="json"),
            timeout_seconds=step.timeout_seconds,
            priority=step.priority,
            max_concurrency=step.max_concurrency,
            resource_pool=step.resource_pool,
            resources=step.resources,
            config=step.config,
        )

    async def _running_counts(
        self,
        *,
        run_id: str,
        document_ids: list[str],
    ) -> dict[str, int]:
        counts: dict[str, int] = {}
        for document_id in document_ids:
            statuses = await self.store.list_statuses(
                run_id=run_id,
                document_id=document_id,
            )
            for process_id, status in statuses.items():
                if status == ProcessStatus.running:
                    counts[process_id] = counts.get(process_id, 0) + 1
        return counts

    def _concurrency_saturated(
        self,
        step: ProcessSpec,
        *,
        running_counts: dict[str, int],
    ) -> bool:
        if step.max_concurrency is None:
            return False
        return running_counts.get(step.id, 0) >= step.max_concurrency

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
            capability=step.capability,
            attempt=attempt,
            input=ProcessInput(
                artifacts=artifacts,
                scheduled_at=base_input.scheduled_at,
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
            **_process_metadata_args(self.pipeline.id, self._step_by_id(process_id)),
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

    def _retry_after(self, step: ProcessSpec) -> datetime | None:
        if step.retry.delay_seconds <= 0:
            return None
        return self._now() + timedelta(seconds=step.retry.delay_seconds)

    async def _pending_retry_after(
        self,
        *,
        run_id: str,
        document_id: str,
        step: ProcessSpec,
        current_status: ProcessStatus | None,
    ) -> datetime | None:
        if current_status != ProcessStatus.waiting or step.retry.delay_seconds <= 0:
            return None

        events = await self.store.list_events(
            run_id=run_id,
            document_id=document_id,
            process_id=step.id,
        )
        retry_after = _latest_retry_after(events)
        if retry_after is None or retry_after <= self._now():
            return None
        return retry_after

    async def _pending_not_before(
        self,
        *,
        run_id: str,
        document_id: str,
        step: ProcessSpec,
        current_status: ProcessStatus | None,
        input: ProcessInput | None,
    ) -> tuple[datetime, datetime | None, datetime | None] | None:
        now = self._now()
        scheduled_at = _normalize_datetime(input.scheduled_at) if input else None
        pending_scheduled_at = (
            scheduled_at if scheduled_at is not None and scheduled_at > now else None
        )
        retry_after = await self._pending_retry_after(
            run_id=run_id,
            document_id=document_id,
            step=step,
            current_status=current_status,
        )
        candidates = [
            candidate
            for candidate in (pending_scheduled_at, retry_after)
            if candidate is not None
        ]
        if not candidates:
            return None
        return max(candidates), pending_scheduled_at, retry_after

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
            **_process_metadata_args(self.pipeline.id, step),
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

    async def _set_waiting_for_children(
        self,
        *,
        run_id: str,
        document_id: str,
        step: ProcessSpec,
        current_status: ProcessStatus | None,
        data: dict,
    ) -> None:
        if current_status != ProcessStatus.waiting:
            await self._set_status(
                run_id=run_id,
                document_id=document_id,
                step=step,
                status=ProcessStatus.waiting,
                event_type="process.waiting_for_children",
                data=data,
            )
            return

        events = await self.store.list_events(
            run_id=run_id,
            document_id=document_id,
            process_id=step.id,
            descending=True,
            limit=1,
        )
        if events and events[0].type == "process.waiting_for_children":
            return
        await self.store.append_event(
            ProcessEvent(
                run_id=run_id,
                document_id=document_id,
                process_id=step.id,
                type="process.waiting_for_children",
                status=ProcessStatus.waiting,
                data=data,
            )
        )

    async def _set_waiting_for_not_before(
        self,
        *,
        run_id: str,
        document_id: str,
        step: ProcessSpec,
        current_status: ProcessStatus | None,
        not_before: datetime,
        scheduled_at: datetime,
        retry_after: datetime | None,
    ) -> None:
        data = {
            "reason": "not_before",
            "needs": step.needs,
            "priority": step.priority,
            "resource_pool": step.resource_pool,
            "resources": step.resources.model_dump(mode="json"),
            "scheduled_at": _format_datetime(scheduled_at),
            "not_before": _format_datetime(not_before),
            "next_status": ProcessStatus.waiting.value,
        }
        if retry_after is not None:
            data["retry_after"] = _format_datetime(retry_after)

        if current_status != ProcessStatus.waiting:
            await self._set_status(
                run_id=run_id,
                document_id=document_id,
                step=step,
                status=ProcessStatus.waiting,
                event_type="process.waiting",
                data=data,
            )
            return

        events = await self.store.list_events(
            run_id=run_id,
            document_id=document_id,
            process_id=step.id,
            descending=True,
            limit=1,
        )
        if events and _same_not_before_wait(events[0], data):
            return
        await self.store.append_event(
            ProcessEvent(
                run_id=run_id,
                document_id=document_id,
                process_id=step.id,
                type="process.waiting",
                status=ProcessStatus.waiting,
                data=data,
            )
        )


def _latest_retry_after(events: list[ProcessEvent]) -> datetime | None:
    for event in reversed(events):
        if event.type not in {"process.retry_scheduled", "process.claim_expired"}:
            continue
        retry_after = _parse_retry_after(event.data.get("retry_after"))
        if retry_after is not None:
            return retry_after
    return None


def _status_value(status: ProcessStatus | None) -> str | None:
    return status.value if status is not None else None


def _wait_cycles(edges: dict[str, list[str]]) -> list[list[str]]:
    cycles: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()
    path: list[str] = []
    visiting: dict[str, int] = {}
    visited: set[str] = set()

    def visit(node: str) -> None:
        if node in visiting:
            cycle = path[visiting[node] :]
            key = tuple(sorted(cycle))
            if key not in seen:
                seen.add(key)
                cycles.append(cycle)
            return
        if node in visited:
            return

        visiting[node] = len(path)
        path.append(node)
        for dependency in edges.get(node, []):
            if dependency in edges:
                visit(dependency)
        path.pop()
        visiting.pop(node, None)
        visited.add(node)

    for node in sorted(edges):
        visit(node)
    return cycles


def _parse_retry_after(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_aware_utc(parsed)


def _normalize_datetime(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return _ensure_aware_utc(value)


def _ensure_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _format_datetime(value: datetime) -> str:
    return _ensure_aware_utc(value).isoformat()


def _same_not_before_wait(event: ProcessEvent, data: dict) -> bool:
    if event.type != "process.waiting" or event.data.get("reason") != "not_before":
        return False
    return (
        event.data.get("scheduled_at") == data.get("scheduled_at")
        and event.data.get("retry_after") == data.get("retry_after")
        and event.data.get("not_before") == data.get("not_before")
    )


def _process_metadata_args(pipeline_id: str, step: ProcessSpec) -> dict[str, str | None]:
    return {
        "pipeline_id": pipeline_id,
        "capability": step.capability,
        "adapter_kind": step.adapter.kind,
        "resource_pool": step.resource_pool,
    }


def resources_compatible(
    required: ResourceSpec,
    available: ResourceSpec,
) -> bool:
    if not required.has_requirements():
        return True
    for field in ("cpu_cores", "memory_mb", "disk_mb", "gpu_count"):
        required_value = getattr(required, field)
        if required_value is None:
            continue
        available_value = getattr(available, field)
        if available_value is None or available_value < required_value:
            return False
    if not set(required.labels).issubset(set(available.labels)):
        return False
    for key, required_value in required.units.items():
        if available.units.get(key, 0) < required_value:
            return False
    return True


def resource_pool_allows(
    required: ResourceSpec,
    *,
    pool_id: str,
    pool_limits: dict[str, ResourceSpec],
    pool_usage: dict[str, ResourceQuantity],
) -> bool:
    limit = pool_limits.get(pool_id)
    if limit is None:
        return True
    used = pool_usage.get(pool_id, ResourceQuantity())
    for field in ("cpu_cores", "memory_mb", "disk_mb", "gpu_count"):
        limit_value = getattr(limit, field)
        required_value = getattr(required, field)
        if limit_value is None or required_value is None:
            continue
        if getattr(used, field) + required_value > limit_value:
            return False
    for key, limit_value in limit.units.items():
        required_value = required.units.get(key)
        if required_value is None:
            continue
        if used.units.get(key, 0) + required_value > limit_value:
            return False
    return True


def _failure_retry_allowed(step: ProcessSpec, error_kind: str | None) -> bool:
    if error_kind is not None and error_kind in set(step.retry.terminal_error_kinds):
        return False
    retry_kinds = set(step.retry.retry_error_kinds)
    if retry_kinds:
        return error_kind in retry_kinds
    return True


def _failure_terminal_reason(step: ProcessSpec, error_kind: str | None) -> str | None:
    if error_kind is not None and error_kind in set(step.retry.terminal_error_kinds):
        return "terminal_error_kind"
    retry_kinds = set(step.retry.retry_error_kinds)
    if retry_kinds and error_kind not in retry_kinds:
        return "non_retryable_error_kind"
    return "max_attempts_exhausted"


def resource_usage_add(
    lhs: ResourceQuantity,
    rhs: ResourceSpec,
) -> ResourceQuantity:
    units = dict(lhs.units)
    for key, value in rhs.units.items():
        units[key] = units.get(key, 0) + value
    return ResourceQuantity(
        cpu_cores=lhs.cpu_cores + (rhs.cpu_cores or 0),
        memory_mb=lhs.memory_mb + (rhs.memory_mb or 0),
        disk_mb=lhs.disk_mb + (rhs.disk_mb or 0),
        gpu_count=lhs.gpu_count + (rhs.gpu_count or 0),
        units=units,
    )


def resource_usage_remaining(
    *,
    limit: ResourceSpec,
    used: ResourceQuantity,
) -> ResourceQuantity:
    return ResourceQuantity(
        cpu_cores=_remaining(limit.cpu_cores, used.cpu_cores),
        memory_mb=int(_remaining(limit.memory_mb, used.memory_mb)),
        disk_mb=int(_remaining(limit.disk_mb, used.disk_mb)),
        gpu_count=int(_remaining(limit.gpu_count, used.gpu_count)),
        units={
            key: max(0.0, value - used.units.get(key, 0.0))
            for key, value in limit.units.items()
        },
    )


def resource_pool_saturated(
    *,
    limit: ResourceSpec,
    used: ResourceQuantity,
) -> bool:
    if not limit.has_requirements():
        return False
    for field in ("cpu_cores", "memory_mb", "disk_mb", "gpu_count"):
        limit_value = getattr(limit, field)
        if limit_value is not None and getattr(used, field) >= limit_value:
            return True
    return any(used.units.get(key, 0) >= value for key, value in limit.units.items())


def _remaining(limit_value: float | int | None, used_value: float | int) -> float:
    if limit_value is None:
        return 0.0
    return max(0.0, float(limit_value) - float(used_value))


def _normalize_resource_pools(
    resource_pools: dict[str, ResourceSpec | dict] | None,
) -> dict[str, ResourceSpec]:
    return {
        pool_id: ResourceSpec.model_validate(value)
        for pool_id, value in (resource_pools or {}).items()
    }


def _normalize_resource_pool_usage(
    resource_pool_usage: dict[str, ResourceQuantity | dict] | None,
) -> dict[str, ResourceQuantity]:
    return {
        pool_id: ResourceQuantity.model_validate(value)
        for pool_id, value in (resource_pool_usage or {}).items()
    }


def _expected_items_match(expected: dict[str, Any], actual: dict[str, Any]) -> bool:
    for key, expected_value in expected.items():
        if actual.get(key) != expected_value:
            return False
    return True


def _matches_any_media_type(actual: str | None, allowed: list[str]) -> bool:
    if actual is None:
        return False
    return any(_media_type_matches(actual, item) for item in allowed)


def _media_type_matches(actual: str, allowed: str) -> bool:
    if actual == allowed:
        return True
    if allowed.endswith("/*"):
        return actual.startswith(f"{allowed[:-2]}/")
    return False
