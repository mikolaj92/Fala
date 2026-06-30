from __future__ import annotations

import hashlib
import io
import json
import os
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, Literal
from urllib.parse import unquote, urlparse

from fala.artifacts import (
    ArtifactStore,
    create_artifact_store,
    digest_from_fala_artifact_uri,
    is_fala_artifact_uri,
    local_path_from_uri,
)
from fala.intake import (
    auto_document_routes_from_registry,
    route_runtime_documents,
    route_runtime_documents_with_report,
)
from fala.lineage import output_with_lineage
from fala.models import (
    ArtifactRef,
    CombinedProjection,
    ExistingDocumentPolicy,
    ExistingRunPolicy,
    PipelineSpec,
    ProcessAction,
    ProcessClaim,
    ProcessEvent,
    ProcessExecutionContext,
    ProcessInput,
    ProcessOutput,
    ProcessSpec,
    ProcessStatus,
    ResourcePoolSpec,
    ResourceQuantity,
    ResourceSpec,
    RuntimeArtifactBlob,
    RuntimeArtifactGcPlan,
    RuntimeDeadLetterItem,
    RuntimeDeadLetterPage,
    RuntimeDocument,
    RuntimeDocumentPage,
    RuntimeDocumentLineage,
    RuntimeDocumentLineageEdge,
    RuntimeDocumentLineageNode,
    RuntimeDocumentStatus,
    RuntimeDocumentInput,
    RuntimeDocumentState,
    OperatorAuditEvent,
    OperatorAuditEventPage,
    OutputDocumentRef,
    SpawnDocumentInput,
    RuntimeOutputDocumentItem,
    RuntimeOutputDocumentPage,
    RuntimeAttemptTrace,
    RuntimeProcessPage,
    RuntimeProcessRecord,
    RuntimeProcessMetrics,
    RuntimeProcessTrace,
    RuntimeQueueMetrics,
    RuntimeResourcePoolMetrics,
    RuntimeRunHealth,
    RuntimeRunHealthIssue,
    RuntimeRunInput,
    RuntimeRunReduction,
    RuntimeRunReductions,
    RuntimeRunRetentionItem,
    RuntimeRunRetentionPlan,
    RuntimeRunResultItem,
    RuntimeRunResults,
    RuntimeStuckWorkItem,
    RuntimeStuckWorkPage,
    RuntimeCapabilityDemand,
    RuntimeCapabilityDemandSummary,
    RuntimeStreamLagItem,
    RuntimeStreamLagPage,
    RunReduceSpec,
    RunOutcome,
    RunStatus,
    RuntimeRun,
    RuntimeStreamCheckpoint,
    RuntimeStreamChunk,
    RuntimeState,
    RuntimeStepReport,
    RuntimeTrace,
    RuntimeWorkerHeartbeat,
    RuntimeWorkerDemand,
    RuntimeWorkerState,
    RuntimeWorkerStatus,
    StreamSpec,
)
from fala.registry import PipelineRegistry
from fala.scheduler import (
    ClaimedProcess,
    PipelineScheduler,
    ProcessControlResult,
    ScheduleResult,
    process_condition_matches,
    resource_pool_allows,
    resource_pool_saturated,
    resource_usage_add,
    resource_usage_remaining,
    resources_compatible,
)
from fala.schema_validation import validate_json_value
from fala.state import (
    build_runtime_document_state,
    build_runtime_state,
    build_runtime_step_report,
    runtime_step_snapshot,
    runtime_stream_snapshots,
)
from fala.store import StateStore


class RuntimeService:
    def __init__(
        self,
        *,
        registry: PipelineRegistry,
        store: StateStore,
        artifact_roots: list[str | Path] | None = None,
        artifact_store_root: str | Path | None = None,
        artifact_store: ArtifactStore | None = None,
    ) -> None:
        self.registry = registry
        self.store = store
        self.artifact_roots = [
            _resolve_runtime_artifact_root(root) for root in artifact_roots or []
        ]
        self.artifact_store = artifact_store or create_artifact_store(artifact_store_root)

    def _operation_type_by_step_for_pipeline(
        self,
        pipeline: PipelineSpec | None,
    ) -> dict[str, str]:
        if pipeline is None:
            return {}
        package_id = self.registry.pipeline_package_id(pipeline.id)
        if package_id is None:
            return {}
        try:
            package = self.registry.package(package_id)
        except Exception:
            return {}
        capabilities = {capability.id: capability for capability in package.capabilities}
        operation_type_by_step: dict[str, str] = {}
        for step in pipeline.steps:
            if step.capability is None:
                continue
            capability = capabilities.get(step.capability)
            if capability is None or capability.operation_type is None:
                continue
            operation_type_by_step[step.id] = capability.operation_type
        return operation_type_by_step

    def _operation_type_for_step(
        self,
        pipeline: PipelineSpec | None,
        step: ProcessSpec | None,
    ) -> str | None:
        if step is None:
            return None
        return self._operation_type_by_step_for_pipeline(pipeline).get(step.id)

    async def record_operator_audit(
        self,
        *,
        action: str,
        actor: str | None = None,
        source: str | None = None,
        run_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
        target: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> OperatorAuditEvent:
        event = OperatorAuditEvent(
            actor=actor,
            source=source,
            action=action,
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            target=target,
            data={key: value for key, value in (data or {}).items() if value is not None},
        )
        await self.store.append_audit_event(event)
        return event

    async def operator_audit(
        self,
        *,
        run_id: str | None = None,
        limit: int | None = 100,
        descending: bool = True,
    ) -> OperatorAuditEventPage:
        events = await self.store.list_audit_events(
            run_id=run_id,
            limit=limit,
            descending=descending,
        )
        filters = {"run_id": run_id} if run_id is not None else {}
        return OperatorAuditEventPage(
            count=len(events),
            filters=filters,
            events=events,
        )

    async def create_run(
        self,
        *,
        run_id: str | None = None,
        title: str | None = None,
        config: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        existing_run_policy: ExistingRunPolicy = "error",
    ) -> RuntimeRun:
        _run_resource_pools(config or {})
        run = RuntimeRun(
            **({"id": run_id} if run_id is not None else {}),
            title=title,
            config=config or {},
            metadata=metadata or {},
        )
        existing = await self.store.get_run(run.id)
        if existing is not None:
            if existing_run_policy == "resume":
                return existing
            raise ValueError(f"Run {run.id!r} already exists")
        await self.store.put_run(run)
        return run

    async def create_run_with_documents(
        self,
        input: RuntimeRunInput,
        *,
        route_report: dict[str, Any] | None = None,
    ) -> tuple[RuntimeRun, list[ScheduleResult]]:
        plan = self.plan_runtime_run_input(input)
        run = await self.create_run(
            run_id=input.run_id,
            title=input.title,
            config=input.config,
            metadata=_run_metadata_with_provenance(
                input.metadata,
                input=input,
                plan=plan,
                route_report=route_report,
            ),
            existing_run_policy=input.existing_run_policy,
        )
        schedules = await self.initialize_documents(
            run_id=run.id,
            pipeline_id=input.pipeline_id,
            documents=input.documents,
            existing_document_policy=input.existing_document_policy,
        )
        run = await self.sync_run_lifecycle(run.id)
        return run, schedules

    async def append_run_documents(
        self,
        *,
        run_id: str,
        documents: list[RuntimeDocumentInput],
        pipeline_id: str | None = None,
        existing_document_policy: ExistingDocumentPolicy = "error",
        route_report: dict[str, Any] | None = None,
    ) -> tuple[RuntimeRun, list[ScheduleResult]]:
        run = await self.store.get_run(run_id)
        if run is None:
            raise LookupError(f"Run {run_id!r} not found")
        if run.status == RunStatus.cancelled:
            raise ValueError(
                f"Run {run_id!r} is cancelled and cannot accept new documents"
            )
        schedules = await self.initialize_documents(
            run_id=run_id,
            pipeline_id=pipeline_id,
            documents=documents,
            existing_document_policy=existing_document_policy,
        )
        run = await self._record_append_provenance(
            run,
            pipeline_id=pipeline_id,
            documents=documents,
            schedules=schedules,
            existing_document_policy=existing_document_policy,
            route_report=route_report,
        )
        run = await self.sync_run_lifecycle(run_id)
        return run, schedules

    async def _record_append_provenance(
        self,
        run: RuntimeRun,
        *,
        pipeline_id: str | None,
        documents: list[RuntimeDocumentInput],
        schedules: list[ScheduleResult],
        existing_document_policy: ExistingDocumentPolicy,
        route_report: dict[str, Any] | None,
    ) -> RuntimeRun:
        metadata = _run_metadata_with_append_provenance(
            run.metadata,
            pipeline_id=pipeline_id,
            documents=documents,
            schedules=schedules,
            existing_document_policy=existing_document_policy,
            route_report=route_report,
        )
        updated = run.model_copy(
            update={
                "metadata": metadata,
                "updated_at": datetime.now(timezone.utc),
            }
        )
        await self.store.put_run(updated)
        return updated

    def route_runtime_document_inputs(
        self,
        documents: list[RuntimeDocumentInput],
        *,
        routes: list[dict[str, Any]] | None = None,
        auto_route: bool = False,
    ) -> list[RuntimeDocumentInput]:
        return route_runtime_documents(
            documents,
            routes=routes or [],
            auto_routes=(
                auto_document_routes_from_registry(self.registry)
                if auto_route
                else []
            ),
        )

    def route_runtime_document_inputs_with_report(
        self,
        documents: list[RuntimeDocumentInput],
        *,
        routes: list[dict[str, Any]] | None = None,
        auto_route: bool = False,
    ) -> tuple[list[RuntimeDocumentInput], dict[str, Any]]:
        return route_runtime_documents_with_report(
            documents,
            routes=routes or [],
            auto_routes=(
                auto_document_routes_from_registry(self.registry)
                if auto_route
                else []
            ),
        )

    async def initialize_documents(
        self,
        *,
        run_id: str,
        documents: list[RuntimeDocumentInput],
        pipeline_id: str | None = None,
        existing_document_policy: ExistingDocumentPolicy = "error",
    ) -> list[ScheduleResult]:
        prepared: list[tuple[RuntimeDocumentInput, str, RuntimeDocument | None]] = []
        schedules: list[ScheduleResult] = []
        for document in documents:
            resolved_pipeline_id = document.pipeline_id or pipeline_id
            if resolved_pipeline_id is None:
                raise ValueError(
                    f"Document {document.document_id!r} requires pipeline_id "
                    "or a batch pipeline_id"
                )
            self.validate_runtime_document_input(
                pipeline_id=resolved_pipeline_id,
                document=document,
            )
            existing_document = await self.store.get_document(
                run_id=run_id,
                document_id=document.document_id,
            )
            if existing_document is not None:
                if existing_document_policy == "error":
                    raise ValueError(
                        f"Document {document.document_id!r} already exists in "
                        f"run {run_id!r}"
                    )
                if (
                    existing_document.pipeline_id is not None
                    and existing_document.pipeline_id != resolved_pipeline_id
                ):
                    raise ValueError(
                        f"Document {document.document_id!r} already initialized "
                        f"with pipeline {existing_document.pipeline_id!r}"
                    )
            prepared.append((document, resolved_pipeline_id, existing_document))

        for document, resolved_pipeline_id, existing_document in prepared:
            if existing_document is None:
                await self.store.put_document(
                    _runtime_document_from_input(
                        run_id=run_id,
                        pipeline_id=resolved_pipeline_id,
                        document=document,
                    )
                )
            schedules.append(
                await self.initialize_document(
                    run_id=run_id,
                    document_id=document.document_id,
                    pipeline_id=resolved_pipeline_id,
                    values=_document_initial_values(document),
                    artifacts=_document_artifacts(document),
                    scheduled_at=document.scheduled_at,
                )
            )
        return schedules

    def validate_runtime_run_input(self, input: RuntimeRunInput) -> None:
        _run_resource_pools(input.config)
        for document in input.documents:
            resolved_pipeline_id = document.pipeline_id or input.pipeline_id
            if resolved_pipeline_id is None:
                raise ValueError(
                    f"Document {document.document_id!r} requires pipeline_id "
                    "or a batch pipeline_id"
                )
            self.validate_runtime_document_input(
                pipeline_id=resolved_pipeline_id,
                document=document,
            )

    def preview_runtime_run_input(self, input: RuntimeRunInput) -> dict[str, Any]:
        self.validate_runtime_run_input(input)
        pipeline_ids = sorted(
            {
                pipeline_id
                for pipeline_id in [
                    input.pipeline_id,
                    *[document.pipeline_id or input.pipeline_id for document in input.documents],
                ]
                if pipeline_id is not None
            }
        )
        return {
            "ok": True,
            "run_id": input.run_id,
            "existing_run_policy": input.existing_run_policy,
            "existing_document_policy": input.existing_document_policy,
            "title": input.title,
            "pipeline_id": input.pipeline_id,
            "document_count": len(input.documents),
            "document_summary": _runtime_document_input_summary(
                documents=input.documents,
                pipeline_id=input.pipeline_id,
            ),
            "documents": [
                {
                    "document_id": document.document_id,
                    "title": document.title,
                    "pipeline_id": document.pipeline_id or input.pipeline_id,
                    "document_type": document.document_type,
                    "media_type": document.media_type,
                    "source_uri": document.source_uri,
                    "value_keys": sorted(document.values),
                    "metadata_keys": sorted(document.metadata),
                    "artifact_count": len(document.artifacts),
                    "artifact_kinds": sorted(
                        {artifact.kind for artifact in document.artifacts}
                    ),
                }
                for document in input.documents
            ],
            "contracts": {
                pipeline_id: self.registry.pipeline_contract(pipeline_id)
                for pipeline_id in pipeline_ids
            },
        }

    def plan_runtime_run_input(self, input: RuntimeRunInput) -> dict[str, Any]:
        preview = self.preview_runtime_run_input(input)
        process_plans: dict[tuple[str, str], dict[str, Any]] = {}
        pool_plans: dict[str, dict[str, Any]] = {}
        document_plans: list[dict[str, Any]] = []

        for document in input.documents:
            pipeline_id = document.pipeline_id or input.pipeline_id
            if pipeline_id is None:
                raise ValueError(
                    f"Document {document.document_id!r} requires pipeline_id "
                    "or a batch pipeline_id"
                )
            pipeline = self.registry.get(pipeline_id)
            queued: list[dict[str, Any]] = []
            waiting: list[dict[str, Any]] = []
            skipped: list[dict[str, Any]] = []
            skipped_process_ids: set[str] = set()
            document_input = ProcessInput(
                values=document.values,
                artifacts=document.artifacts,
                scheduled_at=document.scheduled_at,
            )
            for step in pipeline.steps:
                if not process_condition_matches(
                    step.when,
                    document=document,
                    input=document_input,
                ):
                    status = "skipped"
                    skipped_process_ids.add(step.id)
                elif any(need in skipped_process_ids for need in step.needs):
                    status = "skipped"
                    skipped_process_ids.add(step.id)
                else:
                    status = (
                        "queued"
                        if not step.needs and step.adapter.kind != "manual"
                        else "waiting"
                    )
                step_plan = _planned_step(
                    registry=self.registry,
                    pipeline_id=pipeline_id,
                    step=step,
                    status=status,
                )
                if status == "queued":
                    queued.append(step_plan)
                elif status == "waiting":
                    waiting.append(step_plan)
                else:
                    skipped.append(step_plan)
                _accumulate_process_plan(
                    process_plans,
                    registry=self.registry,
                    pipeline_id=pipeline_id,
                    step=step,
                    status=status,
                )
                _accumulate_pool_plan(
                    pool_plans,
                    pipeline_id=pipeline_id,
                    pool_id=step.resource_pool,
                    step=step,
                    status=status,
                )
            document_plans.append(
                {
                    "document_id": document.document_id,
                    "pipeline_id": pipeline_id,
                    "process_count": len(pipeline.steps),
                    "queued_count": len(queued),
                    "waiting_count": len(waiting),
                    "skipped_count": len(skipped),
                    "queued": queued,
                    "waiting": waiting,
                    "skipped": skipped,
                }
            )

        process_list = [
            _finalize_process_plan(item)
            for item in process_plans.values()
        ]
        process_list.sort(
            key=lambda item: (
                item["pipeline_id"],
                -item["queued_count"],
                -item["waiting_count"],
                -item["priority"],
                item["process_id"],
            )
        )
        resource_pool_limits = _run_resource_pools(input.config)
        resource_pool_plans = [
            _finalize_pool_plan(
                item,
                limit=resource_pool_limits.get(item["id"], ResourceSpec()),
            )
            for item in pool_plans.values()
        ]
        resource_pool_plans.sort(key=lambda item: item["id"])
        worker_demands = [
            demand
            for process in process_list
            if (demand := _planned_worker_demand(process)) is not None
        ]
        return {
            **preview,
            "plan": {
                "document_count": len(document_plans),
                "process_instance_count": sum(
                    document["process_count"] for document in document_plans
                ),
                "queued_count": sum(document["queued_count"] for document in document_plans),
                "waiting_count": sum(
                    document["waiting_count"] for document in document_plans
                ),
                "skipped_count": sum(
                    document["skipped_count"] for document in document_plans
                ),
                "process_group_count": len(process_list),
                "resource_pool_count": len(resource_pool_plans),
                "worker_demand_count": len(worker_demands),
                "processes": process_list,
                "resource_pools": resource_pool_plans,
                "worker_demands": worker_demands,
                "documents": document_plans,
            },
        }

    def validate_runtime_document_input(
        self,
        *,
        pipeline_id: str,
        document: RuntimeDocumentInput,
    ) -> None:
        if document.document_type is None:
            return
        package_id = self.registry.pipeline_package_id(pipeline_id)
        if package_id is None:
            return
        package = self.registry.package(package_id)
        document_types = {item.id: item for item in package.document_types}
        document_type_ids = set(document_types)
        if document_type_ids and document.document_type not in document_type_ids:
            declared = ", ".join(sorted(document_type_ids))
            raise ValueError(
                f"Document {document.document_id!r} type {document.document_type!r} "
                f"is not declared by workflow package {package_id!r} "
                f"(declared: {declared})"
            )
        document_type = document_types.get(document.document_type)
        if document_type is not None:
            validate_json_value(
                document.values,
                document_type.value_schema,
                label=(
                    f"Document {document.document_id!r} values for type "
                    f"{document.document_type!r} value_schema"
                ),
            )
            validate_json_value(
                document.metadata,
                document_type.metadata_schema,
                label=(
                    f"Document {document.document_id!r} metadata for type "
                    f"{document.document_type!r} metadata_schema"
                ),
            )
            _validate_document_media_type(document, media_types=document_type.media_types)
            _validate_document_extension(document, extensions=document_type.extensions)

        pipeline = self.registry.get(pipeline_id)
        capabilities = {item.id: item for item in package.capabilities}
        accepted_document_types: set[str] = set()
        for step in pipeline.steps:
            if step.needs or step.capability is None:
                continue
            capability = capabilities.get(step.capability)
            if capability is not None:
                accepted_document_types.update(capability.accepts_document_types)
        if (
            accepted_document_types
            and document.document_type not in accepted_document_types
        ):
            accepted = ", ".join(sorted(accepted_document_types))
            raise ValueError(
                f"Document {document.document_id!r} type {document.document_type!r} "
                f"is not accepted by pipeline {pipeline_id!r} root capabilities "
                f"(accepted: {accepted})"
            )

    async def get_run(self, run_id: str) -> RuntimeRun | None:
        return await self.store.get_run(run_id)

    async def run_provenance(self, run_id: str) -> dict[str, Any]:
        run = await self.get_run(run_id)
        if run is None:
            raise ValueError(f"Run {run_id!r} does not exist")
        process_runtime = run.metadata.get("process_runtime")
        runtime_metadata = (
            dict(process_runtime)
            if isinstance(process_runtime, dict)
            else {}
        )
        provenance = runtime_metadata.get("run_provenance")
        contract_drift = _run_contract_drift_report(
            self.registry,
            provenance if isinstance(provenance, dict) else None,
        )
        return {
            "ok": True,
            "run_id": run_id,
            "has_provenance": isinstance(provenance, dict),
            "provenance": dict(provenance) if isinstance(provenance, dict) else {},
            "contract_drift": contract_drift,
        }

    async def ensure_run(self, run_id: str) -> RuntimeRun:
        existing = await self.store.get_run(run_id)
        if existing is not None:
            return existing
        run = RuntimeRun(id=run_id)
        await self.store.put_run(run)
        return run

    async def pause_run(self, run_id: str, *, reason: str | None = None) -> RuntimeRun:
        run = await self.ensure_run(run_id)
        if run.status in {RunStatus.completed, RunStatus.failed, RunStatus.cancelled}:
            raise ValueError(f"Run {run_id!r} is terminal and cannot be paused")
        now = datetime.now(timezone.utc)
        updated = run.model_copy(
            update={
                "status": RunStatus.paused,
                "metadata": {
                    **run.metadata,
                    "pause_reason": reason,
                    "paused_at": now.isoformat(),
                },
                "updated_at": now,
            }
        )
        await self.store.put_run(updated)
        return updated

    async def resume_run(
        self,
        run_id: str,
        *,
        reason: str | None = None,
        allow_contract_drift: bool = False,
    ) -> RuntimeRun:
        run = await self.ensure_run(run_id)
        if run.status != RunStatus.paused:
            raise ValueError(f"Run {run_id!r} is not paused")
        await self.ensure_run_contract_current(
            run_id,
            allow_contract_drift=allow_contract_drift,
        )
        now = datetime.now(timezone.utc)
        metadata = dict(run.metadata)
        metadata.pop("pause_reason", None)
        metadata["resumed_at"] = now.isoformat()
        if reason is not None:
            metadata["resume_reason"] = reason
        await self.store.put_run(
            run.model_copy(
                update={
                    "status": RunStatus.created,
                    "metadata": metadata,
                    "updated_at": now,
                }
            )
        )
        return await self.sync_run_lifecycle(run_id)

    async def cancel_run(self, run_id: str, *, reason: str | None = None) -> RuntimeRun:
        run = await self.sync_run_lifecycle(run_id)
        if run.status == RunStatus.cancelled:
            return run
        if run.status in {RunStatus.completed, RunStatus.failed}:
            raise ValueError(f"Run {run_id!r} is terminal and cannot be cancelled")

        now = datetime.now(timezone.utc)
        cancelled_count = 0
        for document_id in await self.store.list_documents(run_id=run_id):
            pipeline_id = await self.store.get_document_pipeline_id(
                run_id=run_id,
                document_id=document_id,
            )
            if pipeline_id is None:
                continue
            try:
                pipeline = self.registry.get(pipeline_id)
            except Exception:
                continue

            statuses = await self.store.list_statuses(
                run_id=run_id,
                document_id=document_id,
            )
            outputs = await self.store.list_outputs(
                run_id=run_id,
                document_id=document_id,
            )
            document_cancelled: list[str] = []
            for step in pipeline.steps:
                if step.id in outputs:
                    continue
                if statuses.get(step.id) in {
                    ProcessStatus.failed,
                    ProcessStatus.skipped,
                    ProcessStatus.cancelled,
                }:
                    continue
                await self.store.clear_claim(
                    run_id=run_id,
                    document_id=document_id,
                    process_id=step.id,
                )
                await self.store.set_status(
                    run_id=run_id,
                    document_id=document_id,
                    process_id=step.id,
                    status=ProcessStatus.cancelled,
                    **_process_metadata_args(pipeline.id, step),
                )
                await self.store.append_event(
                    ProcessEvent(
                        run_id=run_id,
                        document_id=document_id,
                        process_id=step.id,
                        type="process.cancel_requested",
                        status=ProcessStatus.cancelled,
                        data={
                            "reason": reason,
                            "source": "run_cancel",
                            "pipeline_id": pipeline_id,
                        },
                    )
                )
                document_cancelled.append(step.id)

            if document_cancelled:
                cancelled_count += len(document_cancelled)
                await self.store.append_event(
                    ProcessEvent(
                        run_id=run_id,
                        document_id=document_id,
                        process_id=None,
                        type="run.cancel_requested",
                        status=ProcessStatus.cancelled,
                        data={
                            "reason": reason,
                            "pipeline_id": pipeline_id,
                            "affected": document_cancelled,
                        },
                    )
                )

        metadata = dict(run.metadata)
        metadata["cancelled_at"] = now.isoformat()
        metadata["cancelled_process_count"] = cancelled_count
        if reason is not None:
            metadata["cancel_reason"] = reason
        await self.store.put_run(
            run.model_copy(
                update={
                    "status": RunStatus.cancelled,
                    "outcome": RunOutcome.cancelled,
                    "metadata": metadata,
                    "updated_at": now,
                    "finished_at": run.finished_at or now,
                }
            )
        )
        return await self.sync_run_lifecycle(run_id)

    async def sync_run_lifecycle(self, run_id: str) -> RuntimeRun:
        run = await self.ensure_run(run_id)
        await self._reconcile_run_documents(run_id)
        state = await self.load_state_model(run_id, include_events=False)
        status = _run_status_from_runtime_state(state)
        if run.status == RunStatus.paused and status not in {
            RunStatus.completed,
            RunStatus.failed,
            RunStatus.cancelled,
        }:
            status = RunStatus.paused
        if run.status == RunStatus.cancelled:
            status = RunStatus.cancelled
        now = datetime.now(timezone.utc)
        started_at = run.started_at
        finished_at = run.finished_at
        outcome = run.outcome
        if status == RunStatus.running and started_at is None:
            started_at = now
        if status in {RunStatus.completed, RunStatus.failed, RunStatus.cancelled}:
            finished_at = finished_at or now
            outcome = _run_outcome_from_status(status)
        for document_state in state.documents:
            await self._sync_document_from_state(
                run_id=run_id,
                document_state=document_state,
            )
        updated = run.model_copy(
            update={
                "status": status,
                "outcome": outcome,
                "summary": state.summary.model_dump(mode="json"),
                "updated_at": now,
                "started_at": started_at,
                "finished_at": finished_at,
            }
        )
        await self.store.put_run(updated)
        return updated

    async def _sync_document_from_state(
        self,
        *,
        run_id: str,
        document_state: RuntimeDocumentState,
    ) -> None:
        now = datetime.now(timezone.utc)
        document = await self.store.get_document(
            run_id=run_id,
            document_id=document_state.document_id,
        )
        if document is None:
            document = RuntimeDocument(
                run_id=run_id,
                document_id=document_state.document_id,
                pipeline_id=document_state.pipeline_id,
            )
        status_counts: dict[str, int] = {}
        artifact_count = 0
        stream_count = 0
        stream_chunk_count = 0
        stream_artifact_count = 0
        stream_checkpoint_count = 0
        for step in document_state.steps:
            status = step.status.value if isinstance(step.status, ProcessStatus) else str(step.status)
            status_counts[status] = status_counts.get(status, 0) + 1
            artifact_count += step.artifact_count
            stream_count += step.stream_count
            stream_chunk_count += step.stream_chunk_count
            stream_artifact_count += step.stream_artifact_count
            stream_checkpoint_count += step.stream_checkpoint_count
        await self.store.put_document(
            document.model_copy(
                update={
                    "pipeline_id": document_state.pipeline_id or document.pipeline_id,
                    "status": _document_status_from_runtime_document_state(document_state),
                    "summary": {
                        "process_count": len(document_state.steps),
                        "status_counts": status_counts,
                        "claim_count": len(document_state.claims),
                        "output_count": len(document_state.outputs),
                        "projection_count": len(document_state.projections),
                        "artifact_count": artifact_count,
                        "stream_count": stream_count,
                        "stream_chunk_count": stream_chunk_count,
                        "stream_artifact_count": stream_artifact_count,
                        "stream_checkpoint_count": stream_checkpoint_count,
                        "event_count": document_state.event_count,
                    },
                    "updated_at": now,
                }
            )
        )

    async def _reconcile_run_documents(self, run_id: str) -> None:
        for document_id in await self.store.list_documents(run_id=run_id):
            pipeline_id = await self.store.get_document_pipeline_id(
                run_id=run_id,
                document_id=document_id,
            )
            if not pipeline_id:
                continue
            try:
                pipeline = self.registry.get(pipeline_id)
            except Exception:
                continue
            await PipelineScheduler(pipeline, self.store).schedule_ready(
                run_id=run_id,
                document_id=document_id,
            )

    async def initialize_document(
        self,
        *,
        run_id: str,
        document_id: str,
        pipeline_id: str,
        values: dict | None = None,
        artifacts: list[ArtifactRef] | None = None,
        scheduled_at: datetime | None = None,
    ) -> ScheduleResult:
        await self.ensure_run(run_id)
        pipeline = self.registry.get(pipeline_id)
        normalized_scheduled_at = (
            _ensure_aware_utc(scheduled_at) if scheduled_at is not None else None
        )
        existing_document = await self.store.get_document(
            run_id=run_id,
            document_id=document_id,
        )
        if existing_document is None:
            await self.store.put_document(
                RuntimeDocument(
                    run_id=run_id,
                    document_id=document_id,
                    pipeline_id=pipeline_id,
                    scheduled_at=normalized_scheduled_at,
                )
            )
        else:
            document_updates: dict[str, Any] = {}
            if existing_document.pipeline_id != pipeline_id:
                document_updates["pipeline_id"] = pipeline_id
            if (
                normalized_scheduled_at is not None
                and existing_document.scheduled_at != normalized_scheduled_at
            ):
                document_updates["scheduled_at"] = normalized_scheduled_at
            if document_updates:
                document_updates["updated_at"] = datetime.now(timezone.utc)
                await self.store.put_document(
                    existing_document.model_copy(update=document_updates)
                )
        result = await PipelineScheduler(pipeline, self.store).initialize_document(
            run_id=run_id,
            document_id=document_id,
            values=values or {},
            artifacts=artifacts or [],
            scheduled_at=normalized_scheduled_at,
        )
        await self.sync_run_lifecycle(run_id)
        return result

    async def schedule_document(
        self,
        *,
        run_id: str,
        document_id: str,
        pipeline_id: str | None = None,
    ) -> ScheduleResult:
        pipeline = await self.resolve_document_pipeline(
            run_id=run_id,
            document_id=document_id,
            pipeline_id=pipeline_id,
        )
        result = await PipelineScheduler(pipeline, self.store).schedule_ready(
            run_id=run_id,
            document_id=document_id,
        )
        await self.sync_run_lifecycle(run_id)
        return result

    async def claim_next(
        self,
        *,
        run_id: str,
        pipeline_id: str,
        worker_id: str | None = None,
        process_id: str | None = None,
        adapter_kind: str | None = None,
        capabilities: list[str] | None = None,
        resources: ResourceSpec | dict[str, Any] | None = None,
        lease_seconds: float = 300.0,
    ) -> ClaimedProcess | None:
        pipeline = self.registry.get(pipeline_id)
        run = await self.ensure_run(run_id)
        if not await self._run_accepts_claims(run_id):
            return None
        document_ids = await self.store.list_documents(run_id=run_id)
        await self._schedule_ready_for_run(run_id, document_ids=document_ids)
        claim_document_ids = [
            document_id
            for document_id in document_ids
            if await self.store.get_document_pipeline_id(
                run_id=run_id,
                document_id=document_id,
            )
            == pipeline_id
        ]
        resource_pools = _run_resource_pools(run.config)
        resource_usage = await self._resource_pool_usage(run_id)
        claim = await PipelineScheduler(pipeline, self.store).claim_next(
            run_id=run_id,
            document_ids=claim_document_ids,
            worker_id=worker_id,
            process_id=process_id,
            adapter_kind=adapter_kind,
            capabilities=capabilities,
            resources=resources,
            resource_pools=resource_pools,
            resource_pool_usage=resource_usage,
            lease_seconds=lease_seconds,
        )
        await self.sync_run_lifecycle(run_id)
        return claim

    async def _schedule_ready_for_run(
        self,
        run_id: str,
        *,
        document_ids: list[str] | None = None,
    ) -> None:
        for document_id in document_ids or await self.store.list_documents(run_id=run_id):
            pipeline_id = await self.store.get_document_pipeline_id(
                run_id=run_id,
                document_id=document_id,
            )
            if pipeline_id is None:
                continue
            try:
                pipeline = self.registry.get(pipeline_id)
            except Exception:
                continue
            await PipelineScheduler(pipeline, self.store).schedule_ready(
                run_id=run_id,
                document_id=document_id,
            )

    async def _run_accepts_claims(self, run_id: str) -> bool:
        run = await self.store.get_run(run_id)
        if run is None:
            return True
        return run.status not in {
            RunStatus.paused,
            RunStatus.cancelled,
            RunStatus.completed,
            RunStatus.failed,
        }

    async def _ensure_run_accepts_process_writes(self, run_id: str) -> None:
        run = await self.store.get_run(run_id)
        if run is None:
            return
        if run.status in {RunStatus.cancelled, RunStatus.completed, RunStatus.failed}:
            raise ValueError(
                f"Run {run_id!r} is terminal and cannot accept process writes"
            )

    async def record_worker_heartbeat(
        self,
        *,
        run_id: str,
        worker_id: str,
        pipeline_id: str | None = None,
        process_id: str | None = None,
        adapter_kind: str | None = None,
        capabilities: list[str] | None = None,
        resources: ResourceSpec | dict[str, Any] | None = None,
        status: RuntimeWorkerStatus | str = RuntimeWorkerStatus.idle,
        current_document_id: str | None = None,
        current_process_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeWorkerHeartbeat:
        await self.ensure_run(run_id)
        if pipeline_id is not None:
            self.registry.get(pipeline_id)
        now = datetime.now(timezone.utc)
        previous = next(
            (
                heartbeat
                for heartbeat in await self.store.list_worker_heartbeats(run_id=run_id)
                if heartbeat.worker_id == worker_id
            ),
            None,
        )
        heartbeat = RuntimeWorkerHeartbeat(
            run_id=run_id,
            worker_id=worker_id,
            pipeline_id=pipeline_id,
            process_id=process_id,
            adapter_kind=adapter_kind,  # type: ignore[arg-type]
            capabilities=capabilities or [],
            resources=ResourceSpec.model_validate(resources or {}),
            status=RuntimeWorkerStatus(status),
            current_document_id=current_document_id,
            current_process_id=current_process_id,
            started_at=(previous.started_at if previous else None) or now,
            last_seen_at=now,
            metadata=metadata or {},
        )
        await self.store.put_worker_heartbeat(heartbeat)
        return heartbeat

    async def worker_health(
        self,
        run_id: str,
        *,
        stale_after_seconds: float = 60.0,
    ) -> list[RuntimeWorkerState]:
        if stale_after_seconds <= 0:
            raise ValueError("stale_after_seconds must be greater than zero")
        now = datetime.now(timezone.utc)
        states = [
            _worker_state(
                heartbeat,
                now=now,
                stale_after_seconds=stale_after_seconds,
            )
            for heartbeat in await self.store.list_worker_heartbeats(run_id=run_id)
        ]
        states.sort(key=lambda item: (not item.healthy, item.worker_id))
        return states

    async def _resource_pool_usage(self, run_id: str) -> dict[str, ResourceQuantity]:
        state = await self.load_state_model(run_id, include_events=False)
        usage, _running_counts, _queued_counts = _resource_pool_usage_from_state(state)
        return usage

    async def renew_claim(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        pipeline_id: str | None = None,
        worker_id: str | None = None,
        lease_seconds: float = 300.0,
    ) -> ProcessClaim | None:
        pipeline = await self.resolve_document_pipeline(
            run_id=run_id,
            document_id=document_id,
            pipeline_id=pipeline_id,
        )
        return await PipelineScheduler(pipeline, self.store).renew_claim(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            worker_id=worker_id,
            lease_seconds=lease_seconds,
        )

    async def control_process(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        pipeline_id: str | None = None,
        action: ProcessAction | str,
        reason: str | None = None,
        allow_contract_drift: bool = False,
    ) -> ProcessControlResult:
        action = ProcessAction(action)
        if action == ProcessAction.retry:
            await self.ensure_run_contract_current(
                run_id,
                allow_contract_drift=allow_contract_drift,
            )
        pipeline = await self.resolve_document_pipeline(
            run_id=run_id,
            document_id=document_id,
            pipeline_id=pipeline_id,
        )
        scheduler = PipelineScheduler(pipeline, self.store)
        if action == ProcessAction.retry:
            result = await scheduler.retry_process(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                reason=reason,
            )
            await self.sync_run_lifecycle(run_id)
            return result
        if action == ProcessAction.skip:
            result = await scheduler.skip_process(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                reason=reason,
            )
            await self.refresh_projections_for_process(
                pipeline=pipeline,
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
            )
            await self.sync_run_lifecycle(run_id)
            return result
        if action == ProcessAction.fail:
            result = await scheduler.fail_process(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                reason=reason,
            )
            await self.sync_run_lifecycle(run_id)
            return result
        if action == ProcessAction.cancel:
            result = await scheduler.cancel_process(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                reason=reason,
            )
            await self.sync_run_lifecycle(run_id)
            return result
        raise ValueError(f"Unknown process action: {action}")

    async def ensure_run_contract_current(
        self,
        run_id: str,
        *,
        allow_contract_drift: bool = False,
    ) -> None:
        if allow_contract_drift:
            return
        provenance = await self.run_provenance(run_id)
        drift = provenance.get("contract_drift")
        if not isinstance(drift, dict) or not drift.get("drifted"):
            return
        changed = ", ".join(drift.get("changed_pipeline_ids") or [])
        missing = ", ".join(drift.get("missing_pipeline_ids") or [])
        details = "; ".join(
            item
            for item in [
                f"changed pipelines: {changed}" if changed else "",
                f"missing pipelines: {missing}" if missing else "",
            ]
            if item
        )
        suffix = f" ({details})" if details else ""
        raise ValueError(
            f"Run {run_id!r} contract drift detected{suffix}; "
            "pass allow_contract_drift=True to override"
        )

    async def resolve_document_pipeline(
        self,
        *,
        run_id: str,
        document_id: str,
        pipeline_id: str | None = None,
    ) -> PipelineSpec:
        resolved_pipeline_id = pipeline_id or await self.store.get_document_pipeline_id(
            run_id=run_id,
            document_id=document_id,
        )
        if not resolved_pipeline_id:
            raise ValueError(
                f"Pipeline id required for document {document_id!r}; initialize document first"
            )
        return self.registry.get(resolved_pipeline_id)

    async def refresh_projections_for_process(
        self,
        *,
        pipeline: PipelineSpec,
        run_id: str,
        document_id: str,
        process_id: str,
    ) -> list[CombinedProjection]:
        refreshed: list[CombinedProjection] = []
        for combine in pipeline.combines:
            if process_id not in combine.needs:
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
            refreshed.append(projection)
        return refreshed

    async def resolve_artifact_path(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        artifact_id: str,
    ) -> Path:
        output = await self.store.get_output(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        if output is None:
            raise FileNotFoundError("Process output not found")

        artifact = next((item for item in output.artifacts if item.id == artifact_id), None)
        if artifact is None:
            raise FileNotFoundError("Runtime artifact not found")

        return self._resolve_artifact_ref_path(artifact)

    async def resolve_stream_artifact_path(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        stream_id: str,
        chunk_id: str,
        artifact_id: str,
    ) -> Path:
        chunks = await self.store.list_stream_chunks(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            stream_id=stream_id,
        )
        chunk = next((item for item in chunks if item.chunk_id == chunk_id), None)
        if chunk is None:
            raise FileNotFoundError("Runtime stream chunk not found")
        artifact = next((item for item in chunk.artifacts if item.id == artifact_id), None)
        if artifact is None:
            raise FileNotFoundError("Runtime stream artifact not found")
        return self._resolve_artifact_ref_path(artifact)

    def _resolve_artifact_ref_path(self, artifact: ArtifactRef) -> Path:
        if is_fala_artifact_uri(artifact.uri):
            return self.artifact_store.resolve(artifact)

        uri = urlparse(artifact.uri)
        if uri.scheme != "file":
            raise ValueError("Only local file runtime artifacts can be downloaded")
        path = Path(unquote(uri.path)).resolve()
        if not path.exists() or not path.is_file():
            raise FileNotFoundError("Runtime artifact file not found")
        roots = self._runtime_artifact_roots()
        if not any(_is_relative_to(path, root) for root in roots):
            raise PermissionError("Runtime artifact is outside allowed artifact roots")
        return path

    def _open_artifact_ref(self, artifact: ArtifactRef) -> BinaryIO:
        if is_fala_artifact_uri(artifact.uri):
            return self.artifact_store.open(artifact)

        uri = urlparse(artifact.uri)
        if uri.scheme != "file":
            raise ValueError("Only local file runtime artifacts can be opened")
        path = self._resolve_artifact_ref_path(artifact)
        return path.open("rb")

    async def put_process_output(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        output: ProcessOutput,
        pipeline_id: str | None = None,
        worker_id: str | None = None,
        spawned_documents: list[RuntimeDocumentInput] | None = None,
    ) -> ProcessOutput:
        await self._ensure_run_accepts_process_writes(run_id)
        await self.validate_process_output(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            output=output,
            pipeline_id=pipeline_id,
            spawned_documents=spawned_documents,
        )
        stream_chunks = [
            chunk.model_copy(
                update={
                    "artifacts": self.materialize_output_artifacts(
                        ProcessOutput(artifacts=chunk.artifacts)
                    ).artifacts
                }
            )
            for chunk in output.stream_chunks
        ]
        for chunk in stream_chunks:
            await self.validate_stream_chunk(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                stream_id=chunk.stream_id,
                kind=chunk.kind,
                values=chunk.values,
                artifacts=chunk.artifacts,
                metadata=chunk.metadata,
                pipeline_id=pipeline_id,
            )
        output = output.model_copy(update={"stream_chunks": []})
        output = await self._output_with_lineage(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            output=output,
            pipeline_id=pipeline_id,
            worker_id=worker_id,
        )
        materialized = self.materialize_output_artifacts(output)
        await self.store.put_output(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            output=materialized,
        )
        for chunk in stream_chunks:
            await self.append_stream_chunk(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                pipeline_id=pipeline_id,
                stream_id=chunk.stream_id,
                sequence=chunk.sequence,
                kind=chunk.kind,
                values=chunk.values,
                artifacts=chunk.artifacts,
                metadata=chunk.metadata,
            )
        return materialized

    async def complete_process_output(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        output: ProcessOutput,
        pipeline_id: str | None = None,
        worker_id: str | None = None,
    ) -> tuple[
        ProcessOutput,
        list[CombinedProjection],
        ScheduleResult,
        list[ScheduleResult],
    ]:
        pipeline = await self.resolve_document_pipeline(
            run_id=run_id,
            document_id=document_id,
            pipeline_id=pipeline_id,
        )
        spawned_documents, spawn_route_report = self._prepare_spawned_documents(
            parent_document_id=document_id,
            parent_process_id=process_id,
            parent_pipeline_id=pipeline.id,
            output=output,
        )
        if spawn_route_report is not None:
            output = _process_output_with_spawn_route_report(
                output,
                spawn_route_report,
            )
        output = await self.put_process_output(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            output=output,
            pipeline_id=pipeline.id,
            worker_id=worker_id,
            spawned_documents=spawned_documents,
        )
        await self.store.set_status(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            status=ProcessStatus.completed,
            **_process_metadata_args(pipeline.id, _step_by_id(pipeline, process_id)),
        )
        await self.store.clear_claim(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        await self.store.append_event(
            ProcessEvent(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                type="process.output",
                status=ProcessStatus.completed,
                data={
                    "worker_id": worker_id,
                    "artifact_count": len(output.artifacts),
                    "value_keys": sorted(output.values),
                },
            )
        )
        refreshed = await self.refresh_projections_for_process(
            pipeline=pipeline,
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        spawned_schedules = await self._spawn_output_documents(
            run_id=run_id,
            parent_document_id=document_id,
            parent_process_id=process_id,
            documents=spawned_documents,
            route_report=spawn_route_report,
        )
        schedule = await PipelineScheduler(pipeline, self.store).schedule_ready(
            run_id=run_id,
            document_id=document_id,
        )
        await self.sync_run_lifecycle(run_id)
        return output, refreshed, schedule, spawned_schedules

    def _prepare_spawned_documents(
        self,
        *,
        parent_document_id: str,
        parent_process_id: str,
        parent_pipeline_id: str,
        output: ProcessOutput,
    ) -> tuple[list[RuntimeDocumentInput], dict[str, Any] | None]:
        if not output.spawn_documents:
            return [], None
        documents = [
            _runtime_document_input_from_spawn(
                spawn,
                parent_document_id=parent_document_id,
                parent_process_id=parent_process_id,
            )
            for spawn in output.spawn_documents
        ]
        route_report: dict[str, Any] | None = None
        if any(
            document.pipeline_id is None or document.document_type is None
            for document in documents
        ):
            documents, route_report = self._route_spawned_documents(
                documents,
                parent_pipeline_id=parent_pipeline_id,
                emitted_document_types=self._emitted_document_types_for_process(
                    parent_pipeline_id,
                    parent_process_id,
                ),
            )
        for document in documents:
            resolved_pipeline_id = document.pipeline_id or parent_pipeline_id
            self.validate_runtime_document_input(
                pipeline_id=resolved_pipeline_id,
                document=document,
            )
        return documents, route_report

    def _route_spawned_documents(
        self,
        documents: list[RuntimeDocumentInput],
        *,
        parent_pipeline_id: str,
        emitted_document_types: set[str],
    ) -> tuple[list[RuntimeDocumentInput], dict[str, Any]]:
        auto_routes = [
            route
            for route in auto_document_routes_from_registry(self.registry)
            if (
                not emitted_document_types
                or route.get("document_type") in emitted_document_types
            )
        ]
        routed, report = route_runtime_documents_with_report(
            documents,
            auto_routes=auto_routes,
        )
        final_documents: list[RuntimeDocumentInput] = []
        fallback_count = 0
        for document, decision in zip(routed, report["documents"], strict=True):
            final_document = document
            fallback_pipeline_id = None
            if final_document.pipeline_id is None:
                final_document = final_document.model_copy(
                    update={"pipeline_id": parent_pipeline_id}
                )
                fallback_pipeline_id = parent_pipeline_id
                fallback_count += 1
            final_documents.append(final_document)
            decision["final"] = _runtime_document_input_route_summary(final_document)
            if fallback_pipeline_id is not None:
                decision["fallback_pipeline_id"] = fallback_pipeline_id

        report["auto_route_count"] = len(auto_routes)
        report["fallback_pipeline_id"] = parent_pipeline_id
        report["fallback_pipeline_count"] = fallback_count
        report["final_missing_pipeline_count"] = sum(
            1 for document in final_documents if document.pipeline_id is None
        )
        return final_documents, report

    def _emitted_document_types_for_process(
        self,
        pipeline_id: str,
        process_id: str,
    ) -> set[str]:
        package_id = self.registry.pipeline_package_id(pipeline_id)
        if package_id is None:
            return set()
        pipeline = self.registry.get(pipeline_id)
        step = next((item for item in pipeline.steps if item.id == process_id), None)
        if step is None or step.capability is None:
            return set()
        package = self.registry.package(package_id)
        capability = next(
            (item for item in package.capabilities if item.id == step.capability),
            None,
        )
        return (
            set(capability.emits_document_types)
            if capability is not None
            else set()
        )

    async def _spawn_output_documents(
        self,
        *,
        run_id: str,
        parent_document_id: str,
        parent_process_id: str,
        documents: list[RuntimeDocumentInput],
        route_report: dict[str, Any] | None = None,
    ) -> list[ScheduleResult]:
        if not documents:
            return []
        relation_by_document_id = {
            document.document_id: document.relation for document in documents
        }
        schedules = await self.initialize_documents(
            run_id=run_id,
            documents=documents,
            existing_document_policy="error",
        )
        for schedule in schedules:
            await self.store.append_event(
                ProcessEvent(
                    run_id=run_id,
                    document_id=schedule.document_id,
                    process_id=None,
                    type="document.spawned",
                    data={
                        "parent_document_id": parent_document_id,
                        "parent_process_id": parent_process_id,
                        "relation": relation_by_document_id.get(schedule.document_id),
                        "pipeline_id": schedule.pipeline_id,
                        "route": _spawn_route_for_document(
                            route_report,
                            schedule.document_id,
                        ),
                    },
                )
            )
        return schedules

    async def _output_with_lineage(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        output: ProcessOutput,
        pipeline_id: str | None = None,
        worker_id: str | None = None,
    ) -> ProcessOutput:
        pipeline = await self.resolve_document_pipeline(
            run_id=run_id,
            document_id=document_id,
            pipeline_id=pipeline_id,
        )
        step = _require_step(pipeline, process_id)
        base_input = await self.store.get_document_input(
            run_id=run_id,
            document_id=document_id,
        )
        if base_input is None:
            base_input = ProcessInput()
        claim = await self.store.get_claim(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        need_outputs = (
            await self.store.get_outputs(
                run_id=run_id,
                document_id=document_id,
                process_ids=step.needs,
            )
            if step.needs
            else {}
        )
        artifacts = list(base_input.artifacts)
        needs: dict[str, dict[str, Any]] = {}
        for dep in step.needs:
            need_output = need_outputs.get(dep)
            if need_output is None:
                continue
            artifacts.extend(need_output.artifacts)
            needs[dep] = need_output.values
        context = ProcessExecutionContext(
            pipeline_id=pipeline.id,
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            capability=step.capability,
            attempt=claim.attempt if claim is not None else 1,
            input=ProcessInput(
                artifacts=artifacts,
                values={
                    "initial": base_input.values,
                    "needs": needs,
                },
            ),
            config=step.config,
        )
        return output_with_lineage(
            output,
            context=context,
            step=step,
            need_outputs=need_outputs,
            worker_id=worker_id or (claim.worker_id if claim is not None else None),
        )

    async def append_stream_chunk(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        pipeline_id: str | None = None,
        stream_id: str = "main",
        sequence: int | None = None,
        kind: str | None = None,
        values: dict[str, Any] | None = None,
        artifacts: list[ArtifactRef] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeStreamChunk:
        await self._ensure_run_accepts_process_writes(run_id)
        if sequence is None:
            existing = await self.store.list_stream_chunks(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                stream_id=stream_id,
            )
            sequence = existing[-1].sequence + 1 if existing else 0

        materialized = self.materialize_output_artifacts(
            ProcessOutput(artifacts=artifacts or [])
        )
        await self.validate_stream_chunk(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            stream_id=stream_id,
            kind=kind,
            values=values or {},
            artifacts=materialized.artifacts,
            metadata=metadata or {},
            pipeline_id=pipeline_id,
        )
        await self._enforce_stream_backpressure(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            stream_id=stream_id,
            sequence=sequence,
            kind=kind,
            pipeline_id=pipeline_id,
        )
        chunk = RuntimeStreamChunk(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            stream_id=stream_id,
            sequence=sequence,
            kind=kind,
            values=values or {},
            artifacts=materialized.artifacts,
            metadata=metadata or {},
        )
        await self.store.put_stream_chunk(chunk)
        await self.store.append_event(
            ProcessEvent(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                type="process.stream.chunk",
                data={
                    "stream_id": stream_id,
                    "chunk_id": chunk.chunk_id,
                    "sequence": sequence,
                    "kind": kind,
                    "artifact_count": len(chunk.artifacts),
                    "value_keys": sorted(chunk.values),
                },
            )
        )
        return chunk

    async def _enforce_stream_backpressure(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        stream_id: str,
        sequence: int,
        kind: str | None,
        pipeline_id: str | None = None,
    ) -> None:
        limit = await self._stream_backpressure_limit(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            stream_id=stream_id,
            kind=kind,
            pipeline_id=pipeline_id,
        )
        if limit is None:
            return

        checkpoints = await self.store.list_stream_checkpoints(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            stream_id=stream_id,
        )
        slowest_checkpoint = (
            min(checkpoint.sequence for checkpoint in checkpoints)
            if checkpoints
            else -1
        )
        chunks = await self.store.list_stream_chunks(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            stream_id=stream_id,
        )
        candidate_sequences = {
            chunk.sequence for chunk in chunks
        }
        candidate_sequences.add(sequence)
        buffered_count = sum(
            1 for item in candidate_sequences if item > slowest_checkpoint
        )
        if buffered_count > limit:
            raise ValueError(
                f"Process {process_id!r} stream {stream_id!r} backpressure limit "
                f"exceeded: {buffered_count} buffered chunks over slowest checkpoint "
                f"sequence {slowest_checkpoint}; max_buffered_chunks={limit}"
            )

    async def _stream_backpressure_limit(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        stream_id: str,
        kind: str | None,
        pipeline_id: str | None = None,
    ) -> int | None:
        selected_streams = await self._matching_stream_contracts(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            stream_id=stream_id,
            kind=kind,
            pipeline_id=pipeline_id,
            validate=False,
        )
        limits = [
            stream.max_buffered_chunks
            for stream in selected_streams
            if stream.max_buffered_chunks is not None
        ]
        return min(limits) if limits else None

    def _stream_max_buffered_chunks_for_step(
        self,
        *,
        pipeline_id: str | None,
        process_id: str,
        stream_id: str,
    ) -> int | None:
        if pipeline_id is None:
            return None
        package_id = self.registry.pipeline_package_id(pipeline_id)
        if package_id is None:
            return None
        try:
            package = self.registry.package(package_id)
            pipeline = self.registry.get(pipeline_id)
        except Exception:
            return None
        step = next((item for item in pipeline.steps if item.id == process_id), None)
        if step is None or step.capability is None:
            return None
        capability = {
            item.id: item for item in package.capabilities
        }.get(step.capability)
        if capability is None:
            return None
        limits = [
            stream.max_buffered_chunks
            for stream in capability.emits_streams
            if stream.stream_id == stream_id
            and stream.max_buffered_chunks is not None
        ]
        return min(limits) if limits else None

    def _declared_stream_consumers_for_pipeline(
        self,
        pipeline: PipelineSpec | None,
    ) -> dict[tuple[str, str], list[str]]:
        if pipeline is None:
            return {}
        package_id = self.registry.pipeline_package_id(pipeline.id)
        if package_id is None:
            return {}
        try:
            package = self.registry.package(package_id)
        except Exception:
            return {}
        capabilities = {item.id: item for item in package.capabilities}
        consumers: dict[tuple[str, str], set[str]] = {}
        for step in pipeline.steps:
            if step.capability is None:
                continue
            capability = capabilities.get(step.capability)
            if capability is None:
                continue
            for stream in capability.emits_streams:
                if not stream.consumers:
                    continue
                consumers.setdefault((step.id, stream.stream_id), set()).update(
                    stream.consumers
                )
        return {key: sorted(value) for key, value in consumers.items()}

    async def list_stream_chunks(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str | None = None,
        stream_id: str | None = None,
        after_sequence: int | None = None,
        limit: int | None = None,
    ) -> list[RuntimeStreamChunk]:
        return await self.store.list_stream_chunks(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            stream_id=stream_id,
            after_sequence=after_sequence,
            limit=limit,
        )

    async def list_stream_checkpoints(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str | None = None,
        stream_id: str | None = None,
        consumer_id: str | None = None,
    ) -> list[RuntimeStreamCheckpoint]:
        return await self.store.list_stream_checkpoints(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            stream_id=stream_id,
            consumer_id=consumer_id,
        )

    async def put_stream_checkpoint(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        stream_id: str = "main",
        consumer_id: str = "default",
        sequence: int = -1,
        chunk_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeStreamCheckpoint:
        await self._ensure_run_accepts_process_writes(run_id)
        checkpoint = RuntimeStreamCheckpoint(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            stream_id=stream_id,
            consumer_id=consumer_id,
            sequence=sequence,
            chunk_id=chunk_id,
            metadata=metadata or {},
        )
        await self.store.put_stream_checkpoint(checkpoint)
        await self.store.append_event(
            ProcessEvent(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                type="process.stream.checkpoint",
                data={
                    "stream_id": stream_id,
                    "consumer_id": consumer_id,
                    "sequence": sequence,
                    "chunk_id": chunk_id,
                },
            )
        )
        return checkpoint

    async def get_stream_checkpoint(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        stream_id: str = "main",
        consumer_id: str = "default",
    ) -> RuntimeStreamCheckpoint | None:
        return await self.store.get_stream_checkpoint(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            stream_id=stream_id,
            consumer_id=consumer_id,
        )

    async def validate_stream_chunk(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        stream_id: str,
        kind: str | None,
        values: dict[str, Any],
        artifacts: list[ArtifactRef],
        metadata: dict[str, Any],
        pipeline_id: str | None = None,
    ) -> None:
        selected_streams = await self._matching_stream_contracts(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            stream_id=stream_id,
            kind=kind,
            pipeline_id=pipeline_id,
        )
        package_id = self._stream_package_id(pipeline_id)
        if package_id is None:
            resolved_pipeline_id = (
                pipeline_id
                or await self.store.get_document_pipeline_id(
                    run_id=run_id,
                    document_id=document_id,
                )
            )
            package_id = (
                self.registry.pipeline_package_id(resolved_pipeline_id)
                if resolved_pipeline_id
                else None
            )
        if package_id is None:
            return
        package = self.registry.package(package_id)
        resolved_pipeline_id = (
            pipeline_id
            or await self.store.get_document_pipeline_id(
                run_id=run_id,
                document_id=document_id,
            )
        )
        capability_id = self._stream_capability_id(
            pipeline_id=resolved_pipeline_id,
            process_id=process_id,
        )
        for stream in selected_streams:
            validate_json_value(
                values,
                stream.value_schema,
                label=(
                    f"Process {process_id!r} stream {stream_id!r} "
                    f"values for capability {capability_id!r} value_schema"
                ),
            )
            validate_json_value(
                metadata,
                stream.metadata_schema,
                label=(
                    f"Process {process_id!r} stream {stream_id!r} "
                    f"metadata for capability {capability_id!r} metadata_schema"
                ),
            )

        artifact_kinds = {item.id: item for item in package.artifact_kinds}
        artifact_kind_ids = set(artifact_kinds)
        emitted_stream_artifact_kinds = {
            artifact_kind
            for stream in selected_streams
            for artifact_kind in stream.emits_artifact_kinds
        }
        for artifact in artifacts:
            if artifact_kind_ids and artifact.kind not in artifact_kind_ids:
                declared = ", ".join(sorted(artifact_kind_ids))
                raise ValueError(
                    f"Process {process_id!r} stream {stream_id!r} artifact "
                    f"{artifact.id!r} kind {artifact.kind!r} is not declared by "
                    f"workflow package {package_id!r} (declared: {declared})"
                )
            if selected_streams and not emitted_stream_artifact_kinds:
                raise ValueError(
                    f"Process {process_id!r} stream {stream_id!r} capability "
                    f"{capability_id!r} does not declare emitted artifact kinds"
                )
            if (
                selected_streams
                and emitted_stream_artifact_kinds
                and artifact.kind not in emitted_stream_artifact_kinds
            ):
                emitted = ", ".join(sorted(emitted_stream_artifact_kinds))
                raise ValueError(
                    f"Process {process_id!r} stream {stream_id!r} artifact "
                    f"{artifact.id!r} kind {artifact.kind!r} is not emitted by "
                    f"capability {capability_id!r} stream {stream_id!r} "
                    f"(emitted: {emitted})"
                )
            artifact_kind = artifact_kinds.get(artifact.kind)
            if artifact_kind is not None:
                validate_json_value(
                    artifact.metadata,
                    artifact_kind.metadata_schema,
                    label=(
                        f"Process {process_id!r} stream {stream_id!r} artifact "
                        f"{artifact.id!r} metadata for kind {artifact.kind!r} "
                        "metadata_schema"
                    ),
                )
                _validate_artifact_media_type(
                    artifact,
                    media_types=artifact_kind.media_types,
                    process_id=process_id,
                )
                _validate_artifact_extension(
                    artifact,
                    extensions=artifact_kind.extensions,
                    process_id=process_id,
                )
                self._validate_artifact_value(
                    artifact,
                    value_schema=artifact_kind.value_schema,
                    process_id=process_id,
                )

    async def _matching_stream_contracts(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        stream_id: str,
        kind: str | None,
        pipeline_id: str | None = None,
        validate: bool = True,
    ) -> list[StreamSpec]:
        resolved_pipeline_id = pipeline_id or await self.store.get_document_pipeline_id(
            run_id=run_id,
            document_id=document_id,
        )
        if not resolved_pipeline_id:
            return []
        package_id = self.registry.pipeline_package_id(resolved_pipeline_id)
        if package_id is None:
            return []

        package = self.registry.package(package_id)
        pipeline = self.registry.get(resolved_pipeline_id)
        step = next((item for item in pipeline.steps if item.id == process_id), None)
        if step is None:
            raise ValueError(
                f"Pipeline {resolved_pipeline_id!r} has no process {process_id!r}"
            )
        if step.capability is None:
            return []
        capabilities = {item.id: item for item in package.capabilities}
        capability = capabilities.get(step.capability)
        if capability is None:
            raise ValueError(
                f"Process {process_id!r} references unknown capability "
                f"{step.capability!r}"
            )

        stream_contracts = list(capability.emits_streams)
        if not stream_contracts:
            return []
        stream_matches = [
            stream for stream in stream_contracts if stream.stream_id == stream_id
        ]
        if not stream_matches:
            if not validate:
                return []
            declared = ", ".join(sorted({stream.stream_id for stream in stream_contracts}))
            raise ValueError(
                f"Process {process_id!r} capability {step.capability!r} "
                f"does not declare stream {stream_id!r} (declared: {declared})"
            )
        selected_streams = [
            stream
            for stream in stream_matches
            if not stream.kinds or (kind is not None and kind in stream.kinds)
        ]
        if not selected_streams:
            if not validate:
                return []
            declared_kinds = sorted(
                {item for stream in stream_matches for item in stream.kinds}
            )
            declared = ", ".join(declared_kinds) or "<any>"
            raise ValueError(
                f"Process {process_id!r} stream {stream_id!r} kind {kind!r} "
                f"is not emitted by capability {step.capability!r} "
                f"(emitted: {declared})"
            )
        specific_streams = [stream for stream in selected_streams if stream.kinds]
        return specific_streams or selected_streams

    def _stream_package_id(self, pipeline_id: str | None) -> str | None:
        if pipeline_id is None:
            return None
        return self.registry.pipeline_package_id(pipeline_id)

    def _stream_capability_id(
        self,
        *,
        pipeline_id: str | None,
        process_id: str,
    ) -> str | None:
        if pipeline_id is None:
            return None
        try:
            pipeline = self.registry.get(pipeline_id)
        except Exception:
            return None
        step = next((item for item in pipeline.steps if item.id == process_id), None)
        return step.capability if step is not None else None

    async def validate_process_output(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        output: ProcessOutput,
        pipeline_id: str | None = None,
        spawned_documents: list[RuntimeDocumentInput] | None = None,
    ) -> None:
        resolved_pipeline_id = pipeline_id or await self.store.get_document_pipeline_id(
            run_id=run_id,
            document_id=document_id,
        )
        if not resolved_pipeline_id:
            return
        package_id = self.registry.pipeline_package_id(resolved_pipeline_id)
        if package_id is None:
            return

        package = self.registry.package(package_id)
        pipeline = self.registry.get(resolved_pipeline_id)
        step = next((item for item in pipeline.steps if item.id == process_id), None)
        if step is None:
            raise ValueError(
                f"Pipeline {resolved_pipeline_id!r} has no process {process_id!r}"
            )
        if step.capability is None:
            return
        capabilities = {item.id: item for item in package.capabilities}
        capability = capabilities.get(step.capability)
        if capability is None:
            raise ValueError(
                f"Process {process_id!r} references unknown capability "
                f"{step.capability!r}"
            )
        validate_json_value(
            output.values,
            capability.output_schema,
            label=(
                f"Process {process_id!r} output values for capability "
                f"{step.capability!r} output_schema"
            ),
        )

        document_type_ids = {item.id for item in package.document_types}
        document_types = {item.id: item for item in package.document_types}
        document_relations = {item.id: item for item in package.document_relations}
        emitted_document_types = set(capability.emits_document_types)
        source_document = await self.store.get_document(
            run_id=run_id,
            document_id=document_id,
        )
        source_document_type = (
            source_document.document_type if source_document is not None else None
        )
        self._validate_output_documents(
            process_id=process_id,
            package_id=package_id,
            output_documents=output.output_documents,
            output_artifacts=output.artifacts,
            document_types=document_types,
            document_relations=document_relations,
            source_document_type=source_document_type,
            emitted_document_types=emitted_document_types,
        )
        spawned_documents_for_validation = (
            [
                (document.document_id, document.document_type, document.relation)
                for document in spawned_documents
            ]
            if spawned_documents is not None
            else [
                (spawn.document_id, spawn.document_type, spawn.relation)
                for spawn in output.spawn_documents
            ]
        )
        for spawn_document_id, spawn_document_type, spawn_relation in spawned_documents_for_validation:
            if (
                spawn_document_type is not None
                and document_type_ids
                and spawn_document_type not in document_type_ids
            ):
                declared = ", ".join(sorted(document_type_ids))
                raise ValueError(
                    f"Process {process_id!r} spawned document "
                    f"{spawn_document_id!r} type {spawn_document_type!r} "
                    f"is not declared by workflow package {package_id!r} "
                    f"(declared: {declared})"
                )
            self._validate_document_relation(
                process_id=process_id,
                package_id=package_id,
                relation=spawn_relation,
                source_document_type=source_document_type,
                target_document_type=spawn_document_type,
                document_relations=document_relations,
                label=f"spawned document {spawn_document_id!r}",
            )
            if not emitted_document_types:
                continue
            if spawn_document_type is None:
                emitted = ", ".join(sorted(emitted_document_types))
                raise ValueError(
                    f"Process {process_id!r} spawned document "
                    f"{spawn_document_id!r} requires document_type because "
                    f"capability {step.capability!r} declares emitted "
                    f"document types (emitted: {emitted})"
                )
            if spawn_document_type not in emitted_document_types:
                emitted = ", ".join(sorted(emitted_document_types))
                raise ValueError(
                    f"Process {process_id!r} spawned document "
                    f"{spawn_document_id!r} type {spawn_document_type!r} "
                    f"is not emitted by capability {step.capability!r} "
                    f"(emitted: {emitted})"
                )

        artifact_kinds = {item.id: item for item in package.artifact_kinds}
        artifact_kind_ids = set(artifact_kinds)
        emitted_artifact_kinds = set(capability.emits_artifact_kinds)
        for artifact in output.artifacts:
            if artifact_kind_ids and artifact.kind not in artifact_kind_ids:
                declared = ", ".join(sorted(artifact_kind_ids))
                raise ValueError(
                    f"Process {process_id!r} output artifact {artifact.id!r} "
                    f"kind {artifact.kind!r} is not declared by workflow package "
                    f"{package_id!r} (declared: {declared})"
                )
            if not emitted_artifact_kinds and artifact_kind_ids:
                raise ValueError(
                    f"Process {process_id!r} capability {step.capability!r} "
                    "does not declare emitted artifact kinds"
                )
            if emitted_artifact_kinds and artifact.kind not in emitted_artifact_kinds:
                emitted = ", ".join(sorted(emitted_artifact_kinds))
                raise ValueError(
                    f"Process {process_id!r} output artifact {artifact.id!r} "
                    f"kind {artifact.kind!r} is not emitted by capability "
                    f"{step.capability!r} (emitted: {emitted})"
                )
            artifact_kind = artifact_kinds.get(artifact.kind)
            if artifact_kind is not None:
                validate_json_value(
                    artifact.metadata,
                    artifact_kind.metadata_schema,
                    label=(
                        f"Process {process_id!r} output artifact {artifact.id!r} "
                        f"metadata for kind {artifact.kind!r} metadata_schema"
                    ),
                )
                _validate_artifact_media_type(
                    artifact,
                    media_types=artifact_kind.media_types,
                    process_id=process_id,
                )
                _validate_artifact_extension(
                    artifact,
                    extensions=artifact_kind.extensions,
                    process_id=process_id,
                )
                self._validate_artifact_value(
                    artifact,
                    value_schema=artifact_kind.value_schema,
                    process_id=process_id,
                )

    def _validate_output_documents(
        self,
        *,
        process_id: str,
        package_id: str,
        output_documents: list[OutputDocumentRef],
        output_artifacts: list[ArtifactRef],
        document_types: dict[str, Any],
        document_relations: dict[str, Any],
        source_document_type: str | None,
        emitted_document_types: set[str],
    ) -> None:
        if not output_documents:
            return
        artifact_ids = {artifact.id for artifact in output_artifacts}
        for document in output_documents:
            if document.document_type not in document_types:
                declared = ", ".join(sorted(document_types)) or "<none>"
                raise ValueError(
                    f"Process {process_id!r} output document {document.id!r} "
                    f"type {document.document_type!r} is not declared by workflow "
                    f"package {package_id!r} (declared: {declared})"
                )
            if not emitted_document_types and document_types:
                raise ValueError(
                    f"Process {process_id!r} output document {document.id!r} "
                    "requires capability to declare emitted document types"
                )
            if (
                emitted_document_types
                and document.document_type not in emitted_document_types
            ):
                emitted = ", ".join(sorted(emitted_document_types))
                raise ValueError(
                    f"Process {process_id!r} output document {document.id!r} "
                    f"type {document.document_type!r} is not emitted by capability "
                    f"(emitted: {emitted})"
                )
            if document.artifact_id is not None and document.artifact_id not in artifact_ids:
                raise ValueError(
                    f"Process {process_id!r} output document {document.id!r} "
                    f"references unknown output artifact {document.artifact_id!r}"
                )
            self._validate_document_relation(
                process_id=process_id,
                package_id=package_id,
                relation=document.relation,
                source_document_type=source_document_type,
                target_document_type=document.document_type,
                document_relations=document_relations,
                label=f"output document {document.id!r}",
            )
            document_type = document_types[document.document_type]
            validate_json_value(
                document.values,
                document_type.value_schema,
                label=(
                    f"Process {process_id!r} output document {document.id!r} "
                    f"values for type {document.document_type!r} value_schema"
                ),
            )
            validate_json_value(
                document.metadata,
                document_type.metadata_schema,
                label=(
                    f"Process {process_id!r} output document {document.id!r} "
                    f"metadata for type {document.document_type!r} metadata_schema"
                ),
            )
            _validate_output_document_media_type(
                document,
                media_types=document_type.media_types,
                process_id=process_id,
            )
            _validate_output_document_extension(
                document,
                extensions=document_type.extensions,
                process_id=process_id,
            )

    def _validate_document_relation(
        self,
        *,
        process_id: str,
        package_id: str,
        relation: str | None,
        source_document_type: str | None,
        target_document_type: str | None,
        document_relations: dict[str, Any],
        label: str,
    ) -> None:
        if relation is None:
            return
        if not document_relations:
            return
        relation_spec = document_relations.get(relation)
        if relation_spec is None:
            declared = ", ".join(sorted(document_relations)) or "<none>"
            raise ValueError(
                f"Process {process_id!r} {label} relation {relation!r} "
                f"is not declared by workflow package {package_id!r} "
                f"(declared: {declared})"
            )
        if (
            source_document_type is not None
            and relation_spec.source_document_types
            and source_document_type not in relation_spec.source_document_types
        ):
            declared = ", ".join(sorted(relation_spec.source_document_types))
            raise ValueError(
                f"Process {process_id!r} {label} relation {relation!r} "
                f"does not accept source document type {source_document_type!r} "
                f"(accepted: {declared})"
            )
        if (
            target_document_type is not None
            and relation_spec.target_document_types
            and target_document_type not in relation_spec.target_document_types
        ):
            declared = ", ".join(sorted(relation_spec.target_document_types))
            raise ValueError(
                f"Process {process_id!r} {label} relation {relation!r} "
                f"does not accept target document type {target_document_type!r} "
                f"(accepted: {declared})"
            )

    def _validate_artifact_value(
        self,
        artifact: ArtifactRef,
        *,
        value_schema: dict[str, Any],
        process_id: str,
    ) -> None:
        if not value_schema:
            return
        label = (
            f"Process {process_id!r} output artifact {artifact.id!r} "
            f"value for kind {artifact.kind!r} value_schema"
        )
        try:
            handle = self._open_artifact_ref(artifact)
        except (FileNotFoundError, PermissionError, ValueError) as exc:
            raise ValueError(f"{label} cannot be validated: {exc}") from exc
        try:
            with handle:
                with io.TextIOWrapper(handle, encoding="utf-8") as text_handle:
                    value = json.load(text_handle)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"{label} requires JSON artifact content: {exc.msg}"
            ) from exc
        except UnicodeDecodeError as exc:
            raise ValueError(
                f"{label} requires UTF-8 JSON artifact content"
            ) from exc
        validate_json_value(value, value_schema, label=label)

    def materialize_output_artifacts(self, output: ProcessOutput) -> ProcessOutput:
        roots = self._runtime_artifact_roots()
        artifacts: list[ArtifactRef] = []
        changed = False
        for artifact in output.artifacts:
            if is_fala_artifact_uri(artifact.uri):
                artifacts.append(artifact)
                continue
            path = local_path_from_uri(artifact.uri)
            if path is None or not path.exists() or not path.is_file():
                artifacts.append(artifact)
                continue
            if not any(_is_relative_to(path, root) for root in roots):
                artifacts.append(artifact)
                continue
            artifacts.append(
                self.artifact_store.put_file(
                    kind=artifact.kind,
                    path=path,
                    artifact_id=artifact.id,
                    metadata=artifact.metadata,
                )
            )
            changed = True
        if not changed:
            return output
        return output.model_copy(update={"artifacts": artifacts})

    async def load_state_model(
        self,
        run_id: str,
        *,
        include_events: bool = False,
    ) -> RuntimeState:
        documents = []
        for document_id in await self.store.list_documents(run_id=run_id):
            pipeline_id = await self.store.get_document_pipeline_id(
                run_id=run_id,
                document_id=document_id,
            )
            events = (
                await self.store.list_events(run_id=run_id, document_id=document_id)
                if include_events
                else []
            )
            if include_events:
                events = await self._events_with_operation_type(events)
            event_count = await self.store.count_events(
                run_id=run_id,
                document_id=document_id,
            )
            statuses = await self.store.list_statuses(
                run_id=run_id,
                document_id=document_id,
            )
            claims = await self.store.list_claims(
                run_id=run_id,
                document_id=document_id,
            )
            outputs = await self.store.list_outputs(
                run_id=run_id,
                document_id=document_id,
            )
            projections = await self.store.list_projections(
                run_id=run_id,
                document_id=document_id,
            )
            stream_chunks = await self.store.list_stream_chunks(
                run_id=run_id,
                document_id=document_id,
            )
            stream_checkpoints = await self.store.list_stream_checkpoints(
                run_id=run_id,
                document_id=document_id,
            )
            document = await self.store.get_document(
                run_id=run_id,
                document_id=document_id,
            )
            pipeline = None
            if pipeline_id:
                try:
                    pipeline = self.registry.get(pipeline_id)
                except Exception:
                    pipeline = None
            documents.append(
                build_runtime_document_state(
                    document_id=document_id,
                    pipeline_id=pipeline_id,
                    pipeline=pipeline,
                    statuses=statuses,
                    claims=claims,
                    outputs=outputs,
                    projections=projections,
                    stream_chunks=stream_chunks,
                    stream_checkpoints=stream_checkpoints,
                    stream_declared_consumers=(
                        self._declared_stream_consumers_for_pipeline(pipeline)
                    ),
                    operation_type_by_step=(
                        self._operation_type_by_step_for_pipeline(pipeline)
                    ),
                    events=events,
                    event_count=event_count,
                    document=document,
                )
            )
        return build_runtime_state(run_id=run_id, documents=documents)

    async def load_state(
        self,
        run_id: str,
        *,
        include_events: bool = False,
    ) -> dict[str, Any]:
        state = await self.load_state_model(run_id, include_events=include_events)
        return state.model_dump(mode="json")

    async def step_report(self, run_id: str) -> RuntimeStepReport:
        state = await self.load_state_model(run_id, include_events=False)
        return build_runtime_step_report(state)

    async def document_registry(
        self,
        run_id: str,
        *,
        status: RuntimeDocumentStatus | None = None,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        relation: str | None = None,
        parent_document_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> RuntimeDocumentPage:
        if limit < 1:
            raise ValueError("limit must be greater than zero")
        if offset < 0:
            raise ValueError("offset must be greater than or equal to zero")
        documents = await self.store.list_document_records(
            run_id=run_id,
            status=status,
            pipeline_id=pipeline_id,
            document_type=document_type,
            relation=relation,
            parent_document_id=parent_document_id,
            limit=limit + 1,
            offset=offset,
        )
        page_documents = documents[:limit]
        filters = {
            key: value
            for key, value in {
                "status": status.value if status is not None else None,
                "pipeline_id": pipeline_id,
                "document_type": document_type,
                "relation": relation,
                "parent_document_id": parent_document_id,
            }.items()
            if value is not None
        }
        return RuntimeDocumentPage(
            run_id=run_id,
            count=len(page_documents),
            limit=limit,
            offset=offset,
            has_more=len(documents) > limit,
            filters=filters,
            documents=page_documents,
        )

    async def process_registry(
        self,
        run_id: str,
        *,
        status: ProcessStatus | None = None,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        parent_document_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
        capability: str | None = None,
        operation_type: str | None = None,
        adapter_kind: str | None = None,
        resource_pool: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> RuntimeProcessPage:
        if limit < 1:
            raise ValueError("limit must be greater than zero")
        if offset < 0:
            raise ValueError("offset must be greater than or equal to zero")
        keys = await self.store.list_process_record_keys(
            run_id=run_id,
            status=status,
            pipeline_id=pipeline_id,
            document_type=document_type,
            parent_document_id=parent_document_id,
            document_id=document_id,
            process_id=process_id,
            capability=capability,
            adapter_kind=adapter_kind,
            resource_pool=resource_pool,
            limit=None if operation_type is not None else limit + 1,
            offset=0 if operation_type is not None else offset,
        )
        records = [
            await self._runtime_process_record(row)
            for row in keys
        ]
        if operation_type is not None:
            records = [
                record
                for record in records
                if record.operation_type == operation_type
            ]
            page_records = records[offset : offset + limit]
            has_more = offset + limit < len(records)
        else:
            page_records = records[:limit]
            has_more = len(keys) > limit
        filters = {
            key: value
            for key, value in {
                "status": status.value if status is not None else None,
                "pipeline_id": pipeline_id,
                "document_type": document_type,
                "parent_document_id": parent_document_id,
                "document_id": document_id,
                "process_id": process_id,
                "capability": capability,
                "operation_type": operation_type,
                "adapter_kind": adapter_kind,
                "resource_pool": resource_pool,
            }.items()
            if value is not None
        }
        return RuntimeProcessPage(
            run_id=run_id,
            count=len(page_records),
            limit=limit,
            offset=offset,
            has_more=has_more,
            filters=filters,
            processes=page_records,
        )

    async def _runtime_process_record(
        self,
        row: dict[str, Any],
    ) -> RuntimeProcessRecord:
        run_id = str(row["run_id"])
        document_id = str(row["document_id"])
        process_id = str(row["process_id"])
        document = await self.store.get_document(
            run_id=run_id,
            document_id=document_id,
        )
        pipeline_id = (
            row.get("pipeline_id")
            or (document.pipeline_id if document is not None else None)
            or await self.store.get_document_pipeline_id(
                run_id=run_id,
                document_id=document_id,
            )
        )
        pipeline = None
        step = None
        if pipeline_id is not None:
            try:
                pipeline = self.registry.get(pipeline_id)
            except Exception:
                pipeline = None
        if pipeline is not None:
            step = next(
                (item for item in pipeline.steps if item.id == process_id),
                None,
            )
        output = await self.store.get_output(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        claim = await self.store.get_claim(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        chunks = await self.store.list_stream_chunks(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        checkpoints = await self.store.list_stream_checkpoints(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        raw_status = row.get("status")
        status = _process_status_or_none(raw_status)
        snapshot = runtime_step_snapshot(
            process_id=process_id,
            spec=step,
            statuses={process_id: status} if status is not None else {},
            claims={process_id: claim} if claim is not None else {},
            outputs={process_id: output} if output is not None else {},
            streams=runtime_stream_snapshots(
                chunks=chunks,
                checkpoints=checkpoints,
                declared_consumers=self._declared_stream_consumers_for_pipeline(
                    pipeline
                ),
            ).get(process_id, ()),
            operation_type=self._operation_type_for_step(pipeline, step),
        )
        return RuntimeProcessRecord(
            run_id=run_id,
            document_id=document_id,
            document_title=document.title if document is not None else None,
            document_type=document.document_type if document is not None else None,
            document_relation=document.relation if document is not None else None,
            parent_document_id=(
                document.parent_document_id if document is not None else None
            ),
            pipeline_id=pipeline_id,
            process_id=process_id,
            title=snapshot.title,
            capability=snapshot.capability or row.get("capability"),
            operation_type=snapshot.operation_type,
            adapter_kind=snapshot.adapter_kind or row.get("adapter_kind"),
            priority=snapshot.priority,
            max_concurrency=snapshot.max_concurrency,
            resource_pool=snapshot.resource_pool or row.get("resource_pool") or "default",
            resources=snapshot.resources,
            sla=snapshot.sla,
            status=snapshot.status,
            has_claim=snapshot.has_claim,
            worker_id=claim.worker_id if claim is not None else None,
            attempt=claim.attempt if claim is not None else None,
            claim_expires_at=claim.expires_at if claim is not None else None,
            has_output=snapshot.has_output,
            output_value_keys=snapshot.output_value_keys,
            artifact_count=snapshot.artifact_count,
            output_document_count=snapshot.output_document_count,
            metadata_keys=snapshot.metadata_keys,
            stream_count=snapshot.stream_count,
            stream_chunk_count=snapshot.stream_chunk_count,
            stream_artifact_count=snapshot.stream_artifact_count,
            stream_checkpoint_count=snapshot.stream_checkpoint_count,
            status_updated_at=_datetime_or_none(row.get("status_updated_at")),
        )

    async def dead_letter_queue(
        self,
        run_id: str,
        *,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        parent_document_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
        capability: str | None = None,
        operation_type: str | None = None,
        adapter_kind: str | None = None,
        resource_pool: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> RuntimeDeadLetterPage:
        process_page = await self.process_registry(
            run_id,
            status=ProcessStatus.failed,
            pipeline_id=pipeline_id,
            document_type=document_type,
            parent_document_id=parent_document_id,
            document_id=document_id,
            process_id=process_id,
            capability=capability,
            operation_type=operation_type,
            adapter_kind=adapter_kind,
            resource_pool=resource_pool,
            limit=limit,
            offset=offset,
        )
        items = [
            await self._dead_letter_item(process)
            for process in process_page.processes
        ]
        return RuntimeDeadLetterPage(
            run_id=run_id,
            count=len(items),
            limit=limit,
            offset=offset,
            has_more=process_page.has_more,
            filters=process_page.filters,
            items=items,
        )

    async def _dead_letter_item(
        self,
        process: RuntimeProcessRecord,
    ) -> RuntimeDeadLetterItem:
        events = await self.store.list_events(
            run_id=process.run_id,
            document_id=process.document_id,
            process_id=process.process_id,
            descending=True,
            limit=1,
        )
        last_event = events[0] if events else None
        event_data = dict(last_event.data) if last_event is not None else {}
        dead_lettered_at = (
            last_event.ts
            if last_event is not None
            else process.status_updated_at
        )
        return RuntimeDeadLetterItem(
            run_id=process.run_id,
            document_id=process.document_id,
            process_id=process.process_id,
            pipeline_id=process.pipeline_id,
            document_title=process.document_title,
            document_type=process.document_type,
            title=process.title,
            capability=process.capability,
            operation_type=process.operation_type,
            adapter_kind=process.adapter_kind,
            resource_pool=process.resource_pool,
            status=ProcessStatus.failed,
            status_updated_at=process.status_updated_at,
            dead_lettered_at=dead_lettered_at,
            last_event_id=last_event.id if last_event is not None else None,
            last_event_type=last_event.type if last_event is not None else None,
            last_event_at=last_event.ts if last_event is not None else None,
            reason=_string_or_none(event_data.get("reason")),
            error_kind=_string_or_none(event_data.get("error_kind")),
            terminal_reason=_string_or_none(event_data.get("terminal_reason")),
            retry_allowed=_bool_or_none(event_data.get("retry_allowed")),
            worker_id=_string_or_none(event_data.get("worker_id")),
            attempt=_int_or_none(event_data.get("attempt")) or process.attempt,
            max_attempts=_int_or_none(event_data.get("max_attempts")),
            suggested_actions=[
                ProcessAction.retry,
                ProcessAction.skip,
                ProcessAction.cancel,
            ],
            event_data=event_data,
            process=process,
        )

    async def stuck_work(
        self,
        run_id: str,
        *,
        status: ProcessStatus | None = None,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        parent_document_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
        capability: str | None = None,
        operation_type: str | None = None,
        adapter_kind: str | None = None,
        resource_pool: str | None = None,
        waiting_after_seconds: float = 3600.0,
        queued_after_seconds: float = 600.0,
        running_after_seconds: float = 1800.0,
        limit: int = 100,
        offset: int = 0,
    ) -> RuntimeStuckWorkPage:
        if limit < 1:
            raise ValueError("limit must be greater than zero")
        if offset < 0:
            raise ValueError("offset must be greater than or equal to zero")
        thresholds = {
            ProcessStatus.waiting: waiting_after_seconds,
            ProcessStatus.queued: queued_after_seconds,
            ProcessStatus.running: running_after_seconds,
        }
        invalid_thresholds = [
            name
            for name, value in {
                "waiting_after_seconds": waiting_after_seconds,
                "queued_after_seconds": queued_after_seconds,
                "running_after_seconds": running_after_seconds,
            }.items()
            if value < 0
        ]
        if invalid_thresholds:
            raise ValueError(
                f"Stuck-work threshold(s) must be >= 0: {', '.join(invalid_thresholds)}"
            )
        statuses = [ProcessStatus.waiting, ProcessStatus.queued, ProcessStatus.running]
        if status is not None:
            if status not in set(statuses):
                raise ValueError(
                    "stuck-work status must be waiting, queued, or running"
                )
            statuses = [status]

        records: list[RuntimeProcessRecord] = []
        for selected_status in statuses:
            rows = await self.store.list_process_record_keys(
                run_id=run_id,
                status=selected_status,
                pipeline_id=pipeline_id,
                document_type=document_type,
                parent_document_id=parent_document_id,
                document_id=document_id,
                process_id=process_id,
                capability=capability,
                adapter_kind=adapter_kind,
                resource_pool=resource_pool,
                limit=None,
            )
            records.extend([await self._runtime_process_record(row) for row in rows])
        if operation_type is not None:
            records = [
                process
                for process in records
                if process.operation_type == operation_type
            ]

        now = datetime.now(timezone.utc)
        items: list[RuntimeStuckWorkItem] = []
        for process in records:
            item = await self._stuck_work_item(
                process,
                now=now,
                thresholds=thresholds,
            )
            if item is not None:
                items.append(item)

        items.sort(key=_stuck_work_sort_key)
        selected_items = items[offset : offset + limit]
        filters = {
            key: value
            for key, value in {
                "status": status.value if status is not None else None,
                "pipeline_id": pipeline_id,
                "document_type": document_type,
                "parent_document_id": parent_document_id,
                "document_id": document_id,
                "process_id": process_id,
                "capability": capability,
                "operation_type": operation_type,
                "adapter_kind": adapter_kind,
                "resource_pool": resource_pool,
                "waiting_after_seconds": waiting_after_seconds,
                "queued_after_seconds": queued_after_seconds,
                "running_after_seconds": running_after_seconds,
            }.items()
            if value is not None
        }
        return RuntimeStuckWorkPage(
            run_id=run_id,
            count=len(selected_items),
            limit=limit,
            offset=offset,
            has_more=offset + limit < len(items),
            critical_count=sum(1 for item in items if item.severity == "critical"),
            warning_count=sum(1 for item in items if item.severity == "warning"),
            filters=filters,
            items=selected_items,
        )

    async def stream_lag(
        self,
        run_id: str,
        *,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        parent_document_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
        capability: str | None = None,
        operation_type: str | None = None,
        adapter_kind: str | None = None,
        resource_pool: str | None = None,
        stream_id: str | None = None,
        consumer_id: str | None = None,
        min_lag: int = 1,
        over_limit: bool | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> RuntimeStreamLagPage:
        if limit < 1:
            raise ValueError("limit must be greater than zero")
        if offset < 0:
            raise ValueError("offset must be greater than or equal to zero")
        if min_lag < 0:
            raise ValueError("min_lag must be greater than or equal to zero")

        state = await self.load_state_model(run_id)
        items: list[RuntimeStreamLagItem] = []
        for document_state in state.documents:
            document = document_state.document
            if pipeline_id is not None and document_state.pipeline_id != pipeline_id:
                continue
            if (
                document_type is not None
                and (document is None or document.document_type != document_type)
            ):
                continue
            if parent_document_id is not None and (
                document is None or document.parent_document_id != parent_document_id
            ):
                continue
            if document_id is not None and document_state.document_id != document_id:
                continue

            for step in document_state.steps:
                step_adapter_kind = (
                    getattr(step.adapter_kind, "value", step.adapter_kind)
                    if step.adapter_kind is not None
                    else None
                )
                if process_id is not None and step.id != process_id:
                    continue
                if capability is not None and step.capability != capability:
                    continue
                if operation_type is not None and step.operation_type != operation_type:
                    continue
                if adapter_kind is not None and step_adapter_kind != adapter_kind:
                    continue
                if resource_pool is not None and step.resource_pool != resource_pool:
                    continue

                for stream in step.streams:
                    if stream_id is not None and stream.stream_id != stream_id:
                        continue
                    max_buffered_chunks = self._stream_max_buffered_chunks_for_step(
                        pipeline_id=document_state.pipeline_id,
                        process_id=step.id,
                        stream_id=stream.stream_id,
                    )
                    declared_consumers = set(stream.declared_consumers)
                    entries: list[tuple[str | None, int, bool, bool]] = []
                    if consumer_id is not None:
                        if consumer_id in stream.checkpoint_lag:
                            entries.append(
                                (
                                    consumer_id,
                                    stream.checkpoint_lag[consumer_id],
                                    False,
                                    consumer_id in declared_consumers,
                                )
                            )
                        else:
                            entries.append(
                                (
                                    consumer_id,
                                    stream.chunk_count,
                                    True,
                                    consumer_id in declared_consumers,
                                )
                            )
                    else:
                        entries.extend(
                            (
                                selected_consumer_id,
                                lag,
                                False,
                                selected_consumer_id in declared_consumers,
                            )
                            for selected_consumer_id, lag
                            in stream.checkpoint_lag.items()
                        )
                        entries.extend(
                            (declared_consumer, stream.chunk_count, True, True)
                            for declared_consumer in sorted(
                                declared_consumers - set(stream.checkpoint_lag)
                            )
                        )
                        if not entries and stream.chunk_count > 0:
                            entries.append((None, stream.chunk_count, True, False))

                    for (
                        selected_consumer_id,
                        lag,
                        uncheckpointed,
                        declared_consumer,
                    ) in entries:
                        item_over_limit = (
                            max_buffered_chunks is not None
                            and lag > max_buffered_chunks
                        )
                        if lag < min_lag:
                            continue
                        if over_limit is not None and item_over_limit != over_limit:
                            continue
                        checkpoint_sequence = (
                            stream.checkpoint_sequences.get(selected_consumer_id)
                            if selected_consumer_id is not None
                            else None
                        )
                        checkpoint_chunk_id = (
                            stream.checkpoint_chunk_ids.get(selected_consumer_id)
                            if selected_consumer_id is not None
                            else None
                        )
                        checkpoint_updated_at = (
                            stream.checkpoint_updated_at.get(selected_consumer_id)
                            if selected_consumer_id is not None
                            else None
                        )
                        items.append(
                            RuntimeStreamLagItem(
                                run_id=run_id,
                                document_id=document_state.document_id,
                                document_title=(
                                    document.title if document is not None else None
                                ),
                                document_type=(
                                    document.document_type
                                    if document is not None
                                    else None
                                ),
                                parent_document_id=(
                                    document.parent_document_id
                                    if document is not None
                                    else None
                                ),
                                pipeline_id=document_state.pipeline_id,
                                process_id=step.id,
                                title=step.title,
                                capability=step.capability,
                                operation_type=step.operation_type,
                                adapter_kind=step.adapter_kind,
                                resource_pool=step.resource_pool,
                                stream_id=stream.stream_id,
                                consumer_id=selected_consumer_id,
                                lag=lag,
                                chunk_count=stream.chunk_count,
                                checkpoint_count=stream.checkpoint_count,
                                checkpoint_sequence=checkpoint_sequence,
                                checkpoint_chunk_id=checkpoint_chunk_id,
                                checkpoint_updated_at=checkpoint_updated_at,
                                last_sequence=stream.last_sequence,
                                last_chunk_id=stream.last_chunk_id,
                                last_chunk_at=stream.last_chunk_at,
                                max_buffered_chunks=max_buffered_chunks,
                                declared_consumer=declared_consumer,
                                over_limit=item_over_limit,
                                uncheckpointed=uncheckpointed,
                                kind_counts=stream.kind_counts,
                                value_keys=stream.value_keys,
                            )
                        )

        items.sort(key=_stream_lag_sort_key)
        selected_items = items[offset : offset + limit]
        filters = {
            key: value
            for key, value in {
                "pipeline_id": pipeline_id,
                "document_type": document_type,
                "parent_document_id": parent_document_id,
                "document_id": document_id,
                "process_id": process_id,
                "capability": capability,
                "operation_type": operation_type,
                "adapter_kind": adapter_kind,
                "resource_pool": resource_pool,
                "stream_id": stream_id,
                "consumer_id": consumer_id,
                "min_lag": min_lag,
                "over_limit": over_limit,
            }.items()
            if value is not None
        }
        return RuntimeStreamLagPage(
            run_id=run_id,
            count=len(selected_items),
            limit=limit,
            offset=offset,
            has_more=offset + limit < len(items),
            lagging_count=sum(1 for item in items if item.lag > 0),
            over_limit_count=sum(1 for item in items if item.over_limit),
            uncheckpointed_count=sum(1 for item in items if item.uncheckpointed),
            max_lag=max((item.lag for item in items), default=0),
            filters=filters,
            items=selected_items,
        )

    async def _stuck_work_item(
        self,
        process: RuntimeProcessRecord,
        *,
        now: datetime,
        thresholds: dict[ProcessStatus, float],
    ) -> RuntimeStuckWorkItem | None:
        if not isinstance(process.status, ProcessStatus):
            return None
        if process.status not in set(thresholds):
            return None
        events = await self.store.list_events(
            run_id=process.run_id,
            document_id=process.document_id,
            process_id=process.process_id,
        )
        last_event = max(events, key=lambda item: item.ts) if events else None
        status_since = _status_since(process, events)
        status_age_seconds = (
            max(0.0, (now - status_since).total_seconds())
            if status_since is not None
            else None
        )
        retry_after = _latest_retry_after_for_events(events)
        threshold_seconds = _process_sla_threshold(
            process,
            status=process.status,
            defaults=thresholds,
        )

        reason: str | None = None
        severity: Literal["warning", "critical"] = "warning"
        data: dict[str, Any] = {}
        if (
            process.status == ProcessStatus.running
            and process.claim_expires_at is not None
            and process.claim_expires_at <= now
        ):
            reason = "claim_expired"
            severity = "critical"
            data["overdue_seconds"] = max(
                0.0,
                (now - process.claim_expires_at).total_seconds(),
            )
        elif (
            process.status == ProcessStatus.waiting
            and retry_after is not None
            and retry_after <= now
        ):
            reason = "waiting_retry_overdue"
            data["retry_overdue_seconds"] = max(
                0.0,
                (now - retry_after).total_seconds(),
            )
        elif (
            status_age_seconds is not None
            and status_age_seconds >= threshold_seconds
        ):
            reason = f"{process.status.value}_too_long"

        if reason is None:
            return None

        return RuntimeStuckWorkItem(
            run_id=process.run_id,
            document_id=process.document_id,
            process_id=process.process_id,
            pipeline_id=process.pipeline_id,
            document_title=process.document_title,
            document_type=process.document_type,
            title=process.title,
            capability=process.capability,
            operation_type=process.operation_type,
            adapter_kind=process.adapter_kind,
            resource_pool=process.resource_pool,
            status=process.status,
            reason=reason,
            severity=severity,
            status_since=status_since,
            status_age_seconds=status_age_seconds,
            threshold_seconds=threshold_seconds,
            claim_expires_at=process.claim_expires_at,
            retry_after=retry_after,
            last_event_id=last_event.id if last_event is not None else None,
            last_event_type=last_event.type if last_event is not None else None,
            last_event_at=last_event.ts if last_event is not None else None,
            worker_id=process.worker_id,
            attempt=process.attempt,
            suggested_actions=_stuck_work_suggested_actions(process.status),
            data=data,
            process=process,
        )

    async def document_lineage(self, run_id: str) -> RuntimeDocumentLineage:
        state = await self.load_state_model(run_id, include_events=False)
        nodes: list[RuntimeDocumentLineageNode] = []
        edges: list[RuntimeDocumentLineageEdge] = []
        root_document_ids: list[str] = []

        for document_state in state.documents:
            document = document_state.document
            parent_document_id = document_state.parent_document_id
            parent_process_id = document_state.parent_process_id
            relation = document_state.relation
            if parent_document_id is None:
                root_document_ids.append(document_state.document_id)
            else:
                edges.append(
                    RuntimeDocumentLineageEdge(
                        parent_document_id=parent_document_id,
                        child_document_id=document_state.document_id,
                        parent_process_id=parent_process_id,
                        relation=relation,
                        child_pipeline_id=document_state.pipeline_id,
                    )
                )
            nodes.append(
                RuntimeDocumentLineageNode(
                    document_id=document_state.document_id,
                    pipeline_id=document_state.pipeline_id,
                    title=document.title if document else None,
                    document_type=document.document_type if document else None,
                    relation=relation,
                    media_type=document.media_type if document else None,
                    source_uri=document.source_uri if document else None,
                    status=document.status if document else "unknown",
                    parent_document_id=parent_document_id,
                    parent_process_id=parent_process_id,
                    child_document_ids=document_state.child_document_ids,
                    process_count=len(document_state.steps),
                    output_count=len(document_state.outputs),
                    event_count=document_state.event_count,
                )
            )

        return RuntimeDocumentLineage(
            run_id=run_id,
            root_document_ids=root_document_ids,
            node_count=len(nodes),
            edge_count=len(edges),
            nodes=nodes,
            edges=edges,
        )

    async def run_results(
        self,
        run_id: str,
        *,
        pipeline_id: str | None = None,
        process_id: str | None = None,
        document_id: str | None = None,
        document_type: str | None = None,
        operation_type: str | None = None,
    ) -> RuntimeRunResults:
        state = await self.load_state_model(run_id, include_events=False)
        results: list[RuntimeRunResultItem] = []

        for document_state in state.documents:
            document = document_state.document
            if pipeline_id is not None and document_state.pipeline_id != pipeline_id:
                continue
            if document_id is not None and document_state.document_id != document_id:
                continue
            if (
                document_type is not None
                and (document.document_type if document else None) != document_type
            ):
                continue
            for output_process_id, output in sorted(document_state.outputs.items()):
                if process_id is not None and output_process_id != process_id:
                    continue
                step = next(
                    (
                        item
                        for item in document_state.steps
                        if item.id == output_process_id
                    ),
                    None,
                )
                output_operation_type = (
                    step.operation_type if step is not None else None
                )
                if (
                    operation_type is not None
                    and output_operation_type != operation_type
                ):
                    continue
                process_runtime = output.metadata.get("process_runtime")
                lineage = (
                    process_runtime.get("lineage")
                    if isinstance(process_runtime, dict)
                    and isinstance(process_runtime.get("lineage"), dict)
                    else {}
                )
                status = document_state.statuses.get(output_process_id)
                results.append(
                    RuntimeRunResultItem(
                        run_id=run_id,
                        document_id=document_state.document_id,
                        pipeline_id=document_state.pipeline_id,
                        process_id=output_process_id,
                        operation_type=output_operation_type,
                        status=status if status is not None else "unknown",
                        title=document.title if document else None,
                        document_type=document.document_type if document else None,
                        document_relation=document.relation if document else None,
                        media_type=document.media_type if document else None,
                        source_uri=document.source_uri if document else None,
                        parent_document_id=document_state.parent_document_id,
                        parent_process_id=document_state.parent_process_id,
                        child_document_ids=document_state.child_document_ids,
                        output=output,
                        value_keys=sorted(output.values),
                        artifact_count=len(output.artifacts),
                        output_document_count=len(output.output_documents),
                        metadata_keys=sorted(output.metadata),
                        lineage=lineage,
                    )
                )

        filters = {
            "pipeline_id": pipeline_id,
            "process_id": process_id,
            "document_id": document_id,
            "document_type": document_type,
            "operation_type": operation_type,
        }
        return RuntimeRunResults(
            run_id=run_id,
            filters={key: value for key, value in filters.items() if value is not None},
            count=len(results),
            results=results,
        )

    async def output_documents(
        self,
        run_id: str,
        *,
        pipeline_id: str | None = None,
        process_id: str | None = None,
        document_id: str | None = None,
        source_document_type: str | None = None,
        output_document_id: str | None = None,
        document_type: str | None = None,
        relation: str | None = None,
        media_type: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> RuntimeOutputDocumentPage:
        if limit < 1:
            raise ValueError("limit must be greater than zero")
        if offset < 0:
            raise ValueError("offset must be zero or greater")

        state = await self.load_state_model(run_id, include_events=False)
        items: list[RuntimeOutputDocumentItem] = []

        for document_state in state.documents:
            document = document_state.document
            if pipeline_id is not None and document_state.pipeline_id != pipeline_id:
                continue
            if document_id is not None and document_state.document_id != document_id:
                continue
            if (
                source_document_type is not None
                and (document.document_type if document else None) != source_document_type
            ):
                continue
            for output_process_id, output in sorted(document_state.outputs.items()):
                if process_id is not None and output_process_id != process_id:
                    continue
                process_runtime = output.metadata.get("process_runtime")
                lineage = (
                    process_runtime.get("lineage")
                    if isinstance(process_runtime, dict)
                    and isinstance(process_runtime.get("lineage"), dict)
                    else {}
                )
                artifact_by_id = {artifact.id: artifact for artifact in output.artifacts}
                status = document_state.statuses.get(output_process_id)
                for output_document in output.output_documents:
                    if (
                        output_document_id is not None
                        and output_document.id != output_document_id
                    ):
                        continue
                    if (
                        document_type is not None
                        and output_document.document_type != document_type
                    ):
                        continue
                    if relation is not None and output_document.relation != relation:
                        continue
                    if media_type is not None and output_document.media_type != media_type:
                        continue
                    artifact = (
                        artifact_by_id.get(output_document.artifact_id)
                        if output_document.artifact_id is not None
                        else None
                    )
                    items.append(
                        RuntimeOutputDocumentItem(
                            run_id=run_id,
                            source_document_id=document_state.document_id,
                            pipeline_id=document_state.pipeline_id,
                            process_id=output_process_id,
                            status=status if status is not None else "unknown",
                            source_title=document.title if document else None,
                            source_document_type=(
                                document.document_type if document else None
                            ),
                            source_document_relation=(
                                document.relation if document else None
                            ),
                            source_media_type=document.media_type if document else None,
                            source_uri=document.source_uri if document else None,
                            parent_document_id=document_state.parent_document_id,
                            parent_process_id=document_state.parent_process_id,
                            child_document_ids=document_state.child_document_ids,
                            output_document_id=output_document.id,
                            title=output_document.title,
                            document_type=output_document.document_type,
                            media_type=output_document.media_type,
                            uri=output_document.uri,
                            artifact_id=output_document.artifact_id,
                            relation=output_document.relation,
                            output_document=output_document,
                            artifact=artifact,
                            value_keys=sorted(output_document.values),
                            metadata_keys=sorted(output_document.metadata),
                            lineage=lineage,
                        )
                    )

        filters = {
            "pipeline_id": pipeline_id,
            "process_id": process_id,
            "document_id": document_id,
            "source_document_type": source_document_type,
            "output_document_id": output_document_id,
            "document_type": document_type,
            "relation": relation,
            "media_type": media_type,
        }
        page_items = items[offset : offset + limit]
        return RuntimeOutputDocumentPage(
            run_id=run_id,
            count=len(items),
            limit=limit,
            offset=offset,
            has_more=offset + limit < len(items),
            filters={key: value for key, value in filters.items() if value is not None},
            output_documents=page_items,
        )

    async def run_reductions(
        self,
        run_id: str,
        *,
        pipeline_id: str | None = None,
        reduce_id: str | None = None,
    ) -> RuntimeRunReductions:
        reductions: list[RuntimeRunReduction] = []
        for pipeline in sorted(self.registry.all(), key=lambda item: item.id):
            if pipeline_id is not None and pipeline.id != pipeline_id:
                continue
            for spec in pipeline.reduces:
                if reduce_id is not None and spec.id != reduce_id:
                    continue
                results = await self.run_results(
                    run_id,
                    pipeline_id=pipeline.id,
                    process_id=spec.process_id,
                    document_type=spec.document_type,
                )
                reductions.append(
                    RuntimeRunReduction(
                        id=spec.id,
                        run_id=run_id,
                        pipeline_id=pipeline.id,
                        title=spec.title,
                        description=spec.description,
                        tags=spec.tags,
                        mode=spec.mode,
                        filters=results.filters,
                        result_count=results.count,
                        output=_run_reduce_output(spec, results),
                    )
                )
        return RuntimeRunReductions(
            run_id=run_id,
            count=len(reductions),
            reductions=reductions,
        )

    async def artifact_gc(self, *, dry_run: bool = True) -> RuntimeArtifactGcPlan:
        referenced_digests = await self._referenced_artifact_digests()
        blobs = self.artifact_store.list_blobs()
        orphaned_digests = [
            digest for digest, _path, _size in blobs if digest not in referenced_digests
        ]
        deleted_digests = (
            set() if dry_run else set(self.artifact_store.delete_blobs(orphaned_digests))
        )

        orphaned_blobs: list[RuntimeArtifactBlob] = []
        total_bytes = 0
        referenced_bytes = 0
        orphaned_bytes = 0
        deleted_bytes = 0
        referenced_blob_count = 0

        for digest, path, size_bytes in blobs:
            total_bytes += size_bytes
            referenced = digest in referenced_digests
            deleted = digest in deleted_digests
            if referenced:
                referenced_blob_count += 1
                referenced_bytes += size_bytes
                continue
            orphaned_bytes += size_bytes
            if deleted:
                deleted_bytes += size_bytes
            orphaned_blobs.append(
                RuntimeArtifactBlob(
                    digest=digest,
                    uri=f"fala-artifact://sha256/{digest}",
                    path=str(path),
                    size_bytes=size_bytes,
                    referenced=False,
                    deleted=deleted,
                )
            )

        return RuntimeArtifactGcPlan(
            root=self.artifact_store.location,
            dry_run=dry_run,
            referenced_digest_count=len(referenced_digests),
            blob_count=len(blobs),
            referenced_blob_count=referenced_blob_count,
            orphaned_blob_count=len(orphaned_blobs),
            deleted_blob_count=len(deleted_digests),
            total_bytes=total_bytes,
            referenced_bytes=referenced_bytes,
            orphaned_bytes=orphaned_bytes,
            deleted_bytes=deleted_bytes,
            orphaned_blobs=orphaned_blobs,
        )

    async def _referenced_artifact_digests(self) -> set[str]:
        digests: set[str] = set()
        rows = await self.store.list_runs(limit=None)
        for row in rows:
            run_id = str(row["run_id"])
            for document_id in await self.store.list_documents(run_id=run_id):
                document = await self.store.get_document(
                    run_id=run_id,
                    document_id=document_id,
                )
                if document is not None and document.source_uri:
                    _add_fala_artifact_digest(digests, document.source_uri)

                input_payload = await self.store.get_document_input(
                    run_id=run_id,
                    document_id=document_id,
                )
                if input_payload is not None:
                    _add_artifact_ref_digests(digests, input_payload.artifacts)

                outputs = await self.store.list_outputs(
                    run_id=run_id,
                    document_id=document_id,
                )
                for output in outputs.values():
                    _add_artifact_ref_digests(digests, output.artifacts)
                    for document in output.output_documents:
                        if document.uri:
                            _add_fala_artifact_digest(digests, document.uri)

                chunks = await self.store.list_stream_chunks(
                    run_id=run_id,
                    document_id=document_id,
                )
                for chunk in chunks:
                    _add_artifact_ref_digests(digests, chunk.artifacts)
        return digests

    async def run_retention(
        self,
        *,
        before: datetime,
        statuses: list[RunStatus] | None = None,
        dry_run: bool = True,
    ) -> RuntimeRunRetentionPlan:
        cutoff = _ensure_aware_utc(before)
        selected_statuses = statuses or [
            RunStatus.completed,
            RunStatus.failed,
            RunStatus.cancelled,
        ]
        selected_status_set = set(selected_statuses)
        rows = await self.store.list_runs(limit=None)
        items: list[RuntimeRunRetentionItem] = []
        row_counts: Counter[str] = Counter()
        deleted_run_count = 0

        for row in rows:
            run_id = str(row["run_id"])
            run_payload = row.get("run")
            run = (
                RuntimeRun.model_validate(run_payload)
                if isinstance(run_payload, dict)
                else await self.get_run(run_id)
            )
            updated_at = _row_datetime(row.get("updated_at"))
            if updated_at is None and run is not None:
                updated_at = run.updated_at
            updated_at = _ensure_aware_utc(updated_at) if updated_at else None

            if run is None:
                status: RunStatus | str = "unknown"
                title = None
                finished_at = None
            else:
                status = run.status
                title = run.title
                finished_at = run.finished_at

            matched = False
            reason: str | None = None
            if updated_at is None:
                reason = "missing_updated_at"
            elif updated_at >= cutoff:
                reason = "not_before_cutoff"
            elif not isinstance(status, RunStatus) or status not in selected_status_set:
                reason = "status_not_selected"
            else:
                matched = True

            if not matched:
                continue

            counts: dict[str, int] = {}
            deleted = False
            if not dry_run:
                counts = await self.store.delete_run(run_id)
                row_counts.update(counts)
                deleted = True
                deleted_run_count += 1

            items.append(
                RuntimeRunRetentionItem(
                    run_id=run_id,
                    status=status,
                    title=title,
                    updated_at=updated_at,
                    finished_at=finished_at,
                    matched=True,
                    deleted=deleted,
                    reason=reason,
                    row_counts=counts,
                )
            )

        return RuntimeRunRetentionPlan(
            dry_run=dry_run,
            before=cutoff,
            statuses=selected_statuses,
            candidate_count=len(items),
            deleted_run_count=deleted_run_count,
            row_counts=dict(sorted(row_counts.items())),
            runs=items,
        )

    async def queue_metrics(
        self,
        run_id: str,
        *,
        stale_after_seconds: float = 60.0,
    ) -> RuntimeQueueMetrics:
        state = await self.load_state_model(run_id, include_events=False)
        run = await self.store.get_run(run_id)
        resource_pools = _run_resource_pools(run.config if run is not None else {})
        pool_usage, pool_running_counts, pool_queued_counts = (
            _resource_pool_usage_from_state(state)
        )
        pool_metrics = _runtime_resource_pool_metrics(
            resource_pools=resource_pools,
            pool_usage=pool_usage,
            running_counts=pool_running_counts,
            queued_counts=pool_queued_counts,
        )
        workers = await self.worker_health(
            run_id,
            stale_after_seconds=stale_after_seconds,
        )
        generated_at = datetime.now(timezone.utc)
        process_metrics: dict[tuple[str | None, str], dict[str, Any]] = {}

        for document in state.documents:
            events = await self.store.list_events(
                run_id=run_id,
                document_id=document.document_id,
            )
            last_event_by_process = _last_event_ts_by_process(events)
            queued_at_by_process = _last_status_event_ts_by_process(
                events,
                status=ProcessStatus.queued,
            )
            running_at_by_process = _last_status_event_ts_by_process(
                events,
                status=ProcessStatus.running,
            )
            retry_after_by_process = _latest_retry_after_by_process(events)
            for step in document.steps:
                status = (
                    step.status.value
                    if isinstance(step.status, ProcessStatus)
                    else str(step.status)
                )
                key = (document.pipeline_id, step.id)
                metrics = process_metrics.setdefault(
                    key,
                    {
                        "pipeline_id": document.pipeline_id,
                        "process_id": step.id,
                        "title": step.title,
                        "capability": step.capability,
                        "operation_type": step.operation_type,
                        "adapter_kind": step.adapter_kind,
                        "priority": step.priority,
                        "max_concurrency": step.max_concurrency,
                        "resource_pool": step.resource_pool,
                        "resources": step.resources,
                        "counts": {},
                        "claim_count": 0,
                        "output_count": 0,
                        "retry_backoff_count": 0,
                        "missing_worker_count": 0,
                        "resource_blocked_count": 0,
                        "matching_worker_count": 0,
                        "healthy_worker_count": 0,
                        "next_retry_after": None,
                        "next_retry_after_document_id": None,
                        "oldest_queued_at": None,
                        "oldest_queued_document_id": None,
                        "oldest_running_at": None,
                        "oldest_running_document_id": None,
                        "last_event_at": None,
                    },
                )
                matching_workers = _matching_workers(
                    workers,
                    pipeline_id=document.pipeline_id,
                    step=step,
                )
                healthy_worker_count = sum(1 for worker in matching_workers if worker.healthy)
                metrics["matching_worker_count"] = max(
                    metrics["matching_worker_count"],
                    len(matching_workers),
                )
                metrics["healthy_worker_count"] = max(
                    metrics["healthy_worker_count"],
                    healthy_worker_count,
                )
                metrics["counts"][status] = metrics["counts"].get(status, 0) + 1
                if step.has_claim:
                    metrics["claim_count"] += 1
                if step.has_output:
                    metrics["output_count"] += 1

                last_event_at = last_event_by_process.get(step.id)
                if last_event_at is not None and (
                    metrics["last_event_at"] is None
                    or last_event_at > metrics["last_event_at"]
                    ):
                        metrics["last_event_at"] = last_event_at

                if status == ProcessStatus.waiting.value:
                    retry_after = retry_after_by_process.get(step.id)
                    if retry_after is not None and retry_after > generated_at:
                        metrics["retry_backoff_count"] += 1
                        if (
                            metrics["next_retry_after"] is None
                            or retry_after < metrics["next_retry_after"]
                        ):
                            metrics["next_retry_after"] = retry_after
                            metrics["next_retry_after_document_id"] = document.document_id

                if status == ProcessStatus.queued.value:
                    if step.adapter_kind == "queue" and healthy_worker_count == 0:
                        metrics["missing_worker_count"] += 1
                    if not resource_pool_allows(
                        step.resources,
                        pool_id=step.resource_pool,
                        pool_limits=resource_pools,
                        pool_usage=pool_usage,
                    ):
                        metrics["resource_blocked_count"] += 1
                    queued_at = queued_at_by_process.get(step.id)
                    if queued_at is not None and (
                        metrics["oldest_queued_at"] is None
                        or queued_at < metrics["oldest_queued_at"]
                    ):
                        metrics["oldest_queued_at"] = queued_at
                        metrics["oldest_queued_document_id"] = document.document_id

                if status == ProcessStatus.running.value:
                    running_at = (
                        step.claim.claimed_at
                        if step.claim is not None
                        else running_at_by_process.get(step.id)
                    )
                    if running_at is not None and (
                        metrics["oldest_running_at"] is None
                        or running_at < metrics["oldest_running_at"]
                    ):
                        metrics["oldest_running_at"] = running_at
                        metrics["oldest_running_document_id"] = document.document_id

        processes = [
            _runtime_process_metrics(metrics)
            for metrics in process_metrics.values()
        ]
        processes.sort(
            key=lambda item: (
                not item.missing_worker,
                not item.resource_blocked,
                not item.saturated,
                -item.queued_count,
                -item.running_count,
                -item.missing_worker_count,
                -item.resource_blocked_count,
                -item.priority,
                item.pipeline_id or "",
                item.process_id,
            )
        )
        worker_demands = [
            demand
            for process in processes
            if (
                demand := _runtime_worker_demand(
                    process,
                    package_worker_ids=_package_worker_ids_for_process(
                        self.registry,
                        process,
                    ),
                )
            )
            is not None
        ]
        return RuntimeQueueMetrics(
            run_id=run_id,
            generated_at=generated_at,
            document_count=state.summary.document_count,
            process_group_count=len(processes),
            process_instance_count=state.summary.process_count,
            waiting_count=sum(item.waiting_count for item in processes),
            queued_count=sum(item.queued_count for item in processes),
            running_count=sum(item.running_count for item in processes),
            failed_count=sum(item.failed_count for item in processes),
            retry_backoff_count=sum(item.retry_backoff_count for item in processes),
            missing_worker_count=sum(item.missing_worker_count for item in processes),
            missing_worker_process_count=sum(
                1 for item in processes if item.missing_worker
            ),
            resource_blocked_count=sum(
                item.resource_blocked_count for item in processes
            ),
            resource_blocked_process_count=sum(
                1 for item in processes if item.resource_blocked
            ),
            saturated_process_count=sum(1 for item in processes if item.saturated),
            worker_demand_process_count=len(worker_demands),
            worker_deficit_count=sum(
                item.worker_deficit_count for item in worker_demands
            ),
            resource_pools=pool_metrics,
            worker_demands=worker_demands,
            processes=processes,
        )

    async def capability_demands(
        self,
        run_id: str,
        *,
        stale_after_seconds: float = 60.0,
    ) -> RuntimeCapabilityDemandSummary:
        metrics = await self.queue_metrics(
            run_id,
            stale_after_seconds=stale_after_seconds,
        )
        grouped: dict[
            tuple[str | None, str | None, str | None, str],
            dict[str, Any],
        ] = {}
        for demand in metrics.worker_demands:
            key = (
                demand.capability,
                demand.operation_type,
                demand.adapter_kind,
                demand.resource_pool,
            )
            row = grouped.setdefault(
                key,
                {
                    "capability": demand.capability,
                    "operation_type": demand.operation_type,
                    "adapter_kind": demand.adapter_kind,
                    "resource_pool": demand.resource_pool,
                    "pipeline_ids": set(),
                    "process_ids": set(),
                    "queued_count": 0,
                    "running_count": 0,
                    "resource_blocked_count": 0,
                    "claimable_queued_count": 0,
                    "matching_worker_count": 0,
                    "healthy_worker_count": 0,
                    "target_worker_count": 0,
                    "package_worker_ids": set(),
                    "processes": [],
                },
            )
            if demand.pipeline_id is not None:
                row["pipeline_ids"].add(demand.pipeline_id)
            row["process_ids"].add(demand.process_id)
            row["queued_count"] += demand.queued_count
            row["running_count"] += demand.running_count
            row["resource_blocked_count"] += demand.resource_blocked_count
            row["claimable_queued_count"] += demand.claimable_queued_count
            row["matching_worker_count"] = max(
                row["matching_worker_count"],
                demand.matching_worker_count,
            )
            row["healthy_worker_count"] = max(
                row["healthy_worker_count"],
                demand.healthy_worker_count,
            )
            row["target_worker_count"] += demand.target_worker_count
            row["package_worker_ids"].update(demand.package_worker_ids)
            row["processes"].append(demand)

        demands: list[RuntimeCapabilityDemand] = []
        for row in grouped.values():
            worker_deficit_count = max(
                0,
                row["target_worker_count"] - row["healthy_worker_count"],
            )
            processes = sorted(
                row["processes"],
                key=lambda item: (
                    item.pipeline_id or "",
                    item.process_id,
                ),
            )
            demands.append(
                RuntimeCapabilityDemand(
                    capability=row["capability"],
                    operation_type=row["operation_type"],
                    adapter_kind=row["adapter_kind"],
                    resource_pool=row["resource_pool"],
                    pipeline_ids=sorted(row["pipeline_ids"]),
                    process_ids=sorted(row["process_ids"]),
                    process_group_count=len(processes),
                    queued_count=row["queued_count"],
                    running_count=row["running_count"],
                    resource_blocked_count=row["resource_blocked_count"],
                    claimable_queued_count=row["claimable_queued_count"],
                    matching_worker_count=row["matching_worker_count"],
                    healthy_worker_count=row["healthy_worker_count"],
                    target_worker_count=row["target_worker_count"],
                    worker_deficit_count=worker_deficit_count,
                    package_worker_ids=sorted(row["package_worker_ids"]),
                    processes=processes,
                )
            )
        demands.sort(
            key=lambda item: (
                -item.worker_deficit_count,
                -item.claimable_queued_count,
                -item.running_count,
                item.capability or "",
                item.operation_type or "",
                item.adapter_kind or "",
                item.resource_pool,
            )
        )
        return RuntimeCapabilityDemandSummary(
            run_id=run_id,
            generated_at=metrics.generated_at,
            count=len(demands),
            queued_count=sum(item.queued_count for item in demands),
            running_count=sum(item.running_count for item in demands),
            resource_blocked_count=sum(
                item.resource_blocked_count for item in demands
            ),
            claimable_queued_count=sum(
                item.claimable_queued_count for item in demands
            ),
            target_worker_count=sum(item.target_worker_count for item in demands),
            worker_deficit_count=sum(
                item.worker_deficit_count for item in demands
            ),
            demands=demands,
        )

    async def run_health(
        self,
        run_id: str,
        *,
        stale_after_seconds: float = 60.0,
    ) -> RuntimeRunHealth:
        metrics = await self.queue_metrics(
            run_id,
            stale_after_seconds=stale_after_seconds,
        )
        workers = await self.worker_health(
            run_id,
            stale_after_seconds=stale_after_seconds,
        )
        issues: list[RuntimeRunHealthIssue] = []
        for process in metrics.processes:
            if process.missing_worker_count:
                issues.append(
                    RuntimeRunHealthIssue(
                        code="missing_worker",
                        severity="critical",
                        message="Queued process instances have no healthy matching worker.",
                        count=process.missing_worker_count,
                        pipeline_id=process.pipeline_id,
                        process_id=process.process_id,
                        operation_type=process.operation_type,
                        document_id=process.oldest_queued_document_id,
                        data={
                            "healthy_worker_count": process.healthy_worker_count,
                            "matching_worker_count": process.matching_worker_count,
                            "resources": process.resources.model_dump(mode="json"),
                        },
                    )
                )
            if process.failed_count:
                issues.append(
                    RuntimeRunHealthIssue(
                        code="process_failed",
                        severity="critical",
                        message="Process instances failed.",
                        count=process.failed_count,
                        pipeline_id=process.pipeline_id,
                        process_id=process.process_id,
                        operation_type=process.operation_type,
                    )
                )
            if process.retry_backoff_count:
                issues.append(
                    RuntimeRunHealthIssue(
                        code="retry_backoff",
                        severity="warning",
                        message="Process instances are waiting for retry backoff.",
                        count=process.retry_backoff_count,
                        pipeline_id=process.pipeline_id,
                        process_id=process.process_id,
                        operation_type=process.operation_type,
                        document_id=process.next_retry_after_document_id,
                        data={
                            "next_retry_after": (
                                process.next_retry_after.isoformat()
                                if process.next_retry_after is not None
                                else None
                            )
                        },
                    )
                )
            if process.resource_blocked_count:
                issues.append(
                    RuntimeRunHealthIssue(
                        code="resource_quota_blocked",
                        severity="warning",
                        message="Queued process instances are blocked by resource pool quota.",
                        count=process.resource_blocked_count,
                        pipeline_id=process.pipeline_id,
                        process_id=process.process_id,
                        operation_type=process.operation_type,
                        document_id=process.oldest_queued_document_id,
                        data={
                            "resource_pool": process.resource_pool,
                            "resources": process.resources.model_dump(mode="json"),
                        },
                    )
                )
            if process.saturated:
                issues.append(
                    RuntimeRunHealthIssue(
                        code="capacity_saturated",
                        severity="warning",
                        message="Process concurrency capacity is saturated.",
                        count=max(1, process.running_count),
                        pipeline_id=process.pipeline_id,
                        process_id=process.process_id,
                        operation_type=process.operation_type,
                        data={
                            "capacity": process.capacity,
                            "capacity_remaining": process.capacity_remaining,
                        },
                    )
                )

        for pool in metrics.resource_pools:
            if pool.queued_count and pool.saturated:
                issues.append(
                    RuntimeRunHealthIssue(
                        code="resource_pool_saturated",
                        severity="warning",
                        message="Resource pool is saturated while work is queued.",
                        count=pool.queued_count,
                        data={
                            "resource_pool": pool.id,
                            "limit": pool.limit.model_dump(mode="json"),
                            "used": pool.used.model_dump(mode="json"),
                            "remaining": pool.remaining.model_dump(mode="json"),
                        },
                    )
                )

        state = await self.load_state_model(run_id, include_events=False)
        for issue in self._stream_backpressure_health_issues(state):
            issues.append(issue)

        stale_workers = [worker for worker in workers if not worker.healthy]
        for worker in stale_workers:
            issues.append(
                RuntimeRunHealthIssue(
                    code="worker_unhealthy",
                    severity="warning",
                    message="Worker heartbeat is stale or unhealthy.",
                    pipeline_id=worker.pipeline_id,
                    process_id=worker.process_id,
                    operation_type=None,
                    data={
                        "worker_id": worker.worker_id,
                        "status": worker.status.value,
                        "age_seconds": worker.age_seconds,
                    },
                )
            )

        critical_count = sum(1 for issue in issues if issue.severity == "critical")
        warning_count = sum(1 for issue in issues if issue.severity == "warning")
        status = "healthy"
        if warning_count:
            status = "warning"
        if critical_count:
            status = "critical"
        return RuntimeRunHealth(
            run_id=run_id,
            generated_at=datetime.now(timezone.utc),
            status=status,
            issue_count=len(issues),
            critical_count=critical_count,
            warning_count=warning_count,
            worker_count=len(workers),
            healthy_worker_count=sum(1 for worker in workers if worker.healthy),
            stale_worker_count=len(stale_workers),
            metrics=metrics,
            issues=issues,
        )

    def _stream_backpressure_health_issues(
        self,
        state: RuntimeState,
    ) -> list[RuntimeRunHealthIssue]:
        issues: list[RuntimeRunHealthIssue] = []
        for document in state.documents:
            if document.pipeline_id is None:
                continue
            try:
                pipeline = self.registry.get(document.pipeline_id)
            except Exception:
                continue
            package_id = self.registry.pipeline_package_id(document.pipeline_id)
            if package_id is None:
                continue
            try:
                package = self.registry.package(package_id)
            except Exception:
                continue
            capability_by_id = {item.id: item for item in package.capabilities}
            step_by_id = {item.id: item for item in pipeline.steps}
            for step in document.steps:
                step_spec = step_by_id.get(step.id)
                if step_spec is None or step_spec.capability is None:
                    continue
                capability = capability_by_id.get(step_spec.capability)
                if capability is None:
                    continue
                for stream in step.streams:
                    limits = [
                        spec.max_buffered_chunks
                        for spec in capability.emits_streams
                        if spec.stream_id == stream.stream_id
                        and spec.max_buffered_chunks is not None
                    ]
                    if not limits:
                        continue
                    limit = min(limits)
                    if stream.max_checkpoint_lag <= limit:
                        continue
                    issues.append(
                        RuntimeRunHealthIssue(
                            code="stream_backpressure",
                            severity="warning",
                            message=(
                                "Stream checkpoint lag exceeds configured "
                                "backpressure limit."
                            ),
                            count=stream.max_checkpoint_lag,
                            pipeline_id=document.pipeline_id,
                            document_id=document.document_id,
                            process_id=step.id,
                            operation_type=step.operation_type,
                            data={
                                "stream_id": stream.stream_id,
                                "max_buffered_chunks": limit,
                                "max_checkpoint_lag": stream.max_checkpoint_lag,
                                "checkpoint_lag": stream.checkpoint_lag,
                                "last_sequence": stream.last_sequence,
                                "min_checkpoint_sequence": (
                                    stream.min_checkpoint_sequence
                                ),
                            },
                        )
                    )
        return issues

    async def list_process_events(
        self,
        *,
        run_id: str,
        document_id: str | None = None,
        process_id: str | None = None,
        operation_type: str | None = None,
        after_event_id: str | None = None,
        limit: int | None = None,
        descending: bool = False,
    ) -> list[ProcessEvent]:
        events = await self.store.list_events(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            after_event_id=after_event_id,
            limit=None if operation_type is not None else limit,
            descending=descending,
        )
        enriched = await self._events_with_operation_type(events)
        if operation_type is not None:
            enriched = [
                event
                for event in enriched
                if event.operation_type == operation_type
            ]
            if limit is not None:
                enriched = enriched[:limit]
        return enriched

    async def count_process_events(
        self,
        *,
        run_id: str,
        document_id: str | None = None,
        process_id: str | None = None,
        operation_type: str | None = None,
    ) -> int:
        if operation_type is None:
            return await self.store.count_events(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
            )
        events = await self.list_process_events(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            operation_type=operation_type,
            limit=None,
        )
        return len(events)

    async def _events_with_operation_type(
        self,
        events: list[ProcessEvent],
    ) -> list[ProcessEvent]:
        operation_type_by_key: dict[tuple[str, str, str], str | None] = {}
        enriched: list[ProcessEvent] = []
        for event in events:
            operation_type: str | None = None
            if event.process_id is not None:
                key = (event.run_id, event.document_id, event.process_id)
                if key not in operation_type_by_key:
                    operation_type_by_key[key] = await self._operation_type_for_event(
                        run_id=event.run_id,
                        document_id=event.document_id,
                        process_id=event.process_id,
                    )
                operation_type = operation_type_by_key[key]
            enriched.append(event.model_copy(update={"operation_type": operation_type}))
        return enriched

    async def _operation_type_for_event(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
    ) -> str | None:
        pipeline_id = await self.store.get_document_pipeline_id(
            run_id=run_id,
            document_id=document_id,
        )
        if pipeline_id is None:
            return None
        try:
            pipeline = self.registry.get(pipeline_id)
        except Exception:
            return None
        step = next((item for item in pipeline.steps if item.id == process_id), None)
        return self._operation_type_for_step(pipeline, step)

    async def process_trace(
        self,
        run_id: str,
        *,
        document_id: str | None = None,
        process_id: str | None = None,
        operation_type: str | None = None,
    ) -> RuntimeTrace:
        state = await self.load_state_model(run_id, include_events=False)
        process_traces: list[RuntimeProcessTrace] = []
        for document in state.documents:
            if document_id is not None and document.document_id != document_id:
                continue
            for step in document.steps:
                if process_id is not None and step.id != process_id:
                    continue
                if operation_type is not None and step.operation_type != operation_type:
                    continue
                events = await self.list_process_events(
                    run_id=run_id,
                    document_id=document.document_id,
                    process_id=step.id,
                )
                attempts = _runtime_attempt_traces(events)
                process_traces.append(
                    RuntimeProcessTrace(
                        run_id=run_id,
                        document_id=document.document_id,
                        pipeline_id=document.pipeline_id,
                        process_id=step.id,
                        title=step.title,
                        capability=step.capability,
                        operation_type=step.operation_type,
                        adapter_kind=step.adapter_kind,
                        status=step.status,
                        current_claim=step.claim,
                        has_output=step.has_output,
                        output_value_keys=step.output_value_keys,
                        event_count=len(events),
                        attempt_count=sum(
                            1 for attempt in attempts if attempt.attempt is not None
                        ),
                        attempts=attempts,
                    )
                )

        process_traces.sort(
            key=lambda item: (
                item.document_id,
                item.process_id,
            )
        )
        return RuntimeTrace(
            run_id=run_id,
            generated_at=datetime.now(timezone.utc),
            document_id=document_id,
            process_id=process_id,
            operation_type=operation_type,
            process_count=len(process_traces),
            attempt_count=sum(item.attempt_count for item in process_traces),
            event_count=sum(item.event_count for item in process_traces),
            processes=process_traces,
        )

    async def list_run_summaries(self, *, limit: int = 50) -> list[dict[str, Any]]:
        rows = await self.store.list_runs(limit=limit)
        summaries: list[dict[str, Any]] = []
        for row in rows:
            run_id = str(row["run_id"])
            state = await self.load_state_model(run_id, include_events=False)
            current = _current_runtime_item(state)
            run_payload = row.get("run")
            run = RuntimeRun.model_validate(run_payload) if isinstance(run_payload, dict) else None
            status = _run_status_for_summary(run, state)
            summaries.append(
                {
                    "run_id": run_id,
                    "title": run.title if run else None,
                    "status": status.value,
                    "outcome": run.outcome.value if run and run.outcome else None,
                    "outcome_reason": run.outcome_reason if run else None,
                    "created_at": row.get("created_at"),
                    "updated_at": row.get("updated_at"),
                    "started_at": run.started_at.isoformat() if run and run.started_at else None,
                    "finished_at": run.finished_at.isoformat() if run and run.finished_at else None,
                    "summary": state.summary.model_dump(mode="json"),
                    "current_document_id": current.get("document_id"),
                    "current_process_id": current.get("process_id"),
                    "current_status": current.get("status"),
                    "pipeline_counts": state.summary.pipeline_counts,
                    "status_counts": state.summary.status_counts,
                    "project_id": (
                        _run_project_metadata(run.metadata).get("project_id")
                        if run is not None
                        else None
                    ),
                    "project": (
                        _run_project_metadata(run.metadata)
                        if run is not None
                        else None
                    ),
                    "document_type_counts": (
                        _run_document_type_counts(run.metadata)
                        if run is not None
                        else {}
                    ),
                }
            )
        return summaries

    def _runtime_artifact_roots(self) -> list[Path]:
        roots: list[Path] = list(self.artifact_roots)
        roots.extend(_registry_runtime_artifact_roots(self.registry))
        configured = os.environ.get("FALA_ARTIFACT_ROOTS")
        if configured:
            roots.extend(
                _resolve_runtime_artifact_root(item)
                for item in configured.split(os.pathsep)
                if item.strip()
            )
        runtime_root = os.environ.get("PROCESS_RUNTIME_ARTIFACT_ROOT")
        if runtime_root:
            roots.append(_resolve_runtime_artifact_root(runtime_root))
        runtime_dir = os.environ.get("PROCESS_RUNTIME_ARTIFACT_DIR")
        if runtime_dir:
            roots.append(_resolve_runtime_artifact_root(runtime_dir))
        roots.append((Path.cwd() / ".flow-runs" / "process-artifacts").resolve())
        unique: list[Path] = []
        seen: set[str] = set()
        for root in roots:
            key = str(root)
            if key not in seen:
                seen.add(key)
                unique.append(root)
        return unique


def _resolve_runtime_artifact_root(value: str | Path) -> Path:
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = Path.cwd() / path
    return path.resolve()


def _registry_runtime_artifact_roots(registry: PipelineRegistry) -> list[Path]:
    roots: list[Path] = []
    for pipeline in registry.all():
        source = registry.pipeline_source(pipeline.id)
        if source:
            roots.append(
                (Path(source).resolve().parent / ".flow-runs" / "process-artifacts")
                .resolve()
            )
        for step in pipeline.steps:
            roots.append(
                _configured_runtime_artifact_root(
                    cwd=step.adapter.cwd,
                    env=step.adapter.env,
                )
            )
    for package in registry.packages():
        for worker in package.workers:
            roots.append(
                _configured_runtime_artifact_root(
                    cwd=worker.cwd,
                    env=worker.env,
                )
            )
    return roots


def _configured_runtime_artifact_root(
    *,
    cwd: str | None,
    env: dict[str, str],
) -> Path:
    configured_root = env.get("PROCESS_RUNTIME_ARTIFACT_ROOT") or ".flow-runs/process-artifacts"
    path = Path(configured_root).expanduser()
    if not path.is_absolute():
        base = Path(cwd).expanduser() if cwd else Path.cwd()
        path = base / path
    return path.resolve()


def _validate_document_media_type(
    document: RuntimeDocumentInput,
    *,
    media_types: list[str],
) -> None:
    if not document.media_type or not media_types:
        return
    if any(_media_type_matches(document.media_type, allowed) for allowed in media_types):
        return
    accepted = ", ".join(sorted(media_types))
    raise ValueError(
        f"Document {document.document_id!r} media type {document.media_type!r} "
        f"is not accepted by document type {document.document_type!r} "
        f"(accepted: {accepted})"
    )


def _require_step(pipeline: PipelineSpec, process_id: str) -> ProcessSpec:
    for step in pipeline.steps:
        if step.id == process_id:
            return step
    raise ValueError(f"Unknown process id: {process_id}")


def _media_type_matches(actual: str, allowed: str) -> bool:
    actual_base = actual.split(";", 1)[0].strip().lower()
    allowed_base = allowed.split(";", 1)[0].strip().lower()
    if not actual_base or not allowed_base:
        return False
    if allowed_base in {"*/*", "application/octet-stream"}:
        return True
    if allowed_base.endswith("/*"):
        return actual_base.startswith(allowed_base[:-1])
    return actual_base == allowed_base


def _validate_document_extension(
    document: RuntimeDocumentInput,
    *,
    extensions: list[str],
) -> None:
    if not extensions:
        return
    extension = _document_extension(document)
    if extension is None:
        return
    accepted_extensions = {_normalize_extension(item) for item in extensions}
    if extension in accepted_extensions:
        return
    accepted = ", ".join(sorted(accepted_extensions))
    raise ValueError(
        f"Document {document.document_id!r} extension {extension!r} "
        f"is not accepted by document type {document.document_type!r} "
        f"(accepted: {accepted})"
    )


def _document_extension(document: RuntimeDocumentInput) -> str | None:
    for value in (document.source_uri, document.title, document.document_id):
        if not value:
            continue
        path = urlparse(value).path if "://" in value else value
        suffix = Path(unquote(path)).suffix
        if suffix:
            return _normalize_extension(suffix)
    return None


def _validate_artifact_media_type(
    artifact: ArtifactRef,
    *,
    media_types: list[str],
    process_id: str,
) -> None:
    media_type = _artifact_media_type(artifact)
    if media_type is None or not media_types:
        return
    if any(_media_type_matches(media_type, allowed) for allowed in media_types):
        return
    accepted = ", ".join(sorted(media_types))
    raise ValueError(
        f"Process {process_id!r} output artifact {artifact.id!r} "
        f"media type {media_type!r} is not accepted by artifact kind "
        f"{artifact.kind!r} (accepted: {accepted})"
    )


def _artifact_media_type(artifact: ArtifactRef) -> str | None:
    value = artifact.metadata.get("media_type") or artifact.metadata.get("content_type")
    return str(value) if value else None


def _validate_artifact_extension(
    artifact: ArtifactRef,
    *,
    extensions: list[str],
    process_id: str,
) -> None:
    if not extensions:
        return
    extension = _artifact_extension(artifact)
    if extension is None:
        return
    accepted_extensions = {_normalize_extension(item) for item in extensions}
    if extension in accepted_extensions:
        return
    accepted = ", ".join(sorted(accepted_extensions))
    raise ValueError(
        f"Process {process_id!r} output artifact {artifact.id!r} "
        f"extension {extension!r} is not accepted by artifact kind "
        f"{artifact.kind!r} (accepted: {accepted})"
    )


def _artifact_extension(artifact: ArtifactRef) -> str | None:
    values = [
        artifact.metadata.get("filename"),
        artifact.uri,
    ]
    for value in values:
        if not value:
            continue
        path = urlparse(str(value)).path if "://" in str(value) else str(value)
        suffix = Path(unquote(path)).suffix
        if suffix:
            return _normalize_extension(suffix)
    return None


def _validate_output_document_media_type(
    document: OutputDocumentRef,
    *,
    media_types: list[str],
    process_id: str,
) -> None:
    if not document.media_type or not media_types:
        return
    if any(_media_type_matches(document.media_type, allowed) for allowed in media_types):
        return
    accepted = ", ".join(sorted(media_types))
    raise ValueError(
        f"Process {process_id!r} output document {document.id!r} "
        f"media type {document.media_type!r} is not accepted by document type "
        f"{document.document_type!r} (accepted: {accepted})"
    )


def _validate_output_document_extension(
    document: OutputDocumentRef,
    *,
    extensions: list[str],
    process_id: str,
) -> None:
    if not extensions:
        return
    extension = _output_document_extension(document)
    if extension is None:
        return
    accepted_extensions = {_normalize_extension(item) for item in extensions}
    if extension in accepted_extensions:
        return
    accepted = ", ".join(sorted(accepted_extensions))
    raise ValueError(
        f"Process {process_id!r} output document {document.id!r} "
        f"extension {extension!r} is not accepted by document type "
        f"{document.document_type!r} (accepted: {accepted})"
    )


def _output_document_extension(document: OutputDocumentRef) -> str | None:
    for value in (
        document.metadata.get("filename"),
        document.uri,
        document.title,
        document.id,
    ):
        if not value:
            continue
        path = urlparse(str(value)).path if "://" in str(value) else str(value)
        suffix = Path(unquote(path)).suffix
        if suffix:
            return _normalize_extension(suffix)
    return None


def _normalize_extension(value: str) -> str:
    normalized = value.strip().lower()
    if normalized and not normalized.startswith("."):
        normalized = f".{normalized}"
    return normalized


def _runtime_document_from_input(
    *,
    run_id: str,
    pipeline_id: str,
    document: RuntimeDocumentInput,
) -> RuntimeDocument:
    now = datetime.now(timezone.utc)
    return RuntimeDocument(
        run_id=run_id,
        document_id=document.document_id,
        pipeline_id=pipeline_id,
        title=document.title,
        document_type=document.document_type,
        relation=document.relation,
        media_type=document.media_type,
        source_uri=document.source_uri,
        scheduled_at=document.scheduled_at,
        metadata=dict(document.metadata),
        parent_document_id=document.parent_document_id,
        parent_process_id=document.parent_process_id,
        created_at=now,
        updated_at=now,
    )


def _runtime_document_input_from_spawn(
    spawn: SpawnDocumentInput,
    *,
    parent_document_id: str,
    parent_process_id: str,
) -> RuntimeDocumentInput:
    return RuntimeDocumentInput(
        document_id=spawn.document_id,
        pipeline_id=spawn.pipeline_id,
        title=spawn.title,
        document_type=spawn.document_type,
        relation=spawn.relation,
        media_type=spawn.media_type,
        source_uri=spawn.source_uri,
        scheduled_at=spawn.scheduled_at,
        values=dict(spawn.values),
        metadata={
            **spawn.metadata,
            "parent_document_id": parent_document_id,
            "parent_process_id": parent_process_id,
            "relation": spawn.relation,
        },
        artifacts=list(spawn.artifacts),
        parent_document_id=parent_document_id,
        parent_process_id=parent_process_id,
    )


def _process_output_with_spawn_route_report(
    output: ProcessOutput,
    report: dict[str, Any],
) -> ProcessOutput:
    metadata = dict(output.metadata)
    namespace = metadata.get("process_runtime")
    runtime_metadata = dict(namespace) if isinstance(namespace, dict) else {}
    if namespace is not None and not isinstance(namespace, dict):
        runtime_metadata["user_metadata"] = namespace
    runtime_metadata["spawn_route_report"] = report
    metadata["process_runtime"] = runtime_metadata
    return output.model_copy(update={"metadata": metadata})


def _runtime_document_input_route_summary(
    document: RuntimeDocumentInput,
) -> dict[str, Any]:
    return {
        "pipeline_id": document.pipeline_id,
        "document_type": document.document_type,
        "relation": document.relation,
        "media_type": document.media_type,
        "source_uri": document.source_uri,
        "value_keys": sorted(document.values),
        "metadata_keys": sorted(document.metadata),
    }


def _spawn_route_for_document(
    report: dict[str, Any] | None,
    document_id: str,
) -> dict[str, Any] | None:
    if report is None:
        return None
    for decision in report.get("documents", []):
        if decision.get("document_id") == document_id:
            return decision
    return None


def _document_initial_values(document: RuntimeDocumentInput) -> dict[str, Any]:
    values = dict(document.values)
    descriptor = _document_descriptor(document)
    if descriptor and "document" not in values:
        values["document"] = descriptor
    return values


def _document_artifacts(document: RuntimeDocumentInput) -> list[ArtifactRef]:
    artifacts = list(document.artifacts)
    if document.source_uri is None:
        return artifacts
    metadata = dict(document.metadata)
    if document.title is not None:
        metadata.setdefault("title", document.title)
    if document.document_type is not None:
        metadata.setdefault("document_type", document.document_type)
    if document.relation is not None:
        metadata.setdefault("relation", document.relation)
    if document.media_type is not None:
        metadata.setdefault("media_type", document.media_type)
    metadata.setdefault("source", True)
    artifacts.insert(
        0,
        ArtifactRef(
            id=f"doc_{_artifact_id_prefix(document.document_id)}_source",
            kind=document.document_type or "source_document",
            uri=document.source_uri,
            metadata=metadata,
        ),
    )
    return artifacts


def _document_descriptor(document: RuntimeDocumentInput) -> dict[str, Any]:
    descriptor: dict[str, Any] = {"id": document.document_id}
    if document.title is not None:
        descriptor["title"] = document.title
    if document.document_type is not None:
        descriptor["type"] = document.document_type
    if document.relation is not None:
        descriptor["relation"] = document.relation
    if document.media_type is not None:
        descriptor["media_type"] = document.media_type
    if document.source_uri is not None:
        descriptor["source_uri"] = document.source_uri
    if document.parent_document_id is not None:
        descriptor["parent_document_id"] = document.parent_document_id
    if document.parent_process_id is not None:
        descriptor["parent_process_id"] = document.parent_process_id
    if document.metadata:
        descriptor["metadata"] = dict(document.metadata)
    return descriptor


def _artifact_id_prefix(value: str) -> str:
    return "".join(char if char.isalnum() or char in "_.-" else "_" for char in value).strip("._") or "document"


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _last_event_ts_by_process(events: list[ProcessEvent]) -> dict[str, datetime]:
    by_process: dict[str, datetime] = {}
    for event in events:
        if event.process_id is None:
            continue
        if event.process_id not in by_process or event.ts > by_process[event.process_id]:
            by_process[event.process_id] = event.ts
    return by_process


def _last_status_event_ts_by_process(
    events: list[ProcessEvent],
    *,
    status: ProcessStatus,
) -> dict[str, datetime]:
    by_process: dict[str, datetime] = {}
    for event in events:
        if event.process_id is None or event.status != status:
            continue
        if event.process_id not in by_process or event.ts > by_process[event.process_id]:
            by_process[event.process_id] = event.ts
    return by_process


def _latest_retry_after_by_process(events: list[ProcessEvent]) -> dict[str, datetime]:
    by_process: dict[str, datetime] = {}
    for event in events:
        if event.process_id is None:
            continue
        if event.type not in {"process.retry_scheduled", "process.claim_expired"}:
            continue
        retry_after = _parse_event_datetime(event.data.get("retry_after"))
        if retry_after is None:
            continue
        by_process[event.process_id] = retry_after
    return by_process


def _latest_retry_after_for_events(events: list[ProcessEvent]) -> datetime | None:
    retry_after: datetime | None = None
    latest_event_ts: datetime | None = None
    for event in events:
        if event.type not in {"process.retry_scheduled", "process.claim_expired"}:
            continue
        parsed = _parse_event_datetime(event.data.get("retry_after"))
        if parsed is not None and (
            latest_event_ts is None or event.ts >= latest_event_ts
        ):
            retry_after = parsed
            latest_event_ts = event.ts
    return retry_after


def _status_since(
    process: RuntimeProcessRecord,
    events: list[ProcessEvent],
) -> datetime | None:
    if not isinstance(process.status, ProcessStatus):
        return process.status_updated_at
    matching = [
        event.ts
        for event in events
        if event.status == process.status
    ]
    if matching:
        return max(matching)
    if process.status_updated_at is not None:
        return process.status_updated_at
    if events:
        return max(event.ts for event in events)
    return None


def _stuck_work_suggested_actions(
    status: ProcessStatus,
) -> list[ProcessAction]:
    if status == ProcessStatus.waiting:
        return [ProcessAction.retry, ProcessAction.skip, ProcessAction.cancel]
    if status == ProcessStatus.queued:
        return [ProcessAction.retry, ProcessAction.skip, ProcessAction.cancel]
    if status == ProcessStatus.running:
        return [ProcessAction.cancel, ProcessAction.retry]
    return []


def _process_sla_threshold(
    process: RuntimeProcessRecord,
    *,
    status: ProcessStatus,
    defaults: dict[ProcessStatus, float],
) -> float:
    if status == ProcessStatus.waiting:
        return (
            process.sla.waiting_after_seconds
            if process.sla.waiting_after_seconds is not None
            else defaults[status]
        )
    if status == ProcessStatus.queued:
        return (
            process.sla.queued_after_seconds
            if process.sla.queued_after_seconds is not None
            else defaults[status]
        )
    if status == ProcessStatus.running:
        return (
            process.sla.running_after_seconds
            if process.sla.running_after_seconds is not None
            else defaults[status]
        )
    return defaults[status]


def _stuck_work_sort_key(
    item: RuntimeStuckWorkItem,
) -> tuple[int, float, str, str, str]:
    severity_rank = 0 if item.severity == "critical" else 1
    age = item.status_age_seconds or 0.0
    return (
        severity_rank,
        -age,
        item.pipeline_id or "",
        item.document_id,
        item.process_id,
    )


def _stream_lag_sort_key(
    item: RuntimeStreamLagItem,
) -> tuple[int, int, str, str, str, str]:
    return (
        0 if item.over_limit else 1,
        -item.lag,
        item.pipeline_id or "",
        item.document_id,
        item.process_id,
        f"{item.stream_id}/{item.consumer_id or ''}",
    )


def _parse_event_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _run_metadata_with_provenance(
    metadata: dict[str, Any],
    *,
    input: RuntimeRunInput,
    plan: dict[str, Any],
    route_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    merged = dict(metadata)
    namespace = merged.get("process_runtime")
    runtime_metadata = dict(namespace) if isinstance(namespace, dict) else {}
    if namespace is not None and not isinstance(namespace, dict):
        runtime_metadata["user_metadata"] = namespace
    compact_plan = _compact_plan_snapshot(plan["plan"])
    contracts = plan.get("contracts", {})
    runtime_metadata["run_provenance"] = {
        "schema_version": 1,
        "run_input_sha256": _sha256_json(input.model_dump(mode="json")),
        "pipeline_contracts_sha256": _sha256_json(contracts),
        "plan_sha256": _sha256_json(compact_plan),
        "document_count": len(input.documents),
        "pipeline_ids": sorted(contracts),
        "document_summary": plan.get("document_summary", {}),
        "plan": compact_plan,
        "pipeline_contracts": contracts,
    }
    if route_report is not None:
        runtime_metadata["run_provenance"]["route_report_sha256"] = _sha256_json(
            route_report
        )
        runtime_metadata["run_provenance"]["route_report"] = route_report
    merged["process_runtime"] = runtime_metadata
    return merged


def _run_contract_drift_report(
    registry: PipelineRegistry,
    provenance: dict[str, Any] | None,
) -> dict[str, Any]:
    if provenance is None:
        return {
            "ok": False,
            "verifiable": False,
            "drifted": False,
            "status": "no_provenance",
            "stored_pipeline_ids": [],
            "current_pipeline_ids": [],
            "changed_pipeline_ids": [],
            "missing_pipeline_ids": [],
            "missing_snapshot_pipeline_ids": [],
            "stored_pipeline_contracts_sha256": None,
            "current_pipeline_contracts_sha256": None,
            "pipelines": [],
        }
    stored_contracts = provenance.get("pipeline_contracts")
    if not isinstance(stored_contracts, dict) or not stored_contracts:
        return {
            "ok": False,
            "verifiable": False,
            "drifted": False,
            "status": "unverifiable",
            "stored_pipeline_ids": [],
            "current_pipeline_ids": [],
            "changed_pipeline_ids": [],
            "missing_pipeline_ids": [],
            "missing_snapshot_pipeline_ids": [],
            "stored_pipeline_contracts_sha256": provenance.get(
                "pipeline_contracts_sha256"
            ),
            "current_pipeline_contracts_sha256": None,
            "pipelines": [],
        }

    rows: list[dict[str, Any]] = []
    current_contracts: dict[str, Any] = {}
    changed_pipeline_ids: list[str] = []
    missing_pipeline_ids: list[str] = []
    missing_snapshot_pipeline_ids: list[str] = []
    for pipeline_id in sorted(stored_contracts):
        raw_stored_contract = stored_contracts.get(pipeline_id)
        if not isinstance(raw_stored_contract, dict):
            missing_snapshot_pipeline_ids.append(str(pipeline_id))
            rows.append(
                {
                    "pipeline_id": str(pipeline_id),
                    "status": "missing_snapshot",
                    "stored_sha256": None,
                    "current_sha256": None,
                    "package_id": None,
                    "source": None,
                    "error": "Stored pipeline contract is not an object.",
                }
            )
            continue

        stored_contract = dict(raw_stored_contract)
        stored_sha256 = _sha256_json(stored_contract)
        try:
            current_contract = registry.pipeline_contract(str(pipeline_id))
        except Exception as exc:
            missing_pipeline_ids.append(str(pipeline_id))
            rows.append(
                {
                    "pipeline_id": str(pipeline_id),
                    "status": "missing_current",
                    "stored_sha256": stored_sha256,
                    "current_sha256": None,
                    "package_id": stored_contract.get("package_id"),
                    "source": stored_contract.get("source"),
                    "error": str(exc),
                }
            )
            continue

        current_sha256 = _sha256_json(current_contract)
        current_contracts[str(pipeline_id)] = current_contract
        changed = stored_sha256 != current_sha256
        if changed:
            changed_pipeline_ids.append(str(pipeline_id))
        rows.append(
            {
                "pipeline_id": str(pipeline_id),
                "status": "changed" if changed else "unchanged",
                "stored_sha256": stored_sha256,
                "current_sha256": current_sha256,
                "package_id": current_contract.get("package_id")
                or stored_contract.get("package_id"),
                "source": (
                    current_contract.get("source")
                    or stored_contract.get("source")
                ),
                "error": None,
            }
        )

    drifted = bool(
        changed_pipeline_ids
        or missing_pipeline_ids
        or missing_snapshot_pipeline_ids
    )
    current_complete = not missing_pipeline_ids and not missing_snapshot_pipeline_ids
    return {
        "ok": current_complete and not changed_pipeline_ids,
        "verifiable": True,
        "drifted": drifted,
        "status": "drifted" if drifted else "current",
        "stored_pipeline_ids": sorted(str(key) for key in stored_contracts),
        "current_pipeline_ids": sorted(current_contracts),
        "changed_pipeline_ids": changed_pipeline_ids,
        "missing_pipeline_ids": missing_pipeline_ids,
        "missing_snapshot_pipeline_ids": missing_snapshot_pipeline_ids,
        "stored_pipeline_contracts_sha256": provenance.get(
            "pipeline_contracts_sha256"
        ),
        "current_pipeline_contracts_sha256": (
            _sha256_json(current_contracts) if current_complete else None
        ),
        "pipelines": rows,
    }


def _run_project_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    process_runtime = metadata.get("process_runtime")
    if not isinstance(process_runtime, dict):
        return {"project_id": metadata.get("project_id")} if metadata.get("project_id") else {}
    project = process_runtime.get("project")
    if not isinstance(project, dict):
        return {"project_id": metadata.get("project_id")} if metadata.get("project_id") else {}
    result = dict(project)
    if not result.get("project_id") and metadata.get("project_id"):
        result["project_id"] = metadata["project_id"]
    return result


def _run_document_type_counts(metadata: dict[str, Any]) -> dict[str, int]:
    process_runtime = metadata.get("process_runtime")
    if not isinstance(process_runtime, dict):
        return {}
    provenance = process_runtime.get("run_provenance")
    if not isinstance(provenance, dict):
        return {}
    document_summary = provenance.get("document_summary")
    if not isinstance(document_summary, dict):
        return {}
    counts = document_summary.get("document_type_counts")
    if not isinstance(counts, dict):
        return {}
    return {
        str(key): int(value)
        for key, value in counts.items()
        if isinstance(value, int | float)
    }


def _run_metadata_with_append_provenance(
    metadata: dict[str, Any],
    *,
    pipeline_id: str | None,
    documents: list[RuntimeDocumentInput],
    schedules: list[ScheduleResult],
    existing_document_policy: ExistingDocumentPolicy,
    route_report: dict[str, Any] | None,
) -> dict[str, Any]:
    merged = dict(metadata)
    namespace = merged.get("process_runtime")
    runtime_metadata = dict(namespace) if isinstance(namespace, dict) else {}
    if namespace is not None and not isinstance(namespace, dict):
        runtime_metadata["user_metadata"] = namespace
    provenance = runtime_metadata.get("run_provenance")
    if isinstance(provenance, dict):
        provenance = dict(provenance)
    else:
        provenance = {"schema_version": 1}
    append_batches = provenance.get("append_batches")
    append_batches = list(append_batches) if isinstance(append_batches, list) else []
    append_input = {
        "pipeline_id": pipeline_id,
        "existing_document_policy": existing_document_policy,
        "documents": [document.model_dump(mode="json") for document in documents],
    }
    batch: dict[str, Any] = {
        "batch_id": f"append-{len(append_batches) + 1:04d}",
        "appended_at": datetime.now(timezone.utc).isoformat(),
        "append_input_sha256": _sha256_json(append_input),
        "pipeline_id": pipeline_id,
        "existing_document_policy": existing_document_policy,
        "document_count": len(documents),
        "scheduled_count": len(schedules),
        "document_ids": [document.document_id for document in documents],
        "document_summary": _runtime_document_input_summary(
            documents=documents,
            pipeline_id=pipeline_id,
        ),
        "schedule_summary": _append_schedule_summary(schedules),
    }
    if route_report is not None:
        batch["route_report_sha256"] = _sha256_json(route_report)
        batch["route_report"] = route_report
    append_batches.append(batch)
    provenance["append_batches"] = append_batches
    provenance["append_batch_count"] = len(append_batches)
    runtime_metadata["run_provenance"] = provenance
    merged["process_runtime"] = runtime_metadata
    return merged


def _append_schedule_summary(schedules: list[ScheduleResult]) -> dict[str, Any]:
    pipeline_counts: Counter[str] = Counter()
    queued_counts: Counter[str] = Counter()
    waiting_counts: Counter[str] = Counter()
    completed_counts: Counter[str] = Counter()
    failed_counts: Counter[str] = Counter()
    skipped_counts: Counter[str] = Counter()
    cancelled_counts: Counter[str] = Counter()
    for schedule in schedules:
        pipeline_counts[schedule.pipeline_id] += 1
        queued_counts[schedule.pipeline_id] += len(schedule.queued)
        waiting_counts[schedule.pipeline_id] += len(schedule.waiting)
        completed_counts[schedule.pipeline_id] += len(schedule.completed)
        failed_counts[schedule.pipeline_id] += len(schedule.failed)
        skipped_counts[schedule.pipeline_id] += len(schedule.skipped)
        cancelled_counts[schedule.pipeline_id] += len(schedule.cancelled)
    return {
        "schedule_count": len(schedules),
        "pipeline_counts": _sorted_counts(pipeline_counts),
        "queued_counts": _sorted_counts(queued_counts),
        "waiting_counts": _sorted_counts(waiting_counts),
        "completed_counts": _sorted_counts(completed_counts),
        "failed_counts": _sorted_counts(failed_counts),
        "skipped_counts": _sorted_counts(skipped_counts),
        "cancelled_counts": _sorted_counts(cancelled_counts),
    }


def _compact_plan_snapshot(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "document_count": plan["document_count"],
        "process_instance_count": plan["process_instance_count"],
        "queued_count": plan["queued_count"],
        "waiting_count": plan["waiting_count"],
        "process_group_count": plan["process_group_count"],
        "resource_pool_count": plan["resource_pool_count"],
        "worker_demand_count": plan["worker_demand_count"],
        "processes": plan["processes"],
        "resource_pools": plan["resource_pools"],
        "worker_demands": plan["worker_demands"],
    }


def _sha256_json(value: Any) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _run_resource_pools(config: dict[str, Any]) -> dict[str, ResourceSpec]:
    raw = config.get("resource_pools")
    if raw is None:
        return {}
    if isinstance(raw, dict):
        pools: dict[str, ResourceSpec] = {}
        for pool_id, value in raw.items():
            if isinstance(value, dict) and "resources" in value:
                payload = dict(value)
                payload.setdefault("id", pool_id)
                pool = ResourcePoolSpec.model_validate(payload)
                pools[pool.id] = pool.resources
                continue
            pools[str(pool_id)] = ResourceSpec.model_validate(value)
        return pools
    if isinstance(raw, list):
        pools = {}
        for item in raw:
            pool = ResourcePoolSpec.model_validate(item)
            pools[pool.id] = pool.resources
        return pools
    raise ValueError("run config resource_pools must be an object or list")


def _resource_pool_usage_from_state(
    state: RuntimeState,
) -> tuple[dict[str, ResourceQuantity], dict[str, int], dict[str, int]]:
    usage: dict[str, ResourceQuantity] = {}
    running_counts: dict[str, int] = {}
    queued_counts: dict[str, int] = {}
    for document in state.documents:
        for step in document.steps:
            status = (
                step.status.value
                if isinstance(step.status, ProcessStatus)
                else str(step.status)
            )
            pool_id = step.resource_pool or "default"
            if status == ProcessStatus.running.value:
                usage[pool_id] = resource_usage_add(
                    usage.get(pool_id, ResourceQuantity()),
                    step.resources,
                )
                running_counts[pool_id] = running_counts.get(pool_id, 0) + 1
            elif status == ProcessStatus.queued.value:
                queued_counts[pool_id] = queued_counts.get(pool_id, 0) + 1
    return usage, running_counts, queued_counts


def _runtime_resource_pool_metrics(
    *,
    resource_pools: dict[str, ResourceSpec],
    pool_usage: dict[str, ResourceQuantity],
    running_counts: dict[str, int],
    queued_counts: dict[str, int],
) -> list[RuntimeResourcePoolMetrics]:
    pool_ids = sorted(
        set(resource_pools)
        | set(pool_usage)
        | set(running_counts)
        | set(queued_counts)
    )
    metrics: list[RuntimeResourcePoolMetrics] = []
    for pool_id in pool_ids:
        limit = resource_pools.get(pool_id, ResourceSpec())
        used = pool_usage.get(pool_id, ResourceQuantity())
        metrics.append(
            RuntimeResourcePoolMetrics(
                id=pool_id,
                limit=limit,
                used=used,
                remaining=resource_usage_remaining(limit=limit, used=used),
                running_count=running_counts.get(pool_id, 0),
                queued_count=queued_counts.get(pool_id, 0),
                saturated=resource_pool_saturated(limit=limit, used=used),
            )
        )
    return metrics


def _runtime_worker_demand(
    process: RuntimeProcessMetrics,
    *,
    package_worker_ids: list[str],
) -> RuntimeWorkerDemand | None:
    if process.adapter_kind != "queue":
        return None
    claimable_queued_count = max(
        0,
        process.queued_count - process.resource_blocked_count,
    )
    if (
        process.queued_count == 0
        and process.running_count == 0
        and process.resource_blocked_count == 0
    ):
        return None
    unconstrained_target = process.running_count + claimable_queued_count
    target_worker_count = (
        min(process.capacity, unconstrained_target)
        if process.capacity is not None
        else unconstrained_target
    )
    worker_deficit_count = max(
        0,
        target_worker_count - process.healthy_worker_count,
    )
    return RuntimeWorkerDemand(
        pipeline_id=process.pipeline_id,
        process_id=process.process_id,
        title=process.title,
        capability=process.capability,
        operation_type=process.operation_type,
        adapter_kind=process.adapter_kind,
        resource_pool=process.resource_pool,
        resources=process.resources,
        queued_count=process.queued_count,
        running_count=process.running_count,
        resource_blocked_count=process.resource_blocked_count,
        claimable_queued_count=claimable_queued_count,
        matching_worker_count=process.matching_worker_count,
        healthy_worker_count=process.healthy_worker_count,
        target_worker_count=target_worker_count,
        worker_deficit_count=worker_deficit_count,
        capacity=process.capacity,
        capacity_remaining=process.capacity_remaining,
        package_worker_ids=package_worker_ids,
    )


def _package_worker_ids_for_process(
    registry: PipelineRegistry,
    process: RuntimeProcessMetrics,
) -> list[str]:
    if process.pipeline_id is None:
        return []
    package_id = registry.pipeline_package_id(process.pipeline_id)
    if package_id is None:
        return []
    try:
        package = registry.package(package_id)
    except Exception:
        return []
    worker_ids: list[str] = []
    for worker in package.workers:
        if worker.pipeline_id != process.pipeline_id:
            continue
        if worker.process_id is not None and worker.process_id != process.process_id:
            continue
        if worker.adapter_kind != process.adapter_kind:
            continue
        capability_set = set(worker.capabilities)
        if capability_set and process.capability not in capability_set:
            continue
        if not resources_compatible(process.resources, worker.resources):
            continue
        worker_ids.append(worker.id)
    return sorted(worker_ids)


def _package_worker_ids_for_step(
    registry: PipelineRegistry,
    *,
    pipeline_id: str,
    step: ProcessSpec,
) -> list[str]:
    package_id = registry.pipeline_package_id(pipeline_id)
    if package_id is None:
        return []
    try:
        package = registry.package(package_id)
    except Exception:
        return []
    worker_ids: list[str] = []
    for worker in package.workers:
        if worker.pipeline_id != pipeline_id:
            continue
        if worker.process_id is not None and worker.process_id != step.id:
            continue
        if worker.adapter_kind != step.adapter.kind:
            continue
        capability_set = set(worker.capabilities)
        if capability_set and step.capability not in capability_set:
            continue
        if not resources_compatible(step.resources, worker.resources):
            continue
        worker_ids.append(worker.id)
    return sorted(worker_ids)


def _planned_step(
    *,
    registry: PipelineRegistry,
    pipeline_id: str,
    step: ProcessSpec,
    status: str,
) -> dict[str, Any]:
    return {
        "process_id": step.id,
        "title": step.title,
        "status": status,
        "manual_required": step.adapter.kind == "manual",
        "capability": step.capability,
        "adapter_kind": step.adapter.kind,
        "needs": step.needs,
        "priority": step.priority,
        "max_concurrency": step.max_concurrency,
        "resource_pool": step.resource_pool,
        "resources": step.resources.model_dump(mode="json"),
        "declared_worker_ids": _package_worker_ids_for_step(
            registry,
            pipeline_id=pipeline_id,
            step=step,
        ),
    }


def _accumulate_process_plan(
    plans: dict[tuple[str, str], dict[str, Any]],
    *,
    registry: PipelineRegistry,
    pipeline_id: str,
    step: ProcessSpec,
    status: str,
) -> None:
    key = (pipeline_id, step.id)
    plan = plans.setdefault(
        key,
        {
            "pipeline_id": pipeline_id,
            "process_id": step.id,
            "title": step.title,
            "capability": step.capability,
            "adapter_kind": step.adapter.kind,
            "manual_required": step.adapter.kind == "manual",
            "priority": step.priority,
            "max_concurrency": step.max_concurrency,
            "resource_pool": step.resource_pool,
            "resources": step.resources,
            "counts": Counter(),
            "declared_worker_ids": _package_worker_ids_for_step(
                registry,
                pipeline_id=pipeline_id,
                step=step,
            ),
            "queued_resource_total": ResourceQuantity(),
            "total_resource_request": ResourceQuantity(),
            "resource_labels": set(),
        },
    )
    plan["counts"][status] += 1
    plan["resource_labels"].update(step.resources.labels)
    request = _resource_request_quantity(step.resources)
    plan["total_resource_request"] = _resource_quantity_add(
        plan["total_resource_request"],
        request,
    )
    if status == "queued":
        plan["queued_resource_total"] = _resource_quantity_add(
            plan["queued_resource_total"],
            request,
        )


def _finalize_process_plan(plan: dict[str, Any]) -> dict[str, Any]:
    counts = _sorted_counts(plan["counts"])
    queued_count = counts.get("queued", 0)
    waiting_count = counts.get("waiting", 0)
    skipped_count = counts.get("skipped", 0)
    active_count = queued_count + waiting_count
    total_count = active_count + skipped_count
    capacity = plan["max_concurrency"]
    declared_worker_ids = list(plan["declared_worker_ids"])
    initial_target = min(capacity, queued_count) if capacity is not None else queued_count
    eventual_target = min(capacity, active_count) if capacity is not None else active_count
    return {
        "pipeline_id": plan["pipeline_id"],
        "process_id": plan["process_id"],
        "title": plan["title"],
        "capability": plan["capability"],
        "adapter_kind": plan["adapter_kind"],
        "manual_required": plan["manual_required"],
        "priority": plan["priority"],
        "max_concurrency": capacity,
        "resource_pool": plan["resource_pool"],
        "resources": plan["resources"].model_dump(mode="json"),
        "counts": counts,
        "queued_count": queued_count,
        "waiting_count": waiting_count,
        "skipped_count": skipped_count,
        "planned_count": total_count,
        "declared_worker_ids": declared_worker_ids,
        "declared_worker_count": len(declared_worker_ids),
        "missing_declared_worker": (
            plan["adapter_kind"] == "queue" and not declared_worker_ids
        ),
        "initial_target_worker_count": initial_target,
        "eventual_target_worker_count": eventual_target,
        "queued_resource_total": plan["queued_resource_total"].model_dump(mode="json"),
        "total_resource_request": plan["total_resource_request"].model_dump(mode="json"),
        "resource_labels": sorted(plan["resource_labels"]),
    }


def _accumulate_pool_plan(
    plans: dict[str, dict[str, Any]],
    *,
    pipeline_id: str,
    pool_id: str,
    step: ProcessSpec,
    status: str,
) -> None:
    plan = plans.setdefault(
        pool_id,
        {
            "id": pool_id,
            "counts": Counter(),
            "process_refs": set(),
            "queued_resource_total": ResourceQuantity(),
            "total_resource_request": ResourceQuantity(),
            "resource_labels": set(),
        },
    )
    plan["counts"][status] += 1
    plan["process_refs"].add(f"{pipeline_id}.{step.id}")
    plan["resource_labels"].update(step.resources.labels)
    request = _resource_request_quantity(step.resources)
    plan["total_resource_request"] = _resource_quantity_add(
        plan["total_resource_request"],
        request,
    )
    if status == "queued":
        plan["queued_resource_total"] = _resource_quantity_add(
            plan["queued_resource_total"],
            request,
        )


def _finalize_pool_plan(
    plan: dict[str, Any],
    *,
    limit: ResourceSpec,
) -> dict[str, Any]:
    counts = _sorted_counts(plan["counts"])
    return {
        "id": plan["id"],
        "limit": limit.model_dump(mode="json"),
        "counts": counts,
        "queued_count": counts.get("queued", 0),
        "waiting_count": counts.get("waiting", 0),
        "skipped_count": counts.get("skipped", 0),
        "planned_count": sum(counts.values()),
        "process_refs": sorted(plan["process_refs"]),
        "queued_resource_total": plan["queued_resource_total"].model_dump(mode="json"),
        "total_resource_request": plan["total_resource_request"].model_dump(mode="json"),
        "resource_labels": sorted(plan["resource_labels"]),
    }


def _planned_worker_demand(process: dict[str, Any]) -> dict[str, Any] | None:
    if process["adapter_kind"] != "queue":
        return None
    if process["queued_count"] == 0 and process["waiting_count"] == 0:
        return None
    return {
        "pipeline_id": process["pipeline_id"],
        "process_id": process["process_id"],
        "title": process["title"],
        "capability": process["capability"],
        "adapter_kind": process["adapter_kind"],
        "resource_pool": process["resource_pool"],
        "resources": process["resources"],
        "queued_count": process["queued_count"],
        "waiting_count": process["waiting_count"],
        "planned_count": process["planned_count"],
        "initial_target_worker_count": process["initial_target_worker_count"],
        "eventual_target_worker_count": process["eventual_target_worker_count"],
        "declared_worker_ids": process["declared_worker_ids"],
        "declared_worker_count": process["declared_worker_count"],
        "missing_declared_worker": process["missing_declared_worker"],
    }


def _run_reduce_output(
    spec: RunReduceSpec,
    results: RuntimeRunResults,
) -> ProcessOutput:
    metadata = {
        "process_runtime": {
            "run_reduce": {
                "schema_version": 1,
                "reduce_id": spec.id,
                "mode": spec.mode,
                "filters": results.filters,
                "result_count": results.count,
            }
        }
    }
    if spec.mode == "count":
        by_process = Counter(item.process_id for item in results.results)
        by_document_type = Counter(
            item.document_type or "unknown" for item in results.results
        )
        by_pipeline = Counter(item.pipeline_id or "unknown" for item in results.results)
        return ProcessOutput(
            values={
                "count": results.count,
                "by_process": dict(sorted(by_process.items())),
                "by_document_type": dict(sorted(by_document_type.items())),
                "by_pipeline": dict(sorted(by_pipeline.items())),
            },
            metadata=metadata,
        )

    if spec.mode == "collect_outputs":
        return ProcessOutput(
            values={
                "count": results.count,
                "items": [
                    {
                        "document_id": item.document_id,
                        "pipeline_id": item.pipeline_id,
                        "process_id": item.process_id,
                        "document_type": item.document_type,
                        "parent_document_id": item.parent_document_id,
                        "output": item.output.model_dump(mode="json"),
                    }
                    for item in results.results
                ],
            },
            metadata=metadata,
        )

    items: list[dict[str, Any]] = []
    for item in results.results:
        row: dict[str, Any] = {
            "document_id": item.document_id,
            "pipeline_id": item.pipeline_id,
            "process_id": item.process_id,
            "document_type": item.document_type,
            "parent_document_id": item.parent_document_id,
        }
        if spec.value_key is not None:
            row["value_key"] = spec.value_key
            row["value"] = item.output.values.get(spec.value_key)
        else:
            row["values"] = item.output.values
        if spec.include_artifacts:
            row["artifacts"] = [
                artifact.model_dump(mode="json") for artifact in item.output.artifacts
            ]
        items.append(row)

    return ProcessOutput(
        values={"count": results.count, "items": items},
        metadata=metadata,
    )


def _add_artifact_ref_digests(
    digests: set[str],
    artifacts: list[ArtifactRef],
) -> None:
    for artifact in artifacts:
        _add_fala_artifact_digest(digests, artifact.uri)


def _add_fala_artifact_digest(digests: set[str], uri: str) -> None:
    digest = digest_from_fala_artifact_uri(uri)
    if digest is not None:
        digests.add(digest)


def _row_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _ensure_aware_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _resource_request_quantity(resources: ResourceSpec) -> ResourceQuantity:
    return ResourceQuantity(
        cpu_cores=resources.cpu_cores or 0,
        memory_mb=resources.memory_mb or 0,
        disk_mb=resources.disk_mb or 0,
        gpu_count=resources.gpu_count or 0,
        units=dict(resources.units),
    )


def _resource_quantity_add(
    lhs: ResourceQuantity,
    rhs: ResourceQuantity,
) -> ResourceQuantity:
    units = dict(lhs.units)
    for key, value in rhs.units.items():
        units[key] = units.get(key, 0) + value
    return ResourceQuantity(
        cpu_cores=lhs.cpu_cores + rhs.cpu_cores,
        memory_mb=lhs.memory_mb + rhs.memory_mb,
        disk_mb=lhs.disk_mb + rhs.disk_mb,
        gpu_count=lhs.gpu_count + rhs.gpu_count,
        units=units,
    )


def _runtime_process_metrics(metrics: dict[str, Any]) -> RuntimeProcessMetrics:
    counts = dict(metrics["counts"])
    running_count = counts.get(ProcessStatus.running.value, 0)
    max_concurrency = metrics["max_concurrency"]
    capacity_remaining = (
        None
        if max_concurrency is None
        else max(0, max_concurrency - running_count)
    )
    return RuntimeProcessMetrics(
        pipeline_id=metrics["pipeline_id"],
        process_id=metrics["process_id"],
        title=metrics["title"],
        capability=metrics["capability"],
        operation_type=metrics["operation_type"],
        adapter_kind=metrics["adapter_kind"],
        priority=metrics["priority"],
        max_concurrency=max_concurrency,
        resource_pool=metrics["resource_pool"],
        resources=metrics["resources"],
        counts=counts,
        waiting_count=counts.get(ProcessStatus.waiting.value, 0),
        queued_count=counts.get(ProcessStatus.queued.value, 0),
        running_count=running_count,
        completed_count=counts.get(ProcessStatus.completed.value, 0),
        failed_count=counts.get(ProcessStatus.failed.value, 0),
        skipped_count=counts.get(ProcessStatus.skipped.value, 0),
        cancelled_count=counts.get(ProcessStatus.cancelled.value, 0),
        retry_backoff_count=metrics["retry_backoff_count"],
        missing_worker_count=metrics["missing_worker_count"],
        resource_blocked_count=metrics["resource_blocked_count"],
        claim_count=metrics["claim_count"],
        output_count=metrics["output_count"],
        matching_worker_count=metrics["matching_worker_count"],
        healthy_worker_count=metrics["healthy_worker_count"],
        capacity=max_concurrency,
        capacity_remaining=capacity_remaining,
        saturated=(
            max_concurrency is not None and running_count >= max_concurrency
        ),
        missing_worker=metrics["missing_worker_count"] > 0,
        resource_blocked=metrics["resource_blocked_count"] > 0,
        next_retry_after=metrics["next_retry_after"],
        next_retry_after_document_id=metrics["next_retry_after_document_id"],
        oldest_queued_at=metrics["oldest_queued_at"],
        oldest_queued_document_id=metrics["oldest_queued_document_id"],
        oldest_running_at=metrics["oldest_running_at"],
        oldest_running_document_id=metrics["oldest_running_document_id"],
        last_event_at=metrics["last_event_at"],
    )


def _matching_workers(
    workers: list[RuntimeWorkerState],
    *,
    pipeline_id: str | None,
    step: Any,
) -> list[RuntimeWorkerState]:
    return [
        worker
        for worker in workers
        if _worker_matches_step(worker, pipeline_id=pipeline_id, step=step)
    ]


def _worker_matches_step(
    worker: RuntimeWorkerState,
    *,
    pipeline_id: str | None,
    step: Any,
) -> bool:
    if worker.pipeline_id is not None and worker.pipeline_id != pipeline_id:
        return False
    if worker.process_id is not None and worker.process_id != step.id:
        return False
    if worker.adapter_kind is not None and worker.adapter_kind != step.adapter_kind:
        return False
    capability_set = set(worker.capabilities)
    if capability_set and step.capability not in capability_set:
        return False
    if not resources_compatible(step.resources, worker.resources):
        return False
    return True


def _runtime_attempt_traces(events: list[ProcessEvent]) -> list[RuntimeAttemptTrace]:
    grouped: dict[int | None, list[ProcessEvent]] = {}
    for event in sorted(events, key=lambda item: item.ts):
        attempt = _event_attempt(event)
        grouped.setdefault(attempt, []).append(event)
    traces = [
        _runtime_attempt_trace(attempt=attempt, events=items)
        for attempt, items in grouped.items()
    ]
    traces.sort(key=lambda item: (-1 if item.attempt is None else item.attempt))
    return traces


def _runtime_attempt_trace(
    *,
    attempt: int | None,
    events: list[ProcessEvent],
) -> RuntimeAttemptTrace:
    worker_id = next(
        (
            str(event.data["worker_id"])
            for event in events
            if event.data.get("worker_id") is not None
        ),
        None,
    )
    status = next(
        (
            event.status
            for event in reversed(events)
            if event.status is not None
        ),
        None,
    )
    started_at = next(
        (
            event.ts
            for event in events
            if event.type in {"process.claimed", "process.started"}
        ),
        events[0].ts if events else None,
    )
    finished_at = next(
        (
            event.ts
            for event in reversed(events)
            if event.status
            in {
                ProcessStatus.completed,
                ProcessStatus.failed,
                ProcessStatus.skipped,
                ProcessStatus.cancelled,
            }
            or event.type
            in {
                "process.completed",
                "process.failed",
                "process.skip_requested",
                "process.cancel_requested",
            }
        ),
        None,
    )
    duration_seconds = (
        (finished_at - started_at).total_seconds()
        if started_at is not None and finished_at is not None
        else None
    )
    return RuntimeAttemptTrace(
        attempt=attempt,
        worker_id=worker_id,
        status=status,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=duration_seconds,
        event_count=len(events),
        event_types=[event.type for event in events],
        events=events,
    )


def _event_attempt(event: ProcessEvent) -> int | None:
    value = event.data.get("attempt")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _runtime_document_input_summary(
    *,
    documents: list[RuntimeDocumentInput],
    pipeline_id: str | None,
) -> dict[str, Any]:
    pipeline_counts: Counter[str] = Counter()
    document_type_counts: Counter[str] = Counter()
    media_type_counts: Counter[str] = Counter()
    source_scheme_counts: Counter[str] = Counter()
    artifact_kind_counts: Counter[str] = Counter()
    value_keys: set[str] = set()
    metadata_keys: set[str] = set()
    with_source_uri_count = 0
    with_source_sha256_count = 0
    missing_document_type_count = 0
    missing_media_type_count = 0
    for document in documents:
        pipeline_counts[document.pipeline_id or pipeline_id or "<missing>"] += 1
        if document.document_type:
            document_type_counts[document.document_type] += 1
        else:
            missing_document_type_count += 1
        if document.media_type:
            media_type_counts[document.media_type] += 1
        else:
            missing_media_type_count += 1
        if document.source_uri:
            with_source_uri_count += 1
            parsed = urlparse(document.source_uri)
            source_scheme_counts[parsed.scheme or "<none>"] += 1
        else:
            source_scheme_counts["<missing>"] += 1
        if document.metadata.get("source_sha256"):
            with_source_sha256_count += 1
        value_keys.update(document.values)
        metadata_keys.update(document.metadata)
        for artifact in document.artifacts:
            artifact_kind_counts[artifact.kind] += 1
    return {
        "document_count": len(documents),
        "pipeline_counts": _sorted_counts(pipeline_counts),
        "document_type_counts": _sorted_counts(document_type_counts),
        "media_type_counts": _sorted_counts(media_type_counts),
        "source_scheme_counts": _sorted_counts(source_scheme_counts),
        "artifact_kind_counts": _sorted_counts(artifact_kind_counts),
        "value_keys": sorted(value_keys),
        "metadata_keys": sorted(metadata_keys),
        "with_source_uri_count": with_source_uri_count,
        "with_source_sha256_count": with_source_sha256_count,
        "missing_document_type_count": missing_document_type_count,
        "missing_media_type_count": missing_media_type_count,
    }


def _sorted_counts(counter: Counter[str]) -> dict[str, int]:
    return {key: counter[key] for key in sorted(counter)}


def _runtime_state_status(state: RuntimeState) -> str:
    counts = state.summary.status_counts
    for status in ("running", "queued", "waiting", "failed", "cancelled"):
        if counts.get(status, 0) > 0:
            return status
    if state.summary.process_count > 0 and (
        counts.get("completed", 0) + counts.get("skipped", 0)
    ) >= state.summary.process_count:
        return "completed"
    if state.summary.document_count > 0:
        return "idle"
    return "empty"


def _run_status_from_runtime_state(state: RuntimeState) -> RunStatus:
    status = _runtime_state_status(state)
    if status == "running":
        return RunStatus.running
    if status in {"queued", "waiting"}:
        return RunStatus.queued
    if status == "failed":
        return RunStatus.failed
    if status == "cancelled":
        return RunStatus.cancelled
    if status == "completed":
        return RunStatus.completed
    return RunStatus.created


def _run_status_for_summary(run: RuntimeRun | None, state: RuntimeState) -> RunStatus:
    if run is not None and run.status == RunStatus.paused:
        runtime_status = _run_status_from_runtime_state(state)
        if runtime_status not in {
            RunStatus.completed,
            RunStatus.failed,
            RunStatus.cancelled,
        }:
            return RunStatus.paused
    if run is not None and run.status == RunStatus.cancelled:
        return RunStatus.cancelled
    if state.documents:
        return _run_status_from_runtime_state(state)
    if run is not None:
        return run.status
    return RunStatus.created


def _worker_state(
    heartbeat: RuntimeWorkerHeartbeat,
    *,
    now: datetime,
    stale_after_seconds: float,
) -> RuntimeWorkerState:
    last_seen_at = heartbeat.last_seen_at
    if last_seen_at.tzinfo is None:
        last_seen_at = last_seen_at.replace(tzinfo=timezone.utc)
    age_seconds = max(0.0, (now - last_seen_at).total_seconds())
    healthy = (
        age_seconds <= stale_after_seconds
        and heartbeat.status not in {RuntimeWorkerStatus.error, RuntimeWorkerStatus.stopped}
    )
    return RuntimeWorkerState(
        run_id=heartbeat.run_id,
        worker_id=heartbeat.worker_id,
        pipeline_id=heartbeat.pipeline_id,
        process_id=heartbeat.process_id,
        adapter_kind=heartbeat.adapter_kind,
        capabilities=heartbeat.capabilities,
        resources=heartbeat.resources,
        status=heartbeat.status,
        current_document_id=heartbeat.current_document_id,
        current_process_id=heartbeat.current_process_id,
        started_at=heartbeat.started_at,
        last_seen_at=last_seen_at,
        age_seconds=age_seconds,
        stale_after_seconds=stale_after_seconds,
        healthy=healthy,
        metadata=heartbeat.metadata,
    )


def _run_outcome_from_status(status: RunStatus) -> RunOutcome | None:
    if status == RunStatus.completed:
        return RunOutcome.success
    if status == RunStatus.failed:
        return RunOutcome.failed
    if status == RunStatus.cancelled:
        return RunOutcome.cancelled
    return None


def _document_status_from_runtime_document_state(
    state: RuntimeDocumentState,
) -> RuntimeDocumentStatus:
    statuses = [
        step.status.value if isinstance(step.status, ProcessStatus) else str(step.status)
        for step in state.steps
    ]
    if not statuses:
        return RuntimeDocumentStatus.registered
    if any(status == ProcessStatus.failed.value for status in statuses):
        return RuntimeDocumentStatus.failed
    if any(status == ProcessStatus.cancelled.value for status in statuses):
        return RuntimeDocumentStatus.cancelled
    if any(status == ProcessStatus.running.value for status in statuses):
        return RuntimeDocumentStatus.running
    if any(
        status in {ProcessStatus.waiting.value, ProcessStatus.queued.value}
        for status in statuses
    ):
        return RuntimeDocumentStatus.queued
    if all(
        status in {ProcessStatus.completed.value, ProcessStatus.skipped.value}
        for status in statuses
    ):
        return RuntimeDocumentStatus.completed
    return RuntimeDocumentStatus.registered


def _process_status_or_none(value: Any) -> ProcessStatus | None:
    if isinstance(value, ProcessStatus):
        return value
    if value is None:
        return None
    try:
        return ProcessStatus(str(value))
    except ValueError:
        return None


def _datetime_or_none(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if value is None:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def _string_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _bool_or_none(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


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


def _current_runtime_item(state: RuntimeState) -> dict[str, str | None]:
    priority = {"running": 0, "queued": 1, "waiting": 2, "failed": 3}
    best: tuple[int, str, str, str] | None = None
    for document in state.documents:
        for step in document.steps:
            status = step.status.value if isinstance(step.status, ProcessStatus) else str(step.status)
            rank = priority.get(status)
            if rank is None:
                continue
            candidate = (rank, document.document_id, step.id, status)
            if best is None or candidate < best:
                best = candidate
    if best is None:
        return {"document_id": None, "process_id": None, "status": None}
    return {"document_id": best[1], "process_id": best[2], "status": best[3]}
