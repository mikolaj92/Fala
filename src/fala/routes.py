from __future__ import annotations

import asyncio
import json
from collections import Counter
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from time import monotonic
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field, model_validator
from starlette.responses import FileResponse, PlainTextResponse, StreamingResponse

from fala.auth import (
    RuntimeAccessPolicy,
    RuntimeAuthError,
    RuntimePermission,
    api_permission_for_request,
    principal_from_request,
)
from fala.blueprints import (
    get_scaffold_blueprint,
    list_scaffold_blueprints,
    scaffold_blueprint_summary,
)
from fala.metrics import render_prometheus_metrics
from fala.models import (
    AdapterKind,
    ArtifactRef,
    ExistingDocumentPolicy,
    ExistingRunPolicy,
    ProcessAction,
    ProcessEvent,
    ProcessEventPage,
    ProcessOutput,
    ProcessStatus,
    ResourceSpec,
    RuntimeDocumentInput,
    RuntimeDocumentStatus,
    RuntimeRunInput,
    RuntimeWorkerStatus,
    RunStatus,
)
from fala.package_registry import (
    build_workflow_readiness_report,
    build_workflow_registry_index,
    package_readiness,
    package_release,
)
from fala.project import (
    build_project_alert_report,
    build_project_bootstrap_check,
    build_project_bootstrap_commands,
    build_project_lifecycle_report,
    build_project_operations_report,
    build_project_run_history,
    build_project_readiness_report,
    build_project_runtime_run_input,
    build_project_spec_report,
    build_project_supervision_report,
)
from fala.scheduler import PipelineScheduler
from fala.service import RuntimeService
from fala.store_factory import runtime_db_diagnostics, state_store_diagnostics_target

RunAccessGuard = Callable[[str], None]


class ProcessStatusUpdateRequest(BaseModel):
    status: ProcessStatus
    pipeline_id: str | None = None
    worker_id: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)


def _error_kind_from_data(data: dict[str, Any]) -> str | None:
    value = data.get("error_kind")
    return value if isinstance(value, str) and value else None


def _reason_from_data(data: dict[str, Any]) -> str | None:
    value = data.get("reason")
    return value if isinstance(value, str) and value else None


class ProcessEventRequest(BaseModel):
    process_id: str | None = None
    type: str
    status: ProcessStatus | None = None
    data: dict[str, Any] = Field(default_factory=dict)


class InitializeDocumentRuntimeRequest(BaseModel):
    pipeline_id: str
    document_id: str
    values: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[ArtifactRef] = Field(default_factory=list)


class AppendRuntimeDocumentsRequest(BaseModel):
    pipeline_id: str | None = None
    existing_document_policy: ExistingDocumentPolicy = "error"
    auto_route: bool = False
    routes: list[dict[str, Any]] = Field(default_factory=list)
    documents: list[RuntimeDocumentInput] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_documents(self) -> "AppendRuntimeDocumentsRequest":
        if not self.documents:
            raise ValueError("At least one document is required")
        document_ids = [document.document_id for document in self.documents]
        duplicates = sorted({item for item in document_ids if document_ids.count(item) > 1})
        if duplicates:
            raise ValueError(f"Duplicate runtime document id(s): {', '.join(duplicates)}")
        for document in self.documents:
            if (
                document.pipeline_id is None
                and self.pipeline_id is None
                and not self.auto_route
                and not self.routes
            ):
                raise ValueError(
                    f"Document {document.document_id!r} requires pipeline_id "
                    "or a request-level pipeline_id"
                )
        return self


class ClaimProcessRequest(BaseModel):
    pipeline_id: str
    worker_id: str | None = None
    process_id: str | None = None
    adapter_kind: AdapterKind | None = None
    capabilities: list[str] = Field(default_factory=list)
    resources: ResourceSpec = Field(default_factory=ResourceSpec)
    lease_seconds: float = Field(default=300.0, gt=0)


class RenewClaimRequest(BaseModel):
    pipeline_id: str | None = None
    worker_id: str | None = None
    lease_seconds: float = Field(default=300.0, gt=0)


class ProcessActionRequest(BaseModel):
    pipeline_id: str | None = None
    action: ProcessAction
    reason: str | None = None
    allow_contract_drift: bool = False


class DeadLetterReplayRequest(BaseModel):
    pipeline_id: str | None = None
    reason: str | None = None
    allow_contract_drift: bool = False


class ScheduleDocumentRequest(BaseModel):
    pipeline_id: str | None = None


class CreateRuntimeRunRequest(BaseModel):
    run_id: str | None = None
    existing_run_policy: ExistingRunPolicy = "error"
    existing_document_policy: ExistingDocumentPolicy = "error"
    title: str | None = None
    pipeline_id: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    auto_route: bool = False
    routes: list[dict[str, Any]] = Field(default_factory=list)
    documents: list[RuntimeDocumentInput] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_documents(self) -> "CreateRuntimeRunRequest":
        document_ids = [document.document_id for document in self.documents]
        duplicates = sorted({item for item in document_ids if document_ids.count(item) > 1})
        if duplicates:
            raise ValueError(f"Duplicate runtime document id(s): {', '.join(duplicates)}")
        for document in self.documents:
            if (
                document.pipeline_id is None
                and self.pipeline_id is None
                and not self.auto_route
                and not self.routes
            ):
                raise ValueError(
                    f"Document {document.document_id!r} requires pipeline_id "
                    "or a run-level pipeline_id"
                )
        return self


class RunActionRequest(BaseModel):
    action: Literal["pause", "resume", "cancel"]
    reason: str | None = None
    allow_contract_drift: bool = False


class CreateProjectRunRequest(BaseModel):
    run_id: str | None = None
    title: str | None = None
    existing_run_policy: ExistingRunPolicy = "error"
    existing_document_policy: ExistingDocumentPolicy = "error"
    metadata: dict[str, Any] = Field(default_factory=dict)


class ArtifactGcRequest(BaseModel):
    delete: bool = False


class RunRetentionRequest(BaseModel):
    before: str | None = None
    older_than_days: float | None = Field(default=None, gt=0)
    statuses: list[RunStatus] = Field(default_factory=list)
    delete: bool = False


class ProjectLifecycleRequest(BaseModel):
    before: str | None = None
    older_than_days: float | None = Field(default=None, gt=0)
    statuses: list[RunStatus] = Field(default_factory=list)
    include_artifact_gc: bool = True
    delete: bool = False


class WorkerHeartbeatRequest(BaseModel):
    pipeline_id: str | None = None
    process_id: str | None = None
    adapter_kind: AdapterKind | None = None
    capabilities: list[str] = Field(default_factory=list)
    resources: ResourceSpec = Field(default_factory=ResourceSpec)
    status: RuntimeWorkerStatus = RuntimeWorkerStatus.idle
    current_document_id: str | None = None
    current_process_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class StreamChunkRequest(BaseModel):
    sequence: int | None = Field(default=None, ge=0)
    kind: str | None = None
    artifacts: list[ArtifactRef] = Field(default_factory=list)
    values: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class StreamCheckpointRequest(BaseModel):
    sequence: int = Field(default=-1, ge=-1)
    chunk_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


def create_runtime_router(
    service: RuntimeService,
    *,
    access_policy: RuntimeAccessPolicy | None = None,
    ensure_run_access: RunAccessGuard | None = None,
    project_yaml: str | None = None,
) -> APIRouter:
    policy = access_policy or RuntimeAccessPolicy.disabled()

    async def authorize_request(request: Request) -> None:
        try:
            principal = policy.require(request, api_permission_for_request(request))
            run_id = request.path_params.get("run_id")
            if isinstance(run_id, str):
                run = await service.get_run(run_id)
                if run is not None:
                    policy.require_run_metadata(principal, run.metadata)
        except RuntimeAuthError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc

    router = APIRouter(dependencies=[Depends(authorize_request)])

    def require_permission(
        request: Request,
        permission: RuntimePermission,
    ) -> None:
        try:
            policy.require(request, permission)
        except RuntimeAuthError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc

    def run_metadata_for_create(
        request: Request,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        return policy.stamp_run_metadata(
            principal_from_request(request),
            metadata,
        )

    async def ensure_request_run_access(
        request: Request,
        run_id: str | None,
    ) -> None:
        if run_id is None:
            return
        run = await service.get_run(run_id)
        if run is None:
            return
        try:
            policy.require_run_metadata(principal_from_request(request), run.metadata)
        except RuntimeAuthError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc

    async def visible_run_summaries(
        request: Request,
        *,
        limit: int,
    ) -> list[dict[str, Any]]:
        runs = await service.list_run_summaries(limit=limit)
        principal = principal_from_request(request)
        visible_runs: list[dict[str, Any]] = []
        for run_summary in runs:
            run = await service.get_run(str(run_summary["run_id"]))
            try:
                policy.require_run_metadata(
                    principal,
                    run.metadata if run is not None else {},
                )
            except RuntimeAuthError:
                continue
            visible_runs.append(run_summary)
        return visible_runs

    async def project_supervision_report(
        request: Request,
        *,
        package_id: str | None,
        pipeline_id: str | None,
        document_type: str | None,
        operation_type: str | None,
        stuck_status: ProcessStatus | None,
        waiting_after_seconds: float,
        queued_after_seconds: float,
        running_after_seconds: float,
        consumer_id: str | None,
        min_lag: int,
        over_limit: bool | None,
        limit: int,
    ) -> dict[str, Any]:
        if project_yaml is None:
            raise HTTPException(
                status_code=404,
                detail="Fala project manifest is not configured.",
            )
        report = build_project_readiness_report(project_yaml, registry=service.registry)
        project_id = str(report.get("project_id") or "") or None
        runs = await visible_run_summaries(request, limit=500)
        history = build_project_run_history(
            project_id=project_id,
            registry=service.registry,
            runs=runs,
            package_id=package_id,
            pipeline_id=pipeline_id,
            document_type=document_type,
            limit=None,
        )
        dead_letter_pages: list[dict[str, Any]] = []
        stuck_work_pages: list[dict[str, Any]] = []
        stream_lag_pages: list[dict[str, Any]] = []
        for run in history["runs"]:
            run_id = str(run["run_id"])
            dead_letter_pages.append(
                (
                    await service.dead_letter_queue(
                        run_id,
                        pipeline_id=pipeline_id,
                        document_type=document_type,
                        operation_type=operation_type,
                        limit=1000,
                    )
                ).model_dump(mode="json")
            )
            stuck_work_pages.append(
                (
                    await service.stuck_work(
                        run_id,
                        status=stuck_status,
                        pipeline_id=pipeline_id,
                        document_type=document_type,
                        operation_type=operation_type,
                        waiting_after_seconds=waiting_after_seconds,
                        queued_after_seconds=queued_after_seconds,
                        running_after_seconds=running_after_seconds,
                        limit=1000,
                    )
                ).model_dump(mode="json")
            )
            stream_lag_pages.append(
                (
                    await service.stream_lag(
                        run_id,
                        pipeline_id=pipeline_id,
                        document_type=document_type,
                        operation_type=operation_type,
                        consumer_id=consumer_id,
                        min_lag=min_lag,
                        over_limit=over_limit,
                        limit=1000,
                    )
                ).model_dump(mode="json")
            )
        report_data = build_project_supervision_report(
            project_id=project_id,
            registry=service.registry,
            runs=history["runs"],
            dead_letter_pages=dead_letter_pages,
            stuck_work_pages=stuck_work_pages,
            stream_lag_pages=stream_lag_pages,
            package_id=package_id,
            pipeline_id=pipeline_id,
            document_type=document_type,
            operation_type=operation_type,
            limit=limit,
        )
        report_data["filters"].update(
            {
                "stuck_status": (
                    stuck_status.value if stuck_status is not None else None
                ),
                "waiting_after_seconds": waiting_after_seconds,
                "queued_after_seconds": queued_after_seconds,
                "running_after_seconds": running_after_seconds,
                "operation_type": operation_type,
                "consumer_id": consumer_id,
                "min_lag": min_lag,
                "over_limit": over_limit,
            }
        )
        return report_data

    async def project_operations_report(
        request: Request,
        *,
        package_id: str | None,
        pipeline_id: str | None,
        document_type: str | None,
        operation_type: str | None,
        stuck_status: ProcessStatus | None,
        waiting_after_seconds: float,
        queued_after_seconds: float,
        running_after_seconds: float,
        consumer_id: str | None,
        min_lag: int,
        over_limit: bool | None,
        stale_after_seconds: float,
        limit: int,
    ) -> dict[str, Any]:
        if project_yaml is None:
            raise HTTPException(
                status_code=404,
                detail="Fala project manifest is not configured.",
            )
        report = build_project_readiness_report(project_yaml, registry=service.registry)
        project_id = str(report.get("project_id") or "") or None
        runs = await visible_run_summaries(request, limit=500)
        history = build_project_run_history(
            project_id=project_id,
            registry=service.registry,
            runs=runs,
            package_id=package_id,
            pipeline_id=pipeline_id,
            document_type=document_type,
            limit=None,
        )
        supervision = await project_supervision_report(
            request,
            package_id=package_id,
            pipeline_id=pipeline_id,
            document_type=document_type,
            operation_type=operation_type,
            stuck_status=stuck_status,
            waiting_after_seconds=waiting_after_seconds,
            queued_after_seconds=queued_after_seconds,
            running_after_seconds=running_after_seconds,
            consumer_id=consumer_id,
            min_lag=min_lag,
            over_limit=over_limit,
            limit=limit,
        )
        health_reports: list[dict[str, Any]] = []
        for run in history["runs"]:
            health_reports.append(
                (
                    await service.run_health(
                        str(run["run_id"]),
                        stale_after_seconds=stale_after_seconds,
                    )
                ).model_dump(mode="json")
            )
        operations = build_project_operations_report(
            project_id=project_id,
            registry=service.registry,
            runs=history["runs"],
            health_reports=health_reports,
            supervision=supervision,
            package_id=package_id,
            pipeline_id=pipeline_id,
            document_type=document_type,
            operation_type=operation_type,
            limit=limit,
        )
        operations["filters"].update(
            {
                "stuck_status": (
                    stuck_status.value if stuck_status is not None else None
                ),
                "waiting_after_seconds": waiting_after_seconds,
                "queued_after_seconds": queued_after_seconds,
                "running_after_seconds": running_after_seconds,
                "operation_type": operation_type,
                "consumer_id": consumer_id,
                "min_lag": min_lag,
                "over_limit": over_limit,
                "stale_after_seconds": stale_after_seconds,
            }
        )
        return operations

    async def project_lifecycle_report(
        request: Request,
        *,
        package_id: str | None,
        pipeline_id: str | None,
        document_type: str | None,
        before: str | None,
        older_than_days: float | None,
        statuses: list[RunStatus] | None,
        include_artifact_gc: bool,
        delete: bool,
        limit: int,
    ) -> dict[str, Any]:
        if project_yaml is None:
            raise HTTPException(
                status_code=404,
                detail="Fala project manifest is not configured.",
            )
        if delete:
            require_permission(request, RuntimePermission.admin)
        report = build_project_readiness_report(project_yaml, registry=service.registry)
        project_id = str(report.get("project_id") or "") or None
        runs = await visible_run_summaries(request, limit=500)
        history = build_project_run_history(
            project_id=project_id,
            registry=service.registry,
            runs=runs,
            package_id=package_id,
            pipeline_id=pipeline_id,
            document_type=document_type,
            limit=None,
        )
        artifact_gc = (
            (await service.artifact_gc(dry_run=True)).model_dump(mode="json")
            if include_artifact_gc
            else None
        )
        plan = build_project_lifecycle_report(
            project_yaml,
            runs=history["runs"],
            before=before,
            older_than_days=older_than_days,
            statuses=statuses or None,
            artifact_gc=artifact_gc,
            dry_run=not delete,
            limit=limit,
        )
        deleted_run_ids: set[str] = set()
        row_counts: Counter[str] = Counter()
        if delete:
            for item in plan["retention"]["runs"]:
                run_id = str(item["run_id"])
                row_counts.update(await service.store.delete_run(run_id))
                deleted_run_ids.add(run_id)
            artifact_gc = (
                (await service.artifact_gc(dry_run=True)).model_dump(mode="json")
                if include_artifact_gc
                else None
            )
            plan = build_project_lifecycle_report(
                project_yaml,
                runs=history["runs"],
                before=before,
                older_than_days=older_than_days,
                statuses=statuses or None,
                artifact_gc=artifact_gc,
                dry_run=False,
                deleted_run_ids=deleted_run_ids,
                row_counts=dict(row_counts),
                limit=limit,
            )
        plan["filters"].update(
            {
                "package_id": package_id,
                "pipeline_id": pipeline_id,
                "document_type": document_type,
                "include_artifact_gc": include_artifact_gc,
            }
        )
        await audit(
            request,
            action="project.lifecycle.delete" if delete else "project.lifecycle.plan",
            target=f"project:{project_id or 'unknown'}",
            data={
                "dry_run": plan["dry_run"],
                "candidate_count": plan["candidate_count"],
                "deleted_run_count": plan["deleted_run_count"],
                "run_ids": [
                    item["run_id"]
                    for item in plan["retention"]["runs"][:100]
                ],
                "package_id": package_id,
                "pipeline_id": pipeline_id,
                "document_type": document_type,
            },
        )
        return plan

    def guard(run_id: str) -> None:
        if ensure_run_access is not None:
            ensure_run_access(run_id)

    def runtime_run_input_from_request(
        req: CreateRuntimeRunRequest,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeRunInput:
        documents = service.route_runtime_document_inputs(
            req.documents,
            routes=req.routes,
            auto_route=req.auto_route,
        )
        return RuntimeRunInput(
            run_id=req.run_id,
            existing_run_policy=req.existing_run_policy,
            existing_document_policy=req.existing_document_policy,
            title=req.title,
            pipeline_id=req.pipeline_id,
            config=req.config,
            metadata=req.metadata if metadata is None else metadata,
            documents=documents,
        )

    def runtime_run_input_and_route_report_from_request(
        req: CreateRuntimeRunRequest,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> tuple[RuntimeRunInput, dict[str, Any]]:
        documents, route_report = service.route_runtime_document_inputs_with_report(
            req.documents,
            routes=req.routes,
            auto_route=req.auto_route,
        )
        return (
            RuntimeRunInput(
                run_id=req.run_id,
                existing_run_policy=req.existing_run_policy,
                existing_document_policy=req.existing_document_policy,
                title=req.title,
                pipeline_id=req.pipeline_id,
                config=req.config,
                metadata=req.metadata if metadata is None else metadata,
                documents=documents,
            ),
            route_report,
        )

    def pipeline_or_404(pipeline_id: str):
        try:
            return service.registry.get(pipeline_id)
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    def package_or_404(package_id: str):
        try:
            return service.registry.package(package_id)
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    def ensure_pipeline_process(pipeline_id: str, process_id: str):
        pipeline = pipeline_or_404(pipeline_id)
        if process_id not in {step.id for step in pipeline.steps}:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown process id {process_id!r} for pipeline {pipeline_id!r}",
            )
        return pipeline

    def operator_actor(
        request: Request,
        *,
        worker_id: str | None = None,
        default: str | None = "api",
    ) -> str | None:
        principal = principal_from_request(request)
        if principal is not None:
            return principal.actor
        return (
            request.headers.get("x-fala-actor")
            or request.headers.get("x-operator-id")
            or request.headers.get("x-user-email")
            or (f"worker:{worker_id}" if worker_id else None)
            or default
        )

    def operator_source(
        request: Request,
        *,
        worker_id: str | None = None,
        default: str = "api",
    ) -> str:
        principal = principal_from_request(request)
        if principal is not None:
            return principal.source
        return (
            request.headers.get("x-fala-source")
            or ("worker-api" if worker_id else default)
        )

    async def audit(
        request: Request,
        *,
        action: str,
        run_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
        target: str | None = None,
        data: dict[str, Any] | None = None,
        worker_id: str | None = None,
    ) -> None:
        await service.record_operator_audit(
            actor=operator_actor(request, worker_id=worker_id),
            source=operator_source(request, worker_id=worker_id),
            action=action,
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            target=target,
            data=data,
        )

    async def ensure_document_process_id(
        *,
        run_id: str,
        document_id: str,
        process_id: str,
    ) -> None:
        pipeline_id = await service.store.get_document_pipeline_id(
            run_id=run_id,
            document_id=document_id,
        )
        if not pipeline_id:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Pipeline id required for document {document_id!r}; "
                    "initialize document first"
                ),
            )
        ensure_pipeline_process(pipeline_id, process_id)

    async def ensure_process_writer(
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        worker_id: str | None,
    ) -> None:
        claim = await service.store.get_claim(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        if claim is None:
            if worker_id is not None:
                raise HTTPException(status_code=409, detail="Process is not claimed")
            return
        if claim.worker_id is None:
            return
        if claim.expires_at <= datetime.now(timezone.utc):
            raise HTTPException(status_code=409, detail="Process claim expired")
        if worker_id != claim.worker_id:
            raise HTTPException(
                status_code=409,
                detail="Process is claimed by another worker",
            )
        statuses = await service.store.list_statuses(
            run_id=run_id,
            document_id=document_id,
        )
        if statuses.get(process_id) != ProcessStatus.running:
            raise HTTPException(status_code=409, detail="Process is not running")

    async def pipeline_for_write(
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        pipeline_id: str | None,
    ):
        resolved_pipeline_id = pipeline_id or await service.store.get_document_pipeline_id(
            run_id=run_id,
            document_id=document_id,
        )
        if not resolved_pipeline_id:
            return None
        return ensure_pipeline_process(resolved_pipeline_id, process_id)

    @router.get("/process-runtime/packages")
    async def list_runtime_packages() -> dict[str, Any]:
        return {
            "packages": [
                _package_summary(service.registry, package)
                for package in service.registry.packages()
            ]
        }

    @router.get("/process-runtime/audit")
    async def list_runtime_operator_audit(
        request: Request,
        run_id: str | None = Query(default=None),
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> dict[str, Any]:
        await ensure_request_run_access(request, run_id)
        return {
            "audit": (
                await service.operator_audit(
                    run_id=run_id,
                    limit=limit,
                    descending=True,
                )
            ).model_dump(mode="json")
        }

    @router.get("/process-runtime/artifacts/gc")
    async def plan_runtime_artifact_gc() -> dict[str, Any]:
        plan = await service.artifact_gc(dry_run=True)
        return {"artifact_gc": plan.model_dump(mode="json")}

    @router.post("/process-runtime/artifacts/gc")
    async def run_runtime_artifact_gc(
        request: Request,
        req: ArtifactGcRequest,
    ) -> dict[str, Any]:
        if req.delete:
            require_permission(request, RuntimePermission.admin)
        plan = await service.artifact_gc(dry_run=not req.delete)
        await audit(
            request,
            action="artifact.gc.delete" if req.delete else "artifact.gc.plan",
            target="artifact_store",
            data={
                "dry_run": plan.dry_run,
                "orphaned_blob_count": plan.orphaned_blob_count,
                "deleted_blob_count": plan.deleted_blob_count,
                "deleted_bytes": plan.deleted_bytes,
            },
        )
        return {"artifact_gc": plan.model_dump(mode="json")}

    @router.get("/process-runtime/packages/index")
    async def get_runtime_package_index(
        package_id: str | None = Query(default=None),
    ) -> dict[str, Any]:
        try:
            index = build_workflow_registry_index(
                service.registry,
                package_id=package_id,
            )
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"ok": True, "index": index.model_dump(mode="json")}

    @router.get("/process-runtime/project")
    async def get_runtime_project() -> dict[str, Any]:
        if project_yaml is None:
            return {
                "ok": False,
                "configured": False,
                "error": "Fala project manifest is not configured.",
            }
        report = build_project_readiness_report(project_yaml, registry=service.registry)
        return {
            "ok": report["ok"],
            "configured": True,
            "project": report,
        }

    @router.get("/process-runtime/project/spec")
    async def get_runtime_project_spec(
        base_url: str = Query(default="http://localhost:8000"),
        run_id: str | None = Query(default=None),
    ) -> dict[str, Any]:
        if project_yaml is None:
            return {
                "ok": False,
                "configured": False,
                "error": "Fala project manifest is not configured.",
            }
        spec = build_project_spec_report(
            project_yaml,
            registry=service.registry,
            base_url=base_url,
            run_id=run_id,
        )
        return {
            "ok": spec["ok"],
            "configured": True,
            "project_id": spec.get("project_id"),
            "spec": spec,
        }

    @router.get("/process-runtime/project/bootstrap")
    async def get_runtime_project_bootstrap(
        base_url: str = Query(default="http://localhost:8000"),
        run_id: str | None = Query(default=None),
    ) -> dict[str, Any]:
        if project_yaml is None:
            return {
                "ok": False,
                "configured": False,
                "error": "Fala project manifest is not configured.",
            }
        db_target = state_store_diagnostics_target(service.store)
        db_report = (
            runtime_db_diagnostics(db_target, ensure_schema=False)
            if db_target is not None
            else None
        )
        check = build_project_bootstrap_check(
            project_yaml,
            registry=service.registry,
            base_url=base_url,
            run_id=run_id,
            db=db_report,
        )
        commands = build_project_bootstrap_commands(
            project_yaml=project_yaml,
            db_target=db_target,
            base_url=base_url,
            run_id=check["run_id"],
        )
        return {
            "ok": check["ok"],
            "configured": True,
            "project_id": check.get("project_id"),
            "db_configured": db_target is not None,
            "db_target": db_target,
            "check": check,
            "db": db_report,
            "commands": commands,
        }

    @router.get("/process-runtime/project/runs")
    async def get_runtime_project_runs(
        request: Request,
        status: str | None = Query(default=None),
        package_id: str | None = Query(default=None),
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
    ) -> dict[str, Any]:
        if project_yaml is None:
            return {
                "ok": False,
                "configured": False,
                "error": "Fala project manifest is not configured.",
            }
        report = build_project_readiness_report(project_yaml, registry=service.registry)
        runs = await visible_run_summaries(request, limit=500)
        history = build_project_run_history(
            project_id=report.get("project_id"),
            registry=service.registry,
            runs=runs,
            status=status,
            package_id=package_id,
            pipeline_id=pipeline_id,
            document_type=document_type,
            limit=limit,
        )
        return {
            "ok": True,
            "configured": True,
            "project_id": report.get("project_id"),
            "history": history,
        }

    @router.get("/process-runtime/project/supervision")
    async def get_runtime_project_supervision(
        request: Request,
        package_id: str | None = Query(default=None),
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
        stuck_status: ProcessStatus | None = Query(default=None),
        waiting_after_seconds: float = Query(default=3600.0, ge=0),
        queued_after_seconds: float = Query(default=600.0, ge=0),
        running_after_seconds: float = Query(default=1800.0, ge=0),
        consumer_id: str | None = Query(default=None),
        min_lag: int = Query(default=1, ge=0),
        over_limit: bool | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
    ) -> dict[str, Any]:
        if project_yaml is None:
            return {
                "ok": False,
                "configured": False,
                "error": "Fala project manifest is not configured.",
            }
        try:
            supervision = await project_supervision_report(
                request,
                package_id=package_id,
                pipeline_id=pipeline_id,
                document_type=document_type,
                operation_type=operation_type,
                stuck_status=stuck_status,
                waiting_after_seconds=waiting_after_seconds,
                queued_after_seconds=queued_after_seconds,
                running_after_seconds=running_after_seconds,
                consumer_id=consumer_id,
                min_lag=min_lag,
                over_limit=over_limit,
                limit=limit,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "ok": True,
            "configured": True,
            "project_id": supervision.get("project_id"),
            "supervision": supervision,
        }

    @router.get("/process-runtime/project/operations")
    async def get_runtime_project_operations(
        request: Request,
        package_id: str | None = Query(default=None),
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
        stuck_status: ProcessStatus | None = Query(default=None),
        waiting_after_seconds: float = Query(default=3600.0, ge=0),
        queued_after_seconds: float = Query(default=600.0, ge=0),
        running_after_seconds: float = Query(default=1800.0, ge=0),
        consumer_id: str | None = Query(default=None),
        min_lag: int = Query(default=1, ge=0),
        over_limit: bool | None = Query(default=None),
        stale_after_seconds: float = Query(default=60.0, gt=0, le=3600.0),
        limit: int = Query(default=50, ge=1, le=500),
    ) -> dict[str, Any]:
        if project_yaml is None:
            return {
                "ok": False,
                "configured": False,
                "error": "Fala project manifest is not configured.",
            }
        try:
            operations = await project_operations_report(
                request,
                package_id=package_id,
                pipeline_id=pipeline_id,
                document_type=document_type,
                operation_type=operation_type,
                stuck_status=stuck_status,
                waiting_after_seconds=waiting_after_seconds,
                queued_after_seconds=queued_after_seconds,
                running_after_seconds=running_after_seconds,
                consumer_id=consumer_id,
                min_lag=min_lag,
                over_limit=over_limit,
                stale_after_seconds=stale_after_seconds,
                limit=limit,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "ok": True,
            "configured": True,
            "project_id": operations.get("project_id"),
            "operations": operations,
        }

    @router.get("/process-runtime/project/alerts")
    async def get_runtime_project_alerts(
        request: Request,
        package_id: str | None = Query(default=None),
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
        stuck_status: ProcessStatus | None = Query(default=None),
        waiting_after_seconds: float = Query(default=3600.0, ge=0),
        queued_after_seconds: float = Query(default=600.0, ge=0),
        running_after_seconds: float = Query(default=1800.0, ge=0),
        consumer_id: str | None = Query(default=None),
        min_lag: int = Query(default=1, ge=0),
        over_limit: bool | None = Query(default=None),
        stale_after_seconds: float = Query(default=60.0, gt=0, le=3600.0),
        limit: int = Query(default=50, ge=1, le=500),
    ) -> dict[str, Any]:
        if project_yaml is None:
            return {
                "ok": False,
                "configured": False,
                "error": "Fala project manifest is not configured.",
            }
        try:
            operations = await project_operations_report(
                request,
                package_id=package_id,
                pipeline_id=pipeline_id,
                document_type=document_type,
                operation_type=operation_type,
                stuck_status=stuck_status,
                waiting_after_seconds=waiting_after_seconds,
                queued_after_seconds=queued_after_seconds,
                running_after_seconds=running_after_seconds,
                consumer_id=consumer_id,
                min_lag=min_lag,
                over_limit=over_limit,
                stale_after_seconds=stale_after_seconds,
                limit=limit,
            )
            alerts = build_project_alert_report(
                project_yaml,
                operations=operations,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "ok": True,
            "configured": True,
            "project_id": alerts.get("project_id"),
            "alerts": alerts,
            "operations": operations,
        }

    @router.get("/process-runtime/project/lifecycle")
    async def get_runtime_project_lifecycle(
        request: Request,
        package_id: str | None = Query(default=None),
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        before: str | None = Query(default=None),
        older_than_days: float | None = Query(default=None, gt=0),
        status: list[RunStatus] = Query(default=[]),
        include_artifact_gc: bool = Query(default=True),
        limit: int = Query(default=50, ge=1, le=500),
    ) -> dict[str, Any]:
        if project_yaml is None:
            return {
                "ok": False,
                "configured": False,
                "error": "Fala project manifest is not configured.",
            }
        try:
            lifecycle = await project_lifecycle_report(
                request,
                package_id=package_id,
                pipeline_id=pipeline_id,
                document_type=document_type,
                before=before,
                older_than_days=older_than_days,
                statuses=status or None,
                include_artifact_gc=include_artifact_gc,
                delete=False,
                limit=limit,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "ok": True,
            "configured": True,
            "project_id": lifecycle.get("project_id"),
            "lifecycle": lifecycle,
        }

    @router.post("/process-runtime/project/lifecycle")
    async def run_runtime_project_lifecycle(
        request: Request,
        req: ProjectLifecycleRequest,
        package_id: str | None = Query(default=None),
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        limit: int = Query(default=50, ge=1, le=500),
    ) -> dict[str, Any]:
        if project_yaml is None:
            raise HTTPException(
                status_code=404,
                detail="Fala project manifest is not configured.",
            )
        try:
            lifecycle = await project_lifecycle_report(
                request,
                package_id=package_id,
                pipeline_id=pipeline_id,
                document_type=document_type,
                before=req.before,
                older_than_days=req.older_than_days,
                statuses=req.statuses or None,
                include_artifact_gc=req.include_artifact_gc,
                delete=req.delete,
                limit=limit,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "ok": True,
            "configured": True,
            "project_id": lifecycle.get("project_id"),
            "lifecycle": lifecycle,
        }

    @router.post("/process-runtime/project/runs")
    async def create_runtime_project_run(
        request: Request,
        req: CreateProjectRunRequest,
    ) -> dict[str, Any]:
        if project_yaml is None:
            raise HTTPException(
                status_code=404,
                detail="Fala project manifest is not configured.",
            )
        try:
            await ensure_request_run_access(request, req.run_id)
            run_input, route_report = build_project_runtime_run_input(
                project_yaml,
                registry=service.registry,
                run_id=req.run_id,
                title=req.title,
                existing_run_policy=req.existing_run_policy,
                existing_document_policy=req.existing_document_policy,
                metadata=run_metadata_for_create(request, req.metadata),
            )
            run, schedules = await service.create_run_with_documents(
                run_input,
                route_report=route_report,
            )
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await audit(
            request,
            action="project.run.create",
            run_id=run.id,
            target=f"run:{run.id}",
            data={
                "project_yaml": str(project_yaml),
                "document_count": len(run_input.documents),
                "route_report": {
                    key: route_report[key]
                    for key in (
                        "document_count",
                        "routed_count",
                        "unrouted_count",
                        "missing_pipeline_count",
                        "missing_document_type_count",
                    )
                    if key in route_report
                },
            },
        )
        return {
            "ok": True,
            "run": run.model_dump(mode="json"),
            "document_count": len(schedules),
            "route_report": route_report,
            "schedules": [schedule.model_dump(mode="json") for schedule in schedules],
        }

    @router.get("/process-runtime/packages/readiness")
    async def get_runtime_package_readiness_report(
        package_id: str | None = Query(default=None),
    ) -> dict[str, Any]:
        try:
            report = build_workflow_readiness_report(
                service.registry,
                package_id=package_id,
            )
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"ok": report.ok, "readiness": report.model_dump(mode="json")}

    @router.get("/process-runtime/packages/{package_id}/release")
    async def get_runtime_package_release(package_id: str) -> dict[str, Any]:
        try:
            release = package_release(service.registry, package_id)
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"ok": True, "release": release.model_dump(mode="json")}

    @router.get("/process-runtime/packages/{package_id}/readiness")
    async def get_runtime_package_readiness(package_id: str) -> dict[str, Any]:
        try:
            readiness = package_readiness(service.registry, package_id)
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"ok": readiness.ok, "readiness": readiness.model_dump(mode="json")}

    @router.get("/process-runtime/packages/{package_id}")
    async def get_runtime_package(package_id: str) -> dict[str, Any]:
        package = package_or_404(package_id)
        pipeline_ids = service.registry.package_pipeline_ids(package.id)
        return {
            "package": _package_summary(service.registry, package),
            "pipelines": [
                _pipeline_summary(service.registry, service.registry.get(pipeline_id))
                for pipeline_id in pipeline_ids
            ],
        }

    @router.get("/process-runtime/blueprints")
    async def list_runtime_blueprints(
        query: str | None = Query(default=None),
    ) -> dict[str, Any]:
        blueprints = list_scaffold_blueprints(query=query)
        return {
            "ok": True,
            "query": query,
            "blueprint_count": len(blueprints),
            "blueprints": blueprints,
        }

    @router.get("/process-runtime/blueprints/{blueprint_id}")
    async def get_runtime_blueprint(blueprint_id: str) -> dict[str, Any]:
        blueprint = get_scaffold_blueprint(blueprint_id)
        if blueprint is None:
            raise HTTPException(status_code=404, detail="Blueprint not found")
        return {
            "ok": True,
            "blueprint": scaffold_blueprint_summary(blueprint),
        }

    @router.get("/process-runtime/pipelines")
    async def list_runtime_pipelines() -> dict[str, Any]:
        return {
            "packages": [
                _package_summary(service.registry, package)
                for package in service.registry.packages()
            ],
            "pipelines": [
                _pipeline_summary(service.registry, pipeline)
                for pipeline in service.registry.all()
            ],
        }

    @router.get("/process-runtime/pipelines/{pipeline_id}")
    async def get_runtime_pipeline(pipeline_id: str) -> dict[str, Any]:
        pipeline = pipeline_or_404(pipeline_id)
        return {
            "package_id": service.registry.pipeline_package_id(pipeline.id),
            "pipeline": pipeline.model_dump(mode="json"),
        }

    @router.get("/process-runtime/pipelines/{pipeline_id}/contract")
    async def get_runtime_pipeline_contract(pipeline_id: str) -> dict[str, Any]:
        pipeline_or_404(pipeline_id)
        return {
            "ok": True,
            "contract": service.registry.pipeline_contract(pipeline_id),
        }

    @router.get("/process-runtime/runs")
    async def list_runtime_runs(
        request: Request,
        limit: int = Query(default=50, ge=1, le=500),
    ) -> dict[str, Any]:
        return {"runs": await visible_run_summaries(request, limit=limit)}

    @router.post("/process-runtime/runs/validate")
    async def validate_runtime_run(req: CreateRuntimeRunRequest) -> dict[str, Any]:
        try:
            return service.preview_runtime_run_input(runtime_run_input_from_request(req))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.post("/process-runtime/runs/route")
    async def route_runtime_run(req: CreateRuntimeRunRequest) -> dict[str, Any]:
        try:
            routed_input, route_report = runtime_run_input_and_route_report_from_request(
                req
            )
            preview = service.preview_runtime_run_input(routed_input)
            return {
                "ok": True,
                "run_input": routed_input.model_dump(
                    mode="json",
                    exclude_none=True,
                ),
                "route_report": route_report,
                "preview": preview,
            }
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.post("/process-runtime/runs/plan")
    async def plan_runtime_run(req: CreateRuntimeRunRequest) -> dict[str, Any]:
        try:
            return service.plan_runtime_run_input(runtime_run_input_from_request(req))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.post("/process-runtime/runs")
    async def create_runtime_run(
        request: Request,
        req: CreateRuntimeRunRequest,
    ) -> dict[str, Any]:
        try:
            await ensure_request_run_access(request, req.run_id)
            if req.documents:
                run_input, route_report = runtime_run_input_and_route_report_from_request(
                    req,
                    metadata=run_metadata_for_create(request, req.metadata),
                )
                run, schedules = await service.create_run_with_documents(
                    run_input,
                    route_report=route_report if req.auto_route or req.routes else None,
                )
            else:
                run = await service.create_run(
                    run_id=req.run_id,
                    title=req.title,
                    config=req.config,
                    metadata=run_metadata_for_create(request, req.metadata),
                    existing_run_policy=req.existing_run_policy,
                )
                schedules = []
        except ValueError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        await audit(
            request,
            action="run.create",
            run_id=run.id,
            target=f"run:{run.id}",
            data={
                "title": run.title,
                "pipeline_id": req.pipeline_id,
                "document_count": len(req.documents),
                "existing_run_policy": req.existing_run_policy,
                "existing_document_policy": req.existing_document_policy,
            },
        )
        return {
            "ok": True,
            "run": run.model_dump(mode="json"),
            "document_count": len(schedules),
            "schedules": [schedule.model_dump(mode="json") for schedule in schedules],
        }

    @router.get("/process-runtime/runs/retention")
    async def plan_runtime_run_retention(
        before: str | None = Query(default=None),
        older_than_days: float | None = Query(default=None, gt=0),
        status: list[RunStatus] = Query(default=[]),
    ) -> dict[str, Any]:
        try:
            plan = await service.run_retention(
                before=_retention_cutoff(
                    before=before,
                    older_than_days=older_than_days,
                ),
                statuses=status or None,
                dry_run=True,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {"retention": plan.model_dump(mode="json")}

    @router.post("/process-runtime/runs/retention")
    async def run_runtime_run_retention(
        request: Request,
        req: RunRetentionRequest,
    ) -> dict[str, Any]:
        if req.delete:
            require_permission(request, RuntimePermission.admin)
        try:
            plan = await service.run_retention(
                before=_retention_cutoff(
                    before=req.before,
                    older_than_days=req.older_than_days,
                ),
                statuses=req.statuses or None,
                dry_run=not req.delete,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        await audit(
            request,
            action="run.retention.delete" if req.delete else "run.retention.plan",
            target="runtime_state",
            data={
                "dry_run": plan.dry_run,
                "before": plan.before.isoformat(),
                "statuses": [status.value for status in plan.statuses],
                "candidate_count": plan.candidate_count,
                "deleted_run_count": plan.deleted_run_count,
                "run_ids": [item.run_id for item in plan.runs[:100]],
            },
        )
        return {"retention": plan.model_dump(mode="json")}

    @router.get("/process-runtime/runs/{run_id}")
    async def get_runtime_run(run_id: str) -> dict[str, Any]:
        run = await service.get_run(run_id)
        if run is None:
            run_ids = {row["run_id"] for row in await service.store.list_runs(limit=None)}
            if run_id not in run_ids:
                raise HTTPException(status_code=404, detail="Run not found")
            await service.sync_run_lifecycle(run_id)
            run = await service.get_run(run_id)
        return {
            "ok": True,
            "run": run.model_dump(mode="json") if run is not None else None,
            "state": await service.load_state(run_id, include_events=False),
        }

    @router.get("/process-runtime/runs/{run_id}/provenance")
    async def get_runtime_run_provenance(run_id: str) -> dict[str, Any]:
        guard(run_id)
        try:
            return await service.run_provenance(run_id)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @router.post("/process-runtime/runs/{run_id}/actions")
    async def control_runtime_run(
        request: Request,
        run_id: str,
        req: RunActionRequest,
    ) -> dict[str, Any]:
        guard(run_id)
        try:
            if req.action == "pause":
                run = await service.pause_run(run_id, reason=req.reason)
            elif req.action == "resume":
                run = await service.resume_run(
                    run_id,
                    reason=req.reason,
                    allow_contract_drift=req.allow_contract_drift,
                )
            elif req.action == "cancel":
                run = await service.cancel_run(run_id, reason=req.reason)
            else:
                raise ValueError(f"Unknown run action: {req.action}")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        await audit(
            request,
            action=f"run.{req.action}",
            run_id=run_id,
            target=f"run:{run_id}",
            data={
                "reason": req.reason,
                "status": run.status.value,
                "allow_contract_drift": req.allow_contract_drift,
            },
        )
        return {
            "ok": True,
            "run": run.model_dump(mode="json"),
            "state": await service.load_state(run_id, include_events=False),
        }

    @router.get("/runs/{run_id}/process-runtime")
    async def get_run_process_runtime(
        run_id: str,
        include_events: bool = Query(default=False),
    ) -> dict[str, Any]:
        guard(run_id)
        return await service.load_state(run_id, include_events=include_events)

    @router.get("/runs/{run_id}/process-runtime/report")
    async def get_run_process_runtime_report(run_id: str) -> dict[str, Any]:
        guard(run_id)
        return (await service.step_report(run_id)).model_dump(mode="json")

    @router.get("/runs/{run_id}/process-runtime/metrics")
    async def get_run_process_runtime_metrics(run_id: str) -> dict[str, Any]:
        guard(run_id)
        return (await service.queue_metrics(run_id)).model_dump(mode="json")

    @router.get("/runs/{run_id}/process-runtime/metrics/prometheus")
    async def get_run_process_runtime_prometheus_metrics(
        run_id: str,
        stale_after_seconds: float = Query(default=60.0, gt=0, le=3600.0),
    ) -> PlainTextResponse:
        guard(run_id)
        queue_metrics = await service.queue_metrics(
            run_id,
            stale_after_seconds=stale_after_seconds,
        )
        capability_demands = await service.capability_demands(
            run_id,
            stale_after_seconds=stale_after_seconds,
        )
        return PlainTextResponse(
            render_prometheus_metrics(queue_metrics, capability_demands),
            media_type="text/plain; version=0.0.4",
        )

    @router.get("/runs/{run_id}/process-runtime/capability-demands")
    async def get_run_process_runtime_capability_demands(
        run_id: str,
        stale_after_seconds: float = Query(default=60.0, gt=0, le=3600.0),
    ) -> dict[str, Any]:
        guard(run_id)
        return (
            await service.capability_demands(
                run_id,
                stale_after_seconds=stale_after_seconds,
            )
        ).model_dump(mode="json")

    @router.get("/runs/{run_id}/process-runtime/health")
    async def get_run_process_runtime_health(
        run_id: str,
        stale_after_seconds: float = Query(default=60.0, gt=0, le=3600.0),
    ) -> dict[str, Any]:
        guard(run_id)
        try:
            return (
                await service.run_health(
                    run_id,
                    stale_after_seconds=stale_after_seconds,
                )
            ).model_dump(mode="json")
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    @router.get("/runs/{run_id}/process-runtime/trace")
    async def get_run_process_runtime_trace(
        run_id: str,
        document_id: str | None = Query(default=None),
        process_id: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
    ) -> dict[str, Any]:
        guard(run_id)
        return (
            await service.process_trace(
                run_id,
                document_id=document_id,
                process_id=process_id,
                operation_type=operation_type,
            )
        ).model_dump(mode="json")

    @router.get("/runs/{run_id}/process-runtime/workers")
    async def get_run_process_runtime_workers(
        run_id: str,
        stale_after_seconds: float = Query(default=60.0, gt=0, le=3600.0),
    ) -> dict[str, Any]:
        guard(run_id)
        try:
            workers = await service.worker_health(
                run_id,
                stale_after_seconds=stale_after_seconds,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return {
            "ok": True,
            "run_id": run_id,
            "stale_after_seconds": stale_after_seconds,
            "worker_count": len(workers),
            "healthy_count": sum(1 for worker in workers if worker.healthy),
            "workers": [worker.model_dump(mode="json") for worker in workers],
        }

    @router.post("/runs/{run_id}/process-runtime/workers/{worker_id}/heartbeat")
    async def put_run_process_runtime_worker_heartbeat(
        run_id: str,
        worker_id: str,
        req: WorkerHeartbeatRequest,
    ) -> dict[str, Any]:
        guard(run_id)
        try:
            heartbeat = await service.record_worker_heartbeat(
                run_id=run_id,
                worker_id=worker_id,
                pipeline_id=req.pipeline_id,
                process_id=req.process_id,
                adapter_kind=req.adapter_kind,
                capabilities=req.capabilities,
                resources=req.resources,
                status=req.status,
                current_document_id=req.current_document_id,
                current_process_id=req.current_process_id,
                metadata=req.metadata,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {"ok": True, "worker": heartbeat.model_dump(mode="json")}

    @router.get("/runs/{run_id}/process-runtime/events/stream")
    async def stream_run_process_events(
        run_id: str,
        process_id: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
        after_event_id: str | None = Query(default=None),
        batch_limit: int = Query(default=100, ge=1, le=500),
        poll_interval_seconds: float = Query(default=1.0, ge=0.05, le=60.0),
        heartbeat_interval_seconds: float = Query(default=15.0, ge=1.0, le=300.0),
        max_events: int | None = Query(default=None, ge=1, le=10000),
    ) -> StreamingResponse:
        guard(run_id)
        await _validate_event_cursor(
            service=service,
            run_id=run_id,
            document_id=None,
            process_id=process_id,
            operation_type=operation_type,
            after_event_id=after_event_id,
        )
        return _event_stream_response(
            service=service,
            run_id=run_id,
            document_id=None,
            process_id=process_id,
            operation_type=operation_type,
            after_event_id=after_event_id,
            batch_limit=batch_limit,
            poll_interval_seconds=poll_interval_seconds,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            max_events=max_events,
        )

    @router.get("/runs/{run_id}/process-runtime/documents")
    async def list_run_process_documents(
        run_id: str,
        status: RuntimeDocumentStatus | None = Query(default=None),
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        relation: str | None = Query(default=None),
        parent_document_id: str | None = Query(default=None),
        limit: int = Query(default=100, ge=1, le=1000),
        offset: int = Query(default=0, ge=0),
    ) -> dict[str, Any]:
        guard(run_id)
        page = await service.document_registry(
            run_id,
            status=status,
            pipeline_id=pipeline_id,
            document_type=document_type,
            relation=relation,
            parent_document_id=parent_document_id,
            limit=limit,
            offset=offset,
        )
        return page.model_dump(mode="json")

    @router.get("/runs/{run_id}/process-runtime/processes")
    async def list_run_process_records(
        run_id: str,
        status: ProcessStatus | None = Query(default=None),
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        parent_document_id: str | None = Query(default=None),
        document_id: str | None = Query(default=None),
        process_id: str | None = Query(default=None),
        capability: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
        adapter_kind: AdapterKind | None = Query(default=None),
        resource_pool: str | None = Query(default=None),
        limit: int = Query(default=100, ge=1, le=1000),
        offset: int = Query(default=0, ge=0),
    ) -> dict[str, Any]:
        guard(run_id)
        page = await service.process_registry(
            run_id,
            status=status,
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
        return page.model_dump(mode="json")

    @router.get("/runs/{run_id}/process-runtime/dead-letter")
    async def list_run_dead_letter_processes(
        run_id: str,
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        parent_document_id: str | None = Query(default=None),
        document_id: str | None = Query(default=None),
        process_id: str | None = Query(default=None),
        capability: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
        adapter_kind: AdapterKind | None = Query(default=None),
        resource_pool: str | None = Query(default=None),
        limit: int = Query(default=100, ge=1, le=1000),
        offset: int = Query(default=0, ge=0),
    ) -> dict[str, Any]:
        guard(run_id)
        page = await service.dead_letter_queue(
            run_id,
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
        return page.model_dump(mode="json")

    @router.get("/runs/{run_id}/process-runtime/stuck-work")
    async def list_run_stuck_work(
        run_id: str,
        status: ProcessStatus | None = Query(default=None),
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        parent_document_id: str | None = Query(default=None),
        document_id: str | None = Query(default=None),
        process_id: str | None = Query(default=None),
        capability: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
        adapter_kind: AdapterKind | None = Query(default=None),
        resource_pool: str | None = Query(default=None),
        waiting_after_seconds: float = Query(default=3600.0, ge=0),
        queued_after_seconds: float = Query(default=600.0, ge=0),
        running_after_seconds: float = Query(default=1800.0, ge=0),
        limit: int = Query(default=100, ge=1, le=1000),
        offset: int = Query(default=0, ge=0),
    ) -> dict[str, Any]:
        guard(run_id)
        try:
            page = await service.stuck_work(
                run_id,
                status=status,
                pipeline_id=pipeline_id,
                document_type=document_type,
                parent_document_id=parent_document_id,
                document_id=document_id,
                process_id=process_id,
                capability=capability,
                operation_type=operation_type,
                adapter_kind=adapter_kind,
                resource_pool=resource_pool,
                waiting_after_seconds=waiting_after_seconds,
                queued_after_seconds=queued_after_seconds,
                running_after_seconds=running_after_seconds,
                limit=limit,
                offset=offset,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return page.model_dump(mode="json")

    @router.get("/runs/{run_id}/process-runtime/stream-lag")
    async def list_run_stream_lag(
        run_id: str,
        pipeline_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        parent_document_id: str | None = Query(default=None),
        document_id: str | None = Query(default=None),
        process_id: str | None = Query(default=None),
        capability: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
        adapter_kind: AdapterKind | None = Query(default=None),
        resource_pool: str | None = Query(default=None),
        stream_id: str | None = Query(default=None),
        consumer_id: str | None = Query(default=None),
        min_lag: int = Query(default=1, ge=0),
        over_limit: bool | None = Query(default=None),
        limit: int = Query(default=100, ge=1, le=1000),
        offset: int = Query(default=0, ge=0),
    ) -> dict[str, Any]:
        guard(run_id)
        try:
            page = await service.stream_lag(
                run_id,
                pipeline_id=pipeline_id,
                document_type=document_type,
                parent_document_id=parent_document_id,
                document_id=document_id,
                process_id=process_id,
                capability=capability,
                operation_type=operation_type,
                adapter_kind=(
                    getattr(adapter_kind, "value", adapter_kind)
                    if adapter_kind is not None
                    else None
                ),
                resource_pool=resource_pool,
                stream_id=stream_id,
                consumer_id=consumer_id,
                min_lag=min_lag,
                over_limit=over_limit,
                limit=limit,
                offset=offset,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return page.model_dump(mode="json")

    @router.get("/runs/{run_id}/process-runtime/document-lineage")
    async def get_run_document_lineage(run_id: str) -> dict[str, Any]:
        guard(run_id)
        lineage = await service.document_lineage(run_id)
        return {"lineage": lineage.model_dump(mode="json")}

    @router.get("/runs/{run_id}/process-runtime/results")
    async def get_run_process_results(
        run_id: str,
        pipeline_id: str | None = Query(default=None),
        process_id: str | None = Query(default=None),
        document_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
    ) -> dict[str, Any]:
        guard(run_id)
        results = await service.run_results(
            run_id,
            pipeline_id=pipeline_id,
            process_id=process_id,
            document_id=document_id,
            document_type=document_type,
            operation_type=operation_type,
        )
        return {"results": results.model_dump(mode="json")}

    @router.get("/runs/{run_id}/process-runtime/output-documents")
    async def get_run_output_documents(
        run_id: str,
        pipeline_id: str | None = Query(default=None),
        process_id: str | None = Query(default=None),
        document_id: str | None = Query(default=None),
        source_document_type: str | None = Query(default=None),
        output_document_id: str | None = Query(default=None),
        document_type: str | None = Query(default=None),
        relation: str | None = Query(default=None),
        media_type: str | None = Query(default=None),
        limit: int = Query(default=100, ge=1),
        offset: int = Query(default=0, ge=0),
    ) -> dict[str, Any]:
        guard(run_id)
        page = await service.output_documents(
            run_id,
            pipeline_id=pipeline_id,
            process_id=process_id,
            document_id=document_id,
            source_document_type=source_document_type,
            output_document_id=output_document_id,
            document_type=document_type,
            relation=relation,
            media_type=media_type,
            limit=limit,
            offset=offset,
        )
        return page.model_dump(mode="json")

    @router.get("/runs/{run_id}/process-runtime/reductions")
    async def get_run_process_reductions(
        run_id: str,
        pipeline_id: str | None = Query(default=None),
        reduce_id: str | None = Query(default=None),
    ) -> dict[str, Any]:
        guard(run_id)
        reductions = await service.run_reductions(
            run_id,
            pipeline_id=pipeline_id,
            reduce_id=reduce_id,
        )
        return {"reductions": reductions.model_dump(mode="json")}

    @router.post("/runs/{run_id}/process-runtime/documents")
    async def initialize_document_runtime_state(
        request: Request,
        run_id: str,
        req: InitializeDocumentRuntimeRequest,
    ) -> dict[str, Any]:
        guard(run_id)
        try:
            result = await service.initialize_document(
                run_id=run_id,
                document_id=req.document_id,
                pipeline_id=req.pipeline_id,
                values=req.values,
                artifacts=req.artifacts,
            )
            await audit(
                request,
                action="document.initialize",
                run_id=run_id,
                document_id=req.document_id,
                target=f"run:{run_id}/document:{req.document_id}",
                data={
                    "pipeline_id": req.pipeline_id,
                    "value_keys": sorted(req.values),
                    "artifact_count": len(req.artifacts),
                },
            )
            return {"ok": True, "schedule": result.model_dump(mode="json")}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @router.post("/runs/{run_id}/process-runtime/documents/batch")
    async def append_run_process_documents(
        request: Request,
        run_id: str,
        req: AppendRuntimeDocumentsRequest,
    ) -> dict[str, Any]:
        guard(run_id)
        try:
            documents, route_report = service.route_runtime_document_inputs_with_report(
                req.documents,
                routes=req.routes,
                auto_route=req.auto_route,
            )
            run, schedules = await service.append_run_documents(
                run_id=run_id,
                pipeline_id=req.pipeline_id,
                documents=documents,
                existing_document_policy=req.existing_document_policy,
                route_report=route_report if req.auto_route or req.routes else None,
            )
        except LookupError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        await audit(
            request,
            action="documents.append",
            run_id=run_id,
            target=f"run:{run_id}/documents",
            data={
                "pipeline_id": req.pipeline_id,
                "existing_document_policy": req.existing_document_policy,
                "document_count": len(documents),
                "document_ids": [document.document_id for document in documents],
                "scheduled_count": len(schedules),
                "route_report": (
                    route_report
                    if req.auto_route or req.routes
                    else {
                        key: route_report[key]
                        for key in [
                            "document_count",
                            "routed_count",
                            "unrouted_count",
                            "missing_pipeline_count",
                            "missing_document_type_count",
                        ]
                    }
                ),
            },
        )
        return {
            "ok": True,
            "run": run.model_dump(mode="json"),
            "document_count": len(schedules),
            "route_report": route_report,
            "schedules": [schedule.model_dump(mode="json") for schedule in schedules],
        }

    @router.post("/runs/{run_id}/process-runtime/claim")
    async def claim_process_runtime_step(
        run_id: str,
        req: ClaimProcessRequest,
    ) -> dict[str, Any]:
        guard(run_id)
        try:
            claim = await service.claim_next(
                run_id=run_id,
                pipeline_id=req.pipeline_id,
                worker_id=req.worker_id,
                process_id=req.process_id,
                adapter_kind=req.adapter_kind,
                capabilities=req.capabilities,
                resources=req.resources,
                lease_seconds=req.lease_seconds,
            )
            return {
                "ok": True,
                "process": claim.model_dump(mode="json") if claim else None,
            }
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @router.post("/runs/{run_id}/process-runtime/{document_id:path}/events")
    async def append_process_event(
        request: Request,
        run_id: str,
        document_id: str,
        req: ProcessEventRequest,
    ) -> dict[str, Any]:
        guard(run_id)
        if req.process_id is not None:
            await ensure_document_process_id(
                run_id=run_id,
                document_id=document_id,
                process_id=req.process_id,
            )
        event = ProcessEvent(
            run_id=run_id,
            document_id=document_id,
            process_id=req.process_id,
            type=req.type,
            status=req.status,
            data=req.data,
        )
        await service.store.append_event(event)
        await audit(
            request,
            action="process.event.append",
            run_id=run_id,
            document_id=document_id,
            process_id=req.process_id,
            target=(
                f"run:{run_id}/document:{document_id}"
                + (f"/process:{req.process_id}" if req.process_id else "")
            ),
            data={
                "event_id": event.id,
                "event_type": req.type,
                "status": req.status.value if req.status else None,
                "data_keys": sorted(req.data),
            },
        )
        return {"ok": True, "event": event.model_dump(mode="json")}

    @router.get("/runs/{run_id}/process-runtime/{document_id:path}/events")
    async def list_process_events(
        run_id: str,
        document_id: str,
        process_id: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
        after_event_id: str | None = Query(default=None),
        limit: int = Query(default=100, ge=1, le=500),
    ) -> dict[str, Any]:
        guard(run_id)
        if process_id is not None:
            await ensure_document_process_id(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
            )
        try:
            events = await service.list_process_events(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                operation_type=operation_type,
                after_event_id=after_event_id,
                limit=limit + 1,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        selected = events[:limit]
        return ProcessEventPage(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            operation_type=operation_type,
            count=len(selected),
            has_more=len(events) > limit,
            next_after_event_id=selected[-1].id if selected else after_event_id,
            events=selected,
        ).model_dump(mode="json")

    @router.get("/runs/{run_id}/process-runtime/{document_id:path}/events/stream")
    async def stream_document_process_events(
        run_id: str,
        document_id: str,
        process_id: str | None = Query(default=None),
        operation_type: str | None = Query(default=None),
        after_event_id: str | None = Query(default=None),
        batch_limit: int = Query(default=100, ge=1, le=500),
        poll_interval_seconds: float = Query(default=1.0, ge=0.05, le=60.0),
        heartbeat_interval_seconds: float = Query(default=15.0, ge=1.0, le=300.0),
        max_events: int | None = Query(default=None, ge=1, le=10000),
    ) -> StreamingResponse:
        guard(run_id)
        if process_id is not None:
            await ensure_document_process_id(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
            )
        await _validate_event_cursor(
            service=service,
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            operation_type=operation_type,
            after_event_id=after_event_id,
        )
        return _event_stream_response(
            service=service,
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            operation_type=operation_type,
            after_event_id=after_event_id,
            batch_limit=batch_limit,
            poll_interval_seconds=poll_interval_seconds,
            heartbeat_interval_seconds=heartbeat_interval_seconds,
            max_events=max_events,
        )

    @router.post("/runs/{run_id}/process-runtime/{document_id:path}/processes/{process_id}/renew")
    async def renew_process_runtime_claim(
        run_id: str,
        document_id: str,
        process_id: str,
        req: RenewClaimRequest,
    ) -> dict[str, Any]:
        guard(run_id)
        try:
            claim = await service.renew_claim(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                pipeline_id=req.pipeline_id,
                worker_id=req.worker_id,
                lease_seconds=req.lease_seconds,
            )
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {
            "ok": claim is not None,
            "claim": claim.model_dump(mode="json") if claim else None,
        }

    @router.post(
        "/runs/{run_id}/process-runtime/dead-letter/{document_id:path}"
        "/processes/{process_id}/replay"
    )
    async def replay_dead_letter_process(
        request: Request,
        run_id: str,
        document_id: str,
        process_id: str,
        req: DeadLetterReplayRequest,
    ) -> dict[str, Any]:
        guard(run_id)
        try:
            result = await service.control_process(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                pipeline_id=req.pipeline_id,
                action=ProcessAction.retry,
                reason=req.reason or "dead letter replay",
                allow_contract_drift=req.allow_contract_drift,
            )
            await audit(
                request,
                action="process.dead_letter.replay",
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                target=f"run:{run_id}/document:{document_id}/process:{process_id}",
                data={
                    "pipeline_id": req.pipeline_id,
                    "reason": req.reason,
                    "affected": result.affected,
                    "queued_count": len(result.schedule.queued),
                    "waiting_count": len(result.schedule.waiting),
                    "allow_contract_drift": req.allow_contract_drift,
                },
            )
            return {"ok": True, "action": result.model_dump(mode="json")}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @router.post("/runs/{run_id}/process-runtime/{document_id:path}/schedule")
    async def schedule_process_runtime_document(
        request: Request,
        run_id: str,
        document_id: str,
        req: ScheduleDocumentRequest,
    ) -> dict[str, Any]:
        guard(run_id)
        try:
            result = await service.schedule_document(
                run_id=run_id,
                document_id=document_id,
                pipeline_id=req.pipeline_id,
            )
            await audit(
                request,
                action="document.schedule",
                run_id=run_id,
                document_id=document_id,
                target=f"run:{run_id}/document:{document_id}",
                data={
                    "pipeline_id": result.pipeline_id,
                    "queued_count": result.queued_count,
                    "waiting_count": result.waiting_count,
                },
            )
            return {"ok": True, "schedule": result.model_dump(mode="json")}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @router.post("/runs/{run_id}/process-runtime/{document_id:path}/processes/{process_id}/actions")
    async def control_process_runtime_step(
        request: Request,
        run_id: str,
        document_id: str,
        process_id: str,
        req: ProcessActionRequest,
    ) -> dict[str, Any]:
        guard(run_id)
        try:
            result = await service.control_process(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                pipeline_id=req.pipeline_id,
                action=req.action,
                reason=req.reason,
                allow_contract_drift=req.allow_contract_drift,
            )
            await audit(
                request,
                action=f"process.{req.action.value}",
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                target=f"run:{run_id}/document:{document_id}/process:{process_id}",
                data={
                    "pipeline_id": req.pipeline_id,
                    "reason": req.reason,
                    "affected": result.affected,
                    "queued_count": len(result.schedule.queued),
                    "waiting_count": len(result.schedule.waiting),
                    "allow_contract_drift": req.allow_contract_drift,
                },
            )
            return {"ok": True, "action": result.model_dump(mode="json")}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @router.get(
        "/runs/{run_id}/process-runtime/{document_id:path}"
        "/processes/{process_id}/artifacts/{artifact_id}/download"
    )
    async def download_process_runtime_artifact(
        run_id: str,
        document_id: str,
        process_id: str,
        artifact_id: str,
    ) -> FileResponse:
        guard(run_id)
        try:
            path = await service.resolve_artifact_path(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                artifact_id=artifact_id,
            )
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return FileResponse(
            str(path),
            filename=path.name,
            media_type="application/octet-stream",
        )

    @router.post(
        "/runs/{run_id}/process-runtime/{document_id:path}"
        "/processes/{process_id}/streams/{stream_id}/chunks"
    )
    async def append_process_runtime_stream_chunk(
        request: Request,
        run_id: str,
        document_id: str,
        process_id: str,
        stream_id: str,
        req: StreamChunkRequest,
        pipeline_id: str | None = Query(default=None),
        worker_id: str | None = Query(default=None),
    ) -> dict[str, Any]:
        guard(run_id)
        await pipeline_for_write(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            pipeline_id=pipeline_id,
        )
        await ensure_process_writer(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            worker_id=worker_id,
        )
        try:
            chunk = await service.append_stream_chunk(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                pipeline_id=pipeline_id,
                stream_id=stream_id,
                sequence=req.sequence,
                kind=req.kind,
                values=req.values,
                artifacts=req.artifacts,
                metadata=req.metadata,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        await audit(
            request,
            action="stream.chunk.append",
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            target=(
                f"run:{run_id}/document:{document_id}/process:{process_id}"
                f"/stream:{stream_id}"
            ),
            worker_id=worker_id,
            data={
                "pipeline_id": pipeline_id,
                "stream_id": stream_id,
                "chunk_id": chunk.chunk_id,
                "sequence": chunk.sequence,
                "kind": chunk.kind,
                "value_keys": sorted(chunk.values),
                "artifact_count": len(chunk.artifacts),
            },
        )
        return {"ok": True, "chunk": chunk.model_dump(mode="json")}

    @router.get(
        "/runs/{run_id}/process-runtime/{document_id:path}"
        "/processes/{process_id}/streams/{stream_id}/chunks"
    )
    async def list_process_runtime_stream_chunks(
        run_id: str,
        document_id: str,
        process_id: str,
        stream_id: str,
        after_sequence: int | None = Query(default=None, ge=-1),
        limit: int | None = Query(default=None, ge=1, le=10000),
    ) -> dict[str, Any]:
        guard(run_id)
        await ensure_document_process_id(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        chunks = await service.list_stream_chunks(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            stream_id=stream_id,
            after_sequence=after_sequence,
            limit=limit,
        )
        return {
            "ok": True,
            "run_id": run_id,
            "document_id": document_id,
            "process_id": process_id,
            "stream_id": stream_id,
            "chunk_count": len(chunks),
            "chunks": [chunk.model_dump(mode="json") for chunk in chunks],
        }

    @router.get(
        "/runs/{run_id}/process-runtime/{document_id:path}"
        "/processes/{process_id}/streams/{stream_id}/chunks/{chunk_id}"
        "/artifacts/{artifact_id}/download"
    )
    async def download_process_runtime_stream_artifact(
        run_id: str,
        document_id: str,
        process_id: str,
        stream_id: str,
        chunk_id: str,
        artifact_id: str,
    ) -> FileResponse:
        guard(run_id)
        try:
            path = await service.resolve_stream_artifact_path(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                stream_id=stream_id,
                chunk_id=chunk_id,
                artifact_id=artifact_id,
            )
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return FileResponse(
            str(path),
            filename=path.name,
            media_type="application/octet-stream",
        )

    @router.put(
        "/runs/{run_id}/process-runtime/{document_id:path}"
        "/processes/{process_id}/streams/{stream_id}/checkpoints/{consumer_id}"
    )
    async def put_process_runtime_stream_checkpoint(
        request: Request,
        run_id: str,
        document_id: str,
        process_id: str,
        stream_id: str,
        consumer_id: str,
        req: StreamCheckpointRequest,
    ) -> dict[str, Any]:
        guard(run_id)
        await ensure_document_process_id(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        checkpoint = await service.put_stream_checkpoint(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            stream_id=stream_id,
            consumer_id=consumer_id,
            sequence=req.sequence,
            chunk_id=req.chunk_id,
            metadata=req.metadata,
        )
        await audit(
            request,
            action="stream.checkpoint.put",
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            target=(
                f"run:{run_id}/document:{document_id}/process:{process_id}"
                f"/stream:{stream_id}/consumer:{consumer_id}"
            ),
            data={
                "stream_id": stream_id,
                "consumer_id": consumer_id,
                "sequence": checkpoint.sequence,
                "chunk_id": checkpoint.chunk_id,
            },
        )
        return {"ok": True, "checkpoint": checkpoint.model_dump(mode="json")}

    @router.get(
        "/runs/{run_id}/process-runtime/{document_id:path}"
        "/processes/{process_id}/streams/{stream_id}/checkpoints/{consumer_id}"
    )
    async def get_process_runtime_stream_checkpoint(
        run_id: str,
        document_id: str,
        process_id: str,
        stream_id: str,
        consumer_id: str,
    ) -> dict[str, Any]:
        guard(run_id)
        await ensure_document_process_id(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        checkpoint = await service.get_stream_checkpoint(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            stream_id=stream_id,
            consumer_id=consumer_id,
        )
        return {
            "ok": True,
            "checkpoint": checkpoint.model_dump(mode="json") if checkpoint else None,
        }

    @router.put("/runs/{run_id}/process-runtime/{document_id:path}/processes/{process_id}/status")
    async def set_process_status(
        request: Request,
        run_id: str,
        document_id: str,
        process_id: str,
        req: ProcessStatusUpdateRequest,
    ) -> dict[str, Any]:
        guard(run_id)
        pipeline = await pipeline_for_write(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            pipeline_id=req.pipeline_id,
        )
        await ensure_process_writer(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            worker_id=req.worker_id,
        )
        if req.status == ProcessStatus.failed and pipeline is not None:
            action = await PipelineScheduler(pipeline, service.store).record_process_failure(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                reason=_reason_from_data(req.data),
                error_kind=_error_kind_from_data(req.data),
                data=req.data,
            )
            await audit(
                request,
                action="process.status.put",
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                target=f"run:{run_id}/document:{document_id}/process:{process_id}",
                worker_id=req.worker_id,
                data={
                    "pipeline_id": req.pipeline_id,
                    "worker_id": req.worker_id,
                    "status": req.status.value,
                    "data_keys": sorted(req.data),
                },
            )
            return {
                "ok": True,
                "status": req.status,
                "action": action.model_dump(mode="json"),
            }
        process_metadata: dict[str, str | None] = {}
        if pipeline is not None:
            step = next((item for item in pipeline.steps if item.id == process_id), None)
            if step is not None:
                process_metadata = {
                    "pipeline_id": pipeline.id,
                    "capability": step.capability,
                    "adapter_kind": step.adapter.kind,
                    "resource_pool": step.resource_pool,
                }
        await service.store.set_status(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            status=req.status,
            **process_metadata,
        )
        await service.store.append_event(
            ProcessEvent(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                type=f"process.{req.status.value}",
                status=req.status,
                data=req.data,
            )
        )
        await service.sync_run_lifecycle(run_id)
        await audit(
            request,
            action="process.status.put",
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            target=f"run:{run_id}/document:{document_id}/process:{process_id}",
            worker_id=req.worker_id,
            data={
                "pipeline_id": req.pipeline_id,
                "worker_id": req.worker_id,
                "status": req.status.value,
                "data_keys": sorted(req.data),
            },
        )
        return {"ok": True, "status": req.status}

    @router.put("/runs/{run_id}/process-runtime/{document_id:path}/processes/{process_id}/output")
    async def write_process_output(
        request: Request,
        run_id: str,
        document_id: str,
        process_id: str,
        output: ProcessOutput,
        pipeline_id: str | None = Query(default=None),
        worker_id: str | None = Query(default=None),
    ) -> dict[str, Any]:
        guard(run_id)
        pipeline = await pipeline_for_write(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            pipeline_id=pipeline_id,
        )
        await ensure_process_writer(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            worker_id=worker_id,
        )
        try:
            _output, refreshed, schedule, spawned = await service.complete_process_output(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                output=output,
                pipeline_id=pipeline.id if pipeline is not None else pipeline_id,
                worker_id=worker_id,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        await audit(
            request,
            action="process.output.put",
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            target=f"run:{run_id}/document:{document_id}/process:{process_id}",
            worker_id=worker_id,
            data={
                "pipeline_id": pipeline.id if pipeline is not None else pipeline_id,
                "worker_id": worker_id,
                "value_keys": sorted(_output.values),
                "artifact_count": len(_output.artifacts),
                "spawned_document_count": len(spawned),
                "refreshed_projection_count": len(refreshed),
            },
        )
        return {
            "ok": True,
            "schedule": schedule.model_dump(mode="json"),
            "refreshed_projections": [
                item.model_dump(mode="json") for item in refreshed
            ],
            "spawned_documents": [
                item.model_dump(mode="json") for item in spawned
            ],
        }

    return router


def _package_summary(registry, package) -> dict[str, Any]:
    return {
        "id": package.id,
        "title": package.title,
        "description": package.description,
        "tags": package.tags,
        "version": package.version,
        "document_types": [
            document_type.model_dump(mode="json")
            for document_type in package.document_types
        ],
        "artifact_kinds": [
            artifact_kind.model_dump(mode="json")
            for artifact_kind in package.artifact_kinds
        ],
        "capabilities": [
            capability.model_dump(mode="json")
            for capability in package.capabilities
        ],
        "secrets": [
            secret.model_dump(mode="json")
            for secret in package.secrets
        ],
        "pipelines": package.pipelines,
        "pipeline_ids": registry.package_pipeline_ids(package.id),
        "workers": [
            {
                "id": worker.id,
                "title": worker.title,
                "description": worker.description,
                "tags": worker.tags,
                "capabilities": worker.capabilities,
                "pipeline_id": worker.pipeline_id,
                "process_id": worker.process_id,
                "adapter_kind": worker.adapter_kind,
                "command": worker.command,
                "cwd": worker.cwd,
                "env": worker.env,
                "timeout_seconds": worker.timeout_seconds,
                "resources": worker.resources.model_dump(mode="json"),
                "secrets": list(worker.secrets),
                "sandbox": worker.sandbox.model_dump(mode="json"),
            }
            for worker in package.workers
        ],
    }


def _retention_cutoff(
    *,
    before: str | None,
    older_than_days: float | None,
) -> datetime:
    if before is not None and older_than_days is not None:
        raise ValueError("Use only one of before or older_than_days")
    if older_than_days is not None:
        return datetime.now(timezone.utc) - timedelta(days=older_than_days)
    if before is None:
        raise ValueError("before or older_than_days is required")
    try:
        parsed = datetime.fromisoformat(before.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("before must be an ISO datetime") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _pipeline_summary(registry, pipeline) -> dict[str, Any]:
    return {
        "id": pipeline.id,
        "package_id": registry.pipeline_package_id(pipeline.id),
        "title": pipeline.title,
        "description": pipeline.description,
        "tags": pipeline.tags,
        "version": pipeline.version,
        "input_values": pipeline.input_values,
        "steps": [
            {
                "id": step.id,
                "title": step.title,
                "description": step.description,
                "tags": step.tags,
                "capability": step.capability,
                "adapter_kind": step.adapter.kind,
                "needs": step.needs,
                "priority": step.priority,
                "max_concurrency": step.max_concurrency,
                "resource_pool": step.resource_pool,
                "resources": step.resources.model_dump(mode="json"),
            }
            for step in pipeline.steps
        ],
        "combines": [combine.id for combine in pipeline.combines],
        "reduces": [reduce.id for reduce in pipeline.reduces],
    }


async def _validate_event_cursor(
    *,
    service: RuntimeService,
    run_id: str,
    document_id: str | None,
    process_id: str | None,
    after_event_id: str | None,
    operation_type: str | None = None,
) -> None:
    if after_event_id is None:
        return
    try:
        await service.list_process_events(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            operation_type=operation_type,
            after_event_id=after_event_id,
            limit=0,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _event_stream_response(
    *,
    service: RuntimeService,
    run_id: str,
    document_id: str | None,
    process_id: str | None,
    operation_type: str | None,
    after_event_id: str | None,
    batch_limit: int,
    poll_interval_seconds: float,
    heartbeat_interval_seconds: float,
    max_events: int | None,
) -> StreamingResponse:
    async def stream():
        cursor = after_event_id
        emitted = 0
        last_heartbeat = monotonic()
        while True:
            events = await service.list_process_events(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                operation_type=operation_type,
                after_event_id=cursor,
                limit=batch_limit,
            )
            if events:
                for event in events:
                    yield _sse_message(
                        event="process",
                        data=event.model_dump(mode="json"),
                        event_id=event.id,
                    )
                    cursor = event.id
                    emitted += 1
                    if max_events is not None and emitted >= max_events:
                        return
                last_heartbeat = monotonic()
                continue

            now = monotonic()
            if now - last_heartbeat >= heartbeat_interval_seconds:
                yield ": heartbeat\n\n"
                last_heartbeat = now
            await asyncio.sleep(poll_interval_seconds)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


def _sse_message(*, event: str, data: dict[str, Any], event_id: str | None = None) -> str:
    lines: list[str] = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {event}")
    payload = json.dumps(data, separators=(",", ":"), sort_keys=True)
    for line in payload.splitlines() or [""]:
        lines.append(f"data: {line}")
    return "\n".join(lines) + "\n\n"
