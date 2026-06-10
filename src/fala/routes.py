from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.responses import FileResponse

from fala.models import (
    AdapterKind,
    ArtifactRef,
    ProcessAction,
    ProcessEvent,
    ProcessEventPage,
    ProcessOutput,
    ProcessStatus,
)
from fala.scheduler import PipelineScheduler
from fala.service import RuntimeService

RunAccessGuard = Callable[[str], None]


class ProcessStatusUpdateRequest(BaseModel):
    status: ProcessStatus
    pipeline_id: str | None = None
    worker_id: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)


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


class ClaimProcessRequest(BaseModel):
    pipeline_id: str
    worker_id: str | None = None
    process_id: str | None = None
    adapter_kind: AdapterKind | None = None
    lease_seconds: float = Field(default=300.0, gt=0)


class RenewClaimRequest(BaseModel):
    pipeline_id: str | None = None
    worker_id: str | None = None
    lease_seconds: float = Field(default=300.0, gt=0)


class ProcessActionRequest(BaseModel):
    pipeline_id: str | None = None
    action: ProcessAction
    reason: str | None = None


class ScheduleDocumentRequest(BaseModel):
    pipeline_id: str | None = None


def create_runtime_router(
    service: RuntimeService,
    *,
    ensure_run_access: RunAccessGuard | None = None,
) -> APIRouter:
    router = APIRouter()

    def guard(run_id: str) -> None:
        if ensure_run_access is not None:
            ensure_run_access(run_id)

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

    @router.get("/runs/{run_id}/process-runtime")
    async def get_run_process_runtime(
        run_id: str,
        include_events: bool = Query(default=False),
    ) -> dict[str, Any]:
        guard(run_id)
        return await service.load_state(run_id, include_events=include_events)

    @router.post("/runs/{run_id}/process-runtime/documents")
    async def initialize_document_runtime_state(
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
            return {"ok": True, "schedule": result.model_dump(mode="json")}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

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
        return {"ok": True, "event": event.model_dump(mode="json")}

    @router.get("/runs/{run_id}/process-runtime/{document_id:path}/events")
    async def list_process_events(
        run_id: str,
        document_id: str,
        process_id: str | None = Query(default=None),
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
            events = await service.store.list_events(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
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
            count=len(selected),
            has_more=len(events) > limit,
            next_after_event_id=selected[-1].id if selected else after_event_id,
            events=selected,
        ).model_dump(mode="json")

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

    @router.post("/runs/{run_id}/process-runtime/{document_id:path}/schedule")
    async def schedule_process_runtime_document(
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
            return {"ok": True, "schedule": result.model_dump(mode="json")}
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

    @router.post("/runs/{run_id}/process-runtime/{document_id:path}/processes/{process_id}/actions")
    async def control_process_runtime_step(
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

    @router.put("/runs/{run_id}/process-runtime/{document_id:path}/processes/{process_id}/status")
    async def set_process_status(
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
            await PipelineScheduler(pipeline, service.store).record_process_failure(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                data=req.data,
            )
            return {"ok": True, "status": req.status}
        await service.store.set_status(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            status=req.status,
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
        return {"ok": True, "status": req.status}

    @router.put("/runs/{run_id}/process-runtime/{document_id:path}/processes/{process_id}/output")
    async def write_process_output(
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
        await service.store.put_output(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            output=output,
        )
        await service.store.set_status(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            status=ProcessStatus.completed,
        )
        await service.store.clear_claim(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
        )
        if pipeline is not None:
            refreshed = await service.refresh_projections_for_process(
                pipeline=pipeline,
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
            )
            schedule = await PipelineScheduler(pipeline, service.store).schedule_ready(
                run_id=run_id,
                document_id=document_id,
            )
            return {
                "ok": True,
                "schedule": schedule.model_dump(mode="json"),
                "refreshed_projections": [
                    item.model_dump(mode="json") for item in refreshed
                ],
            }
        return {"ok": True}

    return router


def _package_summary(registry, package) -> dict[str, Any]:
    return {
        "id": package.id,
        "title": package.title,
        "description": package.description,
        "tags": package.tags,
        "version": package.version,
        "pipelines": package.pipelines,
        "pipeline_ids": registry.package_pipeline_ids(package.id),
        "workers": [
            {
                "id": worker.id,
                "title": worker.title,
                "description": worker.description,
                "tags": worker.tags,
                "pipeline_id": worker.pipeline_id,
                "process_id": worker.process_id,
                "adapter_kind": worker.adapter_kind,
                "command": worker.command,
                "cwd": worker.cwd,
                "env": worker.env,
                "timeout_seconds": worker.timeout_seconds,
            }
            for worker in package.workers
        ],
    }


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
                "adapter_kind": step.adapter.kind,
                "needs": step.needs,
            }
            for step in pipeline.steps
        ],
        "combines": [combine.id for combine in pipeline.combines],
    }
