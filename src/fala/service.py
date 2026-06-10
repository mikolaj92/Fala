from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from fala.models import (
    ArtifactRef,
    CombinedProjection,
    PipelineSpec,
    ProcessAction,
    ProcessClaim,
    ProcessEvent,
    ProcessStatus,
    RuntimeState,
)
from fala.registry import PipelineRegistry
from fala.scheduler import ClaimedProcess, PipelineScheduler, ProcessControlResult, ScheduleResult
from fala.state import build_runtime_document_state, build_runtime_state
from fala.store import StateStore


class RuntimeService:
    def __init__(
        self,
        *,
        registry: PipelineRegistry,
        store: StateStore,
        artifact_roots: list[str | Path] | None = None,
    ) -> None:
        self.registry = registry
        self.store = store
        self.artifact_roots = [
            _resolve_runtime_artifact_root(root) for root in artifact_roots or []
        ]

    async def initialize_document(
        self,
        *,
        run_id: str,
        document_id: str,
        pipeline_id: str,
        values: dict | None = None,
        artifacts: list[ArtifactRef] | None = None,
    ) -> ScheduleResult:
        pipeline = self.registry.get(pipeline_id)
        return await PipelineScheduler(pipeline, self.store).initialize_document(
            run_id=run_id,
            document_id=document_id,
            values=values or {},
            artifacts=artifacts or [],
        )

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
        return await PipelineScheduler(pipeline, self.store).schedule_ready(
            run_id=run_id,
            document_id=document_id,
        )

    async def claim_next(
        self,
        *,
        run_id: str,
        pipeline_id: str,
        worker_id: str | None = None,
        process_id: str | None = None,
        adapter_kind: str | None = None,
        lease_seconds: float = 300.0,
    ) -> ClaimedProcess | None:
        pipeline = self.registry.get(pipeline_id)
        document_ids = await self.store.list_documents(run_id=run_id)
        return await PipelineScheduler(pipeline, self.store).claim_next(
            run_id=run_id,
            document_ids=document_ids,
            worker_id=worker_id,
            process_id=process_id,
            adapter_kind=adapter_kind,
            lease_seconds=lease_seconds,
        )

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
    ) -> ProcessControlResult:
        pipeline = await self.resolve_document_pipeline(
            run_id=run_id,
            document_id=document_id,
            pipeline_id=pipeline_id,
        )
        scheduler = PipelineScheduler(pipeline, self.store)
        action = ProcessAction(action)
        if action == ProcessAction.retry:
            return await scheduler.retry_process(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                reason=reason,
            )
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
            return result
        if action == ProcessAction.fail:
            return await scheduler.fail_process(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                reason=reason,
            )
        if action == ProcessAction.cancel:
            return await scheduler.cancel_process(
                run_id=run_id,
                document_id=document_id,
                process_id=process_id,
                reason=reason,
            )
        raise ValueError(f"Unknown process action: {action}")

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
                    events=events,
                    event_count=event_count,
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

    def _runtime_artifact_roots(self) -> list[Path]:
        roots: list[Path] = list(self.artifact_roots)
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


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True
