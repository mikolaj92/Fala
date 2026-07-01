from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import quote

import httpx

from fala.models import (
    AdapterKind,
    ArtifactRef,
    ExistingDocumentPolicy,
    ExistingRunPolicy,
    OperatorAuditEventPage,
    ProcessAction,
    ProcessClaim,
    ProcessEvent,
    ProcessEventPage,
    ProcessOutput,
    ProcessStatus,
    ResourceSpec,
    RuntimeCapabilityDemandSummary,
    RuntimeDeadLetterPage,
    RuntimeDocumentPage,
    RuntimeDocumentInput,
    RuntimeOutputDocumentPage,
    RuntimeQueueMetrics,
    RuntimeProcessPage,
    RuntimeRunHealth,
    RuntimeStuckWorkPage,
    RuntimeState,
    RuntimeStepReport,
    RuntimeStreamBatch,
    RuntimeStreamCheckpoint,
    RuntimeStreamChunk,
    RuntimeStreamLagPage,
    RuntimeTrace,
    RuntimeWorkerState,
    RuntimeWorkerStatus,
)
from fala.scheduler import ClaimedProcess, ProcessControlResult, WaitGraphDiagnostic


class ProcessRuntimeClient:
    """Async client for external process workers."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 30.0,
        api_key: str | None = None,
        headers: dict[str, str] | None = None,
        client: httpx.AsyncClient | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        request_headers = dict(headers or {})
        if api_key is not None:
            request_headers.setdefault("authorization", f"Bearer {api_key}")
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout,
            headers=request_headers or None,
            transport=transport,
        )

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> "ProcessRuntimeClient":
        return self

    async def __aexit__(self, *_args: object) -> None:
        await self.aclose()

    async def initialize_document(
        self,
        *,
        run_id: str,
        document_id: str,
        pipeline_id: str,
        values: dict[str, Any] | None = None,
        artifacts: list[ArtifactRef | dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        response = await self._client.post(
            f"/api/runs/{self._part(run_id)}/process-runtime/documents",
            json={
                "pipeline_id": pipeline_id,
                "document_id": document_id,
                "values": values or {},
                "artifacts": [
                    artifact.model_dump(mode="json")
                    if isinstance(artifact, ArtifactRef)
                    else artifact
                    for artifact in artifacts or []
                ],
            },
        )
        response.raise_for_status()
        return response.json()["schedule"]

    async def append_documents(
        self,
        *,
        run_id: str,
        pipeline_id: str | None = None,
        documents: list[RuntimeDocumentInput | dict[str, Any]],
        existing_document_policy: ExistingDocumentPolicy = "error",
        auto_route: bool = False,
        routes: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        response = await self._client.post(
            f"/api/runs/{self._part(run_id)}/process-runtime/documents/batch",
            json={
                "pipeline_id": pipeline_id,
                "existing_document_policy": existing_document_policy,
                "auto_route": auto_route,
                "routes": routes or [],
                "documents": [
                    document.model_dump(mode="json")
                    if isinstance(document, RuntimeDocumentInput)
                    else document
                    for document in documents
                ],
            },
        )
        response.raise_for_status()
        return response.json()

    async def create_run(
        self,
        *,
        run_id: str | None = None,
        title: str | None = None,
        pipeline_id: str | None = None,
        config: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        documents: list[RuntimeDocumentInput | dict[str, Any]] | None = None,
        existing_run_policy: ExistingRunPolicy = "error",
        existing_document_policy: ExistingDocumentPolicy = "error",
        auto_route: bool = False,
        routes: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        response = await self._client.post(
            "/api/process-runtime/runs",
            json={
                "run_id": run_id,
                "existing_run_policy": existing_run_policy,
                "existing_document_policy": existing_document_policy,
                "title": title,
                "pipeline_id": pipeline_id,
                "config": config or {},
                "metadata": metadata or {},
                "auto_route": auto_route,
                "routes": routes or [],
                "documents": [
                    document.model_dump(mode="json")
                    if isinstance(document, RuntimeDocumentInput)
                    else document
                    for document in documents or []
                ],
            },
        )
        response.raise_for_status()
        return response.json()

    async def validate_run(
        self,
        *,
        run_id: str | None = None,
        title: str | None = None,
        pipeline_id: str | None = None,
        config: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        documents: list[RuntimeDocumentInput | dict[str, Any]] | None = None,
        existing_run_policy: ExistingRunPolicy = "error",
        existing_document_policy: ExistingDocumentPolicy = "error",
        auto_route: bool = False,
        routes: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        response = await self._client.post(
            "/api/process-runtime/runs/validate",
            json={
                "run_id": run_id,
                "existing_run_policy": existing_run_policy,
                "existing_document_policy": existing_document_policy,
                "title": title,
                "pipeline_id": pipeline_id,
                "config": config or {},
                "metadata": metadata or {},
                "auto_route": auto_route,
                "routes": routes or [],
                "documents": [
                    document.model_dump(mode="json")
                    if isinstance(document, RuntimeDocumentInput)
                    else document
                    for document in documents or []
                ],
            },
        )
        response.raise_for_status()
        return response.json()

    async def route_run(
        self,
        *,
        run_id: str | None = None,
        title: str | None = None,
        pipeline_id: str | None = None,
        config: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        documents: list[RuntimeDocumentInput | dict[str, Any]] | None = None,
        existing_run_policy: ExistingRunPolicy = "error",
        existing_document_policy: ExistingDocumentPolicy = "error",
        auto_route: bool = True,
        routes: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        response = await self._client.post(
            "/api/process-runtime/runs/route",
            json={
                "run_id": run_id,
                "existing_run_policy": existing_run_policy,
                "existing_document_policy": existing_document_policy,
                "title": title,
                "pipeline_id": pipeline_id,
                "config": config or {},
                "metadata": metadata or {},
                "auto_route": auto_route,
                "routes": routes or [],
                "documents": [
                    document.model_dump(mode="json")
                    if isinstance(document, RuntimeDocumentInput)
                    else document
                    for document in documents or []
                ],
            },
        )
        response.raise_for_status()
        return response.json()

    async def plan_run(
        self,
        *,
        run_id: str | None = None,
        title: str | None = None,
        pipeline_id: str | None = None,
        config: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        documents: list[RuntimeDocumentInput | dict[str, Any]] | None = None,
        existing_run_policy: ExistingRunPolicy = "error",
        existing_document_policy: ExistingDocumentPolicy = "error",
        auto_route: bool = False,
        routes: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        response = await self._client.post(
            "/api/process-runtime/runs/plan",
            json={
                "run_id": run_id,
                "existing_run_policy": existing_run_policy,
                "existing_document_policy": existing_document_policy,
                "title": title,
                "pipeline_id": pipeline_id,
                "config": config or {},
                "metadata": metadata or {},
                "auto_route": auto_route,
                "routes": routes or [],
                "documents": [
                    document.model_dump(mode="json")
                    if isinstance(document, RuntimeDocumentInput)
                    else document
                    for document in documents or []
                ],
            },
        )
        response.raise_for_status()
        return response.json()

    async def control_run(
        self,
        *,
        run_id: str,
        action: str,
        reason: str | None = None,
        allow_contract_drift: bool = False,
    ) -> dict[str, Any]:
        response = await self._client.post(
            f"/api/process-runtime/runs/{self._part(run_id)}/actions",
            json={
                "action": action,
                "reason": reason,
                "allow_contract_drift": allow_contract_drift,
            },
        )
        response.raise_for_status()
        return response.json()

    async def run_provenance(self, *, run_id: str) -> dict[str, Any]:
        response = await self._client.get(
            f"/api/process-runtime/runs/{self._part(run_id)}/provenance"
        )
        response.raise_for_status()
        return response.json()

    async def artifact_gc(self, *, delete: bool = False) -> dict[str, Any]:
        if delete:
            response = await self._client.post(
                "/api/process-runtime/artifacts/gc",
                json={"delete": True},
            )
        else:
            response = await self._client.get("/api/process-runtime/artifacts/gc")
        response.raise_for_status()
        return response.json()["artifact_gc"]

    async def run_retention(
        self,
        *,
        before: str | None = None,
        older_than_days: float | None = None,
        statuses: list[str] | None = None,
        delete: bool = False,
    ) -> dict[str, Any]:
        if delete:
            response = await self._client.post(
                "/api/process-runtime/runs/retention",
                json={
                    "before": before,
                    "older_than_days": older_than_days,
                    "statuses": statuses or [],
                    "delete": True,
                },
            )
        else:
            params: list[tuple[str, str | float]] = []
            if before is not None:
                params.append(("before", before))
            if older_than_days is not None:
                params.append(("older_than_days", older_than_days))
            for status in statuses or []:
                params.append(("status", status))
            response = await self._client.get(
                "/api/process-runtime/runs/retention",
                params=params,
            )
        response.raise_for_status()
        return response.json()["retention"]

    async def operator_audit(
        self,
        *,
        run_id: str | None = None,
        limit: int = 100,
    ) -> OperatorAuditEventPage:
        params: dict[str, Any] = {"limit": limit}
        if run_id is not None:
            params["run_id"] = run_id
        response = await self._client.get(
            "/api/process-runtime/audit",
            params=params,
        )
        response.raise_for_status()
        return OperatorAuditEventPage.model_validate(response.json()["audit"])

    async def list_packages(self) -> dict[str, Any]:
        response = await self._client.get("/api/process-runtime/packages")
        response.raise_for_status()
        return response.json()

    async def get_project(self) -> dict[str, Any]:
        response = await self._client.get("/api/process-runtime/project")
        response.raise_for_status()
        return response.json()

    async def get_project_spec(
        self,
        *,
        base_url: str = "http://localhost:8000",
        run_id: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"base_url": base_url}
        if run_id is not None:
            params["run_id"] = run_id
        response = await self._client.get(
            "/api/process-runtime/project/spec",
            params=params,
        )
        response.raise_for_status()
        return response.json()["spec"]

    async def get_project_bootstrap(
        self,
        *,
        base_url: str = "http://localhost:8000",
        run_id: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"base_url": base_url}
        if run_id is not None:
            params["run_id"] = run_id
        response = await self._client.get(
            "/api/process-runtime/project/bootstrap",
            params=params,
        )
        response.raise_for_status()
        return response.json()

    async def get_project_runs(
        self,
        *,
        status: str | None = None,
        package_id: str | None = None,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": limit}
        if status is not None:
            params["status"] = status
        if package_id is not None:
            params["package_id"] = package_id
        if pipeline_id is not None:
            params["pipeline_id"] = pipeline_id
        if document_type is not None:
            params["document_type"] = document_type
        response = await self._client.get(
            "/api/process-runtime/project/runs",
            params=params,
        )
        response.raise_for_status()
        return response.json()["history"]

    async def get_project_supervision(
        self,
        *,
        package_id: str | None = None,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        operation_type: str | None = None,
        stuck_status: str | None = None,
        waiting_after_seconds: float = 3600.0,
        queued_after_seconds: float = 600.0,
        running_after_seconds: float = 1800.0,
        consumer_id: str | None = None,
        min_lag: int = 1,
        over_limit: bool | None = None,
        limit: int = 50,
    ) -> dict[str, Any]:
        params = {
            key: value
            for key, value in {
                "package_id": package_id,
                "pipeline_id": pipeline_id,
                "document_type": document_type,
                "operation_type": operation_type,
                "stuck_status": stuck_status,
                "waiting_after_seconds": waiting_after_seconds,
                "queued_after_seconds": queued_after_seconds,
                "running_after_seconds": running_after_seconds,
                "consumer_id": consumer_id,
                "min_lag": min_lag,
                "over_limit": over_limit,
                "limit": limit,
            }.items()
            if value is not None
        }
        response = await self._client.get(
            "/api/process-runtime/project/supervision",
            params=params,
        )
        response.raise_for_status()
        return response.json()["supervision"]

    async def get_project_operations(
        self,
        *,
        package_id: str | None = None,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        operation_type: str | None = None,
        stuck_status: str | None = None,
        waiting_after_seconds: float = 3600.0,
        queued_after_seconds: float = 600.0,
        running_after_seconds: float = 1800.0,
        consumer_id: str | None = None,
        min_lag: int = 1,
        over_limit: bool | None = None,
        stale_after_seconds: float = 60.0,
        limit: int = 50,
    ) -> dict[str, Any]:
        params = {
            key: value
            for key, value in {
                "package_id": package_id,
                "pipeline_id": pipeline_id,
                "document_type": document_type,
                "operation_type": operation_type,
                "stuck_status": stuck_status,
                "waiting_after_seconds": waiting_after_seconds,
                "queued_after_seconds": queued_after_seconds,
                "running_after_seconds": running_after_seconds,
                "consumer_id": consumer_id,
                "min_lag": min_lag,
                "over_limit": over_limit,
                "stale_after_seconds": stale_after_seconds,
                "limit": limit,
            }.items()
            if value is not None
        }
        response = await self._client.get(
            "/api/process-runtime/project/operations",
            params=params,
        )
        response.raise_for_status()
        return response.json()["operations"]

    async def get_project_alerts(
        self,
        *,
        package_id: str | None = None,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        operation_type: str | None = None,
        stuck_status: str | None = None,
        waiting_after_seconds: float = 3600.0,
        queued_after_seconds: float = 600.0,
        running_after_seconds: float = 1800.0,
        consumer_id: str | None = None,
        min_lag: int = 1,
        over_limit: bool | None = None,
        stale_after_seconds: float = 60.0,
        limit: int = 50,
    ) -> dict[str, Any]:
        params = {
            key: value
            for key, value in {
                "package_id": package_id,
                "pipeline_id": pipeline_id,
                "document_type": document_type,
                "operation_type": operation_type,
                "stuck_status": stuck_status,
                "waiting_after_seconds": waiting_after_seconds,
                "queued_after_seconds": queued_after_seconds,
                "running_after_seconds": running_after_seconds,
                "consumer_id": consumer_id,
                "min_lag": min_lag,
                "over_limit": over_limit,
                "stale_after_seconds": stale_after_seconds,
                "limit": limit,
            }.items()
            if value is not None
        }
        response = await self._client.get(
            "/api/process-runtime/project/alerts",
            params=params,
        )
        response.raise_for_status()
        return response.json()["alerts"]

    async def get_project_lifecycle(
        self,
        *,
        package_id: str | None = None,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        before: str | None = None,
        older_than_days: float | None = None,
        statuses: list[str] | None = None,
        include_artifact_gc: bool = True,
        limit: int = 50,
    ) -> dict[str, Any]:
        params: list[tuple[str, Any]] = [
            ("include_artifact_gc", include_artifact_gc),
            ("limit", limit),
        ]
        for key, value in {
            "package_id": package_id,
            "pipeline_id": pipeline_id,
            "document_type": document_type,
            "before": before,
            "older_than_days": older_than_days,
        }.items():
            if value is not None:
                params.append((key, value))
        for status in statuses or []:
            params.append(("status", status))
        response = await self._client.get(
            "/api/process-runtime/project/lifecycle",
            params=params,
        )
        response.raise_for_status()
        return response.json()["lifecycle"]

    async def run_project_lifecycle(
        self,
        *,
        package_id: str | None = None,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        before: str | None = None,
        older_than_days: float | None = None,
        statuses: list[str] | None = None,
        include_artifact_gc: bool = True,
        delete: bool = False,
        limit: int = 50,
    ) -> dict[str, Any]:
        params = {
            key: value
            for key, value in {
                "package_id": package_id,
                "pipeline_id": pipeline_id,
                "document_type": document_type,
                "limit": limit,
            }.items()
            if value is not None
        }
        response = await self._client.post(
            "/api/process-runtime/project/lifecycle",
            params=params,
            json={
                "before": before,
                "older_than_days": older_than_days,
                "statuses": statuses or [],
                "include_artifact_gc": include_artifact_gc,
                "delete": delete,
            },
        )
        response.raise_for_status()
        return response.json()["lifecycle"]

    async def create_project_run(
        self,
        *,
        run_id: str | None = None,
        title: str | None = None,
        existing_run_policy: ExistingRunPolicy = "error",
        existing_document_policy: ExistingDocumentPolicy = "error",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = await self._client.post(
            "/api/process-runtime/project/runs",
            json={
                "run_id": run_id,
                "title": title,
                "existing_run_policy": existing_run_policy,
                "existing_document_policy": existing_document_policy,
                "metadata": metadata or {},
            },
        )
        response.raise_for_status()
        return response.json()

    async def get_package(self, package_id: str) -> dict[str, Any]:
        response = await self._client.get(
            f"/api/process-runtime/packages/{self._part(package_id)}"
        )
        response.raise_for_status()
        return response.json()

    async def get_package_index(
        self,
        *,
        package_id: str | None = None,
    ) -> dict[str, Any]:
        params = {"package_id": package_id} if package_id is not None else None
        response = await self._client.get(
            "/api/process-runtime/packages/index",
            params=params,
        )
        response.raise_for_status()
        return response.json()["index"]

    async def get_package_release(self, package_id: str) -> dict[str, Any]:
        response = await self._client.get(
            f"/api/process-runtime/packages/{self._part(package_id)}/release"
        )
        response.raise_for_status()
        return response.json()["release"]

    async def get_package_readiness(
        self,
        *,
        package_id: str | None = None,
    ) -> dict[str, Any]:
        if package_id is None:
            response = await self._client.get(
                "/api/process-runtime/packages/readiness"
            )
        else:
            response = await self._client.get(
                f"/api/process-runtime/packages/{self._part(package_id)}/readiness"
            )
        response.raise_for_status()
        return response.json()["readiness"]

    async def list_blueprints(self, query: str | None = None) -> dict[str, Any]:
        params = {"query": query} if query else None
        response = await self._client.get(
            "/api/process-runtime/blueprints",
            params=params,
        )
        response.raise_for_status()
        return response.json()

    async def get_blueprint(self, blueprint_id: str) -> dict[str, Any]:
        response = await self._client.get(
            f"/api/process-runtime/blueprints/{self._part(blueprint_id)}"
        )
        response.raise_for_status()
        return response.json()["blueprint"]

    async def list_pipelines(self) -> dict[str, Any]:
        response = await self._client.get("/api/process-runtime/pipelines")
        response.raise_for_status()
        return response.json()

    async def get_pipeline(self, pipeline_id: str) -> dict[str, Any]:
        response = await self._client.get(
            f"/api/process-runtime/pipelines/{self._part(pipeline_id)}"
        )
        response.raise_for_status()
        return response.json()

    async def get_pipeline_contract(self, pipeline_id: str) -> dict[str, Any]:
        response = await self._client.get(
            f"/api/process-runtime/pipelines/{self._part(pipeline_id)}/contract"
        )
        response.raise_for_status()
        return response.json()["contract"]

    async def get_state(
        self,
        *,
        run_id: str,
        include_events: bool = False,
    ) -> RuntimeState:
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime",
            params={"include_events": include_events},
        )
        response.raise_for_status()
        return RuntimeState.model_validate(response.json())

    async def get_step_report(self, *, run_id: str) -> RuntimeStepReport:
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/report",
        )
        response.raise_for_status()
        return RuntimeStepReport.model_validate(response.json())

    async def get_queue_metrics(self, *, run_id: str) -> RuntimeQueueMetrics:
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/metrics",
        )
        response.raise_for_status()
        return RuntimeQueueMetrics.model_validate(response.json())

    async def get_capability_demands(
        self,
        *,
        run_id: str,
        stale_after_seconds: float = 60.0,
    ) -> RuntimeCapabilityDemandSummary:
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/capability-demands",
            params={"stale_after_seconds": stale_after_seconds},
        )
        response.raise_for_status()
        return RuntimeCapabilityDemandSummary.model_validate(response.json())

    async def get_prometheus_metrics(
        self,
        *,
        run_id: str,
        stale_after_seconds: float = 60.0,
    ) -> str:
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/metrics/prometheus",
            params={"stale_after_seconds": stale_after_seconds},
        )
        response.raise_for_status()
        return response.text

    async def get_run_health(
        self,
        *,
        run_id: str,
        stale_after_seconds: float = 60.0,
    ) -> RuntimeRunHealth:
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/health",
            params={"stale_after_seconds": stale_after_seconds},
        )
        response.raise_for_status()
        return RuntimeRunHealth.model_validate(response.json())

    async def get_trace(
        self,
        *,
        run_id: str,
        document_id: str | None = None,
        process_id: str | None = None,
        operation_type: str | None = None,
    ) -> RuntimeTrace:
        params: dict[str, Any] = {}
        if document_id is not None:
            params["document_id"] = document_id
        if process_id is not None:
            params["process_id"] = process_id
        if operation_type is not None:
            params["operation_type"] = operation_type
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/trace",
            params=params,
        )
        response.raise_for_status()
        return RuntimeTrace.model_validate(response.json())

    async def worker_heartbeat(
        self,
        *,
        run_id: str,
        worker_id: str,
        pipeline_id: str | None = None,
        process_id: str | None = None,
        adapter_kind: AdapterKind | None = None,
        capabilities: list[str] | None = None,
        resources: ResourceSpec | dict[str, Any] | None = None,
        status: RuntimeWorkerStatus | str = RuntimeWorkerStatus.idle,
        current_document_id: str | None = None,
        current_process_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        status_value = status.value if isinstance(status, RuntimeWorkerStatus) else str(status)
        response = await self._client.post(
            f"/api/runs/{self._part(run_id)}/process-runtime/"
            f"workers/{self._part(worker_id)}/heartbeat",
            json={
                "pipeline_id": pipeline_id,
                "process_id": process_id,
                "adapter_kind": adapter_kind,
                "capabilities": capabilities or [],
                "resources": _resource_payload(resources),
                "status": status_value,
                "current_document_id": current_document_id,
                "current_process_id": current_process_id,
                "metadata": metadata or {},
            },
        )
        response.raise_for_status()
        return response.json()["worker"]

    async def worker_health(
        self,
        *,
        run_id: str,
        stale_after_seconds: float = 60.0,
    ) -> list[RuntimeWorkerState]:
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/workers",
            params={"stale_after_seconds": stale_after_seconds},
        )
        response.raise_for_status()
        return [
            RuntimeWorkerState.model_validate(item)
            for item in response.json()["workers"]
        ]

    async def append_stream_chunk(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        stream_id: str = "main",
        sequence: int | None = None,
        kind: str | None = None,
        values: dict[str, Any] | None = None,
        artifacts: list[ArtifactRef | dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
        pipeline_id: str | None = None,
        worker_id: str | None = None,
    ) -> RuntimeStreamChunk:
        params: dict[str, Any] = {}
        if pipeline_id is not None:
            params["pipeline_id"] = pipeline_id
        if worker_id is not None:
            params["worker_id"] = worker_id
        response = await self._client.post(
            f"/api/runs/{self._part(run_id)}/process-runtime/"
            f"{self._part(document_id)}/processes/{self._part(process_id)}/"
            f"streams/{self._part(stream_id)}/chunks",
            params=params,
            json={
                "sequence": sequence,
                "kind": kind,
                "values": values or {},
                "artifacts": [
                    artifact.model_dump(mode="json")
                    if isinstance(artifact, ArtifactRef)
                    else artifact
                    for artifact in artifacts or []
                ],
                "metadata": metadata or {},
            },
        )
        response.raise_for_status()
        return RuntimeStreamChunk.model_validate(response.json()["chunk"])

    async def list_stream_chunks(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        stream_id: str = "main",
        after_sequence: int | None = None,
        limit: int | None = None,
    ) -> list[RuntimeStreamChunk]:
        params: dict[str, Any] = {}
        if after_sequence is not None:
            params["after_sequence"] = after_sequence
        if limit is not None:
            params["limit"] = limit
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/"
            f"{self._part(document_id)}/processes/{self._part(process_id)}/"
            f"streams/{self._part(stream_id)}/chunks",
            params=params,
        )
        response.raise_for_status()
        return [
            RuntimeStreamChunk.model_validate(item)
            for item in response.json()["chunks"]
        ]

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
        response = await self._client.put(
            f"/api/runs/{self._part(run_id)}/process-runtime/"
            f"{self._part(document_id)}/processes/{self._part(process_id)}/"
            f"streams/{self._part(stream_id)}/checkpoints/{self._part(consumer_id)}",
            json={
                "sequence": sequence,
                "chunk_id": chunk_id,
                "metadata": metadata or {},
            },
        )
        response.raise_for_status()
        return RuntimeStreamCheckpoint.model_validate(response.json()["checkpoint"])

    async def get_stream_checkpoint(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        stream_id: str = "main",
        consumer_id: str = "default",
    ) -> RuntimeStreamCheckpoint | None:
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/"
            f"{self._part(document_id)}/processes/{self._part(process_id)}/"
            f"streams/{self._part(stream_id)}/checkpoints/{self._part(consumer_id)}",
        )
        response.raise_for_status()
        payload = response.json()["checkpoint"]
        if payload is None:
            return None
        return RuntimeStreamCheckpoint.model_validate(payload)

    async def read_stream_batch(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        stream_id: str = "main",
        consumer_id: str = "default",
        after_sequence: int | None = None,
        limit: int = 100,
    ) -> RuntimeStreamBatch:
        if limit < 1:
            raise ValueError("limit must be greater than zero")
        checkpoint = await self.get_stream_checkpoint(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            stream_id=stream_id,
            consumer_id=consumer_id,
        )
        cursor = (
            after_sequence
            if after_sequence is not None
            else (checkpoint.sequence if checkpoint is not None else -1)
        )
        chunks = await self.list_stream_chunks(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            stream_id=stream_id,
            after_sequence=cursor,
            limit=limit,
        )
        last = chunks[-1] if chunks else None
        return RuntimeStreamBatch(
            run_id=run_id,
            document_id=document_id,
            process_id=process_id,
            stream_id=stream_id,
            consumer_id=consumer_id,
            checkpoint=checkpoint,
            after_sequence=cursor,
            limit=limit,
            chunk_count=len(chunks),
            chunks=chunks,
            last_sequence=last.sequence if last is not None else None,
            last_chunk_id=last.chunk_id if last is not None else None,
        )

    async def commit_stream_batch(
        self,
        batch: RuntimeStreamBatch,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> RuntimeStreamCheckpoint | None:
        if batch.last_sequence is None:
            return batch.checkpoint
        return await self.put_stream_checkpoint(
            run_id=batch.run_id,
            document_id=batch.document_id,
            process_id=batch.process_id,
            stream_id=batch.stream_id,
            consumer_id=batch.consumer_id,
            sequence=batch.last_sequence,
            chunk_id=batch.last_chunk_id,
            metadata=metadata or {},
        )

    async def document_page(
        self,
        *,
        run_id: str,
        status: str | None = None,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        relation: str | None = None,
        parent_document_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> RuntimeDocumentPage:
        params = {
            key: value
            for key, value in {
                "status": status,
                "pipeline_id": pipeline_id,
                "document_type": document_type,
                "relation": relation,
                "parent_document_id": parent_document_id,
                "limit": limit,
                "offset": offset,
            }.items()
            if value is not None
        }
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/documents",
            params=params,
        )
        response.raise_for_status()
        return RuntimeDocumentPage.model_validate(response.json())

    async def list_documents(
        self,
        *,
        run_id: str,
        status: str | None = None,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        relation: str | None = None,
        parent_document_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        page = await self.document_page(
            run_id=run_id,
            status=status,
            pipeline_id=pipeline_id,
            document_type=document_type,
            relation=relation,
            parent_document_id=parent_document_id,
            limit=limit,
            offset=offset,
        )
        return [document.model_dump(mode="json") for document in page.documents]

    async def process_page(
        self,
        *,
        run_id: str,
        status: str | None = None,
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
        params = {
            key: value
            for key, value in {
                "status": status,
                "pipeline_id": pipeline_id,
                "document_type": document_type,
                "parent_document_id": parent_document_id,
                "document_id": document_id,
                "process_id": process_id,
                "capability": capability,
                "operation_type": operation_type,
                "adapter_kind": adapter_kind,
                "resource_pool": resource_pool,
                "limit": limit,
                "offset": offset,
            }.items()
            if value is not None
        }
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/processes",
            params=params,
        )
        response.raise_for_status()
        return RuntimeProcessPage.model_validate(response.json())

    async def list_processes(
        self,
        *,
        run_id: str,
        status: str | None = None,
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
    ) -> list[dict[str, Any]]:
        page = await self.process_page(
            run_id=run_id,
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
        return [process.model_dump(mode="json") for process in page.processes]

    async def dead_letter_page(
        self,
        *,
        run_id: str,
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
        params = {
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
                "limit": limit,
                "offset": offset,
            }.items()
            if value is not None
        }
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/dead-letter",
            params=params,
        )
        response.raise_for_status()
        return RuntimeDeadLetterPage.model_validate(response.json())

    async def list_dead_letters(
        self,
        *,
        run_id: str,
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
    ) -> list[dict[str, Any]]:
        page = await self.dead_letter_page(
            run_id=run_id,
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
        return [item.model_dump(mode="json") for item in page.items]

    async def replay_dead_letter(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        pipeline_id: str | None = None,
        reason: str | None = None,
        allow_contract_drift: bool = False,
    ) -> ProcessControlResult:
        payload: dict[str, Any] = {"allow_contract_drift": allow_contract_drift}
        if pipeline_id is not None:
            payload["pipeline_id"] = pipeline_id
        if reason is not None:
            payload["reason"] = reason
        response = await self._client.post(
            f"/api/runs/{self._part(run_id)}/process-runtime/dead-letter/"
            f"{self._part(document_id)}/processes/{self._part(process_id)}/replay",
            json=payload,
        )
        response.raise_for_status()
        return ProcessControlResult.model_validate(response.json()["action"])

    async def stuck_work_page(
        self,
        *,
        run_id: str,
        status: str | None = None,
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
        params = {
            key: value
            for key, value in {
                "status": status,
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
                "limit": limit,
                "offset": offset,
            }.items()
            if value is not None
        }
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/stuck-work",
            params=params,
        )
        response.raise_for_status()
        return RuntimeStuckWorkPage.model_validate(response.json())

    async def list_stuck_work(
        self,
        *,
        run_id: str,
        status: str | None = None,
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
    ) -> list[dict[str, Any]]:
        page = await self.stuck_work_page(
            run_id=run_id,
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
        return [item.model_dump(mode="json") for item in page.items]

    async def diagnose_waits(
        self,
        *,
        run_id: str,
        document_id: str,
        pipeline_id: str | None = None,
    ) -> WaitGraphDiagnostic:
        params = {
            key: value
            for key, value in {
                "document_id": document_id,
                "pipeline_id": pipeline_id,
            }.items()
            if value is not None
        }
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/wait-diagnostics",
            params=params,
        )
        response.raise_for_status()
        return WaitGraphDiagnostic.model_validate(response.json())

    async def stream_lag_page(
        self,
        *,
        run_id: str,
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
        params = {
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
                "limit": limit,
                "offset": offset,
            }.items()
            if value is not None
        }
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/stream-lag",
            params=params,
        )
        response.raise_for_status()
        return RuntimeStreamLagPage.model_validate(response.json())

    async def list_stream_lag(
        self,
        *,
        run_id: str,
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
    ) -> list[dict[str, Any]]:
        page = await self.stream_lag_page(
            run_id=run_id,
            pipeline_id=pipeline_id,
            document_type=document_type,
            parent_document_id=parent_document_id,
            document_id=document_id,
            process_id=process_id,
            capability=capability,
            operation_type=operation_type,
            adapter_kind=adapter_kind,
            resource_pool=resource_pool,
            stream_id=stream_id,
            consumer_id=consumer_id,
            min_lag=min_lag,
            over_limit=over_limit,
            limit=limit,
            offset=offset,
        )
        return [item.model_dump(mode="json") for item in page.items]

    async def document_lineage(self, *, run_id: str) -> dict[str, Any]:
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/document-lineage",
        )
        response.raise_for_status()
        return response.json()["lineage"]

    async def run_results(
        self,
        *,
        run_id: str,
        pipeline_id: str | None = None,
        process_id: str | None = None,
        document_id: str | None = None,
        document_type: str | None = None,
        operation_type: str | None = None,
    ) -> dict[str, Any]:
        params = {
            key: value
            for key, value in {
                "pipeline_id": pipeline_id,
                "process_id": process_id,
                "document_id": document_id,
                "document_type": document_type,
                "operation_type": operation_type,
            }.items()
            if value is not None
        }
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/results",
            params=params,
        )
        response.raise_for_status()
        return response.json()["results"]

    async def output_document_page(
        self,
        *,
        run_id: str,
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
        params = {
            key: value
            for key, value in {
                "pipeline_id": pipeline_id,
                "process_id": process_id,
                "document_id": document_id,
                "source_document_type": source_document_type,
                "output_document_id": output_document_id,
                "document_type": document_type,
                "relation": relation,
                "media_type": media_type,
                "limit": limit,
                "offset": offset,
            }.items()
            if value is not None
        }
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/output-documents",
            params=params,
        )
        response.raise_for_status()
        return RuntimeOutputDocumentPage.model_validate(response.json())

    async def output_documents(
        self,
        *,
        run_id: str,
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
    ) -> list[dict[str, Any]]:
        page = await self.output_document_page(
            run_id=run_id,
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
        return [item.model_dump(mode="json") for item in page.output_documents]

    async def run_reductions(
        self,
        *,
        run_id: str,
        pipeline_id: str | None = None,
        reduce_id: str | None = None,
    ) -> dict[str, Any]:
        params = {
            key: value
            for key, value in {
                "pipeline_id": pipeline_id,
                "reduce_id": reduce_id,
            }.items()
            if value is not None
        }
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/reductions",
            params=params,
        )
        response.raise_for_status()
        return response.json()["reductions"]

    async def attach_run(
        self,
        *,
        run_id: str,
        pipeline_id: str,
    ) -> dict[str, Any]:
        response = await self._client.post(
            f"/api/runs/{self._part(run_id)}/process-runtime/attach",
            json={"pipeline_id": pipeline_id},
        )
        response.raise_for_status()
        return response.json()

    async def claim_next(
        self,
        *,
        run_id: str,
        pipeline_id: str,
        worker_id: str | None = None,
        process_id: str | None = None,
        adapter_kind: AdapterKind | None = None,
        capabilities: list[str] | None = None,
        resources: ResourceSpec | dict[str, Any] | None = None,
        lease_seconds: float = 300.0,
    ) -> ClaimedProcess | None:
        response = await self._client.post(
            f"/api/runs/{self._part(run_id)}/process-runtime/claim",
            json={
                "pipeline_id": pipeline_id,
                "worker_id": worker_id,
                "process_id": process_id,
                "adapter_kind": adapter_kind,
                "capabilities": capabilities or [],
                "resources": _resource_payload(resources),
                "lease_seconds": lease_seconds,
            },
        )
        response.raise_for_status()
        payload = response.json().get("process")
        return ClaimedProcess.model_validate(payload) if payload else None

    async def renew_claim(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        pipeline_id: str,
        worker_id: str | None = None,
        lease_seconds: float = 300.0,
    ) -> ProcessClaim | None:
        response = await self._client.post(
            f"/api/runs/{self._part(run_id)}/process-runtime/"
            f"{self._part(document_id)}/processes/{self._part(process_id)}/renew",
            json={
                "pipeline_id": pipeline_id,
                "worker_id": worker_id,
                "lease_seconds": lease_seconds,
            },
        )
        response.raise_for_status()
        payload = response.json().get("claim")
        return ProcessClaim.model_validate(payload) if payload else None

    async def schedule_document(
        self,
        *,
        run_id: str,
        document_id: str,
        pipeline_id: str | None = None,
    ) -> dict[str, Any]:
        response = await self._client.post(
            f"/api/runs/{self._part(run_id)}/process-runtime/"
            f"{self._part(document_id)}/schedule",
            json={"pipeline_id": pipeline_id},
        )
        response.raise_for_status()
        return response.json()["schedule"]

    async def control_process(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        action: ProcessAction | str,
        pipeline_id: str | None = None,
        reason: str | None = None,
        allow_contract_drift: bool = False,
    ) -> ProcessControlResult:
        payload: dict[str, Any] = {
            "action": action.value if isinstance(action, ProcessAction) else str(action),
            "allow_contract_drift": allow_contract_drift,
        }
        if pipeline_id is not None:
            payload["pipeline_id"] = pipeline_id
        if reason is not None:
            payload["reason"] = reason
        response = await self._client.post(
            f"/api/runs/{self._part(run_id)}/process-runtime/"
            f"{self._part(document_id)}/processes/{self._part(process_id)}/actions",
            json=payload,
        )
        response.raise_for_status()
        return ProcessControlResult.model_validate(response.json()["action"])

    async def write_status(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        status: ProcessStatus,
        worker_id: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        response = await self._client.put(
            f"/api/runs/{self._part(run_id)}/process-runtime/"
            f"{self._part(document_id)}/processes/{self._part(process_id)}/status",
            json={
                "status": status.value,
                "worker_id": worker_id,
                "data": data or {},
            },
        )
        response.raise_for_status()
        return response.json()

    async def write_output(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        output: ProcessOutput | dict[str, Any],
        pipeline_id: str | None = None,
        worker_id: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if pipeline_id:
            params["pipeline_id"] = pipeline_id
        if worker_id:
            params["worker_id"] = worker_id
        normalized = (
            output
            if isinstance(output, ProcessOutput)
            else ProcessOutput.model_validate(output)
        )
        response = await self._client.put(
            f"/api/runs/{self._part(run_id)}/process-runtime/"
            f"{self._part(document_id)}/processes/{self._part(process_id)}/output",
            params=params,
            json=normalized.model_dump(mode="json"),
        )
        response.raise_for_status()
        return response.json()

    async def append_event(
        self,
        *,
        run_id: str,
        document_id: str,
        event: ProcessEvent | dict[str, Any],
    ) -> ProcessEvent:
        payload = event.model_dump(mode="json") if isinstance(event, ProcessEvent) else event
        response = await self._client.post(
            f"/api/runs/{self._part(run_id)}/process-runtime/{self._part(document_id)}/events",
            json=payload,
        )
        response.raise_for_status()
        return ProcessEvent.model_validate(response.json()["event"])

    async def list_events(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str | None = None,
        operation_type: str | None = None,
        after_event_id: str | None = None,
        limit: int = 100,
    ) -> ProcessEventPage:
        params: dict[str, Any] = {"limit": limit}
        if process_id is not None:
            params["process_id"] = process_id
        if operation_type is not None:
            params["operation_type"] = operation_type
        if after_event_id is not None:
            params["after_event_id"] = after_event_id
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/{self._part(document_id)}/events",
            params=params,
        )
        response.raise_for_status()
        return ProcessEventPage.model_validate(response.json())

    async def stream_events(
        self,
        *,
        run_id: str,
        document_id: str | None = None,
        process_id: str | None = None,
        operation_type: str | None = None,
        after_event_id: str | None = None,
        batch_limit: int = 100,
        poll_interval_seconds: float = 1.0,
        heartbeat_interval_seconds: float = 15.0,
        max_events: int | None = None,
    ) -> AsyncIterator[ProcessEvent]:
        params: dict[str, Any] = {
            "batch_limit": batch_limit,
            "poll_interval_seconds": poll_interval_seconds,
            "heartbeat_interval_seconds": heartbeat_interval_seconds,
        }
        if process_id is not None:
            params["process_id"] = process_id
        if operation_type is not None:
            params["operation_type"] = operation_type
        if after_event_id is not None:
            params["after_event_id"] = after_event_id
        if max_events is not None:
            params["max_events"] = max_events

        if document_id is None:
            path = f"/api/runs/{self._part(run_id)}/process-runtime/events/stream"
        else:
            path = (
                f"/api/runs/{self._part(run_id)}/process-runtime/"
                f"{self._part(document_id)}/events/stream"
            )

        async with self._client.stream("GET", path, params=params) as response:
            response.raise_for_status()
            async for event in _aiter_sse_process_events(response):
                yield event

    def _part(self, value: str) -> str:
        return quote(str(value), safe="")


def _resource_payload(resources: ResourceSpec | dict[str, Any] | None) -> dict[str, Any]:
    if resources is None:
        return {}
    if isinstance(resources, ResourceSpec):
        return resources.model_dump(mode="json")
    return resources


async def _aiter_sse_process_events(
    response: httpx.Response,
) -> AsyncIterator[ProcessEvent]:
    event_name: str | None = None
    data_lines: list[str] = []
    async for line in response.aiter_lines():
        if line == "":
            if event_name == "process" and data_lines:
                yield ProcessEvent.model_validate_json("\n".join(data_lines))
            event_name = None
            data_lines = []
            continue
        if line.startswith(":"):
            continue
        field, separator, value = line.partition(":")
        if not separator:
            continue
        value = value[1:] if value.startswith(" ") else value
        if field == "event":
            event_name = value
        elif field == "data":
            data_lines.append(value)
