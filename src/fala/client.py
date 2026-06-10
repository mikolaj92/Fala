from __future__ import annotations

from typing import Any
from urllib.parse import quote

import httpx

from fala.models import (
    AdapterKind,
    ArtifactRef,
    ProcessAction,
    ProcessClaim,
    ProcessEvent,
    ProcessEventPage,
    ProcessOutput,
    ProcessStatus,
    RuntimeState,
)
from fala.scheduler import ClaimedProcess, ProcessControlResult


class ProcessRuntimeClient:
    """Async client for external process workers."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 30.0,
        client: httpx.AsyncClient | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout,
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

    async def list_packages(self) -> dict[str, Any]:
        response = await self._client.get("/api/process-runtime/packages")
        response.raise_for_status()
        return response.json()

    async def get_package(self, package_id: str) -> dict[str, Any]:
        response = await self._client.get(
            f"/api/process-runtime/packages/{self._part(package_id)}"
        )
        response.raise_for_status()
        return response.json()

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
        lease_seconds: float = 300.0,
    ) -> ClaimedProcess | None:
        response = await self._client.post(
            f"/api/runs/{self._part(run_id)}/process-runtime/claim",
            json={
                "pipeline_id": pipeline_id,
                "worker_id": worker_id,
                "process_id": process_id,
                "adapter_kind": adapter_kind,
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
    ) -> ProcessControlResult:
        payload: dict[str, Any] = {
            "action": action.value if isinstance(action, ProcessAction) else str(action)
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
    ) -> None:
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
        after_event_id: str | None = None,
        limit: int = 100,
    ) -> ProcessEventPage:
        params: dict[str, Any] = {"limit": limit}
        if process_id is not None:
            params["process_id"] = process_id
        if after_event_id is not None:
            params["after_event_id"] = after_event_id
        response = await self._client.get(
            f"/api/runs/{self._part(run_id)}/process-runtime/{self._part(document_id)}/events",
            params=params,
        )
        response.raise_for_status()
        return ProcessEventPage.model_validate(response.json())

    def _part(self, value: str) -> str:
        return quote(str(value), safe="")
