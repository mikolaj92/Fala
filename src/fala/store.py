from __future__ import annotations

import asyncio
from typing import Any, Protocol

from fala.models import (
    CombinedProjection,
    OperatorAuditEvent,
    ProcessClaim,
    ProcessEvent,
    ProcessInput,
    ProcessOutput,
    ProcessStatus,
    RuntimeDocument,
    RuntimeDocumentStatus,
    RuntimeRun,
    RuntimeStreamCheckpoint,
    RuntimeStreamChunk,
    RuntimeWorkerHeartbeat,
)


class StateStore(Protocol):
    async def append_audit_event(self, event: OperatorAuditEvent) -> None:
        ...

    async def list_audit_events(
        self,
        *,
        run_id: str | None = None,
        limit: int | None = None,
        descending: bool = False,
    ) -> list[OperatorAuditEvent]:
        ...

    async def append_event(self, event: ProcessEvent) -> None:
        ...

    async def list_events(
        self,
        *,
        run_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
        after_event_id: str | None = None,
        limit: int | None = None,
        descending: bool = False,
    ) -> list[ProcessEvent]:
        ...

    async def count_events(
        self,
        *,
        run_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
    ) -> int:
        ...

    async def list_runs(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        ...

    async def put_run(self, run: RuntimeRun) -> None:
        ...

    async def get_run(self, run_id: str) -> RuntimeRun | None:
        ...

    async def delete_run(self, run_id: str) -> dict[str, int]:
        ...

    async def put_worker_heartbeat(self, heartbeat: RuntimeWorkerHeartbeat) -> None:
        ...

    async def list_worker_heartbeats(
        self, *, run_id: str | None = None
    ) -> list[RuntimeWorkerHeartbeat]:
        ...

    async def put_stream_chunk(self, chunk: RuntimeStreamChunk) -> None:
        ...

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
        ...

    async def put_stream_checkpoint(
        self, checkpoint: RuntimeStreamCheckpoint
    ) -> None:
        ...

    async def get_stream_checkpoint(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        stream_id: str,
        consumer_id: str,
    ) -> RuntimeStreamCheckpoint | None:
        ...

    async def list_stream_checkpoints(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str | None = None,
        stream_id: str | None = None,
        consumer_id: str | None = None,
    ) -> list[RuntimeStreamCheckpoint]:
        ...

    async def set_status(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        status: ProcessStatus,
        pipeline_id: str | None = None,
        capability: str | None = None,
        adapter_kind: str | None = None,
        resource_pool: str | None = None,
    ) -> None:
        ...

    async def clear_status(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> None:
        ...

    async def put_document_input(
        self,
        *,
        run_id: str,
        document_id: str,
        input: ProcessInput,
        pipeline_id: str | None = None,
    ) -> None:
        ...

    async def get_document_input(
        self, *, run_id: str, document_id: str
    ) -> ProcessInput | None:
        ...

    async def get_document_pipeline_id(
        self, *, run_id: str, document_id: str
    ) -> str | None:
        ...

    async def put_document(self, document: RuntimeDocument) -> None:
        ...

    async def get_document(
        self, *, run_id: str, document_id: str
    ) -> RuntimeDocument | None:
        ...

    async def list_document_records(
        self,
        *,
        run_id: str,
        status: RuntimeDocumentStatus | None = None,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        relation: str | None = None,
        parent_document_id: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[RuntimeDocument]:
        ...

    async def list_process_record_keys(
        self,
        *,
        run_id: str,
        status: ProcessStatus | None = None,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        parent_document_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
        capability: str | None = None,
        adapter_kind: str | None = None,
        resource_pool: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        ...

    async def put_claim(self, claim: ProcessClaim) -> None:
        ...

    async def try_claim_process(self, claim: ProcessClaim) -> bool:
        ...

    async def get_claim(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> ProcessClaim | None:
        ...

    async def clear_claim(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> None:
        ...

    async def put_output(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        output: ProcessOutput,
    ) -> None:
        ...

    async def clear_output(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> None:
        ...

    async def get_output(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> ProcessOutput | None:
        ...

    async def get_outputs(
        self, *, run_id: str, document_id: str, process_ids: list[str]
    ) -> dict[str, ProcessOutput]:
        ...

    async def put_projection(self, projection: CombinedProjection) -> None:
        ...

    async def clear_projections(self, *, run_id: str, document_id: str) -> None:
        ...

    async def get_projection(
        self, *, run_id: str, document_id: str, projection_id: str
    ) -> CombinedProjection | None:
        ...

    async def list_documents(self, *, run_id: str) -> list[str]:
        ...

    async def list_statuses(
        self, *, run_id: str, document_id: str
    ) -> dict[str, ProcessStatus]:
        ...

    async def list_outputs(
        self, *, run_id: str, document_id: str
    ) -> dict[str, ProcessOutput]:
        ...

    async def list_claims(
        self, *, run_id: str, document_id: str
    ) -> dict[str, ProcessClaim]:
        ...

    async def list_projections(
        self, *, run_id: str, document_id: str
    ) -> dict[str, CombinedProjection]:
        ...


class InMemoryStateStore:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._audit_events: list[OperatorAuditEvent] = []
        self._events: list[ProcessEvent] = []
        self._statuses: dict[tuple[str, str, str], ProcessStatus] = {}
        self._process_metadata: dict[tuple[str, str, str], dict[str, str | None]] = {}
        self._document_inputs: dict[tuple[str, str], ProcessInput] = {}
        self._document_pipeline_ids: dict[tuple[str, str], str] = {}
        self._documents: dict[tuple[str, str], RuntimeDocument] = {}
        self._claims: dict[tuple[str, str, str], ProcessClaim] = {}
        self._outputs: dict[tuple[str, str, str], ProcessOutput] = {}
        self._projections: dict[tuple[str, str, str], CombinedProjection] = {}
        self._runs: dict[str, RuntimeRun] = {}
        self._worker_heartbeats: dict[tuple[str, str], RuntimeWorkerHeartbeat] = {}
        self._stream_chunks: dict[tuple[str, str, str, str, int], RuntimeStreamChunk] = {}
        self._stream_checkpoints: dict[
            tuple[str, str, str, str, str], RuntimeStreamCheckpoint
        ] = {}

    async def append_audit_event(self, event: OperatorAuditEvent) -> None:
        async with self._lock:
            self._audit_events.append(event)

    async def list_audit_events(
        self,
        *,
        run_id: str | None = None,
        limit: int | None = None,
        descending: bool = False,
    ) -> list[OperatorAuditEvent]:
        async with self._lock:
            events = [
                event
                for event in self._audit_events
                if run_id is None or event.run_id == run_id
            ]
            events.sort(key=lambda item: (item.ts, item.id), reverse=descending)
            if limit is not None:
                events = events[:limit]
            return events

    async def append_event(self, event: ProcessEvent) -> None:
        async with self._lock:
            self._events.append(event)

    async def list_events(
        self,
        *,
        run_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
        after_event_id: str | None = None,
        limit: int | None = None,
        descending: bool = False,
    ) -> list[ProcessEvent]:
        async with self._lock:
            events = [
                event
                for event in self._events
                if (run_id is None or event.run_id == run_id)
                and (document_id is None or event.document_id == document_id)
                and (process_id is None or event.process_id == process_id)
            ]
            if after_event_id is not None:
                for index, event in enumerate(events):
                    if event.id == after_event_id:
                        events = events[index + 1 :]
                        break
                else:
                    raise ValueError("after_event_id not found")
            if descending:
                events = list(reversed(events))
            if limit is not None:
                events = events[:limit]
            return events

    async def count_events(
        self,
        *,
        run_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
    ) -> int:
        async with self._lock:
            return sum(
                1
                for event in self._events
                if (run_id is None or event.run_id == run_id)
                and (document_id is None or event.document_id == document_id)
                and (process_id is None or event.process_id == process_id)
            )

    async def list_runs(self, *, limit: int | None = None) -> list[dict[str, Any]]:
        async with self._lock:
            run_ids = {event.run_id for event in self._events}
            run_ids.update(run_id for run_id, _document_id, _process_id in self._statuses)
            run_ids.update(run_id for run_id, _document_id in self._document_inputs)
            run_ids.update(run_id for run_id, _document_id in self._documents)
            run_ids.update(run_id for run_id, _document_id, _process_id in self._claims)
            run_ids.update(run_id for run_id, _document_id, _process_id in self._outputs)
            run_ids.update(run_id for run_id, _document_id, _projection_id in self._projections)
            run_ids.update(
                run_id
                for run_id, _document_id, _process_id, _stream_id, _sequence
                in self._stream_chunks
            )
            run_ids.update(
                run_id
                for run_id, _document_id, _process_id, _stream_id, _consumer_id
                in self._stream_checkpoints
            )
            run_ids.update(self._runs)

            rows: list[dict[str, Any]] = []
            for run_id in sorted(run_ids):
                run = self._runs.get(run_id)
                event_times = [
                    event.ts
                    for event in self._events
                    if event.run_id == run_id
                ]
                created_at = min(event_times).isoformat() if event_times else None
                updated_at = max(event_times).isoformat() if event_times else None
                if run is not None:
                    created_at = run.created_at.isoformat()
                    updated_at = run.updated_at.isoformat()
                rows.append(
                    {
                        "run_id": run_id,
                        "created_at": created_at,
                        "updated_at": updated_at,
                        "run": run.model_dump(mode="json") if run is not None else None,
                    }
                )
            if limit is not None:
                rows = rows[:limit]
            return rows

    async def put_run(self, run: RuntimeRun) -> None:
        async with self._lock:
            self._runs[run.id] = run

    async def get_run(self, run_id: str) -> RuntimeRun | None:
        async with self._lock:
            return self._runs.get(run_id)

    async def delete_run(self, run_id: str) -> dict[str, int]:
        async with self._lock:
            counts: dict[str, int] = {}

            counts["process_runs"] = 1 if self._runs.pop(run_id, None) is not None else 0

            events = [event for event in self._events if event.run_id == run_id]
            counts["process_events"] = len(events)
            self._events = [event for event in self._events if event.run_id != run_id]

            for table, mapping in [
                ("process_statuses", self._statuses),
                ("process_metadata", self._process_metadata),
                ("process_document_inputs", self._document_inputs),
                ("process_documents", self._documents),
                ("process_claims", self._claims),
                ("process_outputs", self._outputs),
                ("process_projections", self._projections),
                ("process_worker_heartbeats", self._worker_heartbeats),
                ("process_stream_chunks", self._stream_chunks),
                ("process_stream_checkpoints", self._stream_checkpoints),
            ]:
                keys = [key for key in mapping if key[0] == run_id]
                counts[table] = len(keys)
                for key in keys:
                    mapping.pop(key, None)
            return counts

    async def put_worker_heartbeat(self, heartbeat: RuntimeWorkerHeartbeat) -> None:
        async with self._lock:
            self._worker_heartbeats[(heartbeat.run_id, heartbeat.worker_id)] = heartbeat

    async def list_worker_heartbeats(
        self, *, run_id: str | None = None
    ) -> list[RuntimeWorkerHeartbeat]:
        async with self._lock:
            return [
                heartbeat
                for (stored_run_id, _worker_id), heartbeat in sorted(
                    self._worker_heartbeats.items()
                )
                if run_id is None or stored_run_id == run_id
            ]

    async def put_stream_chunk(self, chunk: RuntimeStreamChunk) -> None:
        async with self._lock:
            self._stream_chunks[
                (
                    chunk.run_id,
                    chunk.document_id,
                    chunk.process_id,
                    chunk.stream_id,
                    chunk.sequence,
                )
            ] = chunk

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
        async with self._lock:
            chunks = [
                chunk
                for (
                    stored_run_id,
                    stored_document_id,
                    stored_process_id,
                    stored_stream_id,
                    sequence,
                ), chunk in sorted(self._stream_chunks.items())
                if stored_run_id == run_id
                and stored_document_id == document_id
                and (process_id is None or stored_process_id == process_id)
                and (stream_id is None or stored_stream_id == stream_id)
                and (after_sequence is None or sequence > after_sequence)
            ]
            if limit is not None:
                chunks = chunks[:limit]
            return chunks

    async def put_stream_checkpoint(
        self, checkpoint: RuntimeStreamCheckpoint
    ) -> None:
        async with self._lock:
            self._stream_checkpoints[
                (
                    checkpoint.run_id,
                    checkpoint.document_id,
                    checkpoint.process_id,
                    checkpoint.stream_id,
                    checkpoint.consumer_id,
                )
            ] = checkpoint

    async def get_stream_checkpoint(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        stream_id: str,
        consumer_id: str,
    ) -> RuntimeStreamCheckpoint | None:
        async with self._lock:
            return self._stream_checkpoints.get(
                (run_id, document_id, process_id, stream_id, consumer_id)
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
        async with self._lock:
            return [
                checkpoint
                for (
                    stored_run_id,
                    stored_document_id,
                    stored_process_id,
                    stored_stream_id,
                    stored_consumer_id,
                ), checkpoint in sorted(self._stream_checkpoints.items())
                if stored_run_id == run_id
                and stored_document_id == document_id
                and (process_id is None or stored_process_id == process_id)
                and (stream_id is None or stored_stream_id == stream_id)
                and (consumer_id is None or stored_consumer_id == consumer_id)
            ]

    async def set_status(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        status: ProcessStatus,
        pipeline_id: str | None = None,
        capability: str | None = None,
        adapter_kind: str | None = None,
        resource_pool: str | None = None,
    ) -> None:
        async with self._lock:
            key = (run_id, document_id, process_id)
            self._statuses[key] = status
            metadata = self._process_metadata.get(key, {})
            for name, value in {
                "pipeline_id": pipeline_id,
                "capability": capability,
                "adapter_kind": adapter_kind,
                "resource_pool": resource_pool,
            }.items():
                if value is not None:
                    metadata[name] = value
            if metadata:
                self._process_metadata[key] = metadata

    async def clear_status(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> None:
        async with self._lock:
            key = (run_id, document_id, process_id)
            self._statuses.pop(key, None)
            self._process_metadata.pop(key, None)

    async def put_document_input(
        self,
        *,
        run_id: str,
        document_id: str,
        input: ProcessInput,
        pipeline_id: str | None = None,
    ) -> None:
        async with self._lock:
            self._document_inputs[(run_id, document_id)] = input
            if pipeline_id is not None:
                self._document_pipeline_ids[(run_id, document_id)] = pipeline_id

    async def get_document_input(
        self, *, run_id: str, document_id: str
    ) -> ProcessInput | None:
        async with self._lock:
            return self._document_inputs.get((run_id, document_id))

    async def get_document_pipeline_id(
        self, *, run_id: str, document_id: str
    ) -> str | None:
        async with self._lock:
            document = self._documents.get((run_id, document_id))
            if document is not None and document.pipeline_id is not None:
                return document.pipeline_id
            return self._document_pipeline_ids.get((run_id, document_id))

    async def put_document(self, document: RuntimeDocument) -> None:
        async with self._lock:
            self._documents[(document.run_id, document.document_id)] = document
            if document.pipeline_id is not None:
                self._document_pipeline_ids[(document.run_id, document.document_id)] = document.pipeline_id

    async def get_document(
        self, *, run_id: str, document_id: str
    ) -> RuntimeDocument | None:
        async with self._lock:
            return self._documents.get((run_id, document_id))

    async def list_document_records(
        self,
        *,
        run_id: str,
        status: RuntimeDocumentStatus | None = None,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        relation: str | None = None,
        parent_document_id: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[RuntimeDocument]:
        async with self._lock:
            documents = [
                document
                for (stored_run_id, _document_id), document in sorted(self._documents.items())
                if stored_run_id == run_id
                and (status is None or document.status == status)
                and (pipeline_id is None or document.pipeline_id == pipeline_id)
                and (document_type is None or document.document_type == document_type)
                and (relation is None or document.relation == relation)
                and (
                    parent_document_id is None
                    or document.parent_document_id == parent_document_id
                )
            ]
            if offset:
                documents = documents[offset:]
            if limit is not None:
                documents = documents[:limit]
            return documents

    async def list_process_record_keys(
        self,
        *,
        run_id: str,
        status: ProcessStatus | None = None,
        pipeline_id: str | None = None,
        document_type: str | None = None,
        parent_document_id: str | None = None,
        document_id: str | None = None,
        process_id: str | None = None,
        capability: str | None = None,
        adapter_kind: str | None = None,
        resource_pool: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        async with self._lock:
            keys = set(self._statuses)
            keys.update(self._claims)
            keys.update(self._outputs)
            keys.update(
                (stored_run_id, stored_document_id, stored_process_id)
                for (
                    stored_run_id,
                    stored_document_id,
                    stored_process_id,
                    _stream_id,
                    _sequence,
                ) in self._stream_chunks
            )
            keys.update(
                (stored_run_id, stored_document_id, stored_process_id)
                for (
                    stored_run_id,
                    stored_document_id,
                    stored_process_id,
                    _stream_id,
                    _consumer_id,
                ) in self._stream_checkpoints
            )

            rows: list[dict[str, Any]] = []
            for stored_run_id, stored_document_id, stored_process_id in sorted(keys):
                if stored_run_id != run_id:
                    continue
                if document_id is not None and stored_document_id != document_id:
                    continue
                if process_id is not None and stored_process_id != process_id:
                    continue
                key = (stored_run_id, stored_document_id, stored_process_id)
                current_status: ProcessStatus | str = self._statuses.get(key) or (
                    ProcessStatus.completed
                    if key in self._outputs
                    else (
                        ProcessStatus.running
                        if key in self._claims
                        else "unknown"
                    )
                )
                if status is not None and current_status != status:
                    continue
                document = self._documents.get((stored_run_id, stored_document_id))
                metadata = self._process_metadata.get(key, {})
                stored_pipeline_id = (
                    metadata.get("pipeline_id")
                    or (
                        document.pipeline_id
                        if document is not None and document.pipeline_id is not None
                        else None
                    )
                    or self._document_pipeline_ids.get(
                        (stored_run_id, stored_document_id)
                    )
                )
                stored_capability = metadata.get("capability")
                stored_adapter_kind = metadata.get("adapter_kind")
                stored_resource_pool = metadata.get("resource_pool")
                if pipeline_id is not None and stored_pipeline_id != pipeline_id:
                    continue
                if capability is not None and stored_capability != capability:
                    continue
                if adapter_kind is not None and stored_adapter_kind != adapter_kind:
                    continue
                if resource_pool is not None and stored_resource_pool != resource_pool:
                    continue
                if (
                    document_type is not None
                    and (document is None or document.document_type != document_type)
                ):
                    continue
                if (
                    parent_document_id is not None
                    and (
                        document is None
                        or document.parent_document_id != parent_document_id
                    )
                ):
                    continue
                rows.append(
                    {
                        "run_id": stored_run_id,
                        "document_id": stored_document_id,
                        "process_id": stored_process_id,
                        "status": (
                            current_status.value
                            if isinstance(current_status, ProcessStatus)
                            else current_status
                        ),
                        "pipeline_id": stored_pipeline_id,
                        "capability": stored_capability,
                        "adapter_kind": stored_adapter_kind,
                        "resource_pool": stored_resource_pool,
                        "status_updated_at": None,
                    }
                )
            if offset:
                rows = rows[offset:]
            if limit is not None:
                rows = rows[:limit]
            return rows

    async def put_claim(self, claim: ProcessClaim) -> None:
        async with self._lock:
            self._claims[(claim.run_id, claim.document_id, claim.process_id)] = claim

    async def try_claim_process(self, claim: ProcessClaim) -> bool:
        async with self._lock:
            key = (claim.run_id, claim.document_id, claim.process_id)
            if key in self._outputs:
                return False
            if self._statuses.get(key) != ProcessStatus.queued:
                return False
            self._claims[key] = claim
            self._statuses[key] = ProcessStatus.running
            return True

    async def get_claim(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> ProcessClaim | None:
        async with self._lock:
            return self._claims.get((run_id, document_id, process_id))

    async def clear_claim(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> None:
        async with self._lock:
            self._claims.pop((run_id, document_id, process_id), None)

    async def put_output(
        self,
        *,
        run_id: str,
        document_id: str,
        process_id: str,
        output: ProcessOutput,
    ) -> None:
        async with self._lock:
            self._outputs[(run_id, document_id, process_id)] = output

    async def clear_output(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> None:
        async with self._lock:
            self._outputs.pop((run_id, document_id, process_id), None)

    async def get_output(
        self, *, run_id: str, document_id: str, process_id: str
    ) -> ProcessOutput | None:
        async with self._lock:
            return self._outputs.get((run_id, document_id, process_id))

    async def get_outputs(
        self, *, run_id: str, document_id: str, process_ids: list[str]
    ) -> dict[str, ProcessOutput]:
        async with self._lock:
            outputs: dict[str, ProcessOutput] = {}
            for process_id in process_ids:
                output = self._outputs.get((run_id, document_id, process_id))
                if output is not None:
                    outputs[process_id] = output
            return outputs

    async def put_projection(self, projection: CombinedProjection) -> None:
        async with self._lock:
            self._projections[(projection.run_id, projection.document_id, projection.id)] = projection

    async def clear_projections(self, *, run_id: str, document_id: str) -> None:
        async with self._lock:
            keys = [
                key
                for key in self._projections
                if key[0] == run_id and key[1] == document_id
            ]
            for key in keys:
                self._projections.pop(key, None)

    async def get_projection(
        self, *, run_id: str, document_id: str, projection_id: str
    ) -> CombinedProjection | None:
        async with self._lock:
            return self._projections.get((run_id, document_id, projection_id))

    async def list_documents(self, *, run_id: str) -> list[str]:
        async with self._lock:
            document_ids = {
                event.document_id
                for event in self._events
                if event.run_id == run_id
            }
            document_ids.update(
                document_id
                for stored_run_id, document_id, _process_id in self._statuses
                if stored_run_id == run_id
            )
            document_ids.update(
                document_id
                for stored_run_id, document_id in self._document_inputs
                if stored_run_id == run_id
            )
            document_ids.update(
                document_id
                for stored_run_id, document_id in self._documents
                if stored_run_id == run_id
            )
            document_ids.update(
                document_id
                for stored_run_id, document_id, _process_id in self._claims
                if stored_run_id == run_id
            )
            document_ids.update(
                document_id
                for stored_run_id, document_id, _process_id in self._outputs
                if stored_run_id == run_id
            )
            document_ids.update(
                document_id
                for stored_run_id, document_id, _projection_id in self._projections
                if stored_run_id == run_id
            )
            return sorted(document_ids)

    async def list_statuses(
        self, *, run_id: str, document_id: str
    ) -> dict[str, ProcessStatus]:
        async with self._lock:
            return {
                process_id: status
                for (stored_run_id, stored_document_id, process_id), status in self._statuses.items()
                if stored_run_id == run_id and stored_document_id == document_id
            }

    async def list_outputs(
        self, *, run_id: str, document_id: str
    ) -> dict[str, ProcessOutput]:
        async with self._lock:
            return {
                process_id: output
                for (stored_run_id, stored_document_id, process_id), output in self._outputs.items()
                if stored_run_id == run_id and stored_document_id == document_id
            }

    async def list_claims(
        self, *, run_id: str, document_id: str
    ) -> dict[str, ProcessClaim]:
        async with self._lock:
            return {
                process_id: claim
                for (stored_run_id, stored_document_id, process_id), claim in self._claims.items()
                if stored_run_id == run_id and stored_document_id == document_id
            }

    async def list_projections(
        self, *, run_id: str, document_id: str
    ) -> dict[str, CombinedProjection]:
        async with self._lock:
            return {
                projection_id: projection
                for (
                    stored_run_id,
                    stored_document_id,
                    projection_id,
                ), projection in self._projections.items()
                if stored_run_id == run_id and stored_document_id == document_id
            }
