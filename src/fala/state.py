from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from fala.models import (
    CombinedProjection,
    PipelineSpec,
    ProcessClaim,
    ProcessEvent,
    ProcessOutput,
    ProcessSlaSpec,
    ProcessSpec,
    ProcessStatus,
    ResourceSpec,
    RuntimeDocument,
    RuntimeDocumentStepReport,
    RuntimeStepReport,
    RuntimeStepReportItem,
    RuntimeStepReportSummary,
    RuntimeDocumentState,
    RuntimeState,
    RuntimeStateSummary,
    RuntimeStreamCheckpoint,
    RuntimeStreamChunk,
    RuntimeStreamSnapshot,
    RuntimeStepSnapshot,
)


def build_runtime_state(
    *,
    run_id: str,
    documents: Sequence[RuntimeDocumentState],
) -> RuntimeState:
    documents_list = _attach_document_children(list(documents))
    return RuntimeState(
        run_id=run_id,
        summary=runtime_state_summary(documents_list),
        documents=documents_list,
    )


def build_runtime_step_report(state: RuntimeState) -> RuntimeStepReport:
    documents: list[RuntimeDocumentStepReport] = []
    steps: list[RuntimeStepReportItem] = []

    for document in state.documents:
        document_steps: list[RuntimeStepReportItem] = []
        output_process_ids = set(document.outputs)
        for position, step in enumerate(document.steps):
            status = _status_value(step.status)
            blocked_by = [
                process_id
                for process_id in step.needs
                if process_id not in output_process_ids
            ]
            claim = step.claim
            item = RuntimeStepReportItem(
                run_id=state.run_id,
                document_id=document.document_id,
                document_title=document.document.title if document.document else None,
                document_type=(
                    document.document.document_type if document.document else None
                ),
                document_relation=(
                    document.document.relation
                    if document.document
                    else document.relation
                ),
                parent_document_id=document.parent_document_id,
                pipeline_id=document.pipeline_id,
                process_id=step.id,
                position=position,
                title=step.title,
                capability=step.capability,
                operation_type=step.operation_type,
                adapter_kind=step.adapter_kind,
                priority=step.priority,
                resource_pool=step.resource_pool,
                status=step.status,
                status_category=_status_category(status),
                needs=list(step.needs),
                blocked_by=blocked_by,
                is_blocked=bool(blocked_by) and not _is_terminal_status(status),
                is_active=status == ProcessStatus.running.value or step.has_claim,
                is_terminal=_is_terminal_status(status),
                has_claim=step.has_claim,
                worker_id=claim.worker_id if claim is not None else None,
                attempt=claim.attempt if claim is not None else None,
                claim_expires_at=claim.expires_at if claim is not None else None,
                has_output=step.has_output,
                output_value_keys=list(step.output_value_keys),
                artifact_count=step.artifact_count,
                output_document_count=step.output_document_count,
                metadata_keys=list(step.metadata_keys),
                stream_count=step.stream_count,
                stream_chunk_count=step.stream_chunk_count,
                stream_artifact_count=step.stream_artifact_count,
                stream_checkpoint_count=step.stream_checkpoint_count,
            )
            document_steps.append(item)
            steps.append(item)

        document_counts = _step_report_counts(document_steps)
        documents.append(
            RuntimeDocumentStepReport(
                run_id=state.run_id,
                document_id=document.document_id,
                document_title=document.document.title if document.document else None,
                document_type=(
                    document.document.document_type if document.document else None
                ),
                document_relation=(
                    document.document.relation
                    if document.document
                    else document.relation
                ),
                parent_document_id=document.parent_document_id,
                parent_process_id=document.parent_process_id,
                child_document_ids=list(document.child_document_ids),
                child_document_count=document.child_document_count,
                pipeline_id=document.pipeline_id,
                process_count=len(document_steps),
                terminal_process_count=document_counts["terminal_process_count"],
                active_process_count=document_counts["active_process_count"],
                blocked_process_count=document_counts["blocked_process_count"],
                completed_process_count=document_counts["completed_process_count"],
                failed_process_count=document_counts["failed_process_count"],
                skipped_process_count=document_counts["skipped_process_count"],
                cancelled_process_count=document_counts["cancelled_process_count"],
                status_counts=document_counts["status_counts"],
                progress=document_counts["progress"],
                steps=document_steps,
            )
        )

    counts = _step_report_counts(steps)
    pipeline_counts: dict[str, int] = {}
    operation_type_counts: dict[str, int] = {}
    for document in documents:
        pipeline_counts[document.pipeline_id or "unknown"] = (
            pipeline_counts.get(document.pipeline_id or "unknown", 0) + 1
        )
    for step in steps:
        if step.operation_type is not None:
            operation_type_counts[step.operation_type] = (
                operation_type_counts.get(step.operation_type, 0) + 1
            )

    return RuntimeStepReport(
        run_id=state.run_id,
        summary=RuntimeStepReportSummary(
            document_count=len(documents),
            process_count=len(steps),
            terminal_process_count=counts["terminal_process_count"],
            active_process_count=counts["active_process_count"],
            blocked_process_count=counts["blocked_process_count"],
            completed_process_count=counts["completed_process_count"],
            failed_process_count=counts["failed_process_count"],
            skipped_process_count=counts["skipped_process_count"],
            cancelled_process_count=counts["cancelled_process_count"],
            status_counts=counts["status_counts"],
            pipeline_counts=pipeline_counts,
            operation_type_counts=operation_type_counts,
            claim_count=sum(1 for step in steps if step.has_claim),
            output_count=sum(1 for step in steps if step.has_output),
            artifact_count=sum(step.artifact_count for step in steps),
            output_document_count=sum(step.output_document_count for step in steps),
            stream_chunk_count=sum(step.stream_chunk_count for step in steps),
            progress=counts["progress"],
        ),
        documents=documents,
        steps=steps,
    )


def runtime_state_summary(
    documents: Sequence[RuntimeDocumentState],
) -> RuntimeStateSummary:
    status_counts: dict[str, int] = {}
    pipeline_counts: dict[str, int] = {}
    operation_type_counts: dict[str, int] = {}
    process_count = 0
    artifact_count = 0
    output_document_count = 0
    stream_count = 0
    stream_chunk_count = 0
    stream_artifact_count = 0
    stream_checkpoint_count = 0

    for document in documents:
        pipeline_id = document.pipeline_id or "unknown"
        pipeline_counts[pipeline_id] = pipeline_counts.get(pipeline_id, 0) + 1
        process_count += len(document.steps)
        for step in document.steps:
            status = step.status.value if isinstance(step.status, ProcessStatus) else step.status
            status_counts[status] = status_counts.get(status, 0) + 1
            if step.operation_type is not None:
                operation_type_counts[step.operation_type] = (
                    operation_type_counts.get(step.operation_type, 0) + 1
                )
            artifact_count += step.artifact_count
            output_document_count += step.output_document_count
            stream_count += step.stream_count
            stream_chunk_count += step.stream_chunk_count
            stream_artifact_count += step.stream_artifact_count
            stream_checkpoint_count += step.stream_checkpoint_count

    return RuntimeStateSummary(
        document_count=len(documents),
        root_document_count=sum(1 for document in documents if not document.parent_document_id),
        child_document_count=sum(1 for document in documents if document.parent_document_id),
        spawned_document_count=sum(1 for document in documents if document.parent_document_id),
        process_count=process_count,
        status_counts=status_counts,
        pipeline_counts=pipeline_counts,
        operation_type_counts=operation_type_counts,
        claim_count=sum(len(document.claims) for document in documents),
        output_count=sum(len(document.outputs) for document in documents),
        output_document_count=output_document_count,
        projection_count=sum(len(document.projections) for document in documents),
        artifact_count=artifact_count,
        stream_count=stream_count,
        stream_chunk_count=stream_chunk_count,
        stream_artifact_count=stream_artifact_count,
        stream_checkpoint_count=stream_checkpoint_count,
        event_count=sum(document.event_count for document in documents),
    )


def _step_report_counts(
    steps: Sequence[RuntimeStepReportItem],
) -> dict[str, Any]:
    status_counts: dict[str, int] = {}
    for step in steps:
        status = _status_value(step.status)
        status_counts[status] = status_counts.get(status, 0) + 1
    terminal_process_count = sum(1 for step in steps if step.is_terminal)
    return {
        "terminal_process_count": terminal_process_count,
        "active_process_count": sum(1 for step in steps if step.is_active),
        "blocked_process_count": sum(1 for step in steps if step.is_blocked),
        "completed_process_count": status_counts.get(ProcessStatus.completed.value, 0),
        "failed_process_count": status_counts.get(ProcessStatus.failed.value, 0),
        "skipped_process_count": status_counts.get(ProcessStatus.skipped.value, 0),
        "cancelled_process_count": status_counts.get(ProcessStatus.cancelled.value, 0),
        "status_counts": status_counts,
        "progress": terminal_process_count / len(steps) if steps else 0.0,
    }


def _status_value(status: ProcessStatus | str) -> str:
    if isinstance(status, ProcessStatus):
        return status.value
    return status


def _is_terminal_status(status: str) -> bool:
    return status in {
        ProcessStatus.completed.value,
        ProcessStatus.failed.value,
        ProcessStatus.skipped.value,
        ProcessStatus.cancelled.value,
    }


def _status_category(status: str) -> str:
    if _is_terminal_status(status):
        return "terminal"
    if status == ProcessStatus.running.value:
        return "running"
    if status == ProcessStatus.queued.value:
        return "queued"
    if status == ProcessStatus.waiting.value:
        return "waiting"
    return "unknown"


def build_runtime_document_state(
    *,
    document_id: str,
    pipeline_id: str | None,
    document: RuntimeDocument | None = None,
    pipeline: PipelineSpec | None,
    statuses: Mapping[str, ProcessStatus],
    claims: Mapping[str, ProcessClaim],
    outputs: Mapping[str, ProcessOutput],
    projections: Mapping[str, CombinedProjection],
    stream_chunks: Sequence[RuntimeStreamChunk] = (),
    stream_checkpoints: Sequence[RuntimeStreamCheckpoint] = (),
    stream_declared_consumers: Mapping[tuple[str, str], Sequence[str]] | None = None,
    operation_type_by_step: Mapping[str, str | None] | None = None,
    events: Sequence[ProcessEvent] = (),
    event_count: int | None = None,
) -> RuntimeDocumentState:
    events_list = list(events)
    stream_snapshots = runtime_stream_snapshots(
        chunks=stream_chunks,
        checkpoints=stream_checkpoints,
        declared_consumers=stream_declared_consumers,
    )
    return RuntimeDocumentState(
        document_id=document_id,
        pipeline_id=pipeline_id,
        relation=document.relation if document else None,
        parent_document_id=document.parent_document_id if document else None,
        parent_process_id=document.parent_process_id if document else None,
        document=document,
        steps=runtime_step_snapshots(
            pipeline=pipeline,
            statuses=statuses,
            claims=claims,
            outputs=outputs,
            stream_snapshots=stream_snapshots,
            operation_type_by_step=operation_type_by_step,
        ),
        statuses=dict(statuses),
        claims=dict(claims),
        outputs=dict(outputs),
        projections=dict(projections),
        events=events_list,
        event_count=len(events_list) if event_count is None else event_count,
    )


def _attach_document_children(
    documents: list[RuntimeDocumentState],
) -> list[RuntimeDocumentState]:
    children: dict[str, list[str]] = {}
    for document in documents:
        parent_document_id = (
            document.parent_document_id
            or (document.document.parent_document_id if document.document else None)
        )
        if parent_document_id:
            children.setdefault(parent_document_id, []).append(document.document_id)

    enriched: list[RuntimeDocumentState] = []
    for document in documents:
        parent_document_id = (
            document.parent_document_id
            or (document.document.parent_document_id if document.document else None)
        )
        parent_process_id = (
            document.parent_process_id
            or (document.document.parent_process_id if document.document else None)
        )
        relation = document.relation or (
            document.document.relation if document.document else None
        )
        child_document_ids = sorted(children.get(document.document_id, []))
        enriched.append(
            document.model_copy(
                update={
                    "relation": relation,
                    "parent_document_id": parent_document_id,
                    "parent_process_id": parent_process_id,
                    "child_document_ids": child_document_ids,
                    "child_document_count": len(child_document_ids),
                }
            )
        )
    return enriched


def runtime_step_snapshots(
    *,
    pipeline: PipelineSpec | None,
    statuses: Mapping[str, ProcessStatus],
    claims: Mapping[str, ProcessClaim],
    outputs: Mapping[str, ProcessOutput],
    stream_snapshots: Mapping[str, Sequence[RuntimeStreamSnapshot]] | None = None,
    operation_type_by_step: Mapping[str, str | None] | None = None,
) -> list[RuntimeStepSnapshot]:
    stream_snapshots = stream_snapshots or {}
    operation_type_by_step = operation_type_by_step or {}
    if pipeline is not None:
        process_ids = [step.id for step in pipeline.steps]
        spec_by_id = {step.id: step for step in pipeline.steps}
    else:
        process_ids = sorted(
            set(statuses) | set(claims) | set(outputs) | set(stream_snapshots)
        )
        spec_by_id = {}

    return [
        runtime_step_snapshot(
            process_id=process_id,
            spec=spec_by_id.get(process_id),
            statuses=statuses,
            claims=claims,
            outputs=outputs,
            streams=stream_snapshots.get(process_id, ()),
            operation_type=operation_type_by_step.get(process_id),
        )
        for process_id in process_ids
    ]


def runtime_step_snapshot(
    *,
    process_id: str,
    spec: ProcessSpec | None,
    statuses: Mapping[str, ProcessStatus],
    claims: Mapping[str, ProcessClaim],
    outputs: Mapping[str, ProcessOutput],
    streams: Sequence[RuntimeStreamSnapshot] = (),
    operation_type: str | None = None,
) -> RuntimeStepSnapshot:
    output = outputs.get(process_id)
    status = statuses.get(process_id)
    claim = claims.get(process_id)
    streams_list = list(streams)
    return RuntimeStepSnapshot(
        id=process_id,
        title=spec.title if spec is not None else None,
        description=spec.description if spec is not None else None,
        tags=list(spec.tags) if spec is not None else [],
        capability=spec.capability if spec is not None else None,
        operation_type=operation_type,
        needs=list(spec.needs) if spec is not None else [],
        adapter_kind=spec.adapter.kind if spec is not None else None,
        priority=spec.priority if spec is not None else 0,
        max_concurrency=spec.max_concurrency if spec is not None else None,
        resource_pool=spec.resource_pool if spec is not None else "default",
        resources=spec.resources if spec is not None else ResourceSpec(),
        sla=spec.sla if spec is not None else ProcessSlaSpec(),
        status=status if status is not None else ("completed" if output else "unknown"),
        has_claim=claim is not None,
        claim=claim,
        has_output=output is not None,
        output_value_keys=sorted(output.values) if output else [],
        artifact_count=len(output.artifacts) if output else 0,
        output_document_count=len(output.output_documents) if output else 0,
        metadata_keys=sorted(output.metadata) if output else [],
        streams=streams_list,
        stream_count=len(streams_list),
        stream_chunk_count=sum(stream.chunk_count for stream in streams_list),
        stream_artifact_count=sum(stream.artifact_count for stream in streams_list),
        stream_checkpoint_count=sum(
            stream.checkpoint_count for stream in streams_list
        ),
    )


def runtime_stream_snapshots(
    *,
    chunks: Sequence[RuntimeStreamChunk],
    checkpoints: Sequence[RuntimeStreamCheckpoint],
    declared_consumers: Mapping[tuple[str, str], Sequence[str]] | None = None,
) -> dict[str, list[RuntimeStreamSnapshot]]:
    grouped_chunks: dict[tuple[str, str], list[RuntimeStreamChunk]] = {}
    grouped_checkpoints: dict[tuple[str, str], list[RuntimeStreamCheckpoint]] = {}
    for chunk in chunks:
        grouped_chunks.setdefault((chunk.process_id, chunk.stream_id), []).append(chunk)
    for checkpoint in checkpoints:
        grouped_checkpoints.setdefault(
            (checkpoint.process_id, checkpoint.stream_id),
            [],
        ).append(checkpoint)

    by_process: dict[str, list[RuntimeStreamSnapshot]] = {}
    for key in sorted(set(grouped_chunks) | set(grouped_checkpoints)):
        process_id, stream_id = key
        declared = sorted(set((declared_consumers or {}).get(key, ())))
        stream_chunks = sorted(grouped_chunks.get(key, []), key=lambda item: item.sequence)
        stream_checkpoints = sorted(
            grouped_checkpoints.get(key, []),
            key=lambda item: item.consumer_id,
        )
        kind_counts: dict[str, int] = {}
        value_keys: set[str] = set()
        for chunk in stream_chunks:
            if chunk.kind is not None:
                kind_counts[chunk.kind] = kind_counts.get(chunk.kind, 0) + 1
            value_keys.update(chunk.values)
        checkpoint_lag = {
            checkpoint.consumer_id: sum(
                1 for chunk in stream_chunks if chunk.sequence > checkpoint.sequence
            )
            for checkpoint in stream_checkpoints
        }
        checkpoint_sequence_map = {
            checkpoint.consumer_id: checkpoint.sequence
            for checkpoint in stream_checkpoints
        }
        checkpoint_chunk_ids = {
            checkpoint.consumer_id: checkpoint.chunk_id
            for checkpoint in stream_checkpoints
        }
        checkpoint_updated_at = {
            checkpoint.consumer_id: checkpoint.updated_at
            for checkpoint in stream_checkpoints
        }
        checkpoint_sequence_values = [
            checkpoint.sequence for checkpoint in stream_checkpoints
        ]
        last = stream_chunks[-1] if stream_chunks else None
        first = stream_chunks[0] if stream_chunks else None
        snapshot = RuntimeStreamSnapshot(
            run_id=(
                (last or stream_checkpoints[0]).run_id
                if (last is not None or stream_checkpoints)
                else ""
            ),
            document_id=(
                (last or stream_checkpoints[0]).document_id
                if (last is not None or stream_checkpoints)
                else ""
            ),
            process_id=process_id,
            stream_id=stream_id,
            declared_consumers=declared,
            chunk_count=len(stream_chunks),
            artifact_count=sum(len(chunk.artifacts) for chunk in stream_chunks),
            checkpoint_count=len(stream_checkpoints),
            checkpoint_consumers=[
                checkpoint.consumer_id for checkpoint in stream_checkpoints
            ],
            checkpoint_lag=checkpoint_lag,
            checkpoint_sequences=checkpoint_sequence_map,
            checkpoint_chunk_ids=checkpoint_chunk_ids,
            checkpoint_updated_at=checkpoint_updated_at,
            max_checkpoint_lag=(
                max(checkpoint_lag.values())
                if checkpoint_lag
                else len(stream_chunks)
            ),
            kind_counts=kind_counts,
            value_keys=sorted(value_keys),
            first_sequence=first.sequence if first is not None else None,
            last_sequence=last.sequence if last is not None else None,
            min_checkpoint_sequence=(
                min(checkpoint_sequence_values)
                if checkpoint_sequence_values
                else None
            ),
            max_checkpoint_sequence=(
                max(checkpoint_sequence_values)
                if checkpoint_sequence_values
                else None
            ),
            last_chunk_id=last.chunk_id if last is not None else None,
            last_chunk_at=last.created_at if last is not None else None,
        )
        by_process.setdefault(process_id, []).append(snapshot)
    return by_process
