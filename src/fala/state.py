from __future__ import annotations

from collections.abc import Mapping, Sequence

from fala.models import (
    CombinedProjection,
    PipelineSpec,
    ProcessClaim,
    ProcessEvent,
    ProcessOutput,
    ProcessSpec,
    ProcessStatus,
    RuntimeDocumentState,
    RuntimeState,
    RuntimeStateSummary,
    RuntimeStepSnapshot,
)


def build_runtime_state(
    *,
    run_id: str,
    documents: Sequence[RuntimeDocumentState],
) -> RuntimeState:
    documents_list = list(documents)
    return RuntimeState(
        run_id=run_id,
        summary=runtime_state_summary(documents_list),
        documents=documents_list,
    )


def runtime_state_summary(
    documents: Sequence[RuntimeDocumentState],
) -> RuntimeStateSummary:
    status_counts: dict[str, int] = {}
    pipeline_counts: dict[str, int] = {}
    process_count = 0
    artifact_count = 0

    for document in documents:
        pipeline_id = document.pipeline_id or "unknown"
        pipeline_counts[pipeline_id] = pipeline_counts.get(pipeline_id, 0) + 1
        process_count += len(document.steps)
        for step in document.steps:
            status = step.status.value if isinstance(step.status, ProcessStatus) else step.status
            status_counts[status] = status_counts.get(status, 0) + 1
            artifact_count += step.artifact_count

    return RuntimeStateSummary(
        document_count=len(documents),
        process_count=process_count,
        status_counts=status_counts,
        pipeline_counts=pipeline_counts,
        claim_count=sum(len(document.claims) for document in documents),
        output_count=sum(len(document.outputs) for document in documents),
        projection_count=sum(len(document.projections) for document in documents),
        artifact_count=artifact_count,
        event_count=sum(document.event_count for document in documents),
    )


def build_runtime_document_state(
    *,
    document_id: str,
    pipeline_id: str | None,
    pipeline: PipelineSpec | None,
    statuses: Mapping[str, ProcessStatus],
    claims: Mapping[str, ProcessClaim],
    outputs: Mapping[str, ProcessOutput],
    projections: Mapping[str, CombinedProjection],
    events: Sequence[ProcessEvent] = (),
    event_count: int | None = None,
) -> RuntimeDocumentState:
    events_list = list(events)
    return RuntimeDocumentState(
        document_id=document_id,
        pipeline_id=pipeline_id,
        steps=runtime_step_snapshots(
            pipeline=pipeline,
            statuses=statuses,
            claims=claims,
            outputs=outputs,
        ),
        statuses=dict(statuses),
        claims=dict(claims),
        outputs=dict(outputs),
        projections=dict(projections),
        events=events_list,
        event_count=len(events_list) if event_count is None else event_count,
    )


def runtime_step_snapshots(
    *,
    pipeline: PipelineSpec | None,
    statuses: Mapping[str, ProcessStatus],
    claims: Mapping[str, ProcessClaim],
    outputs: Mapping[str, ProcessOutput],
) -> list[RuntimeStepSnapshot]:
    if pipeline is not None:
        process_ids = [step.id for step in pipeline.steps]
        spec_by_id = {step.id: step for step in pipeline.steps}
    else:
        process_ids = sorted(set(statuses) | set(claims) | set(outputs))
        spec_by_id = {}

    return [
        runtime_step_snapshot(
            process_id=process_id,
            spec=spec_by_id.get(process_id),
            statuses=statuses,
            claims=claims,
            outputs=outputs,
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
) -> RuntimeStepSnapshot:
    output = outputs.get(process_id)
    status = statuses.get(process_id)
    claim = claims.get(process_id)
    return RuntimeStepSnapshot(
        id=process_id,
        title=spec.title if spec is not None else None,
        description=spec.description if spec is not None else None,
        tags=list(spec.tags) if spec is not None else [],
        needs=list(spec.needs) if spec is not None else [],
        adapter_kind=spec.adapter.kind if spec is not None else None,
        status=status if status is not None else ("completed" if output else "unknown"),
        has_claim=claim is not None,
        claim=claim,
        has_output=output is not None,
        output_value_keys=sorted(output.values) if output else [],
        artifact_count=len(output.artifacts) if output else 0,
        metadata_keys=sorted(output.metadata) if output else [],
    )
