from __future__ import annotations

from typing import Any

from fala.models import (
    ArtifactRef,
    OutputDocumentRef,
    ProcessExecutionContext,
    ProcessOutput,
    ProcessSpec,
)


def output_with_lineage(
    output: ProcessOutput,
    *,
    context: ProcessExecutionContext,
    step: ProcessSpec | None = None,
    need_outputs: dict[str, ProcessOutput] | None = None,
    worker_id: str | None = None,
) -> ProcessOutput:
    metadata = dict(output.metadata)
    namespace = metadata.get("process_runtime")
    runtime_metadata = dict(namespace) if isinstance(namespace, dict) else {}
    if namespace is not None and not isinstance(namespace, dict):
        runtime_metadata["user_metadata"] = namespace

    initial_values = context.input.values.get("initial")
    needs_values = context.input.values.get("needs")
    dependency_outputs = need_outputs or {}
    lineage: dict[str, Any] = {
        "schema_version": 1,
        "pipeline_id": context.pipeline_id,
        "run_id": context.run_id,
        "document_id": context.document_id,
        "process_id": context.process_id,
        "capability": context.capability,
        "attempt": context.attempt,
        "needs": list(step.needs if step is not None else sorted(dependency_outputs)),
        "input_artifact_count": len(context.input.artifacts),
        "input_artifacts": [
            _artifact_summary(artifact) for artifact in context.input.artifacts
        ],
        "initial_value_keys": sorted(initial_values)
        if isinstance(initial_values, dict)
        else [],
        "needs_value_keys": {
            process_id: sorted(values)
            for process_id, values in (needs_values or {}).items()
            if isinstance(values, dict)
        }
        if isinstance(needs_values, dict)
        else {},
        "dependency_outputs": [
            {
                "process_id": process_id,
                "value_keys": sorted(need_output.values),
                "artifact_count": len(need_output.artifacts),
                "output_document_count": len(need_output.output_documents),
                "artifacts": [
                    _artifact_summary(artifact)
                    for artifact in need_output.artifacts
                ],
                "output_documents": [
                    _output_document_summary(document)
                    for document in need_output.output_documents
                ],
            }
            for process_id, need_output in sorted(dependency_outputs.items())
        ],
        "output_document_count": len(output.output_documents),
        "output_documents": [
            _output_document_summary(document)
            for document in output.output_documents
        ],
    }
    if worker_id is not None:
        lineage["worker_id"] = worker_id

    runtime_metadata["lineage"] = lineage
    metadata["process_runtime"] = runtime_metadata
    return output.model_copy(update={"metadata": metadata})


def _artifact_summary(artifact: ArtifactRef) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "id": artifact.id,
        "kind": artifact.kind,
        "uri": artifact.uri,
        "metadata_keys": sorted(artifact.metadata),
    }
    for key in ("sha256", "size_bytes", "filename", "media_type", "content_type"):
        if key in artifact.metadata:
            summary[key] = artifact.metadata[key]
    return summary


def _output_document_summary(document: OutputDocumentRef) -> dict[str, Any]:
    return {
        "id": document.id,
        "document_type": document.document_type,
        "media_type": document.media_type,
        "uri": document.uri,
        "artifact_id": document.artifact_id,
        "relation": document.relation,
        "metadata_keys": sorted(document.metadata),
        "value_keys": sorted(document.values),
    }
