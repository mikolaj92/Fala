from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

from fala.schema_validation import validate_json_schema

AdapterKind = Literal["subprocess", "http", "queue", "manual"]
ExistingDocumentPolicy = Literal["error", "reuse"]
ExistingRunPolicy = Literal["error", "resume"]
RUNTIME_ID_PATTERN = r"^[A-Za-z][A-Za-z0-9_.-]{0,127}$"
RuntimeId = Annotated[str, Field(pattern=RUNTIME_ID_PATTERN)]


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:12]}"


class ProcessStatus(StrEnum):
    waiting = "waiting"
    queued = "queued"
    running = "running"
    completed = "completed"
    failed = "failed"
    skipped = "skipped"
    cancelled = "cancelled"


class ProcessAction(StrEnum):
    retry = "retry"
    skip = "skip"
    fail = "fail"
    cancel = "cancel"


class RunStatus(StrEnum):
    created = "created"
    paused = "paused"
    queued = "queued"
    running = "running"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"


class RunOutcome(StrEnum):
    success = "success"
    partial = "partial"
    failed = "failed"
    cancelled = "cancelled"


class RuntimeWorkerStatus(StrEnum):
    starting = "starting"
    idle = "idle"
    working = "working"
    stopping = "stopping"
    stopped = "stopped"
    error = "error"


class RuntimeDocumentStatus(StrEnum):
    registered = "registered"
    queued = "queued"
    running = "running"
    completed = "completed"
    failed = "failed"
    cancelled = "cancelled"


class ProcessActionInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    action: ProcessAction
    reason: str | None = None


class ArtifactRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId = Field(default_factory=lambda: new_id("artifact"))
    kind: str
    uri: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class ProcessInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    artifacts: list[ArtifactRef] = Field(default_factory=list)
    scheduled_at: datetime | None = None
    values: dict[str, Any] = Field(default_factory=dict)


class SpawnDocumentInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    document_id: str = Field(default_factory=lambda: new_id("doc"))
    pipeline_id: RuntimeId | None = None
    title: str | None = None
    document_type: RuntimeId | None = None
    relation: RuntimeId | None = None
    media_type: str | None = None
    source_uri: str | None = None
    scheduled_at: datetime | None = None
    values: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    artifacts: list[ArtifactRef] = Field(default_factory=list)
    parent_document_id: str | None = None
    parent_process_id: RuntimeId | None = None


class ProcessOutputStreamChunk(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stream_id: RuntimeId = "main"
    sequence: int | None = Field(default=None, ge=0)
    kind: RuntimeId | None = None
    artifacts: list[ArtifactRef] = Field(default_factory=list)
    values: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class OutputDocumentRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId = Field(default_factory=lambda: new_id("output_doc"))
    title: str | None = None
    document_type: RuntimeId
    media_type: str | None = None
    uri: str | None = None
    artifact_id: RuntimeId | None = None
    relation: RuntimeId = "derived"
    values: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class ProcessOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    artifacts: list[ArtifactRef] = Field(default_factory=list)
    values: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    output_documents: list[OutputDocumentRef] = Field(default_factory=list)
    spawn_documents: list[SpawnDocumentInput] = Field(default_factory=list)
    stream_chunks: list[ProcessOutputStreamChunk] = Field(default_factory=list)


class RuntimeStreamChunk(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    document_id: str
    process_id: RuntimeId
    stream_id: RuntimeId = "main"
    chunk_id: RuntimeId = Field(default_factory=lambda: new_id("chunk"))
    sequence: int = Field(ge=0)
    kind: RuntimeId | None = None
    artifacts: list[ArtifactRef] = Field(default_factory=list)
    values: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class RuntimeStreamCheckpoint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    document_id: str
    process_id: RuntimeId
    stream_id: RuntimeId = "main"
    consumer_id: RuntimeId = "default"
    sequence: int = Field(default=-1, ge=-1)
    chunk_id: RuntimeId | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class RuntimeStreamBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    document_id: str
    process_id: RuntimeId
    stream_id: RuntimeId = "main"
    consumer_id: RuntimeId = "default"
    checkpoint: RuntimeStreamCheckpoint | None = None
    after_sequence: int = Field(default=-1, ge=-1)
    limit: int = Field(default=100, ge=1)
    chunk_count: int = Field(default=0, ge=0)
    chunks: list[RuntimeStreamChunk] = Field(default_factory=list)
    last_sequence: int | None = None
    last_chunk_id: RuntimeId | None = None


class RuntimeDocumentInput(SpawnDocumentInput):
    pass


class RuntimeRunInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str | None = None
    existing_run_policy: ExistingRunPolicy = "error"
    existing_document_policy: ExistingDocumentPolicy = "error"
    title: str | None = None
    pipeline_id: RuntimeId | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    documents: list[RuntimeDocumentInput] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_documents(self) -> "RuntimeRunInput":
        document_ids = [document.document_id for document in self.documents]
        duplicates = sorted({item for item in document_ids if document_ids.count(item) > 1})
        if duplicates:
            raise ValueError(f"Duplicate runtime document id(s): {', '.join(duplicates)}")
        for document in self.documents:
            if document.pipeline_id is None and self.pipeline_id is None:
                raise ValueError(
                    f"Document {document.document_id!r} requires pipeline_id "
                    "or a run-level pipeline_id"
                )
        return self


class RuntimeDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    document_id: str
    pipeline_id: RuntimeId | None = None
    title: str | None = None
    document_type: RuntimeId | None = None
    relation: RuntimeId | None = None
    media_type: str | None = None
    source_uri: str | None = None
    scheduled_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    parent_document_id: str | None = None
    parent_process_id: RuntimeId | None = None
    status: RuntimeDocumentStatus = RuntimeDocumentStatus.registered
    summary: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class RuntimeDocumentPage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool = True
    run_id: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    count: int = Field(ge=0)
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)
    has_more: bool = False
    filters: dict[str, Any] = Field(default_factory=dict)
    documents: list[RuntimeDocument] = Field(default_factory=list)


class DocumentTypeSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    media_types: list[str] = Field(default_factory=list)
    extensions: list[str] = Field(default_factory=list)
    value_schema: dict[str, Any] = Field(default_factory=dict)
    metadata_schema: dict[str, Any] = Field(default_factory=dict)


class DocumentRelationSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    source_document_types: list[RuntimeId] = Field(default_factory=list)
    target_document_types: list[RuntimeId] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_document_types(self) -> "DocumentRelationSpec":
        _validate_unique_values(
            f"Document relation {self.id!r} source_document_types",
            self.source_document_types,
        )
        _validate_unique_values(
            f"Document relation {self.id!r} target_document_types",
            self.target_document_types,
        )
        return self


class OperationTypeSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    category: RuntimeId | None = None


class ArtifactKindSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    media_types: list[str] = Field(default_factory=list)
    extensions: list[str] = Field(default_factory=list)
    value_schema: dict[str, Any] = Field(default_factory=dict)
    metadata_schema: dict[str, Any] = Field(default_factory=dict)


class StreamSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    stream_id: RuntimeId = Field(alias="stream")
    kinds: list[RuntimeId] = Field(default_factory=list)
    consumers: list[RuntimeId] = Field(default_factory=list)
    emits_artifact_kinds: list[RuntimeId] = Field(default_factory=list)
    max_buffered_chunks: int | None = Field(default=None, ge=1)
    value_schema: dict[str, Any] = Field(default_factory=dict)
    metadata_schema: dict[str, Any] = Field(default_factory=dict)


class CapabilitySpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    operation_type: RuntimeId | None = None
    accepts_document_types: list[RuntimeId] = Field(default_factory=list)
    accepts_artifact_kinds: list[RuntimeId] = Field(default_factory=list)
    emits_document_types: list[RuntimeId] = Field(default_factory=list)
    emits_artifact_kinds: list[RuntimeId] = Field(default_factory=list)
    emits_streams: list[StreamSpec] = Field(default_factory=list)
    config_schema: dict[str, Any] = Field(default_factory=dict)
    output_schema: dict[str, Any] = Field(default_factory=dict)


class AdapterSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: AdapterKind
    command: list[str] | None = None
    cwd: str | None = None
    env: dict[str, str] = Field(default_factory=dict)
    url: str | None = None
    queue: str | None = None
    timeout_seconds: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def validate_boundary_fields(self) -> "AdapterSpec":
        if self.kind == "subprocess":
            if not self.command:
                raise ValueError("subprocess adapter requires non-empty command")
            if self.url is not None:
                raise ValueError("subprocess adapter cannot define url")
            if self.queue is not None:
                raise ValueError("subprocess adapter cannot define queue")
            return self

        if self.kind == "http":
            if not self.url:
                raise ValueError("http adapter requires url")
            if self.command is not None:
                raise ValueError("http adapter cannot define command")
            if self.cwd is not None:
                raise ValueError("http adapter cannot define cwd")
            if self.env:
                raise ValueError("http adapter cannot define env")
            if self.queue is not None:
                raise ValueError("http adapter cannot define queue")
            return self

        if self.kind == "manual":
            if self.command is not None:
                raise ValueError("manual adapter cannot define command")
            if self.cwd is not None:
                raise ValueError("manual adapter cannot define cwd")
            if self.env:
                raise ValueError("manual adapter cannot define env")
            if self.url is not None:
                raise ValueError("manual adapter cannot define url")
            if self.queue is not None:
                raise ValueError("manual adapter cannot define queue")
            if self.timeout_seconds is not None:
                raise ValueError("manual adapter cannot define timeout_seconds")
            return self

        if not self.queue:
            raise ValueError("queue adapter requires queue")
        if self.command is not None:
            raise ValueError("queue adapter cannot define command")
        if self.cwd is not None:
            raise ValueError("queue adapter cannot define cwd")
        if self.env:
            raise ValueError("queue adapter cannot define env")
        if self.url is not None:
            raise ValueError("queue adapter cannot define url")
        return self


class RetryPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_attempts: int = Field(default=1, ge=1)
    delay_seconds: float = Field(default=0.0, ge=0)
    retry_error_kinds: list[RuntimeId] = Field(default_factory=list)
    terminal_error_kinds: list[RuntimeId] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_error_kinds(self) -> "RetryPolicy":
        _validate_unique_values("retry_error_kinds", self.retry_error_kinds)
        _validate_unique_values("terminal_error_kinds", self.terminal_error_kinds)
        overlap = sorted(set(self.retry_error_kinds).intersection(self.terminal_error_kinds))
        if overlap:
            raise ValueError(
                "RetryPolicy error kind(s) cannot be both retryable and terminal: "
                f"{', '.join(overlap)}"
            )
        return self


class ResourceSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cpu_cores: float | None = Field(default=None, gt=0)
    memory_mb: int | None = Field(default=None, ge=1)
    disk_mb: int | None = Field(default=None, ge=1)
    gpu_count: int | None = Field(default=None, ge=1)
    labels: list[RuntimeId] = Field(default_factory=list)
    units: dict[RuntimeId, float] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_resources(self) -> "ResourceSpec":
        duplicates = sorted({item for item in self.labels if self.labels.count(item) > 1})
        if duplicates:
            raise ValueError(f"Duplicate resource label(s): {', '.join(duplicates)}")
        invalid_units = sorted(key for key, value in self.units.items() if value <= 0)
        if invalid_units:
            raise ValueError(
                f"Resource unit value must be greater than zero: {', '.join(invalid_units)}"
            )
        return self

    def has_requirements(self) -> bool:
        return any(
            (
                self.cpu_cores is not None,
                self.memory_mb is not None,
                self.disk_mb is not None,
                self.gpu_count is not None,
                bool(self.labels),
                bool(self.units),
            )
        )


class ResourceQuantity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    cpu_cores: float = Field(default=0.0, ge=0)
    memory_mb: int = Field(default=0, ge=0)
    disk_mb: int = Field(default=0, ge=0)
    gpu_count: int = Field(default=0, ge=0)
    units: dict[RuntimeId, float] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_resources(self) -> "ResourceQuantity":
        invalid_units = sorted(key for key, value in self.units.items() if value < 0)
        if invalid_units:
            raise ValueError(
                f"Resource unit value must be greater than or equal to zero: {', '.join(invalid_units)}"
            )
        return self


class ResourcePoolSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId = "default"
    title: str | None = None
    description: str | None = None
    resources: ResourceSpec = Field(default_factory=ResourceSpec)


class ProcessConditionSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    document_types: list[RuntimeId] = Field(default_factory=list)
    media_types: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    values: dict[str, Any] = Field(default_factory=dict)


class ProcessSlaSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    waiting_after_seconds: float | None = Field(default=None, ge=0)
    queued_after_seconds: float | None = Field(default=None, ge=0)
    running_after_seconds: float | None = Field(default=None, ge=0)

    def configured(self) -> bool:
        return any(
            value is not None
            for value in (
                self.waiting_after_seconds,
                self.queued_after_seconds,
                self.running_after_seconds,
            )
        )


class ChildDocumentWaitSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    from_processes: list[RuntimeId] = Field(default_factory=list)
    document_types: list[RuntimeId] = Field(default_factory=list)
    relations: list[RuntimeId] = Field(default_factory=list)
    required_statuses: list[RuntimeDocumentStatus] = Field(
        default_factory=lambda: [RuntimeDocumentStatus.completed]
    )
    min_count: int = Field(default=0, ge=0)

    @model_validator(mode="after")
    def validate_statuses(self) -> "ChildDocumentWaitSpec":
        _validate_unique_values(
            "ChildDocumentWaitSpec from_processes",
            self.from_processes,
        )
        _validate_unique_values(
            "ChildDocumentWaitSpec document_types",
            self.document_types,
        )
        _validate_unique_values(
            "ChildDocumentWaitSpec relations",
            self.relations,
        )
        statuses = [status.value for status in self.required_statuses]
        _validate_unique_values("ChildDocumentWaitSpec required_statuses", statuses)
        if not statuses:
            raise ValueError("ChildDocumentWaitSpec required_statuses cannot be empty")
        return self


class ProcessSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    capability: RuntimeId | None = None
    adapter: AdapterSpec
    needs: list[str] = Field(default_factory=list)
    timeout_seconds: float | None = Field(default=None, gt=0)
    priority: int = 0
    max_concurrency: int | None = Field(default=None, ge=1)
    retry: RetryPolicy = Field(default_factory=RetryPolicy)
    resource_pool: RuntimeId = "default"
    resources: ResourceSpec = Field(default_factory=ResourceSpec)
    sla: ProcessSlaSpec = Field(default_factory=ProcessSlaSpec)
    when: ProcessConditionSpec = Field(default_factory=ProcessConditionSpec)
    wait_for_children: ChildDocumentWaitSpec | None = None
    config: dict[str, Any] = Field(default_factory=dict)


class WorkItemPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claim_strategy: Literal["parallel", "sequential"] = "parallel"
    order_by: str = Field(default="index", min_length=1)


class CombineSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    needs: list[RuntimeId]
    mode: Literal["latest"] = "latest"
    emit_partial: bool = False


class RunReduceSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    process_id: RuntimeId | None = None
    document_type: RuntimeId | None = None
    mode: Literal["collect_values", "collect_outputs", "count"] = "collect_values"
    value_key: str | None = None
    include_artifacts: bool = False


class PipelineSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    version: str = "1"
    input_values: dict[str, str] = Field(default_factory=dict)
    work_items: WorkItemPolicy = Field(default_factory=WorkItemPolicy)
    allow_feedback_cycles: bool = False
    steps: list[ProcessSpec]
    combines: list[CombineSpec] = Field(default_factory=list)
    reduces: list[RunReduceSpec] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_graph(self) -> "PipelineSpec":
        step_ids = [step.id for step in self.steps]
        duplicates = sorted({item for item in step_ids if step_ids.count(item) > 1})
        if duplicates:
            raise ValueError(f"Duplicate process id(s): {', '.join(duplicates)}")

        known = set(step_ids)
        for step in self.steps:
            missing = sorted(set(step.needs) - known)
            if missing:
                raise ValueError(
                    f"Process {step.id!r} depends on unknown process id(s): {', '.join(missing)}"
                )
            if step.id in step.needs:
                raise ValueError(f"Process {step.id!r} cannot depend on itself")

        for combine in self.combines:
            missing = sorted(set(combine.needs) - known)
            if missing:
                raise ValueError(
                    f"Combine {combine.id!r} depends on unknown process id(s): {', '.join(missing)}"
                )

        reduce_ids = [reduce.id for reduce in self.reduces]
        reduce_duplicates = sorted({item for item in reduce_ids if reduce_ids.count(item) > 1})
        if reduce_duplicates:
            raise ValueError(f"Duplicate run reduce id(s): {', '.join(reduce_duplicates)}")
        for reduce in self.reduces:
            if reduce.process_id is not None and reduce.process_id not in known:
                raise ValueError(
                    f"Run reduce {reduce.id!r} references unknown process id "
                    f"{reduce.process_id!r}"
                )

        if not self.allow_feedback_cycles:
            _validate_acyclic(self.steps)
        return self


class WorkflowWorkerSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    capabilities: list[RuntimeId] = Field(default_factory=list)
    pipeline_id: RuntimeId
    process_id: RuntimeId | None = None
    adapter_kind: Literal["queue"] = "queue"
    command: list[str] = Field(min_length=1)
    cwd: str | None = None
    env: dict[str, str] = Field(default_factory=dict)
    timeout_seconds: float | None = Field(default=None, gt=0)
    resources: ResourceSpec = Field(default_factory=ResourceSpec)
    secrets: list[RuntimeId] = Field(default_factory=list)
    sandbox: "WorkerSandboxSpec" = Field(default_factory=lambda: WorkerSandboxSpec())

    @model_validator(mode="after")
    def validate_worker_policy(self) -> "WorkflowWorkerSpec":
        _validate_unique_values(f"Workflow package worker {self.id!r} secrets", self.secrets)
        return self


class WorkflowSecretSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    env_var: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
    required: bool = True
    kubernetes_secret_name: str | None = None
    kubernetes_secret_key: str = "value"


class WorkerSandboxSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_as_non_root: bool = True
    read_only_root_filesystem: bool = True
    allow_privilege_escalation: bool = False
    drop_capabilities: list[str] = Field(default_factory=lambda: ["ALL"])
    seccomp_profile: str | None = "RuntimeDefault"

    @model_validator(mode="after")
    def validate_sandbox(self) -> "WorkerSandboxSpec":
        duplicates = sorted(
            {item for item in self.drop_capabilities if self.drop_capabilities.count(item) > 1}
        )
        if duplicates:
            raise ValueError(
                f"Duplicate sandbox capability drop(s): {', '.join(duplicates)}"
            )
        return self


class WorkflowPackageSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    version: str = "1"
    document_types: list[DocumentTypeSpec] = Field(default_factory=list)
    document_relations: list[DocumentRelationSpec] = Field(default_factory=list)
    operation_types: list[OperationTypeSpec] = Field(default_factory=list)
    artifact_kinds: list[ArtifactKindSpec] = Field(default_factory=list)
    capabilities: list[CapabilitySpec] = Field(default_factory=list)
    secrets: list[WorkflowSecretSpec] = Field(default_factory=list)
    pipelines: list[str] = Field(min_length=1)
    workers: list[WorkflowWorkerSpec] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_pipeline_paths(self) -> "WorkflowPackageSpec":
        _validate_unique_ids("workflow package document type", self.document_types)
        _validate_unique_ids(
            "workflow package document relation",
            self.document_relations,
        )
        _validate_unique_ids("workflow package operation type", self.operation_types)
        _validate_unique_ids("workflow package artifact kind", self.artifact_kinds)
        _validate_unique_ids("workflow package capability", self.capabilities)
        _validate_unique_ids("workflow package secret", self.secrets)

        for document_type in self.document_types:
            validate_json_schema(
                document_type.value_schema,
                label=f"Document type {document_type.id!r} value_schema",
            )
            validate_json_schema(
                document_type.metadata_schema,
                label=f"Document type {document_type.id!r} metadata_schema",
            )
        for artifact_kind in self.artifact_kinds:
            validate_json_schema(
                artifact_kind.value_schema,
                label=f"Artifact kind {artifact_kind.id!r} value_schema",
            )
            validate_json_schema(
                artifact_kind.metadata_schema,
                label=f"Artifact kind {artifact_kind.id!r} metadata_schema",
            )
        for capability in self.capabilities:
            validate_json_schema(
                capability.config_schema,
                label=f"Capability {capability.id!r} config_schema",
            )
            validate_json_schema(
                capability.output_schema,
                label=f"Capability {capability.id!r} output_schema",
            )
            for stream in capability.emits_streams:
                _validate_unique_values(
                    (
                        f"Capability {capability.id!r} stream "
                        f"{stream.stream_id!r} consumers"
                    ),
                    stream.consumers,
                )
                validate_json_schema(
                    stream.value_schema,
                    label=(
                        f"Capability {capability.id!r} stream "
                        f"{stream.stream_id!r} value_schema"
                    ),
                )
                validate_json_schema(
                    stream.metadata_schema,
                    label=(
                        f"Capability {capability.id!r} stream "
                        f"{stream.stream_id!r} metadata_schema"
                    ),
                )

        document_type_ids = {item.id for item in self.document_types}
        artifact_kind_ids = {item.id for item in self.artifact_kinds}
        operation_type_ids = {item.id for item in self.operation_types}
        for relation in self.document_relations:
            _validate_known_refs(
                f"Document relation {relation.id!r} source_document_types",
                relation.source_document_types,
                document_type_ids,
            )
            _validate_known_refs(
                f"Document relation {relation.id!r} target_document_types",
                relation.target_document_types,
                document_type_ids,
            )
        for capability in self.capabilities:
            _validate_known_refs(
                f"Capability {capability.id!r} operation_type",
                [capability.operation_type] if capability.operation_type else [],
                operation_type_ids,
            )
            _validate_known_refs(
                f"Capability {capability.id!r} accepts_document_types",
                capability.accepts_document_types,
                document_type_ids,
            )
            _validate_known_refs(
                f"Capability {capability.id!r} accepts_artifact_kinds",
                capability.accepts_artifact_kinds,
                artifact_kind_ids,
            )
            _validate_known_refs(
                f"Capability {capability.id!r} emits_document_types",
                capability.emits_document_types,
                document_type_ids,
            )
            _validate_known_refs(
                f"Capability {capability.id!r} emits_artifact_kinds",
                capability.emits_artifact_kinds,
                artifact_kind_ids,
            )
            for stream in capability.emits_streams:
                _validate_known_refs(
                    (
                        f"Capability {capability.id!r} stream "
                        f"{stream.stream_id!r} emits_artifact_kinds"
                    ),
                    stream.emits_artifact_kinds,
                    artifact_kind_ids,
                )

        capability_ids = {item.id for item in self.capabilities}
        secret_ids = {item.id for item in self.secrets}
        secret_env_by_id = {item.id: item.env_var for item in self.secrets}
        for worker in self.workers:
            _validate_known_refs(
                f"Workflow package worker {worker.id!r} capabilities",
                worker.capabilities,
                capability_ids,
            )
            _validate_known_refs(
                f"Workflow package worker {worker.id!r} secrets",
                worker.secrets,
                secret_ids,
            )
            conflicting_env = sorted(
                secret_env_by_id[secret_id]
                for secret_id in worker.secrets
                if secret_env_by_id[secret_id] in worker.env
            )
            if conflicting_env:
                raise ValueError(
                    f"Workflow package worker {worker.id!r} env overrides secret "
                    f"env var(s): {', '.join(conflicting_env)}"
                )

        seen: set[str] = set()
        for path in self.pipelines:
            if not path or not path.strip():
                raise ValueError("Workflow package pipeline paths cannot be empty")
            parts = path.split("/")
            if path.startswith("/") or "\\" in path or ".." in parts:
                raise ValueError(
                    "Workflow package pipeline paths must stay inside the package"
                )
            if path in seen:
                raise ValueError(f"Duplicate workflow package pipeline path: {path!r}")
            seen.add(path)
        worker_ids = [worker.id for worker in self.workers]
        duplicates = sorted({item for item in worker_ids if worker_ids.count(item) > 1})
        if duplicates:
            raise ValueError(f"Duplicate workflow package worker id(s): {', '.join(duplicates)}")
        return self


class RuntimeRun(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: new_id("run"))
    title: str | None = None
    status: RunStatus = RunStatus.created
    outcome: RunOutcome | None = None
    outcome_reason: str | None = None
    config: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    summary: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: datetime | None = None
    finished_at: datetime | None = None


class RuntimeWorkerHeartbeat(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    worker_id: str
    pipeline_id: RuntimeId | None = None
    process_id: RuntimeId | None = None
    adapter_kind: AdapterKind | None = None
    capabilities: list[RuntimeId] = Field(default_factory=list)
    resources: ResourceSpec = Field(default_factory=ResourceSpec)
    status: RuntimeWorkerStatus = RuntimeWorkerStatus.idle
    current_document_id: str | None = None
    current_process_id: RuntimeId | None = None
    started_at: datetime | None = None
    last_seen_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: dict[str, Any] = Field(default_factory=dict)


class RuntimeWorkerState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    worker_id: str
    pipeline_id: RuntimeId | None = None
    process_id: RuntimeId | None = None
    adapter_kind: AdapterKind | None = None
    capabilities: list[RuntimeId] = Field(default_factory=list)
    resources: ResourceSpec = Field(default_factory=ResourceSpec)
    status: RuntimeWorkerStatus
    current_document_id: str | None = None
    current_process_id: RuntimeId | None = None
    started_at: datetime | None = None
    last_seen_at: datetime
    age_seconds: float = Field(ge=0)
    stale_after_seconds: float = Field(gt=0)
    healthy: bool
    metadata: dict[str, Any] = Field(default_factory=dict)


def _validate_acyclic(steps: list[ProcessSpec]) -> None:
    graph: dict[str, list[str]] = defaultdict(list)
    for step in steps:
        graph[step.id].extend(step.needs)

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node: str) -> None:
        if node in visited:
            return
        if node in visiting:
            raise ValueError(f"Pipeline contains a cycle at process {node!r}")
        visiting.add(node)
        for dep in graph[node]:
            visit(dep)
        visiting.remove(node)
        visited.add(node)

    for step in steps:
        visit(step.id)


def _validate_unique_ids(label: str, items: list[Any]) -> None:
    ids = [item.id for item in items]
    duplicates = sorted({item for item in ids if ids.count(item) > 1})
    if duplicates:
        raise ValueError(f"Duplicate {label} id(s): {', '.join(duplicates)}")


def _validate_unique_values(label: str, values: list[str]) -> None:
    duplicates = sorted({item for item in values if values.count(item) > 1})
    if duplicates:
        raise ValueError(f"Duplicate {label} value(s): {', '.join(duplicates)}")


def _validate_known_refs(label: str, refs: list[str], known: set[str]) -> None:
    missing = sorted(set(refs) - known)
    if missing:
        raise ValueError(f"{label} reference unknown id(s): {', '.join(missing)}")


class ProcessExecutionContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_id: RuntimeId
    run_id: str
    document_id: str
    process_id: RuntimeId
    capability: RuntimeId | None = None
    attempt: int
    input: ProcessInput
    config: dict[str, Any] = Field(default_factory=dict)


class ProcessEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId = Field(default_factory=lambda: new_id("event"))
    run_id: str
    document_id: str
    process_id: RuntimeId | None
    operation_type: RuntimeId | None = None
    type: str
    status: ProcessStatus | None = None
    data: dict[str, Any] = Field(default_factory=dict)
    ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ProcessEventPage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool = True
    run_id: str
    document_id: str
    process_id: RuntimeId | None = None
    operation_type: RuntimeId | None = None
    count: int = Field(ge=0)
    has_more: bool
    next_after_event_id: RuntimeId | None = None
    events: list[ProcessEvent]


class OperatorAuditEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId = Field(default_factory=lambda: new_id("audit"))
    ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    actor: str | None = None
    source: str | None = None
    action: str
    run_id: str | None = None
    document_id: str | None = None
    process_id: RuntimeId | None = None
    target: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)


class OperatorAuditEventPage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool = True
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    count: int = Field(ge=0)
    filters: dict[str, Any] = Field(default_factory=dict)
    events: list[OperatorAuditEvent] = Field(default_factory=list)


class ProcessClaim(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    document_id: str
    process_id: RuntimeId
    worker_id: str | None = None
    attempt: int = Field(ge=1)
    claimed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: datetime


class CombinedProjection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    run_id: str
    document_id: str
    complete: bool
    latest: dict[str, ProcessOutput]
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class DocumentRunResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    document_id: str
    outputs: dict[str, ProcessOutput]
    projections: dict[str, CombinedProjection]
    events: list[ProcessEvent]


class RuntimeStreamSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    document_id: str
    process_id: RuntimeId
    stream_id: RuntimeId
    declared_consumers: list[RuntimeId] = Field(default_factory=list)
    chunk_count: int = Field(default=0, ge=0)
    artifact_count: int = Field(default=0, ge=0)
    checkpoint_count: int = Field(default=0, ge=0)
    checkpoint_consumers: list[RuntimeId] = Field(default_factory=list)
    checkpoint_lag: dict[str, int] = Field(default_factory=dict)
    checkpoint_sequences: dict[str, int] = Field(default_factory=dict)
    checkpoint_chunk_ids: dict[str, RuntimeId | None] = Field(default_factory=dict)
    checkpoint_updated_at: dict[str, datetime] = Field(default_factory=dict)
    max_checkpoint_lag: int = Field(default=0, ge=0)
    kind_counts: dict[str, int] = Field(default_factory=dict)
    value_keys: list[str] = Field(default_factory=list)
    first_sequence: int | None = None
    last_sequence: int | None = None
    min_checkpoint_sequence: int | None = None
    max_checkpoint_sequence: int | None = None
    last_chunk_id: RuntimeId | None = None
    last_chunk_at: datetime | None = None


class RuntimeStreamLagItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    document_id: str
    document_title: str | None = None
    document_type: RuntimeId | None = None
    parent_document_id: str | None = None
    pipeline_id: RuntimeId | None = None
    process_id: RuntimeId
    title: str | None = None
    capability: RuntimeId | None = None
    operation_type: RuntimeId | None = None
    adapter_kind: AdapterKind | None = None
    resource_pool: RuntimeId = "default"
    stream_id: RuntimeId
    consumer_id: RuntimeId | None = None
    lag: int = Field(ge=0)
    chunk_count: int = Field(default=0, ge=0)
    checkpoint_count: int = Field(default=0, ge=0)
    checkpoint_sequence: int | None = None
    checkpoint_chunk_id: RuntimeId | None = None
    checkpoint_updated_at: datetime | None = None
    last_sequence: int | None = None
    last_chunk_id: RuntimeId | None = None
    last_chunk_at: datetime | None = None
    max_buffered_chunks: int | None = Field(default=None, ge=0)
    declared_consumer: bool = False
    over_limit: bool = False
    uncheckpointed: bool = False
    kind_counts: dict[str, int] = Field(default_factory=dict)
    value_keys: list[str] = Field(default_factory=list)


class RuntimeStreamLagPage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool = True
    run_id: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    count: int = Field(ge=0)
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)
    has_more: bool = False
    lagging_count: int = Field(default=0, ge=0)
    over_limit_count: int = Field(default=0, ge=0)
    uncheckpointed_count: int = Field(default=0, ge=0)
    max_lag: int = Field(default=0, ge=0)
    filters: dict[str, Any] = Field(default_factory=dict)
    items: list[RuntimeStreamLagItem] = Field(default_factory=list)


class RuntimeStepSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    capability: RuntimeId | None = None
    operation_type: RuntimeId | None = None
    needs: list[str] = Field(default_factory=list)
    adapter_kind: AdapterKind | None = None
    priority: int = 0
    max_concurrency: int | None = None
    resource_pool: RuntimeId = "default"
    resources: ResourceSpec = Field(default_factory=ResourceSpec)
    sla: ProcessSlaSpec = Field(default_factory=ProcessSlaSpec)
    status: ProcessStatus | Literal["unknown"]
    has_claim: bool = False
    claim: ProcessClaim | None = None
    has_output: bool = False
    output_value_keys: list[str] = Field(default_factory=list)
    artifact_count: int = Field(default=0, ge=0)
    output_document_count: int = Field(default=0, ge=0)
    metadata_keys: list[str] = Field(default_factory=list)
    streams: list[RuntimeStreamSnapshot] = Field(default_factory=list)
    stream_count: int = Field(default=0, ge=0)
    stream_chunk_count: int = Field(default=0, ge=0)
    stream_artifact_count: int = Field(default=0, ge=0)
    stream_checkpoint_count: int = Field(default=0, ge=0)


class RuntimeProcessRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    document_id: str
    document_title: str | None = None
    document_type: RuntimeId | None = None
    document_relation: RuntimeId | None = None
    parent_document_id: str | None = None
    pipeline_id: RuntimeId | None = None
    process_id: RuntimeId
    title: str | None = None
    capability: RuntimeId | None = None
    operation_type: RuntimeId | None = None
    adapter_kind: AdapterKind | None = None
    priority: int = 0
    max_concurrency: int | None = None
    resource_pool: RuntimeId = "default"
    resources: ResourceSpec = Field(default_factory=ResourceSpec)
    sla: ProcessSlaSpec = Field(default_factory=ProcessSlaSpec)
    status: ProcessStatus | Literal["unknown"]
    has_claim: bool = False
    worker_id: str | None = None
    attempt: int | None = None
    claim_expires_at: datetime | None = None
    has_output: bool = False
    output_value_keys: list[str] = Field(default_factory=list)
    artifact_count: int = Field(default=0, ge=0)
    output_document_count: int = Field(default=0, ge=0)
    metadata_keys: list[str] = Field(default_factory=list)
    stream_count: int = Field(default=0, ge=0)
    stream_chunk_count: int = Field(default=0, ge=0)
    stream_artifact_count: int = Field(default=0, ge=0)
    stream_checkpoint_count: int = Field(default=0, ge=0)
    status_updated_at: datetime | None = None


class RuntimeProcessPage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool = True
    run_id: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    count: int = Field(ge=0)
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)
    has_more: bool = False
    filters: dict[str, Any] = Field(default_factory=dict)
    processes: list[RuntimeProcessRecord] = Field(default_factory=list)


class RuntimeStepReportItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    document_id: str
    document_title: str | None = None
    document_type: RuntimeId | None = None
    document_relation: RuntimeId | None = None
    parent_document_id: str | None = None
    pipeline_id: RuntimeId | None = None
    process_id: RuntimeId
    position: int = Field(ge=0)
    title: str | None = None
    capability: RuntimeId | None = None
    operation_type: RuntimeId | None = None
    adapter_kind: AdapterKind | None = None
    priority: int = 0
    resource_pool: RuntimeId = "default"
    status: ProcessStatus | Literal["unknown"]
    status_category: Literal["waiting", "queued", "running", "terminal", "unknown"]
    needs: list[str] = Field(default_factory=list)
    blocked_by: list[str] = Field(default_factory=list)
    is_blocked: bool = False
    is_active: bool = False
    is_terminal: bool = False
    has_claim: bool = False
    worker_id: str | None = None
    attempt: int | None = None
    claim_expires_at: datetime | None = None
    has_output: bool = False
    output_value_keys: list[str] = Field(default_factory=list)
    artifact_count: int = Field(default=0, ge=0)
    output_document_count: int = Field(default=0, ge=0)
    metadata_keys: list[str] = Field(default_factory=list)
    stream_count: int = Field(default=0, ge=0)
    stream_chunk_count: int = Field(default=0, ge=0)
    stream_artifact_count: int = Field(default=0, ge=0)
    stream_checkpoint_count: int = Field(default=0, ge=0)


class RuntimeDocumentStepReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    document_id: str
    document_title: str | None = None
    document_type: RuntimeId | None = None
    document_relation: RuntimeId | None = None
    parent_document_id: str | None = None
    parent_process_id: RuntimeId | None = None
    child_document_ids: list[str] = Field(default_factory=list)
    child_document_count: int = Field(default=0, ge=0)
    pipeline_id: RuntimeId | None = None
    process_count: int = Field(default=0, ge=0)
    terminal_process_count: int = Field(default=0, ge=0)
    active_process_count: int = Field(default=0, ge=0)
    blocked_process_count: int = Field(default=0, ge=0)
    completed_process_count: int = Field(default=0, ge=0)
    failed_process_count: int = Field(default=0, ge=0)
    skipped_process_count: int = Field(default=0, ge=0)
    cancelled_process_count: int = Field(default=0, ge=0)
    status_counts: dict[str, int] = Field(default_factory=dict)
    progress: float = Field(default=0.0, ge=0.0, le=1.0)
    steps: list[RuntimeStepReportItem] = Field(default_factory=list)


class RuntimeStepReportSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    document_count: int = Field(default=0, ge=0)
    process_count: int = Field(default=0, ge=0)
    terminal_process_count: int = Field(default=0, ge=0)
    active_process_count: int = Field(default=0, ge=0)
    blocked_process_count: int = Field(default=0, ge=0)
    completed_process_count: int = Field(default=0, ge=0)
    failed_process_count: int = Field(default=0, ge=0)
    skipped_process_count: int = Field(default=0, ge=0)
    cancelled_process_count: int = Field(default=0, ge=0)
    status_counts: dict[str, int] = Field(default_factory=dict)
    pipeline_counts: dict[str, int] = Field(default_factory=dict)
    operation_type_counts: dict[str, int] = Field(default_factory=dict)
    claim_count: int = Field(default=0, ge=0)
    output_count: int = Field(default=0, ge=0)
    artifact_count: int = Field(default=0, ge=0)
    output_document_count: int = Field(default=0, ge=0)
    stream_chunk_count: int = Field(default=0, ge=0)
    progress: float = Field(default=0.0, ge=0.0, le=1.0)


class RuntimeStepReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool = True
    run_id: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    summary: RuntimeStepReportSummary = Field(default_factory=RuntimeStepReportSummary)
    documents: list[RuntimeDocumentStepReport] = Field(default_factory=list)
    steps: list[RuntimeStepReportItem] = Field(default_factory=list)


class RuntimeDeadLetterItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    document_id: str
    process_id: RuntimeId
    pipeline_id: RuntimeId | None = None
    document_title: str | None = None
    document_type: RuntimeId | None = None
    title: str | None = None
    capability: RuntimeId | None = None
    operation_type: RuntimeId | None = None
    adapter_kind: AdapterKind | None = None
    resource_pool: RuntimeId = "default"
    status: ProcessStatus
    status_updated_at: datetime | None = None
    dead_lettered_at: datetime | None = None
    last_event_id: str | None = None
    last_event_type: str | None = None
    last_event_at: datetime | None = None
    reason: str | None = None
    error_kind: str | None = None
    terminal_reason: str | None = None
    retry_allowed: bool | None = None
    worker_id: str | None = None
    attempt: int | None = None
    max_attempts: int | None = None
    suggested_actions: list[ProcessAction] = Field(default_factory=list)
    event_data: dict[str, Any] = Field(default_factory=dict)
    process: RuntimeProcessRecord


class RuntimeDeadLetterPage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool = True
    run_id: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    count: int = Field(ge=0)
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)
    has_more: bool = False
    filters: dict[str, Any] = Field(default_factory=dict)
    items: list[RuntimeDeadLetterItem] = Field(default_factory=list)


class RuntimeStuckWorkItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    document_id: str
    process_id: RuntimeId
    pipeline_id: RuntimeId | None = None
    document_title: str | None = None
    document_type: RuntimeId | None = None
    title: str | None = None
    capability: RuntimeId | None = None
    operation_type: RuntimeId | None = None
    adapter_kind: AdapterKind | None = None
    resource_pool: RuntimeId = "default"
    status: ProcessStatus
    reason: str
    severity: Literal["warning", "critical"] = "warning"
    status_since: datetime | None = None
    status_age_seconds: float | None = Field(default=None, ge=0)
    threshold_seconds: float | None = Field(default=None, ge=0)
    claim_expires_at: datetime | None = None
    retry_after: datetime | None = None
    last_event_id: str | None = None
    last_event_type: str | None = None
    last_event_at: datetime | None = None
    worker_id: str | None = None
    attempt: int | None = None
    suggested_actions: list[ProcessAction] = Field(default_factory=list)
    data: dict[str, Any] = Field(default_factory=dict)
    process: RuntimeProcessRecord


class RuntimeStuckWorkPage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool = True
    run_id: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    count: int = Field(ge=0)
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)
    has_more: bool = False
    critical_count: int = Field(default=0, ge=0)
    warning_count: int = Field(default=0, ge=0)
    filters: dict[str, Any] = Field(default_factory=dict)
    items: list[RuntimeStuckWorkItem] = Field(default_factory=list)


class RuntimeDocumentState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    document_id: str
    pipeline_id: str | None = None
    relation: RuntimeId | None = None
    parent_document_id: str | None = None
    parent_process_id: RuntimeId | None = None
    child_document_ids: list[str] = Field(default_factory=list)
    child_document_count: int = Field(default=0, ge=0)
    document: RuntimeDocument | None = None
    steps: list[RuntimeStepSnapshot] = Field(default_factory=list)
    statuses: dict[str, ProcessStatus] = Field(default_factory=dict)
    claims: dict[str, ProcessClaim] = Field(default_factory=dict)
    outputs: dict[str, ProcessOutput] = Field(default_factory=dict)
    projections: dict[str, CombinedProjection] = Field(default_factory=dict)
    events: list[ProcessEvent] = Field(default_factory=list)
    event_count: int = Field(default=0, ge=0)


class RuntimeStateSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    document_count: int = Field(default=0, ge=0)
    root_document_count: int = Field(default=0, ge=0)
    child_document_count: int = Field(default=0, ge=0)
    spawned_document_count: int = Field(default=0, ge=0)
    process_count: int = Field(default=0, ge=0)
    status_counts: dict[str, int] = Field(default_factory=dict)
    pipeline_counts: dict[str, int] = Field(default_factory=dict)
    operation_type_counts: dict[str, int] = Field(default_factory=dict)
    claim_count: int = Field(default=0, ge=0)
    output_count: int = Field(default=0, ge=0)
    output_document_count: int = Field(default=0, ge=0)
    projection_count: int = Field(default=0, ge=0)
    artifact_count: int = Field(default=0, ge=0)
    stream_count: int = Field(default=0, ge=0)
    stream_chunk_count: int = Field(default=0, ge=0)
    stream_artifact_count: int = Field(default=0, ge=0)
    stream_checkpoint_count: int = Field(default=0, ge=0)
    event_count: int = Field(default=0, ge=0)


class RuntimeState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    summary: RuntimeStateSummary = Field(default_factory=RuntimeStateSummary)
    documents: list[RuntimeDocumentState] = Field(default_factory=list)


class RuntimeDocumentLineageNode(BaseModel):
    model_config = ConfigDict(extra="forbid")

    document_id: str
    pipeline_id: str | None = None
    title: str | None = None
    document_type: RuntimeId | None = None
    relation: RuntimeId | None = None
    media_type: str | None = None
    source_uri: str | None = None
    status: RuntimeDocumentStatus | Literal["unknown"] = "unknown"
    parent_document_id: str | None = None
    parent_process_id: RuntimeId | None = None
    child_document_ids: list[str] = Field(default_factory=list)
    process_count: int = Field(default=0, ge=0)
    output_count: int = Field(default=0, ge=0)
    event_count: int = Field(default=0, ge=0)


class RuntimeDocumentLineageEdge(BaseModel):
    model_config = ConfigDict(extra="forbid")

    parent_document_id: str
    child_document_id: str
    parent_process_id: RuntimeId | None = None
    relation: RuntimeId | None = None
    child_pipeline_id: RuntimeId | None = None


class RuntimeDocumentLineage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    root_document_ids: list[str] = Field(default_factory=list)
    node_count: int = Field(default=0, ge=0)
    edge_count: int = Field(default=0, ge=0)
    nodes: list[RuntimeDocumentLineageNode] = Field(default_factory=list)
    edges: list[RuntimeDocumentLineageEdge] = Field(default_factory=list)


class RuntimeRunResultItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    document_id: str
    pipeline_id: str | None = None
    process_id: RuntimeId
    operation_type: RuntimeId | None = None
    status: ProcessStatus | Literal["unknown"] = "unknown"
    title: str | None = None
    document_type: RuntimeId | None = None
    document_relation: RuntimeId | None = None
    media_type: str | None = None
    source_uri: str | None = None
    parent_document_id: str | None = None
    parent_process_id: RuntimeId | None = None
    child_document_ids: list[str] = Field(default_factory=list)
    output: ProcessOutput
    value_keys: list[str] = Field(default_factory=list)
    artifact_count: int = Field(default=0, ge=0)
    output_document_count: int = Field(default=0, ge=0)
    metadata_keys: list[str] = Field(default_factory=list)
    lineage: dict[str, Any] = Field(default_factory=dict)


class RuntimeRunResults(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    count: int = Field(default=0, ge=0)
    filters: dict[str, Any] = Field(default_factory=dict)
    results: list[RuntimeRunResultItem] = Field(default_factory=list)


class RuntimeOutputDocumentItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    source_document_id: str
    pipeline_id: RuntimeId | None = None
    process_id: RuntimeId
    status: ProcessStatus | Literal["unknown"] = "unknown"
    source_title: str | None = None
    source_document_type: RuntimeId | None = None
    source_document_relation: RuntimeId | None = None
    source_media_type: str | None = None
    source_uri: str | None = None
    parent_document_id: str | None = None
    parent_process_id: RuntimeId | None = None
    child_document_ids: list[str] = Field(default_factory=list)
    output_document_id: RuntimeId
    title: str | None = None
    document_type: RuntimeId
    media_type: str | None = None
    uri: str | None = None
    artifact_id: RuntimeId | None = None
    relation: RuntimeId = "derived"
    output_document: OutputDocumentRef
    artifact: ArtifactRef | None = None
    value_keys: list[str] = Field(default_factory=list)
    metadata_keys: list[str] = Field(default_factory=list)
    lineage: dict[str, Any] = Field(default_factory=dict)


class RuntimeOutputDocumentPage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ok: bool = True
    run_id: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    count: int = Field(ge=0)
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)
    has_more: bool = False
    filters: dict[str, Any] = Field(default_factory=dict)
    output_documents: list[RuntimeOutputDocumentItem] = Field(default_factory=list)


class RuntimeRunReduction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    run_id: str
    pipeline_id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    mode: Literal["collect_values", "collect_outputs", "count"]
    filters: dict[str, Any] = Field(default_factory=dict)
    result_count: int = Field(default=0, ge=0)
    output: ProcessOutput
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class RuntimeRunReductions(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    count: int = Field(default=0, ge=0)
    reductions: list[RuntimeRunReduction] = Field(default_factory=list)


class RuntimeArtifactBlob(BaseModel):
    model_config = ConfigDict(extra="forbid")

    digest: str
    uri: str
    path: str
    size_bytes: int = Field(default=0, ge=0)
    referenced: bool = False
    deleted: bool = False


class RuntimeArtifactGcPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    root: str
    dry_run: bool = True
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    referenced_digest_count: int = Field(default=0, ge=0)
    blob_count: int = Field(default=0, ge=0)
    referenced_blob_count: int = Field(default=0, ge=0)
    orphaned_blob_count: int = Field(default=0, ge=0)
    deleted_blob_count: int = Field(default=0, ge=0)
    total_bytes: int = Field(default=0, ge=0)
    referenced_bytes: int = Field(default=0, ge=0)
    orphaned_bytes: int = Field(default=0, ge=0)
    deleted_bytes: int = Field(default=0, ge=0)
    orphaned_blobs: list[RuntimeArtifactBlob] = Field(default_factory=list)


class RuntimeRunRetentionItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    status: RunStatus | Literal["unknown"] = "unknown"
    title: str | None = None
    updated_at: datetime | None = None
    finished_at: datetime | None = None
    matched: bool = False
    deleted: bool = False
    reason: str | None = None
    row_counts: dict[str, int] = Field(default_factory=dict)


class RuntimeRunRetentionPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dry_run: bool = True
    before: datetime
    statuses: list[RunStatus] = Field(default_factory=list)
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    candidate_count: int = Field(default=0, ge=0)
    deleted_run_count: int = Field(default=0, ge=0)
    row_counts: dict[str, int] = Field(default_factory=dict)
    runs: list[RuntimeRunRetentionItem] = Field(default_factory=list)


class RuntimeProcessMetrics(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_id: str | None = None
    process_id: RuntimeId
    title: str | None = None
    capability: RuntimeId | None = None
    operation_type: RuntimeId | None = None
    adapter_kind: AdapterKind | None = None
    priority: int = 0
    max_concurrency: int | None = None
    resource_pool: RuntimeId = "default"
    resources: ResourceSpec = Field(default_factory=ResourceSpec)
    counts: dict[str, int] = Field(default_factory=dict)
    waiting_count: int = Field(default=0, ge=0)
    queued_count: int = Field(default=0, ge=0)
    running_count: int = Field(default=0, ge=0)
    completed_count: int = Field(default=0, ge=0)
    failed_count: int = Field(default=0, ge=0)
    skipped_count: int = Field(default=0, ge=0)
    cancelled_count: int = Field(default=0, ge=0)
    retry_backoff_count: int = Field(default=0, ge=0)
    missing_worker_count: int = Field(default=0, ge=0)
    resource_blocked_count: int = Field(default=0, ge=0)
    claim_count: int = Field(default=0, ge=0)
    output_count: int = Field(default=0, ge=0)
    matching_worker_count: int = Field(default=0, ge=0)
    healthy_worker_count: int = Field(default=0, ge=0)
    capacity: int | None = None
    capacity_remaining: int | None = None
    saturated: bool = False
    missing_worker: bool = False
    resource_blocked: bool = False
    next_retry_after: datetime | None = None
    next_retry_after_document_id: str | None = None
    oldest_queued_at: datetime | None = None
    oldest_queued_document_id: str | None = None
    oldest_running_at: datetime | None = None
    oldest_running_document_id: str | None = None
    last_event_at: datetime | None = None


class RuntimeResourcePoolMetrics(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    limit: ResourceSpec = Field(default_factory=ResourceSpec)
    used: ResourceQuantity = Field(default_factory=ResourceQuantity)
    remaining: ResourceQuantity = Field(default_factory=ResourceQuantity)
    running_count: int = Field(default=0, ge=0)
    queued_count: int = Field(default=0, ge=0)
    saturated: bool = False


class RuntimeWorkerDemand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_id: str | None = None
    process_id: RuntimeId
    title: str | None = None
    capability: RuntimeId | None = None
    operation_type: RuntimeId | None = None
    adapter_kind: AdapterKind | None = None
    resource_pool: RuntimeId = "default"
    resources: ResourceSpec = Field(default_factory=ResourceSpec)
    queued_count: int = Field(default=0, ge=0)
    running_count: int = Field(default=0, ge=0)
    resource_blocked_count: int = Field(default=0, ge=0)
    claimable_queued_count: int = Field(default=0, ge=0)
    matching_worker_count: int = Field(default=0, ge=0)
    healthy_worker_count: int = Field(default=0, ge=0)
    target_worker_count: int = Field(default=0, ge=0)
    worker_deficit_count: int = Field(default=0, ge=0)
    capacity: int | None = None
    capacity_remaining: int | None = None
    package_worker_ids: list[RuntimeId] = Field(default_factory=list)


class RuntimeCapabilityDemand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    capability: RuntimeId | None = None
    operation_type: RuntimeId | None = None
    adapter_kind: AdapterKind | None = None
    resource_pool: RuntimeId = "default"
    pipeline_ids: list[str] = Field(default_factory=list)
    process_ids: list[RuntimeId] = Field(default_factory=list)
    process_group_count: int = Field(default=0, ge=0)
    queued_count: int = Field(default=0, ge=0)
    running_count: int = Field(default=0, ge=0)
    resource_blocked_count: int = Field(default=0, ge=0)
    claimable_queued_count: int = Field(default=0, ge=0)
    matching_worker_count: int = Field(default=0, ge=0)
    healthy_worker_count: int = Field(default=0, ge=0)
    target_worker_count: int = Field(default=0, ge=0)
    worker_deficit_count: int = Field(default=0, ge=0)
    package_worker_ids: list[RuntimeId] = Field(default_factory=list)
    processes: list[RuntimeWorkerDemand] = Field(default_factory=list)


class RuntimeCapabilityDemandSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    count: int = Field(default=0, ge=0)
    queued_count: int = Field(default=0, ge=0)
    running_count: int = Field(default=0, ge=0)
    resource_blocked_count: int = Field(default=0, ge=0)
    claimable_queued_count: int = Field(default=0, ge=0)
    target_worker_count: int = Field(default=0, ge=0)
    worker_deficit_count: int = Field(default=0, ge=0)
    demands: list[RuntimeCapabilityDemand] = Field(default_factory=list)


class RuntimeQueueMetrics(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    document_count: int = Field(default=0, ge=0)
    process_group_count: int = Field(default=0, ge=0)
    process_instance_count: int = Field(default=0, ge=0)
    waiting_count: int = Field(default=0, ge=0)
    queued_count: int = Field(default=0, ge=0)
    running_count: int = Field(default=0, ge=0)
    failed_count: int = Field(default=0, ge=0)
    retry_backoff_count: int = Field(default=0, ge=0)
    missing_worker_count: int = Field(default=0, ge=0)
    missing_worker_process_count: int = Field(default=0, ge=0)
    resource_blocked_count: int = Field(default=0, ge=0)
    resource_blocked_process_count: int = Field(default=0, ge=0)
    saturated_process_count: int = Field(default=0, ge=0)
    worker_demand_process_count: int = Field(default=0, ge=0)
    worker_deficit_count: int = Field(default=0, ge=0)
    resource_pools: list[RuntimeResourcePoolMetrics] = Field(default_factory=list)
    worker_demands: list[RuntimeWorkerDemand] = Field(default_factory=list)
    processes: list[RuntimeProcessMetrics] = Field(default_factory=list)


class RuntimeRunHealthIssue(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: RuntimeId
    severity: Literal["info", "warning", "critical"]
    message: str
    count: int = Field(default=1, ge=1)
    pipeline_id: str | None = None
    process_id: RuntimeId | None = None
    operation_type: RuntimeId | None = None
    document_id: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)


class RuntimeRunHealth(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    status: Literal["healthy", "warning", "critical"] = "healthy"
    issue_count: int = Field(default=0, ge=0)
    critical_count: int = Field(default=0, ge=0)
    warning_count: int = Field(default=0, ge=0)
    worker_count: int = Field(default=0, ge=0)
    healthy_worker_count: int = Field(default=0, ge=0)
    stale_worker_count: int = Field(default=0, ge=0)
    metrics: RuntimeQueueMetrics
    issues: list[RuntimeRunHealthIssue] = Field(default_factory=list)


class RuntimeAttemptTrace(BaseModel):
    model_config = ConfigDict(extra="forbid")

    attempt: int | None = None
    worker_id: str | None = None
    status: ProcessStatus | str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
    duration_seconds: float | None = None
    event_count: int = Field(default=0, ge=0)
    event_types: list[str] = Field(default_factory=list)
    events: list[ProcessEvent] = Field(default_factory=list)


class RuntimeProcessTrace(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    document_id: str
    pipeline_id: str | None = None
    process_id: RuntimeId
    title: str | None = None
    capability: RuntimeId | None = None
    operation_type: RuntimeId | None = None
    adapter_kind: AdapterKind | None = None
    status: ProcessStatus | Literal["unknown"]
    current_claim: ProcessClaim | None = None
    has_output: bool = False
    output_value_keys: list[str] = Field(default_factory=list)
    event_count: int = Field(default=0, ge=0)
    attempt_count: int = Field(default=0, ge=0)
    attempts: list[RuntimeAttemptTrace] = Field(default_factory=list)


class RuntimeTrace(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    generated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    document_id: str | None = None
    process_id: RuntimeId | None = None
    operation_type: RuntimeId | None = None
    process_count: int = Field(default=0, ge=0)
    attempt_count: int = Field(default=0, ge=0)
    event_count: int = Field(default=0, ge=0)
    processes: list[RuntimeProcessTrace] = Field(default_factory=list)
