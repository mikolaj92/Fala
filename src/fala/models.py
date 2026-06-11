from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator

AdapterKind = Literal["subprocess", "http", "queue"]
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
    values: dict[str, Any] = Field(default_factory=dict)


class ProcessOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    artifacts: list[ArtifactRef] = Field(default_factory=list)
    values: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


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


class ProcessSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    adapter: AdapterSpec
    needs: list[str] = Field(default_factory=list)
    timeout_seconds: float | None = Field(default=None, gt=0)
    retry: RetryPolicy = Field(default_factory=RetryPolicy)
    config: dict[str, Any] = Field(default_factory=dict)


class CombineSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    needs: list[RuntimeId]
    mode: Literal["latest"] = "latest"
    emit_partial: bool = False


class WorkItemPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    claim_strategy: Literal["parallel", "sequential"] = "parallel"
    order_by: str = Field(default="index", min_length=1)


class PipelineSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    version: str = "1"
    input_values: dict[str, str] = Field(default_factory=dict)
    work_items: WorkItemPolicy = Field(default_factory=WorkItemPolicy)
    steps: list[ProcessSpec]
    combines: list[CombineSpec] = Field(default_factory=list)

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

        _validate_acyclic(self.steps)
        return self


class WorkflowWorkerSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    pipeline_id: RuntimeId
    process_id: RuntimeId | None = None
    adapter_kind: Literal["queue"] = "queue"
    command: list[str] = Field(min_length=1)
    cwd: str | None = None
    env: dict[str, str] = Field(default_factory=dict)
    timeout_seconds: float | None = Field(default=None, gt=0)


class WorkflowPackageSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    version: str = "1"
    pipelines: list[str] = Field(min_length=1)
    workers: list[WorkflowWorkerSpec] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_pipeline_paths(self) -> "WorkflowPackageSpec":
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


class ProcessExecutionContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pipeline_id: RuntimeId
    run_id: str
    document_id: str
    process_id: RuntimeId
    attempt: int
    input: ProcessInput
    config: dict[str, Any] = Field(default_factory=dict)


class ProcessEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId = Field(default_factory=lambda: new_id("event"))
    run_id: str
    document_id: str
    process_id: RuntimeId | None
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
    count: int = Field(ge=0)
    has_more: bool
    next_after_event_id: RuntimeId | None = None
    events: list[ProcessEvent]


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


class RuntimeStepSnapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: RuntimeId
    title: str | None = None
    description: str | None = None
    tags: list[str] = Field(default_factory=list)
    needs: list[str] = Field(default_factory=list)
    adapter_kind: AdapterKind | None = None
    status: ProcessStatus | Literal["unknown"]
    has_claim: bool = False
    claim: ProcessClaim | None = None
    has_output: bool = False
    output_value_keys: list[str] = Field(default_factory=list)
    artifact_count: int = Field(default=0, ge=0)
    metadata_keys: list[str] = Field(default_factory=list)


class RuntimeDocumentState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    document_id: str
    pipeline_id: str | None = None
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
    process_count: int = Field(default=0, ge=0)
    status_counts: dict[str, int] = Field(default_factory=dict)
    pipeline_counts: dict[str, int] = Field(default_factory=dict)
    claim_count: int = Field(default=0, ge=0)
    output_count: int = Field(default=0, ge=0)
    projection_count: int = Field(default=0, ge=0)
    artifact_count: int = Field(default=0, ge=0)
    event_count: int = Field(default=0, ge=0)


class RuntimeState(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    summary: RuntimeStateSummary = Field(default_factory=RuntimeStateSummary)
    documents: list[RuntimeDocumentState] = Field(default_factory=list)
