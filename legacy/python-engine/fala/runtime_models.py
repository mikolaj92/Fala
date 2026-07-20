"""Domain models and enums for the Impulse runtime.

Extracted from runtime_backend for maintainability. Public imports remain
stable via ``fala.runtime_backend`` re-exports.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:12]}"


def _now() -> datetime:
    return datetime.now(timezone.utc)


class RunStatus(StrEnum):
    created = "created"
    active = "active"
    waiting = "waiting"
    completed = "completed"
    failed = "failed"
    cancel_requested = "cancel_requested"
    cancelled = "cancelled"
    timed_out = "timed_out"


class Run(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: _new_id("run"))
    status: RunStatus = RunStatus.created
    title: str | None = None
    package_id: str | None = None
    package_version: str | None = None
    package_digest: str | None = None
    correlation_path_id: str | None = None
    correlation_path_digest: str | None = None
    runtime_version: str | None = None
    backend_version: str | None = None
    schema_version: int = Field(default=1, ge=1)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)
    started_at: datetime | None = None
    finished_at: datetime | None = None


class RuntimeRunRetentionItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    status: RunStatus
    created_at: datetime
    updated_at: datetime
    finished_at: datetime | None = None
    deleted: bool = False
    row_counts: dict[str, int] = Field(default_factory=dict)


class RuntimeRunRetentionPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dry_run: bool = True
    before: datetime
    statuses: list[RunStatus] = Field(default_factory=list)
    generated_at: datetime = Field(default_factory=_now)
    candidate_count: int = Field(default=0, ge=0)
    deleted_run_count: int = Field(default=0, ge=0)
    row_counts: dict[str, int] = Field(default_factory=dict)
    runs: list[RuntimeRunRetentionItem] = Field(default_factory=list)


class RuntimeReactionGcPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dry_run: bool = True
    reaction_root: str | None = None
    generated_at: datetime = Field(default_factory=_now)
    referenced_count: int = Field(default=0, ge=0)
    blob_count: int = Field(default=0, ge=0)
    candidate_count: int = Field(default=0, ge=0)
    deleted_count: int = Field(default=0, ge=0)
    bytes_reclaimable: int = Field(default=0, ge=0)
    bytes_reclaimed: int = Field(default=0, ge=0)
    candidates: list[str] = Field(default_factory=list)
    deleted: list[str] = Field(default_factory=list)


class RuntimeJournalMaintenancePlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    dry_run: bool = True
    older_than_days: float
    keep_last: int | None = None
    vacuum: bool = True
    generated_at: datetime = Field(default_factory=_now)
    retention: RuntimeRunRetentionPlan | None = None
    reaction_gc: RuntimeReactionGcPlan | None = None
    vacuum_result: dict[str, Any] | None = None
    runs_archived: int = Field(default=0, ge=0)
    bytes_reclaimed: int = Field(default=0, ge=0)


@dataclass(frozen=True)
class RuntimeReactionBlob:
    digest: str
    size_bytes: int
    location: str | None = None


@dataclass(frozen=True)
class RuntimeReactionStore:
    root: Path | None = None
    blobs: dict[str, RuntimeReactionBlob] = field(default_factory=dict)


class Impulse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: _new_id("impulse"))
    run_id: str
    impulse_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)


class ImpulseType(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    run_id: str
    title: str | None = None
    description: str | None = None
    media_types: list[str] = Field(default_factory=list)
    value_schema: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)


class ImpulseRelation(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: _new_id("impulse_relation"))
    run_id: str
    relation_type: str
    source_impulse_id: str
    target_impulse_id: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_now)


class RuntimeCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: _new_id("command"))
    run_id: str
    command_type: str
    idempotency_key: str
    actor: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_now)


class RuntimeEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: _new_id("event"))
    run_id: str
    event_type: str
    schema_version: int = Field(default=1, ge=1)
    impulse_id: str | None = None
    process_id: str | None = None
    sequence: int | None = None
    command_id: str | None = None
    actor: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_now)


class CommandSubmission(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command: RuntimeCommand
    events: list[RuntimeEvent] = Field(default_factory=list)
    replayed: bool = False


class Association(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: _new_id("association"))
    run_id: str
    kind: str
    impulse_id: str | None = None
    values: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_now)


class Reaction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: _new_id("reaction"))
    run_id: str
    kind: str
    uri: str
    impulse_id: str | None = None
    media_type: str | None = None
    size_bytes: int | None = Field(default=None, ge=0)
    content_hash: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_now)


class ProcessStatus(StrEnum):
    pending = "pending"
    ready = "ready"
    running = "running"
    waiting = "waiting"
    retry_wait = "retry_wait"
    succeeded = "succeeded"
    failed = "failed"
    cancel_requested = "cancel_requested"
    cancelled = "cancelled"
    timed_out = "timed_out"


class Process(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: _new_id("process"))
    run_id: str
    process_type: str
    impulse_id: str | None = None
    status: ProcessStatus = ProcessStatus.pending
    priority: int = 0
    attempt: int = Field(default=0, ge=0)
    max_attempts: int = Field(default=1, ge=1)
    available_at: datetime = Field(default_factory=_now)
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    input: dict[str, Any] = Field(default_factory=dict)
    output: dict[str, Any] = Field(default_factory=dict)
    error: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)
    started_at: datetime | None = None
    finished_at: datetime | None = None
    output_schema: dict[str, Any] = Field(default_factory=dict)


class HomeostatStatus(StrEnum):
    open = "open"
    completed = "completed"
    cancelled = "cancelled"
    expired = "expired"


class Homeostat(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: _new_id("homeostat"))
    run_id: str
    kind: str
    impulse_id: str | None = None
    status: HomeostatStatus = HomeostatStatus.open
    values: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)
    attempt: int = Field(default=0, ge=0)
    max_attempts: int = Field(default=1, ge=1)
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)


class WaitDiagnosticIssue(BaseModel):
    model_config = ConfigDict(extra="forbid")

    process_id: str
    status: ProcessStatus | None = None
    reason: str
    blocked_by: list[str] = Field(default_factory=list)
    dependency_statuses: dict[str, str | None] = Field(default_factory=dict)
    data: dict[str, Any] = Field(default_factory=dict)


class WaitGraphDiagnostic(BaseModel):
    model_config = ConfigDict(extra="forbid")

    run_id: str
    impulse_id: str | None = None
    deadlocked: bool = False
    deadlocks: list[list[str]] = Field(default_factory=list)
    wait_edges: dict[str, list[str]] = Field(default_factory=dict)
    blocked: list[WaitDiagnosticIssue] = Field(default_factory=list)
    open_homeostats: list[str] = Field(default_factory=list)
    pending: list[str] = Field(default_factory=list)
    ready: list[str] = Field(default_factory=list)
    running: list[str] = Field(default_factory=list)
    waiting: list[str] = Field(default_factory=list)
    retry_wait: list[str] = Field(default_factory=list)
    succeeded: list[str] = Field(default_factory=list)
    failed: list[str] = Field(default_factory=list)
    cancel_requested: list[str] = Field(default_factory=list)
    cancelled: list[str] = Field(default_factory=list)
    timed_out: list[str] = Field(default_factory=list)


class Projection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: _new_id("projection"))
    run_id: str
    name: str
    version: int = 1
    data: dict[str, Any] = Field(default_factory=dict)
    source_event_sequence: int = 0
    updated_at: datetime = Field(default_factory=_now)
    # Freshness watermark, stamped at read time by the service (never
    # persisted): True when events past source_event_sequence exist.
    stale: bool = False


class RunBoundary(BaseModel):
    """What one runtime may know about another run: derived status + counters.

    The boundary association is computed from the run's own journal, so a
    parent runtime closing a delegation loop reads the child's outcome without
    trusting a manually maintained status row.
    """

    model_config = ConfigDict(extra="forbid")

    run_id: str
    status: RunStatus
    derived_status: RunStatus
    process_status_counts: dict[str, int] = Field(default_factory=dict)
    event_watermark: int = 0


class RuntimeRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    uri: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class RunRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    runtime: RuntimeRef
    run_id: str


class EventRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    runtime: RuntimeRef
    run_id: str
    event_id: str | None = None
    sequence: int | None = Field(default=None, ge=1)


class RuntimeBudget(BaseModel):
    """Delegation budget. ``None`` means unlimited; ``0`` means exhausted."""

    model_config = ConfigDict(extra="forbid")

    runtime_hops: int | None = Field(default=None, ge=0)
    spawned_runs: int | None = Field(default=None, ge=0)
    impulse_count: int | None = Field(default=None, ge=0)
    wall_time_seconds: float | None = Field(default=None, ge=0)
    attempts: int | None = Field(default=None, ge=0)
    reaction_bytes: int | None = Field(default=None, ge=0)


class RuntimePool(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    runtimes: list[RuntimeRef] = Field(default_factory=list)
    impulse_types: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class DelegationPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: _new_id("delegation_policy"))
    pool_id: str
    impulse_types: list[str] = Field(default_factory=list)
    budget: RuntimeBudget = Field(default_factory=RuntimeBudget)
    metadata: dict[str, Any] = Field(default_factory=dict)


class BridgeDeliveryStatus(StrEnum):
    pending = "pending"
    delivered = "delivered"
    imported = "imported"
    failed = "failed"


class BridgeDelivery(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=lambda: _new_id("bridge"))
    run_id: str
    idempotency_key: str = Field(default_factory=lambda: _new_id("bridge_key"))
    source: RunRef
    target: RunRef
    impulse: Impulse
    event_ref: EventRef | None = None
    pool_id: str | None = None
    budget: RuntimeBudget = Field(default_factory=RuntimeBudget)
    status: BridgeDeliveryStatus = BridgeDeliveryStatus.pending
    attempts: int = Field(default=0, ge=0)
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=_now)
    updated_at: datetime = Field(default_factory=_now)


