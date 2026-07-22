from __future__ import annotations

import asyncio
import json
import sqlite3
from contextlib import closing
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field
from jsonschema import Draft202012Validator, ValidationError as JsonSchemaValidationError

from fala.reactions import content_address_json
from fala.errors import FalaBudgetExceeded, FalaValidationError


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid4().hex[:12]}"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _dumps(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _loads(value: str) -> dict[str, Any]:
    loaded = json.loads(value)
    if not isinstance(loaded, dict):
        raise ValueError("Stored runtime JSON payload is not an object")
    return loaded


def _loads_str_list(value: str) -> list[str]:
    loaded = json.loads(value)
    if not isinstance(loaded, list) or not all(isinstance(item, str) for item in loaded):
        raise ValueError("Stored runtime JSON payload is not a string list")
    return loaded


def _loads_runtime_refs(value: str) -> list["RuntimeRef"]:
    loaded = json.loads(value)
    if not isinstance(loaded, list):
        raise ValueError("Stored runtime JSON payload is not a runtime ref list")
    return [RuntimeRef.model_validate(item) for item in loaded]


def _dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _ensure_runtime_event_columns(connection: sqlite3.Connection) -> None:
    columns = {
        row[1]
        for row in connection.execute("PRAGMA table_info(runtime_events)").fetchall()
    }
    if "process_id" not in columns:
        connection.execute("ALTER TABLE runtime_events ADD COLUMN process_id TEXT")
    if "schema_version" not in columns:
        connection.execute(
            "ALTER TABLE runtime_events ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1"
        )


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


class RuntimeBackend(Protocol):
    async def create_run(
        self,
        run: Run,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission: ...

    async def put_run(self, run: Run) -> None: ...

    async def transition_run(
        self,
        *,
        run_id: str,
        status: RunStatus,
        command: RuntimeCommand,
        events: Sequence[RuntimeEvent] = (),
    ) -> tuple[Run, CommandSubmission]: ...

    async def get_run(self, *, run_id: str) -> Run | None: ...

    async def list_runs(
        self,
        *,
        status: RunStatus | None = None,
        limit: int | None = None,
    ) -> list[Run]: ...

    async def delete_run(self, *, run_id: str) -> dict[str, int]: ...

    async def vacuum(self) -> dict[str, Any]: ...

    async def put_runtime_pool(self, pool: RuntimePool) -> None: ...

    async def get_runtime_pool(self, *, pool_id: str) -> RuntimePool | None: ...

    async def list_runtime_pools(self) -> list[RuntimePool]: ...

    async def put_delegation_policy(self, policy: DelegationPolicy) -> None: ...

    async def get_delegation_policy(
        self, *, policy_id: str
    ) -> DelegationPolicy | None: ...

    async def list_delegation_policies(
        self, *, pool_id: str | None = None
    ) -> list[DelegationPolicy]: ...

    async def put_impulse_type(self, impulse_type: ImpulseType) -> None: ...

    async def register_impulse_type(
        self,
        impulse_type: ImpulseType,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission: ...

    async def get_impulse_type(
        self, *, run_id: str, impulse_type_id: str
    ) -> ImpulseType | None: ...

    async def list_impulse_types(self, *, run_id: str) -> list[ImpulseType]: ...

    async def put_impulse(self, impulse: Impulse) -> None: ...

    async def accept_impulse(
        self,
        impulse: Impulse,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission: ...

    async def get_impulse(self, *, run_id: str, impulse_id: str) -> Impulse | None: ...

    async def list_impulses(
        self,
        *,
        run_id: str,
        impulse_type: str | None = None,
        limit: int | None = None,
    ) -> list[Impulse]: ...

    async def put_impulse_relation(self, relation: ImpulseRelation) -> None: ...

    async def record_impulse_relation(
        self,
        relation: ImpulseRelation,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission: ...

    async def get_impulse_relation(
        self, *, run_id: str, relation_id: str
    ) -> ImpulseRelation | None: ...

    async def list_impulse_relations(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
        relation_type: str | None = None,
    ) -> list[ImpulseRelation]: ...

    async def submit_command(
        self, command: RuntimeCommand, *, events: Sequence[RuntimeEvent] = ()
    ) -> CommandSubmission: ...

    async def get_command_by_idempotency(
        self, *, run_id: str, idempotency_key: str
    ) -> RuntimeCommand | None: ...

    async def get_command(
        self, *, run_id: str, command_id: str
    ) -> RuntimeCommand | None: ...

    async def list_commands(
        self,
        *,
        run_id: str,
        command_type: str | None = None,
        actor: str | None = None,
        limit: int | None = None,
    ) -> list[RuntimeCommand]: ...

    async def list_events(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
        after_sequence: int | None = None,
        limit: int | None = None,
    ) -> list[RuntimeEvent]: ...

    async def put_association(self, association: Association) -> None: ...

    async def record_association(
        self,
        association: Association,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission: ...

    async def list_associations(
        self, *, run_id: str, impulse_id: str | None = None
    ) -> list[Association]: ...

    async def put_reaction(self, reaction: Reaction) -> None: ...

    async def record_reaction(
        self,
        reaction: Reaction,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission: ...

    async def get_reaction(self, *, run_id: str, reaction_id: str) -> Reaction | None: ...

    async def list_reactions(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
        kind: str | None = None,
    ) -> list[Reaction]: ...

    async def put_process(self, process: Process) -> None: ...

    async def schedule_process(
        self,
        process: Process,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission: ...

    async def get_process(self, *, run_id: str, process_id: str) -> Process | None: ...

    async def list_processes(
        self,
        *,
        run_id: str,
        status: ProcessStatus | None = None,
        impulse_id: str | None = None,
    ) -> list[Process]: ...

    async def claim_next_ready_process(
        self,
        *,
        worker_id: str,
        run_id: str | None = None,
        lease_seconds: float = 300.0,
        all_runs: bool = False,
    ) -> Process | None: ...

    async def complete_process(
        self,
        *,
        run_id: str,
        process_id: str,
        output: dict[str, Any] | None = None,
    ) -> Process: ...

    async def fail_process(
        self,
        *,
        run_id: str,
        process_id: str,
        error: dict[str, Any] | None = None,
    ) -> Process: ...

    async def retry_process(
        self,
        *,
        run_id: str,
        process_id: str,
        available_at: datetime | None = None,
        error: dict[str, Any] | None = None,
    ) -> Process: ...

    async def transition_process(
        self,
        *,
        run_id: str,
        process_id: str,
        status: ProcessStatus,
        command: RuntimeCommand,
        events: Sequence[RuntimeEvent] = (),
        output: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        available_at: datetime | None = None,
        input: dict[str, Any] | None = None,
    ) -> tuple[Process, CommandSubmission]: ...

    async def cancel_process(
        self,
        *,
        run_id: str,
        process_id: str,
        error: dict[str, Any] | None = None,
    ) -> Process: ...

    async def timeout_process(
        self,
        *,
        run_id: str,
        process_id: str,
        error: dict[str, Any] | None = None,
    ) -> Process: ...

    async def put_homeostat(self, homeostat: Homeostat) -> None: ...

    async def save_homeostat(
        self,
        homeostat: Homeostat,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission: ...

    async def get_homeostat(self, *, run_id: str, homeostat_id: str) -> Homeostat | None: ...

    async def transition_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        status: HomeostatStatus,
        command: RuntimeCommand,
        events: Sequence[RuntimeEvent] = (),
        values: dict[str, Any] | None = None,
    ) -> tuple[Homeostat, CommandSubmission]: ...

    async def complete_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        values: dict[str, Any] | None = None,
    ) -> Homeostat: ...

    async def cancel_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        values: dict[str, Any] | None = None,
    ) -> Homeostat: ...

    async def expire_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        values: dict[str, Any] | None = None,
    ) -> Homeostat: ...

    async def list_homeostats(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
        status: HomeostatStatus | None = None,
    ) -> list[Homeostat]: ...

    async def put_projection(self, projection: Projection) -> None: ...

    async def save_projection(
        self,
        projection: Projection,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission: ...

    async def get_projection(self, *, run_id: str, name: str) -> Projection | None: ...

    async def list_projections(self, *, run_id: str) -> list[Projection]: ...

    async def rebuild_projections(
        self,
        *,
        run_id: str,
        names: Sequence[str] | None = None,
    ) -> list[Projection]: ...

    async def rebuild_projections_with_command(
        self,
        *,
        run_id: str,
        names: Sequence[str] | None,
        command: RuntimeCommand,
        events: Sequence[RuntimeEvent] = (),
    ) -> tuple[list[Projection], CommandSubmission]: ...

    async def put_outbox_delivery(self, delivery: BridgeDelivery) -> None: ...

    async def enqueue_outbox_delivery(
        self,
        delivery: BridgeDelivery,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission: ...

    async def deliver_outbox_delivery(
        self,
        delivery: BridgeDelivery,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission: ...

    async def get_outbox_delivery(
        self, *, run_id: str, delivery_id: str
    ) -> BridgeDelivery | None: ...

    async def list_outbox_deliveries(
        self,
        *,
        run_id: str,
        status: BridgeDeliveryStatus | None = None,
    ) -> list[BridgeDelivery]: ...

    async def put_inbox_delivery(self, delivery: BridgeDelivery) -> None: ...

    async def import_inbox_delivery(
        self,
        delivery: BridgeDelivery,
        impulse: Impulse,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission: ...

    async def get_inbox_delivery(
        self, *, run_id: str, delivery_id: str
    ) -> BridgeDelivery | None: ...

    async def list_inbox_deliveries(
        self,
        *,
        run_id: str,
        status: BridgeDeliveryStatus | None = None,
    ) -> list[BridgeDelivery]: ...


_BRIDGE_TABLES = {"bridge_outbox", "bridge_inbox"}
_BUILT_IN_PROJECTIONS = ("run_summary",)
_SQLITE_SCHEMA_VERSION = 6
SQLITE_RUNTIME_SCHEMA_VERSION = _SQLITE_SCHEMA_VERSION
_TERMINAL_RUN_STATUSES = {
    RunStatus.completed,
    RunStatus.failed,
    RunStatus.cancelled,
    RunStatus.timed_out,
}
_TERMINAL_PROCESS_STATUSES = {
    ProcessStatus.succeeded,
    ProcessStatus.failed,
    ProcessStatus.cancelled,
    ProcessStatus.timed_out,
}
def _derive_boundary_status(
    status: RunStatus,
    processes: Sequence[Process],
) -> RunStatus:
    """Derive a run's observable status from its process statuses.

    The stored run row can lag reality (nobody called set_run_status); the
    processes cannot. A terminal stored status stays authoritative, otherwise
    any failed-like effector fails the run, all-succeeded completes it, and any
    waiting effector suspends it.
    """
    if status in _TERMINAL_RUN_STATUSES:
        return status
    failure_statuses = _TERMINAL_PROCESS_STATUSES - {ProcessStatus.succeeded}
    if any(process.status in failure_statuses for process in processes):
        return RunStatus.failed
    if processes and all(
        process.status == ProcessStatus.succeeded for process in processes
    ):
        return RunStatus.completed
    if any(
        process.status == ProcessStatus.waiting for process in processes
    ):
        return RunStatus.waiting
    return status


_PROCESS_TRANSITION_COMMANDS = {
    ProcessStatus.ready: "process.ready",
    ProcessStatus.succeeded: "process.complete",
    ProcessStatus.failed: "process.fail",
    ProcessStatus.retry_wait: "process.retry",
    ProcessStatus.waiting: "process.wait",
    ProcessStatus.cancelled: "process.cancel",
    ProcessStatus.timed_out: "process.timeout",
}
_HOMEOSTAT_TRANSITION_COMMANDS = {
    HomeostatStatus.completed: "homeostat.complete",
    HomeostatStatus.cancelled: "homeostat.cancel",
    HomeostatStatus.expired: "homeostat.expire",
}
_RUN_STATUS_TRANSITIONS = {
    RunStatus.created: {
        RunStatus.active,
        RunStatus.waiting,
        RunStatus.completed,
        RunStatus.failed,
        RunStatus.cancel_requested,
        RunStatus.cancelled,
        RunStatus.timed_out,
    },
    RunStatus.active: {
        RunStatus.waiting,
        RunStatus.completed,
        RunStatus.failed,
        RunStatus.cancel_requested,
        RunStatus.cancelled,
        RunStatus.timed_out,
    },
    RunStatus.waiting: {
        RunStatus.active,
        RunStatus.completed,
        RunStatus.failed,
        RunStatus.cancel_requested,
        RunStatus.cancelled,
        RunStatus.timed_out,
    },
    RunStatus.cancel_requested: {
        RunStatus.cancelled,
        RunStatus.failed,
        RunStatus.timed_out,
    },
}


class Correlator:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=30.0,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 30000")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _init_schema(self) -> None:
        with closing(self._connect()) as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    title TEXT,
                    package_id TEXT,
                    package_version TEXT,
                    package_digest TEXT,
                    correlation_path_id TEXT,
                    correlation_path_digest TEXT,
                    runtime_version TEXT,
                    backend_version TEXT,
                    schema_version INTEGER NOT NULL,
                    metadata TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT
                );
        
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    id TEXT PRIMARY KEY,
                    version INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    applied_at TEXT NOT NULL
                );
        
                CREATE TABLE IF NOT EXISTS impulses (
                    run_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    impulse_type TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, id)
                );
        
                CREATE TABLE IF NOT EXISTS impulse_types (
                    run_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    title TEXT,
                    description TEXT,
                    media_types TEXT NOT NULL,
                    value_schema_json TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, id)
                );
        
                CREATE TABLE IF NOT EXISTS impulse_relations (
                    run_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    relation_type TEXT NOT NULL,
                    source_impulse_id TEXT NOT NULL,
                    target_impulse_id TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, id),
                    FOREIGN KEY (run_id, source_impulse_id)
                        REFERENCES impulses (run_id, id),
                    FOREIGN KEY (run_id, target_impulse_id)
                        REFERENCES impulses (run_id, id)
                );
        
                CREATE TABLE IF NOT EXISTS runtime_commands (
                    run_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    command_type TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    actor TEXT,
                    correlation_id TEXT,
                    causation_id TEXT,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, id),
                    UNIQUE (run_id, idempotency_key)
                );
        
                CREATE TABLE IF NOT EXISTS runtime_events (
                    run_id TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    schema_version INTEGER NOT NULL DEFAULT 1,
                    impulse_id TEXT,
                    process_id TEXT,
                    command_id TEXT,
                    actor TEXT,
                    correlation_id TEXT,
                    causation_id TEXT,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, sequence),
                    UNIQUE (run_id, id),
                    FOREIGN KEY (run_id, command_id)
                        REFERENCES runtime_commands (run_id, id)
                );
        
                CREATE TABLE IF NOT EXISTS associations (
                    run_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    impulse_id TEXT,
                    values_json TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, id)
                );
        
                CREATE TABLE IF NOT EXISTS reactions (
                    run_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    uri TEXT NOT NULL,
                    impulse_id TEXT,
                    media_type TEXT,
                    size_bytes INTEGER,
                    content_hash TEXT,
                    metadata TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, id),
                    FOREIGN KEY (run_id, impulse_id)
                        REFERENCES impulses (run_id, id)
                );
        
                CREATE TABLE IF NOT EXISTS processes (
                    run_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    process_type TEXT NOT NULL,
                    impulse_id TEXT,
                    status TEXT NOT NULL,
                    priority INTEGER NOT NULL,
                    attempt INTEGER NOT NULL,
                    max_attempts INTEGER NOT NULL,
                    available_at TEXT NOT NULL,
                    lease_owner TEXT,
                    lease_expires_at TEXT,
                    input_json TEXT NOT NULL,
                    output_json TEXT NOT NULL,
                    error_json TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    output_schema_json TEXT NOT NULL DEFAULT '{}',
                    PRIMARY KEY (run_id, id),
                    FOREIGN KEY (run_id, impulse_id)
                        REFERENCES impulses (run_id, id)
                );
        
                CREATE TABLE IF NOT EXISTS homeostats (
                    run_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    impulse_id TEXT,
                    status TEXT NOT NULL,
                    values_json TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, id)
                );
        
                CREATE TABLE IF NOT EXISTS projections (
                    run_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    id TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    source_event_sequence INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, name)
                );
        
                CREATE TABLE IF NOT EXISTS bridge_outbox (
                    run_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    source_ref TEXT NOT NULL,
                    target_ref TEXT NOT NULL,
                    impulse_json TEXT NOT NULL,
                    event_ref TEXT,
                    pool_id TEXT,
                    budget TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL,
                    metadata TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, id),
                    UNIQUE (run_id, idempotency_key)
                );
        
                CREATE TABLE IF NOT EXISTS bridge_inbox (
                    run_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    source_ref TEXT NOT NULL,
                    target_ref TEXT NOT NULL,
                    impulse_json TEXT NOT NULL,
                    event_ref TEXT,
                    pool_id TEXT,
                    budget TEXT NOT NULL,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL,
                    metadata TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (run_id, id),
                    UNIQUE (run_id, idempotency_key)
                );
        
                CREATE INDEX IF NOT EXISTS idx_runtime_events_impulse
                    ON runtime_events (run_id, impulse_id, sequence);
                CREATE INDEX IF NOT EXISTS idx_runs_status
                    ON runs (status, updated_at);
        
                CREATE TABLE IF NOT EXISTS runtime_pools (
                    id TEXT PRIMARY KEY,
                    runtimes_json TEXT NOT NULL,
                    impulse_types TEXT NOT NULL,
                    metadata TEXT NOT NULL
                );
        
                CREATE TABLE IF NOT EXISTS delegation_policies (
                    id TEXT PRIMARY KEY,
                    pool_id TEXT NOT NULL,
                    impulse_types TEXT NOT NULL,
                    budget TEXT NOT NULL,
                    metadata TEXT NOT NULL,
                    FOREIGN KEY (pool_id)
                        REFERENCES runtime_pools (id)
                );
        
                CREATE INDEX IF NOT EXISTS idx_delegation_policies_pool
                    ON delegation_policies (pool_id, id);
        
                CREATE INDEX IF NOT EXISTS idx_impulse_relations_source
                    ON impulse_relations (run_id, source_impulse_id, relation_type);
                CREATE INDEX IF NOT EXISTS idx_impulse_relations_target
                    ON impulse_relations (run_id, target_impulse_id, relation_type);
                CREATE INDEX IF NOT EXISTS idx_associations_impulse
                    ON associations (run_id, impulse_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_reactions_impulse
                    ON reactions (run_id, impulse_id, kind, created_at);
                CREATE INDEX IF NOT EXISTS idx_processes_ready
                    ON processes (status, available_at, priority, created_at);
                CREATE INDEX IF NOT EXISTS idx_processes_run_status
                    ON processes (run_id, status, updated_at);
                CREATE INDEX IF NOT EXISTS idx_processes_impulse
                    ON processes (run_id, impulse_id, status);
                CREATE INDEX IF NOT EXISTS idx_homeostats_status
                    ON homeostats (run_id, status, updated_at);
                CREATE INDEX IF NOT EXISTS idx_bridge_outbox_status
                    ON bridge_outbox (run_id, status, updated_at);
                CREATE INDEX IF NOT EXISTS idx_bridge_inbox_status
                    ON bridge_inbox (run_id, status, updated_at);
                """
            )
            _ensure_runtime_event_columns(connection)
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_runtime_events_process
                    ON runtime_events (run_id, process_id, sequence)
                """
            )
            connection.executescript(
                """
                CREATE TRIGGER IF NOT EXISTS runtime_events_no_update
                BEFORE UPDATE ON runtime_events
                BEGIN
                    SELECT RAISE(ABORT, 'runtime_events is append-only');
                END;
        
                CREATE TRIGGER IF NOT EXISTS runtime_events_no_delete
                BEFORE DELETE ON runtime_events
                BEGIN
                    SELECT RAISE(ABORT, 'runtime_events is append-only');
                END;
        
                CREATE TRIGGER IF NOT EXISTS runtime_commands_no_update
                BEFORE UPDATE ON runtime_commands
                BEGIN
                    SELECT RAISE(ABORT, 'runtime_commands is append-only');
                END;
        
                CREATE TRIGGER IF NOT EXISTS runtime_commands_no_delete
                BEFORE DELETE ON runtime_commands
                BEGIN
                    SELECT RAISE(ABORT, 'runtime_commands is append-only');
                END;
                """
            )
            connection.execute(
                """
                INSERT INTO schema_migrations (id, version, name, applied_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    version = excluded.version,
                    name = excluded.name
                """,
                (
                    "runtime_backend",
                    _SQLITE_SCHEMA_VERSION,
                    "runtime_backend",
                    _now().isoformat(),
                ),
            )
            connection.commit()

    async def create_run(
        self,
        run: Run,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != run.id:
            raise ValueError("run.create command run_id must match run id")
        if command.command_type != "run.create":
            raise ValueError("create_run requires command_type 'run.create'")
        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    """
                    SELECT * FROM runtime_commands
                    WHERE run_id = ? AND idempotency_key = ?
                    """,
                    (command.run_id, command.idempotency_key),
                ).fetchone()
                if existing is not None:
                    connection.commit()
                    return CommandSubmission(
                        command=_command_from_row(existing),
                        events=[],
                        replayed=True,
                    )
                if (
                    connection.execute(
                        "SELECT 1 FROM runs WHERE id = ?",
                        (run.id,),
                    ).fetchone()
                    is not None
                ):
                    raise ValueError(f"Run already exists: {run.id!r}")

                _insert_run_row(connection, run)
                _insert_runtime_command_row(connection, command)
                stored_events = _append_runtime_events(connection, command, events)
                connection.commit()
                return CommandSubmission(
                    command=command,
                    events=stored_events,
                    replayed=False,
                )
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def put_run(self, run: Run) -> None:
        async with self._lock:
            with closing(self._connect()) as connection:
                connection.execute(
                    """
                    INSERT INTO runs (
                        id, status, title, package_id, package_version,
                        package_digest, correlation_path_id, correlation_path_digest, runtime_version,
                        backend_version, schema_version, metadata, created_at,
                        updated_at, started_at, finished_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        status = excluded.status,
                        title = excluded.title,
                        package_id = excluded.package_id,
                        package_version = excluded.package_version,
                        package_digest = excluded.package_digest,
                        correlation_path_id = excluded.correlation_path_id,
                        correlation_path_digest = excluded.correlation_path_digest,
                        runtime_version = excluded.runtime_version,
                        backend_version = excluded.backend_version,
                        schema_version = excluded.schema_version,
                        metadata = excluded.metadata,
                        created_at = excluded.created_at,
                        updated_at = excluded.updated_at,
                        started_at = excluded.started_at,
                        finished_at = excluded.finished_at
                    """,
                    _run_args(run),
                )
                connection.commit()

    async def transition_run(
        self,
        *,
        run_id: str,
        status: RunStatus,
        command: RuntimeCommand,
        events: Sequence[RuntimeEvent] = (),
    ) -> tuple[Run, CommandSubmission]:
        if command.run_id != run_id:
            raise ValueError("run transition command run_id must match run_id")
        if command.command_type == "run.cancel":
            if status != RunStatus.cancel_requested:
                raise ValueError("run.cancel requires status 'cancel_requested'")
        elif command.command_type != "run.status.set":
            raise ValueError(
                "transition_run requires command_type 'run.status.set' or 'run.cancel'"
            )

        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing_command = connection.execute(
                    """
                    SELECT * FROM runtime_commands
                    WHERE run_id = ? AND idempotency_key = ?
                    """,
                    (command.run_id, command.idempotency_key),
                ).fetchone()
                if existing_command is not None:
                    stored_command = _command_from_row(existing_command)
                    stored_run_id = stored_command.payload.get("run_id", run_id)
                    stored_run = connection.execute(
                        "SELECT * FROM runs WHERE id = ?",
                        (str(stored_run_id),),
                    ).fetchone()
                    if stored_run is None:
                        raise ValueError(
                            "Replayed run transition has no stored run: "
                            f"{stored_run_id!r}"
                        )
                    connection.commit()
                    return (
                        _run_from_row(stored_run),
                        CommandSubmission(
                            command=stored_command,
                            events=[],
                            replayed=True,
                        ),
                    )

                row = connection.execute(
                    "SELECT * FROM runs WHERE id = ?",
                    (run_id,),
                ).fetchone()
                if row is None:
                    raise ValueError(f"Unknown run: {run_id!r}")
                run = _run_from_row(row)
                if run.status != status:
                    _validate_run_status_transition(run.status, status)

                now = _now()
                started_at = run.started_at or (
                    now if status == RunStatus.active else None
                )
                finished_at = (
                    now if status in _TERMINAL_RUN_STATUSES else run.finished_at
                )
                _insert_runtime_command_row(connection, command)
                connection.execute(
                    """
                    UPDATE runs
                    SET status = ?,
                        updated_at = ?,
                        started_at = ?,
                        finished_at = ?
                    WHERE id = ?
                    """,
                    (
                        status.value,
                        now.isoformat(),
                        started_at.isoformat() if started_at is not None else None,
                        finished_at.isoformat() if finished_at is not None else None,
                        run_id,
                    ),
                )
                updated = connection.execute(
                    "SELECT * FROM runs WHERE id = ?",
                    (run_id,),
                ).fetchone()
                stored_events = _append_runtime_events(connection, command, events)
                connection.commit()
                return (
                    _run_from_row(updated),
                    CommandSubmission(
                        command=command,
                        events=stored_events,
                        replayed=False,
                    ),
                )
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def get_run(self, *, run_id: str) -> Run | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM runs WHERE id = ?",
                (run_id,),
            ).fetchone()
        return _run_from_row(row) if row is not None else None

    async def list_runs(
        self,
        *,
        status: RunStatus | None = None,
        limit: int | None = None,
    ) -> list[Run]:
        clauses: list[str] = []
        params: list[Any] = []
        if status is not None:
            clauses.append("status = ?")
            params.append(status.value)
        sql = "SELECT * FROM runs"
        if clauses:
            sql += f" WHERE {' AND '.join(clauses)}"
        sql += " ORDER BY created_at ASC, id ASC"
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with closing(self._connect()) as connection:
            rows = connection.execute(sql, params).fetchall()
        return [_run_from_row(row) for row in rows]

    async def delete_run(self, *, run_id: str) -> dict[str, int]:
        tables = [
            "bridge_inbox",
            "bridge_outbox",
            "projections",
            "homeostats",
            "processes",
            "reactions",
            "associations",
            "impulse_relations",
            "impulse_types",
            "impulses",
            "runtime_events",
            "runtime_commands",
            "runs",
        ]
        counts: dict[str, int] = {}
        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                connection.execute("DROP TRIGGER IF EXISTS runtime_events_no_delete")
                connection.execute("DROP TRIGGER IF EXISTS runtime_commands_no_delete")
                for table in tables:
                    column = "id" if table == "runs" else "run_id"
                    cursor = connection.execute(f"DELETE FROM {table} WHERE {column} = ?", (run_id,))
                    counts[table] = int(cursor.rowcount if cursor.rowcount is not None else 0)
                connection.commit()
                return counts
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()
                self._init_schema()

    async def vacuum(self) -> dict[str, Any]:
        before_size = self.path.stat().st_size if self.path.exists() else 0
        async with self._lock:
            with closing(self._connect()) as connection:
                before_pages = int(connection.execute("PRAGMA page_count").fetchone()[0])
                before_free = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
                connection.execute("VACUUM")
                after_pages = int(connection.execute("PRAGMA page_count").fetchone()[0])
                after_free = int(connection.execute("PRAGMA freelist_count").fetchone()[0])
        after_size = self.path.stat().st_size if self.path.exists() else 0
        return {
            "store_type": "sqlite",
            "vacuumed": True,
            "path": str(self.path),
            "bytes_before": before_size,
            "bytes_after": after_size,
            "bytes_reclaimed": max(0, before_size - after_size),
            "before": {"page_count": before_pages, "freelist_count": before_free},
            "after": {"page_count": after_pages, "freelist_count": after_free},
        }

    async def put_runtime_pool(self, pool: RuntimePool) -> None:
        async with self._lock:
            with closing(self._connect()) as connection:
                connection.execute(
                    """
                    INSERT INTO runtime_pools (
                        id, runtimes_json, impulse_types, metadata
                    ) VALUES (?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        runtimes_json = excluded.runtimes_json,
                        impulse_types = excluded.impulse_types,
                        metadata = excluded.metadata
                    """,
                    _runtime_pool_args(pool),
                )
                connection.commit()

    async def get_runtime_pool(self, *, pool_id: str) -> RuntimePool | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM runtime_pools WHERE id = ?",
                (pool_id,),
            ).fetchone()
        return _runtime_pool_from_row(row) if row is not None else None

    async def list_runtime_pools(self) -> list[RuntimePool]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT * FROM runtime_pools
                ORDER BY id ASC
                """
            ).fetchall()
        return [_runtime_pool_from_row(row) for row in rows]

    async def put_delegation_policy(self, policy: DelegationPolicy) -> None:
        async with self._lock:
            with closing(self._connect()) as connection:
                connection.execute(
                    """
                    INSERT INTO delegation_policies (
                        id, pool_id, impulse_types, budget, metadata
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET
                        pool_id = excluded.pool_id,
                        impulse_types = excluded.impulse_types,
                        budget = excluded.budget,
                        metadata = excluded.metadata
                    """,
                    _delegation_policy_args(policy),
                )
                connection.commit()

    async def get_delegation_policy(
        self,
        *,
        policy_id: str,
    ) -> DelegationPolicy | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM delegation_policies WHERE id = ?",
                (policy_id,),
            ).fetchone()
        return _delegation_policy_from_row(row) if row is not None else None

    async def list_delegation_policies(
        self,
        *,
        pool_id: str | None = None,
    ) -> list[DelegationPolicy]:
        clauses: list[str] = []
        params: list[Any] = []
        if pool_id is not None:
            clauses.append("pool_id = ?")
            params.append(pool_id)
        sql = "SELECT * FROM delegation_policies"
        if clauses:
            sql += f" WHERE {' AND '.join(clauses)}"
        sql += " ORDER BY pool_id ASC, id ASC"
        with closing(self._connect()) as connection:
            rows = connection.execute(sql, params).fetchall()
        return [_delegation_policy_from_row(row) for row in rows]

    async def put_impulse_type(self, impulse_type: ImpulseType) -> None:
        async with self._lock:
            with closing(self._connect()) as connection:
                _require_run_row(connection, impulse_type.run_id)
                connection.execute(
                    """
                    INSERT INTO impulse_types (
                        run_id, id, title, description, media_types,
                        value_schema_json, metadata, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, id) DO UPDATE SET
                        title = excluded.title,
                        description = excluded.description,
                        media_types = excluded.media_types,
                        value_schema_json = excluded.value_schema_json,
                        metadata = excluded.metadata,
                        created_at = excluded.created_at,
                        updated_at = excluded.updated_at
                    """,
                    (
                        impulse_type.run_id,
                        impulse_type.id,
                        impulse_type.title,
                        impulse_type.description,
                        json.dumps(impulse_type.media_types),
                        _dumps(impulse_type.value_schema),
                        _dumps(impulse_type.metadata),
                        impulse_type.created_at.isoformat(),
                        impulse_type.updated_at.isoformat(),
                    ),
                )
                connection.commit()

    async def register_impulse_type(
        self,
        impulse_type: ImpulseType,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != impulse_type.run_id:
            raise ValueError(
                "impulse_type.register command run_id must match impulse type run_id"
            )
        if command.command_type != "impulse_type.register":
            raise ValueError(
                "register_impulse_type requires command_type 'impulse_type.register'"
            )
        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    """
                    SELECT * FROM runtime_commands
                    WHERE run_id = ? AND idempotency_key = ?
                    """,
                    (command.run_id, command.idempotency_key),
                ).fetchone()
                if existing is not None:
                    connection.commit()
                    return CommandSubmission(
                        command=_command_from_row(existing),
                        events=[],
                        replayed=True,
                    )
                _require_run_row(connection, impulse_type.run_id)
                if (
                    connection.execute(
                        "SELECT 1 FROM impulse_types WHERE run_id = ? AND id = ?",
                        (impulse_type.run_id, impulse_type.id),
                    ).fetchone()
                    is not None
                ):
                    raise ValueError(f"Impulse type already exists: {impulse_type.id!r}")

                _insert_runtime_command_row(connection, command)
                _insert_impulse_type_row(connection, impulse_type)
                stored_events = _append_runtime_events(connection, command, events)
                connection.commit()
                return CommandSubmission(
                    command=command,
                    events=stored_events,
                    replayed=False,
                )
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def get_impulse_type(
        self,
        *,
        run_id: str,
        impulse_type_id: str,
    ) -> ImpulseType | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM impulse_types WHERE run_id = ? AND id = ?",
                (run_id, impulse_type_id),
            ).fetchone()
        return _impulse_type_from_row(row) if row is not None else None

    async def list_impulse_types(self, *, run_id: str) -> list[ImpulseType]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT * FROM impulse_types
                WHERE run_id = ?
                ORDER BY id ASC
                """,
                (run_id,),
            ).fetchall()
        return [_impulse_type_from_row(row) for row in rows]

    async def put_impulse(self, impulse: Impulse) -> None:
        async with self._lock:
            with closing(self._connect()) as connection:
                _require_run_row(connection, impulse.run_id)
                connection.execute(
                    """
                    INSERT INTO impulses (
                        run_id, id, impulse_type, payload, metadata,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, id) DO UPDATE SET
                        impulse_type = excluded.impulse_type,
                        payload = excluded.payload,
                        metadata = excluded.metadata,
                        created_at = excluded.created_at,
                        updated_at = excluded.updated_at
                    """,
                    (
                        impulse.run_id,
                        impulse.id,
                        impulse.impulse_type,
                        _dumps(impulse.payload),
                        _dumps(impulse.metadata),
                        impulse.created_at.isoformat(),
                        impulse.updated_at.isoformat(),
                    ),
                )
                connection.commit()

    async def accept_impulse(
        self,
        impulse: Impulse,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != impulse.run_id:
            raise ValueError("impulse.accept command run_id must match impulse run_id")
        if command.command_type != "impulse.accept":
            raise ValueError("accept_impulse requires command_type 'impulse.accept'")
        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    """
                    SELECT * FROM runtime_commands
                    WHERE run_id = ? AND idempotency_key = ?
                    """,
                    (command.run_id, command.idempotency_key),
                ).fetchone()
                if existing is not None:
                    connection.commit()
                    return CommandSubmission(
                        command=_command_from_row(existing),
                        events=[],
                        replayed=True,
                    )
                _require_run_row(connection, impulse.run_id)
                if (
                    connection.execute(
                        "SELECT 1 FROM impulses WHERE run_id = ? AND id = ?",
                        (impulse.run_id, impulse.id),
                    ).fetchone()
                    is not None
                ):
                    raise ValueError(f"Impulse already exists: {impulse.id!r}")

                _insert_runtime_command_row(connection, command)
                _insert_impulse_row(connection, impulse)
                stored_events = _append_runtime_events(connection, command, events)
                connection.commit()
                return CommandSubmission(
                    command=command,
                    events=stored_events,
                    replayed=False,
                )
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def get_impulse(self, *, run_id: str, impulse_id: str) -> Impulse | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM impulses WHERE run_id = ? AND id = ?",
                (run_id, impulse_id),
            ).fetchone()
        return _impulse_from_row(row) if row is not None else None

    async def list_impulses(
        self,
        *,
        run_id: str,
        impulse_type: str | None = None,
        limit: int | None = None,
    ) -> list[Impulse]:
        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if impulse_type is not None:
            clauses.append("impulse_type = ?")
            params.append(impulse_type)
        sql = f"""
            SELECT * FROM impulses
            WHERE {' AND '.join(clauses)}
            ORDER BY created_at ASC, id ASC
        """
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with closing(self._connect()) as connection:
            rows = connection.execute(sql, params).fetchall()
        return [_impulse_from_row(row) for row in rows]

    async def put_impulse_relation(self, relation: ImpulseRelation) -> None:
        async with self._lock:
            with closing(self._connect()) as connection:
                _require_run_row(connection, relation.run_id)
                connection.execute(
                    """
                    INSERT INTO impulse_relations (
                        run_id, id, relation_type, source_impulse_id,
                        target_impulse_id, metadata, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, id) DO UPDATE SET
                        relation_type = excluded.relation_type,
                        source_impulse_id = excluded.source_impulse_id,
                        target_impulse_id = excluded.target_impulse_id,
                        metadata = excluded.metadata,
                        created_at = excluded.created_at
                    """,
                    (
                        relation.run_id,
                        relation.id,
                        relation.relation_type,
                        relation.source_impulse_id,
                        relation.target_impulse_id,
                        _dumps(relation.metadata),
                        relation.created_at.isoformat(),
                    ),
                )
                connection.commit()

    async def record_impulse_relation(
        self,
        relation: ImpulseRelation,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != relation.run_id:
            raise ValueError(
                "impulse_relation.record command run_id must match relation run_id"
            )
        if command.command_type != "impulse_relation.record":
            raise ValueError(
                "record_impulse_relation requires command_type "
                "'impulse_relation.record'"
            )
        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    """
                    SELECT * FROM runtime_commands
                    WHERE run_id = ? AND idempotency_key = ?
                    """,
                    (command.run_id, command.idempotency_key),
                ).fetchone()
                if existing is not None:
                    connection.commit()
                    return CommandSubmission(
                        command=_command_from_row(existing),
                        events=[],
                        replayed=True,
                    )
                _require_run_row(connection, relation.run_id)
                if (
                    connection.execute(
                        "SELECT 1 FROM impulse_relations WHERE run_id = ? AND id = ?",
                        (relation.run_id, relation.id),
                    ).fetchone()
                    is not None
                ):
                    raise ValueError(
                        f"Impulse relation already exists: {relation.id!r}"
                    )

                _insert_runtime_command_row(connection, command)
                _insert_impulse_relation_row(connection, relation)
                stored_events = _append_runtime_events(connection, command, events)
                connection.commit()
                return CommandSubmission(
                    command=command,
                    events=stored_events,
                    replayed=False,
                )
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def get_impulse_relation(
        self,
        *,
        run_id: str,
        relation_id: str,
    ) -> ImpulseRelation | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM impulse_relations WHERE run_id = ? AND id = ?",
                (run_id, relation_id),
            ).fetchone()
        return _impulse_relation_from_row(row) if row is not None else None

    async def list_impulse_relations(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
        relation_type: str | None = None,
    ) -> list[ImpulseRelation]:
        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if impulse_id is not None:
            clauses.append("(source_impulse_id = ? OR target_impulse_id = ?)")
            params.extend([impulse_id, impulse_id])
        if relation_type is not None:
            clauses.append("relation_type = ?")
            params.append(relation_type)
        with closing(self._connect()) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM impulse_relations
                WHERE {' AND '.join(clauses)}
                ORDER BY created_at ASC, id ASC
                """,
                params,
            ).fetchall()
        return [_impulse_relation_from_row(row) for row in rows]

    async def submit_command(
        self,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    """
                    SELECT * FROM runtime_commands
                    WHERE run_id = ? AND idempotency_key = ?
                    """,
                    (command.run_id, command.idempotency_key),
                ).fetchone()
                if existing is not None:
                    connection.commit()
                    return CommandSubmission(
                        command=_command_from_row(existing),
                        events=[],
                        replayed=True,
                    )
                if command.command_type == "run.create":
                    raise ValueError("run.create commands must use create_run")
                _require_run_row(connection, command.run_id)
                _insert_runtime_command_row(connection, command)
                stored_events = _append_runtime_events(connection, command, events)

                connection.commit()
                return CommandSubmission(
                    command=command,
                    events=stored_events,
                    replayed=False,
                )
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def get_command_by_idempotency(
        self, *, run_id: str, idempotency_key: str
    ) -> RuntimeCommand | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT * FROM runtime_commands
                WHERE run_id = ? AND idempotency_key = ?
                """,
                (run_id, idempotency_key),
            ).fetchone()
        return _command_from_row(row) if row is not None else None

    async def get_command(
        self, *, run_id: str, command_id: str
    ) -> RuntimeCommand | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                """
                SELECT * FROM runtime_commands
                WHERE run_id = ? AND id = ?
                """,
                (run_id, command_id),
            ).fetchone()
        return _command_from_row(row) if row is not None else None

    async def list_commands(
        self,
        *,
        run_id: str,
        command_type: str | None = None,
        actor: str | None = None,
        limit: int | None = None,
    ) -> list[RuntimeCommand]:
        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if command_type is not None:
            clauses.append("command_type = ?")
            params.append(command_type)
        if actor is not None:
            clauses.append("actor = ?")
            params.append(actor)
        sql = f"""
            SELECT * FROM runtime_commands
            WHERE {' AND '.join(clauses)}
            ORDER BY created_at ASC, id ASC
        """
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with closing(self._connect()) as connection:
            rows = connection.execute(sql, params).fetchall()
        return [_command_from_row(row) for row in rows]

    async def list_events(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
        after_sequence: int | None = None,
        limit: int | None = None,
    ) -> list[RuntimeEvent]:
        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if impulse_id is not None:
            clauses.append("impulse_id = ?")
            params.append(impulse_id)
        if after_sequence is not None:
            clauses.append("sequence > ?")
            params.append(after_sequence)
        sql = f"""
            SELECT * FROM runtime_events
            WHERE {' AND '.join(clauses)}
            ORDER BY sequence ASC
        """
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with closing(self._connect()) as connection:
            rows = connection.execute(sql, params).fetchall()
        return [_event_from_row(row) for row in rows]

    async def put_association(self, association: Association) -> None:
        async with self._lock:
            with closing(self._connect()) as connection:
                _require_run_row(connection, association.run_id)
                connection.execute(
                    """
                    INSERT INTO associations (
                        run_id, id, kind, impulse_id, values_json,
                        metadata, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, id) DO UPDATE SET
                        kind = excluded.kind,
                        impulse_id = excluded.impulse_id,
                        values_json = excluded.values_json,
                        metadata = excluded.metadata,
                        created_at = excluded.created_at
                    """,
                    (
                        association.run_id,
                        association.id,
                        association.kind,
                        association.impulse_id,
                        _dumps(association.values),
                        _dumps(association.metadata),
                        association.created_at.isoformat(),
                    ),
                )
                connection.commit()

    async def record_association(
        self,
        association: Association,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != association.run_id:
            raise ValueError(
                "association.record command run_id must match association run_id"
            )
        if command.command_type != "association.record":
            raise ValueError(
                "record_association requires command_type 'association.record'"
            )
        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    """
                    SELECT * FROM runtime_commands
                    WHERE run_id = ? AND idempotency_key = ?
                    """,
                    (command.run_id, command.idempotency_key),
                ).fetchone()
                if existing is not None:
                    connection.commit()
                    return CommandSubmission(
                        command=_command_from_row(existing),
                        events=[],
                        replayed=True,
                    )
                _require_run_row(connection, association.run_id)
                if (
                    connection.execute(
                        "SELECT 1 FROM associations WHERE run_id = ? AND id = ?",
                        (association.run_id, association.id),
                    ).fetchone()
                    is not None
                ):
                    raise ValueError(f"Association already exists: {association.id!r}")

                _insert_runtime_command_row(connection, command)
                _insert_association_row(connection, association)
                stored_events = _append_runtime_events(connection, command, events)
                connection.commit()
                return CommandSubmission(
                    command=command,
                    events=stored_events,
                    replayed=False,
                )
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def list_associations(
        self, *, run_id: str, impulse_id: str | None = None
    ) -> list[Association]:
        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if impulse_id is not None:
            clauses.append("impulse_id = ?")
            params.append(impulse_id)
        with closing(self._connect()) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM associations
                WHERE {' AND '.join(clauses)}
                ORDER BY created_at ASC, id ASC
                """,
                params,
            ).fetchall()
        return [_association_from_row(row) for row in rows]

    async def put_reaction(self, reaction: Reaction) -> None:
        async with self._lock:
            with closing(self._connect()) as connection:
                _require_run_row(connection, reaction.run_id)
                connection.execute(
                    """
                    INSERT INTO reactions (
                        run_id, id, kind, uri, impulse_id, media_type,
                        size_bytes, content_hash, metadata, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, id) DO NOTHING
                    """,
                    (
                        reaction.run_id,
                        reaction.id,
                        reaction.kind,
                        reaction.uri,
                        reaction.impulse_id,
                        reaction.media_type,
                        reaction.size_bytes,
                        reaction.content_hash,
                        _dumps(reaction.metadata),
                        reaction.created_at.isoformat(),
                    ),
                )
                connection.commit()

    async def record_reaction(
        self,
        reaction: Reaction,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != reaction.run_id:
            raise ValueError(
                "reaction.record command run_id must match reaction run_id"
            )
        if command.command_type != "reaction.record":
            raise ValueError("record_reaction requires command_type 'reaction.record'")
        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    """
                    SELECT * FROM runtime_commands
                    WHERE run_id = ? AND idempotency_key = ?
                    """,
                    (command.run_id, command.idempotency_key),
                ).fetchone()
                if existing is not None:
                    connection.commit()
                    return CommandSubmission(
                        command=_command_from_row(existing),
                        events=[],
                        replayed=True,
                    )
                _require_run_row(connection, reaction.run_id)
                if (
                    connection.execute(
                        "SELECT 1 FROM reactions WHERE run_id = ? AND id = ?",
                        (reaction.run_id, reaction.id),
                    ).fetchone()
                    is not None
                ):
                    raise ValueError(f"Reaction already exists: {reaction.id!r}")

                _insert_runtime_command_row(connection, command)
                _insert_reaction_row(connection, reaction)
                stored_events = _append_runtime_events(connection, command, events)
                connection.commit()
                return CommandSubmission(
                    command=command,
                    events=stored_events,
                    replayed=False,
                )
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def get_reaction(self, *, run_id: str, reaction_id: str) -> Reaction | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM reactions WHERE run_id = ? AND id = ?",
                (run_id, reaction_id),
            ).fetchone()
        return _reaction_from_row(row) if row is not None else None

    async def list_reactions(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
        kind: str | None = None,
    ) -> list[Reaction]:
        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if impulse_id is not None:
            clauses.append("impulse_id = ?")
            params.append(impulse_id)
        if kind is not None:
            clauses.append("kind = ?")
            params.append(kind)
        with closing(self._connect()) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM reactions
                WHERE {' AND '.join(clauses)}
                ORDER BY created_at ASC, id ASC
                """,
                params,
            ).fetchall()
        return [_reaction_from_row(row) for row in rows]

    async def put_process(self, process: Process) -> None:
        async with self._lock:
            with closing(self._connect()) as connection:
                _require_run_row(connection, process.run_id)
                connection.execute(
                    """
                    INSERT INTO processes (
                        run_id, id, process_type, impulse_id, status, priority,
                        attempt, max_attempts, available_at, lease_owner,
                        lease_expires_at, input_json, output_json, error_json,
                        metadata, output_schema_json, created_at, updated_at, started_at, finished_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, id) DO UPDATE SET
                        process_type = excluded.process_type,
                        impulse_id = excluded.impulse_id,
                        status = excluded.status,
                        priority = excluded.priority,
                        attempt = excluded.attempt,
                        max_attempts = excluded.max_attempts,
                        available_at = excluded.available_at,
                        lease_owner = excluded.lease_owner,
                        lease_expires_at = excluded.lease_expires_at,
                        input_json = excluded.input_json,
                        output_json = excluded.output_json,
                        error_json = excluded.error_json,
                        metadata = excluded.metadata,
                        output_schema_json = excluded.output_schema_json,
                        created_at = excluded.created_at,
                        updated_at = excluded.updated_at,
                        started_at = excluded.started_at,
                        finished_at = excluded.finished_at
                    """,
                    _process_args(process),
                )
                connection.commit()

    async def schedule_process(
        self,
        process: Process,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != process.run_id:
            raise ValueError(
                "process.schedule command run_id must match process run_id"
            )
        if command.command_type != "process.schedule":
            raise ValueError("schedule_process requires command_type 'process.schedule'")
        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    """
                    SELECT * FROM runtime_commands
                    WHERE run_id = ? AND idempotency_key = ?
                    """,
                    (command.run_id, command.idempotency_key),
                ).fetchone()
                if existing is not None:
                    connection.commit()
                    return CommandSubmission(
                        command=_command_from_row(existing),
                        events=[],
                        replayed=True,
                    )
                if process.status not in {
                    ProcessStatus.pending,
                    ProcessStatus.ready,
                }:
                    raise ValueError(
                        "schedule_process requires process status 'pending' or 'ready'"
                    )
                _require_run_row(connection, process.run_id)
                if (
                    connection.execute(
                        "SELECT 1 FROM processes WHERE run_id = ? AND id = ?",
                        (process.run_id, process.id),
                    ).fetchone()
                    is not None
                ):
                    raise ValueError(f"Process already exists: {process.id!r}")

                # Validate correlation_path marker if present (LUKA3: catch bad conduction metadata
                # at persistence boundary, not only at CorrelationPathSpec parse time).
                cp_marker = process.metadata.get("correlation_path")
                if isinstance(cp_marker, dict):
                    from fala.correlation_paths import _validate_correlation_path_marker
                    _validate_correlation_path_marker(cp_marker, process_id=process.id)
                _insert_runtime_command_row(connection, command)
                _insert_process_row(connection, process)
                stored_events = _append_runtime_events(connection, command, events)
                connection.commit()
                return CommandSubmission(
                    command=command,
                    events=stored_events,
                    replayed=False,
                )
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def get_process(self, *, run_id: str, process_id: str) -> Process | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM processes WHERE run_id = ? AND id = ?",
                (run_id, process_id),
            ).fetchone()
        return _process_from_row(row) if row is not None else None

    async def list_processes(
        self,
        *,
        run_id: str,
        status: ProcessStatus | None = None,
        impulse_id: str | None = None,
    ) -> list[Process]:
        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if status is not None:
            clauses.append("status = ?")
            params.append(status.value)
        if impulse_id is not None:
            clauses.append("impulse_id = ?")
            params.append(impulse_id)
        with closing(self._connect()) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM processes
                WHERE {' AND '.join(clauses)}
                ORDER BY priority DESC, available_at ASC, created_at ASC, id ASC
                """,
                params,
            ).fetchall()
        return [_process_from_row(row) for row in rows]

    async def claim_next_ready_process(
        self,
        *,
        worker_id: str,
        run_id: str | None = None,
        lease_seconds: float = 300.0,
        all_runs: bool = False,
    ) -> Process | None:
        if lease_seconds <= 0:
            raise ValueError("lease_seconds must be greater than zero")
        if run_id is None and not all_runs:
            raise ValueError(
                "claim_next_ready_process requires run_id, or all_runs=True to "
                "claim across every run"
            )
        now = _now()
        expires_at = now + timedelta(seconds=lease_seconds)
        clauses = [
            """
            (
                status = ?
                OR (status = ? AND available_at <= ?)
                OR (status = ? AND lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
            )
            """,
            "attempt < max_attempts",
        ]
        params: list[Any] = [
            ProcessStatus.ready.value,
            ProcessStatus.retry_wait.value,
            now.isoformat(),
            ProcessStatus.running.value,
            now.isoformat(),
        ]
        if run_id is not None:
            clauses.append("run_id = ?")
            params.append(run_id)
        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                # Homeostatic closure: a crashed process (expired lease) with no
                # attempts left can never be re-claimed; fail it here so the run
                # settles instead of hanging forever.
                expired_clauses = [
                    "status = ?",
                    "lease_expires_at IS NOT NULL",
                    "lease_expires_at <= ?",
                    "attempt >= max_attempts",
                ]
                expired_params: list[Any] = [
                    ProcessStatus.running.value,
                    now.isoformat(),
                ]
                if run_id is not None:
                    expired_clauses.append("run_id = ?")
                    expired_params.append(run_id)
                expired_rows = connection.execute(
                    f"SELECT * FROM processes WHERE {' AND '.join(expired_clauses)}",
                    expired_params,
                ).fetchall()
                for expired_row in expired_rows:
                    expired = _process_from_row(expired_row)
                    expired_error = {
                        "type": "lease_expired",
                        "message": "process lease expired with no attempts left",
                        "lease_owner": expired.lease_owner,
                    }
                    connection.execute(
                        """
                        UPDATE processes
                        SET status = ?,
                            lease_owner = NULL,
                            lease_expires_at = NULL,
                            error_json = ?,
                            updated_at = ?,
                            finished_at = ?
                        WHERE run_id = ? AND id = ?
                        """,
                        (
                            ProcessStatus.failed.value,
                            _dumps(expired_error),
                            now.isoformat(),
                            now.isoformat(),
                            expired.run_id,
                            expired.id,
                        ),
                    )
                    fail_command = RuntimeCommand(
                        run_id=expired.run_id,
                        command_type="process.fail",
                        idempotency_key=(
                            f"process.fail:{expired.id}:{expired.attempt}"
                        ),
                        actor=worker_id,
                        payload={"process_id": expired.id},
                    )
                    _insert_runtime_command_row(connection, fail_command)
                    _append_runtime_events(
                        connection,
                        fail_command,
                        [
                            RuntimeEvent(
                                run_id=expired.run_id,
                                impulse_id=expired.impulse_id,
                                process_id=expired.id,
                                event_type="process.failed",
                                payload={
                                    "process_id": expired.id,
                                    "attempt": expired.attempt,
                                    "input_digest": content_address_json(
                                        expired.input
                                    ),
                                    "error_digest": content_address_json(
                                        expired_error
                                    ),
                                },
                            )
                        ],
                    )
                row = connection.execute(
                    f"""
                    SELECT * FROM processes
                    WHERE {' AND '.join(clauses)}
                    ORDER BY priority DESC, available_at ASC, created_at ASC, id ASC
                    LIMIT 1
                    """,
                    params,
                ).fetchone()
                if row is None:
                    connection.commit()
                    return None
                connection.execute(
                    """
                    UPDATE processes
                    SET status = ?,
                        attempt = attempt + 1,
                        lease_owner = ?,
                        lease_expires_at = ?,
                        updated_at = ?,
                        started_at = COALESCE(started_at, ?)
                    WHERE run_id = ? AND id = ?
                    """,
                    (
                        ProcessStatus.running.value,
                        worker_id,
                        expires_at.isoformat(),
                        now.isoformat(),
                        now.isoformat(),
                        row["run_id"],
                        row["id"],
                    ),
                )
                updated = connection.execute(
                    "SELECT * FROM processes WHERE run_id = ? AND id = ?",
                    (row["run_id"], row["id"]),
                ).fetchone()
                claim_command = RuntimeCommand(
                    run_id=row["run_id"],
                    command_type="process.claim",
                    idempotency_key=(
                        f"process.claim:{row['id']}:{updated['attempt']}"
                    ),
                    actor=worker_id,
                    payload={
                        "process_id": row["id"],
                        "worker_id": worker_id,
                        "attempt": updated["attempt"],
                        "lease_expires_at": expires_at.isoformat(),
                    },
                )
                _insert_runtime_command_row(connection, claim_command)
                _append_runtime_events(
                    connection,
                    claim_command,
                    [
                        RuntimeEvent(
                            run_id=row["run_id"],
                            impulse_id=row["impulse_id"],
                            process_id=row["id"],
                            event_type="process.claimed",
                            payload={
                                "process_id": row["id"],
                                "worker_id": worker_id,
                                "attempt": updated["attempt"],
                                "lease_expires_at": expires_at.isoformat(),
                            },
                        )
                    ],
                )
                connection.commit()
                return _process_from_row(updated)
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def complete_process(
        self,
        *,
        run_id: str,
        process_id: str,
        output: dict[str, Any] | None = None,
    ) -> Process:
        return await self._finish_process(
            run_id=run_id,
            process_id=process_id,
            status=ProcessStatus.succeeded,
            output=output or {},
            error={},
        )

    async def fail_process(
        self,
        *,
        run_id: str,
        process_id: str,
        error: dict[str, Any] | None = None,
    ) -> Process:
        return await self._finish_process(
            run_id=run_id,
            process_id=process_id,
            status=ProcessStatus.failed,
            output={},
            error=error or {},
        )

    async def cancel_process(
        self,
        *,
        run_id: str,
        process_id: str,
        error: dict[str, Any] | None = None,
    ) -> Process:
        return await self._stop_process(
            run_id=run_id,
            process_id=process_id,
            status=ProcessStatus.cancelled,
            error=error or {},
        )

    async def timeout_process(
        self,
        *,
        run_id: str,
        process_id: str,
        error: dict[str, Any] | None = None,
    ) -> Process:
        return await self._stop_process(
            run_id=run_id,
            process_id=process_id,
            status=ProcessStatus.timed_out,
            error=error or {},
        )

    async def retry_process(
        self,
        *,
        run_id: str,
        process_id: str,
        available_at: datetime | None = None,
        error: dict[str, Any] | None = None,
    ) -> Process:
        next_available_at = available_at or _now()
        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT * FROM processes WHERE run_id = ? AND id = ?",
                    (run_id, process_id),
                ).fetchone()
                if row is None:
                    raise ValueError(f"Unknown process: {process_id!r}")
                process = _process_from_row(row)
                if process.status not in {
                    ProcessStatus.running,
                    ProcessStatus.failed,
                }:
                    raise ValueError(
                        f"Process {process_id!r} cannot be retried from status: "
                        f"{process.status.value}"
                    )
                if process.attempt >= process.max_attempts:
                    raise ValueError(f"Process {process_id!r} exhausted retry attempts")
                now = _now()
                connection.execute(
                    """
                    UPDATE processes
                    SET status = ?,
                        available_at = ?,
                        lease_owner = NULL,
                        lease_expires_at = NULL,
                        error_json = ?,
                        updated_at = ?,
                        finished_at = NULL
                    WHERE run_id = ? AND id = ?
                    """,
                    (
                        ProcessStatus.retry_wait.value,
                        next_available_at.isoformat(),
                        _dumps(error or process.error),
                        now.isoformat(),
                        run_id,
                        process_id,
                    ),
                )
                updated = connection.execute(
                    "SELECT * FROM processes WHERE run_id = ? AND id = ?",
                    (run_id, process_id),
                ).fetchone()
                connection.commit()
                return _process_from_row(updated)
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def transition_process(
        self,
        *,
        run_id: str,
        process_id: str,
        status: ProcessStatus,
        command: RuntimeCommand,
        events: Sequence[RuntimeEvent] = (),
        output: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        available_at: datetime | None = None,
        input: dict[str, Any] | None = None,
    ) -> tuple[Process, CommandSubmission]:
        expected_command_type = _PROCESS_TRANSITION_COMMANDS.get(status)
        if expected_command_type is None:
            raise ValueError(f"Unsupported process transition status: {status.value}")
        if command.run_id != run_id:
            raise ValueError("process transition command run_id must match run_id")
        if command.command_type != expected_command_type:
            raise ValueError(
                f"transition to {status.value!r} requires command_type "
                f"{expected_command_type!r}"
            )

        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing_command = connection.execute(
                    """
                    SELECT * FROM runtime_commands
                    WHERE run_id = ? AND idempotency_key = ?
                    """,
                    (command.run_id, command.idempotency_key),
                ).fetchone()
                if existing_command is not None:
                    stored_command = _command_from_row(existing_command)
                    stored_process_id = stored_command.payload.get(
                        "process_id",
                        process_id,
                    )
                    stored_process = connection.execute(
                        "SELECT * FROM processes WHERE run_id = ? AND id = ?",
                        (run_id, str(stored_process_id)),
                    ).fetchone()
                    if stored_process is None:
                        raise ValueError(
                            "Replayed process transition has no stored process: "
                            f"{stored_process_id!r}"
                        )
                    connection.commit()
                    return (
                        _process_from_row(stored_process),
                        CommandSubmission(
                            command=stored_command,
                            events=[],
                            replayed=True,
                        ),
                    )

                row = connection.execute(
                    "SELECT * FROM processes WHERE run_id = ? AND id = ?",
                    (run_id, process_id),
                ).fetchone()
                if row is None:
                    raise ValueError(f"Unknown process: {process_id!r}")
                process = _process_from_row(row)
                now = _now()

                _insert_runtime_command_row(connection, command)
                if status in {
                    ProcessStatus.succeeded,
                    ProcessStatus.failed,
                }:
                    # waiting -> succeeded/failed closes a suspended effector from
                    # an external outcome (homeostat resolution, child-run boundary).
                    if process.status not in {
                        ProcessStatus.running,
                        ProcessStatus.waiting,
                    }:
                        raise ValueError(
                            f"Process {process_id!r} is not running or waiting: "
                            f"{process.status.value}"
                        )
                    if (
                        process.lease_owner is not None
                        and command.actor != process.lease_owner
                    ):
                        raise ValueError(
                            f"Process {process_id!r} lease is held by "
                            f"{process.lease_owner!r}; actor "
                            f"{command.actor!r} cannot finish it"
                        )
                    stored_output = (
                        output or {}
                        if status == ProcessStatus.succeeded
                        else {}
                    )
                    stored_error = (
                        error or {}
                        if status == ProcessStatus.failed
                        else {}
                    )
                    connection.execute(
                        """
                        UPDATE processes
                        SET status = ?,
                            lease_owner = NULL,
                            lease_expires_at = NULL,
                            output_json = ?,
                            error_json = ?,
                            updated_at = ?,
                            finished_at = ?
                        WHERE run_id = ? AND id = ?
                        """,
                        (
                            status.value,
                            _dumps(stored_output),
                            _dumps(stored_error),
                            now.isoformat(),
                            now.isoformat(),
                            run_id,
                            process_id,
                        ),
                    )
                    # Atomic correlation_path advance: ready dependent pending effectors
                    # in the *same* transaction as this success. This eliminates the
                    # race between complete_process and a later advance_correlation_path
                    # call (the previous non-atomic pattern). Downstream readies use the
                    # canonical idempotency key so explicit advance later replays as no-op.
                    marker = process.metadata.get("correlation_path") or {}
                    cp_id = marker.get("correlation_path_id")
                    auto_advance = marker.get("auto_advance", True)
                    if cp_id and isinstance(cp_id, str) and auto_advance:
                        all_proc_rows = connection.execute(
                            "SELECT * FROM processes WHERE run_id = ?",
                            (run_id,),
                        ).fetchall()
                        all_procs = [_process_from_row(r) for r in all_proc_rows]
                        try:
                            from fala.correlation_paths import compute_correlation_path_readies
                            readies = compute_correlation_path_readies(all_procs, cp_id)
                        except Exception:
                            # Bad metadata should not fail the effector completion itself.
                            readies = []
                        for dep_id, new_input in readies:
                            ready_idem = f"process.ready:{dep_id}"
                            existing_ready = connection.execute(
                                """
                                SELECT 1 FROM runtime_commands
                                WHERE run_id = ? AND idempotency_key = ?
                                """,
                                (run_id, ready_idem),
                            ).fetchone()
                            if existing_ready is not None:
                                continue
                            ready_cmd = RuntimeCommand(
                                run_id=run_id,
                                command_type="process.ready",
                                idempotency_key=ready_idem,
                                actor=command.actor,
                                correlation_id=command.correlation_id,
                                causation_id=command.id,
                                payload={"process_id": dep_id},
                            )
                            ready_evt = RuntimeEvent(
                                run_id=run_id,
                                impulse_id=process.impulse_id,
                                process_id=dep_id,
                                event_type="process.readied",
                                payload={"process_id": dep_id},
                            )
                            _insert_runtime_command_row(connection, ready_cmd)
                            reg = (new_input or {}).get("regulation") or {}
                            ma_override = reg.get("max_attempts") if isinstance(reg, dict) else None
                            if ma_override is not None:
                                connection.execute(
                                    """
                                    UPDATE processes
                                    SET status = ?,
                                        input_json = ?,
                                        max_attempts = ?,
                                        updated_at = ?
                                    WHERE run_id = ? AND id = ?
                                    """,
                                    (
                                        ProcessStatus.ready.value,
                                        _dumps(new_input),
                                        int(ma_override),
                                        now.isoformat(),
                                        run_id,
                                        dep_id,
                                    ),
                                )
                            else:
                                connection.execute(
                                    """
                                    UPDATE processes
                                    SET status = ?,
                                        input_json = ?,
                                        updated_at = ?
                                    WHERE run_id = ? AND id = ?
                                    """,
                                    (
                                        ProcessStatus.ready.value,
                                        _dumps(new_input),
                                        now.isoformat(),
                                        run_id,
                                        dep_id,
                                    ),
                                )
                            _append_runtime_events(connection, ready_cmd, [ready_evt])
                    # Atomic dead-upstream cancellation: pending downstreams whose
                    # conduction can never be satisfied are cancelled in the same tx
                    cp_marker = process.metadata.get("correlation_path") or {}
                    cp_id = cp_marker.get("correlation_path_id")
                    auto_advance = cp_marker.get("auto_advance", True)
                    if cp_id and isinstance(cp_id, str) and auto_advance:
                        all_proc_rows = connection.execute(
                            "SELECT * FROM processes WHERE run_id = ?",
                            (run_id,),
                        ).fetchall()
                        all_procs = [_process_from_row(r) for r in all_proc_rows]
                        try:
                            from fala.correlation_paths import compute_correlation_path_dead_cancellations
                            cancels = compute_correlation_path_dead_cancellations(all_procs, cp_id)
                        except Exception:
                            cancels = []
                        for dep_id, err in cancels:
                            cancel_idem = f"process.cancel:{dep_id}:dead"
                            existing_c = connection.execute(
                                """
                                SELECT 1 FROM runtime_commands
                                WHERE run_id = ? AND idempotency_key = ?
                                """,
                                (run_id, cancel_idem),
                            ).fetchone()
                            if existing_c is not None:
                                continue
                            cancel_cmd = RuntimeCommand(
                                run_id=run_id,
                                command_type="process.cancel",
                                idempotency_key=cancel_idem,
                                actor=command.actor,
                                correlation_id=command.correlation_id,
                                causation_id=command.id,
                                payload={"process_id": dep_id},
                            )
                            cancel_evt = RuntimeEvent(
                                run_id=run_id,
                                impulse_id=process.impulse_id,
                                process_id=dep_id,
                                event_type="process.cancelled",
                                payload={"process_id": dep_id, "reason": "dead_upstream"},
                            )
                            _insert_runtime_command_row(connection, cancel_cmd)
                            connection.execute(
                                """
                                UPDATE processes
                                SET status = ?,
                                    lease_owner = NULL,
                                    lease_expires_at = NULL,
                                    error_json = ?,
                                    updated_at = ?,
                                    finished_at = ?
                                WHERE run_id = ? AND id = ?
                                """,
                                (
                                    ProcessStatus.cancelled.value,
                                    _dumps(err),
                                    now.isoformat(),
                                    now.isoformat(),
                                    run_id,
                                    dep_id,
                                ),
                            )
                            _append_runtime_events(connection, cancel_cmd, [cancel_evt])
                elif status == ProcessStatus.retry_wait:
                    if process.status not in {
                        ProcessStatus.running,
                        ProcessStatus.failed,
                    }:
                        raise ValueError(
                            f"Process {process_id!r} cannot be retried from status: "
                            f"{process.status.value}"
                        )
                    if (
                        process.lease_owner is not None
                        and command.actor != process.lease_owner
                    ):
                        raise ValueError(
                            f"Process {process_id!r} lease is held by "
                            f"{process.lease_owner!r}; actor "
                            f"{command.actor!r} cannot retry it"
                        )
                    if process.attempt >= process.max_attempts:
                        raise ValueError(
                            f"Process {process_id!r} exhausted retry attempts"
                        )
                    connection.execute(
                        """
                        UPDATE processes
                        SET status = ?,
                            available_at = ?,
                            lease_owner = NULL,
                            lease_expires_at = NULL,
                            error_json = ?,
                            updated_at = ?,
                            finished_at = NULL
                        WHERE run_id = ? AND id = ?
                        """,
                        (
                            ProcessStatus.retry_wait.value,
                            (available_at or now).isoformat(),
                            _dumps(error or process.error),
                            now.isoformat(),
                            run_id,
                            process_id,
                        ),
                    )
                elif status == ProcessStatus.waiting:
                    if process.status != ProcessStatus.running:
                        raise ValueError(
                            f"Process {process_id!r} cannot wait from status: "
                            f"{process.status.value}"
                        )
                    connection.execute(
                        """
                        UPDATE processes
                        SET status = ?,
                            lease_owner = NULL,
                            lease_expires_at = NULL,
                            output_json = ?,
                            updated_at = ?
                        WHERE run_id = ? AND id = ?
                        """,
                        (
                            ProcessStatus.waiting.value,
                            _dumps(output or process.output),
                            now.isoformat(),
                            run_id,
                            process_id,
                        ),
                    )
                elif status == ProcessStatus.ready:
                    if process.status != ProcessStatus.pending:
                        raise ValueError(
                            f"Process {process_id!r} cannot become ready from "
                            f"status: {process.status.value}"
                        )
                    ready_input = input if input is not None else process.input
                    reg = (ready_input or {}).get("regulation") or {}
                    ma_override = reg.get("max_attempts") if isinstance(reg, dict) else None
                    if ma_override is not None:
                        connection.execute(
                            """
                            UPDATE processes
                            SET status = ?,
                                input_json = ?,
                                max_attempts = ?,
                                updated_at = ?
                            WHERE run_id = ? AND id = ?
                            """,
                            (
                                ProcessStatus.ready.value,
                                _dumps(ready_input),
                                int(ma_override),
                                now.isoformat(),
                                run_id,
                                process_id,
                            ),
                        )
                    else:
                        connection.execute(
                            """
                            UPDATE processes
                            SET status = ?,
                                input_json = ?,
                                updated_at = ?
                            WHERE run_id = ? AND id = ?
                            """,
                            (
                                ProcessStatus.ready.value,
                                _dumps(ready_input),
                                now.isoformat(),
                                run_id,
                                process_id,
                            ),
                        )
                else:
                    if process.status in _TERMINAL_PROCESS_STATUSES:
                        raise ValueError(
                            f"Process {process_id!r} status is terminal: "
                            f"{process.status.value}"
                        )
                    connection.execute(
                        """
                        UPDATE processes
                        SET status = ?,
                            lease_owner = NULL,
                            lease_expires_at = NULL,
                            error_json = ?,
                            updated_at = ?,
                            finished_at = ?
                        WHERE run_id = ? AND id = ?
                        """,
                        (
                            status.value,
                            _dumps(error or {}),
                            now.isoformat(),
                            now.isoformat(),
                            run_id,
                            process_id,
                        ),
                    )

                updated = connection.execute(
                    "SELECT * FROM processes WHERE run_id = ? AND id = ?",
                    (run_id, process_id),
                ).fetchone()
                stored_events = _append_runtime_events(connection, command, events)
                connection.commit()
                return (
                    _process_from_row(updated),
                    CommandSubmission(
                        command=command,
                        events=stored_events,
                        replayed=False,
                    ),
                )
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def _stop_process(
        self,
        *,
        run_id: str,
        process_id: str,
        status: ProcessStatus,
        error: dict[str, Any],
    ) -> Process:
        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT * FROM processes WHERE run_id = ? AND id = ?",
                    (run_id, process_id),
                ).fetchone()
                if row is None:
                    raise ValueError(f"Unknown process: {process_id!r}")
                process = _process_from_row(row)
                if process.status in _TERMINAL_PROCESS_STATUSES:
                    raise ValueError(
                        f"Process {process_id!r} status is terminal: "
                        f"{process.status.value}"
                    )
                now = _now()
                connection.execute(
                    """
                    UPDATE processes
                    SET status = ?,
                        lease_owner = NULL,
                        lease_expires_at = NULL,
                        error_json = ?,
                        updated_at = ?,
                        finished_at = ?
                    WHERE run_id = ? AND id = ?
                    """,
                    (
                        status.value,
                        _dumps(error),
                        now.isoformat(),
                        now.isoformat(),
                        run_id,
                        process_id,
                    ),
                )
                updated = connection.execute(
                    "SELECT * FROM processes WHERE run_id = ? AND id = ?",
                    (run_id, process_id),
                ).fetchone()
                connection.commit()
                return _process_from_row(updated)
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def _finish_process(
        self,
        *,
        run_id: str,
        process_id: str,
        status: ProcessStatus,
        output: dict[str, Any],
        error: dict[str, Any],
    ) -> Process:
        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT * FROM processes WHERE run_id = ? AND id = ?",
                    (run_id, process_id),
                ).fetchone()
                if row is None:
                    raise ValueError(f"Unknown process: {process_id!r}")
                process = _process_from_row(row)
                if process.status not in {
                    ProcessStatus.running,
                    ProcessStatus.waiting,
                }:
                    raise ValueError(
                        f"Process {process_id!r} is not running or waiting: "
                        f"{process.status.value}"
                    )
                now = _now()
                connection.execute(
                    """
                    UPDATE processes
                    SET status = ?,
                        lease_owner = NULL,
                        lease_expires_at = NULL,
                        output_json = ?,
                        error_json = ?,
                        updated_at = ?,
                        finished_at = ?
                    WHERE run_id = ? AND id = ?
                    """,
                    (
                        status.value,
                        _dumps(output),
                        _dumps(error),
                        now.isoformat(),
                        now.isoformat(),
                        run_id,
                        process_id,
                    ),
                )
                updated = connection.execute(
                    "SELECT * FROM processes WHERE run_id = ? AND id = ?",
                    (run_id, process_id),
                ).fetchone()
                connection.commit()
                return _process_from_row(updated)
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def put_homeostat(self, homeostat: Homeostat) -> None:
        async with self._lock:
            with closing(self._connect()) as connection:
                _require_run_row(connection, homeostat.run_id)
                connection.execute(
                    """
                    INSERT INTO homeostats (
                        run_id, id, kind, impulse_id, status, values_json,
                        metadata, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id, id) DO UPDATE SET
                        kind = excluded.kind,
                        impulse_id = excluded.impulse_id,
                        status = excluded.status,
                        values_json = excluded.values_json,
                        metadata = excluded.metadata,
                        created_at = excluded.created_at,
                        updated_at = excluded.updated_at
                    """,
                    (
                        homeostat.run_id,
                        homeostat.id,
                        homeostat.kind,
                        homeostat.impulse_id,
                        homeostat.status.value,
                        _dumps(homeostat.values),
                        _dumps(homeostat.metadata),
                        homeostat.created_at.isoformat(),
                        homeostat.updated_at.isoformat(),
                    ),
                )
                connection.commit()

    async def save_homeostat(
        self,
        homeostat: Homeostat,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != homeostat.run_id:
            raise ValueError("homeostat command run_id must match homeostat run_id")
        if command.command_type not in {"homeostat.save", "homeostat.open"}:
            raise ValueError(
                "save_homeostat requires command_type 'homeostat.save' or 'homeostat.open'"
            )
        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    """
                    SELECT * FROM runtime_commands
                    WHERE run_id = ? AND idempotency_key = ?
                    """,
                    (command.run_id, command.idempotency_key),
                ).fetchone()
                if existing is not None:
                    connection.commit()
                    return CommandSubmission(
                        command=_command_from_row(existing),
                        events=[],
                        replayed=True,
                    )
                _require_run_row(connection, homeostat.run_id)
                if (
                    connection.execute(
                        "SELECT 1 FROM homeostats WHERE run_id = ? AND id = ?",
                        (homeostat.run_id, homeostat.id),
                    ).fetchone()
                    is not None
                ):
                    raise ValueError(f"Homeostat already exists: {homeostat.id!r}")

                _insert_runtime_command_row(connection, command)
                _insert_homeostat_row(connection, homeostat)
                stored_events = _append_runtime_events(connection, command, events)
                connection.commit()
                return CommandSubmission(
                    command=command,
                    events=stored_events,
                    replayed=False,
                )
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def get_homeostat(self, *, run_id: str, homeostat_id: str) -> Homeostat | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM homeostats WHERE run_id = ? AND id = ?",
                (run_id, homeostat_id),
            ).fetchone()
        return _homeostat_from_row(row) if row is not None else None

    async def transition_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        status: HomeostatStatus,
        command: RuntimeCommand,
        events: Sequence[RuntimeEvent] = (),
        values: dict[str, Any] | None = None,
    ) -> tuple[Homeostat, CommandSubmission]:
        expected_command_type = _HOMEOSTAT_TRANSITION_COMMANDS.get(status)
        if expected_command_type is None:
            raise ValueError(f"Unsupported homeostat terminal status: {status.value}")
        if command.run_id != run_id:
            raise ValueError("homeostat transition command run_id must match run_id")
        if command.command_type != expected_command_type:
            raise ValueError(
                f"transition to {status.value!r} requires command_type "
                f"{expected_command_type!r}"
            )
        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing_command = connection.execute(
                    """
                    SELECT * FROM runtime_commands
                    WHERE run_id = ? AND idempotency_key = ?
                    """,
                    (command.run_id, command.idempotency_key),
                ).fetchone()
                if existing_command is not None:
                    stored_command = _command_from_row(existing_command)
                    stored_homeostat_id = stored_command.payload.get("homeostat_id", homeostat_id)
                    stored_homeostat = connection.execute(
                        "SELECT * FROM homeostats WHERE run_id = ? AND id = ?",
                        (run_id, str(stored_homeostat_id)),
                    ).fetchone()
                    if stored_homeostat is None:
                        raise ValueError(
                            "Replayed homeostat transition has no stored homeostat: "
                            f"{stored_homeostat_id!r}"
                        )
                    connection.commit()
                    return (
                        _homeostat_from_row(stored_homeostat),
                        CommandSubmission(
                            command=stored_command,
                            events=[],
                            replayed=True,
                        ),
                    )

                row = connection.execute(
                    "SELECT * FROM homeostats WHERE run_id = ? AND id = ?",
                    (run_id, homeostat_id),
                ).fetchone()
                if row is None:
                    raise ValueError(f"Unknown homeostat: {homeostat_id!r}")
                homeostat = _homeostat_from_row(row)
                if homeostat.status != HomeostatStatus.open:
                    raise ValueError(
                        f"Homeostat {homeostat_id!r} is not open: {homeostat.status.value}"
                    )

                now = _now()
                _insert_runtime_command_row(connection, command)
                connection.execute(
                    """
                    UPDATE homeostats
                    SET status = ?,
                        values_json = ?,
                        updated_at = ?
                    WHERE run_id = ? AND id = ?
                    """,
                    (
                        status.value,
                        _dumps(values or {}),
                        now.isoformat(),
                        run_id,
                        homeostat_id,
                    ),
                )
                updated = connection.execute(
                    "SELECT * FROM homeostats WHERE run_id = ? AND id = ?",
                    (run_id, homeostat_id),
                ).fetchone()
                stored_events = _append_runtime_events(connection, command, events)
                connection.commit()
                return (
                    _homeostat_from_row(updated),
                    CommandSubmission(
                        command=command,
                        events=stored_events,
                        replayed=False,
                    ),
                )
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def complete_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        values: dict[str, Any] | None = None,
    ) -> Homeostat:
        return await self._finish_homeostat(
            run_id=run_id,
            homeostat_id=homeostat_id,
            status=HomeostatStatus.completed,
            values=values,
        )

    async def cancel_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        values: dict[str, Any] | None = None,
    ) -> Homeostat:
        return await self._finish_homeostat(
            run_id=run_id,
            homeostat_id=homeostat_id,
            status=HomeostatStatus.cancelled,
            values=values,
        )

    async def expire_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        values: dict[str, Any] | None = None,
    ) -> Homeostat:
        return await self._finish_homeostat(
            run_id=run_id,
            homeostat_id=homeostat_id,
            status=HomeostatStatus.expired,
            values=values,
        )

    async def _finish_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        status: HomeostatStatus,
        values: dict[str, Any] | None,
    ) -> Homeostat:
        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                row = connection.execute(
                    "SELECT * FROM homeostats WHERE run_id = ? AND id = ?",
                    (run_id, homeostat_id),
                ).fetchone()
                if row is None:
                    raise ValueError(f"Unknown homeostat: {homeostat_id!r}")
                homeostat = _homeostat_from_row(row)
                # Allow re-transition from terminal only if max_attempts not exhausted and caller intends to reopen.
                # For damping we treat "complete" as "settled" and do not reopen automatically.
                # Re-open is performed via save_homeostat with status=open and bumped attempt.
                if homeostat.status != HomeostatStatus.open:
                    if homeostat.attempt >= homeostat.max_attempts:
                        raise ValueError(
                            f"Homeostat {homeostat_id!r} exhausted attempts and is terminal: {homeostat.status.value}"
                        )
                    # permit transition only for explicit reopen path below
                now = _now()
                connection.execute(
                    """
                    UPDATE homeostats
                    SET status = ?,
                        values_json = ?,
                        updated_at = ?
                    WHERE run_id = ? AND id = ?
                    """,
                    (
                        status.value,
                        _dumps(values if values is not None else homeostat.values),
                        now.isoformat(),
                        run_id,
                        homeostat_id,
                    ),
                )
                updated = connection.execute(
                    "SELECT * FROM homeostats WHERE run_id = ? AND id = ?",
                    (run_id, homeostat_id),
                ).fetchone()
                connection.commit()
                return _homeostat_from_row(updated)
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def list_homeostats(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
        status: HomeostatStatus | None = None,
    ) -> list[Homeostat]:
        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if impulse_id is not None:
            clauses.append("impulse_id = ?")
            params.append(impulse_id)
        if status is not None:
            clauses.append("status = ?")
            params.append(status.value)
        with closing(self._connect()) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM homeostats
                WHERE {' AND '.join(clauses)}
                ORDER BY updated_at ASC, id ASC
                """,
                params,
            ).fetchall()
        return [_homeostat_from_row(row) for row in rows]

    async def put_projection(self, projection: Projection) -> None:
        async with self._lock:
            with closing(self._connect()) as connection:
                _require_run_row(connection, projection.run_id)
                _upsert_projection_row(connection, projection)
                connection.commit()

    async def save_projection(
        self,
        projection: Projection,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.run_id != projection.run_id:
            raise ValueError(
                "projection.save command run_id must match projection run_id"
            )
        if command.command_type != "projection.save":
            raise ValueError("save_projection requires command_type 'projection.save'")
        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    """
                    SELECT * FROM runtime_commands
                    WHERE run_id = ? AND idempotency_key = ?
                    """,
                    (command.run_id, command.idempotency_key),
                ).fetchone()
                if existing is not None:
                    connection.commit()
                    return CommandSubmission(
                        command=_command_from_row(existing),
                        events=[],
                        replayed=True,
                    )
                _require_run_row(connection, projection.run_id)
                _insert_runtime_command_row(connection, command)
                _upsert_projection_row(connection, projection)
                stored_events = _append_runtime_events(connection, command, events)
                connection.commit()
                return CommandSubmission(
                    command=command,
                    events=stored_events,
                    replayed=False,
                )
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def get_projection(self, *, run_id: str, name: str) -> Projection | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                "SELECT * FROM projections WHERE run_id = ? AND name = ?",
                (run_id, name),
            ).fetchone()
        return _projection_from_row(row) if row is not None else None

    async def list_projections(self, *, run_id: str) -> list[Projection]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT * FROM projections
                WHERE run_id = ?
                ORDER BY name ASC
                """,
                (run_id,),
            ).fetchall()
        return [_projection_from_row(row) for row in rows]

    async def rebuild_projections(
        self,
        *,
        run_id: str,
        names: Sequence[str] | None = None,
    ) -> list[Projection]:
        requested = (
            list(dict.fromkeys(names))
            if names is not None
            else list(_BUILT_IN_PROJECTIONS)
        )
        unsupported = sorted(set(requested) - set(_BUILT_IN_PROJECTIONS))
        if unsupported:
            raise ValueError(f"Unknown projection rebuild name: {unsupported[0]}")

        rebuilt: list[Projection] = []
        async with self._lock:
            with closing(self._connect()) as connection:
                _require_run_row(connection, run_id)
                for name in requested:
                    projection = _build_run_summary_projection(connection, run_id)
                    _upsert_projection_row(connection, projection)
                    rebuilt.append(projection)
                connection.commit()
        return rebuilt

    async def rebuild_projections_with_command(
        self,
        *,
        run_id: str,
        names: Sequence[str] | None,
        command: RuntimeCommand,
        events: Sequence[RuntimeEvent] = (),
    ) -> tuple[list[Projection], CommandSubmission]:
        if command.run_id != run_id:
            raise ValueError("projection.rebuild command run_id must match run_id")
        if command.command_type != "projection.rebuild":
            raise ValueError(
                "rebuild_projections_with_command requires command_type "
                "'projection.rebuild'"
            )
        requested = (
            list(dict.fromkeys(names)) if names is not None else list(_BUILT_IN_PROJECTIONS)
        )
        unsupported = sorted(set(requested) - set(_BUILT_IN_PROJECTIONS))
        if unsupported:
            raise ValueError(f"Unknown projection rebuild name: {unsupported[0]}")

        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    """
                    SELECT * FROM runtime_commands
                    WHERE run_id = ? AND idempotency_key = ?
                    """,
                    (command.run_id, command.idempotency_key),
                ).fetchone()
                if existing is not None:
                    stored_command = _command_from_row(existing)
                    projections: list[Projection] = []
                    for name in requested:
                        row = connection.execute(
                            "SELECT * FROM projections WHERE run_id = ? AND name = ?",
                            (run_id, name),
                        ).fetchone()
                        if row is None:
                            raise ValueError(
                                "Replayed projection rebuild has no stored projection: "
                                f"{name!r}"
                            )
                        projections.append(_projection_from_row(row))
                    connection.commit()
                    return (
                        projections,
                        CommandSubmission(
                            command=stored_command,
                            events=[],
                            replayed=True,
                        ),
                    )

                _require_run_row(connection, run_id)
                _insert_runtime_command_row(connection, command)
                stored_events = _append_runtime_events(connection, command, events)
                rebuilt: list[Projection] = []
                for name in requested:
                    projection = _build_run_summary_projection(connection, run_id)
                    _upsert_projection_row(connection, projection)
                    rebuilt.append(projection)
                connection.commit()
                return (
                    rebuilt,
                    CommandSubmission(
                        command=command,
                        events=stored_events,
                        replayed=False,
                    ),
                )
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def put_outbox_delivery(self, delivery: BridgeDelivery) -> None:
        await self._put_bridge_delivery("bridge_outbox", delivery)

    async def enqueue_outbox_delivery(
        self,
        delivery: BridgeDelivery,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.command_type != "bridge.outbox.enqueue":
            raise ValueError(
                "enqueue_outbox_delivery requires command_type "
                "'bridge.outbox.enqueue'"
            )
        return await self._submit_bridge_delivery(
            "bridge_outbox",
            delivery,
            command,
            events=events,
        )

    async def deliver_outbox_delivery(
        self,
        delivery: BridgeDelivery,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.command_type != "bridge.outbox.deliver":
            raise ValueError(
                "deliver_outbox_delivery requires command_type "
                "'bridge.outbox.deliver'"
            )
        return await self._submit_bridge_delivery(
            "bridge_outbox",
            delivery,
            command,
            events=events,
        )

    async def get_outbox_delivery(
        self,
        *,
        run_id: str,
        delivery_id: str,
    ) -> BridgeDelivery | None:
        return await self._get_bridge_delivery(
            "bridge_outbox",
            run_id=run_id,
            delivery_id=delivery_id,
        )

    async def list_outbox_deliveries(
        self,
        *,
        run_id: str,
        status: BridgeDeliveryStatus | None = None,
    ) -> list[BridgeDelivery]:
        return await self._list_bridge_deliveries(
            "bridge_outbox",
            run_id=run_id,
            status=status,
        )

    async def put_inbox_delivery(self, delivery: BridgeDelivery) -> None:
        await self._put_bridge_delivery("bridge_inbox", delivery)

    async def import_inbox_delivery(
        self,
        delivery: BridgeDelivery,
        impulse: Impulse,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent] = (),
    ) -> CommandSubmission:
        if command.command_type != "bridge.inbox.import":
            raise ValueError(
                "import_inbox_delivery requires command_type 'bridge.inbox.import'"
            )
        if impulse.run_id != delivery.run_id:
            raise ValueError("imported impulse run_id must match delivery run_id")
        return await self._submit_bridge_delivery(
            "bridge_inbox",
            delivery,
            command,
            events=events,
            impulse=impulse,
        )

    async def get_inbox_delivery(
        self,
        *,
        run_id: str,
        delivery_id: str,
    ) -> BridgeDelivery | None:
        return await self._get_bridge_delivery(
            "bridge_inbox",
            run_id=run_id,
            delivery_id=delivery_id,
        )

    async def list_inbox_deliveries(
        self,
        *,
        run_id: str,
        status: BridgeDeliveryStatus | None = None,
    ) -> list[BridgeDelivery]:
        return await self._list_bridge_deliveries(
            "bridge_inbox",
            run_id=run_id,
            status=status,
        )

    async def _put_bridge_delivery(
        self,
        table: str,
        delivery: BridgeDelivery,
    ) -> None:
        _require_bridge_table(table)
        async with self._lock:
            with closing(self._connect()) as connection:
                _require_run_row(connection, delivery.run_id)
                _upsert_bridge_delivery_row(connection, table, delivery)
                connection.commit()

    async def _submit_bridge_delivery(
        self,
        table: str,
        delivery: BridgeDelivery,
        command: RuntimeCommand,
        *,
        events: Sequence[RuntimeEvent],
        impulse: Impulse | None = None,
    ) -> CommandSubmission:
        _require_bridge_table(table)
        if command.run_id != delivery.run_id:
            raise ValueError("bridge command run_id must match delivery run_id")
        async with self._lock:
            connection = self._connect()
            try:
                connection.execute("BEGIN IMMEDIATE")
                existing = connection.execute(
                    """
                    SELECT * FROM runtime_commands
                    WHERE run_id = ? AND idempotency_key = ?
                    """,
                    (command.run_id, command.idempotency_key),
                ).fetchone()
                if existing is not None:
                    connection.commit()
                    return CommandSubmission(
                        command=_command_from_row(existing),
                        events=[],
                        replayed=True,
                    )
                _require_run_row(connection, delivery.run_id)
                if (
                    command.command_type == "bridge.outbox.enqueue"
                    and delivery.budget.spawned_runs is not None
                ):
                    # Enforced inside the enqueue transaction so concurrent
                    # enqueues cannot race past the budget.
                    spawned = {
                        spawned_run_id
                        for (spawned_run_id,) in connection.execute(
                            """
                            SELECT DISTINCT json_extract(target_ref, '$.run_id')
                            FROM bridge_outbox
                            WHERE run_id = ?
                            """,
                            (delivery.run_id,),
                        )
                        if spawned_run_id
                    }
                    if (
                        delivery.target.run_id not in spawned
                        and len(spawned) >= delivery.budget.spawned_runs
                    ):
                        raise FalaBudgetExceeded(
                            "Bridge delivery exceeded spawned_runs budget",
                            details={
                                "run_id": delivery.run_id,
                                "delivery_id": delivery.id,
                                "spawned_runs": delivery.budget.spawned_runs,
                                "spawned_run_ids": sorted(spawned),
                            },
                        )
                _insert_runtime_command_row(connection, command)
                if impulse is not None:
                    _upsert_impulse_row(connection, impulse)
                _upsert_bridge_delivery_row(connection, table, delivery)
                stored_events = _append_runtime_events(connection, command, events)
                connection.commit()
                return CommandSubmission(
                    command=command,
                    events=stored_events,
                    replayed=False,
                )
            except Exception:
                connection.rollback()
                raise
            finally:
                connection.close()

    async def _get_bridge_delivery(
        self,
        table: str,
        *,
        run_id: str,
        delivery_id: str,
    ) -> BridgeDelivery | None:
        _require_bridge_table(table)
        with closing(self._connect()) as connection:
            row = connection.execute(
                f"SELECT * FROM {table} WHERE run_id = ? AND id = ?",
                (run_id, delivery_id),
            ).fetchone()
        return _bridge_delivery_from_row(row) if row is not None else None

    async def _list_bridge_deliveries(
        self,
        table: str,
        *,
        run_id: str,
        status: BridgeDeliveryStatus | None = None,
    ) -> list[BridgeDelivery]:
        _require_bridge_table(table)
        clauses = ["run_id = ?"]
        params: list[Any] = [run_id]
        if status is not None:
            clauses.append("status = ?")
            params.append(status.value)
        with closing(self._connect()) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM {table}
                WHERE {' AND '.join(clauses)}
                ORDER BY updated_at ASC, id ASC
                """,
                params,
            ).fetchall()
        return [_bridge_delivery_from_row(row) for row in rows]


class RuntimeBackendService:
    def __init__(self, backend: RuntimeBackend) -> None:
        self.backend = backend

    @classmethod
    def sqlite(cls, path: str | Path) -> "RuntimeBackendService":
        return cls(Correlator(path))

    async def _replayed_submission(
        self,
        *,
        run_id: str,
        idempotency_key: str,
    ) -> CommandSubmission | None:
        command = await self.backend.get_command_by_idempotency(
            run_id=run_id,
            idempotency_key=idempotency_key,
        )
        if command is None:
            return None
        return CommandSubmission(command=command, events=[], replayed=True)

    async def create_run(
        self,
        run: Run,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Run, CommandSubmission]:
        command = RuntimeCommand(
            run_id=run.id,
            command_type="run.create",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={"run_id": run.id, "status": run.status.value},
        )
        event = RuntimeEvent(
            run_id=run.id,
            event_type="run.created",
            payload={"run_id": run.id, "status": run.status.value},
        )
        submission = await self.backend.create_run(run, command, events=[event])
        if submission.replayed:
            existing_run_id = submission.command.payload.get("run_id", run.id)
            existing = await self.backend.get_run(run_id=str(existing_run_id))
            if existing is None:
                raise ValueError(
                    "Replayed run create command has no stored run: "
                    f"{existing_run_id!r}"
                )
            return existing, submission

        return run, submission

    async def set_run_status(
        self,
        *,
        run_id: str,
        status: RunStatus,
        idempotency_key: str,
        reason: str | None = None,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Run, CommandSubmission]:
        return await self._set_run_status(
            run_id=run_id,
            status=status,
            command_type="run.status.set",
            event_type="run.status.changed",
            idempotency_key=idempotency_key,
            reason=reason,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def cancel_run(
        self,
        *,
        run_id: str,
        idempotency_key: str,
        reason: str | None = None,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Run, CommandSubmission]:
        return await self._set_run_status(
            run_id=run_id,
            status=RunStatus.cancel_requested,
            command_type="run.cancel",
            event_type="run.cancel_requested",
            idempotency_key=idempotency_key,
            reason=reason,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def _set_run_status(
        self,
        *,
        run_id: str,
        status: RunStatus,
        command_type: str,
        event_type: str,
        idempotency_key: str,
        reason: str | None = None,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Run, CommandSubmission]:
        existing = await self.backend.get_run(run_id=run_id)
        if existing is None:
            raise ValueError(f"Unknown run: {run_id!r}")
        replay = await self._replayed_submission(
            run_id=run_id,
            idempotency_key=idempotency_key,
        )
        if replay is not None:
            return existing, replay
        if existing.status != status:
            _validate_run_status_transition(existing.status, status)
        command = RuntimeCommand(
            run_id=run_id,
            command_type=command_type,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={
                "run_id": run_id,
                "status": status.value,
                **({"reason": reason} if reason is not None else {}),
            },
        )
        event = RuntimeEvent(
            run_id=run_id,
            event_type=event_type,
            payload={
                "from": existing.status.value,
                "to": status.value,
                **({"reason": reason} if reason is not None else {}),
            },
        )
        return await self.backend.transition_run(
            run_id=run_id,
            status=status,
            command=command,
            events=[event],
        )

    async def register_impulse_type(
        self,
        impulse_type: ImpulseType,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[ImpulseType, CommandSubmission]:
        command = RuntimeCommand(
            run_id=impulse_type.run_id,
            command_type="impulse_type.register",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={"impulse_type_id": impulse_type.id},
        )
        event = RuntimeEvent(
            run_id=impulse_type.run_id,
            event_type="impulse_type.registered",
            payload={"impulse_type_id": impulse_type.id},
        )
        submission = await self.backend.register_impulse_type(
            impulse_type,
            command,
            events=[event],
        )
        if submission.replayed:
            existing_type_id = submission.command.payload.get(
                "impulse_type_id",
                impulse_type.id,
            )
            existing = await self.backend.get_impulse_type(
                run_id=impulse_type.run_id,
                impulse_type_id=str(existing_type_id),
            )
            if existing is None:
                raise ValueError(
                    "Replayed impulse type command has no stored impulse type: "
                    f"{existing_type_id!r}"
            )
            return existing, submission

        return impulse_type, submission

    async def accept_impulse(
        self,
        impulse: Impulse,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Impulse, CommandSubmission]:
        command = RuntimeCommand(
            run_id=impulse.run_id,
            command_type="impulse.accept",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={"impulse_id": impulse.id, "impulse_type": impulse.impulse_type},
        )
        event = RuntimeEvent(
            run_id=impulse.run_id,
            impulse_id=impulse.id,  # note: event field impulse_id kept for now as FK name
            event_type="impulse.accepted",
            payload={"impulse_type": impulse.impulse_type},
        )
        submission = await self.backend.accept_impulse(
            impulse,
            command,
            events=[event],
        )
        if submission.replayed:
            existing_impulse_id = submission.command.payload.get("impulse_id", impulse.id)
            existing = await self.backend.get_impulse(
                run_id=impulse.run_id,
                impulse_id=str(existing_impulse_id),
            )
            if existing is None:
                raise ValueError(
                    "Replayed impulse acceptance command has no stored impulse: "
                    f"{existing_impulse_id!r}"
                )
            return existing, submission

        return impulse, submission

    async def record_impulse_relation(
        self,
        relation: ImpulseRelation,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[ImpulseRelation, CommandSubmission]:
        command = RuntimeCommand(
            run_id=relation.run_id,
            command_type="impulse_relation.record",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={
                "relation_id": relation.id,
                "relation_type": relation.relation_type,
                "source_impulse_id": relation.source_impulse_id,
                "target_impulse_id": relation.target_impulse_id,
            },
        )
        event = RuntimeEvent(
            run_id=relation.run_id,
            impulse_id=relation.source_impulse_id,
            event_type="impulse_relation.recorded",
            payload={
                "relation_id": relation.id,
                "relation_type": relation.relation_type,
                "target_impulse_id": relation.target_impulse_id,
            },
        )
        submission = await self.backend.record_impulse_relation(
            relation,
            command,
            events=[event],
        )
        if submission.replayed:
            existing_relation_id = submission.command.payload.get(
                "relation_id",
                relation.id,
            )
            existing = await self.backend.get_impulse_relation(
                run_id=relation.run_id,
                relation_id=str(existing_relation_id),
            )
            if existing is None:
                raise ValueError(
                    "Replayed impulse relation command has no stored relation: "
                    f"{existing_relation_id!r}"
            )
            return existing, submission

        return relation, submission

    async def record_association(
        self,
        association: Association,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Association, CommandSubmission]:
        command = RuntimeCommand(
            run_id=association.run_id,
            command_type="association.record",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={"association_id": association.id, "kind": association.kind},
        )
        event = RuntimeEvent(
            run_id=association.run_id,
            impulse_id=association.impulse_id,
            event_type="association.recorded",
            payload={"association_id": association.id, "kind": association.kind},
        )
        submission = await self.backend.record_association(
            association,
            command,
            events=[event],
        )
        if submission.replayed:
            associations = await self.backend.list_associations(
                run_id=association.run_id,
                impulse_id=association.impulse_id,
            )
            for existing in associations:
                if existing.id == submission.command.payload.get("association_id"):
                    return existing, submission
            raise ValueError(
                "Replayed association command has no stored association: "
                f"{submission.command.payload.get('association_id')!r}"
            )

        return association, submission

    async def record_reaction(
        self,
        reaction: Reaction,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Reaction, CommandSubmission]:
        # LUKA10: for correlation_path context (when the caller provides a process context
        # indirectly via impulse_id), require impulse_id to keep reactions bound to the
        # originating impulse. If no impulse_id, the reaction is still accepted (global
        # reactions remain useful for cross-impulse metadata), but we do not create
        # "invisible" reactions detached from a tor when an impulse_id was expected.
        # For now we only warn by rejecting None when run has active correlation_path
        # processes that expect impulse-scoped reactions. Simpler and stronger: require
        # impulse_id whenever the reaction is recorded for a run that already has
        # at least one impulse.
        if reaction.impulse_id is None:
            # Check whether the run already has impulses; if yes, require impulse_id.
            has_impulses = False
            try:
                imps = await self.backend.list_impulses(run_id=reaction.run_id)
                has_impulses = bool(imps)
            except Exception:
                has_impulses = False
            if has_impulses:
                raise FalaValidationError(
                    f"Reaction {reaction.id!r} for run {reaction.run_id!r} must carry impulse_id (orphan reactions leak entropy)",
                    details={"reaction_id": reaction.id, "run_id": reaction.run_id},
                )
        command = RuntimeCommand(
            run_id=reaction.run_id,
            command_type="reaction.record",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={
                "reaction_id": reaction.id,
                "kind": reaction.kind,
                "uri": reaction.uri,
            },
        )
        event = RuntimeEvent(
            run_id=reaction.run_id,
            impulse_id=reaction.impulse_id,
            event_type="reaction.recorded",
            payload={
                "reaction_id": reaction.id,
                "kind": reaction.kind,
                "uri": reaction.uri,
                "content_hash": reaction.content_hash,
            },
        )
        submission = await self.backend.record_reaction(
            reaction,
            command,
            events=[event],
        )
        if submission.replayed:
            existing_reaction_id = submission.command.payload.get(
                "reaction_id",
                reaction.id,
            )
            existing = await self.backend.get_reaction(
                run_id=reaction.run_id,
                reaction_id=str(existing_reaction_id),
            )
            if existing is None:
                raise ValueError(
                    "Replayed reaction command has no stored reaction: "
                    f"{existing_reaction_id!r}"
                )
            return existing, submission

        return reaction, submission

    async def schedule_process(
        self,
        process: Process,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Process, CommandSubmission]:
        replay = await self._replayed_submission(
            run_id=process.run_id,
            idempotency_key=idempotency_key,
        )
        if replay is not None:
            existing_process_id = replay.command.payload.get("process_id", process.id)
            existing = await self.backend.get_process(
                run_id=process.run_id,
                process_id=str(existing_process_id),
            )
            if existing is None:
                raise ValueError(
                    "Replayed process schedule command has no stored process: "
                    f"{existing_process_id!r}"
                )
            return existing, replay
        if process.status not in {
            ProcessStatus.pending,
            ProcessStatus.ready,
        }:
            raise ValueError(
                "schedule_process requires process status 'pending' or 'ready'"
            )
        command = RuntimeCommand(
            run_id=process.run_id,
            command_type="process.schedule",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={
                "process_id": process.id,
                "process_type": process.process_type,
                "impulse_id": process.impulse_id,
                "status": process.status.value,
            },
        )
        event = RuntimeEvent(
            run_id=process.run_id,
            impulse_id=process.impulse_id,
            process_id=process.id,
            event_type="process.scheduled",
            payload={
                "process_id": process.id,
                "process_type": process.process_type,
                "status": process.status.value,
            },
        )
        submission = await self.backend.schedule_process(
            process,
            command,
            events=[event],
        )
        if submission.replayed:
            existing_process_id = submission.command.payload.get("process_id", process.id)
            existing = await self.backend.get_process(
                run_id=process.run_id,
                process_id=str(existing_process_id),
            )
            if existing is None:
                raise ValueError(
                    "Replayed process schedule command has no stored process: "
                    f"{existing_process_id!r}"
                )
            return existing, submission

        return process, submission

    async def ready_process(
        self,
        *,
        run_id: str,
        process_id: str,
        input: dict[str, Any] | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Process, CommandSubmission]:
        existing = await self.backend.get_process(run_id=run_id, process_id=process_id)
        if existing is None:
            raise ValueError(f"Unknown process: {process_id!r}")
        replay = await self._replayed_submission(
            run_id=run_id,
            idempotency_key=idempotency_key,
        )
        if replay is not None:
            return existing, replay
        if existing.status != ProcessStatus.pending:
            raise ValueError(
                f"Process {process_id!r} cannot become ready from status: "
                f"{existing.status.value}"
            )
        command = RuntimeCommand(
            run_id=run_id,
            command_type="process.ready",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={"process_id": process_id},
        )
        event = RuntimeEvent(
            run_id=run_id,
            impulse_id=existing.impulse_id,
            process_id=process_id,
            event_type="process.readied",
            payload={"process_id": process_id},
        )
        return await self.backend.transition_process(
            run_id=run_id,
            process_id=process_id,
            status=ProcessStatus.ready,
            command=command,
            events=[event],
            input=input,
        )

    async def claim_next_ready_process(
        self,
        *,
        worker_id: str,
        run_id: str | None = None,
        lease_seconds: float = 300.0,
        all_runs: bool = False,
    ) -> Process | None:
        return await self.backend.claim_next_ready_process(
            worker_id=worker_id,
            run_id=run_id,
            lease_seconds=lease_seconds,
            all_runs=all_runs,
        )

    async def complete_process(
        self,
        *,
        run_id: str,
        process_id: str,
        output: dict[str, Any] | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Process, CommandSubmission]:
        existing = await self.backend.get_process(run_id=run_id, process_id=process_id)
        if existing is None:
            raise ValueError(f"Unknown process: {process_id!r}")
        replay = await self._replayed_submission(
            run_id=run_id,
            idempotency_key=idempotency_key,
        )
        if replay is not None:
            return existing, replay
        if existing.status not in {
            ProcessStatus.running,
            ProcessStatus.waiting,
        }:
            raise ValueError(
                f"Process {process_id!r} is not running or waiting: "
                f"{existing.status.value}"
            )
        # Enforce declared output_schema (captured at schedule from capability) when present.
        # This closes the semantic gap: previously output_schema in CapabilitySpec was
        # validated only at package load time, never against actual effector output.
        out_schema = existing.output_schema or {}
        if out_schema:
            try:
                Draft202012Validator(out_schema).validate(output or {})
            except JsonSchemaValidationError as exc:
                raise FalaValidationError(
                    f"Process {process_id!r} output does not match capability output_schema: {exc.message}",
                    details={"process_id": process_id, "schema": out_schema, "output": output or {}},
                ) from exc
        command = RuntimeCommand(
            run_id=run_id,
            command_type="process.complete",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={"process_id": process_id},
        )
        event = RuntimeEvent(
            run_id=run_id,
            impulse_id=existing.impulse_id,
            process_id=process_id,
            event_type="process.completed",
            # Attempt fingerprint: content addresses of what went in and what
            # came out, so a replay can verify an execution without carrying
            # the full payloads around.
            payload={
                "process_id": process_id,
                "attempt": existing.attempt,
                "input_digest": content_address_json(existing.input),
                "output_digest": content_address_json(output or {}),
            },
        )
        return await self.backend.transition_process(
            run_id=run_id,
            process_id=process_id,
            status=ProcessStatus.succeeded,
            command=command,
            events=[event],
            output=output,
        )

    async def fail_process(
        self,
        *,
        run_id: str,
        process_id: str,
        error: dict[str, Any] | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Process, CommandSubmission]:
        existing = await self.backend.get_process(run_id=run_id, process_id=process_id)
        if existing is None:
            raise ValueError(f"Unknown process: {process_id!r}")
        replay = await self._replayed_submission(
            run_id=run_id,
            idempotency_key=idempotency_key,
        )
        if replay is not None:
            return existing, replay
        if existing.status not in {
            ProcessStatus.running,
            ProcessStatus.waiting,
        }:
            raise ValueError(
                f"Process {process_id!r} is not running or waiting: "
                f"{existing.status.value}"
            )
        command = RuntimeCommand(
            run_id=run_id,
            command_type="process.fail",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={"process_id": process_id},
        )
        event = RuntimeEvent(
            run_id=run_id,
            impulse_id=existing.impulse_id,
            process_id=process_id,
            event_type="process.failed",
            payload={
                "process_id": process_id,
                "attempt": existing.attempt,
                "input_digest": content_address_json(existing.input),
                "error_digest": content_address_json(error or {}),
            },
        )
        return await self.backend.transition_process(
            run_id=run_id,
            process_id=process_id,
            status=ProcessStatus.failed,
            command=command,
            events=[event],
            error=error,
        )

    async def cancel_process(
        self,
        *,
        run_id: str,
        process_id: str,
        error: dict[str, Any] | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Process, CommandSubmission]:
        return await self._stop_process(
            run_id=run_id,
            process_id=process_id,
            error=error,
            status=ProcessStatus.cancelled,
            command_type="process.cancel",
            event_type="process.cancelled",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def timeout_process(
        self,
        *,
        run_id: str,
        process_id: str,
        error: dict[str, Any] | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Process, CommandSubmission]:
        return await self._stop_process(
            run_id=run_id,
            process_id=process_id,
            error=error,
            status=ProcessStatus.timed_out,
            command_type="process.timeout",
            event_type="process.timed_out",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def _stop_process(
        self,
        *,
        run_id: str,
        process_id: str,
        error: dict[str, Any] | None,
        status: ProcessStatus,
        command_type: str,
        event_type: str,
        idempotency_key: str,
        actor: str | None,
        correlation_id: str | None,
        causation_id: str | None,
    ) -> tuple[Process, CommandSubmission]:
        existing = await self.backend.get_process(run_id=run_id, process_id=process_id)
        if existing is None:
            raise ValueError(f"Unknown process: {process_id!r}")
        replay = await self._replayed_submission(
            run_id=run_id,
            idempotency_key=idempotency_key,
        )
        if replay is not None:
            return existing, replay
        if existing.status in _TERMINAL_PROCESS_STATUSES:
            raise ValueError(
                f"Process {process_id!r} status is terminal: {existing.status.value}"
            )
        command = RuntimeCommand(
            run_id=run_id,
            command_type=command_type,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={"process_id": process_id},
        )
        event = RuntimeEvent(
            run_id=run_id,
            impulse_id=existing.impulse_id,
            process_id=process_id,
            event_type=event_type,
            payload={"process_id": process_id},
        )
        if status == ProcessStatus.cancelled:
            return await self.backend.transition_process(
                run_id=run_id,
                process_id=process_id,
                status=ProcessStatus.cancelled,
                command=command,
                events=[event],
                error=error,
            )
        if status == ProcessStatus.timed_out:
            return await self.backend.transition_process(
                run_id=run_id,
                process_id=process_id,
                status=ProcessStatus.timed_out,
                command=command,
                events=[event],
                error=error,
            )
        raise ValueError(f"Unsupported process stop status: {status.value}")

    async def retry_process(
        self,
        *,
        run_id: str,
        process_id: str,
        available_at: datetime | None = None,
        error: dict[str, Any] | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Process, CommandSubmission]:
        existing = await self.backend.get_process(run_id=run_id, process_id=process_id)
        if existing is None:
            raise ValueError(f"Unknown process: {process_id!r}")
        replay = await self._replayed_submission(
            run_id=run_id,
            idempotency_key=idempotency_key,
        )
        if replay is not None:
            return existing, replay
        if existing.status not in {
            ProcessStatus.running,
            ProcessStatus.failed,
        }:
            raise ValueError(
                f"Process {process_id!r} cannot be retried from status: "
                f"{existing.status.value}"
            )
        command = RuntimeCommand(
            run_id=run_id,
            command_type="process.retry",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={"process_id": process_id},
        )
        event = RuntimeEvent(
            run_id=run_id,
            impulse_id=existing.impulse_id,
            process_id=process_id,
            event_type="process.retry_scheduled",
            payload={"process_id": process_id},
        )
        return await self.backend.transition_process(
            run_id=run_id,
            process_id=process_id,
            status=ProcessStatus.retry_wait,
            command=command,
            events=[event],
            available_at=available_at,
            error=error,
        )

    async def wait_process(
        self,
        *,
        run_id: str,
        process_id: str,
        output: dict[str, Any] | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Process, CommandSubmission]:
        existing = await self.backend.get_process(run_id=run_id, process_id=process_id)
        if existing is None:
            raise ValueError(f"Unknown process: {process_id!r}")
        replay = await self._replayed_submission(
            run_id=run_id,
            idempotency_key=idempotency_key,
        )
        if replay is not None:
            return existing, replay
        if existing.status != ProcessStatus.running:
            raise ValueError(
                f"Process {process_id!r} cannot wait from status: "
                f"{existing.status.value}"
            )
        command = RuntimeCommand(
            run_id=run_id,
            command_type="process.wait",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={"process_id": process_id},
        )
        event = RuntimeEvent(
            run_id=run_id,
            impulse_id=existing.impulse_id,
            process_id=process_id,
            event_type="process.waiting",
            payload={"process_id": process_id},
        )
        return await self.backend.transition_process(
            run_id=run_id,
            process_id=process_id,
            status=ProcessStatus.waiting,
            command=command,
            events=[event],
            output=output,
        )

    async def save_homeostat(
        self,
        homeostat: Homeostat,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Homeostat, CommandSubmission]:
        command = RuntimeCommand(
            run_id=homeostat.run_id,
            command_type="homeostat.save",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={"homeostat_id": homeostat.id, "status": homeostat.status.value},
        )
        event = RuntimeEvent(
            run_id=homeostat.run_id,
            impulse_id=homeostat.impulse_id,
            event_type="homeostat.saved",
            payload={"homeostat_id": homeostat.id, "status": homeostat.status.value},
        )
        submission = await self.backend.save_homeostat(homeostat, command, events=[event])
        if submission.replayed:
            existing_homeostat_id = submission.command.payload.get("homeostat_id", homeostat.id)
            existing = await self.backend.get_homeostat(
                run_id=homeostat.run_id,
                homeostat_id=str(existing_homeostat_id),
            )
            if existing is None:
                raise ValueError(
                    "Replayed homeostat command has no stored homeostat: "
                    f"{existing_homeostat_id!r}"
                )
            return existing, submission

        return homeostat, submission

    async def open_homeostat(
        self,
        homeostat: Homeostat,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Homeostat, CommandSubmission]:
        if homeostat.status != HomeostatStatus.open:
            raise ValueError("open_homeostat requires homeostat status 'open'")
        existing = await self.backend.get_homeostat(run_id=homeostat.run_id, homeostat_id=homeostat.id)
        replay = await self._replayed_submission(
            run_id=homeostat.run_id,
            idempotency_key=idempotency_key,
        )
        if replay is not None:
            if existing is None:
                raise ValueError(f"Replayed homeostat open has no stored homeostat: {homeostat.id!r}")
            return existing, replay
        if existing is not None:
            if existing.status == HomeostatStatus.open:
                raise ValueError(f"Homeostat already exists: {homeostat.id!r}")
            # damping retry path: allow reopen from terminal if attempts remain
            if existing.attempt >= (homeostat.max_attempts or existing.max_attempts):
                raise ValueError(f"Homeostat {homeostat.id!r} has no remaining attempts for reopen")
            # bump attempt for reopen
            reopened = existing.model_copy(update={"status": HomeostatStatus.open, "attempt": existing.attempt + 1})
            # fallthrough to save via backend
            homeostat = reopened
        command = RuntimeCommand(
            run_id=homeostat.run_id,
            command_type="homeostat.open",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={"homeostat_id": homeostat.id, "kind": homeostat.kind},
        )
        event = RuntimeEvent(
            run_id=homeostat.run_id,
            impulse_id=homeostat.impulse_id,
            event_type="homeostat.opened",
            payload={"homeostat_id": homeostat.id, "kind": homeostat.kind},
        )
        submission = await self.backend.save_homeostat(homeostat, command, events=[event])
        if submission.replayed:
            existing_homeostat_id = submission.command.payload.get("homeostat_id", homeostat.id)
            existing = await self.backend.get_homeostat(
                run_id=homeostat.run_id,
                homeostat_id=str(existing_homeostat_id),
            )
            if existing is None:
                raise ValueError(
                    "Replayed homeostat open command has no stored homeostat: "
                    f"{existing_homeostat_id!r}"
                )
            return existing, submission

        return homeostat, submission

    async def complete_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        values: dict[str, Any] | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Homeostat, CommandSubmission]:
        return await self._finish_homeostat(
            run_id=run_id,
            homeostat_id=homeostat_id,
            values=values,
            status=HomeostatStatus.completed,
            command_type="homeostat.complete",
            event_type="homeostat.completed",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def cancel_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        values: dict[str, Any] | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Homeostat, CommandSubmission]:
        return await self._finish_homeostat(
            run_id=run_id,
            homeostat_id=homeostat_id,
            values=values,
            status=HomeostatStatus.cancelled,
            command_type="homeostat.cancel",
            event_type="homeostat.cancelled",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def expire_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        values: dict[str, Any] | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Homeostat, CommandSubmission]:
        return await self._finish_homeostat(
            run_id=run_id,
            homeostat_id=homeostat_id,
            values=values,
            status=HomeostatStatus.expired,
            command_type="homeostat.expire",
            event_type="homeostat.expired",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )

    async def _finish_homeostat(
        self,
        *,
        run_id: str,
        homeostat_id: str,
        values: dict[str, Any] | None,
        status: HomeostatStatus,
        command_type: str,
        event_type: str,
        idempotency_key: str,
        actor: str | None,
        correlation_id: str | None,
        causation_id: str | None,
    ) -> tuple[Homeostat, CommandSubmission]:
        existing = await self.backend.get_homeostat(run_id=run_id, homeostat_id=homeostat_id)
        if existing is None:
            raise ValueError(f"Unknown homeostat: {homeostat_id!r}")
        replay = await self._replayed_submission(
            run_id=run_id,
            idempotency_key=idempotency_key,
        )
        if replay is not None:
            return existing, replay
        if existing.status != HomeostatStatus.open:
            raise ValueError(f"Homeostat {homeostat_id!r} is not open: {existing.status.value}")
        command = RuntimeCommand(
            run_id=run_id,
            command_type=command_type,
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={
                "homeostat_id": homeostat_id,
                "status": status.value,
                "value_keys": sorted((values or {}).keys()),
            },
        )
        event = RuntimeEvent(
            run_id=run_id,
            impulse_id=existing.impulse_id,
            event_type=event_type,
            payload={
                "homeostat_id": homeostat_id,
                "status": status.value,
                "value_keys": sorted((values or {}).keys()),
            },
        )
        return await self.backend.transition_homeostat(
            run_id=run_id,
            homeostat_id=homeostat_id,
            status=status,
            command=command,
            events=[event],
            values=values,
        )

    async def save_projection(
        self,
        projection: Projection,
        *,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[Projection, CommandSubmission]:
        command = RuntimeCommand(
            run_id=projection.run_id,
            command_type="projection.save",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={"projection_name": projection.name, "version": projection.version},
        )
        event = RuntimeEvent(
            run_id=projection.run_id,
            event_type="projection.saved",
            payload={"projection_name": projection.name, "version": projection.version},
        )
        submission = await self.backend.save_projection(
            projection,
            command,
            events=[event],
        )
        if submission.replayed:
            existing_name = submission.command.payload.get(
                "projection_name",
                projection.name,
            )
            existing = await self.backend.get_projection(
                run_id=projection.run_id,
                name=str(existing_name),
            )
            if existing is None:
                raise ValueError(
                    "Replayed projection command has no stored projection: "
                    f"{existing_name!r}"
                )
            return existing, submission

        return projection, submission

    async def rebuild_projections(
        self,
        *,
        run_id: str,
        names: Sequence[str] | None = None,
        idempotency_key: str,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[list[Projection], CommandSubmission]:
        requested = (
            list(dict.fromkeys(names)) if names is not None else list(_BUILT_IN_PROJECTIONS)
        )
        unsupported = sorted(set(requested) - set(_BUILT_IN_PROJECTIONS))
        if unsupported:
            raise ValueError(f"Unknown projection rebuild name: {unsupported[0]}")
        command = RuntimeCommand(
            run_id=run_id,
            command_type="projection.rebuild",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={"projection_names": requested},
        )
        event = RuntimeEvent(
            run_id=run_id,
            event_type="projection.rebuilt",
            payload={"projection_names": requested},
        )
        return await self.backend.rebuild_projections_with_command(
            run_id=run_id,
            names=requested,
            command=command,
            events=[event],
        )

    async def enqueue_bridge_delivery(
        self,
        delivery: BridgeDelivery,
        *,
        idempotency_key: str | None = None,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[BridgeDelivery, CommandSubmission]:
        delivery_key = idempotency_key or delivery.idempotency_key
        _validate_bridge_budget(delivery)
        delivery = delivery.model_copy(
            update={
                "idempotency_key": delivery_key,
                "run_id": delivery.source.run_id,
                "status": BridgeDeliveryStatus.pending,
                "updated_at": _now(),
            }
        )
        command = RuntimeCommand(
            run_id=delivery.run_id,
            command_type="bridge.outbox.enqueue",
            idempotency_key=delivery_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload=_bridge_command_payload(delivery),
        )
        event = RuntimeEvent(
            run_id=delivery.run_id,
            impulse_id=delivery.impulse.id,
            event_type="bridge.outbox.enqueued",
            payload=_bridge_event_payload(delivery),
        )
        submission = await self.backend.enqueue_outbox_delivery(
            delivery,
            command,
            events=[event],
        )
        if submission.replayed:
            existing = await self.backend.get_outbox_delivery(
                run_id=delivery.run_id,
                delivery_id=str(submission.command.payload.get("delivery_id")),
            )
            if existing is None:
                raise ValueError(
                    "Replayed bridge enqueue command has no stored outbox delivery: "
                    f"{submission.command.payload.get('delivery_id')!r}"
                )
            return existing, submission

        return delivery, submission

    async def import_bridge_delivery(
        self,
        delivery: BridgeDelivery,
        *,
        idempotency_key: str | None = None,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[BridgeDelivery, CommandSubmission]:
        delivery_key = idempotency_key or delivery.idempotency_key
        _validate_bridge_budget(delivery, next_attempt=True)
        imported_budget = _consume_bridge_budget(delivery.budget)
        local_impulse = delivery.impulse.model_copy(
            update={
                "run_id": delivery.target.run_id,
                "metadata": {
                    **delivery.impulse.metadata,
                    "source_runtime_id": delivery.source.runtime.id,
                    "source_run_id": delivery.source.run_id,
                    "source_impulse_id": delivery.impulse.id,
                },
                "updated_at": _now(),
            }
        )
        imported = delivery.model_copy(
            update={
                "run_id": delivery.target.run_id,
                "idempotency_key": delivery_key,
                "impulse": local_impulse,
                "status": BridgeDeliveryStatus.imported,
                "attempts": delivery.attempts + 1,
                "budget": imported_budget,
                "updated_at": _now(),
            }
        )
        command = RuntimeCommand(
            run_id=imported.run_id,
            command_type="bridge.inbox.import",
            idempotency_key=delivery_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload=_bridge_command_payload(imported),
        )
        event = RuntimeEvent(
            run_id=imported.run_id,
            impulse_id=imported.impulse.id,
            event_type="bridge.inbox.imported",
            payload=_bridge_event_payload(imported),
        )
        submission = await self.backend.import_inbox_delivery(
            imported,
            local_impulse,
            command,
            events=[event],
        )
        if submission.replayed:
            existing = await self.backend.get_inbox_delivery(
                run_id=imported.run_id,
                delivery_id=str(submission.command.payload.get("delivery_id")),
            )
            if existing is None:
                raise ValueError(
                    "Replayed bridge import command has no stored inbox delivery: "
                    f"{submission.command.payload.get('delivery_id')!r}"
                )
            return existing, submission

        return imported, submission

    async def deliver_bridge_delivery(
        self,
        *,
        run_id: str,
        delivery_id: str,
        target: "RuntimeBackendService",
        idempotency_key: str,
        import_idempotency_key: str | None = None,
        actor: str | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> tuple[
        BridgeDelivery,
        BridgeDelivery,
        CommandSubmission,
        CommandSubmission,
    ]:
        delivery = await self.backend.get_outbox_delivery(
            run_id=run_id,
            delivery_id=delivery_id,
        )
        if delivery is None:
            raise ValueError(f"Unknown outbox delivery: {delivery_id!r}")
        _validate_bridge_budget(delivery, next_attempt=True)

        imported, import_submission = await target.import_bridge_delivery(
            delivery,
            idempotency_key=import_idempotency_key or f"{idempotency_key}:import",
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
        )
        delivered = delivery.model_copy(
            update={
                "status": BridgeDeliveryStatus.delivered,
                "attempts": delivery.attempts + 1,
                "budget": _consume_bridge_budget(delivery.budget),
                "updated_at": _now(),
            }
        )
        command = RuntimeCommand(
            run_id=delivery.run_id,
            command_type="bridge.outbox.deliver",
            idempotency_key=idempotency_key,
            actor=actor,
            correlation_id=correlation_id,
            causation_id=causation_id,
            payload={
                **_bridge_command_payload(delivered),
                "inbox_run_id": imported.run_id,
                "inbox_delivery_id": imported.id,
            },
        )
        event = RuntimeEvent(
            run_id=delivery.run_id,
            impulse_id=delivery.impulse.id,
            event_type="bridge.outbox.delivered",
            payload={
                **_bridge_event_payload(delivered),
                "inbox_run_id": imported.run_id,
                "inbox_delivery_id": imported.id,
            },
        )
        delivery_submission = await self.backend.deliver_outbox_delivery(
            delivered,
            command,
            events=[event],
        )
        if delivery_submission.replayed:
            existing = await self.backend.get_outbox_delivery(
                run_id=delivery.run_id,
                delivery_id=str(delivery_submission.command.payload.get("delivery_id")),
            )
            if existing is None:
                raise ValueError(
                    "Replayed bridge delivery command has no stored outbox delivery: "
                    f"{delivery_submission.command.payload.get('delivery_id')!r}"
                )
            return existing, imported, delivery_submission, import_submission

        return delivered, imported, delivery_submission, import_submission

    async def list_associations(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
    ) -> list[Association]:
        return await self.backend.list_associations(
            run_id=run_id,
            impulse_id=impulse_id,
        )

    async def list_reactions(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
        kind: str | None = None,
    ) -> list[Reaction]:
        return await self.backend.list_reactions(
            run_id=run_id,
            impulse_id=impulse_id,
            kind=kind,
        )

    async def list_runs(
        self,
        *,
        status: RunStatus | None = None,
        limit: int | None = None,
    ) -> list[Run]:
        return await self.backend.list_runs(status=status, limit=limit)

    async def run_retention(
        self,
        *,
        before: datetime,
        statuses: Sequence[RunStatus] | None = None,
        dry_run: bool = True,
        keep_run_ids: set[str] | None = None,
    ) -> RuntimeRunRetentionPlan:
        selected_statuses = list(statuses or _TERMINAL_RUN_STATUSES)
        keep_run_ids = keep_run_ids or set()
        rows: list[Run] = []
        for status in selected_statuses:
            rows.extend(await self.backend.list_runs(status=status))
        rows = [
            run
            for run in rows
            if run.id not in keep_run_ids
            and (run.finished_at or run.updated_at or run.created_at) < before
        ]
        rows.sort(key=lambda run: (run.created_at, run.id))

        items: list[RuntimeRunRetentionItem] = []
        row_counts: dict[str, int] = {}
        deleted_run_count = 0
        for run in rows:
            counts: dict[str, int] = {}
            deleted = False
            if not dry_run:
                counts = await self.backend.delete_run(run_id=run.id)
                for table, count in counts.items():
                    row_counts[table] = row_counts.get(table, 0) + count
                deleted = bool(counts.get("runs"))
                if deleted:
                    deleted_run_count += 1
            items.append(
                RuntimeRunRetentionItem(
                    run_id=run.id,
                    status=run.status,
                    created_at=run.created_at,
                    updated_at=run.updated_at,
                    finished_at=run.finished_at,
                    deleted=deleted,
                    row_counts=counts,
                )
            )
        return RuntimeRunRetentionPlan(
            dry_run=dry_run,
            before=before,
            statuses=selected_statuses,
            candidate_count=len(items),
            deleted_run_count=deleted_run_count,
            row_counts=dict(sorted(row_counts.items())),
            runs=items,
        )

    async def run_reaction_gc(
        self,
        *,
        reaction_store: RuntimeReactionStore | None = None,
        dry_run: bool = True,
    ) -> RuntimeReactionGcPlan:
        if reaction_store is None:
            return RuntimeReactionGcPlan(dry_run=dry_run)
        referenced = await _referenced_reaction_digests(self.backend)
        candidates = [
            blob
            for digest, blob in sorted(reaction_store.blobs.items())
            if digest not in referenced
        ]
        deleted: list[str] = []
        if not dry_run and reaction_store.root is not None:
            for blob in candidates:
                if blob.location is None:
                    continue
                path = Path(blob.location)
                if path.exists() and path.is_file():
                    path.unlink()
                    deleted.append(blob.digest)
        return RuntimeReactionGcPlan(
            dry_run=dry_run,
            reaction_root=str(reaction_store.root) if reaction_store.root is not None else None,
            referenced_count=len(referenced),
            blob_count=len(reaction_store.blobs),
            candidate_count=len(candidates),
            deleted_count=len(deleted),
            bytes_reclaimable=sum(blob.size_bytes for blob in candidates),
            bytes_reclaimed=sum(blob.size_bytes for blob in candidates if blob.digest in deleted),
            candidates=[blob.digest for blob in candidates],
            deleted=deleted,
        )

    async def maintain_journal(
        self,
        *,
        older_than_days: float,
        keep_last: int | None = None,
        vacuum: bool = True,
        dry_run: bool = True,
        reaction_store: RuntimeReactionStore | None = None,
    ) -> RuntimeJournalMaintenancePlan:
        if older_than_days < 0:
            raise ValueError("older_than_days must be non-negative")
        if keep_last is not None and keep_last < 0:
            raise ValueError("keep_last must be non-negative")
        before = _now() - timedelta(days=older_than_days)
        keep_run_ids: set[str] = set()
        if keep_last:
            terminal_runs: list[Run] = []
            for status in _TERMINAL_RUN_STATUSES:
                terminal_runs.extend(await self.backend.list_runs(status=status))
            terminal_runs.sort(
                key=lambda run: (run.finished_at or run.updated_at or run.created_at, run.id),
                reverse=True,
            )
            keep_run_ids = {run.id for run in terminal_runs[:keep_last]}

        retention = await self.run_retention(
            before=before,
            dry_run=dry_run,
            keep_run_ids=keep_run_ids,
        )
        reaction_gc = await self.run_reaction_gc(
            reaction_store=reaction_store,
            dry_run=dry_run,
        )
        vacuum_result = None
        if vacuum and not dry_run:
            vacuum_result = await self.backend.vacuum()

        return RuntimeJournalMaintenancePlan(
            dry_run=dry_run,
            older_than_days=older_than_days,
            keep_last=keep_last,
            vacuum=vacuum,
            retention=retention,
            reaction_gc=reaction_gc,
            vacuum_result=vacuum_result,
            runs_archived=retention.deleted_run_count,
            bytes_reclaimed=reaction_gc.bytes_reclaimed
            + int((vacuum_result or {}).get("bytes_reclaimed", 0)),
        )

    async def save_runtime_pool(self, pool: RuntimePool) -> RuntimePool:
        await self.backend.put_runtime_pool(pool)
        return pool

    async def get_runtime_pool(self, *, pool_id: str) -> RuntimePool | None:
        return await self.backend.get_runtime_pool(pool_id=pool_id)

    async def list_runtime_pools(self) -> list[RuntimePool]:
        return await self.backend.list_runtime_pools()

    async def save_delegation_policy(
        self,
        policy: DelegationPolicy,
    ) -> DelegationPolicy:
        await self.backend.put_delegation_policy(policy)
        return policy

    async def get_delegation_policy(
        self,
        *,
        policy_id: str,
    ) -> DelegationPolicy | None:
        return await self.backend.get_delegation_policy(policy_id=policy_id)

    async def list_delegation_policies(
        self,
        *,
        pool_id: str | None = None,
    ) -> list[DelegationPolicy]:
        return await self.backend.list_delegation_policies(pool_id=pool_id)

    async def list_processes(
        self,
        *,
        run_id: str,
        status: ProcessStatus | None = None,
        impulse_id: str | None = None,
    ) -> list[Process]:
        return await self.backend.list_processes(
            run_id=run_id,
            status=status,
            impulse_id=impulse_id,
        )

    async def list_impulses(
        self,
        *,
        run_id: str,
        impulse_type: str | None = None,
        limit: int | None = None,
    ) -> list[Impulse]:
        return await self.backend.list_impulses(
            run_id=run_id,
            impulse_type=impulse_type,
            limit=limit,
        )

    async def list_impulse_types(self, *, run_id: str) -> list[ImpulseType]:
        return await self.backend.list_impulse_types(run_id=run_id)

    async def list_impulse_relations(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
        relation_type: str | None = None,
    ) -> list[ImpulseRelation]:
        return await self.backend.list_impulse_relations(
            run_id=run_id,
            impulse_id=impulse_id,
            relation_type=relation_type,
        )

    async def list_homeostats(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
        status: HomeostatStatus | None = None,
    ) -> list[Homeostat]:
        return await self.backend.list_homeostats(
            run_id=run_id,
            impulse_id=impulse_id,
            status=status,
        )

    async def diagnose_waits(
        self,
        *,
        run_id: str,
        impulse_id: str | None = None,
    ) -> WaitGraphDiagnostic:
        processes = await self.backend.list_processes(
            run_id=run_id,
            impulse_id=impulse_id,
        )
        homeostats = await self.backend.list_homeostats(run_id=run_id, impulse_id=impulse_id)
        return _diagnose_impulse_waits(
            run_id=run_id,
            impulse_id=impulse_id,
            processes=processes,
            homeostats=homeostats,
        )

    async def list_projections(self, *, run_id: str) -> list[Projection]:
        return await self.backend.list_projections(run_id=run_id)

    async def get_projection(
        self,
        *,
        run_id: str,
        name: str,
    ) -> Projection | None:
        projection = await self.backend.get_projection(run_id=run_id, name=name)
        if projection is None:
            return None
        # Freshness is measured against domain events only: saving or
        # rebuilding a projection journals its own projection.* event, which
        # would otherwise mark every just-written projection stale.
        events = await self.backend.list_events(run_id=run_id)
        watermark = 0
        for event in reversed(events):
            if not event.event_type.startswith("projection."):
                watermark = event.sequence
                break
        return projection.model_copy(
            update={"stale": projection.source_event_sequence < watermark}
        )

    async def observe_run(self, *, run_id: str) -> RunBoundary:
        run = await self.backend.get_run(run_id=run_id)
        if run is None:
            raise ValueError(f"Unknown run: {run_id!r}")
        processes = await self.backend.list_processes(run_id=run_id)
        counts: dict[str, int] = {}
        for process in processes:
            counts[process.status.value] = counts.get(process.status.value, 0) + 1
        return RunBoundary(
            run_id=run_id,
            status=run.status,
            derived_status=_derive_boundary_status(run.status, processes),
            process_status_counts=dict(sorted(counts.items())),
            event_watermark=await self._event_watermark(run_id=run_id),
        )

    async def _event_watermark(self, *, run_id: str) -> int:
        events = await self.backend.list_events(run_id=run_id)
        return events[-1].sequence if events else 0

    async def list_outbox_deliveries(
        self,
        *,
        run_id: str,
        status: BridgeDeliveryStatus | None = None,
    ) -> list[BridgeDelivery]:
        return await self.backend.list_outbox_deliveries(
            run_id=run_id,
            status=status,
        )

    async def list_inbox_deliveries(
        self,
        *,
        run_id: str,
        status: BridgeDeliveryStatus | None = None,
    ) -> list[BridgeDelivery]:
        return await self.backend.list_inbox_deliveries(
            run_id=run_id,
            status=status,
        )


def _require_bridge_table(table: str) -> None:
    if table not in _BRIDGE_TABLES:
        raise ValueError(f"Unsupported bridge table: {table!r}")


def _bridge_delivery_args(delivery: BridgeDelivery) -> tuple[Any, ...]:
    return (
        delivery.run_id,
        delivery.id,
        delivery.idempotency_key,
        _dumps(delivery.source.model_dump(mode="json")),
        _dumps(delivery.target.model_dump(mode="json")),
        _dumps(delivery.impulse.model_dump(mode="json")),
        _dumps(delivery.event_ref.model_dump(mode="json"))
        if delivery.event_ref is not None
        else None,
        delivery.pool_id,
        _dumps(delivery.budget.model_dump(mode="json")),
        delivery.status.value,
        delivery.attempts,
        _dumps(delivery.metadata),
        delivery.created_at.isoformat(),
        delivery.updated_at.isoformat(),
    )


def _bridge_command_payload(delivery: BridgeDelivery) -> dict[str, Any]:
    payload = {
        "delivery_id": delivery.id,
        "impulse_id": delivery.impulse.id,
        "source": delivery.source.model_dump(mode="json"),
        "target": delivery.target.model_dump(mode="json"),
        "pool_id": delivery.pool_id,
        "budget": delivery.budget.model_dump(mode="json"),
    }
    if delivery.event_ref is not None:
        payload["event_ref"] = delivery.event_ref.model_dump(mode="json")
    return payload


def _bridge_event_payload(delivery: BridgeDelivery) -> dict[str, Any]:
    return {
        **_bridge_command_payload(delivery),
        "status": delivery.status.value,
        "attempts": delivery.attempts,
    }


def _validate_bridge_budget(
    delivery: BridgeDelivery,
    *,
    next_attempt: bool = False,
) -> None:
    if delivery.status != BridgeDeliveryStatus.pending:
        return
    budget = delivery.budget
    if budget.runtime_hops is not None and budget.runtime_hops < 1:
        raise FalaBudgetExceeded(
            "Bridge delivery exceeded runtime hop budget",
            details={"delivery_id": delivery.id, "runtime_hops": budget.runtime_hops},
        )
    if budget.impulse_count is not None and budget.impulse_count < 1:
        raise FalaBudgetExceeded(
            "Bridge delivery exceeded impulse budget",
            details={"delivery_id": delivery.id, "impulse_count": budget.impulse_count},
        )
    if (
        budget.attempts is not None
        and next_attempt
        and delivery.attempts + 1 > budget.attempts
    ):
        raise FalaBudgetExceeded(
            "Bridge delivery exceeded attempt budget",
            details={
                "delivery_id": delivery.id,
                "attempts": budget.attempts,
                "next_attempt": delivery.attempts + 1,
            },
        )


def _consume_bridge_budget(budget: RuntimeBudget) -> RuntimeBudget:
    return budget.model_copy(
        update={
            "runtime_hops": max(0, budget.runtime_hops - 1)
            if budget.runtime_hops is not None
            else None,
            "impulse_count": max(0, budget.impulse_count - 1)
            if budget.impulse_count is not None
            else None,
        }
    )


def _validate_run_status_transition(
    current: RunStatus,
    target: RunStatus,
) -> None:
    if current in _TERMINAL_RUN_STATUSES:
        raise ValueError(f"Run status {current.value!r} is terminal")
    allowed = _RUN_STATUS_TRANSITIONS.get(current, set())
    if target not in allowed:
        raise ValueError(
            f"Invalid run status transition: {current.value!r} -> {target.value!r}"
        )


def _require_run_row(connection: sqlite3.Connection, run_id: str) -> None:
    row = connection.execute("SELECT 1 FROM runs WHERE id = ?", (run_id,)).fetchone()
    if row is None:
        raise ValueError(f"Unknown run: {run_id!r}")


def _insert_run_row(connection: sqlite3.Connection, run: Run) -> None:
    connection.execute(
        """
        INSERT INTO runs (
            id, status, title, package_id, package_version,
            package_digest, correlation_path_id, correlation_path_digest, runtime_version,
            backend_version, schema_version, metadata, created_at,
            updated_at, started_at, finished_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        _run_args(run),
    )


def _insert_runtime_command_row(
    connection: sqlite3.Connection,
    command: RuntimeCommand,
) -> None:
    connection.execute(
        """
        INSERT INTO runtime_commands (
            run_id, id, command_type, idempotency_key, actor,
            correlation_id, causation_id, payload, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            command.run_id,
            command.id,
            command.command_type,
            command.idempotency_key,
            command.actor,
            command.correlation_id,
            command.causation_id,
            _dumps(command.payload),
            command.created_at.isoformat(),
        ),
    )


def _insert_impulse_type_row(
    connection: sqlite3.Connection,
    impulse_type: ImpulseType,
) -> None:
    connection.execute(
        """
        INSERT INTO impulse_types (
            run_id, id, title, description, media_types,
            value_schema_json, metadata, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            impulse_type.run_id,
            impulse_type.id,
            impulse_type.title,
            impulse_type.description,
            json.dumps(impulse_type.media_types),
            _dumps(impulse_type.value_schema),
            _dumps(impulse_type.metadata),
            impulse_type.created_at.isoformat(),
            impulse_type.updated_at.isoformat(),
        ),
    )


def _insert_impulse_row(connection: sqlite3.Connection, impulse: Impulse) -> None:
    connection.execute(
        """
        INSERT INTO impulses (
            run_id, id, impulse_type, payload, metadata,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            impulse.run_id,
            impulse.id,
            impulse.impulse_type,
            _dumps(impulse.payload),
            _dumps(impulse.metadata),
            impulse.created_at.isoformat(),
            impulse.updated_at.isoformat(),
        ),
    )


def _upsert_impulse_row(connection: sqlite3.Connection, impulse: Impulse) -> None:
    connection.execute(
        """
        INSERT INTO impulses (
            run_id, id, impulse_type, payload, metadata,
            created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id, id) DO UPDATE SET
            impulse_type = excluded.impulse_type,
            payload = excluded.payload,
            metadata = excluded.metadata,
            updated_at = excluded.updated_at
        """,
        (
            impulse.run_id,
            impulse.id,
            impulse.impulse_type,
            _dumps(impulse.payload),
            _dumps(impulse.metadata),
            impulse.created_at.isoformat(),
            impulse.updated_at.isoformat(),
        ),
    )


def _insert_impulse_relation_row(
    connection: sqlite3.Connection,
    relation: ImpulseRelation,
) -> None:
    connection.execute(
        """
        INSERT INTO impulse_relations (
            run_id, id, relation_type, source_impulse_id,
            target_impulse_id, metadata, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            relation.run_id,
            relation.id,
            relation.relation_type,
            relation.source_impulse_id,
            relation.target_impulse_id,
            _dumps(relation.metadata),
            relation.created_at.isoformat(),
        ),
    )


def _insert_association_row(
    connection: sqlite3.Connection,
    association: Association,
) -> None:
    connection.execute(
        """
        INSERT INTO associations (
            run_id, id, kind, impulse_id, values_json,
            metadata, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            association.run_id,
            association.id,
            association.kind,
            association.impulse_id,
            _dumps(association.values),
            _dumps(association.metadata),
            association.created_at.isoformat(),
        ),
    )


def _insert_reaction_row(
    connection: sqlite3.Connection,
    reaction: Reaction,
) -> None:
    connection.execute(
        """
        INSERT INTO reactions (
            run_id, id, kind, uri, impulse_id, media_type,
            size_bytes, content_hash, metadata, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            reaction.run_id,
            reaction.id,
            reaction.kind,
            reaction.uri,
            reaction.impulse_id,
            reaction.media_type,
            reaction.size_bytes,
            reaction.content_hash,
            _dumps(reaction.metadata),
            reaction.created_at.isoformat(),
        ),
    )


def _insert_process_row(connection: sqlite3.Connection, process: Process) -> None:
    connection.execute(
        """
        INSERT INTO processes (
            run_id, id, process_type, impulse_id, status, priority,
            attempt, max_attempts, available_at, lease_owner,
            lease_expires_at, input_json, output_json, error_json,
            metadata, output_schema_json, created_at, updated_at, started_at, finished_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        _process_args(process),
    )


def _insert_homeostat_row(connection: sqlite3.Connection, homeostat: Homeostat) -> None:
    connection.execute(
        """
        INSERT INTO homeostats (
            run_id, id, kind, impulse_id, status,
            values_json, metadata, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            homeostat.run_id,
            homeostat.id,
            homeostat.kind,
            homeostat.impulse_id,
            homeostat.status.value,
            _dumps(homeostat.values),
            _dumps(homeostat.metadata),
            homeostat.created_at.isoformat(),
            homeostat.updated_at.isoformat(),
        ),
    )


def _upsert_bridge_delivery_row(
    connection: sqlite3.Connection,
    table: str,
    delivery: BridgeDelivery,
) -> None:
    _require_bridge_table(table)
    connection.execute(
        f"""
        INSERT INTO {table} (
            run_id, id, idempotency_key, source_ref, target_ref,
            impulse_json, event_ref, pool_id, budget, status,
            attempts, metadata, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id, id) DO UPDATE SET
            idempotency_key = excluded.idempotency_key,
            source_ref = excluded.source_ref,
            target_ref = excluded.target_ref,
            impulse_json = excluded.impulse_json,
            event_ref = excluded.event_ref,
            pool_id = excluded.pool_id,
            budget = excluded.budget,
            status = excluded.status,
            attempts = excluded.attempts,
            metadata = excluded.metadata,
            created_at = excluded.created_at,
            updated_at = excluded.updated_at
        """,
        _bridge_delivery_args(delivery),
    )


def _upsert_projection_row(
    connection: sqlite3.Connection,
    projection: Projection,
) -> None:
    connection.execute(
        """
        INSERT INTO projections (
            run_id, name, id, version, data,
            source_event_sequence, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id, name) DO UPDATE SET
            id = excluded.id,
            version = excluded.version,
            data = excluded.data,
            source_event_sequence = excluded.source_event_sequence,
            updated_at = excluded.updated_at
        """,
        (
            projection.run_id,
            projection.name,
            projection.id,
            projection.version,
            _dumps(projection.data),
            projection.source_event_sequence,
            projection.updated_at.isoformat(),
        ),
    )


def _append_runtime_events(
    connection: sqlite3.Connection,
    command: RuntimeCommand,
    events: Sequence[RuntimeEvent],
) -> list[RuntimeEvent]:
    stored_events: list[RuntimeEvent] = []
    for event in events:
        next_sequence = connection.execute(
            """
            SELECT COALESCE(MAX(sequence), 0) + 1
            FROM runtime_events
            WHERE run_id = ?
            """,
            (command.run_id,),
        ).fetchone()[0]
        stored_event = event.model_copy(
            update={
                "run_id": command.run_id,
                "sequence": int(next_sequence),
                "command_id": command.id,
                "actor": event.actor if event.actor is not None else command.actor,
                "correlation_id": event.correlation_id
                if event.correlation_id is not None
                else command.correlation_id,
                "causation_id": event.causation_id
                if event.causation_id is not None
                else command.causation_id,
            }
        )
        connection.execute(
            """
            INSERT INTO runtime_events (
                run_id, sequence, id, event_type, impulse_id,
                process_id, schema_version, command_id, actor,
                correlation_id, causation_id, payload, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                stored_event.run_id,
                stored_event.sequence,
                stored_event.id,
                stored_event.event_type,
                stored_event.impulse_id,
                stored_event.process_id,
                stored_event.schema_version,
                stored_event.command_id,
                stored_event.actor,
                stored_event.correlation_id,
                stored_event.causation_id,
                _dumps(stored_event.payload),
                stored_event.created_at.isoformat(),
            ),
        )
        stored_events.append(stored_event)
    return stored_events


def _count_rows(connection: sqlite3.Connection, table: str, run_id: str) -> int:
    return int(
        connection.execute(
            f"SELECT COUNT(*) FROM {table} WHERE run_id = ?",
            (run_id,),
        ).fetchone()[0]
    )


def _group_counts(
    connection: sqlite3.Connection,
    *,
    table: str,
    column: str,
    run_id: str,
) -> dict[str, int]:
    rows = connection.execute(
        f"""
        SELECT {column}, COUNT(*) AS count
        FROM {table}
        WHERE run_id = ?
        GROUP BY {column}
        ORDER BY {column} ASC
        """,
        (run_id,),
    ).fetchall()
    return {str(row[0]): int(row["count"]) for row in rows}


def _int_scalar(
    connection: sqlite3.Connection,
    sql: str,
    params: tuple[Any, ...],
) -> int:
    value = connection.execute(sql, params).fetchone()[0]
    return int(value or 0)


def _build_run_summary_projection(
    connection: sqlite3.Connection,
    run_id: str,
) -> Projection:
    event_type_counts = _group_counts(
        connection,
        table="runtime_events",
        column="event_type",
        run_id=run_id,
    )
    source_event_sequence = int(
        connection.execute(
            "SELECT COALESCE(MAX(sequence), 0) FROM runtime_events WHERE run_id = ?",
            (run_id,),
        ).fetchone()[0]
    )
    resource_accounting = {
        "reaction_bytes": _int_scalar(
            connection,
            "SELECT COALESCE(SUM(size_bytes), 0) FROM reactions WHERE run_id = ?",
            (run_id,),
        ),
        "bridge_command_count": _int_scalar(
            connection,
            """
            SELECT COUNT(*)
            FROM runtime_commands
            WHERE run_id = ? AND command_type LIKE 'bridge.%'
            """,
            (run_id,),
        ),
        "bridge_delivery_count": _count_rows(connection, "bridge_outbox", run_id)
        + _count_rows(connection, "bridge_inbox", run_id),
        "process_attempts": _int_scalar(
            connection,
            "SELECT COALESCE(SUM(attempt), 0) FROM processes WHERE run_id = ?",
            (run_id,),
        ),
        "process_input_bytes": _int_scalar(
            connection,
            "SELECT COALESCE(SUM(LENGTH(input_json)), 0) FROM processes WHERE run_id = ?",
            (run_id,),
        ),
        "process_output_bytes": _int_scalar(
            connection,
            "SELECT COALESCE(SUM(LENGTH(output_json)), 0) FROM processes WHERE run_id = ?",
            (run_id,),
        ),
        "spawned_run_count": _int_scalar(
            connection,
            """
            SELECT COUNT(DISTINCT json_extract(target_ref, '$.run_id'))
            FROM bridge_outbox
            WHERE run_id = ?
            """,
            (run_id,),
        ),
        "subprocess_count": _int_scalar(
            connection,
            """
            SELECT COUNT(*)
            FROM processes
            WHERE run_id = ? AND process_type = 'subprocess'
            """,
            (run_id,),
        ),
    }
    data = {
        "reaction_count": _count_rows(connection, "reactions", run_id),
        "impulse_count": _count_rows(connection, "impulses", run_id),
        "impulse_type_counts": _group_counts(
            connection,
            table="impulses",
            column="impulse_type",
            run_id=run_id,
        ),
        "event_count": sum(event_type_counts.values()),
        "event_type_counts": event_type_counts,
        "homeostat_count": _count_rows(connection, "homeostats", run_id),
        "homeostat_status_counts": _group_counts(
            connection,
            table="homeostats",
            column="status",
            run_id=run_id,
        ),
        "association_count": _count_rows(connection, "associations", run_id),
        "process_count": _count_rows(connection, "processes", run_id),
        "process_status_counts": _group_counts(
            connection,
            table="processes",
            column="status",
            run_id=run_id,
        ),
        "resource_accounting": resource_accounting,
        "run_id": run_id,
        "source_event_sequence": source_event_sequence,
    }
    return Projection(
        id="projection_run_summary",
        run_id=run_id,
        name="run_summary",
        version=1,
        data=data,
        source_event_sequence=source_event_sequence,
    )


def _diagnose_impulse_waits(
    *,
    run_id: str,
    impulse_id: str | None,
    processes: Sequence[Process],
    homeostats: Sequence[Homeostat],
) -> WaitGraphDiagnostic:
    processes_by_id = {process.id: process for process in processes}
    homeostats_by_id = {homeostat.id: homeostat for homeostat in homeostats}
    buckets: dict[str, list[str]] = {
        status.value: [] for status in ProcessStatus
    }
    for process in sorted(processes, key=lambda item: item.id):
        buckets[process.status.value].append(process.id)

    open_homeostats = sorted(
        homeostat.id for homeostat in homeostats if homeostat.status == HomeostatStatus.open
    )
    wait_edges: dict[str, list[str]] = {}
    blocked: list[WaitDiagnosticIssue] = []

    for process in sorted(processes, key=lambda item: item.id):
        if process.status == ProcessStatus.retry_wait:
            blocked.append(
                WaitDiagnosticIssue(
                    process_id=process.id,
                    status=process.status,
                    reason="retry_wait",
                    data={"available_at": process.available_at.isoformat()},
                )
            )
            continue
        if process.status != ProcessStatus.waiting:
            continue

        process_dependencies = _impulse_wait_refs(
            process,
            "wait_for_processes",
            "wait_for_process_ids",
            "blocked_by_processes",
        )
        homeostat_dependencies = _impulse_wait_refs(
            process,
            "wait_for_homeostats",
            "wait_for_homeostat_ids",
            "blocked_by_homeostats",
        )
        blocked_by: list[str] = []
        dependency_statuses: dict[str, str | None] = {}
        process_edges: list[str] = []

        for dependency_id in process_dependencies:
            dependency = processes_by_id.get(dependency_id)
            dependency_status = (
                dependency.status.value if dependency is not None else None
            )
            dependency_statuses[dependency_id] = dependency_status
            if dependency is None or dependency.status != ProcessStatus.succeeded:
                blocked_by.append(dependency_id)
                process_edges.append(dependency_id)

        for homeostat_id in homeostat_dependencies:
            homeostat = homeostats_by_id.get(homeostat_id)
            homeostat_status = homeostat.status.value if homeostat is not None else None
            key = f"homeostat:{homeostat_id}"
            dependency_statuses[key] = homeostat_status
            if homeostat is None or homeostat.status != HomeostatStatus.completed:
                blocked_by.append(key)

        if process_edges:
            wait_edges[process.id] = process_edges
        blocked.append(
            WaitDiagnosticIssue(
                process_id=process.id,
                status=process.status,
                reason="waiting"
                if blocked_by
                else "waiting_without_known_blocker",
                blocked_by=blocked_by,
                dependency_statuses=dependency_statuses,
            )
        )

    deadlocks = _impulse_wait_cycles(wait_edges)
    return WaitGraphDiagnostic(
        run_id=run_id,
        impulse_id=impulse_id,
        deadlocked=bool(deadlocks),
        deadlocks=deadlocks,
        wait_edges=wait_edges,
        blocked=blocked,
        open_homeostats=open_homeostats,
        pending=buckets[ProcessStatus.pending.value],
        ready=buckets[ProcessStatus.ready.value],
        running=buckets[ProcessStatus.running.value],
        waiting=buckets[ProcessStatus.waiting.value],
        retry_wait=buckets[ProcessStatus.retry_wait.value],
        succeeded=buckets[ProcessStatus.succeeded.value],
        failed=buckets[ProcessStatus.failed.value],
        cancel_requested=buckets[ProcessStatus.cancel_requested.value],
        cancelled=buckets[ProcessStatus.cancelled.value],
        timed_out=buckets[ProcessStatus.timed_out.value],
    )


def _impulse_wait_refs(process: Process, *keys: str) -> list[str]:
    values: list[str] = []
    for source in (process.input, process.metadata):
        for key in keys:
            value = source.get(key)
            if isinstance(value, str):
                values.append(value)
            elif isinstance(value, list):
                values.extend(item for item in value if isinstance(item, str))
    return list(dict.fromkeys(values))


def _impulse_wait_cycles(edges: dict[str, list[str]]) -> list[list[str]]:
    cycles: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()
    path: list[str] = []
    visiting: dict[str, int] = {}
    visited: set[str] = set()

    def visit(node: str) -> None:
        if node in visiting:
            cycle = path[visiting[node] :]
            key = tuple(sorted(cycle))
            if key not in seen:
                seen.add(key)
                cycles.append(cycle)
            return
        if node in visited:
            return

        visiting[node] = len(path)
        path.append(node)
        for dependency in edges.get(node, []):
            if dependency in edges:
                visit(dependency)
        path.pop()
        visiting.pop(node, None)
        visited.add(node)

    for node in sorted(edges):
        visit(node)
    return cycles


def _run_args(run: Run) -> tuple[Any, ...]:
    return (
        run.id,
        run.status.value,
        run.title,
        run.package_id,
        run.package_version,
        run.package_digest,
        run.correlation_path_id,
        run.correlation_path_digest,
        run.runtime_version,
        run.backend_version,
        run.schema_version,
        _dumps(run.metadata),
        run.created_at.isoformat(),
        run.updated_at.isoformat(),
        run.started_at.isoformat() if run.started_at is not None else None,
        run.finished_at.isoformat() if run.finished_at is not None else None,
    )


def _run_from_row(row: sqlite3.Row) -> Run:
    return Run(
        id=row["id"],
        status=RunStatus(row["status"]),
        title=row["title"],
        package_id=row["package_id"],
        package_version=row["package_version"],
        package_digest=row["package_digest"],
        correlation_path_id=row["correlation_path_id"],
        correlation_path_digest=row["correlation_path_digest"],
        runtime_version=row["runtime_version"],
        backend_version=row["backend_version"],
        schema_version=row["schema_version"],
        metadata=_loads(row["metadata"]),
        created_at=_dt(row["created_at"]),
        updated_at=_dt(row["updated_at"]),
        started_at=_dt(row["started_at"]) if row["started_at"] is not None else None,
        finished_at=_dt(row["finished_at"]) if row["finished_at"] is not None else None,
    )


def _runtime_pool_args(pool: RuntimePool) -> tuple[Any, ...]:
    return (
        pool.id,
        json.dumps(
            [runtime.model_dump(mode="json") for runtime in pool.runtimes],
            sort_keys=True,
            separators=(",", ":"),
        ),
        json.dumps(pool.impulse_types, sort_keys=True, separators=(",", ":")),
        _dumps(pool.metadata),
    )


def _runtime_pool_from_row(row: sqlite3.Row) -> RuntimePool:
    return RuntimePool(
        id=row["id"],
        runtimes=_loads_runtime_refs(row["runtimes_json"]),
        impulse_types=_loads_str_list(row["impulse_types"]),
        metadata=_loads(row["metadata"]),
    )


def _delegation_policy_args(policy: DelegationPolicy) -> tuple[Any, ...]:
    return (
        policy.id,
        policy.pool_id,
        json.dumps(policy.impulse_types, sort_keys=True, separators=(",", ":")),
        _dumps(policy.budget.model_dump(mode="json")),
        _dumps(policy.metadata),
    )


def _delegation_policy_from_row(row: sqlite3.Row) -> DelegationPolicy:
    return DelegationPolicy(
        id=row["id"],
        pool_id=row["pool_id"],
        impulse_types=_loads_str_list(row["impulse_types"]),
        budget=RuntimeBudget.model_validate(_loads(row["budget"])),
        metadata=_loads(row["metadata"]),
    )


def _process_args(process: Process) -> tuple[Any, ...]:
    return (
        process.run_id,
        process.id,
        process.process_type,
        process.impulse_id,
        process.status.value,
        process.priority,
        process.attempt,
        process.max_attempts,
        process.available_at.isoformat(),
        process.lease_owner,
        process.lease_expires_at.isoformat()
        if process.lease_expires_at is not None
        else None,
        _dumps(process.input),
        _dumps(process.output),
        _dumps(process.error),
        _dumps(process.metadata),
        _dumps(process.output_schema),
        process.created_at.isoformat(),
        process.updated_at.isoformat(),
        process.started_at.isoformat() if process.started_at is not None else None,
        process.finished_at.isoformat() if process.finished_at is not None else None,
    )


def _process_from_row(row: sqlite3.Row) -> Process:
    return Process(
        id=row["id"],
        run_id=row["run_id"],
        process_type=row["process_type"],
        impulse_id=row["impulse_id"],
        status=ProcessStatus(row["status"]),
        priority=row["priority"],
        attempt=row["attempt"],
        max_attempts=row["max_attempts"],
        available_at=_dt(row["available_at"]),
        lease_owner=row["lease_owner"],
        lease_expires_at=_dt(row["lease_expires_at"])
        if row["lease_expires_at"] is not None
        else None,
        input=_loads(row["input_json"]),
        output=_loads(row["output_json"]),
        error=_loads(row["error_json"]),
        metadata=_loads(row["metadata"]),
        output_schema=_loads(row["output_schema_json"]),
        created_at=_dt(row["created_at"]),
        updated_at=_dt(row["updated_at"]),
        started_at=_dt(row["started_at"]) if row["started_at"] is not None else None,
        finished_at=_dt(row["finished_at"]) if row["finished_at"] is not None else None,
    )
def _impulse_from_row(row: sqlite3.Row) -> Impulse:
    return Impulse(
        id=row["id"],
        run_id=row["run_id"],
        impulse_type=row["impulse_type"],
        payload=_loads(row["payload"]),
        metadata=_loads(row["metadata"]),
        created_at=_dt(row["created_at"]),
        updated_at=_dt(row["updated_at"]),
    )


def _impulse_type_from_row(row: sqlite3.Row) -> ImpulseType:
    return ImpulseType(
        id=row["id"],
        run_id=row["run_id"],
        title=row["title"],
        description=row["description"],
        media_types=_loads_str_list(row["media_types"]),
        value_schema=_loads(row["value_schema_json"]),
        metadata=_loads(row["metadata"]),
        created_at=_dt(row["created_at"]),
        updated_at=_dt(row["updated_at"]),
    )


def _impulse_relation_from_row(row: sqlite3.Row) -> ImpulseRelation:
    return ImpulseRelation(
        id=row["id"],
        run_id=row["run_id"],
        relation_type=row["relation_type"],
        source_impulse_id=row["source_impulse_id"],
        target_impulse_id=row["target_impulse_id"],
        metadata=_loads(row["metadata"]),
        created_at=_dt(row["created_at"]),
    )


def _command_from_row(row: sqlite3.Row) -> RuntimeCommand:
    return RuntimeCommand(
        id=row["id"],
        run_id=row["run_id"],
        command_type=row["command_type"],
        idempotency_key=row["idempotency_key"],
        actor=row["actor"],
        correlation_id=row["correlation_id"],
        causation_id=row["causation_id"],
        payload=_loads(row["payload"]),
        created_at=_dt(row["created_at"]),
    )


def _event_from_row(row: sqlite3.Row) -> RuntimeEvent:
    return RuntimeEvent(
        id=row["id"],
        run_id=row["run_id"],
        event_type=row["event_type"],
        schema_version=row["schema_version"],
        impulse_id=row["impulse_id"],
        process_id=row["process_id"],
        sequence=row["sequence"],
        command_id=row["command_id"],
        actor=row["actor"],
        correlation_id=row["correlation_id"],
        causation_id=row["causation_id"],
        payload=_loads(row["payload"]),
        created_at=_dt(row["created_at"]),
    )


def _association_from_row(row: sqlite3.Row) -> Association:
    return Association(
        id=row["id"],
        run_id=row["run_id"],
        kind=row["kind"],
        impulse_id=row["impulse_id"],
        values=_loads(row["values_json"]),
        metadata=_loads(row["metadata"]),
        created_at=_dt(row["created_at"]),
    )


def _reaction_from_row(row: sqlite3.Row) -> Reaction:
    return Reaction(
        id=row["id"],
        run_id=row["run_id"],
        kind=row["kind"],
        uri=row["uri"],
        impulse_id=row["impulse_id"],
        media_type=row["media_type"],
        size_bytes=row["size_bytes"],
        content_hash=row["content_hash"],
        metadata=_loads(row["metadata"]),
        created_at=_dt(row["created_at"]),
    )


def _homeostat_from_row(row: sqlite3.Row) -> Homeostat:
    return Homeostat(
        id=row["id"],
        run_id=row["run_id"],
        kind=row["kind"],
        impulse_id=row["impulse_id"],
        status=HomeostatStatus(row["status"]),
        values=_loads(row["values_json"]),
        metadata=_loads(row["metadata"]),
        created_at=_dt(row["created_at"]),
        updated_at=_dt(row["updated_at"]),
    )


def _projection_from_row(row: sqlite3.Row) -> Projection:
    return Projection(
        id=row["id"],
        run_id=row["run_id"],
        name=row["name"],
        version=row["version"],
        data=_loads(row["data"]),
        source_event_sequence=row["source_event_sequence"],
        updated_at=_dt(row["updated_at"]),
    )


def _bridge_delivery_from_row(row: sqlite3.Row) -> BridgeDelivery:
    return BridgeDelivery(
        id=row["id"],
        run_id=row["run_id"],
        idempotency_key=row["idempotency_key"],
        source=RunRef.model_validate(_loads(row["source_ref"])),
        target=RunRef.model_validate(_loads(row["target_ref"])),
        impulse=Impulse.model_validate(_loads(row["impulse_json"])),
        event_ref=EventRef.model_validate(_loads(row["event_ref"]))
        if row["event_ref"] is not None
        else None,
        pool_id=row["pool_id"],
        budget=RuntimeBudget.model_validate(_loads(row["budget"])),
        status=BridgeDeliveryStatus(row["status"]),
        attempts=row["attempts"],
        metadata=_loads(row["metadata"]),
        created_at=_dt(row["created_at"]),
        updated_at=_dt(row["updated_at"]),
    )


_TERMINAL_RUN_STATUSES = (
    RunStatus.completed,
    RunStatus.failed,
    RunStatus.cancelled,
    RunStatus.timed_out,
)


async def _referenced_reaction_digests(backend: RuntimeBackend) -> set[str]:
    digests: set[str] = set()
    for run in await backend.list_runs():
        for reaction in await backend.list_reactions(run_id=run.id):
            digest = reaction.content_hash
            if digest and digest.startswith("sha256:"):
                digest = digest.removeprefix("sha256:")
            if not digest and reaction.uri.startswith("fala-reaction://sha256/"):
                digest = reaction.uri.rsplit("/", 1)[-1]
            if digest:
                digests.add(digest.lower())
    return digests


__all__ = [
    "Reaction",
    "BridgeDelivery",
    "BridgeDeliveryStatus",
    "ProcessStatus",
    "Impulse",
    "RunStatus",
    "ImpulseRelation",
    "ImpulseType",
    "WaitDiagnosticIssue",
    "WaitGraphDiagnostic",
    "CommandSubmission",
    "DelegationPolicy",
    "EventRef",
    "Homeostat",
    "HomeostatStatus",
    "Association",
    "Process",
    "Projection",
    "RuntimeBackend",
    "RuntimeBackendService",
    "RuntimeBudget",
    "RuntimeCommand",
    "RuntimeEvent",
    "RuntimePool",
    "RuntimeRef",
    "Run",
    "RunBoundary",
    "RunRef",
    "SQLITE_RUNTIME_SCHEMA_VERSION",
    "Correlator",
]



